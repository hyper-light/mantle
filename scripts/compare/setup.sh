#!/usr/bin/env bash
# Makes a lane's comparator workspace for scripts/doors-trials.sh (its COMPARE): the Mac's
# (MACHINE=mac, aarch64-apple-darwin) at /Users/adalundhe/mantle-compare, or the Mac's Linux VM's
# (MACHINE=vm, aarch64-unknown-linux, Docker's VM on the Mac) at /Users/adalundhe/mantle-compare/vm.
# Each comparator is at the version and revision the box's COMPARATORS.md pins (its copy beside
# this script): the project's own release for the lane's platform where it publishes one, checked
# against the checksum it publishes, or else built from source at the pinned tag with the box's
# build commands (its scripts/build-*.sh), natively on the Mac and for the VM in a linux/arm64
# container on the VM.
#
#   scripts/compare/setup.sh mac|vm [--dry-run] [NAME...]
#
# NAME is image (vm only), garage, seaweedfs, rustfs, minio, msquic or quinn; with none, every one
# of the lane's, after the start scripts. --dry-run prints every URL, checksum and command and
# changes nothing. Run it on the Mac (macOS arm64); the vm lane's steps also need `docker` to
# reach the VM.
#
# The layout is the box's: <name>/bin, <name>/start.sh (msquic/secnetperf.sh), scripts/ and
# COMPARATORS.md (the box's, then this lane's table), beside src/<name> (shallow clones at the
# pinned tags), dl/ (downloads, with the checksum files their projects publish), manifest/ (each
# step's rows: file, version, revision, sha256, built or downloaded, from) and logs/. Every step is
# idempotent: what is present and matches its manifest is kept, a download resumes, a build resumes
# from its intermediates and removes them once it succeeds. A step that fails is reported with its
# log and the rest still run; the end of the run names what the lane lacks, for S3_COMPARATORS.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
SELF=$HERE/$(basename "$0")

# A lane's root is literal, so a removal never names a path a variable left empty (remove_under).
case ${1:-} in
  mac) C=/Users/adalundhe/mantle-compare ;;
  vm) C=/Users/adalundhe/mantle-compare/vm ;;
  *) sed -n '2,24p' "$0"; exit 2 ;;
esac
LANE=$1
shift
DRY=0
STEP=
NAMES=()
while [ $# -gt 0 ]; do
  case $1 in
    --dry-run) DRY=1 ;;
    --step) STEP=${2:?--step NAME}; shift ;;
    image | garage | seaweedfs | rustfs | minio | msquic | quinn) NAMES+=("$1") ;;
    *) echo "setup.sh: no step $1" >&2; exit 2 ;;
  esac
  shift
done
SRC=$C/src
DL=$C/dl
T=$C/build-tmp
MANIFEST=$C/manifest
LOGS=$C/logs
VM_IMAGE=mantle-bench:1.98.1

# The pins: the box's COMPARATORS.md, row by row.
GARAGE_VERSION=v2.5.0
GARAGE_REV=586726edb0170bb46f604e84fba09e5659610649
GARAGE_GIT=https://git.deuxfleurs.fr/Deuxfleurs/garage.git
SEAWEEDFS_VERSION=4.52
SEAWEEDFS_REV=984eac6a790be89e2a11de175a7413e1e3e32dd3
RUSTFS_VERSION=1.0.4
RUSTFS_REV=46337942d55f3a8e6a97b147ca98d9ea8115555d
MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
MINIO_REV=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
MINIO_GIT=https://github.com/minio/minio.git
MSQUIC_VERSION=v2.6.3
MSQUIC_REV=16ca4d8d785d34fb22285e7c652e477ac16475a5
QUICTLS_REV=ff36838bb69801cad56823159a036977bcbe5c75
MSQUIC_GIT=https://github.com/microsoft/msquic.git
QUINN_VERSION=quinn-proto-0.11.18
QUINN_REV=eaec0db4bcb698f76df736d89938743c88a2ad5f
QUINN_GIT=https://github.com/quinn-rs/quinn.git
# The box's build tools: cargo 1.98.1, Go 1.27.2 with GOTOOLCHAIN=local, and cmake 4.4.4 with
# ninja from a scratch venv. The box's record names no ninja version; 1.13.2 is the newest PyPI
# had when the box built (published 2026-08-30, the build 2026-11-04).
RUST=1.98.1
GO=1.27.2
CMAKE=4.4.4
NINJA=1.13.2

# The lane's release files and the sha256 each project publishes for them: GitHub's asset digests
# of the SeaweedFS and RustFS releases (read through the GitHub API, 2026-11-18; RustFS's also in
# the release's SHA256SUMS, itself checked by its digest) and go.dev/dl's for Go. Garage publishes
# none. The box's -j 8 was its 8-CPU set; a lane's build takes the CPUs it sees.
case $LANE in
  mac)
    PLATFORM=aarch64-apple-darwin
    SEAWEEDFS_FILE=darwin_arm64.tar.gz
    SEAWEEDFS_SHA=7aed4f7db8048a8aedab47a71ea4dd3875caa5abdb925d6aa72fb65438a47910
    RUSTFS_FILE=rustfs-macos-aarch64-v1.0.4.zip
    RUSTFS_SHA=49057482769d7904ef95dc40457d71122f48cbb8e9e0725a0817a3ec6daf8270
    GO_FILE=go1.27.2.darwin-arm64.tar.gz
    GO_SHA=76812b213b1b2302c978d28fa52fa92d541704b9e7d9d5db8002c50e4018c4c5
    LIBMSQUIC=libmsquic.2.6.3.dylib
    # Builds run on the Mac, their intermediates under build-tmp.
    B=$T
    # shellcheck disable=SC2016
    JOBS='$(sysctl -n hw.ncpu)'
    PRE=
    ;;
  vm)
    PLATFORM=aarch64-unknown-linux
    GARAGE_TARGET=aarch64-unknown-linux-musl
    SEAWEEDFS_FILE=linux_arm64.tar.gz
    SEAWEEDFS_SHA=b70a09540bf8aa2b9ea2a48b3b16abe27e162e90a0c3bf041f8d414caf52de8a
    RUSTFS_FILE=rustfs-linux-aarch64-musl-v1.0.4.zip
    RUSTFS_SHA=81414fd0e41969f4f5c2e0555951cee76d3f224c112c269c80f52b31276f187a
    LIBMSQUIC=libmsquic.so.2.6.3
    # Builds run in a container, their intermediates in the step's Docker volume at /build; the
    # sources are the host user's, which git in the container's root refuses without this.
    B=/build
    # shellcheck disable=SC2016
    JOBS='$(nproc)'
    PRE="git config --global --add safe.directory '*'"
    ;;
esac
RUSTFS_SUMS_SHA=835f1a59e05ee63c5c4e0759ca5ae41ed0dab54814f7fad254486bf963dc0657

die() {
  echo "setup.sh: $*" >&2
  exit 1
}

# A command that changes something: printed, then run unless --dry-run.
run() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  [ "$DRY" = 1 ] || "$@"
}

sha256() { shasum -a 256 "$1" | cut -d' ' -f1; }

# Removes REL, build-tmp/NAME, dl/unpack/NAME or manifest/STEP.new, under the lane's literal root
# alone.
remove_under() {
  [[ $1 =~ ^((build-tmp|dl/unpack)/[a-z0-9-]+|manifest/[a-z]+\.new)$ ]] || die "remove_under: $1"
  case $LANE in
    mac) run rm -rf /Users/adalundhe/mantle-compare/"$1" ;;
    vm) run rm -rf /Users/adalundhe/mantle-compare/vm/"$1" ;;
  esac
}

# fetch URL FILE SHA256: dl/FILE, downloaded (a partial download resumed) and checked against the
# published SHA256. SHA256 `none`: nothing is published, so the first download's is recorded in
# dl/FILE.sha256 and every later run checks against it.
fetch() {
  local url=$1 file=$2 want=$3 got
  echo "fetch $url"
  if [ "$want" = none ]; then
    echo "  to $DL/$file, sha256 none published: the first download's is kept in $DL/$file.sha256 and checked after"
  else
    echo "  to $DL/$file, sha256 $want"
  fi
  [ "$DRY" = 1 ] && return 0
  if [ "$want" = none ]; then
    want=
    [ -f "$DL/$file.sha256" ] && want=$(cat "$DL/$file.sha256")
  fi
  if [ ! -f "$DL/$file" ]; then
    mkdir -p "$DL"
    curl --fail --location --silent --show-error --continue-at - --output "$DL/$file.part" "$url"
    got=$(sha256 "$DL/$file.part")
    [ -z "$want" ] || [ "$got" = "$want" ] ||
      die "$DL/$file.part has sha256 $got, not $want: remove it by its literal path and rerun"
    [ -n "$want" ] || echo "$got" > "$DL/$file.sha256"
    mv "$DL/$file.part" "$DL/$file"
  fi
  got=$(sha256 "$DL/$file")
  [ -z "$want" ] || [ "$got" = "$want" ] || die "$DL/$file has sha256 $got, not $want"
  echo "  sha256 $got: checked"
}

# clone NAME URL TAG REV: src/NAME, a shallow clone at TAG, its HEAD checked against REV.
clone() {
  local name=$1 url=$2 tag=$3 rev=$4 head
  echo "source $url at $tag, revision $rev"
  if [ "$DRY" = 1 ] || [ ! -d "$SRC/$name/.git" ]; then
    run git clone --quiet --depth 1 --branch "$tag" "$url" "$SRC/$name"
  fi
  [ "$DRY" = 1 ] && return 0
  head=$(git -C "$SRC/$name" rev-parse HEAD)
  [ "$head" = "$rev" ] || die "$SRC/$name is at $head, not $rev: remove it by its literal path and rerun"
}

# build VOLUME IMAGE SCRIPT: the build SCRIPT (the box's commands), printed, then run by bash unless
# --dry-run: on the Mac itself; for the vm lane in a linux/arm64 container of IMAGE, the lane's
# root mounted where it is and the Docker volume VOLUME at /build.
build() {
  local volume=$1 image=$2 body kept=$C/scripts/build-$STEP.sh
  body=$(printf '%s\n' "$3" | sed '/^[[:space:]]*$/d')
  echo "+ write $kept: the script below, as the box keeps its build scripts"
  if [ "$DRY" = 0 ]; then
    {
      echo '#!/usr/bin/env bash'
      echo "# $STEP for the $LANE lane ($PLATFORM), written by mantle's scripts/compare/setup.sh $LANE."
      [ "$LANE" = mac ] ||
        echo "# Run as: docker run --rm --platform linux/arm64 -v $C:$C -v mantle-compare-vm-$STEP:/build $image bash $kept"
      printf '%s\n' "$body"
    } > "$kept"
    chmod 0755 "$kept"
  fi
  if [ "$LANE" = mac ]; then
    echo "+ bash -c SCRIPT, SCRIPT:"
    printf '%s\n' "$body" | sed 's/^/    /'
    [ "$DRY" = 1 ] || bash -c "$body"
  else
    local cmd=(docker run --rm --platform linux/arm64 -v "$C:$C" -v "$volume:/build" "$image" bash -c)
    printf '+'
    printf ' %q' "${cmd[@]}"
    echo " SCRIPT, SCRIPT:"
    printf '%s\n' "$body" | sed 's/^/    /'
    [ "$DRY" = 1 ] || "${cmd[@]}" "$body"
  fi
}

# A built step's intermediates removed once it succeeds: under build-tmp on the Mac, its Docker
# volume for the vm lane.
scrub() {
  local name=$1 dir
  shift
  if [ "$LANE" = vm ]; then
    run docker volume rm --force "mantle-compare-vm-$name"
  else
    for dir in "$@"; do remove_under "build-tmp/$dir"; done
  fi
}

# row NAME FILE VERSION REVISION MADE FROM: the step's manifest row for FILE (under the root), its
# sha256 read from the file; in a dry run, the row printed after `= `.
row() {
  if [ "$DRY" = 1 ]; then
    printf '= %s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "(once made)" "$5" "$6"
  else
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$(sha256 "$C/$2")" "$5" "$6" >> "$MANIFEST/$STEP.new"
  fi
}

# Whether the step's manifest names VERSION and REVISION and each file it lists still has its
# sha256; then the step is done.
present() {
  local f=$MANIFEST/$STEP.tsv file version rev sum
  [ "$DRY" = 0 ] && [ -s "$f" ] || return 1
  while IFS=$'\t' read -r _ file version rev sum _; do
    [ "$version" = "$1" ] && [ "$rev" = "$2" ] || return 1
    [ -f "$C/$file" ] && [ "$(sha256 "$C/$file")" = "$sum" ] || return 1
  done < "$f"
  echo "present: $f matches"
}

# Unpacks a checked archive into its own new, empty directory and installs the one file NAME in it
# as DEST.
unpack() {
  local archive=$1 name=$2 dest=$3 dir=$DL/unpack/$2 found
  remove_under "dl/unpack/$name"
  run mkdir -p "$dir"
  case $archive in
    *.zip) run unzip -q "$archive" -d "$dir" ;;
    *) run tar -xzf "$archive" -C "$dir" ;;
  esac
  run install -d "$(dirname "$dest")"
  if [ "$DRY" = 1 ]; then
    echo "+ install -m 0755 $dir/.../$name $dest"
    return 0
  fi
  found=$(find "$dir" -type f -name "$name")
  [ -n "$found" ] && [ "$(printf '%s\n' "$found" | wc -l | tr -d ' ')" = 1 ] || die "$archive holds no single $name: $found"
  run install -m 0755 "$found" "$dest"
  remove_under "dl/unpack/$name"
}

step_files() {
  local n
  for n in garage minio rustfs seaweedfs; do
    run install -d "$C/$n/bin"
    run install -m 0755 "$HERE/$n/start.sh" "$C/$n/start.sh"
  done
  run install -d "$C/msquic/bin" "$C/quinn/bin" "$C/scripts"
  run install -m 0755 "$HERE/msquic/secnetperf.sh" "$C/msquic/secnetperf.sh"
  [ "$LANE" = mac ] || run install -m 0644 "$HERE/mantle-bench.Dockerfile" "$C/scripts/mantle-bench.Dockerfile"
}

# The VM lane's image, which its cases run in and its msquic and quinn build in.
step_image() {
  [ "$LANE" = vm ] || die "the image is the vm lane's"
  # Always built: docker's layer cache makes a rebuild of an unchanged Dockerfile a no-op.
  run docker build --platform linux/arm64 -t "$VM_IMAGE" -f "$HERE/mantle-bench.Dockerfile" "$HERE"
  if [ "$DRY" = 1 ]; then
    printf '= image\t-\trust:%s\tmantle-bench.Dockerfile\t(image id once built)\tbuilt\tdocker build, scripts/compare/mantle-bench.Dockerfile\n' "$RUST"
  else
    printf 'image\t-\trust:%s\tmantle-bench.Dockerfile\t%s\tbuilt\tdocker build, scripts/compare/mantle-bench.Dockerfile (%s)\n' \
      "$RUST" "$(docker image inspect --format '{{.Id}}' "$VM_IMAGE")" "$VM_IMAGE" >> "$MANIFEST/$STEP.new"
  fi
}

step_garage() {
  present "$GARAGE_VERSION" "$GARAGE_REV" && return 0
  if [ "$LANE" = vm ]; then
    local file=garage-$GARAGE_VERSION-$GARAGE_TARGET
    fetch "https://garagehq.deuxfleurs.fr/_releases/$GARAGE_VERSION/$GARAGE_TARGET/garage" "$file" none
    run install -d "$C/garage/bin"
    run install -m 0755 "$DL/$file" "$C/garage/bin/garage"
    row garage garage/bin/garage "$GARAGE_VERSION" "$GARAGE_REV" downloaded \
      "https://garagehq.deuxfleurs.fr/_releases/$GARAGE_VERSION/$GARAGE_TARGET/garage (no checksum published; sha256 as first downloaded)"
    return 0
  fi
  # No macOS release: the release site publishes musl Linux builds alone.
  clone garage "$GARAGE_GIT" "$GARAGE_VERSION" "$GARAGE_REV"
  build - - "set -euo pipefail
JOBS=$JOBS
export CARGO_TARGET_DIR=$B/garage-target CARGO_HOME=$B/garage-cargo RUSTUP_TOOLCHAIN=$RUST
cd $SRC/garage
cargo build --locked --release -j \"\$JOBS\" -p garage --bin garage
install -d $C/garage/bin
install -m 0755 $B/garage-target/release/garage $C/garage/bin/garage"
  row garage garage/bin/garage "$GARAGE_VERSION" "$GARAGE_REV" built \
    "source at $GARAGE_VERSION, cargo $RUST \`build --locked --release -p garage --bin garage\` (default features: bundled-libs, metrics, lmdb, sqlite, k2v)"
  scrub garage garage-target garage-cargo
}

step_seaweedfs() {
  present "$SEAWEEDFS_VERSION" "$SEAWEEDFS_REV" && return 0
  local url=https://github.com/seaweedfs/seaweedfs/releases/download/$SEAWEEDFS_VERSION/$SEAWEEDFS_FILE
  local file=seaweedfs-$SEAWEEDFS_VERSION-$SEAWEEDFS_FILE published got
  fetch "$url" "$file" "$SEAWEEDFS_SHA"
  fetch "$url.md5" "$file.md5" none
  echo "md5 of $DL/$file against the published $DL/$file.md5"
  if [ "$DRY" = 0 ]; then
    published=$(awk '{print $1}' "$DL/$file.md5")
    got=$(md5 -q "$DL/$file")
    [ "$got" = "$published" ] || die "$DL/$file has md5 $got, the release publishes $published"
  fi
  unpack "$DL/$file" weed "$C/seaweedfs/bin/weed"
  [ "$LANE" = vm ] || [ "$DRY" = 1 ] || "$C/seaweedfs/bin/weed" version
  row seaweedfs seaweedfs/bin/weed "$SEAWEEDFS_VERSION" "$SEAWEEDFS_REV" downloaded \
    "$url: sha256 $SEAWEEDFS_SHA matches GitHub's asset digest, md5 the published .md5"
}

step_rustfs() {
  present "$RUSTFS_VERSION" "$RUSTFS_REV" && return 0
  local base=https://github.com/rustfs/rustfs/releases/download/$RUSTFS_VERSION listed
  fetch "$base/SHA256SUMS" "rustfs-$RUSTFS_VERSION-SHA256SUMS" "$RUSTFS_SUMS_SHA"
  fetch "$base/$RUSTFS_FILE" "$RUSTFS_FILE" "$RUSTFS_SHA"
  echo "sha256 of $DL/$RUSTFS_FILE against its line in $DL/rustfs-$RUSTFS_VERSION-SHA256SUMS"
  if [ "$DRY" = 0 ]; then
    listed=$(awk -v f="$RUSTFS_FILE" '{n = $2; sub(/^[*]/, "", n); sub(/^.*\//, "", n); if (n == f) print $1}' \
      "$DL/rustfs-$RUSTFS_VERSION-SHA256SUMS")
    [ "$listed" = "$RUSTFS_SHA" ] || die "SHA256SUMS lists $RUSTFS_FILE as ${listed:-nothing}, not $RUSTFS_SHA"
  fi
  unpack "$DL/$RUSTFS_FILE" rustfs "$C/rustfs/bin/rustfs"
  row rustfs rustfs/bin/rustfs "$RUSTFS_VERSION" "$RUSTFS_REV" downloaded \
    "$base/$RUSTFS_FILE: sha256 $RUSTFS_SHA matches GitHub's asset digest and the release SHA256SUMS (kept in dl/)"
}

step_minio() {
  present "$MINIO_VERSION" "$MINIO_REV" && return 0
  clone minio "$MINIO_GIT" "$MINIO_VERSION" "$MINIO_REV"
  local go_path='' goos=linux image=golang:$GO
  if [ "$LANE" = mac ]; then
    goos=darwin
    image=-
    go_path="export PATH=$T/go/bin:\$PATH"
    fetch "https://go.dev/dl/$GO_FILE" "$GO_FILE" "$GO_SHA"
    if [ "$DRY" = 1 ] || [ "$(head -n 1 "$T/go/VERSION" 2> /dev/null)" != "go$GO" ]; then
      remove_under build-tmp/go
      run mkdir -p "$T"
      run tar -xzf "$DL/$GO_FILE" -C "$T"
    fi
  fi
  # build-minio.sh's commands, GOOS the lane's; -modcacherw so the module cache can be removed.
  build mantle-compare-vm-minio "$image" "set -euo pipefail
$PRE
$go_path
JOBS=$JOBS
export GOCACHE=$B/minio-gocache GOMODCACHE=$B/minio-gomod GOTOOLCHAIN=local GOFLAGS=\"-p=\$JOBS -modcacherw\" CGO_ENABLED=0 GOOS=$goos GOARCH=arm64
go version | grep -q 'go$GO ' || { echo \"not Go $GO: \$(go version)\" >&2; exit 1; }
cd $SRC/minio
LDF=\$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go 2025-10-15T17:29:55Z)
echo \"ldflags: \$LDF\"
go build -tags kqueue -trimpath --ldflags \"\$LDF\" -o $C/minio/bin/minio"
  row minio minio/bin/minio "$MINIO_VERSION" "$MINIO_REV" built \
    "source at the tag, Go $GO (GOTOOLCHAIN=local, CGO off, GOOS=$goos), \`go build -tags kqueue -trimpath\` with gen-ldflags.go's release ldflags, as build-minio.sh"
  scrub minio go minio-gocache minio-gomod
}

step_msquic() {
  present "$MSQUIC_VERSION" "$MSQUIC_REV" && return 0
  clone msquic "$MSQUIC_GIT" "$MSQUIC_VERSION" "$MSQUIC_REV"
  # The submodules the box kept: quictls (the TLS library built), clog and googletest.
  run git -C "$SRC/msquic" submodule update --init --depth 1 submodules/quictls submodules/clog submodules/googletest
  if [ "$DRY" = 0 ] && [ "$(git -C "$SRC/msquic/submodules/quictls" rev-parse HEAD)" != "$QUICTLS_REV" ]; then
    die "quictls is not at $QUICTLS_REV"
  fi
  # build-msquic.sh's commands. CMAKE_POLICY_VERSION_MINIMUM: cmake 4 refuses a project that asks
  # for compatibility below 3.5 unless it is set (cmake 4.0 release notes, "Deprecated and Removed
  # Features"); quictls's make takes the jobs through MAKEFLAGS.
  build mantle-compare-vm-msquic "$VM_IMAGE" "set -euo pipefail
$PRE
JOBS=$JOBS
python3 -m venv $B/msquic-venv
$B/msquic-venv/bin/pip install --quiet --disable-pip-version-check cmake==$CMAKE ninja==$NINJA
export PATH=$B/msquic-venv/bin:\$PATH CMAKE_BUILD_PARALLEL_LEVEL=\$JOBS MAKEFLAGS=-j\$JOBS
cmake -S $SRC/msquic -B $B/msquic-build -G Ninja -DCMAKE_BUILD_TYPE=Release \\
  -DQUIC_TLS_LIB=quictls -DQUIC_BUILD_PERF=ON -DQUIC_BUILD_TOOLS=OFF -DQUIC_BUILD_TEST=OFF \\
  -DQUIC_ENABLE_LOGGING=OFF -DCMAKE_POLICY_VERSION_MINIMUM=3.5
cmake --build $B/msquic-build -j \"\$JOBS\" --target secnetperf
install -d $C/msquic/bin
cp -Pf $B/msquic-build/bin/Release/secnetperf $B/msquic-build/bin/Release/libmsquic.* $C/msquic/bin/"
  local how="source at the tag, quictls $QUICTLS_REV; cmake $CMAKE + ninja $NINJA from a venv, -DQUIC_TLS_LIB=quictls -DQUIC_BUILD_PERF=ON, Release, as build-msquic.sh"
  [ "$LANE" = mac ] || how="$how; in $VM_IMAGE"
  local lib=msquic/bin/$LIBMSQUIC f
  if [ "$DRY" = 0 ]; then
    for f in "$C"/msquic/bin/libmsquic.*; do [ -L "$f" ] || lib=${f#"$C"/}; done
  fi
  row msquic msquic/bin/secnetperf "$MSQUIC_VERSION" "$MSQUIC_REV" built "$how"
  row msquic "$lib" "$MSQUIC_VERSION" "$MSQUIC_REV" built "$how"
  scrub msquic msquic-build msquic-venv
}

step_quinn() {
  present "$QUINN_VERSION" "$QUINN_REV" && return 0
  clone quinn "$QUINN_GIT" "$QUINN_VERSION" "$QUINN_REV"
  # build-quinn.sh's commands.
  build mantle-compare-vm-quinn "$VM_IMAGE" "set -euo pipefail
$PRE
JOBS=$JOBS
export CARGO_TARGET_DIR=$B/quinn-target CARGO_HOME=$B/quinn-cargo RUSTUP_TOOLCHAIN=$RUST
cd $SRC/quinn
cargo build --locked --release -j \"\$JOBS\" -p perf --bin quinn-perf
install -d $C/quinn/bin
install -m 0755 $B/quinn-target/release/quinn-perf $C/quinn/bin/quinn-perf"
  local how="source at the tag, cargo $RUST \`build --locked --release -p perf --bin quinn-perf\`, as build-quinn.sh"
  [ "$LANE" = mac ] || how="$how; in $VM_IMAGE"
  row quinn quinn/bin/quinn-perf "$QUINN_VERSION" "$QUINN_REV" built "$how"
  scrub quinn quinn-target quinn-cargo
}

# One step, in a process of its own so that `set -e` holds in it: its manifest written whole once
# it succeeds.
if [ -n "$STEP" ]; then
  case $STEP in
    files | image | garage | seaweedfs | rustfs | minio | msquic | quinn) ;;
    *) die "no step $STEP" ;;
  esac
  if [ "$DRY" = 0 ] && [ "$STEP" != files ]; then
    mkdir -p "$MANIFEST"
    : > "$MANIFEST/$STEP.new"
  fi
  "step_$STEP"
  if [ "$DRY" = 0 ] && [ -s "$MANIFEST/$STEP.new" ]; then
    mv "$MANIFEST/$STEP.new" "$MANIFEST/$STEP.tsv"
  elif [ "$DRY" = 0 ] && [ -f "$MANIFEST/$STEP.new" ]; then
    remove_under "manifest/$STEP.new"
  fi
  exit 0
fi

preflight() {
  [ "$(uname -s)/$(uname -m)" = Darwin/arm64 ] || die "the $LANE lane is made on the Mac (Darwin/arm64), not $(uname -s)/$(uname -m)"
  local t
  for t in curl git shasum md5 tar unzip install awk; do command -v "$t" > /dev/null || die "needs $t"; done
  case $LANE in
    mac)
      for t in python3 perl make cc rustup; do command -v "$t" > /dev/null || die "needs $t"; done
      rustup run "$RUST" cargo --version > /dev/null 2>&1 || run rustup toolchain install "$RUST" --profile minimal
      ;;
    vm)
      [ "$(docker info --format '{{.OSType}}/{{.Architecture}}')" = linux/aarch64 ] ||
        die "docker's VM is not linux/aarch64: $(docker info --format '{{.OSType}}/{{.Architecture}}')"
      ;;
  esac
}

ALL=(garage seaweedfs rustfs minio msquic quinn)
[ "$LANE" = mac ] || ALL=(image "${ALL[@]}")
[ ${#NAMES[@]} -gt 0 ] || NAMES=("${ALL[@]}")
echo "setup.sh $LANE: $PLATFORM workspace $C$([ "$DRY" = 1 ] && echo ', dry run: nothing is changed')"
if [ "$DRY" = 0 ]; then
  preflight
  mkdir -p "$LOGS" "$MANIFEST"
fi
failed=()
planned=
for s in files "${NAMES[@]}"; do
  if [ "$DRY" = 1 ]; then
    echo
    echo "== $s"
    out=$(bash "$SELF" "$LANE" --dry-run --step "$s") || failed+=("$s")
    printf '%s\n' "$out" | grep -v '^= ' || true
    planned+=$(printf '%s\n' "$out" | sed -n 's/^= //p')$'\n'
  elif bash "$SELF" "$LANE" --step "$s" > "$LOGS/$s.log" 2>&1; then
    echo "$s: done (log $LOGS/$s.log)"
  else
    failed+=("$s")
    echo "$s: FAILED: $(tail -n 1 "$LOGS/$s.log") (log $LOGS/$s.log)"
  fi
done

# What the lane has: name, file, version, revision, sha256, built or downloaded.
echo
echo "The $LANE lane ($PLATFORM), $C:"
{
  printf 'name\tfile\tversion\trevision\tsha256\tmade\n'
  if [ "$DRY" = 1 ]; then
    printf '%s' "$planned" | sed '/^$/d' | cut -f1-6
  else
    for s in "${ALL[@]}"; do [ ! -f "$MANIFEST/$s.tsv" ] || cut -f1-6 "$MANIFEST/$s.tsv"; done
  fi
} | column -t -s $'\t'

# The lane's COMPARATORS.md: the box's, its rows kept as the box's, then this lane's.
lane_md() {
  local s
  cat "$HERE/COMPARATORS.md"
  echo
  echo "## The $LANE lane ($PLATFORM): \`$C\`"
  echo
  echo "Made by mantle's \`scripts/compare/setup.sh $LANE\` at $(git -C "$HERE" rev-parse --short HEAD 2> /dev/null || echo 'an unknown commit') ($(date -u +%F)). The table above is the box's (Linux x86_64),"
  echo "kept as the box's. Here are the same versions and revisions for $PLATFORM; the start scripts are the"
  echo "box's (mantle's \`scripts/compare/<name>/start.sh\`), so the durability settings above hold as written."
  echo
  echo "| Comparator | File | Version | Revision | sha256 | Made | From |"
  echo "|---|---|---|---|---|---|---|"
  for s in "${ALL[@]}"; do
    [ ! -f "$MANIFEST/$s.tsv" ] ||
      awk -F'\t' '{printf "| %s | `%s` | %s | `%s` | `%s` | %s | %s |\n", $1, $2, $3, $4, $5, $6, $7}' "$MANIFEST/$s.tsv"
  done
  echo
  for s in "${ALL[@]}"; do
    [ -f "$MANIFEST/$s.tsv" ] || echo "- Not made on this lane: $s ($(tail -n 1 "$LOGS/$s.log" 2> /dev/null || echo 'not run')); see \`logs/$s.log\`."
  done
  case $LANE in
    mac)
      if [ -f "$MANIFEST/garage.tsv" ]; then
        echo "- Garage publishes no macOS build (its release site has musl Linux builds alone), so it is built here from"
        echo "  the tag with the crate's default features; \`lmdb\`, the engine start.sh names, is one of them."
      fi
      echo "- MinIO's \`MINIO_FS_OSYNC=on\` is sync(2), which on macOS \"may return before the buffers are completely"
      echo "  flushed\" (macOS sync(2), BUGS): its full-sync mode promises less here than on Linux."
      echo "- msquic: \`msquic/secnetperf.sh\` finds libmsquic through DYLD_LIBRARY_PATH."
      echo "- Builds took every CPU (\`hw.ncpu\`) and no memory limit: macOS has no cgroups."
      ;;
    vm)
      echo "- Garage's release site publishes no checksum or signature: the sha256 is the first download's, kept in"
      echo "  \`dl/\` and checked on every later run."
      echo "- msquic and quinn were built in \`$VM_IMAGE\` (rust:$RUST, Debian 13) on the VM, so they link the glibc"
      echo "  they run with; MinIO in \`golang:$GO\` (CGO off, static). Each build took the CPUs its container saw."
      echo "- doors-trials.sh mounts this root read-only into each case's container of \`$VM_IMAGE\`."
      ;;
  esac
}
if [ "$DRY" = 1 ]; then
  echo
  echo "+ write $C/COMPARATORS.md: scripts/compare/COMPARATORS.md (the box's), then the $LANE lane's table from $MANIFEST"
else
  lane_md > "$C/COMPARATORS.md.new"
  mv "$C/COMPARATORS.md.new" "$C/COMPARATORS.md"
  echo "wrote $C/COMPARATORS.md"
fi

# What a failed step leaves the trials: an S3 comparator the lane lacks is left out by S3_COMPARATORS;
# a QUIC arm it lacks fails its cases, each accounted with its reason.
if [ ${#failed[@]} -gt 0 ]; then
  keep=
  for a in minio garage seaweedfs rustfs; do [ ! -f "$MANIFEST/$a.tsv" ] || keep+=" $a"; done
  echo
  echo "Not made: ${failed[*]}. Each one's log says why; a rerun resumes. Until each is made, the lane's trials"
  echo "run with S3_COMPARATORS=\"${keep# }\" (doors-trials.sh)."
  exit 1
fi
