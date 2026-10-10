#!/usr/bin/env bash
# Runs the workspace tests on Linux in a container (the host's architecture by default; pass
# --platform linux/amd64 for x86_64 under emulation). TMPDIR is a bind-mounted host
# directory or a tmpfs, so tests see a real file system rather than the image's overlay.
#
# A small or busy host, live: CPUS=<taskset list> (0, 0-1, ...) builds with every CPU, then runs
# the tests on those CPUs alone. std::thread::available_parallelism follows the affinity mask
# (sched_getaffinity(2)), so the engine's worker pools and the test harness size themselves to
# that host. HOGS=<n> also keeps n busy loops on those CPUs while the tests run: the tests share
# the CPUs with load they did not start, as on a loaded machine. The loops end with the
# container.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
platform="${PLATFORM:-}"
tmpfs_mode="${TMPFS:-0}"
cpus="${CPUS:-}"
hogs="${HOGS:-0}"
args=(run --rm -v "$PWD":/work -w /work
  -v mantle-cargo-registry:/usr/local/cargo/registry
  -v "mantle-target-${platform//\//-}":/work/target
  -e CARGO_TERM_COLOR=never)
[ -n "$platform" ] && args+=(--platform "$platform")
[ -n "${CARGO_BUILD_JOBS:-}" ] && args+=(-e CARGO_BUILD_JOBS)
if [ "$tmpfs_mode" = "1" ]; then
  args+=(--tmpfs /scratch:rw,exec,size=2g -e TMPDIR=/scratch)
else
  args+=(-v mantle-scratch:/scratch -e TMPDIR=/scratch)
fi
build=":"
load=":"
run="cargo test --workspace --locked $*"
if [ -n "$cpus" ]; then
  build="cargo test --workspace --locked --no-run"
  run="taskset -c $cpus $run"
fi
if [ "$hogs" != "0" ]; then
  [ -n "$cpus" ] || { echo "HOGS needs CPUS: the busy loops share the CPUs the tests run on" >&2; exit 2; }
  load="for _ in \$(seq $hogs); do taskset -c $cpus sh -c 'while :; do :; done' & done"
fi
docker "${args[@]}" rust:1.98.0 bash -c "rustup component add clippy rustfmt >/dev/null 2>&1; set -e; $build; $load; $run"
