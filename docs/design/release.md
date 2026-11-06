# Release

Status: built 2026-11-05. What a release of mantle publishes, for which targets, and how each archive
is proven before it is published. The facts it rests on are research/79's.

## 1. What is published

A release is the GitHub Release of a tag `vVERSION`, where VERSION is the workspace's
(`[workspace.package]` in `Cargo.toml`) and the tag must name it. For each supported target it
holds one archive and that archive's SHA-256:

| Target | Built and checked on | Archive |
|---|---|---|
| Linux x86_64 | `ubuntu-22.04` | `mantle-VERSION-x86_64-unknown-linux-gnu.tar.gz` |
| Linux arm64 | `ubuntu-22.04-arm` | `mantle-VERSION-aarch64-unknown-linux-gnu.tar.gz` |
| macOS arm64 | `macos-15` | `mantle-VERSION-aarch64-apple-darwin.tar.gz` |
| Windows x86_64 | `windows-2025` | `mantle-VERSION-x86_64-pc-windows-msvc.zip` |
| Windows arm64 | `windows-11-arm` | `mantle-VERSION-aarch64-pc-windows-msvc.zip` |

macOS on x86_64 is not a supported target (the owner, 2026-11-05) and has no archive. An archive
holds one directory of its own name: the `mantle` binary (`mantle.exe` on Windows, mode 755
elsewhere), which is both a cell's node (`mantle init`, `mantle serve`) and its client (the object
commands over the native QUIC protocol, while any S3 client works against its S3 listener), with
the repository's `LICENSE` and `README.md`. Every entry carries the commit's time and no owner, so
the same binary packs to the same bytes. Beside each archive, `ARCHIVE.sha256` holds its SHA-256 in
`sha256sum`'s form, which `sha256sum --check` (`shasum -a 256 --check` on macOS) verifies.

The binary is the workspace's release profile, `cargo build --release --locked -p mantle --target
TARGET` on the pinned toolchain, as `Dockerfile` and the gates' last step build it, from the
commit's sources with nothing cached. Each target is built on a runner of its own OS and
architecture. vorpal's release builds these targets the same way except Windows on arm64, which it
cross-builds on x86_64 (research/79 [1]); GitHub's `windows-11-arm` runs it natively [4], so each
mantle archive is built and checked on the kind of machine it is for.

**Linux.** A binary takes each glibc symbol at the version its build host's glibc has, so it runs
where glibc is at least the newest version among those it takes. The Linux archives are built on
Ubuntu 22.04, the oldest Ubuntu GitHub runs, whose glibc is 2.35 (research/79 [4], [8]), so they
take no symbol newer than 2.35's.

**macOS.** The linker signs an arm64 binary ad hoc, which is all macOS needs to run it; the binary is
not notarized, so one downloaded by a browser carries the quarantine attribute and Gatekeeper
refuses it until the attribute is removed (`xattr -d com.apple.quarantine mantle`). It runs on macOS
11.0 and later (`minos 11.0` in its `LC_BUILD_VERSION`, the least rustc supports on arm64,
research/79 [11]) and links only the system's libraries: libSystem, libiconv, CoreFoundation and
IOKit (`otool -L`).

**Windows.** The C runtime is linked in (`-C target-feature=+crt-static`, research/79 [9]), the
release build alone setting it. Linked dynamically, as rustc links it unless told otherwise [9], the
binary needs Visual C++'s `vcruntime140.dll`, which an application's users "must" be given, since
"most users probably don't have Visual Studio installed" [10]; the Universal CRT is part of Windows
10 and later [10]. Linked in, the binary needs no Visual C++ Redistributable on its host.

## 2. How each archive is proven

Before its job uploads it, each target's archive is checked as a user takes it, on its own runner
(`scripts/release.py check`):

1. The archive's SHA-256 is the one beside it, and it holds exactly the directory, the binary at
   mode 755, `LICENSE` and `README.md`.
2. The unpacked binary says `mantle VERSION`.
3. It makes a cell of three on loopback with `mantle init`, each founder's QUIC, plane and S3 ports
   stated (each one the OS gave free), its volume in a directory of its own and its failure domain
   its own. It serves the cell as three `mantle serve` processes from what init wrote, each waited
   for until it says it bound the stated addresses and that the cell's root range enlisted it, and
   then ready to `mantle status`.
4. Through the native QUIC door, as the client of a directory `mantle init client` made, its object
   commands: `mb`; `cp` of a 3 MiB object from a file and back to one, and of 9 MiB from standard
   input, past the 8 MiB runs a copy from standard input is read in, and back to standard output;
   `head`; `ls` of the buckets, of a prefix's keys with their sizes, and of the bucket's prefixes; a
   multipart upload's every step (`create`, `put` of a 5 MiB part from a file and the last from
   standard input, `parts`, `uploads`, `complete`), read back, and another upload aborted.
5. Through the S3 HTTP door, the AWS CLI v2 with the profile `mantle init` wrote (its environment's
   own `AWS_*` variables removed, as they outrank a profile): `s3 mb`, `s3 cp` of an object below the
   CLI's 8 MiB multipart threshold and one past it, up and back, `s3 ls` with their sizes and
   `s3api head-object`.
6. Across the doors: the AWS CLI reads back the objects the native door put, the native door the
   one the AWS CLI put, and each lists the other's bucket. Then each door removes its own objects
   and bucket: `head` of a removed object says `NoSuchKey`, and `rb` of a bucket still holding an
   object `BucketNotEmpty`.
7. One founder stops in order at a `stop` line, which asks the orderly stop on every host, its peers
   serving on: it says `stopping` and `stopped` and exits 0.
8. The binary's transfer harness at the scale CI's suite runs it (`mantle bench transfer --small
   1000 --large 20M --count 4 --seed 5`, `crates/mantle/tests/transfer.rs`): cells of three, a
   thousand small objects half through each door and four 20 MiB objects natively in runs and in
   parts, by the AWS CLI from its standard input and by boto3 in parts, every byte checked against
   its generator through both doors and another node while each founder is killed and started
   again. It exits non-zero when any check is not exact.

Every object read back is compared byte for byte with what was put. Every wait ends on what a
process says or on its end, never on a guess of time; GitHub's 6 hours a job bounds the whole
(research/79 [12]). The AWS CLI and boto3 are the ones `scripts/stock-clients.sh` installs for the
gates, boto3 pinned with every hash. A failed check prints each node's last output.

## 3. How it runs

`.github/workflows/release.yml`:

- A pushed tag `v*` runs everything. The `version` job reads the workspace's version and refuses a
  tag that names another. One job a target builds, packs and checks its archive, then uploads it as
  the run's artifact (research/79 [6]). Once all five pass, `publish` downloads them, checks each
  archive's SHA-256 again, requires a run of the gates (`ci.yml`) that passed for the tagged commit,
  and creates the tag's release with every archive and checksum, which `gh release create` keeps a
  draft until every asset is up [7]; a version with a pre-release part (`0.2.0-rc.1`) is marked a
  pre-release. Its notes are GitHub's generated ones after a line naming this document. A publish
  that fails part way leaves at most a draft, deleted before the job is run again.
- A push that changes the release's own files (the workflow, `scripts/release.py`, the stock
  clients' script and pins), on any branch, runs everything but `publish`: a branch push is a dry
  run [2]. So does a manual run (`workflow_dispatch`, once the workflow is on the default branch
  [3]), even on a tag.
- Only `publish` may write, and only the repository's contents, beside reading the gates' runs;
  every other job reads alone, and `publish` runs no code of the repository's [5]. Each action is
  pinned to its commit, its version beside it [5], and no job keeps the checkout's credentials. A
  newer push to a branch cancels its older run; a tag's run is never cancelled.

The gates run the suites on every target for every push; the release proves the binary it
publishes. vorpal's release runs its tests itself [1]; mantle's gates take hours on six targets,
so `publish` asks for their result instead. A tag on a commit whose gates have not passed builds and
checks its archives but publishes them only once the gates pass and `publish` is run again.
