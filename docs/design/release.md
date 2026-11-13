# Release

Status: built 2026-11-05. What a release of mantle publishes, for which targets, and how each archive
is proven before it is published. The facts it rests on are research/85's.

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
cross-builds on x86_64 (research/85 [1]); GitHub's `windows-11-arm` runs it natively [4], so each
mantle archive is built and checked on the kind of machine it is for.

**Linux.** A binary takes each glibc symbol at the version its build host's glibc has, so it runs
where glibc is at least the newest version among those it takes. The Linux archives are built on
Ubuntu 22.04, the oldest Ubuntu GitHub runs, whose glibc is 2.35 (research/85 [4], [8]). Measured,
the newest they take is `GLIBC_2.34`, for `__libc_start_main`, `dlsym` and the threads' calls,
which glibc 2.34 moved into libc, so they run where glibc is 2.34 or newer; beside libc they need
libm, libgcc_s and the dynamic loader alone (`objdump -T` and `objdump -p` of dry run 42173080345's
binaries, both architectures). They carry the release profile's line tables, 53 MB of x86_64's 76
MB (`.debug_*` sections), which macOS and Windows keep outside the binary.

**macOS.** The linker signs an arm64 binary ad hoc, which is all macOS needs to run it; the binary is
not notarized, so one downloaded by a browser carries the quarantine attribute and Gatekeeper
refuses it until the attribute is removed (`xattr -d com.apple.quarantine mantle`). It runs on macOS
11.0 and later (`minos 11.0` in its `LC_BUILD_VERSION`, the least rustc supports on arm64,
research/85 [11]) and links only the system's libraries: libSystem, libiconv, CoreFoundation and
IOKit (`otool -L` of dry run 42173080345's binary).

**Windows.** The C runtime is linked in (`-C target-feature=+crt-static`, research/85 [9]), the
release build alone setting it. Linked dynamically, as rustc links it unless told otherwise [9], the
binary needs Visual C++'s `vcruntime140.dll`, which an application's users "must" be given, since
"most users probably don't have Visual Studio installed" [10]; the Universal CRT is part of Windows
10 and later [10]. Linked in, the binary needs no Visual C++ Redistributable on its host: measured,
each imports Windows' own DLLs alone, `KERNEL32`, `advapi32`, `ntdll`, `ws2_32`,
`bcryptprimitives` and the core's `realtime` and `synch` API sets (`objdump -p` of dry run
42173080345's binaries, both architectures). AWS-LC's build sees the choice (`CARGO_ENCODED_RUSTFLAGS`
in its build log). The Windows archives' `LICENSE` and `README.md` have the CRLF line ends of the
runner's checkout.

## 2. How each archive is proven

Before its job uploads it, each target's archive is checked as a user takes it, on its own runner
(`scripts/release.py check`):

1. The archive's SHA-256 is the one beside it, and it holds exactly the directory, the binary at
   mode 755, `LICENSE` and `README.md`.
2. The unpacked binary says `mantle VERSION`.
3. It makes a cell of three with `mantle init`, each founder's QUIC, plane and S3 ports stated
   (each one the OS gave free), its volume of 512 MiB in a directory of its own (sized from the
   segments the check writes, `scripts/release.py`) and its failure domain its own:
   all on loopback but the first founder's S3 endpoint, at every address and named `localhost`, which
   init serves under TLS, certifies for that name and writes into both profiles as
   `https://localhost:PORT` with its authority as `ca_bundle`. Every S3 client of the check, the S3
   door and the AWS CLI alike, dials it by a name of two addresses, `::1` and `127.0.0.1`, which the
   listener at `0.0.0.0` answers on the second; on Windows the refused first costs about 2 s of SYN
   retries before the second answers, a cost of the platform, not a wait the check makes. It
   serves the cell as three `mantle serve` processes from what init wrote, each waited for until it
   says it bound the stated addresses and that the cell's root range enlisted it, and then ready to
   `mantle status`.
4. Through each of the client's doors in turn (docs/design/doors.md), `--door native`, the native
   protocol over QUIC, and `--door s3`, S3's HTTP API SigV4-signed at the endpoint of the S3 profile
   `mantle init client` gave the client, as the client of a directory `mantle init client` made,
   every object command into a bucket of the door's own: `mb`; `cp` from a file of 3 MiB, below the
   AWS CLI's 8 MiB multipart threshold, and of 16 MiB, past it, each back to a file; from standard
   input of 9 MiB and of 100 KB, each back to standard output; `head`; `ls` of a prefix's keys with
   their sizes and of the bucket's prefixes; a multipart upload's every step (`create`, `put` of a 5
   MiB part from a file and the last from standard input, `parts`, `uploads`, `complete`), read
   back, and another upload aborted. A copy from standard input of a stated length (`--size`), fed a
   megabyte at a time and fed no more once it says a run (the native door) or a part (the S3 door)
   committed, is killed (SIGKILL; `TerminateProcess` on Windows): the cell lists its open upload
   holding exactly the runs or parts and bytes the copy said durable, and the copy run again says it
   resumes there, commits each later one and none before, and the object reads back whole, its ETag
   its bytes' MD5 through the native door and its parts' combined MD5 through the S3 door.
5. Across the doors: each door reads back, to standard output and to a file, every object the other
   put; both list the buckets, a bucket's keys and its uploads line for line alike, and head the
   same objects alike; `ls` through the door `MANTLE_DOOR` names, as the client `MANTLE_CLIENT`
   names, says what `--door s3 --client` says.
   The S3 door is asked again over plain HTTP too: a copy of the client whose S3 profile names the
   second founder's loopback endpoint, `http://127.0.0.1:PORT`, runs `mb`, `cp` from a file and from
   standard input, up and back, `ls`, `head`, `rm` and `rb`, each object read back through the native
   door.
6. The stock client, the AWS CLI v2 with the profile `mantle init` wrote (its environment's own
   `AWS_*` variables removed, as they outrank a profile): `s3 mb`, `s3 cp` of an object below the
   CLI's 8 MiB multipart threshold and one past it, up and back, `s3 ls` with their sizes and
   `s3api head-object`; each of the client's doors reads back what it put, and it reads back what
   each put. Then each door removes its own objects and bucket: `head` of a removed object says
   `NoSuchKey` through the native door and its bodiless `404` through the S3 door, and `rb` of a
   bucket still holding an object `BucketNotEmpty` through both.
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
(research/85 [12]). The AWS CLI and boto3 are the ones `scripts/stock-clients.sh` installs for the
gates, boto3 pinned with every hash. A failed check prints each node's last output.

## 3. How it runs

`.github/workflows/release.yml`:

- A pushed tag `v*` runs everything. One job a target reads the workspace's version itself
  (`scripts/release.py version`), refusing a tag that names another, then builds, packs and checks
  its archive and uploads it as the run's artifact (research/85 [6]). No target's job waits on
  another job to begin: a job that needs another is queued only once that one completes (research/85
  [17]), so under a full queue a job of its own for the version would have the targets wait in line
  twice, once behind what was queued before the push and again behind what was queued while it
  waited. Once all five pass, `publish` downloads them, checks each
  archive's SHA-256 again, requires a run of the gates (`ci.yml`) that passed for the tagged commit,
  and creates the tag's release with every archive and checksum, which `gh release create` keeps a
  draft until every asset is up [7]; a version with a pre-release part (`0.2.0-rc.1`) is marked a
  pre-release; `publish` takes the version from the tag, which every target's job held to the
  workspace's. Its notes are GitHub's generated ones after a line naming this document. A publish
  that fails part way leaves at most a draft, deleted before the job is run again.
- Every push to a branch and every pull request runs everything but `publish`, the same check a
  tag's run makes, so what a tag publishes is what its pull request proved; a pull request's run
  checks the merge of its branch into its base. A change under `docs/` alone is skipped (research/85
  [15]): the binary is built from everything else, and the archive's `README.md` and `LICENSE` sit
  outside `docs/`. A manual run (`workflow_dispatch`, once the workflow is on the default branch
  [3]) does the same, even on a tag. Its cost is runners held at once, not money (the repository is
  public, research/85 [14]): five jobs a run, one of them macOS, of the 5 macOS jobs a Free plan runs
  at once [12], beside the gates' two for each push and pull request.
- Only `publish` may write, and only the repository's contents, beside reading the gates' runs;
  every other job reads alone, and `publish` runs no code of the repository's [5]. A pull request
  from a fork gets a token that reads alone [15], and `publish` never runs for one. Each action is
  pinned to its commit, its version beside it [5], and no job keeps the checkout's credentials. A
  newer push to a branch or a pull request cancels its older run; a tag's run is never cancelled.

The gates run the suites on every target for every push; the release proves the binary it
publishes. vorpal's release runs its tests itself [1]; mantle's gates take hours on six targets,
so `publish` asks for their result instead. A tag on a commit whose gates have not passed builds and
checks its archives but publishes them only once the gates pass and `publish` is run again.
