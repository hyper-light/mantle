<p align="center">
  <a href="docs/assets/brand/mantle-mark-preview.png">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/assets/brand/mantle-mark-dark.svg">
      <source media="(prefers-color-scheme: light)" srcset="docs/assets/brand/mantle-mark-light.svg">
      <img src="docs/assets/brand/mantle-mark-light.svg" alt="Mantle logo: a globe cut open along a seam, showing its surface on one side and strata rings around a solid core on the other" width="90" height="90">
    </picture>
  </a>
</p>

<h1 align="center">mantle</h1>
<p align="center"><em>File-system and object store for exabyte scale.</em></p>

Mantle is a distributed object store and file system with an S3-compatible API, built for
the data AI agents produce: checkpoints, transcripts, datasets, embeddings and build
artifacts. Existing S3 clients work with it unchanged, including the AWS CLI, boto3, the
AWS SDKs and rclone.

The design follows Meta's Tectonic file system. Storage nodes own their local disks and
store chunks. A metadata service, replicated with Raft, records which chunks make up each
object and where each chunk is stored. Clients split large objects into erasure-coded
chunks and write them directly to the storage nodes. The same binary runs as a single
process on a laptop or as a cluster of many machines.

A write completes only after its data is durable on every node it was sent to, and a read
that starts after the write completes returns the new data. Conditional writes
(`If-None-Match: *` and `If-Match`) let an agent claim a key, or update an object only if
it has not changed since the agent read it. Data is checksummed with CRC-32C when it
arrives and verified on every read; a chunk that fails verification is read from another
copy and repaired.

Operating systems often misreport storage devices. In a Linux VM under Docker Desktop on
macOS, for example, the virtual disk is reported as rotational although it is backed by
flash, and the file system reports 1.88 TB free on a host with 24 GB free. Mantle checks
what the OS reports against measurements of the device, and uses the measurements to set
I/O sizes, queue depths and commit batching. `mantle disk probe` shows both:

```console
$ mantle disk probe ~/mantle-data --measure
~/mantle-data
  disk          APPLE SSD AP8192Z, internal flash
  file system   APFS, 46.0 GB free
  write cache   not reported; mantle flushes the drive cache on every commit
measuring with a scratch file of up to 268 MB (removed afterwards)
measured in 13 s:
  reads         64 concurrent 4 KiB reads give the highest throughput; mantle uses up to 64
  throughput    15.0 GB/s read, 22.2 GB/s write (1 MiB transfers)
  commits       4.46 ms per durable write; concurrent writes share a flush
```

On this machine a durable write takes about 4.5 ms, because macOS flushes the drive's
write cache (`F_FULLFSYNC`) before the write completes. Mantle writes concurrent requests
in a batch that shares one flush, so the flush time adds latency to each write but does
not limit the number of writes per second.

> [!NOTE]
> Captured from a release build on an Apple M5 Max running macOS 26, with the home
> directory shortened to `~`.

> [!IMPORTANT]
> Mantle has no release yet. The storage-device layer is implemented and tested on Linux,
> macOS and Windows, on x86_64 and arm64: device identification, direct I/O with the
> correct flush call for each platform, device measurement, and CRC-32C and CRC-64/NVME
> checksums. `mantle disk probe` is built on it. The chunk store is in progress; erasure
> coding, the metadata service and the S3 gateway follow. The gateway will implement the
> S3 API that standard clients use: multipart uploads, including resuming an interrupted
> upload, PUT, GET with byte ranges, HEAD, DELETE and batch delete, copy, ListObjects and
> ListObjectsV2, versioning, conditional requests, checksums and presigned URLs.
> [docs/STATUS.md](docs/STATUS.md) tracks each component.

## Install

There are no release binaries yet. To build from source you need Rust 1.98, which
`rust-toolchain.toml` pins, so `rustup` installs it on first use:

```sh
git clone https://github.com/hyper-light/mantle && cd mantle
cargo build --release -p mantle --locked
sudo mv target/release/mantle /usr/local/bin/   # or add target/release to your PATH
mantle --help
```

## Quickstart

```sh
mantle disk probe ~/mantle-data
```

This prints what the operating system reports about the device that holds the directory:
the drive model and type, the file system and its free space, and whether the drive has a
volatile write cache. It returns immediately.

With `--measure`, mantle also benchmarks the device through the same direct-I/O path it
uses for data. It writes a scratch file of at most 256 MB, or a tenth of the free space if
that is smaller, and deletes the file when it finishes, including after an error.
`--verbose` lists every property the OS did not report.

## How it works

This section describes the design. [docs/STATUS.md](docs/STATUS.md) lists which parts
are implemented.

**Durability.** A write is acknowledged only after the device confirms it is durable:
`fdatasync` on Linux, `fcntl(F_FULLFSYNC)` on macOS, where `fsync` does not flush the
drive's cache, and `FlushFileBuffers` on Windows. If a flush fails, mantle stops writing
to that disk and recovers from its on-disk state. It does not retry the flush, because
after a failed flush the kernel may have discarded the data without writing it (Rebello
et al., USENIX ATC 2020).

**Write path.** Each disk holds one volume: a large file or a raw block device, opened
with direct I/O and written sequentially in fixed-size segments. One loop per disk writes
and flushes each batch of requests; requests that arrive during a flush go into the next
batch.

**Integrity.** Each record on disk stores the identity of its chunk and a CRC-32C for
every 64 KiB of data. A separate index log holds a second copy of each record's identity
and location. Together they detect bit rot, torn writes, misdirected writes, and writes
the drive acknowledged but did not persist. A read that fails verification is retried
against another copy, and the damaged copy is rebuilt.

**Metadata.** Object names are kept in sorted order and divided into ranges, each
replicated with Raft. A range splits when it grows or when its request rate increases,
which is also how S3 scales request rates per key prefix. Chunk locations are stored
separately from object names, so repairing a disk updates locations without rewriting
names.

**Redundancy.** Large objects are erasure-coded, with their chunks placed in different
racks. Small objects are written as three replicas and re-encoded once the block holding
them is full. When a disk fails, the blocks with the fewest surviving chunks are repaired
first.

The design documents are in [docs/design/](docs/design/), starting with the
[chunk store](docs/design/chunk-store.md). The papers and platform documentation they
cite are summarized in [docs/research/](docs/research/).

## From a laptop to a fleet

The S3 API is the same at every scale. You configure how many failures a write must
tolerate; mantle places data to meet that requirement, and reports it when the available
hardware cannot.

| Deployment | A write completes when | Tolerates |
|---|---|---|
| One disk | the data is on that disk | no disk failure; mantle reports this at startup |
| One machine with several disks | each chunk is on a different disk | disk failures up to the configured redundancy |
| Several machines | the chunks are on disks in different racks | machine and rack failures up to the configured redundancy |
| Several zones | the chunks are spread across zones | the loss of a zone, at the cost of a cross-zone round trip per write |

Metadata ranges are replicated with the Raft implementation from
[focal](https://github.com/hyper-light/focal), including its fast-track commit (Fast
Raft; Castiglia, Goldberg and Patterson, ICDCS 2020), which saves a round trip when a
write is proposed by a replica that is not the leader.

## Documentation

| Doc | Contents |
|---|---|
| [Status](docs/STATUS.md) | Implemented components, and the tests that will complete the rest |
| [Chunk store](docs/design/chunk-store.md) | On-disk layout, the write path, crash recovery, space reclamation |
| [Research](docs/research/) | Summaries of the papers and platform documentation the design cites |
| [Measurements](docs/measurements/) | Measurements taken on real hardware and the design changes they led to |
| [Bugs](docs/bugs/) | Defects found, their causes, and the tests that cover them |

## Contributing / development

```sh
bash scripts/gates.sh            # all checks, in order; stops at the first failure
bash scripts/check-targets.sh    # lint all six platform targets from one machine
bash scripts/linux-test.sh       # run the tests on Linux in a container
```

[CLAUDE.md](CLAUDE.md) lists the rules for changes. Production code does not panic, every
queue and cache is bounded, design decisions cite their sources, and performance claims
reference the measurement that supports them.

## Acknowledgements

The architecture follows Tectonic (Pan et al., USENIX FAST 2021). The storage layer draws
on Haystack and f4 for storing many objects in a few large files, on the log-structured
file system of Rosenblum and Ousterhout for reclaiming space in them, and on Ceph's
BlueStore for managing disks directly instead of through a local file system. The
consensus core comes from [focal](https://github.com/hyper-light/focal), and the
engineering rules from focal and [slates](https://github.com/hyper-light/slates).

## License

MIT — © 2026 Hyperlight. See [LICENSE](LICENSE).
