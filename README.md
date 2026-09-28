<p align="center">
  <a href="docs/assets/brand/mantle-strata-preview.png">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/assets/brand/mantle-strata-dark.svg">
      <source media="(prefers-color-scheme: light)" srcset="docs/assets/brand/mantle-strata-light.svg">
      <img src="docs/assets/brand/mantle-strata-light.svg" alt="Mantle logo: a globe cut open along a seam, showing its surface on one side and strata rings around a solid core on the other" width="90" height="90">
    </picture>
  </a>
</p>

<h1 align="center">mantle</h1>
<p align="center"><em>File-system and object store for exabyte scale.</em></p>

Mantle is a distributed object store and file system for the data that AI agents
produce: checkpoints, transcripts, datasets, embeddings and build artifacts. It implements
the S3 API, so the AWS CLI, boto3, the AWS SDKs, rclone and other S3 clients work with it
unchanged. The same binary runs as a single process on a laptop or as a cluster of many
machines.

Mantle acknowledges a write only after the data is durable on every node that stores it,
and a read that starts after that returns the new data. Agents can coordinate through
conditional writes: `If-None-Match: *` creates an object only if its key is unused, and
`If-Match` replaces an object only if it has not changed since it was read. Data is
checksummed with CRC-32C when it is received and verified again whenever it is read; a
chunk that fails verification is served from another copy and repaired.

The design follows Meta's Tectonic file system. Storage nodes own their disks and store
chunks, a metadata service replicated with Raft records where every chunk is, and clients
write chunks directly to the storage nodes. [How it works](#how-it-works) follows a write
and a read through these parts.

> [!IMPORTANT]
> Mantle has no release yet. The storage-device layer is implemented and tested on Linux,
> macOS and Windows, on x86_64 and arm64: device identification, direct I/O with the
> correct flush call for each platform, device measurement, and CRC-32C and CRC-64/NVME
> checksums. The chunk store is in progress; erasure coding, the metadata service and the
> S3 gateway follow. The gateway will implement the S3 API that standard clients use:
> multipart uploads, including resuming an interrupted upload, PUT, GET with byte ranges,
> HEAD, DELETE and batch delete, copy, ListObjects and ListObjectsV2, versioning,
> conditional requests, checksums and presigned URLs. [docs/STATUS.md](docs/STATUS.md)
> tracks each component.

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

So far the only command is the disk probe. Before mantle stores data on a device, it
checks what the operating system reports about the device and then measures it, because
the reported values are often wrong. In a Linux VM under Docker Desktop on macOS, for
example, the virtual disk is reported as rotational although it is backed by flash, and
the file system reports 1.88 TB free on a host with 24 GB free. `mantle disk probe` runs
the same checks on the device that holds a directory:

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

The first three lines come from the operating system and print immediately. The rest
comes from `--measure`, which benchmarks the device through the same direct-I/O path that
mantle uses for data. It writes a scratch file of at most 256 MB, or a tenth of the free
space if that is smaller, and deletes it when it finishes, including after an error.
Mantle uses these results to choose I/O sizes, queue depths and how many writes to group
into each flush. On this Mac a durable write takes about 4.5 ms, because macOS flushes
the drive's write cache (`F_FULLFSYNC`) before the write completes; grouping concurrent
writes into one flush keeps that cost from limiting how many writes complete per second.
`--verbose` lists every property the operating system did not report.

> [!NOTE]
> Captured from a release build on an Apple M5 Max running macOS 26, with the home
> directory shortened to `~`.

## How it works

This section describes the design. [docs/STATUS.md](docs/STATUS.md) lists which parts
are implemented.

**Writing an object.** The S3 gateway authenticates the request and passes the data to
mantle's client library. An object smaller than a block is appended to a shared block
that is replicated on three storage nodes and re-encoded with erasure coding once the
block is full. A larger object is split into blocks, and each block is erasure-coded into
chunks that go to disks in different racks. The metadata service chooses where each chunk
goes. The object's name is committed only after every chunk is durable, so a failed
upload never leaves a partial object visible.

**Storing a chunk.** Each disk holds one volume, a large file or a raw block device
opened with direct I/O and written sequentially in fixed-size segments. One writer loop
per disk takes the requests that arrived during the previous flush, writes them as a
batch, and flushes the device once: `fdatasync` on Linux, `fcntl(F_FULLFSYNC)` on macOS,
where `fsync` does not flush the drive's cache, and `FlushFileBuffers` on Windows. The
requests are acknowledged after the flush returns. If a flush fails, mantle stops writing
to that disk and recovers from what is on it. It does not retry the flush, because the
kernel may already have discarded the data (Rebello et al., USENIX ATC 2020).

**Reading and checking data.** Each record on disk stores the identity of its chunk and
a CRC-32C for every 64 KiB of data, and a separate index log holds a second copy of each
record's identity and location. A read verifies both, which detects bit rot, torn writes,
misdirected writes, and writes the drive acknowledged but did not persist. A chunk that
fails verification is read from another copy and rebuilt. Every disk is also scrubbed in
the background at least every two weeks.

**Finding objects.** Object names are kept in sorted order and divided into ranges, each
replicated with Raft. A range splits when it grows large or receives more requests, which
is also how S3 scales request rates per key prefix. Chunk locations are stored separately
from names, so repairing a disk changes locations without rewriting names.

**Recovering from failures.** When a disk or machine fails, the blocks with the fewest
surviving chunks are rebuilt first, from their remaining chunks, onto other disks. Space
held by deleted data is reclaimed by copying the live records out of mostly empty
segments and reusing the segments.

The design documents are in [docs/design/](docs/design/), starting with the
[chunk store](docs/design/chunk-store.md). The papers and platform documentation they
cite are summarized in [docs/research/](docs/research/).

## From a laptop to a fleet

The S3 API is the same at every scale. You configure how many failures a write must
tolerate, and mantle places data to meet that requirement, or reports that the available
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
