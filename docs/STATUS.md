# Status

Updated 2026-09-28. A component is complete when its tests pass on all six CI targets
(Linux, macOS and Windows on x86_64 and arm64) and the evidence listed for it has been
recorded.

## Implemented

`mantle disk probe` reports what the operating system knows about the device that holds a
directory and, with `--measure`, benchmarks the device. It is built on:

- **Device identification** on Linux (sysfs, including device-mapper and md members),
  macOS (the I/O Registry, including APFS containers and disk images) and Windows (the
  volume management functions and storage IOCTLs). Tested on physical disks, a mounted
  disk image and a virtual disk.
- **The file layer**: direct I/O where the file system supports it, aligned positional
  reads and writes, and each platform's full flush. Tested on APFS, ext4, tmpfs and NTFS.
- **Calibration**: reads, writes and flush latency measured through the file layer. Each
  measurement runs three times and the median is reported. The scratch file is removed
  whether the run succeeds or fails.
- **Checksums**: CRC-32C for stored and transmitted data, and CRC-64/NVME for S3
  checksums, both verified against published test vectors.
- **A simulated device** for crash testing: writes not yet flushed are lost, kept or torn
  at sector granularity when it crashes, a failed flush leaves their durability unknown,
  and reads and writes can be made to fail or return corrupted bytes.

## In progress

**Chunk store** ([design](design/chunk-store.md)). Implemented: the volume layout with
two superblocks, self-describing data records with a CRC-32C per checksum block, the
index log with a second copy of every record's identity, the group-commit writer that
flushes once per batch and fences the volume on a failed write or flush, reads that
verify every byte they return, checkpoints and log wrap-around, reuse of segments that
empty out, cleaning of partly dead segments chosen by cost-benefit (relocations batched
into the cleaner's own stream, a reserve segment kept for it, and passes that stop when
they cannot gain space), and crash recovery (replay, verification of the last batch,
roll-forward, and a checkpoint that makes recovery's corrections durable). Tested with
randomized workloads, cleaning included, cut by power loss at every point on the
simulated device (20,000 runs per soak: no acknowledged write lost, no unverified byte
returned) and with bit flips, read errors, damaged superblocks, damaged log frames and
failed writes and flushes. On real file systems (APFS, ext4 and tmpfs), a writing process
killed with `kill -9` at random points loses no write it acknowledged.

The scrubber verifies every live fragment in the background, paced so a pass takes the
configured period (7 days by default) and continuous once damage is found; damaged
chunks are listed for repair.

Remaining before it is done:

- Writing new file regions once before use where calibration measures a first-write penalty.
- An I/O path that keeps the measured number of reads in flight.
- `mantle bench chunk` measures puts and reads next to the same reads through the file
  layer ([measurements](measurements/2026-09-28-chunk-store-benchmark.md)). Remaining: each
  point repeated until its result is statistically stable, and large puts brought closer
  to the device's durable bandwidth.

**Erasure coding** (`mantle-ec`). Systematic Reed–Solomon over GF(2^16) with contiguous
data chunks, from `reed-solomon-simd` behind an unwind boundary. Every combination of lost
chunks within the tolerance of RS(2,1) through RS(9,6) is rebuilt byte for byte in the
tests, and `mantle bench ec` measures encoding and rebuilding per code
([measurements](measurements/2026-09-28-erasure-coding.md)).

Remaining before it is done:

- Choosing the code for a block from the failure domains available and a durability
  target, computed from failure and repair rates under correlated failures
  (docs/research/04 §A5) rather than from a table.

## Planned, in order

1. **Metadata service.** Ranges of object names, file layouts and chunk locations, each
   replicated with focal's Raft implementation and its fast-track commit, over QUIC for
   bulk transfers and a UDP transport for consensus messages, and lazy deletion that keeps
   an unreferenced block's chunks for a grace period. Done when a linearizability
   checker accepts histories recorded under network partitions, process crashes and disk
   faults, both in deterministic simulation and with real processes.
2. **S3 gateway.** Request signing (including presigned URLs and chunked uploads with
   trailing checksums), buckets, PUT, GET with byte ranges, HEAD, DELETE and batch
   delete, copy, ListObjects and ListObjectsV2, multipart uploads including resuming an
   interrupted upload, versioning, conditional requests and checksums. Done when
   end-to-end suites using the AWS CLI, boto3 and the AWS SDK for Rust pass against a
   running mantle, and ceph's s3-tests pass for every supported feature.
3. **Multi-machine operation.** Placement across racks and zones, repair ordered by
   remaining redundancy, rebalancing, and retiring disks that start to fail. Done when
   tests that fail disks, machines and racks during writes show that every acknowledged
   write can still be read back.
4. **Cells.** A replicated map of which cell owns each key range, routing from cached
   copies of it with redirects after a range moves, moving a range between cells while it
   is read and written, and adding and retiring cells. Done when ranges move between cells
   under a mixed workload with no lost write and no stale read, and failing or upgrading
   one cell leaves the requests of every other cell unaffected.
