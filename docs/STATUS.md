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

## Planned, in order

1. **Chunk store.** One log-structured volume per disk with group commit, self-describing
   records, a second copy of each record's identity in an index log, crash recovery,
   cleaning and scrubbing ([design](design/chunk-store.md)). Done when randomized
   workloads crashed at random points on the simulated device never lose an acknowledged
   write or return data that fails verification, and the same workloads survive
   `kill -9` on real disks.
2. **Erasure coding.** Reed–Solomon layouts chosen from the number of available failure
   domains. Done when encoding and decoding are benchmarked for each layout, and every
   combination of lost chunks within a layout's tolerance is rebuilt correctly.
3. **Metadata service.** Ranges of object names, file layouts and chunk locations, each
   replicated with focal's Raft implementation and its fast-track commit, over QUIC for
   bulk transfers and a UDP transport for consensus messages. Done when a linearizability
   checker accepts histories recorded under network partitions, process crashes and disk
   faults, both in deterministic simulation and with real processes.
4. **S3 gateway.** Request signing (including presigned URLs and chunked uploads with
   trailing checksums), buckets, PUT, GET with byte ranges, HEAD, DELETE and batch
   delete, copy, ListObjects and ListObjectsV2, multipart uploads including resuming an
   interrupted upload, versioning, conditional requests and checksums. Done when
   end-to-end suites using the AWS CLI, boto3 and the AWS SDK for Rust pass against a
   running mantle, and ceph's s3-tests pass for every supported feature.
5. **Multi-machine operation.** Placement across racks and zones, repair ordered by
   remaining redundancy, rebalancing, and retiring disks that start to fail. Done when
   tests that fail disks, machines and racks during writes show that every acknowledged
   write can still be read back.
