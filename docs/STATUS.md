# Where mantle stands

Updated 2026-09-28. Each piece closes when its tests pass on all six platform targets in
CI and the evidence named below is recorded.

## What you can run today

`mantle disk probe` tells you what the operating system says about the storage under a
directory and, with `--measure`, what that storage actually does. Under it sit the
foundations every later layer uses:

- **Device identification** on Linux (sysfs, including device-mapper and md members),
  macOS (the I/O Registry, including APFS containers and disk images) and Windows (the
  volume functions and storage IOCTLs). Tested on real devices, a mounted disk image and a
  virtual disk.
- **A file layer that makes writes safe the way each platform requires**: direct I/O where
  the file system accepts it, aligned positional transfers, and the platform's full flush.
  Tested on APFS, ext4 and tmpfs.
- **Calibration** that measures reads, writes and commits through that layer, repeats each
  point and reports the median, and never leaves its scratch file behind.
- **Checksums**: CRC-32C for everything mantle stores and sends, CRC-64/NVME for the S3
  checksum clients now send by default, checked against the published test vectors.

## What is being built, in order

1. **Chunk store.** One log-structured volume per disk: group commit, self-describing
   records, a second copy of every record's identity, crash recovery, cleaning and
   scrubbing ([design](design/chunk-store.md)). Closes when a simulated device that loses,
   tears and corrupts unflushed writes at every point of a randomized workload never makes
   it lose an answered write or return a byte that fails its check, and when the same
   workload survives `kill -9` on real disks.
2. **Erasure coding.** Reed–Solomon profiles chosen from the failure domains you have.
   Closes when encode and decode are benchmarked at each profile's shape and every
   combination of lost pieces within a profile's tolerance rebuilds.
3. **Metadata service.** Ranges of names, file layouts and block locations, each replicated
   on focal's Raft core with its fast track, over QUIC for transfers and a UDP plane for
   consensus. Closes when a linearizability checker passes histories recorded under
   partitions, crashes and disk faults in deterministic simulation and on real processes.
4. **S3 gateway.** The API your tools use: signatures (including presigned URLs and
   chunked uploads with trailing checksums), buckets, put, get with ranges, head, delete,
   batch delete, copy, listings, multipart uploads with resumption, versioning,
   conditional requests and checksums. Closes when the AWS CLI, boto3 and the AWS SDK for
   Rust pass end-to-end suites against a running mantle, and ceph's s3-tests pass for
   every feature mantle claims.
5. **A fleet.** Placement across racks and zones, repair ordered by remaining redundancy,
   rebalancing, and a disk's retirement after it starts failing. Closes when multi-machine
   runs lose disks, machines and racks mid-write and every answered write reads back.
