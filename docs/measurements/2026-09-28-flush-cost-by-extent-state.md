# Durable-append cost by extent state

**Question.** Does a durable write into space the file system has allocated but not yet
written cost more than overwriting written space? The literature leaves it open
(docs/research/03 §15.7 P3: "The exact behaviour of `fallocate` unwritten extents under
`O_DIRECT` is NOT ADDRESSED ... UNVERIFIED").

**Method.** `crates/disk/examples/extent_flush.rs`: 512 sequential 64 KiB direct writes,
each followed by `sync_data`, depth 1, over a 32 MiB span: (1) into a freshly
preallocated file, (2) over the same region again, (3) extending a file with no
preallocation.

| Platform | Case | p50 | p99 | Throughput |
|---|---|---|---|---|
| macOS 26, APFS, Apple SSD AP8192Z (F_NOCACHE, F_FULLFSYNC) | preallocated, first write | 4.72 ms | 6.82 ms | 15.6 MiB/s |
| | overwrite | 4.72 ms | 6.82 ms | 15.5 MiB/s |
| | extending | 4.72 ms | 6.29 ms | 15.6 MiB/s |
| Linux 6.12 (Docker Desktop VM), ext4 on virtio-blk (O_DIRECT, fdatasync) | preallocated, first write | 2.62 ms | 50.3 ms | 9.0 MiB/s |
| | overwrite | 0.46 ms | 14.7 ms | 42.3 MiB/s |
| | extending | 1.31 ms | 21.0 ms | 23.6 MiB/s |

**Reading.** On ext4, writing into unwritten extents makes every `fdatasync` commit the
extent conversion through the journal: 5.7x the median of an overwrite, 4.7x less
throughput; growing the file costs 2.8x. On APFS the full device flush dominates and
extent state does not matter. The Linux VM's virtual disk is not real hardware, so the
absolute numbers are not a device fact; the ratio reflects ext4's journalling, which the
VM does not change.

**Consequence.** Steady-state appends must overwrite blocks that are already written
where the file system journals extent state. The chunk store writes each new region of a
volume file once before use when calibration measures the first-write penalty, reuses
segments circularly, and skips the pre-write where it measures none (APFS here). Raw
block devices have no extent state.
