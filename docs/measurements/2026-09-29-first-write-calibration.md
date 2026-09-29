# First-write penalty, as calibration measures it

**Question.** Does calibration tell a file system that charges a durable write into
never-written space from one that does not? The chunk store writes a volume once at format
where it does (docs/design/chunk-store.md §2), so the decision rests on this measurement.

**Method.** `mantle disk probe --measure` at commit-time `Plan::standard` (release build).
Before anything else touches the preallocated scratch file, calibration runs rounds of 64
sequential 4 KiB durable writes, each one written then fully flushed, every round into space
no round has written. Then it runs the same writes over the space they wrote. Each point
runs 3 to 6 rounds, until its throughput's 95% confidence interval is within ±5% of the mean.
A penalty is declared when the first writes' interval lies wholly below the overwrites'
(`Calibration::first_write_penalty`).

| Platform | First write, as a multiple of an overwrite | Decision |
|---|---|---|
| macOS 26.4.1, APFS, Apple SSD AP8192Z (F_NOCACHE, F_FULLFSYNC) | 0.99x | not pre-written |
| Linux 6.12.76 (Docker Desktop VM), ext4 on virtio-blk (O_DIRECT, fdatasync) | 6.3x | written once at format |

**Reading.** The decision follows the file system. It agrees with the extent-state
measurement of the day before, 5.7x on ext4 and none on APFS
([2026-09-28](2026-09-28-flush-cost-by-extent-state.md)), where ext4 commits each extent's
conversion through its journal at the flush. The virtual disk is not real hardware, so the
ratio speaks for ext4's journaling rather than for a device.
