# The Raft log against the device it runs on

**Question.** How many replicas' appends does one flush of the shared Raft log commit, and
how close does an append's latency come to one durable write on the device?

**Method.** `mantle bench log <dir>` (crates/mantle/src/bench_log.rs), release build, macOS 26
on an Apple M5 Max, APFS on the internal Apple SSD AP8192Z. The command calibrates the device,
then for each entry size and number of replicas creates a log in a scratch file with 16 MiB
segments. Closed-loop replicas each append one entry at a time to their own group, wait until
it is durable, and compact behind themselves every 64 entries, for a step of one or three
seconds. The log counts the frames it flushed and the updates they carried.

**Device, through the file layer.** A 4 KiB write followed by `F_FULLFSYNC`: 4.46 ms. 32 MiB
written and then flushed: 3.47 GB/s.

## Findings

**1. One flush commits every replica's append.** With the writer's wait for returning
submitters (finding 2), each flush carries every replica's append: 2.0 appends per flush
with 2 replicas, 4.0 with 4, 16.0 with 16, 63.7 with 64 and 249–255 with 256. The device
completes about 220 durable writes a second, and the log commits 30–47K appends a second at
256 replicas, for entries of 128 B to 1 KiB. With 16 KiB entries the rate is 29.9K a second,
489 MB/s of entries: a flush of 4 MiB still costs about one durable write here.

**2. Few replicas alternated between batches until the writer waited for them.** The first
build formed each batch at once from whatever was queued, and the replicas a batch had just
answered missed the next one. The chunk store saw the same with few writers
(2026-09-28-chunk-store-benchmark.md, finding 5). Measured over 3 s points, with 1 KiB entries:

| Replicas | Before: appends/s | per flush | p50 | After: appends/s | per flush | p50 |
|---|---|---|---|---|---|---|
| 2 | 154 | 1.0 | 11.8 ms | 426 | 2.0 | 4.33 ms |
| 4 | 187 | 2.1 | 19.4 ms | 771 | 4.0 | 4.33 ms |
| 8 | 771 | 4.5 | 8.65 ms | 1.13K | 8.0 | 5.24 ms |

The writer now waits for answered submitters as long as waiting is expected to lower total
latency, the rule the chunk store's writer derived. Both writers take it from one place,
`mantle_disk::commit`.

**3. An append costs one durable write, except when this machine stalls.** Median latency
is 4.33–4.46 ms in most rows at every replica count and entry size, against 4.46 ms for the
device's durable 4 KiB write. Some rows show a p50 of 5–11 ms, or a p99.9 above 90 ms, and
repeating a row moves them elsewhere. This is the burst stalling below the file system that
the chunk store's benchmark recorded (finding 2 there). A point of a few seconds cannot
separate it from the log's own cost, so a row's tail is not a property of the log.

## Open

- The same benchmark on Linux with `fdatasync` on NVMe and on disks, where a flush is cheaper
  and the log's own per-frame work is a larger share of an append.
- Points repeated until the result is statistically stable, as the chunk benchmark needs too
  (docs/STATUS.md).
