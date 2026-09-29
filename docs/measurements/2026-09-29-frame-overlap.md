# The index frame beside a batch's records

**Question.** A batch of the chunk store writes its records into a segment, its index frame
into the log, and then flushes once. The frame used to be written after the records, and
cost a quarter of durable bandwidth ([2026-09-28](2026-09-28-chunk-store-benchmark.md),
finding 8). Does issuing it at the same time as the records remove that cost?

**Method.** `cargo run --release -p mantle-disk --example frame_overlap -- . 32 64`, and the
same with 8 MiB batches. The run used a release build on macOS 26.4.1 on an Apple M5 Max,
APFS on the internal Apple SSD AP8192Z, with direct I/O and `F_FULLFSYNC`.

- Batches cycle through 1 GiB of written space. The 4 KiB frame goes into a separate region
  2 GiB in, as the log sits apart from the segments.
- Three patterns run interleaved, one batch of each in turn, 64 of each:
  - the data, then a flush;
  - the data, then the frame, then a flush;
  - the data and the frame issued together on two threads, then a flush.

| Batch | Pattern | p50 | Throughput |
|---|---|---|---|
| 32 MiB | data, flush | 6.72 ms | 5.10 GB/s |
| 32 MiB | data, frame after, flush | 8.61 ms | 3.97 GB/s |
| 32 MiB | data and frame at once, flush | 6.69 ms | 5.02 GB/s |
| 8 MiB | data, flush | 5.65 ms | 1.51 GB/s |
| 8 MiB | data, frame after, flush | 5.75 ms | 1.43 GB/s |
| 8 MiB | data and frame at once, flush | 5.69 ms | 1.49 GB/s |

**Reading.** At 32 MiB, a frame written after the data waits for the data's transfer to end,
then pays its own before the flush: 1.9 ms, a fifth of the batch. Issued together, the frame
costs nothing measurable. At 8 MiB the flush's 4.5 ms dominates, and the frame's cost is
within 2% either way.

**In the store.** `mantle bench chunk . --skip-device` alternated between the writer before
the change and after it. Each run's point ran six one-second rounds.

- 8 MiB puts, 4 writers, four alternations:
  - before: 3.27, 3.31, 3.44, 3.31 GB/s;
  - after: 3.48, 3.35, 3.60, 3.38 GB/s.
- The change is ahead in every pair, 3.6% on average. p50 latency fell from 9.70–9.96 ms to
  8.91–9.44 ms, also in every pair.
- The gain is smaller than the raw pattern's because in the store the frame was 7.6% of the
  writer's time (finding 8 of the first benchmark).
- 4 KiB puts, one writer and sixteen, three alternations: level (233 and 235 puts/s with one
  writer in the quiet pair). A thread per batch for the frame costs nothing visible against
  a 4.3 ms flush. One pair was stalled by the environment in both builds, with p99.9 above
  85 ms.

**Consequence.** The writer encodes a batch's regions and frame first, then issues their
writes together and flushes once. Every write of a batch is made durable by that one flush,
and until the flush any of them may reach the device in any order however they are issued.
So recovery sees nothing it did not before, and the crash soak checks it.

## The next batch during the last one's flush

**Question.** Would a writer that writes batch N+1 while batch N flushes, with flushes
still one at a time, reach more of the device's durable bandwidth? Two independent streams
each writing and flushing reached 5.99 GB/s against 5.12 for one (finding 9 of the first
benchmark). A group-commit writer cannot run two flushes at once, so the pipelined pattern
is its own measurement.

**Method.** `cargo run --release -p mantle-disk --example flush_pipeline -- . 32 8`, and the
same at 8 MiB. Each batch is its data and a 4 KiB frame, issued together.

- Serially, a batch is written and then flushed.
- Pipelined, a flusher thread flushes batch N while the caller writes batch N+1, and the next
  flush starts once both have ended.
- The two alternate in blocks of 16 batches, 8 blocks each.

| Batch | Serial | Pipelined |
|---|---|---|
| 32 MiB | 4.84 GB/s | 5.31 GB/s |
| 8 MiB | 1.49 GB/s | 1.45 GB/s |

**Reading.** At 8 MiB, a 1.2 ms write overlapped with a 4.5 ms flush would shorten the batch
by a fifth if the two overlapped. Nothing is gained, so writes issued during an
`F_FULLFSYNC` wait for it. At 32 MiB the overlap gains 10%.

**Consequence.** A pipelined writer must validate each batch against the one still flushing,
fail both batches when a flush fails, and let recovery verify every batch after the last
completed flush. Frames already carry that point as their group. That complexity is not
worth 10% at the largest batches and nothing at 8 MiB on this device. The writer stays
serial until a device measures more, for example NVMe under `fdatasync`, where a flush may
not hold writes.

