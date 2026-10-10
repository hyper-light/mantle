# What a persist record costs the Raft log's appends

**Question.** Each frame's flush now also writes the frame's persist record, one block at the
start of the file, apart from the frame (docs/design/raft-log.md §2, audit S01). How much does
that second write cost an append?

**Method.** `mantle bench log <dir> --seconds 3 --sizes 1K --replicas 1,64 --skip-device`,
release builds of 011466c, the commit before the change, and of the
change, run alternately six times each on the same directory, then three rounds of the
1, 4, 16 and 64 replica points. macOS 26 on an Apple M5 Max, APFS on the internal Apple SSD,
the machine of docs/measurements/2026-09-28-raft-log-benchmark.md. Closed-loop replicas
append one 1 KiB entry at a time to their own group and wait for it to be durable.

## Findings

**1. About 4% fewer appends a second with one replica, about 2% with 64; latency unchanged.**
Appends a second over the six alternating rounds:

| Replicas | Before | After | Median before | Median after |
|---|---|---|---|---|
| 1 | 198, 203, 250, 238, 245, 180 | 202, 187, 237, 237, 219, 175 | 220.5 | 210.5 |
| 64 | 11.3K, 11.7K, 14.4K, 14.8K, 13.9K, 11.2K | 11.1K, 11.7K, 12.8K, 12.6K, 13.2K, 12.6K | 12.8K | 12.6K |

With one replica the change was lower in five of the six pairs. The median append took
4.33 ms before and after, one `F_FULLFSYNC`: the record's write is issued before the flush
and costs a fraction of it. Both differences lie within the rounds' own spread on this
machine, which the 2026-09-28 benchmark attributes to stalls below the file system, so the
figures bound the cost rather than fix it. Protocol-aware recovery measured the same design
at up to 4% on SSD and 8–10% on HDD, where the record's separate place costs a seek
(AGL+18 §5.2; docs/research/03 AGL+18-F9).

**2. Batching is unchanged.** Appends per flush were 63.3–63.9 at 64 replicas before and
after: a busy log confirms each frame with the next frame's record and never pays the idle
confirmation's flush.

## Open

- The same comparison on Linux with `fdatasync`, on NVMe and on disks, where a flush is
  cheaper and a second write is a larger share, and on HDD, where it costs a seek.
- Issuing the record's write beside the frame's rather than after it, as the chunk store
  issues a batch's frame beside its records (docs/measurements/2026-09-29-frame-overlap.md),
  if the Linux runs show the serial write matters.
