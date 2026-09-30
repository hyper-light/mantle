# Answering a log update only once its flush is confirmed

**Question.** The Raft log answered a frame's updates at the frame's own flush. The frame's
persist record, written in that flush, may survive a frame torn by a crash, so recovery could
tell a flushed frame from a torn one only by a later record confirming the flush: the next
frame's, or a confirmation written once the log fell idle for one measured flush time. The
audit (S01, round three) cut power after a frame's flush and before its confirmation, damaged
the frame, and recovery dropped an acknowledged entry without marking the member uncertain.
The log now answers a frame's updates only once its confirmation is durable: the next
frame's record when work is queued, or a confirmation written on its own at once
(docs/design/raft-log.md §6). What does that cost an append?

**Method.** `mantle bench log $DIR --seconds 3 --sizes 1K --replicas 1,2,4,16,64,256
--skip-device`, release builds of commit 6f00bc4 (before) and of the change on it (after),
run before, after, before, after. macOS 26.4.1 on an Apple M5 Max, APFS on the internal
Apple SSD AP8192Z, the machine otherwise idle but for the other runs. Closed-loop replicas
each append one 1 KiB entry to their own group at a time and wait for it to be answered.
Each cell gives the two runs.

## Findings

**1. An append now waits for two flushes, and a closed-loop replica appends half as often.**

| Replicas | Before: appends/s | p50 | After: appends/s | p50 |
|---|---|---|---|---|
| 1 | 240 · 220 | 4.33 ms | 107 · 119 | 8.65 ms |
| 2 | 432 · 463 | 4.33 ms | 243 · 203 | 8.65 ms |
| 4 | 879 · 759 | 4.33 ms | 454 · 443 | 8.65 ms |
| 16 | 3.72K · 3.54K | 4.33 ms | 1.88K · 1.88K | 8.65 ms |
| 64 | 14.4K · 14.1K | 4.33 ms | 6.44K · 7.50K | 8.65 ms |
| 256 | 45.8K · 47.4K | 5.37 ms | 23.3K · 27.4K | 9.44–9.70 ms |

Here a flush, `F_FULLFSYNC`, takes about 4.3 ms whatever it carries
(2026-09-28-raft-log-benchmark.md). A replica's append is answered after its frame's flush
and the flush of the record confirming it, and a closed-loop replica sends its next append
only then, so no frame follows at once and every confirmation is a flush of its own. With 64
replicas the frames carried 44–47 updates rather than 63: the writer's wait for returning
replicas learns from answers that now come a flush later.

**2. What recovers the rate.** The second flush is the price of telling a torn frame from a
damaged one, which a single flush cannot do when the frame and the record that describes it
are written together (AGL+18 §3.3.3). It costs a flush of its own only when nothing is queued
behind the frame, as here, where every replica waits on its own answer. A replica with more
than one ready in flight (audit §5.1, the node's scheduler) would put its next frame behind
the last, and that frame's record would confirm it; neither that nor a load of that shape has
been built or measured.
