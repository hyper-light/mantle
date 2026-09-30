# Reading the logs at open, and entries no longer in memory

**Question.** Opening a volume replays its index log from the last checkpoint and then, to
tell a torn tail from an acknowledged frame damaged since, examines every block of the log
for a frame of a later flush group (docs/design/chunk-store.md §6). Each frame took a read of
its first block and another of the whole frame, and the search read the log one block at a
time (audit P07). Both now read through a window of one largest frame, 4 MiB. What does an
open cost before and after?

**Method.** `mantle bench chunk --skip-device --sizes 4K --workers 1,16 --rounds 10`, which
ends by closing its volume and opening it again, timed, with the frames replayed and the
log's size. Release builds of the same tree with and without the windowed reader, run in
turn, twice each, on the internal SSD (APFS on the Apple SSD AP8192Z, macOS 26.4.1, Apple M5
Max, otherwise idle), reading with `F_NOCACHE`, and once each on an APFS RAM disk with
15,610 segments of 256 KiB (docs/measurements/2026-09-29-segment-table.md). The volume is
4.29 GB with an index sized for 700,000 4 KiB chunks, so its log is 168–169 MB, 41,000
blocks of 4 KiB.

## Findings

**1. An open that took a second takes 50–80 ms.**

| Device | Without | With |
|---|---|---|
| SSD | 1.16 s, 3,885 frames replayed · 1.09 s, 4,669 frames | 75.6 ms, 4,783 frames · 52.3 ms, 4,668 frames |
| RAM disk | 1.44 s, 17,326 frames | 116 ms, 9,141 frames |

Without the window, the search alone read the log's 41,000 blocks one at a time, and the
replay read each frame twice: about 50,000 reads on the SSD, about 22 µs each. With it the
search reads the log in 41 windows, each at most twice, and the replay the frames since the
checkpoint in as many windows as they fill.

**2. The corruption proofs are the same.** The search still examines every block, and a
frame whose header names a later flush group is still verified whole before it counts, so
`damage_inside_the_log_refuses_to_open_but_a_torn_tail_does_not` (`crates/chunk/tests/corruption.rs`)
and the power-loss tests pass unchanged; `opening_reads_the_log_a_window_at_a_time` (`crates/chunk/src/volume.rs`) counts
the reads that start in the log for an open that replays 600 frames and finds them within
four per window of the log.

## The Raft log

**Question.** The Raft log's open walked each live segment frame by frame, each frame a read
of its first block, a read of the whole frame and a look at the file's length; searched the
rest of the last segment, and every slot whose header does not read, a block at a time; and
walked the highest segment again to replay it. A read of entries no longer in memory read
each entry alone, however many shared a block. The open and the writer's sweep now read
through a window of one segment, the length is taken once, and entries whose blocks touch
are read together (docs/design/raft-log.md §6, §7).

**Method.** `mantle bench log --skip-device --seconds 5 --sizes 128,16K --replicas 1,16,256`,
release builds of the same tree with and without the change, run in turn, twice each, on the
same SSD. Each point appends for five seconds, each replica compacting to keep 64 entries,
then reopens the log, timed, and reads back every replica's kept entries from the file.

**Findings.** Reopen time and read-back, without and with, the two runs of each:

| Entry | Replicas | Reopen without | Reopen with | Read back without | Read back with |
|---|---|---|---|---|---|
| 128 B | 1 | 183 ms · 159 ms | 11.0 ms · 12.7 ms | 116 in 5.07 ms · 86 in 6.31 ms | 92 in 1.09 ms · 106 in 421 µs |
| 128 B | 16 | 131 ms · 236 ms | 17.1 ms · 17.2 ms | 1,653 in 70.0 ms · 1,480 in 63.7 ms | 1,274 in 7.05 ms · 1,633 in 17.0 ms |
| 128 B | 256 | 365 ms · 154 ms | 40.5 ms · 17.4 ms | 25,822 in 2.46 s · 23,232 in 1.58 s | 23,684 in 1.59 s · 19,761 in 1.48 s |
| 16 KiB | 1 | 98.2 ms · 220 ms | 8.87 ms · 7.99 ms | 126 in 17.4 ms · 113 in 8.92 ms | 111 in 1.48 ms · 65 in 1.01 ms |
| 16 KiB | 16 | 123 ms · 185 ms | 13.4 ms · 18.0 ms | 1,313 in 113 ms · 1,476 in 233 ms | 1,289 in 112 ms · 1,870 in 177 ms |
| 16 KiB | 256 | 228 ms · 262 ms | 108 ms · 81.1 ms | 25,070 in 2.86 s · 20,531 in 2.56 s | 28,980 in 2.85 s · 25,578 in 2.60 s |

Pairing the runs in order, reopening took 2 to 27 times less. Reading back is faster where a group's entries lie in
consecutive blocks: one replica's, one to a frame, and 16 replicas' 128 B entries, 16 to a
one-block frame. Where a frame holds many groups' entries across several blocks, 256 replicas
or 16 KiB entries, a group's entries lie apart and are still read one at a time, at about the
latency of one random read each; reading through the gaps between them is what would help
there, and is not done.

The tests `opening_reads_each_segment_through_a_window` and
`entries_not_in_memory_that_lie_together_are_read_at_once` (`crates/log/tests/log.rs`) count
the reads: at most three a segment and two persist slots for an open, where a window refilled
on every read takes 208 for seven segments, and one read for 200 entries written in one update,
where reading each alone takes 200.
