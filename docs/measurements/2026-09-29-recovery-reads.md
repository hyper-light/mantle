# Reading the chunk store's index log at open

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
