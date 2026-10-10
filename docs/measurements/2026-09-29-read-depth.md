# How many reads a volume holds at the device

**Question.** Before this change the chunk store bounded nothing about reads: every caller's
thread read at the device, so a flood of callers put the whole flood in the device's queue.
A volume now holds a stated number of reads at the device, lets as many more wait in the
order they came, and refuses the rest with `Busy` (docs/design/chunk-store.md §7). How many
should it hold: the depth of greatest power, Kleinrock's optimum that calibration already
reported (docs/research/11 §13.3), or the depth where throughput stops growing?

**Method.** Two runs of `mantle bench chunk --sizes 4K,64K --workers 1,16,64 --rounds 10`,
a release build on macOS 26.4.1 on an Apple M5 Max, APFS on the internal Apple SSD AP8192Z,
the machine otherwise idle. Each run calibrates the device, formats a volume holding the
depth calibration gives, and measures puts, reads through the store, and the same reads
through the file layer at the same number in flight. The benchmark runs no more readers
than the volume holds and lets wait, so no read is refused.

- The first run held the depth of greatest power, throughput squared over depth.
- The second held the shallowest depth whose throughput interval overlaps the fastest
  point's, with calibration's ladder of 4 KiB random reads going on past 64, four times
  deeper a step, while each step was faster than the one before beyond both intervals.

## Findings

**1. The depth of greatest power leaves throughput behind.** Calibration's ladder in the
first run:

| 4 KiB reads in flight | Throughput | p50 | p99 |
|---|---|---|---|
| 1 | 14.0K/s | 71.7 µs | 92.2 µs |
| 4 | 54.8K/s | 73.7 µs | 100 µs |
| 16 | 165K/s | 94.2 µs | 147 µs |
| 64 | 214K/s | 295 µs | 393 µs |

Power peaks at 16, where throughput still has 30% to gain by 64. With 16 at the device and
16 more waiting, 32 callers read through the store at 88.7K/s with a median of 336 µs; the
same 32 reads through the file layer, all at the device, ran at 202K/s with a median of
143 µs. For 64 KiB reads the store ran 74.1K/s at 410 µs in half its rounds, the file layer
175K/s at 172 µs. Two things cost it. Held at 16, the device can do no better than 16's
165K/s, below what 32 at the device reached; reads that would have been served in parallel
waited their turn instead. And this first version woke every waiting read each time a turn
was given back, which cost the rest.

**2. Past saturation the device loses throughput as well as latency.** The second run's
ladder went on to 256:

| 4 KiB reads in flight | Throughput | p50 | p99 |
|---|---|---|---|
| 1 | 14.4K/s | 69.6 µs | 81.9 µs |
| 4 | 53.7K/s | 71.7 µs | 98.3 µs |
| 16 | 160K/s | 92.2 µs | 168 µs |
| 64 | 219K/s | 287 µs | 410 µs |
| 256 | 193K/s | 1.31 ms | 1.84 ms |

Throughput stops growing at 64 and falls by 256, where the median read takes four and a half
times as long. Before the gate, 256 callers would have put the device there.

**3. Held at saturation, the store reads as fast as the file layer.** The second run's
volume held 64 at the device and let 64 more wait:

| Point | Store | p50 | File layer | p50 |
|---|---|---|---|---|
| 4 KiB, 16 in flight | 107K/s | 98.3 µs | 103K/s | 90.1 µs |
| 4 KiB, 64 in flight | 214K/s in 5 of 10 rounds, 95.0K/s in 5 | 295–303 µs | 211K/s | 270 µs |
| 64 KiB, 16 in flight | 66.7K/s | 139 µs | 67.6K/s | 135 µs |
| 64 KiB, 64 in flight | 128K/s | 360 µs | 118K/s | 344 µs |

The two states of 4 KiB reads at 64 in flight are the drive's stalls under a stream of full
flushes ([2026-09-29 states](2026-09-29-chunk-store-states.md)); the file layer's rounds
alternate as well.

## What changed

- A volume holds the depth where calibration finds throughput stops growing
  (`Calibration::random_read_saturation`), and calibration's ladder goes on past its planned
  depths until it finds it, no deeper than NVMe's 65,535 commands a queue.
- The gate hands a freed turn to the read that has waited longest and wakes only that read;
  the first version woke every waiting read on each turn given back.
- The depth of greatest power stays in the device report, as what it is: the point where
  latency starts to cost more than the throughput it buys.
