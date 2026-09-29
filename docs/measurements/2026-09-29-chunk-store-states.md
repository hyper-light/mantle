# The chunk store benchmark's rounds, in states

**Question.** The rounds of `mantle bench chunk` spread ±7–107% when summarized by a mean
([2026-09-29 rounds](2026-09-29-chunk-store-rounds.md)). Are its rounds one distribution
around one value, and are they independent? If not, what makes them otherwise, and how should
a point be reported?

**Method.** Release builds, macOS 26.4.1 on an Apple M5 Max, APFS on the internal Apple SSD
AP8192Z, the machine otherwise idle.

- **A dimensioning run** (Kalibera and Jones, docs/research/21 §2.4): `mantle bench chunk
  --skip-device --sizes 4K,64K,1M --workers 1,16` changed to run exactly twenty rounds a point,
  record every round's throughput, and repeat on four fresh volumes in one process: 80 rounds
  of each of 18 point-operations. A round is a one-second step of puts, of reads of what they
  wrote, and of the same reads through the file layer, then deletes. The change was not kept.
- **Reads after writes, without the store**: `crates/disk/examples/fresh_reads.rs`, forty
  rounds each of three kinds (below).
- **The benchmark as it now reports**: `mantle bench chunk` at its defaults, each point's rounds
  judged as docs/design/measurement.md describes.

## Findings

**1. Reads fall in two states.** Taking a point's upper quartile as its fast state, ten of the
twelve read points have at least a fifth of their rounds below two fifths of it. Puts sit
mostly in their fast state.

| Point | Rounds at or above 0.8 of the upper quartile | below 0.4 | between |
|---|---|---|---|
| put 4 KiB × 1 | 62 | 5 | 13 |
| get 4 KiB × 1 | 43 | 27 | 10 |
| file layer 4 KiB × 1 | 53 | 22 | 5 |
| put 4 KiB × 16 | 71 | 1 | 8 |
| get 4 KiB × 16 | 70 | 8 | 2 |
| file layer 4 KiB × 16 | 22 | 44 | 14 |
| put 64 KiB × 1 | 57 | 1 | 22 |
| get 64 KiB × 1 | 34 | 45 | 1 |
| file layer 64 KiB × 1 | 39 | 26 | 15 |
| put 64 KiB × 16 | 78 | 2 | 0 |
| get 64 KiB × 16 | 54 | 22 | 4 |
| file layer 64 KiB × 16 | 41 | 23 | 16 |
| put 1 MiB × 1 | 60 | 5 | 15 |
| get 1 MiB × 1 | 41 | 23 | 16 |
| file layer 1 MiB × 1 | 37 | 37 | 6 |
| put 1 MiB × 16 | 54 | 3 | 23 |
| get 1 MiB × 16 | 44 | 21 | 15 |
| file layer 1 MiB × 16 | 52 | 3 | 25 |

Gets of 64 KiB at one in flight ran at 9,077–10,683 a second in 34 of 80 rounds and at
498–3,362 in 45, with one between. Their mean, 5,198, is a rate no round ran at.

**2. The slow state is the drive's, after full flushes.** `fresh_reads` repeats a one-writer
round without the store: a second of one-at-a-time 64 KiB writes through the file layer, each
followed by the platform's full flush (`F_FULLFSYNC`), then a second of one-at-a-time random
64 KiB reads of what the round wrote, then a second of the same reads over data written before
the rounds began. Two controls replace the first second: the same writes without flushes,
paced to no more than the flushed rate (they reached three quarters of it), and a second with
no writes. A round is slowed when its reads ran below four fifths of the run's median.

| First second of each round | Writes/s | Rounds slowed: fresh reads | Rounds slowed: old reads |
|---|---|---|---|
| 64 KiB writes, each flushed | 256 | 13 of 40 | 11 of 40 |
| 64 KiB writes, not flushed | 194 | 1 of 40 | 0 of 40 |
| idle | 0 | 1 of 40 | 0 of 40 |

With flushes, about every 9 s the drive spends about a second during which every read takes
6.7–8.5 ms instead of about 0.1 ms, whether it reads what was just written or what was written
before. Without flushes, one such second in 120. The store's reads at one in flight run at a
tenth to a fifth of their rate in the rounds a stall falls in, and its file-layer reads the
same; the store's read path adds nothing to it. Reads that must meet a latency bound cannot count on one
drive through such a second.

**3. After a large write the drive stays slow for tens of seconds.** The second volume's fill
ran at 426 MB/s where the other three ran at 734–815 MB/s. In rounds 3 and 5 to 14 of its first
point, about 36 s, puts of 4 KiB at one in flight ran at 81–144 a second and gets at
1,088–3,892, against 238–249 puts and 11,636–15,229 gets in its last six rounds. This is the
state in time that finding 7 of the [first benchmark](2026-09-28-chunk-store-benchmark.md) saw
after its fill.

**4. The rounds are often ordered in time.** Of the 72 series of twenty rounds, 30 fail the
order checks of docs/design/measurement.md §2: 15 persist in a state and 15 alternate, where
independent rounds would fail at most about seven. Kalibera and Jones's estimators (their
Eq. 1–3, with a fresh volume costing 5.7 s beside 3.1 s a round) give:

| Point | Rounds' CV | T₁²/mean² | T₂²/mean² | Rounds a volume (Eq. 3) |
|---|---|---|---|---|
| put 4 KiB × 1 | 0.25 | 0.052 | 0.016 | 3 |
| get 4 KiB × 1 | 0.58 | 0.308 | 0.036 | 4 |
| file layer 4 KiB × 1 | 0.55 | 0.205 | 0.130 | 2 |
| put 4 KiB × 16 | 0.15 | 0.022 | 0.002 | 5 |
| get 4 KiB × 16 | 0.26 | 0.066 | 0.004 | 6 |
| file layer 4 KiB × 16 | 1.00 | 0.602 | 0.520 | 2 |
| put 64 KiB × 1 | 0.25 | 0.062 | 0.001 | 11 |
| get 64 KiB × 1 | 0.75 | 0.550 | 0.012 | 10 |
| file layer 64 KiB × 1 | 0.56 | 0.285 | 0.035 | 4 |
| put 64 KiB × 16 | 0.12 | 0.014 | ≤ 0 | no bound |
| get 64 KiB × 16 | 0.49 | 0.250 | ≤ 0 | no bound |
| file layer 64 KiB × 16 | 0.65 | 0.429 | ≤ 0 | no bound |
| put 1 MiB × 1 | 0.25 | 0.055 | 0.013 | 3 |
| get 1 MiB × 1 | 0.50 | 0.222 | 0.036 | 4 |
| file layer 1 MiB × 1 | 0.69 | 0.418 | 0.077 | 4 |
| put 1 MiB × 16 | 0.25 | 0.052 | 0.014 | 3 |
| get 1 MiB × 16 | 0.44 | 0.201 | ≤ 0 | no bound |
| file layer 1 MiB × 16 | 0.25 | 0.066 | ≤ 0 | no bound |

T₁² is the variance between rounds of one volume, T₂² what the volumes add. Their method
assumes independent rounds, which the order checks deny, so these counts are recorded and not
used.
They do say that for some points the volume a run happens to get moves its result as much as
its rounds do: a report from one volume says nothing of that.

**5. Most points are two states, or ordered in time.** `mantle bench chunk` at its defaults,
every point's rounds judged as docs/design/measurement.md describes, took 33 minutes. Of its
48 point-operations, 23 fall in two states, 31 are ordered in time (14 persist, 17 alternate),
and 6 are one independent state within ±5% of its median, all of them puts: 4 KiB at 1 and 4
in flight (240 and 854 a second, ±1%), 64 KiB at 4 (919, ±2%), 1 MiB at 1 and 16 (220 and
1,990, ±5%) and 8 MiB at 4 (403, 3.38 GB/s, ±2%). By operation:

| Operation | Point-operations | In two states | Ordered in time |
|---|---|---|---|
| put | 16 | 3 | 8 |
| get | 16 | 10 | 12 |
| file layer | 16 | 10 | 11 |

Some rows, each a state's median, its interval's wider side, and its rounds:

| Point | Get | File layer |
|---|---|---|
| 64 KiB × 1 | 8,850 ±4% in 15 of 30; 2,040 ±53% in 15 | 8,830 ±21% in 14; 2,210 ±97% in 16 |
| 64 KiB × 16 | 113K ±9% in 17; 16.6K ±61% in 13 | 111K in 17; 2,310 in 13, persistent |
| 1 MiB × 64 | 13.4K in 14; 7,020 in 16, alternating | 13.4K in 14; 6,660 in 16, alternating |
| 8 MiB × 4 | 1,680 in 15; 951 in 15, alternating | 1,690 in 15; 782 in 15, alternating |

Where reads move 1 MiB or more with four or more in flight, the store's fast state matches the
file layer's at five of the six points, about 14 GB/s at the most, the sequential read rate
calibration measures here (14.7 GB/s): the store adds nothing measurable to a read in that
state. At the sixth, 8 MiB with 16 in flight, the store's rounds were judged one state, with a
median of 1,370 beside the file layer's fast state of 1,630.

## Consequences

- `mantle bench chunk` reports each point's states, each state's median with its 95% interval
  when its rounds are independent, and whether the rounds are ordered in time
  (docs/design/measurement.md).
- The spread of the [2026-09-29 rounds](2026-09-29-chunk-store-rounds.md) was the two states of
  finding 1, and "about 140 rounds for ±5%" there applied a formula for normal rounds to
  rounds that are not.
- Open: locating where states change in time, and repetition across volumes and processes
  (docs/design/measurement.md §7).

## Baseline

The run of finding 5 is the baseline `mantle bench chunk` holds puts and reads to, state by
state.
