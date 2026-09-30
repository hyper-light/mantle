# The segment table at tens of thousands of segments

**Question.** Every writer batch walked the whole segment table several times: a map of every
segment in use, every segment scanned for ones to free, every free segment gathered and
sorted, the free ones counted, and the table copied whole for readers. Every scrub step
summed every segment's live bytes, and every victim the cleaner chose scanned the table once
per victim already tried in its pass (audit P06). A 20 TB device of 256 MiB segments has
about 75,000 segments. The table now keeps its free segments, the segments the writer can
free without copying, its counts by state and its live bytes as segments change, hands
readers only the segments a batch changed, and the cleaner looks tried victims up in a set.
What did the passes cost, and what is left?

**Method.** `mantle bench chunk --skip-device --sizes 4K --workers 1,16,64 --rounds 10
--segment-size S`, release builds of commit 5a9ce28 without the change (the benchmark's new
segment-size option added) and with it, on an APFS RAM disk (`hdiutil attach -nomount
ram://…`), so that a batch's flush costs next to nothing and the store's own work is what is
measured. macOS 26.4.1 on an Apple M5 Max, the machine otherwise idle. The benchmark's volume
is 4 GiB or a tenth of the free space, whichever is less, and smaller segments stand in for a
larger device's count of them: an 8 GiB RAM disk gave an 856 MB volume, a 48 GiB one a 4.29 GB
volume. The benchmark first writes the whole volume once, in chunks of the most a segment's
record holds, then measures 4 KiB puts, each point in rounds of one second until its
throughput is within ±5% at 95% confidence or ten rounds have run.

## Findings

**1. At 62,403 segments the store could not fill its volume.** Without the change, the first
pass over a 4.29 GB volume of 64 KiB segments had not finished after 30 minutes, when it was
stopped, having used 1,850 s of CPU. Samples of the process (`sample`, 3 s) found the writer
waiting for a lock as it published its batch in 87% of its samples, and the cleaner running,
not waiting for the writer, in 91% of its own: filled in chunks of nearly a segment, every
sealed segment's only dead bytes are its record's padding, so every one was a candidate, each
chosen by a pass over the table with a linear search of the victims tried, under the table's
read lock, which the writer's publication waits for. With the table kept, the
same pass took 73 s (58.6 MB/s); and with segments whose dead bytes cannot pay for packing
their live data no longer taken as victims (finding 3), 4.0 s (1.08 GB/s).

**2. Puts no longer pay for the table, and their tail shrinks.** 4 KiB puts, ops/s and p50,
without and with the change:

| Segments | In flight | Without | With |
|---|---|---|---|
| 2,882 | 1 | 4.70K, 197 µs | 4.85K, 197 µs |
| 2,882 | 16 | 35.5K ±72%, 319 µs, p99 2.69 ms | 60.1K ±9%, 254 µs, p99 467 µs |
| 2,882 | 64 | 141K, 426 µs | 140K, 467 µs |
| 11,530 | 1 | 3.62K, 270 µs | 4.58K, 217 µs |
| 11,530 | 16 | 45.5K, 336 µs | 49.3K, 311 µs |
| 11,530 | 64 | 99.1K, 655 µs | 135K, 459 µs |
| 15,610 | 1 | 3.89K, 242 µs | 4.94K, 184 µs |
| 15,610 | 16 | 49.4K, 303 µs, p99 2.62 ms | 59.9K, 246 µs, p99 705 µs |
| 15,610 | 64 | 113K, 557 µs, p99 4.19 ms | 143K, 418 µs, p99 737 µs |
| 62,403 | 1 | — | 4.97K, 184 µs |
| 62,403 | 16 | — | 58.4K, 262 µs |
| 62,403 | 64 | — | 109K, 557 µs |

The first six rows are the 856 MB volume, with the table alone; the rest the 4.29 GB volume,
with the table and the rule for victims of finding 3.

Without the change a lone put's median grew with the segments, from 197 µs at 2,882 to
242–270 µs at 11,530–15,610; with it, it was 184–217 µs from 2,882 segments to 62,403. The
11,530-segment runs spent 50.5 s of CPU without the change and 18.7 s with it for the same
points. At 62,403 segments and 64 in flight, two runs with the table gave 149K/s and 109K/s;
the second's rounds persisted in a state, so it has no interval.

**3. A segment whose dead bytes cannot pay for packing is not a victim.** The cleaner's pass
stops once its victims should have given back a segment's worth of space net of packing and
have not, but a victim whose dead bytes do not cover packing its live data adds nothing to
that sum, so a volume of such segments was cleaned victim after victim, each relocation
moving a chunk to free nothing. Such segments are no longer candidates, and a pass that finds
no candidate is futile like one that gains nothing, so a put that finds no free segment is
answered `Full` rather than `Busy` until data dies. The first pass of the 15,610-segment
volume ran at 55.7 MB/s without the change, 483 MB/s with the table alone, and 3.21 GB/s with
both.

**4. What is left.** A victim is still chosen over the whole table, since its score moves
with the clock: at 62,403 segments one choice held the table's read lock for a pass of the
table, against a victim's relocation, which reads and writes what is live in it. A scrub step
costs one lookup of its segment and one range of the index's record places.
