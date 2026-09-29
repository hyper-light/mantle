# The chunk store benchmark in rounds

**Question.** How stable is each point of `mantle bench chunk` once it runs in rounds? The
single passes of the first benchmark measured the drive's recent history as much as the
store ([2026-09-28](2026-09-28-chunk-store-benchmark.md), finding 7).

**Method.** `mantle bench chunk .` at commit-time defaults, release build, macOS 26.4.1 on
an Apple M5 Max, APFS on the internal Apple SSD AP8192Z, the machine otherwise idle. Each
point (chunk size and requests in flight) runs rounds of one-second puts, reads and
file-layer reads, until the throughput of all three is within ±5% at 95% confidence or six
rounds have run (`calibrate::Rounds::STANDARD`; docs/research/11 §13.3). The table gives the
half-width of each mean's 95% interval, as a share of the mean; every point ran six rounds.

| Chunk | In flight | Put | Get | File layer |
|---|---|---|---|---|
| 4 KiB | 1 / 4 / 16 / 64 | ±2% / 2% / 2% / 2% | ±37% / 46% / 7% / 60% | ±54% / 57% / 10% / 23% |
| 64 KiB | 1 / 4 / 16 / 64 | ±1% / 16% / 47% / 11% | ±72% / 64% / 26% / 15% | ±48% / 13% / 107% / 42% |
| 1 MiB | 1 / 4 / 16 / 64 | ±1% / 4% / 16% / 1% | ±16% / 13% / 41% / 45% | ±67% / 55% / 43% / 22% |
| 8 MiB | 1 / 4 / 8 / 8 | ±5% / 8% / 5% / 8% | ±42% / 34% / 31% / 41% | ±8% / 30% / 31% / 19% |

(8 MiB puts run at most 8 writers, what the store's queue admits; the reads run at the
requested depth.)

## Findings

**1. Puts are stable; reads are not, in the store or beneath it.** Twelve of sixteen put
points are within ±5%. Reads through the store spread ±7–72%, and the same reads straight
through the file layer spread ±8–107%, so the spread is the environment's, as finding 2 of
the first benchmark found it: every thread's reads stall together for about 6 ms at a time,
in bursts, at random moments. A one-second round catches none or several.

**2. ±5% on reads is unaffordable here.** A spread of 0.3 needs about 140 rounds for ±5%
(docs/research/11 §16.3). The benchmark states the interval each point reached instead of
claiming a precision it lacks (§16.4), so a comparison between runs can see which
differences the noise could have made.

**3. Large puts stay under the device's durable bandwidth.** 8 MiB puts reach 3.24–3.44
GB/s against the 5.02 GB/s calibration measured for 32 MiB written and then flushed: 65–69%,
as finding 6 and finding 8 of the first benchmark explain.

## Consequences

- Each point states its rounds and interval; puts are reported to the precision they
  reach, and reads to the precision this machine allows.
- Open (docs/research/11 §16.3–§16.4): the step and round counts from a dimensioning run
  (Kalibera and Jones, 2013), and results that alternate between two states reported per
  state rather than as one mean.
