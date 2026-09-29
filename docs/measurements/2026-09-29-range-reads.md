# Range reads that read only their checksum blocks

**Question.** A range read of a chunk read one span from the record's header through the
last checksum block it needed, so a read near the end of a large chunk read everything before
it: 4 KiB at the end of an 8 MiB fragment read 8,392,704 bytes (audit P01). A read now takes
the header and checksum table and, apart, the checksum blocks the range touches, unless the
payload between them is no more than the device's read gap
(docs/design/chunk-store.md §7). What does that change for range reads, and which gap?

**Method.** `mantle bench chunk --sizes 4K,1M,8M --workers 1,16 --rounds 10`, release builds
on macOS 26.4.1 on an Apple M5 Max, APFS on the internal Apple SSD AP8192Z, the machine
otherwise idle, from commit 92fcc9d with the change applied. For each chunk larger than
4 KiB the benchmark reads 4 KiB ranges at random 4 KiB-aligned places within the chunks it
just wrote, at the same number in flight as its whole-chunk reads. Three builds differ only
in the gap:

- **Before:** the plan never splits, which reads exactly what the code before this change
  read.
- **Idle gap:** the depth-one median of a small random read times the saturated sequential
  rate, about 1 MB here: the break-even for one read on an idle device.
- **Saturated gap:** the saturated sequential rate over the saturated small-read rate, less
  one small read: the break-even in device time when the device serves the most reads
  (docs/research/11 §8.4). This is the rule the store keeps.

The runs went idle gap, before, saturated gap, before, saturated gap. Each point runs rounds of
one second until its throughput is within ±5% at 95% confidence or ten rounds have run; the
tables give the median of the rounds, and where the rounds fall in two states, each with its
share of the rounds. The last run's calibration put the saturated gap at 51.2 kB, which its
output prints; the earlier builds did not print theirs.

## Findings

**1. Past the first checksum block, range reads of large chunks are several times faster.**
4 KiB ranges of 8 MiB chunks:

| In flight | Before | Idle gap | Saturated gap |
|---|---|---|---|
| 1 | 2.42K/s, p50 418 µs (6 of 10 rounds); 523/s, 1.84 ms (4 of 10) · 2.19K/s, 434 µs | 5.78K/s, 168 µs (4 of 10); 3.34K/s, 180 µs (6 of 10) | 5.42K/s, 172 µs · 3.07K/s, 184 µs |
| 16 | 2.35K/s, 6.55 ms · 1.99K/s, 6.95 ms | 35.3K/s, 262 µs | 54.3K/s, 229 µs · 37.8K/s, 242 µs |

Before, a range read passed over half the chunk on average, 4 MiB, which at one in flight
takes about 400 µs at this device's single-stream rate; now it reads the header and one 64
KiB checksum block. At 16 in flight the reads before were bound by bytes, 16 reads of 4 MiB
at a time, and ran at 2.0–2.4K/s; reading apart they ran at 38–54K/s, 16 to 27 times as
many.

**2. The saturated gap is the right break-even here; the idle one wins only alone.**
4 KiB ranges of 1 MiB chunks:

| In flight | Before | Idle gap | Saturated gap |
|---|---|---|---|
| 1 | 4.94K/s, p50 135 µs · 7.63K/s, 129 µs | 5.96K/s, 129 µs | 5.41K/s, 168 µs · 1.14K/s, 172 µs (±419%) |
| 16 | 13.1K/s, 688 µs · 8.43K/s, 1.84 ms | 10.3K/s, 672 µs | 24.7K/s, 233 µs · 36.2K/s, 246 µs |

With a gap of about 1 MB, every range in a 1 MiB chunk is read through, as before, and at one
in flight that is the faster choice: reading apart adds an access, about 40 µs at the median.
At 16 in flight reading apart ran at 25–36K/s against 8–13K/s before and 10K/s with the idle
gap, whose reads spend the device on bytes no one asked for. The loss alone is bounded by one
access; the loss under load grows with the chunk. The idle gap also lost at 8 MiB and 16 in
flight, 35.3K/s against 54.3K/s, since it still read through up to 1 MB. This is the trade
docs/research/11 §8.4 derives, and why the store takes the break-even at saturation.

**3. Whole-chunk reads are unchanged.** A read from the start of a chunk needs its first
checksum block, so it is one read of the same bytes as before. Their throughput varied
between runs as the drive's state did, 3.15–10.7K/s for 1 MiB chunks at 16 in flight, low and
high in before and after runs alike.

**4. The drive's state moved between runs, less than the change.** Calibration's sequential
reads ran at 14.6, 8.23, 12.6, 9.90 and 12.7 GB/s in run order, and its durable 4 KiB write
at 4.33, 6.29, 4.33, 4.33 and 7.47 ms (docs/measurements/2026-09-29-chunk-store-states.md
describes the stalls). Both before runs found the slower reads, and before, range reads were
bound by bytes, so part of their distance from the after runs is the drive's: at most the
ratio of the rates, 1.8, against the 16 to 27 times measured. The last run's 1 MiB points at
one in flight spread ±346–419% and are given for completeness only.
