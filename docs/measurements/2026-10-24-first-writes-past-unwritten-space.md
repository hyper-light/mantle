# First writes past never-written space

**Question.** Why did a cell of three answer its first put `Failed` on windows-aarch64, and does
calibration see the cost that caused it? The chunk store writes a volume once at format where
calibration finds a first-write penalty (docs/design/chunk-store.md §2), so the decision rests on
this measurement.

**What failed.** CI run 40097647853 (c056e26e), windows-aarch64 only: serve.rs's stated cell
answered its first object with a chunk, one byte, `Failed`; Linux, macOS and windows-x86_64
passed. A replica's chunk put waits one idle timeout (3.07 s) for its answer once the peer has the
request (hyper-transport `Progress`); with nothing from the peer for that long the exchange ends
`Lost(Stalled)`, the gateway counts it a refusal and offers the chunk to the placement's next
volume, and a cell of three has none spare (`PutError::Unplaced`).

**Method.** Scratch branch `diag/put-failed` (331538a8), diag run 40102138405: the serve suite as
CI runs it, three rounds on windows-11-arm, with every volume batch slower than 100 ms printed with
the bytes it spanned, and every calibration's first-write and overwrite points and decision. Two
arms: volumes as calibration decided, and every volume written whole at format whatever
calibration said.

| arm | rounds failed | volume batches over 100 ms | slowest (ms) | calibrations that chose prewrite |
|---|---|---|---|---|
| as calibration decided | 3 of 3 | 37 | 8,002, 7,959, 7,886, 6,550, 6,548 | 3 of 12 |
| written whole at format | 0 of 3 | 11 | 148, 139, 125, 124, 124 | (all, forced) |

All 25 batches of half a second or more (0.77 to 8.0 s) were volumes' first: each spanned bytes
33,554,432 to 100,671,488 or 100,737,024, the index log's first frame at 32 MiB and the volume's
first data at 96 MiB, past the unwritten rest of the log (chunk-store.md §2; a standard volume's
log is 64 MiB).

**Reading.** NTFS keeps a valid data length, and a write past it waits while the file system fills
the space before it with zeros (research/02 §4.7: the SetFileValidData remarks, KB 156932, and
`CcZeroData`). Calibration's first writes went one after another from the start of a fresh span,
each where the last ended, so none landed past unwritten space and NTFS charged them nothing: an
overwrite's throughput over theirs ran 0.76x to 1.24x on that runner, and the decision was the
noise's.
The extent conversion ext4 charges at the flush (2026-09-28-flush-cost-by-extent-state.md) shows
on any first write, so the sequential method saw ext4's cost and missed NTFS's.

**First change: first writes past a stretch.** Calibration made first writes as a volume does: one
small durable write a round, each at the end of its share of the scratch span (256 MiB over at most
six rounds, so past about 42 MiB never written), then the same writes over the places they wrote,
as two points, the penalty test unchanged. `mantle disk probe --measure`, three times alone on each
platform, in the directory the tests keep their volumes in (CI: diag run 40105172243, debug build;
this Mac and fedora: release build). The ratio is an overwrite's throughput over a first write's.

| platform | first write against an overwrite | decision |
|---|---|---|
| macOS 26.4, APFS, Apple SSD AP8192Z (this Mac) | 0.57x, 1.47x, 1.07x | not pre-written, 3 of 3 |
| Fedora, Btrfs on NVMe (fedora.attlocal.net) | 1.02x, 1.11x, 1.08x | not pre-written, 3 of 3 |
| macOS 15 aarch64, APFS (CI runner) | 2.6x, 2.21x, 1.70x | written once at format, 1 of 3 |
| Ubuntu 24.04 x86_64, ext4 (CI runner) | 4.6x, 5.5x, 4.1x | written once at format, 3 of 3 |
| Ubuntu 24.04 aarch64, ext4 (CI runner) | 8.1x, 8.5x, 3.6x | written once at format, 3 of 3 |
| Windows Server 2025 x86_64, NTFS (CI runner) | 54.5x, 74.0x, 71.4x | written once at format, 3 of 3 |
| Windows 11 aarch64, NTFS (CI runner) | 65.9x, 74.9x, 70.9x | written once at format, 3 of 3 |

Inside the serve suite, nine nodes calibrating a round beside the other tests' cells, the two
points did not part every time. On windows-11-arm 27 of 27 calibrations chose prewrite (diag run
40105172243); on windows-2025, 25 of 27 there and 20 of 27 in diag run 40112384706. The nine that did
not had measured 2.1x to 30.2x, and in eight of them the overwrites' interval had spread to ±80% or
more, once to ±142%, the load moving between the first writes' rounds and the overwrites'. In
40112384706 the volumes left unwritten took 1.1 to 2.2 s for their first batch, short of the put's
3.07 s only by the faster disk; prewritten volumes took up to 1.3 s beside the same load.

**Second change: pairs.** Each round now makes its first write and at once the same write again
over the place it wrote, a paired observation under one load, and the decision is the 95%
confidence interval of the rounds' log latency ratios lying wholly above zero
(`Calibration::first_write_penalty`; research/11 §13.3, from JAIN17). The profile's version went to
2, so every device's decision is measured again. A control on this Mac, every place written first
so each pair is two overwrites, gave 0.81x, 1.25x and 1.13x alone and 0.98x, 1.16x and 1.06x three at
once, none a penalty: a pair's order adds nothing measurable. Three probes alone and three at once
on each platform (CI: diag run 40114258424; this Mac and fedora, the latter at a load average of 46
on 32 cores); the ratio is the geometric mean of the rounds' first-write latency over the
overwrite's:

| platform | alone | three at once | decision |
|---|---|---|---|
| macOS 26.4, APFS (this Mac) | 1.03x, 1.38x, 1.45x | 1.35x, 1.5x, 1.6x | written once at format, 2 of 6 |
| Fedora, Btrfs on NVMe | 0.98x, 1.65x, 1.01x | 1.00x, 0.87x, 0.98x | not pre-written, 6 of 6 |
| macOS 15 aarch64, APFS (CI runner) | 2.24x, 1.43x, 1.46x | 1.70x, 2.11x, 1.83x | not pre-written, 6 of 6 |
| Ubuntu 24.04 x86_64, ext4 (CI runner) | 7.8x, 7.7x, 4.1x | 5.2x, 3.8x, 5.2x | written once at format, 6 of 6 |
| Ubuntu 24.04 aarch64, ext4 (CI runner) | 8.0x, 4.3x, 4.7x | 7.2x, 5.8x, 4.7x | written once at format, 6 of 6 |
| Windows Server 2025 x86_64, NTFS (CI runner) | 27.2x, 110.3x, 77.1x | 3.91x, 16.3x, 24.5x | written once at format, 5 of 6 |
| Windows 11 aarch64, NTFS (CI runner) | 130.6x, 107.3x, 93.4x | 68.2x, 79.3x, 50.7x | written once at format, 6 of 6 |

In the serve suite, all 27 logged calibrations on each Windows runner chose prewrite, their
intervals' lower bounds 1.09x at the least on windows-2025 and 9.1x on windows-11-arm, and three
rounds passed on each, 21 of 21 tests; the slowest volume batch took 397 ms on windows-11-arm and
572 ms on windows-2025. One volume on windows-2025 was still formatted unwritten: the cell of one
made by `mantle init`, whose profile init's own calibration had measured (its output is not
logged), a miss like the probe's 3.91x.

**Third: NTFS by its documentation.** A miss on NTFS costs a volume's first data write the fill of
the rest of its log, seconds on a slow disk, and NTFS's fill is documented rather than inferred
(research/02 §4.7). So a volume on NTFS is written once at format whatever the measurement says,
and on other file systems the measurement decides (`mantle_disk::profile::prewrite`): the
identification decides where its documentation does, and the measurement checks it, as CLAUDE.md
§5 has every device's plan derive from both.

**Reading.** The pairs find NTFS's fill and ext4's conversion alone and beside other calibrations,
NTFS's with a margin on windows-11-arm and close to the noise on windows-2025 under load, where its
disk fills faster; Btrfs's preallocated extents cost nothing. APFS on this Mac's SSD shows no penalty
alone and up to 1.6x with three calibrations writing at once, which two of three pairs' intervals
placed above one: the stalls a file's first writes put on its other transfers under load
(research/48 §2) are a cost there too, and a volume calibrated under load is written once at format.

**Open.** A replica's chunk put still waits one idle timeout for its answer whatever its device
takes; a durable write slower than that, from any cause, fails the put in a cell with no spare
volume (STATUS, item 8).
