# What the review's fixes cost the metadata path

**Question.** The fixes to defects the review of 2026-09-30 found (resolution ledger R10–R20)
put work on two metadata paths: a write now reads and writes its file's mark, a completion
marks each part it adopts, and the gateway's completion driver now pairs the listed parts
with the part rows in one walk instead of searching the list for each row. What does each
cost, or save?

**Method.** Release builds of three trees, run in turn on an Apple M5 Max under macOS 26.4.1:

- `75001d8`, before the fixes, with one line of `bench_meta.rs` corrected: its completion
  sent the ETag `whole`, which the Name range has refused since the completion checks of
  audit §16.5, so the benchmark had stopped at its first step, unnoticed. It now has a test
  that runs it (`bench_meta::tests::every_step_runs`), as `bench_log` does now as well.
- `78a9f1f`, before the pairing change, with the completion driver benchmark below adapted
  to that tree's types.
- This tree.

`mantle bench meta --seconds 1` applies each command over the model engine, a sorted map in
memory, on one core. `mantle bench gateway --seconds 1` times the completion driver alone:
the ranges' answers are made up front, and each page of part rows is a slice of rows already
read, so the time is the driver's pairing, ETag and file of parts. The machine was **not
idle**: other projects' builds held the load average at 15–19 throughout, so absolute times
are indicative only. The builds were run alternately, two or three times each, so load
falls on both alike, and the rows no fix touches (directories, files of extents) match
between builds within a few percent, which bounds what load did to the comparison.

## Findings

**1. The gateway's completion driver is 8.2 times faster at 10,000 parts.** Median of each
run, two runs each:

| Parts | `78a9f1f` | This tree |
|---|---|---|
| 1 | 584 ns | 625 ns |
| 1,000 | 250–251 µs | 131–132 µs |
| 10,000 | 13.9–14.0 ms | 1.69–1.71 ms |

The walk replaces a search of the listed parts for each row, some 10^8 comparisons at 10,000.
One part is 7% slower: the completion now carries a SHA-256 of its listing, which a retry must
match (R13), a fixed cost.

**2. The Name range's completion costs what it did.** Three runs each:

| Parts | `75001d8` | This tree, first fix | This tree |
|---|---|---|---|
| 100 | 54.3–64.5 µs | 60.4 µs | 55.3–58.4 µs |
| 1,000 | 590–737 µs | 655–672 µs | 590–639 µs |
| 10,000 | 6.55–7.73 ms | 7.34–7.47 ms | 6.55–7.34 ms |

The first fix for R20 read each adopted part's mark for its deadline, 10,000 point reads,
about 10% of the completion. A part's row now records its file's deadline and the completion
takes it from the row it already reads (row format 5, entry format 4), and the cost returned
to the old range.

**3. A session's entry of puts costs about 15% more.** An entry of 256 puts from one session:
328–442 µs before, 385–401 µs after, the steadiest runs 328–336 µs against 385–401 µs; one
put alone, 7.3–9.5 µs against 7.4–8.5 µs. Each put now reads its file's mark, to answer a
copy as its first delivery was, and writes it (R16, R20). The read is what recognising a copy
takes: nothing else says a file was taken, and the mark is what outlives the version and the
session. Its cost falls on the model engine here; on a device the mark shares the engine's
batch with the version's rows.

**4. The block sweep's checks by a row per block named: 155 times faster across files.**
`mantle bench meta`, this tree before and after the change, two runs after; files of 646 blocks,
the most a PUT's file names (audit B08):

| Check | By scanning the file's extents | By the file's row per block |
|---|---|---|
| One block of each of 512 files | 60.8 ms | 385–393 µs |
| One file's 646 blocks, 64 a page | 983 µs | 51.2 µs |
| 512 blocks of a 10,000-extent file | 1.74 ms | 50.2 µs |
| Writing a file of 10,000 extents | 1.70 ms | 2.95–3.01 ms |

A scan read every extent of the file for each check, and the sweep checks each file of a page
apart, so a page of blocks from distinct files cost one scan a block. The rows double a file
write's, once, where checks come for every page of due blocks (audit P05). Row format 6.

Rates on an idle machine remain to be recorded; these comparisons stand without them.
