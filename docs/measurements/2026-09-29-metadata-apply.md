# Applying the metadata layers' heaviest commands

**Question.** What do the metadata layers' heaviest commands cost to apply, on the one core a
range applies on, and what did four of them cost before their changes (audit P02–P05)?

- Completing a multipart upload, whose listed parts are looked up by binary search since
  f422d1e (P02).
- An entry of many commands from one session whose history of answers is full (P04).
- A create or delete attempt learning a directory of many Name ranges and deciding whether
  they cover the bucket, then learning of one split (P03).
- The block sweep's check of a page of blocks against a file of the most extents (P05).

**Method.** `mantle bench meta --seconds 1`, release builds of the same tree with and without
the changes to sessions and the coordinator, run in turn, twice each, on an Apple M5 Max
under macOS 26.4.1, otherwise idle. Each step runs over the model engine, a sorted map in
memory, so the times are the layers' own work; an engine on a device adds its reads and
writes. Each is timed alone, its setup outside the timing, and repeated for a second and at
least five times; the tables give the median of all the runs of the two runs, and the
slowest. Completions list every part, each of 5 MiB; a session keeps 256 answers; the
directory's ranges divide the bucket's keys and come in an order other than theirs; the file
has 10,000 extents, each of its own block, and the page asks about its last blocks.

## Findings

**1. Completing an upload costs about 0.65 µs a part, 6.3–6.7 ms at 10,000.** Both builds,
all four runs:

| Parts | p50 | Slowest |
|---|---|---|
| 1 | 1.63–1.82 µs | 53–107 µs |
| 100 | 53.2–56.3 µs | 129–167 µs |
| 1,000 | 590–623 µs | 827–993 µs |
| 10,000 | 6.29–6.68 ms | 6.55–7.78 ms |

Linear in the parts: the binary search f422d1e brought replaced the audit's n(n+1)/2
comparisons, 50,005,000 at 10,000 parts. What remains is each part's row read and removed.

**2. A session's entry no longer rewrites its answers per command.**

| Commands in the entry | Before | After |
|---|---|---|
| 1 | 11.5 µs | 7.17–7.29 µs |
| 16 | 160 µs | 25.1 µs |
| 256 | 2.56–2.62 ms | 328 µs |

Before, each command decoded the session's 256 kept answers, searched them, and encoded and
wrote them back: at 256 commands that was 65,536 answers decoded and encoded for one entry.

**3. Learning ranges is no longer quadratic.**

| Ranges | Learn before | Learn after | A split, before | A split, after |
|---|---|---|---|---|
| 1 | 125 ns | 125 ns | 211 ns | 211–251 ns |
| 10 | 1.31 µs | 543 ns | 847 ns | 503 ns |
| 100 | 58.4–59.4 µs | 5.12 µs | 25.1–25.6 µs | 2.88 µs |
| 1,000 | 4.72–4.85 ms | 53.2 µs | 1.93–2.03 ms | 23.6–24.1 µs |
| 10,000 | 436–453 ms | 590 µs | 185–189 ms | 242 µs |

Before, each descriptor learned was looked up among those known and the list sorted again,
and each step of the coverage walk looked at every range: about R² log R and R². After, the
directory is gathered once by range, sorted once, and walked once.

**4. A check of blocks costs one read of the file's extents, whatever the page.** Checking
1, 64 or 512 blocks of a file of 10,000 extents took 1.64–1.70 ms at the median in every run,
and writing that file 1.70 ms. The cost is the scan of the file's extents, not the page: a
file whose due blocks come in k pages is scanned k times. No file names more than 646 blocks
(audit B08), so pages of that many blocks scan a file at most twice. An index of each file's
blocks would make a check a lookup per block for a row per block written with the file; with
pages of that size it would save at most one scan of a file, as long as writing it, and it is
not kept.
