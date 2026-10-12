# 43 — Read accelerators at scale: what views and maplets cost, and what others do

**Status:** research input for `docs/design/engine-structure.md` §5–6 (views, maplets) and the
store's allocator (§E1). This is not a decision record; the decisions it supports belong in the
design.
**Compiled:** 2026-10-11.
**Scope:** what REMIX views and maplets cost mantle at 100M keys and where; what RemixDB,
SplinterDB, DiscoDB, Chucky, SlimDB and GRF build, when, on which thread, and at what cost; the
literature on building optional indexes on demand; and the A/B of the designs that follow. Each
measurement names the directory that holds its raw output.

Marks: **[paper]** a paper's statement (section, figure, table); **[code]** what a repository's
source does at a pinned commit; **[README]** a maintainer's unreviewed claim; **[plot]** read off a
figure; **[inferred]** arithmetic or inference with its inputs; **[mantle]** mantle's own code or
measurement.

---

## 1. What they cost mantle [mantle]

Ranges path (`benches/ranges.rs`, one shard, the client on it), 100M puts then 1M gets and 1M
seeks of 10, 16-byte keys, 100-byte values; traces from temporary instruments
(`benchmark-results/mantle-pacediag-100m-20261011`, `pacediag*.patch`).

| run | fill | view builds | maplet builds | share of fill |
|---|---|---|---|---|
| traced, host load ~16 | 202–238 s | 60.0–62.9 s | 60.6–65.2 s | 52–62% |
| traced | 82 s | 18.0 s | 0.1 s | 22% |

- Nothing reads either structure during a fill. Which bundles get them, and how many maplets a trial
  declines after building (306 in one run), varies with timing: 3.7× the view work and 290× the
  maplet work between runs at the same per-unit cost (~100 ns an entry for views, 30–54 ns a hash
  for maplets).
- On the async path a view is always built from scratch (`Job::Build`, since a rebuild's merge-point
  reads cannot be readied ahead); a root pivot's view is rebuilt after every rotation from every run,
  1.1M → 4.5M entries over 7 rotations, a quadratic total.
- Turned off entirely (`DIAG_NO_ACCEL`), the 100M fill plus its flush took 58 s instead of 82–112 s;
  gets were unchanged (they use filters, never views) and seeks moved within run-to-run noise.

Microbenchmarks (`benches/remix.rs`, `benches/maplet.rs`):

| | 2 runs | 4 runs | 8 runs |
|---|---|---|---|
| seek+10 through a view vs merging the runs | 2,750 vs 2,500 ns | 5,042 vs 5,375 ns | 6,125 vs 11,083 ns |
| full view build | 29.7 ms | 77.2 ms | 186 ms |

- A full build spends 46% of its time comparing keys: every entry scans every run's head
  (O(runs) per entry, where a loser tree is O(log runs)).
- A rebuild of a run 1/100 the old size reads ~10,000 pages (one key read per halving of each
  merge point's search); 28% of its time walks the old selectors.
- In-memory maplet routes against the bundle's Bloom filters: present keys 23.7 vs 7.1 ns at 2
  branches and 49.9 vs 34.1 ns at 8; absent keys 23.0 vs 16.5 ns at 2 and 29.3 vs 59.5 ns at 8.

Seeks at 100M spend ~62% of the shard's time on page access (`mincore` 23–26%, `pread` 22%,
copies 14%; `read_page_paced` → `read_resident`), with views or without: a view seek still
positions a cursor in each run, so it reads as many pages as a merge.

## 2. What the others build, when, and why [paper, code]

(From `scratchpad/research-views-maplets.md`, the full survey with citations; summarized.)

- **RemixDB** builds a partition's REMIX at every compaction that adds tables, on its compaction
  workers; writers never build. Its rebuild is a linear two-way merge driven by the old view's run
  selectors over compact key-only copies (CKBs, `msstbc`, sst.c:3242); binary merge points only
  past an old:new ratio of 5 + old tables (sst.c:3214). Its README reports the CKBs doubled random
  load (0.50 → 0.96 MOPS): REMIX rebuilding was about half the load time before them. Its 4 GB
  memtable keeps the number of rebuilds small (GRF §3.2: "an unusual setting").
- **SplinterDB** merges a pivot's routing filter (its maplet) after each bundle compaction, as a
  background task: the old filter plus the new branch's fingerprints, made by the data compaction
  (`routing_filter_add`, routing_filter.c:338; trunk.c:3872). Fixed 26-bit fingerprints keep an old
  filter re-splittable. Its header warns a background task run on a foreground thread can take
  "many milliseconds" (splinterdb.h:125).
- **DiscoDB** updates its index from the compaction's history without reading the runs; still loads
  ~50% slower than with no index.
- **Chucky, SlimDB, GRF**: global key-to-run maps; GRF measured Chucky's insert path about equal to
  per-file Bloom filters and run-ID updates the slowest.
- **They build eagerly because their structure is mandatory**: RemixDB's tables carry no index or
  filter (REMIX §4.1), SplinterDB's maplet replaces per-branch filters. Mantle's views and maplets
  are optional beside branch filters and indexes, so that reason does not hold for it.
- **On demand:** Bourbon (OSDI '20 §4.4) learns a file only after waiting as long as learning takes
  and only if the expected lookups repay it (the 2-competitive ski-rental rule); adaptive indexing
  charges construction to the queries that need it (cracking, CIDR '07; adaptive merging, PVLDB
  2011); RocksDB builds cheap filters for short-lived levels.

## 3. The A/B of the designs [mantle]

`benchmark-results/mantle-acc-variants-20261011` (BUILD.txt names each arm's commit): main, the
space fix alone (the base of the rest), and on it: none (no views, maplets or hash lists); maplets
bought by gets' measured rent; SplinterDB-style filters (no Bloom filters, each branch's own maplet
in its pages, bundle maplets merged on workers at every bundle change, read through the cache,
saved in the image); views bought by seeks' rent; one seek-rent account buying the cheaper of a
view and a consolidation. Three interleaved rounds at 1M, 10M and 100M, RocksDB 11.8.1 beside.

Medians of three rounds (ops/s; `benchmark-results/mantle-acc-variants-20261011`):

| 10M | main | base | none | maplets gated | SplinterDB filters | views, own rent | views, one rent | RocksDB 18 |
|---|---|---|---|---|---|---|---|---|
| fill | 1.46M | 1.53M | 2.11M | 2.15M | 1.72M | 1.65M | 1.94M | 1.04M |
| read | 482K | 572K | 641K | 523K | 542K | 638K | 612K | 308K |
| seek | 144K | 183K | 225K | 151K | 149K | 121K | 112K | 126K |

| 100M | main | base | none | maplets gated | SplinterDB filters | views, own rent | views, one rent | RocksDB 18 |
|---|---|---|---|---|---|---|---|---|
| fill | 1.57M | 1.19M | 2.10M | 2.11M | 1.90M | 2.04M | 2.25M | 1.20M |
| read | 174K | 285K | 438K | 198K | 354K | 430K | 449K | 284K |
| read p99.9 µs | 139 | 135 | 7.3 | 17.4 | 9.7 | 8.5 | 7.0 | 10.5 |
| seek | 38K | 43K | 63K | 54K | 59K | 53K | 47K | 74K |
| seek p99 µs | 49 | 80 | 34 | 46 | 36 | 147 | 156 | 22 |

At 1M the arms are within a few percent of each other, except the SplinterDB-style filters'
reads (2.03M against 2.35-2.40M: a get reads its filter's page where a Bloom filter answers from
memory).

- **No accelerator wins on balance**: against the base, +76% fill, +54% reads, +47% seeks at
  100M, the best seeks and seek tails of every arm, and read p99.9 from 135 to 7 µs.
- **Bought on demand, an accelerator costs the phase that buys it**: maplets bought by gets cost
  the reads (198K), views bought by seeks cost the seeks (47-53K, p99 147-156 µs).
- **SplinterDB-style filters** keep filter memory with the cache budget instead of ~1.25 B a key
  resident, for a get's filter page read: 19% of the reads at 100M. Whether that pays at 1B is
  measured in §4.
- One account buying the cheaper of a view and a consolidation reads and fills best at 100M but
  seeks worst; seeks are mantle's weakest axis against RocksDB, so it does not win.

## 4. At 1B keys

The finalists at 1B (one round, the same keys, values, seed, memtable and cache;
`n1000000000-r1-*`): none, one seek-rent account, and RocksDB at 18 jobs. Main without the space
fix cannot fit on the disk (5× space amplification at 100M) and was not run.

| 1B | none | views, one rent | RocksDB 18 |
|---|---|---|---|
| fill | 1.90M | 1.09M | 938K |
| read | 31K | 20K | 19K |
| read p99 µs | 125 | 246 | 1,062 |
| seek | 3.4K | 3.4K | 7.4K |
| seek p99.9 µs | 7,209 | 8,651 | 2,188 |

- At 1B the views' build cost grows past their gain: fill 1.74× slower than none for no seek gain.
- The SplinterDB-style arm failed within two minutes of its 1B fill ("maplet hashes out of
  order"). Its maplet task read a bundle's branches on a worker while a flush and the child's
  compactions dropped them; with extent reuse their extents were rewritten under it
  (`msplinter-1b-diag`). 100M never hit the race. Fixed in the arm as SplinterDB's tasks do,
  each holding a reference to what it reads (acc-msplinter C6).
- Fixed, it filled 1B keys and then could not checkpoint: each maplet's block index rides in its
  branch's descriptor, and the trunk image outgrew the 66 MB its header page can name. SplinterDB
  keeps its filter's index in pages, a page read more a probe. Rather than build that to rank the
  arm, it ran with checkpoints skipped (`--no-checkpoint`, both arms, `n1000000000-r4-*`): its
  indexes in memory, an upper bound on the paged design.

| 1B, no checkpoints | none | SplinterDB filters, indexes in memory |
|---|---|---|
| fill | 1.58M | 1.12M |
| read | 35.6K | 34.9K |
| read p99 µs | 135 | 184 |
| seek | 3.77K | 7.84K |
| seek p50 µs | 188 | 123 |
| seek p99.9 µs | 3,015 | 2,490 |
| peak bytes | 146 GB | 123 GB |
| entries rewritten during the queries | 559M | 133M |

- The SplinterDB arm loses fill at every size (−18% at 10M, −10% at 100M, −29% at 1B) and reads
  at every size below 1B (−15%, −19%), even at its bound.
- Its 1B seeks are twice none's, but a seek reads no maplet (scan.rs merges a cursor a branch; a
  bundle's maplet routes only gets). What differs is the trunk the queries ran on: none's query
  phase rewrote 559M entries alongside the seeks, the SplinterDB arm's slower fill had done most
  of its rewriting before them (133M during). That rewriting's cost lands on seeks: 182 µs of
  process CPU a seek against 55, 12.7 context switches against 3.7, 82% of page reads served
  from the OS cache against 91%. It is fixed in none directly, not by maplets.
- **Verdict:** none, on balance at every size. Views, maplets and the branches' hash lists are
  removed.
- Seeks are mantle's gap at 1B (0.46–0.51× RocksDB). The phase is not seeks alone: maintenance
  rewrote 546–559M entries during each none query phase (seek-paid consolidations in r1, other
  trunk work in r4), while RocksDB's queries start after `waitforcompaction`. Mantle's database
  was 146–154 GB against RocksDB's 102 GB, which this machine's 128 GB mostly caches. Where
  maintenance lands, space amplification and the page access path (§1) are the next targets.

## 5. Where a 1B seek's time goes [mantle]

The final layout (no views, maplets or hash lists) at 1B, checkpointed as the bench runs it, with
temporary counters and a 30-second `sample` of the seek phase
(`benchmark-results/mantle-seek-1b-20261012`, `seekdiag.patch`, `run-measured`):

| 1B seek phase | |
|---|---|
| seeks | 3.72K/s; p50 205 µs, p99.9 4.3 ms |
| sources a seek opens | 10.0 (most 8-10 or 14-15) |
| page reads a seek | 10.1: 8.5 the OS held (`mincore` and `pread` on the shard), 1.5 from the device, 0.5 from mantle's cache |
| the shard thread, sampled | 78% parked in `kevent` for a device read's completion, ~20% running (most of it `mincore` and `pread`) |
| maintenance during the reads / seeks | none / 79.5M entries rewritten by 13 seek-paid consolidations |

A seek positioned its sources one at a time (`ScanMerge::open_async`): each source's leaf read,
when the device served it, was awaited before the next source's started, so a seek waited one
device read for every source the OS did not hold, in turn. Started together, the reads overlap: a
seek waits about as long as its slowest one. The issuer's attachment bounds how many are out at
once; past it, a read is handed over as an earlier one lands.

