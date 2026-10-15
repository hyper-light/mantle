# 57. Write amplification: what a 100M fill writes, and what it need not

Sources.
- **[runs]** `benchmark-results/mantle-splinterdb-20261013/runs`: the 100M rounds r1421–r1433
  (integ2), r1511–r1512 (hybrid1) and r1631–r1632 (corr2, e43e632), inline (`x_`) and queued
  (`n_`). Each run's `postfill` counters, taken once the fill's flush has run maintenance to its
  end, and its rusage (`oublock`: 512-byte blocks the process caused to be written, counted as
  pages are dirtied).
- **[diag]** r1701 `x_wadiag1`: e43e632 with each consolidation's settles counted by the leaf's
  first key and printed (md5 `08d90942`, a DIAGNOSTIC build, used for nothing else).
- **[sdb]** r1511 `sdb32` (32 GiB cache) and r1701 `sdb` (256 MiB cache): `splinter_bench` over
  SplinterDB 7d607b11, the same VM, disk and workload.
- **[cc]** SplinterDB `src/clockcache.c`: `clockcache_extent_discard` "will evict and discard
  dirty pages" of an extent whose references are gone; `clockcache_acquire_batch` writes back the
  batch `cleaner_gap` (512) batches ahead of the eviction hand; `core.c` writes back the dirty
  pages at a checkpoint (`cache_writeback_dirty`).

## 1. Where a 100M fill's bytes go

Every entry the trunk writes is counted once, by the work that wrote it: `entries_written` is
`pivot_entries` plus the leaves' settles by cause, exactly (r1632 x_corr2: 260,466,366 +
122,717,654 + 44,418,885 = 427,602,905). The packed memtables enter the root as in-flight
branches; the root's pivot compactions are their first write.

| run | written | pivots | settles, paid | settles, size | stalls | oublock |
|---|---|---|---|---|---|---|
| r1631 x_corr2 | 418.8M | 262.5M | 113.9M | 42.3M | 9 | 53.9 GB |
| r1632 x_corr2 | 427.6M | 260.5M | 122.7M | 44.4M | 22 | 58.8 GB |
| r1631 n_corr2 | 456.7M | 287.1M | 128.3M | 36.4M | 0 | 52.0 GB |
| r1632 n_corr2 | 464.0M | 292.9M | 129.5M | 36.7M | 0 | 52.6 GB |

Of r1632 x_corr2's 58.8 GB, 14.35 GB is the zeros its 34 segments were written whole with
(research/48 §5), and about 44.4 GB the trunk's pages. The run read 480 blocks from the device in
all: every compaction read its inputs from memory, the store's cache or the OS's.

## 2. A whole consolidation settled every leaf twice

The paid settles (cause `Seeks`) come from the drain's whole consolidation after the fill
(research/53 §11). At 10M (r1621–r1623, a trunk of three levels) they wrote exactly the live
entries, 6,319,178: each leaf once. At 100M they wrote about twice the 63.2M live entries.
[diag] counted them:

| consolidation | settles | leaves | a leaf's second settle | entries those wrote |
|---|---|---|---|---|
| 1, an empty trunk at open | 0 | 0 | 0 | 0 |
| 2, the drain after the fill | 144 | 71 | 73 | 66.5M of 127.2M |

At its start, a whole consolidation marks paid every leaf of several branches, and every index
pivot holding branches. A paid leaf settled at once, while the pivots above it waited for their
in-flight compactions before flushing. Each paid flush then landed in a leaf already settled,
paid it again (`flush_pivot`: a paid flush pays every pivot of its child), and the leaf settled a
second time. In a 100M trunk, root, two index levels and 74 leaves, the flushes come down in
waves, and every leaf took one after its first settle. The first settle's 60.8M entries were
merged again whole by the second.

**The change, as first made (4fbe780), and its faults.** A paid leaf waited while a pivot on its
path from the root would still flush in the consolidation, kept its rent, left its in-flight
bundles to the settle, and settled once the flushes landed. The drain then settled each leaf once
(63.2M entries at 100M against 124–127M), but two faults followed, both from index pivots still
flushing as soon as their own in-flight bundles were compacted:

- **A whole consolidation could end early.**
  - A flush could land in a leaf while a pivot higher on its path was still to flush, so the leaf
    waited.
  - That higher flush then landed in a node above the leaf, and nothing looked at the leaf again.
  - With no task out, the consolidation ended with the leaf unsettled and its rent unspent, and a
    second consolidation settled it.
  - Measured: 3 consolidations by the postfill counters in 2 of 4 runs of 4fbe780 at 100M, against
    2 in every run of e43e632.
- **Data could be left above the leaves.**
  - A pivot that had gone down was given data by a later flush from a pivot above it marked only
    for the descent, not paid. Such a flush pays nothing below it, so the data stayed.
  - Measured on 4fbe780 with every waiting leaf woken at each spend, so the first fault could not
    mask it: 5 of 10 runs of `a_drain_consolidates_whole_in_one_consolidation` (6 workloads each)
    ended with 5–14 entries at an index level. e43e632 ended none so, in 10 of 10.
  - There, every leaf's settle started at once, and a pivot never flushes into a child whose change
    has started, so the index pivots waited for the flushes above them by that ordering alone.

**The design kept.** A whole consolidation is a pass down from the root: in it, a pivot's flush
and a paid leaf's settle each wait while a pivot above it on its path will still flush
(`Trunk::flush_to_come`).
- Each pivot then flushes once, after every pivot above it, so no data lands in a node whose
  pivots have gone down.
- The pivots that will still flush on a path are those above its first that has flushed. The
  last to flush above a waiting node is its parent's, whose flush lands in that node and looks
  at it again.
- A consolidation that seeks' rent bought pays no pivot it flushes into, and is unchanged.

**The tests** (`tests/shard_db_workers.rs`).

| test | asserts | e43e632 | 4fbe780 | kept |
|---|---|---|---|---|
| `a_whole_consolidation_settles_each_leaf_once` | settles at most the leaves, their entries at most the leaves' | fails (65 settles, 21 leaves) | passes | 10 of 10 |
| `a_drain_consolidates_whole_in_one_consolidation` | one consolidation, ended whole; no index branch; every leaf one branch | 10 of 10 | fails (a second consolidation) | 10 of 10 |

Each test passes only with both waits.
- Without the leaf's, the first fails 10 of 10.
- Without the pivot's, the second fails (4fbe780's row).
- A wake of every waiting node at each spend, and payment by every flush carrying data, passed
  10 of 10 when each was taken out alone with the two waits in place; neither is kept.

## 3. What is not changed here, and why

- **The segments' zeros** (14.35 GB at 100M, one write per byte the store grows by). A first write
  stalls the file's other writers on ext4, APFS and NTFS (research/48 §4). On ext4 and XFS a write
  into an unwritten extent changes metadata the next flush must commit, which an overwrite does
  not (hyper-block `BlockFile::fills_new_space`). The zeros buy both.
- **Pages written and freed before any checkpoint needs them.**
  - SplinterDB's cache is write-back [cc]: a page reaches the device when the cleaner hand passes
    it ahead of eviction or at a checkpoint, and an extent freed while its pages are dirty is
    discarded, never written.
  - With a 32 GiB cache its 100M run wrote 11.6 GB; with 256 MiB, 49.5 GB, more than mantle's
    pages (44.4 GB).
  - Mantle's page cache is written through: every page a job builds is written as it is built.
    Its workers then read their inputs through their own handles on the file.
  - Doing what SplinterDB does needs workers to read pages that exist only in the shard's memory,
    which is a change to how pages are shared between the shard and its workers (open; §4).
- **The pivots' compactions** (2.6 entries a put at 100M). This is the size-tiered B^ε-tree's one
  write a level (research/34 §1): the root's, which is each memtable's first write, then one at
  each of two index levels.

## 4. Open

- The first A/B (r1711–r1713, 4fbe780) was stopped once its faults were found; its finished cases
  stand as 4fbe780's.
- Write-back with discard (§3) is the remaining lever the size of the gap to SplinterDB's large
  cache. Its design comes first: pages shared with workers without copies, dirty pages bounded by
  the cache and cleaned ahead of eviction, a checkpoint's write-back before its flush, a freed
  extent's dirty pages dropped, and a failed write's fence.

## 5. Measured: the pass down from the root (c2da2c3), and what each node waits for

[runs] r1731 at 100M and r1731–r1733 at 10M: c2da2c3 (`wa2`, md5 `a106fb38`) against e43e632
(`corr2`, md5 `c0f87536`), interleaved, inline (`x_`) and queued (`n_`); every case's recovery
exact.

| 100M, r1731 | fills/s | stalls | postfill flush | entries written | settled by consolidation | GB written |
|---|---|---|---|---|---|---|
| c2da2c3, inline | 3.27M | 1 | 3.47 s | 389.1M | 63.2M | 45.6 |
| e43e632, inline | 3.31M | 1 | 6.56 s | 455.1M | 133.2M | 55.7 |
| c2da2c3, queued | 1.82M | 0 | 3.06 s | 392.0M | 63.2M | 44.0 |
| e43e632, queued | 1.84M | 0 | 5.16 s | 466.4M | 133.9M | 53.3 |

| 10M, median of 3 | entries written | settled by consolidation | postfill flush, each round |
|---|---|---|---|
| c2da2c3, inline | 19.78M | 6.32M | 1.39, 0.56, 0.69 s |
| e43e632, inline | 19.35M | 6.32M | 1.47, 0.54, 0.66 s |
| c2da2c3, queued | 19.63M | 6.32M | 1.87, 5.16, 5.64 s |
| e43e632, queued | 21.30M | 7.17M | 0.98, 2.85, 1.62 s |

At 100M the drain wrote each leaf once and took 41–47% less time, and the run wrote 17–18% fewer
bytes, at the same fill (one stall or none in each arm). At 10M, a trunk of three levels, e43e632
already settled nearly every leaf once, so the pass had little to save there. It still made every
paid leaf and pivot wait while any pivot above it was to flush, holding pivots with nothing,
and the queued drain took longer in all three rounds.

**The refinement.** A node now waits only for a pivot above it that is to flush and holds data:
- in its bundle;
- in live in-flight bundles;
- in a compaction out.

A pivot holding nothing brings nothing down. Data comes down a path from a paid pivot (a whole
consolidation starts with no live in-flight bundle, its trunk idle), and a paid flush pays the next
pivot on the path, so the last flush above a waiting node still lands in it and looks at it again.
Both waits remain necessary:
- without the leaf's, `a_whole_consolidation_settles_each_leaf_once` fails 10 of 10 runs;
- without the pivot's, `a_drain_consolidates_whole_in_one_consolidation` fails 2 of 10;
- with both, both pass 10 of 10.

**Measured (kept: 91ddf13).** [runs] r1751–r1753, 91ddf13 (`wa3`, md5 `db11055d`) against
e43e632 at 100M and 10M, interleaved, with RocksDB 11.8.1 and SplinterDB in each 100M round; every
mantle case's recovery exact.

| 100M, median of 3 | entries written | settled by consolidation | GB written | postfill flush | CPU |
|---|---|---|---|---|---|
| 91ddf13, inline | 381.3M | 63.2M | 45.9 | 2.67 s | 175 s |
| e43e632, inline | 450.4M | 131.5M | 54.9 | 4.64 s | 195 s |
| 91ddf13, queued | 394.5M | 63.2M | 44.3 | 2.61 s | 195 s |
| e43e632, queued | 465.8M | 131.8M | 54.1 | 5.00 s | 208 s |

The paired ratios, round by round:
- entries written: 0.835–0.852;
- bytes: 0.808–0.854;
- the drain: 0.37–0.83, shorter in every round of both modes.

| 100M, median of 3 | fills/s | gets/s | seeks/s |
|---|---|---|---|
| 91ddf13, inline | 3.71M | 1.43M | 343K |
| e43e632, inline | 3.63M | 1.37M | 336K |
| 91ddf13, queued | 1.94M | 1.06M | 322K |
| e43e632, queued | 1.99M | 1.02M | 320K |
| RocksDB, 32 GiB cache | 759K | 212K | 70K |
| SplinterDB, 32 GiB cache | 1.11M | 356K | 44K |

At 10M (r1751–r1753) the queued drain took 0.48, 0.40, 0.38 s against e43e632's 0.56, 0.50,
0.48 s (c2da2c3's: 1.87, 5.16, 5.64 s), writing 20.48M entries against 21.33M; inline was
unchanged (19.82M both, 0.53 s both). Throughput at both sizes was within the rounds' spread of
e43e632's. The seek phase did identical work in every pair (sources opened, cache hits, no page
from the file or device), so its rates differed by the host alone.

## 6. The root's compactions copy a single memtable

The drain's cut leaves the fill's writes, where the device saturates, as they were (pivot entries
274.7M against 276.5M at 100M). [diag2] r1761 (91ddf13 with each pivot compaction's depth and
input branches printed, md5 `6b3d35a3`, DIAGNOSTIC) counted the pivot compactions of a 100M fill
by depth and inputs:

| depth | input branches | compactions |
|---|---|---|
| 0 (the root) | 1 | 309 |
| 0 | 2–3 | 30 |
| 0 | 4+ | 18 |
| 1 | 1 | 94 |
| 1 | 2–3 | 398 |
| 1 | 4+ | 107 |
| 2 | 4+ | 291 |

- **At the root.** 309 of the root's 357 compactions took one input: a packed memtable's branch,
  rewritten split at the root's pivots, its entries and their order unchanged.
- **Below the root.** The levels below merge two or more branches.

The entries those copies wrote are counted next (§7).

## 7. Step one and two for the root's copies

[diag3] r1771: 91ddf13 with every pivot compaction's depth, input branches and entries written
printed at its apply (md5 `ccd98886`, DIAGNOSTIC; recovery exact). Its prints sum to the run's
`pivot_entries` exactly (267,027,371).

| depth | input branches | compactions | entries written | of pivot writes |
|---|---|---|---|---|
| 0 (the root) | 1 | 303 | 78.8M | 29.5% |
| 0 | 2–3 | 12 | 7.1M | 2.7% |
| 0 | 4+ | 34 | 13.1M | 4.9% |
| 1 | 1 | 154 | 3.5M | 1.3% |
| 1 | 2–3 | 442 | 66.4M | 24.9% |
| 1 | 4+ | 45 | 15.7M | 5.9% |
| 2 | 1 | 14 | 0.3M | 0.1% |
| 2 | 2–3 | 25 | 3.2M | 1.2% |
| 2 | 4+ | 336 | 72.5M | 27.2% |
| 3 | 4+ | 15 | 6.6M | 2.5% |

**Need it at all?** A root compaction with one input branch reads the branch its packing just
wrote and writes, for its pivot, the entries of that branch in the pivot's range, in the same
order: it merges nothing and drops nothing. Had the packing ended a branch at each of the root's
pivot keys, each pivot's would already be written.

**This much?** 78.8M entries, about 21% of the 374.0M the run wrote and 0.8 a put. Every one is
written in the fill, the phase whose writes saturate the device (§1).

**The design.**
1. The root's pivot keys go out with a memtable's packing, and the packing ends a branch at each.
2. On its return, with the root's pivots as they were, each part is its pivot's newest bundle
   branch. That holds only when no pivot taking one has a live in-flight bundle, a compaction out,
   or a child that will split, which is the case in which that pivot's compaction would have had
   this branch alone for its input.
3. Otherwise the parts enter as one in-flight bundle and are compacted as now. The root's merges of
   several memtables (20.2M entries) are real merges and stay.
4. Reads find the same bundles: one branch a memtable for each root pivot, until the pivot flushes.

**The change** (`Trunk::add_parts`, `Trunk::root_cuts`, `Sorter::ends_below`).
- A worker's packing gets the root's pivot keys with its job and ends a branch at each.
- Pending memtables are kept as groups, so the root's room counts memtables. A get probes the
  branch of its key's range, and a scan takes every branch as a source.
- On incorporation, a group whose cuts are the root's pivot keys, where no pivot taking a branch
  has a live in-flight bundle, a compaction out or a child to split, puts each branch in its
  pivot's bundle as its newest. Any other group enters as one in-flight bundle.
- Each branch begins an extent, so the packing's grant is one extent larger a cut.
- A packing with cuts takes room in the shard's cache, as a compaction does, and keeps its pages
  there as it writes them. A group whose branches go into the bundles has those pages adopted, as
  a compaction's are. A group that goes in flight gives the room back unread, its compaction's
  pages being the ones to cache. Any failure gives it back.
- The shard's own packing, with no worker, still writes one branch.

**Its first form (01f1f7d) and its fault.** It kept no packed page in the cache. A branch placed
whole was then read from the device by any get that reached it:
`a_workers_pages_are_cached_where_they_were_written` failed in CI on linux-x86_64, linux-aarch64
and macos-aarch64, and locally in 14 of 20 runs, with 1–3 device reads, each a get of a key in a
branch left in the root's bundles. With the room taken, it passed 20 of 20.

**The tests** (`tests/trunk_test.rs`, deterministic: a simulated file, no worker).

| test | what it runs | what it asserts |
|---|---|---|
| `batches_packed_at_the_roots_pivots_enter_its_bundles_as_they_are` | each batch packed at the root's pivot keys | every batch taken whole once the root is an index node (`parted_entries` = those batches' entries) |
| `batches_packed_at_other_keys_enter_in_flight_and_are_compacted` | every batch's cuts drop one of the root's keys | none taken (`parted_entries` 0) |

- Both check, after every batch, every key's newest value and the store's extents against the
  trunk's.
- Placing each branch in the next pivot fails the first test's reads.
- Not checking the cuts against the root's pivot keys fails the second's.
- Never placing fails the first's count.

`tests/shard_db_workers.rs` `a_packing_taken_into_the_roots_bundles_is_cached_where_it_was_written`,
with workers and a cache:
- It runs a workload to the end of its maintenance, then flushes a memtable of twenty keys written
  nowhere else, whose packing finds the root idle.
- It asserts all twenty entries are taken whole, and that their gets (no memtable kept, so each
  reaches the root's branch) read no page from the device.
- Giving the room back instead of adopting it fails it (3 device reads), and so does a packing
  taking no room (2). Never placing fails its count.

## 8. The page cache's per-page work on the shard: a grant and an adoption

**What the trace showed.** The fill's slowest puts lie inside the range task's long "taking worker
results" sections:
- At 100M they took 6–42 ms; at 10M, 5–7 ms.
- Diagnostic: the scalar event trace on wa-wr 667de8f, branch trace-wr.
- Runs: `diag-runs/n100000000-r1901..r1902-t_trwr1` and `n10000000-r1901..r1904-t_trwr1`.

A frame-pointer profile of the traced build (`prof/ab3-tr100m`, two rounds) was cut to those
sections by the trace's base on CLOCK_MONOTONIC.
- Inside them the shard was sampled at 0.48 of the time, against 0.50 over the whole fill: it ran
  there, it was not descheduled.
- Round 1: of 143.1 ms sampled, `Cache::adopt` took 64.5 ms. The trunk's dispatch took 59.5 ms,
  almost all in `Store::grant`'s forgets.
- Round 2: 67.0 and 55.0 ms of 138.6 ms.
- A worker's compaction comes back with pages written into memory the cache lent it. Before it
  is applied:
  - every page of every extent granted it was forgotten from the cache, one map probe a page;
  - its result's adoption forgot each page again, looked it up in the ghost, then mapped it.

**Counted** (diagnostic counters, trace-wr dbfa3af; `n100000000-r1911-t_trwr2`,
`n10000000-r1911..r1912-t_trwr2`, whole runs):

| per-page work | 100M probes | found a page | 10M probes (two runs) | found |
|---|---|---|---|---|
| a grant's forget | 16,542,944 | 5,741,144 (35%) | 1,260,480 / 1,259,840 | 89,306 / 44,211 (7%, 3.5%) |
| an adoption's forget | 12,048,789 | 0 | 643,605 / 625,043 | 0 / 0 |
| an adoption's ghost lookup | 12,048,789 | 0 | 643,605 / 625,043 | 0 / 0 |
| evictions an adoption ran | — | 0 | — | 0 / 0 |
| the paced forgetting | 5,140,416 | 4,311,854 (84%) | 552,832 / 576,704 | 354,780 / 381,307 |

**Step one: is the work needed at all?**
- An adoption's forget: no. Every page a job writes lies in an extent granted it, and the grant
  forgot whatever the cache held there; none of 12.0M found a page. The map's insert returns the
  mapping it replaces, so a page found there would be released in that same probe.
- An adoption's ghost lookup: not while the ghost is empty. The ghost holds only addresses the
  cache evicted, and at 100M and 10M it evicted none. Empty, it names this address no more than
  any other; holding entries, the lookup stays and the rule is unchanged.

**Step two: does a grant need this much?** It needs the 35% (100M) and 3.5–7% (10M) of its probes
that found a page: dead branches' pages, which hold the cache's memory until forgotten. Only an
extent whose forgetting is pending can have any. The cache holds pages read or written through
extents held, and a released extent enters the forgetting at once. One bit an extent carries that
from the release:
- a grant forgets the extents whose bit is set, and takes the bit;
- the paced forgetting passes over an extent whose bit is gone, as it already passed over one
  held again.

**Tests.**
- `store::cache` `an_adopted_page_replaces_the_page_its_address_held`: an adopted page displaces
  an older page at its address in the one probe. Without releasing the displaced slot it fails:
  five pages live where four should be.
- `store::cache` `an_adopted_page_at_an_address_the_ghost_names_enters_main`: an empty ghost puts
  adopted pages in small; one the ghost names goes to main, as before.
- `tests/store_grant_pressure.rs` `a_freed_extents_pages_leave_the_cache_once_whoever_reaches_it_first`:
  - the paced forgetting reaches a freed extent first, then a grant of it finds nothing cached;
  - a grant reaches one first and forgets its pages;
  - an extent freed twice before the forgetting is forgotten once, its newest pages.
  - Each check reads through an injected device fault, so a stale cached page would answer. A
    grant that skipped a pending extent, or a forgetting that skipped every extent, fails it.
