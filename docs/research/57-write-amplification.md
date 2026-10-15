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

- Measured: §2's design at 100M and 10M against e43e632, interleaved. Results to follow in §5.
  The first A/B (r1711–r1713, 4fbe780) was stopped once its faults were found; its finished cases
  stand as 4fbe780's.
- Write-back with discard (§3) is the remaining lever the size of the gap to SplinterDB's large
  cache. Its design comes first: pages shared with workers without copies, dirty pages bounded by
  the cache and cleaned ahead of eviction, a checkpoint's write-back before its flush, a freed
  extent's dirty pages dropped, and a failed write's fence.
