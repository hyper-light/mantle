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

**The change.** In a consolidation, a paid leaf whose path from the root still holds a pivot that
will flush (paid, or in a whole consolidation one a paid pivot lies below) waits for that flush.
While it waits:
- it keeps its rent;
- its in-flight bundles are left for the settle;
- the flush's arrival looks at it again.

It then settles once, over everything the consolidation brought it
(`Trunk::flush_to_come`).

**The test.** `tests/shard_db_workers.rs`
`a_whole_consolidation_settles_each_leaf_once` drains a four-level trunk on workers (fanout 3).
It asserts:
- the consolidation's paid settles are at most the leaves it leaves;
- they wrote at most the entries those leaves hold.

On e43e632 the same test failed with 65 settles over 21 leaves.

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

- Measured: §2's change at 100M and 10M against e43e632, interleaved. Results to follow in §5.
- Write-back with discard (§3) is the remaining lever the size of the gap to SplinterDB's large
  cache. Its design comes first: pages shared with workers without copies, dirty pages bounded by
  the cache and cleaned ahead of eviction, a checkpoint's write-back before its flush, a freed
  extent's dirty pages dropped, and a failed write's fence.
