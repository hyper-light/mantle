# 50. Seeks at scale: the trunk's shape, and what consolidation rewrites

Sources. Measured on this Mac (APFS, the internal SSD) on 2026-10-12/13 under the host's own load
(other benchmarks running), the ranges path (`benches/ranges.rs`), one shard, 16-byte keys,
100-byte values, seed 301, 64 MiB memtable, 256 MiB cache, seeks of 10 entries.
- **[instr]** `benchmark-results/mantle-gate1b-20261012/n1000000000-r1-rangesinstr`: dev bc9e910
  with scan counters, 1B puts, 1M gets, 1M seeks; beside a 100M batch.
- **[perquery]** `benchmark-results/mantle-perquery-20261013`: each query type in an arm of its
  own after the same fill (gets alone, seeks alone), consolidation as built and off
  (`--consolidation=never`), RocksDB 11.8.1 beside; 10M three rounds, 100M two.
- **[paid]**, **[shape]** `benchmark-results/mantle-seeks-20261013`: the same tree with counters for
  where a seek's sources come from (`charged_*`), which pivots seeks' rent paid for
  (`paid_leaves`, `paid_index`), the pivots a paid flush marked below it (`paid_below`), and the
  trunk's shape by depth after the fill's drain and after the seeks (`*_shape`).
- **[rocks]** `benchmark-results/mantle-gate1b-20261012/n1000000000-r1-rocks18`: RocksDB 11.8.1,
  18 background jobs, the same keys, values, seed, memtable and cache, `waitforcompaction` before
  its queries.
- **[src]** SplinterDB at `vmware/splinterdb@938676b`, `src/trunk.c` (the commit research/34
  cites): `leaf_might_need_to_split`, `leaf_split_target_num_leaves`, `leaf_split`,
  `leaf_split_init`, `trunk_node_receive_bundles`, `bundle_compaction_create`,
  `restore_balance_index`, `flush_then_compact`.

## 1. What a seek costs, and what consolidation does to it

Seeks alone after a fill, consolidation as built (seeks' rent buys it, research/43) and off:

| keys | consolidation | seeks/s | sources a seek | page reads a seek | entries rewritten during the seeks |
|---|---|---|---|---|---|
| 10M | as built | 153 K (median of 3) | 2.34 | 1.46 | 10.5 M |
| 10M | off | 75 K | 6.51 | 2.96 | 0 |
| 10M | RocksDB | 125 K | | | |
| 100M | as built [shape] | 51 K | 5.43 | 4.81 | 18.7 M |
| 100M | off [shape] | 36 K | 10.36 | 6.95 | 0 |
| 1B | as built [instr] | 2.5 K | 5.83 | 5.93 (63% from memory) | 570 M |
| 1B | RocksDB [rocks] | 10.4 K | | | |

At 1B the seek phase rewrote 570M entries, 0.9 of the 632M keys the fill left, for 1M seeks of 10:
1,115 pivot compactions, 218 leaf compactions and 116 s of the workers' merging, 278 µs of
process CPU a seek. dev's own 1B run rewrote 875M [gate1b `rangessegments6`]. RocksDB's queries
begin after `waitforcompaction`; at 1B its database is 102 GB against mantle's 168 GB, on a host
with 128 GB of memory.

## 2. Where the sources come from: the shape after a 100M fill

The trunk after the fill's drain, before any seek [shape], consolidation off:

| depth | nodes | pivots | branches | most a pivot | entries |
|---|---|---|---|---|---|
| 0 (root) | 1 | 2 | 10 | 5 | 2.4 M |
| 1 | 2 | 11 | 25 | 8 | 9.2 M |
| 2 | 11 | 77 | 190 | 3 | 35.2 M |
| 3 (leaves) | 77 | 77 | 77 | 1 | 37.1 M |

A seek opened 10.36 sources: 9.36 from index pivots, 1.00 from leaves. 56% of the entries sit
above the leaves, and every leaf holds one branch: no flush reached a leaf after the settle that
made it. Data leaves an index pivot only when its bundle passes `fanout` branches
(`flush_due`), whatever those branches hold, and a leaf is rewritten once it passes
`leaf_entries`, about one memtable's entries (`want_change`).

## 3. What the seeks paid for, and what was rewritten

Seeks pay each pivot they pass rent for its extra sources and a pivot is paid once the rent covers
its consolidation's price: its own bundle's entries and pages, plus, for an index pivot over a
leaf, the leaf's (`Trunk::charge`). A paid flush then marks every pivot of the child it lands in
paid (`flush_pivot`), so the consolidation of one index pivot rewrites its subtree:

| keys | index pivots paid | leaves paid | pivots marked by paid flushes | rewritten |
|---|---|---|---|---|
| 10M [paid] | 14 | 0 | 24 | 10.7 M (the whole store's worth) |
| 100M [shape] | 11 | 0 | 74 | 18.7 M |

No leaf earned its own consolidation: every payment was at an index pivot, priced at its own
bundle, and the rewrite was its subtree's. The marks are also undone midway: a seek after a paid
flush finds the pivots below with sources of a new shape, which voids their payment
(`SeekRent::shape`), so at 100M the consolidation emptied depths 0 and 1 into depth 2 (263
branches after it) and stopped there, the leaves untouched; at 10M the leaves it reached settled
by size, not as paid (`settle_entries_size` 6.09 M of the 10.7 M).

## 4. SplinterDB's rules [src]

- **Flushes.** `restore_balance_index` flushes each child its policy names (its eventual branch
  count past `F`, or its tuples past the routing filter's limit), then the fullest remaining child
  if it holds more than `incorporation_size_kv_bytes` (the memtable's size, `m`). Every pass
  through an index node pushes its heaviest pivot down: index pivots hold about `m` each, and the
  data lives in the leaves.
- **What a flush moves.** `flush_then_compact` hands the child the pivot's bundle and its live
  in-flight bundles by reference (`trunk_node_receive_bundles`), and the child restores its own
  balance before any compaction.
- **Leaves.** `leaf_might_need_to_split` is false until the leaf's bytes pass `m · F` (or its
  tuples the filter's limit). Then `leaf_split_target_num_leaves` asks for `round(unique bytes /
  m)` leaves, split at even byte boundaries from the branches' pivot statistics. A leaf that needs
  one leaf and has more than `F` branches is compacted whole in place. New leaves receive the old
  leaf's bundles by reference (`leaf_split_init`), and each one's first compaction is a full merge
  (`bundle_compaction_create`, `MERGE_FULL` for an empty leaf).

Mantle's trunk takes SplinterDB's bundles, flushes by reference and bundle compactions
(research/34 §1) but not these three rules: it flushes by branch count alone, flushes a pivot's
bundle only, and rewrites a leaf past `m` rather than `m · F`.

## 5. Step one: is the work needed at all?

- **Consolidation bought by seeks.** Needed: without it seeks open 2.8× (10M) and 1.9× (100M) the
  sources and run 2.0× and 1.4× slower. Not as built: the payments are an index pivot's, the work
  its subtree's, so what seeks buy is unbounded by what they paid. At 1B it is most of the store.
- **Index levels holding most of the data.** Not needed: they hold it because no rule moves it down
  until a pivot passes `fanout` branches. Each index level adds its pivot's branches to every seek.
- **Leaf rewrites at `m`.** SplinterDB lets a leaf reach `m · F` before rewriting it by size; in a
  10M fill mantle's size settles rewrote 6.1 M of 20.6 M entries [paid], at 100M 45 M of 300 M
  [shape].

## 6. Step two: how much?

- **A consolidation** should rewrite what its payment priced: the paid pivot's own bundle, flushed
  once into its child, or the paid leaf's bundle, settled to one branch. Each pivot is paid by its
  own seeks' rent, all but its dearest source: the one branch a settled leaf keeps and a flushed
  bundle becomes in its child.
- **Index levels** should hold what one pass through them leaves: at most about `m` in a pivot
  once its node has received data, as SplinterDB's fullest-child flush keeps them.
- **Leaves** should be rewritten by size only past `m · F`, into `m`-sized parts; below that, only
  past `F` branches, into one leaf.

## 7. Steps three to five: the arms

Each built as a variant of the same tree and run per query type (fill, gets alone, seeks alone)
against it, with consolidation off as a control and RocksDB beside, at 10M, 100M and 1B:
- **local**: seeks' rent pays each pivot for itself, priced at its own bundle; a paid flush marks
  nothing below (a drain's full consolidation still does).
- **stbe**: SplinterDB's rules: the fullest pivot of a node that has received data flushes past
  `m` entries, and a leaf settles by size only past `m · F`, by branches into one leaf.
- **stbe + local**.
