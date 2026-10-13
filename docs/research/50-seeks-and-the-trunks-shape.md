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
- **[cache]** `benchmark-results/mantle-arms-20261013`: the arms, and the cache's variants with
  256 MiB and 32 GiB of cache beside RocksDB with both (§9).
- **[contig]** `benchmark-results/mantle-contig-20261013`: the price-fixed arm, the layout's steps
  and the gate at 1M, 10M, 100M and 1B, each beside RocksDB sized as it is (§10 to §12).
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

## 8. How fast a consolidation runs

At 1B the seek phase's consolidations merged for 116 s of worker time over the 405 s phase
[instr]: 0.29 workers busy on average, on a host with 18 cores. The pool admits a job past its
measured need only when the shard waits on it (`Trunk::admit`: a pending branch, a root without
room, a step that waits); otherwise it admits as many as the last period kept busy, `busy /
period` by Little's law (`pool.rs`). A consolidation starts a period (`start_consolidation` calls
`period_started`) after a seek phase that kept no worker busy, so its need measures 1, and a
consolidation run one job at a time measures 1 again: the cap measures itself. Little's law sizes
a pool to a steady arrival of work; a consolidation is one batch, every seek pays rent until it is
done, and SILK gives such deeper work the cores the foreground leaves (research/34 §5).
- **fastcons**: a consolidation in progress admits its jobs within the cores, as a waiting shard
  does.
- **popenfast**: popen and fastcons together.

## 9. A cache sized to the machine

Sources. **[cache]** `benchmark-results/mantle-arms-20261013`: `stbelocalfast` (seek-stbelocalfast
58a836b), `slfc` (9542868: the cache in chunks made as reads want slots), `slfzc` (slfc with the
cursor reading leaves in place in the cache), `slfm` (73ac148: the shard's memory sized from the
machine, `--machine-memory`); each with 256 MiB of cache and with 32 GiB (`…big`), RocksDB 11.8.1
beside with both; 10M three to six rounds, 100M one to four.

The cache reserved its slots up to its limit as reads came. With 32 GiB at 10M the gets made 7.74
allocations, 31.7 KB of allocation and 1.97 page faults each, and ran at 292 K/s against 678 K/s
with 256 MiB: a larger cache was slower. The cache now takes memory in chunks as reads want slots,
each chunk at most `TUNE_STEP_PERCENT` of the limit (the memory tuner's step, Luo & Carey, PVLDB
2021 §5.4), a read with no free slot making its own, and a chunk only while the machine has twice
its bytes free; a lowered limit gives back whole chunks, the last first.

| 10M | gets/s | allocations a get | faults a get | seeks/s |
|---|---|---|---|---|
| reserved to the limit, 32 GiB | 292 K | 7.74 | 1.97 | 211 K |
| chunks, 32 GiB | 771 K | 0.00 | 0.06 | 222 K |
| chunks, leaves read in place, 32 GiB | 801 K | 0.00 | 0.06 | 231 K |
| RocksDB, 32 GiB | 326 K | | | 172 K |

Sized to the machine (`ShardDb::follow_machine`: the machine's effective capacity and the memory
available now, from hyper-rt's facts, research/36's tuner over it): at 10M 2.35 M puts/s, 685 K
gets/s and 209 K seeks/s; at 100M (one round) 2.09 M, 412 K and 84 K against RocksDB with 32 GiB's
989 K, 244 K and 73 K. A 100M run with 32 GiB fixed, no machine gate, filled at 1.07 M with one
put of 1.1 s; not explained.

## 10. A consolidation's price, in the seeks' units

Seeks pay a pivot rent in wall time, the nanoseconds their extra sources cost them, and the
pivot's consolidation is bought once the rent covers its price (§3). The price was its merging's
CPU time on one core. The merging runs on the maintenance workers idle when it is bought, beside
the seeks, so what it costs them is that work spread over those workers: compared with a
wall-time rent, a CPU-time price overcharged every consolidation by the idle workers' count. At
10M, 1M seeks over 8 leaves of two branches each bought none. The price is now the merging over
the idle workers (`wall_price`, `Pool::idle`).

**[contig]** `benchmark-results/mantle-contig-20261013`, one interleaved pass (10M three rounds,
100M two, the host as it was), each arm sized to the machine: `slft` (seek-stbelocalfast 0d703db,
the control) and `slfp` (the same with the price fixed, seek-slfp 8658da5), RocksDB with 256 MiB
and 32 GiB beside:

| keys | arm | puts/s | gets/s | seeks/s | sources a seek | rewritten during the seeks |
|---|---|---|---|---|---|---|
| 10M | control | 2.06 M | 611 K | 193 K | 2.96 | 2.8 M |
| 10M | price fixed | 1.91 M | 640 K | 282 K | 1.36 | 9.1 M |
| 10M | RocksDB 256 MiB / 32 GiB | 785 K / 871 K | 266 K / 295 K | 97 K / 151 K | | |
| 100M | control | 1.36 M | 311 K | 60 K | 6.56 | 14.6 M |
| 100M | price fixed | 1.75 M | 378 K | 110 K | 2.07 | 91.0 M |
| 100M | RocksDB 256 MiB / 32 GiB | 762 K / 773 K | 204 K / 200 K | 74 K / 65 K | | |

Seeks rose 1.46× at 10M and 1.82× at 100M; what they bought was rewritten during their own phase.
The fills run the same code in both arms: their spread, 1.27 M to 2.02 M puts/s across the
control's 100M rounds, is the host's.

## 11. A layout contiguous by key: buffers bounded by size, flushes that carry

Index pivots flush past `fanout` branches whatever those hold, so at 100M a third of the entries
sit in index buffers after the fill (§2), and a seek opens a source in each level it passes. Two
changes were measured against it [contig]:

- **Index pivots flushed past a memtable's entries** (`contig1`, seek-contig bd8804b, on the fill's
  own path; SplinterDB's fullest-child flush past one incorporation, §4): at 10M the entries above
  the leaves fell from 41% to 7% and gets rose 9%, but seeks fell 5%: the runs moved into the
  leaves rather than merging, 1.88 branches a leaf against 1.25, and each put wrote 1.72 entries
  against 1.47. At 100M the fill fell to 0.81× (1.64 M against 2.02 M puts/s, one put 1.5 s) and
  seeks to 0.82×: flushes for size set off compactions the fill's maintenance waited behind.
- **The same in idle time only** (`contig3`, seek-contig 472667e: `Trunk::balance_step`, owed once
  nothing else is): the fill matched the price-fixed arm's (10M 1.97 M against 1.91 M, 100M
  1.73 M against 1.75 M), gets at 10M too (646 K against 640 K), but at 100M gets fell to 0.75×
  (284 K against 378 K) and seeks to 0.90× (99 K against 110 K). Its gets ran fewer instructions
  (16.0 K against 17.5 K a get) in more cycles (14.2 K against 11.1 K), with more kernel time
  (1.19 µs against 0.90 µs) and a longer tail (p99.99 160 µs against 36 µs), the page reads the
  same (0.57 a get). Not kept.
- **A full pivot carries its live in-flight branches down** (seek-slfpc a94d32b, on the
  price-fixed arm): a pivot holding `fanout` branches compacted each in-flight arrival into the
  branch that filled it, and the flush that followed carried that branch down to be merged again;
  such a pivot now flushes at once, the in-flight branches uncompacted and named by both nodes'
  lists until each lets them go (SplinterDB flushes a pivot whose eventual branch count passes F,
  `restore_balance_index` [src]). Where nothing else flushes full pivots (`contig3`), such merging
  was 0% of the pivots' merging at 10M and 9 to 11% at 100M (`passthrough_entries`). On the
  price-fixed arm, whose fullest-pivot flush (§4) empties pivots first, the carry took 36 to 87
  branches a 100M fill, the entries written fell 0.3% (3.34 against 3.35 a put), and the fill,
  gets and seeks were within the host's spread (1.66 M against 1.59 M puts/s, 327 K against 326 K
  gets/s, 91.7 K against 92.4 K seeks/s, three rounds each). Not kept.

Merging data once, at the leaf, is not open to a Bε-tree: a buffer's merge at each level is what
lets one leaf rewrite absorb many arrivals (Brodal and Fagerberg, "Lower bounds for external
memory dictionaries", SODA 2003; Bender et al., "An introduction to Bε-trees and
write-optimization", ;login: 40(5), 2015). Carried uncompacted to where they rest, in-flight
branches would either stay as sources at every pivot until enough of a pivot's range arrived, or
reach the leaves in small batches that rewrite each leaf more often. The merge a carry can skip
is the one before a flush, and a pivot reaches it only when nothing else flushes it first.

## 12. What a merge costs

Sources. **[merge]** `benches/compaction.rs`: a leaf's settle (one branch, and a newer one of an
eighth its entries, half of them overwriting) and a pivot's compaction (four branches of a quarter
each, interleaved), merged whole on one thread as a worker merges a job, each warmed first;
instructions an input entry, exact under the host's load, and macOS `sample` profiles of the 10M
settle. Three invocations of three rounds an arm, alternating.

A merged entry cost about 2,200 instructions: 2,190 a settled one and 2,280 a compacted one at 10M,
2,240 and 2,390 at 100M, where the merge also waited on its input reads (wall time twice CPU time).
The settle's profile:

| share of the merge's samples | what |
|---|---|
| 33% | the range filter's build: its trie's insert (24.5% with what it calls) and each key copied into its builder (10.4%) |
| ~40% under load | byte copies: the cursor decoding input pages, the page builder's push and encode, the cache's copy of each written page, the kernel's on pread and pwrite |
| 9.8% | pwrite, an extent's run of pages a call |
| 8.8% | the merge's own step |

The range filter serves bounded scans, an S3 LIST by prefix (§1's seeks have no end key: of 3M
sources at 1M and 100M they skipped none, the filter never asked [contig]). Every branch keeps it;
its build was three layers each checking a key's order against its own copy of the key before, each
finding their shared prefix, each copying the key. The branch builder's order check now finds the
shared prefix once, and the filter and its trie are told it: no key is copied but by the builder.
Instructions an entry fell 12% for a settle (2,195 to 1,932) and 9% for a compaction (2,280 to
2,080), the filter's bytes the same (`a_filter_pushed_with_shared_prefixes_encodes_as_one_cut_from_whole_keys`).
The merge also keeps the least key of its other inputs: while the current key is below it, a step
moves the current input alone, one comparison where it compared every input twice and copied the
key. Alone it saved 2.3% a settled entry and cost 0.5% a compacted one, whose four interleaved
inputs change the least nearly every entry, and the pick now finds two.

Direct I/O (`CachingRequest::PreferDirect`; `F_NOCACHE` on this Mac's APFS) doubled the merge's
wall time (settle 352 to 401 ns an entry against 151 to 205 buffered), its CPU time the same or more
(177 to 199 against 150 to 187 ns) and 5% more instructions: no read-ahead or write-behind serves a
merge reading and writing in turn, and the kernel's copies it avoids did not show as CPU saved.

## 13. Pages workers write, cached where they were written; memory moving between regions

**What a read paid for.** In 1M gets after a 10M fill, 0.49 a get read a page from below the
shard's cache, 0.57 at 100M, and seeks at 100M 1.57 a seek; the cache held 0.7 MB after the fill
against a limit of 31.9M pages, and nothing was evicted. The shard's own store wrote 16.7K pages
of a 100M fill: maintenance workers wrote the rest through stores of their own with no cache, so
each such read was the first of a page a worker had just written, and a read with no slot made one
(0.07 page faults a get at 10M, 0.16 at 100M). With 5M gets, the cache warm after the first
million, the same build answered 867K gets/s against 638K in the 1M-get phase.

**Lent and adopted.** The shard's cache lends each compaction job room for its inputs' pages, within
its limit and while the machine has twice the memory free (`Cache::lend`); the worker's store keeps
each node page it writes there (`Kept::keep`, no allocation), and the buffer comes back with the
job's result, adopted as a chunk whose slots hold those pages when the trunk applies the job's
branches (`Cache::adopt`); a failed or abandoned job gives the room back. SplinterDB fills its
cache's pages in place for a pack (clockcache.c `clockcache_alloc`); this reaches the same by
handing the memory over and back, one owner at a time. Against the build without it [contig],
three rounds at 10M and two at 100M, RocksDB beside:

| keys | | puts/s | gets/s | seeks/s | page faults a get |
|---|---|---|---|---|---|
| 10M | lent | 2.44 M | 905 K | 397 K | 0.0065 |
| 10M | not lent | 2.30 M | 727 K | 342 K | 0.068 |
| 10M | RocksDB 256 MiB / 32 GiB | 922 K / 901 K | 282 K / 311 K | 108 K / 164 K | |
| 100M | lent | 1.92 M | 516 K | 126 K | |
| 100M | not lent | 1.74 M | 261 K | 109 K | |
| 100M | RocksDB 256 MiB / 32 GiB | 861 K / 837 K | 245 K / 233 K | 84 K / 98 K | |

Every get's page came from the cache (no device read in the gets). Two costs remain. Each loan is
fresh memory, so a worker's first touch of it faults (0.078 faults a seek at 10M, whose
consolidations rewrite about 1.2 GB: about 75K faults of 16 KiB pages), and the memory of pages
forgotten when their branch is merged away is not lent again: the cache held 55.5 GB after the
100M seeks against 4.3 GB not lent, its pages' memory following the bytes written, not the data
live. Loans must reuse what forgotten pages free.

**Memory moving between regions.** A retune gave each region its new share at once: the page cache
a smaller one it trims down to over half a cycle, the record cache a larger one it grew into at
once. On one CPU the record cache took a 4 KiB page while the page cache still held 22 KiB above
its share, and the regions held 1,536 bytes past the budget (`shard_db_paced`'s
`memory_moving_between_regions_never_holds_more_than_the_budget`, 7 of 40 runs). A region now grows
past what it holds only into the budget's room, and each trim step grants again what it freed:
0 of 60 runs.
