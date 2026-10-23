# 66. What the trunk holds at once

An engine's statement of its memory (`shard_db::bounded_bytes`, `shard_db::pooled_bytes`,
node.md §1.4) took its pools at the most of each kind it had ever had out at once, measured
(`Store::pool_peaks`). This note finds what bounds each by rule, from the trunk's fanout `F` and
its height `h`, so the statement takes them from the configuration.

All measurements are `tests/engine_memory.rs`'s fills at a range's floor (fanout 8, the floor's
page, `leaf_entries` a memtable's entries at the least entry, 1,894), inline (no workers), with a
scan of ten rows every 97 puts; release builds on the Apple M-series dev machine.

## 1. Live in-flight bundles

A flush moved a pivot's bundle into its child's in-flight list whatever the child held: 1,468 live
bundles at a node for 32,000 rows of 2,132-byte keys, every scan through it opening each. A node
now takes a flush only below `F` live bundles for every pivot, the bound the root's room keeps for
its pending packings (engine-structure.md, Flush; ea8bfa4e): at most `3F + 1` live a pivot.

## 2. What a compaction builds

A pivot's compaction merged every live in-flight bundle of its pivot into one branch. Each input
spans its node's whole range, and on keys in order a hot pivot's inputs carried far more than a
leaf:

| fill | branch built by a compaction, most entries | leaf limit (`F` memtables) |
|---|---|---|
| 8-byte keys, 1.28M rows, before | 68,184 | 15,152 |
| 2,132-byte keys, 32K rows, before | 15,040 (a settle's part) | 15,152 |
| 8-byte keys, 1.28M rows, after | 15,152 | 15,152 |

A compaction now takes its live bundles oldest first while the entries of their pages in its range
stay within the leaf limit, one bundle at least (`Trunk::plan_compact`); the newer wait above its
branch for the next. The entries of the pages a range falls in hold every entry a branch has in it
(`Branch::merge_size`), so they bound what the merge writes: its builder is sized by them, never
split. By induction every branch a compaction builds holds at most the leaf limit or a memtable's
entries: a packing holds a memtable's, a settle's part at most the leaf limit (`leaf_parts`), and a
compaction at most its inputs in range, each built under the same bound. SplinterDB bounds the same
work another way: it compacts each bundle a flush hands a child on its own (`bundle_compaction`)
and flushes a pivot whose tuples pass its routing filter's limit (trunk.c
`trunk_flush_policy_should_flush_to_child`, research/50 §4).

## 3. Compactions holding their inputs at once

Without workers the shard steps its compactions a budget at a time, and each holds its cursors
open until it finishes. Two held theirs at once in every fill (the root's and one below), but
nothing bounded it: a task whose output had no room was passed over and the next opened. One task
of each pass opens its inputs at a time now (`Trunk::step_inline`, `TrunkStats::open_most`).

## 4. Sources on a read's path

A scan's segment listed every part of each packing not yet in the tree, and every part of each
in-flight bundle at the root: a packing is cut at the root's pivots, and a node's pivots run past
`F` while its split waits for room. The parts' ranges are the cuts', and a pivot key once made is
never taken out, so a segment or a pivot lies within one cut's range: a read or a compaction takes
only the part whose range meets it (`Pending::part`, `InFlight::within`). A path then opens at most
`2F + 1` sources at the root (its packings and live bundles, at most `F`, and the pivot's bundle) and
`4F + 2` at each level below (`TrunkConfig::sources_most`).

## 5. The statement

| fill | height | cursors stated / most spans out | builders stated / most lists out | held of stated |
|---|---|---|---|---|
| 8-byte keys, 40K rows | 3 | 153 / 16 | 5 / 3 | 3.3 of 65.4 MB |
| 8-byte keys, 1.28M rows | 5 | 221 / 36 | 5 / 4 | 12.9 of 82.0 MB |
| 2,132-byte keys, 32K rows | 3 | 153 / 38 | 5 / 3 | 11.4 of 2,750 MB |

Cursors at once are a scan's sources and each compaction's inputs, at most `4F + 2`, two
compactions on the shard without workers or one a worker (`TrunkConfig::cursors_most`); builders,
the shard's packing and each compaction's two, the part it builds and the one whose filter it is
still writing (`builders_most`), each building at most the leaf limit's entries (`built_most`). Each
pool's peak is checked against its kind's count in the test. At 2,132-byte keys the statement is
loose by its builders' tries, which `builder_bytes` counts at a level and a label a key byte for a
leaf's limit of keys: the bound below the data that node.md §1.4 names open.

## 6. The release gate

The build of these rules (engine-inflight 3be9ec0e, whose production code 1f951f7b keeps) against
its base (engine-follow 3cc71771, the production code of 072fed1a), RocksDB and SplinterDB on the
VM lane (Docker Desktop at 96 GiB, the carryover-balanced order of `run7.sh`), each figure its arm
over the base in the same round, medians (`paired.py`):

| keys | rounds | fill | gets | seeks |
|---|---|---|---|---|
| 1M | 4 | 1.012 | 1.066 | 0.999 |
| 10M | 4 | 1.046 | 1.050 | 1.010 |
| 100M | 16 | 1.011 | 0.978 | 0.978 |

At 100M the Mac ran other sessions' builds at a load of 20-30, and both arms' gets ran from 98
thousand to 2.2 million a second with the engine in the same state in every case (178 kept
memtables holding every key found, 13-14 GB of cache, no device read; one case's gets waited 4.6 s
at once): a round's ratio ran from 0.43 to 22, so twelve rounds (R0 7530) joined the gate's four
(R0 7460). Their medians lie within what the layout alone moves a 100M phase, 3-4%
(engine-follow's A/A). The fill took fewer page faults in every one of the sixteen rounds: 741
thousand (711-760 thousand) against 943 thousand (919-993 thousand), 21% fewer, the compactions'
outputs held to a leaf's limit and one of them opening its inputs at a time.

Against RocksDB and SplinterDB, the gate's rounds (medians of each arm's figures):

| keys | over SplinterDB, fill / gets / seeks | over RocksDB, fill / gets / seeks |
|---|---|---|
| 1M | 4.74 / 2.44 / 3.56 | 6.59 / 6.94 / 5.69 |
| 10M | 4.12 / 2.96 / 4.56 | 5.00 / 10.02 / 7.92 |
| 100M | 3.41 / 3.52 / 11.54 | 4.64 / 4.87 / 7.91 |
