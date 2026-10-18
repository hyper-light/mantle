# 62. A leaf index lookup

A seek's placing spends about 210 ns of its ~880 ns at 10M finding the leaf its key falls in
(research/61 §2), and every get that passes a branch's filter makes the same lookup
(`Branch::get_hashed`, `prepare_leaf`). Both are `Branch::leaf_of`, the floor of the branch's
leaf index: the Fast Succinct Trie of its leaves' separators (research/35). This note records
where that lookup's instructions went and what each change to it measured.

The measurements use `benches/leaf_index.rs`: one branch built from `KEYS` keys, its leaf index
looked up at keys drawn over twice its range, in two shapes. The number shape is the shard
benchmark's 16-byte key, a big-endian number padded with `'0'` bytes as RocksDB's db_bench
writes them. The object shape is S3-like names under 4,099 prefixes
(`tenant-NNNN/photos/2026/NNNNNNNNNNNN.jpg`).

## 1. Where a lookup went

Callgrind of the lookups alone, one shape a run (the Mac's Linux VM, aarch64; main b8bd6cd plus
the bench; 1M keys, 2 × 100,000 lookups; `native-ab/cg/cgleaf-base-*`). Instructions a lookup,
by function, inlined callees included:

| shape | `Bits::select1` | `Trie::step` | `Trie::floor` | total |
|---|---|---|---|---|
| number | 1,391 (59%) | 570 | 405 | 2,377 |
| object | 2,323 (29%) | 3,729 (47%) | 1,733 | 7,916 |

`select1` on S-LOUDS finds a node's first label: it reads the sample of every 64th node start,
then scans words by popcount to the node, then selects within the word. Its cost follows the
level's shape:

- In the number shape's first sparse level, 31 nodes hold up to 256 labels each, so one sample
  covers the whole level and a select scans up to ~124 words: ~650 instructions a call.
- In the object shape's single-label chains, the scan is a word or two, but the in-word select
  (three halving steps and a byte loop) still costs ~100 instructions a call, about 24 calls a
  lookup.

The rest went to each level's control flow rather than its arithmetic. The costliest lines were
the `Fallback` and `Node` matches, the level looked up again by each helper (about four times a
level), and the bounds checks of `Bits::get`. Of the rank's lines, only its mask was near the
top.

## 2. Node starts kept beside the levels

Each sparse level keeps where its nodes start, built from S-LOUDS when the level is built or
read. The trie's bytes do not change: a test pins the bytes main b8bd6cd writes for a set of
tries (`the_bytes_written_are_main_b8bd6cds`).

A node holds at most 256 labels, one a byte value. Node `n` starts at its block's first node's
position (every 256th node, `u32`), plus one label for each node before it in the block, plus
those nodes' labels past their first (`u16`: 255 nodes past the first label hold at most
255 × 255 = 65,025). A level whose nodes all hold one label keeps no extras. There a node starts
at its own number, and these levels are the runs of single-child chains that shared prefixes
make. A node of more than 256 labels is refused as corrupt when a level is read.

The levels' shapes, at 1M keys (from a probe over each trie's levels, not kept):

| trie | sparse nodes | labels | one-label nodes | kept if every node's start were 2 B | kept |
|---|---|---|---|---|---|
| number leaf index | 7,852 | 37,889 | 8 | 15.9 KB | 15.9 KB |
| number range filter | 7,813 | 1,000,000 | 0 | 15.8 KB | 15.8 KB |
| object leaf index | 119,826 | 149,841 | 107,078 | 241.7 KB | 27.5 KB |
| object range filter | 348,669 | 1,348,259 | 73,782 | 702.9 KB | 555.3 KB |

The object leaf index's levels 2–19 and 22–30 hold one label a node (each prefix's
`/photos/2026/` and the separators' tails). The object range filter's levels 1–18 do too. Its
cost is in the levels where names diverge (180,200 nodes of two labels, 81,980 of ten).

## 3. The descent

`Trie::floor` walks the dense levels, then the sparse ones. In the sparse part it finds each
level once a byte, and reads the node's labels from its starts, then the label's child by rank.
A fallback to a smaller label keeps its node. So the value of the key a label ends is found by
the label's rank and its node's own-key rank, without S-LOUDS's rank to find the node.
`rightmost` descends the same way. `select1`, its samples and `Select` left the bit vector.
