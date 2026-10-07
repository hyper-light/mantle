# 35. Fast Succinct Tries and SuRF

Source: Zhang, Lim, Leis, Andersen, Kaminsky, Keeton, Pavlo, *SuRF: Practical Range Query
Filtering with Fast Succinct Tries*, SIGMOD 2018 (Best Paper), §2–§4. Marked **[paper]**.
Read for E6 (docs/design/engine-structure.md §5): the in-memory index of a branch's leaves and
the range filters of its bundles.

## 1. The Fast Succinct Trie (FST) [paper §2]

- A static trie of byte labels (fanout 256), built in one pass over sorted keys, holding each
  key's minimal distinguishing prefix and a fixed-length value. It answers ExactKeySearch,
  LowerBound (first key ≥ k) and MoveToNext.
- **LOUDS** (Jacobson): nodes in breadth-first order, each as its degree in unary. Navigation
  with rank and select: child k of the node at p is `select0(rank1(p + k)) + 1`; parent of p is
  `select1(rank0(p))` [paper §2.1].
- **LOUDS-Dense** for the upper levels: each node is three 256-bit bitmaps — D-Labels (branch
  labels), D-HasChild (branch continues), D-IsPrefixKey (the prefix itself is a key) — and the
  values in level order. Child: `D-ChildNodePos(pos) = 256 · rank1(D-HasChild, pos)`; value:
  `rank1(D-Labels, pos) − rank1(D-HasChild, pos) + rank1(D-IsPrefixKey, ⌊pos/256⌋) − 1`
  [paper §2.2].
- **LOUDS-Sparse** for the lower levels: S-Labels (a byte per branch; 0xFF first in a node marks
  that the prefix is itself a key), S-HasChild (a bit per label), S-LOUDS (a bit per label, set
  on a node's first). Child: `select1(S-LOUDS, rank1(S-HasChild, pos) + 1)`; parent:
  `select1(S-HasChild, rank1(S-LOUDS, pos) − 1)`; value: `pos − rank1(S-HasChild, pos) − 1`.
  10 bits a node (8 + 1 + 1) against an information-theoretic 9.44 [paper §2.3, §2.5].
- **LOUDS-DS**: dense levels down to the cutoff `l`, the largest with
  `LOUDS-Dense-Size(l) · R ≤ LOUDS-Sparse-Size(l)`, `R = 64` by default [paper §2.4].
- **Rank**: one level of lookup table, a 32-bit count each `B` bits, then popcount; `B = 64`
  for dense (50% overhead on small bitmaps), `B = 512` for sparse (6.25%) [paper §2.6].
- **Select**: a lookup table sampling every `S`th set bit, then popcount; `S = 64`, 9–17% of
  the bit vector, 1–2% overall. Needed only on S-LOUDS [paper §2.6].
- **Label search**: SIMD over a node's labels (most nodes have under 8) [paper §2.6].
- **Iterators** keep a cursor a level, so range scans move each forward without rank/select
  after the first descent [paper §2.4].
- Measured: FST is among the fastest order-preserving indexes for point and range queries at
  the least memory, against B+tree, ART and compact ART [paper §4.1, Fig. 5]; 4–15× faster than
  earlier succinct tries [paper §2].

## 2. SuRF [paper §3]

- SuRF-Base: the trie of minimal distinguishing prefixes, one byte past each; 10 bits a key on
  64-bit integers, 14 on emails; false positives when a query shares a stored prefix.
- SuRF-Hash: n hash bits a key, point-query FPR < 2^-n; SuRF-Real: the n key bits after the
  prefix, improving point and range FPR; SuRF-Mixed combines them [paper §3.2–3.4].
- `moveToNext(k)` returns the first key ≥ k and a false-positive flag when only a prefix
  matched; `count(lo, hi)` bounds range counts [paper §3.5].
- In RocksDB in place of Bloom filters: open seeks up to 1.5×, closed seeks up to 5× on a 100 GB
  dataset [paper abstract, §6].

## 3. What mantle takes

- A branch's leaves are routed by an FST of their separators (the minimal prefix above the
  previous leaf's last key), the value each leaf's page number: the index held in memory at
  a few bytes a leaf (10 bits a trie node), where page interiors hold a full key and a child
  entry a leaf.
- The same trie, built over a bundle's keys with suffix bits, is that bundle's range filter.
