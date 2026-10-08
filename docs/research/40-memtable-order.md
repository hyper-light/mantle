# 40. A hash memtable that sorts only what order needs

Sources, marked as cited. None was fetched and re-read for this note; each is cited for the one
well-known result it is named for, and a re-read is owed before any further claim leans on them.
- **[Knuth]** Knuth, *The Art of Computer Programming*, Vol. 3, §5.2.5, sorting by distribution:
  least-significant-digit radix sort, stable, one pass a digit.
- **[AlphaSort]** Nyberg, Barclay, Cvetanovic, Gray, Lomet, *AlphaSort: A RISC Machine Sort*,
  SIGMOD 1994: sort (key-prefix, pointer) pairs, not records or bare pointers, so compares read
  the pairs in sequence and touch a record only when prefixes tie.
- **[BS]** Bentley, Saxe, *Decomposable searching problems I: static-to-dynamic transformation*,
  J. Algorithms 1(4), 1980: the logarithmic method, sorted runs merged as a binary counter.
- **[OvL]** Overmars, van Leeuwen, *Worst-case optimal insertion and deletion methods for
  decomposable searching problems*, Information Processing Letters 12(4), 1981: the logarithmic
  method de-amortized, each level's rebuild spread over the insertions that fill the level again.

## 1. Why (crates/engine/src/memtable/hashed.rs)

The B-tree memtable spent most of a put in `BTreeMem::search`: a descent of cache misses and key
compares for every insert (sampled 2026-10-07: 1,533 of the memtable's 1,779 samples). A put needs
no order; only a scan and a packing walk do. So entries go to an arena with a hash index of each
key's newest entry, and order is built separately, where it costs least.

## 2. The design

- **Prefixes** [AlphaSort]. A run holds `(8 key bytes, offset)` pairs. The 8 bytes start past the
  prefix every key in the memtable shares with its first key, so keys under one common prefix (a
  range's keys) still differ in them. Prefixes taken before the shared prefix shrank are taken
  again when their tail is sorted.
- **Radix** [Knuth]. A tail is sorted by its prefixes a byte a pass, least significant first: one
  counting pass gives every byte's histogram, a byte with one value across the tail costs no pass.
  The sort is stable, so entries of one prefix stay in arrival order. Ties of distinct keys (keys
  that share 8 bytes past the shared prefix) are then merge-sorted by key, each tie on its own;
  last, the newest of each key is kept in place.
- **Runs** [BS, OvL]. Runs are merged within a level (a run of `len` entries is at level
  `bits(len)`), at most one merge a level. A seal of `t` entries pays every merge in progress
  `2t` moves: a level-`k` run holds at least `2^(k-1)` entries, so two more reach the level only
  after `2^k` more are sealed, and a level's merge moves fewer than `2^(k+1)`. However often scans
  seal, a level holds at most two runs and one merge's two inputs
  (`however_often_scans_seal_a_walk_merges_logarithmically_many_runs`; paying `t` let runs reach 25
  at 49 entries).
- **Paced as it fills, in bounded chunks.** The active memtable sorts its tail in chunks of at
  most `ORDER_BOUND` entries (or, near its bound, of the room left), each paid at its own rate
  by the puts that follow it, so it ends as the next is due; a sort still open when the next
  chunk is due is finished first. Each merge in progress moves twice the entries written
  [OvL]; a seal charges only entries whose writes did not. A scan after any burst of puts then
  sorts at most two chunks. Sorting in chunks of half the room left, as first built, made the
  first seek after a fill sort half a memtable: 3.15 ms at 10M puts, 11.8 ms at 1M; bounded,
  107 us and 60 us. Sorting only at rotation crowded the walk into the room's last fraction (up
  to 171 entries packed in one put, p99 4.1-4.6 us).
- **Walk.** A heap of the runs by their next entry; equal keys across runs yield the newest.
- **Buffers.** A finished sort's scratch and a merge's inputs are kept as the spare for the next
  output; a cleared memtable keeps its two largest buffers and its index's table
  (`IncMap::clear`), so a fill after the first allocates no entry buffer and never migrates its
  index.

## 3. Measured (benches/shard_db.rs, 10 M random puts, 16-byte keys, 100-byte values, 64 MiB memtable)

Per put, `/usr/bin/time -l` counts with a run of no puts subtracted. The machine was shared and
loaded (load average 40–65), so throughput and latency swing between runs; instructions do not.

| memtable | instructions | cycles | p50 µs | p99 µs |
|---|---|---|---|---|
| B-tree (dev) | 9,327–9,404 | 5,084–5,292 | 0.88–0.92 | 2.79–2.88 |
| hash, merge sort of offsets | 13,487–13,490 | 5,578–5,762 | — | 4.46–4.75 |
| hash, merge sort of prefixes | 11,195–11,202 | 3,415–3,550* | 0.38–0.42 | 4.25–4.62 |
| hash, radix of prefixes, at rotation | 9,504–9,590 | 3,954–4,108 | 0.33–0.38 | 3.79–3.96 |
| hash, radix, paced as it fills, heap walk | 9,543–9,560 | 4,228–4,306 | 0.58–0.62 | 2.71–2.75 |

\* measured while the machine was less loaded than for the other rows.

The last row's cycles are 16–19% under the B-tree's. Its p50 rose from the row above as the fill's
sort moved onto the active memtable's puts. Throughput in the same paired runs was 25–38% higher
than dev's.
