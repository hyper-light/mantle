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
- **[RocksDB-vector]** RocksDB 11.8.1 (`abeebd963`), `memtable/vectorrep.cc:199-218`
  (primary source, read 2026-10-13): `VectorRep::Iterator::DoSort` sorts an immutable vector
  memtable once, in place, when it is first iterated, which for a full memtable is its flush on
  a background thread; puts only append.

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

## 4. Where the order is paid: the shard or the packing worker (2026-10-13)

Measured: a 100M-key fill (benches/ranges.rs, one range on one shard, 16-byte keys, 100-byte
values, 64 MiB memtable, machine-sized cache; seek-mergecost c8b4511's bench, rangesmcr-bin),
`sample` of 10 s from 25 s into the fill
(benchmark-results/mantle-contig-20261013/prof-fill-mcr-100m). The shard thread was busy in all
6,844 of its samples, none in a wait; the 17 maintenance workers were busy in 8,107 of 116,456
(7%). The put rate is the shard thread's. Of its samples, inclusive:

| what | share |
|---|---|
| the active memtable's paced order (`HashMem::order_put`: merges 10.7%, radix sorts 5.2%) | 16.5% |
| feeding the frozen memtable to its worker (`feed_one`: the heap walk's `sift` and `cmp`, 8.3% and 4.5% overall, the encode, 6.8%, and copies) | 20.4% |
| inserts (`insert_hashed`) | 7.9% |

A 10M fill (2.5 s from 1.2 s in, prof-fill-mcr-10m): order and walk 39% of the shard together.

1. **Is the order needed at all?** A put needs none and a get needs none: both go through the
   hash index. A scan needs the order of the memtables it reads, and a packing builds its branch
   in key order. The shard orders a memtable as it fills so that the walk is ready when it
   rotates, and the walk serves a worker that could sort the entries itself: the order is
   needed, but not on the shard, and not before a scan asks for it.
2. **This much?** A fill that no scan reads needs no order on the shard. A memtable that
   workers will pack keeps a copy of its arena as entries are written (its mirror,
   `HashMem::mirror_into`), handed whole to a worker at the rotation; the worker keys every
   entry by its prefix and filter hash, as an insert keys the tail, sorts them by the same
   prefix radix sort (§2, `Sorter`), keeps the newest of each key, and builds,
   [RocksDB-vector]'s deferral done on a worker. A scan still needs the order of what it reads
   (`HashMem::seal`), so a memtable that scans read keeps the paced order of §2, as does the one
   after it, so that scans that recur find runs ready. A scan into a memtable with no order
   paid sorts its tail at once, the cost §2's pacing bounded; it is paid only by the first scan
   after a scan-free fill.
3. **Elsewhere, later.** On the packing worker, after the rotation: workers sat idle 93% of the
   fill measured.
4. **Fewer resources.** The shard's work per entry falls from its share of the merges, the
   heap walk and an encode to one more copy of the entry's bytes as it is written, into memory
   reserved once to the arena's bound, so the copy never reallocates; nothing is left to feed
   at the rotation, and no compare is made on the shard.
5. **Spread.** One worker a memtable, as now; frozen memtables pack in parallel.

Cost: a memtable's copy doubles its arena's memory until its worker has built the branch,
then the buffers go back for the next memtable's copy (`ShardDb::memtable_bytes` counts them,
out or kept).

Measured (benches/ranges.rs, 16-byte keys, 100-byte values, 1M gets and 1M seeks of 10 after the
fill, machine-sized cache, three interleaved rounds an arm, the host loaded by other work as it
is; benchmark-results/mantle-contig-20261013 r141-143, medians, puts a second):

| keys | the shard orders and feeds | a worker sorts | with the range's lend before a wait | both | RocksDB 11.8.1, 256 MiB cache |
|---|---|---|---|---|---|
| 10M | 2,484,976 | 2,836,159 | 3,133,737 | 4,926,794 | 1,022,821 |
| 100M | 2,037,307 | 2,392,375 | 2,786,816 | 4,375,238 | 846,942 |

Gets and seeks were the same in every arm within the rounds' spread (at 100M, 630-652 thousand
gets and 170-179 thousand seeks a second). The worker's sort alone gained 14-17%: with the order
gone from the shard, a same-shard client's puts still crossed the range's request channel
whenever a packing was out on a worker, since the range waited for it holding its engine
(docs/design/engine-structure.md, "Same-shard clients"). The two together gained 98% at 10M and
115% at 100M, more than their product: each removed what held the other's gain back on the same
thread. A 10M fill's shard thread then showed no order work at all; its clock reads became the
largest share left (31% of samples, prof-fill-mcp-10m).
