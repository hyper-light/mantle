# 56. A memtable's packing: where its time goes, and what it computes twice

Sources.
- **[fill]** `benchmark-results/mantle-splinterdb-20261013/prof/pack100m-*`, `pack10m-*`: the
  ranges path's fill (`benches/ranges.rs`, one shard, 16-byte keys, 100-byte values, seed 301)
  of pieces with the packing's cache loan (`88183e5`, built with line tables), `perf record`
  (cpu-clock, 1999 Hz, call graphs by frame pointer) of the whole process for 15 s from 8 s into
  a 100M fill and 2 s from 1 s into a 10M fill, in Docker Desktop's Linux VM on this Mac, 2026-10-15.
- **[bench]** `benches/packing.rs`: one 64 MiB memtable (562,324 entries, filled as the fill fills
  one) packed into a store on a buffered file, round after round, wall nanoseconds an entry;
  `prof/packing-base-1*` and `prof/packing-trielvl*` the same under `perf record`.
- **[gate]** runs `n10000000-r541..543` and `n100000000-r541..543`: the full bench (`inside3.sh`'s
  mantle arms) beside main, RocksDB and SplinterDB, three quiet rounds interleaved; and
  `n100000000-r551..553`, the same with the trie change (`e3a5c76`) and §4's (`a5a0a55`) as
  arms, every run's recovery verified exact.
- Zhang et al., "SuRF: Practical Range Query Filtering with Fast Succinct Tries", SIGMOD 2018: a
  range filter keeps each key cut one byte past what it shares with either neighbour.

## 1. What a packing costs

A packing turns a full memtable into the trunk's branches on a maintenance worker: its arena's
copy sorted, then walked in key order into the pages of one branch a piece. At 100M [fill] the
walk is 22.1% of the process's samples and the sort 2.2%, the largest single cost of the fill,
ahead of compaction's merges (12.6%); the index over kept memtables the packing feeds is another
8.6%. Alone [bench] a packing takes 240-250 ns an entry, about 135 ms a memtable. A memtable
fills in about 139 ms at 10M, so one packing worker is busy nearly all the time a fill runs: any
slowdown lets frozen memtables queue, and a rotation then waits for one (research/55 §1).

Within the walk [fill], by the function each sample was in:

| | share of the walk |
|---|---|
| the builder's own code (`Builder::add_inner`, `Builder::insert`) | 50.5% |
| the range filter's trie (`TrieBuilder::push_key`, `Level::push`) | 13.9% |
| an entry copied into its page (`Page::push`) and the C library's copies | 20.6% |
| the page encoded, its CRC, the filter's insert, the trie's packed values | 6.5% |
| the walk, the arena's entries parsed, the keys hashed | 4.5% |

## 2. Step one: what a packing must do

| work an entry | needed | done now |
|---|---|---|
| its key hashed | once | three times: at the put, in the sort, in the walk |
| the bytes its key shares with the key before | once | twice: against the previous key, against the page's first |
| its key kept as the previous key | at a page's end | every entry |
| its bytes copied into the page's payload | once | twice: into the page, then into the payload |
| its key's cut form in the range filter | once (S3's LIST reads it) | once |
| its filter bits, its page's CRC | once | once |

## 3. A trie level's accessor

`TrieBuilder::level`, taken for every label of every key the range filter and the leaf index
hold, kept the growth a new level needs in its body, so it saved and restored a 240-byte frame on
each call: 12.6% of a packing [bench]. With the growth in a cold function the accessor inlines.
The packing bench, interleaved, 15 rounds each: 253.3 ns an entry before, 234.1 after (medians),
7.6% less. The full bench at 10M [gate] shows no change in the fill's rate, which the shard
thread's puts set, not the packing worker: 3.93M puts a second against 4.05M, the rounds spread
3.71-4.20M.

## 4. A key hashed once in the packing

A put hashes its key once for the record cache and the memtable, which keeps the hash's high 32
bits with each entry for the branch's filter. The packing hashed every key again twice: its sort
for the high bits, its walk for the whole hash the index over kept memtables keys on. The sort
now lists each key's whole hash with its entry's offset as it keys the entry, in the order the
entries were written, and the walk hashes nothing. A key written twice is listed twice, its newer
entry after; the index keeps the last entry it is given for a hash, so it holds each key's
newest, as the walk's list in key order gave it. The kept memtable's key count is the memtable's
own, each key once.

## 5. The put's hash in its entry

§4's list still left two costs: the memtable's index, growing, hashed every key it moved to its
larger table, and the list held every entry the memtable had, an overwritten key's more than
once, so the index over kept memtables took more entries than keys (its bytes at 100M 19.28 GiB
where the trie change's were 18.17 GiB [gate]). A put's paced attempts each hashed
the key again, as did a get refused its inline attempt and queued.

A memtable entry now holds the whole 64-bit hash its put computed, after its lengths:
operation, key length, value length, hash (8 bytes), key, value. Everything that reads the entry
later takes the hash from it:
- the index, growing, moves each entry by its stored hash;
- the packing's sort keys each entry by the stored hash's high 32 bits, its records still 16
  bytes;
- the walk, over the sort's survivors (each key's newest entry, whole keys compared), lists
  each key once, in key order, with its stored hash: the index over kept memtables takes one
  entry a key.

A client hashes a put, a deletion or a get once, and its request carries the hash from the
inline attempt to the range's queue and through every attempt a paced put makes.

Two keys of one whole hash stay two entries of the memtable and two of the walk's list; the index
over kept memtables holds the later one's entry for the hash, and a get of the other finds a key
not its own there and reads the trunk. The engine's tests drive this with two 16-byte keys of one
xxh3-64 hash, and grow a memtable's index with entries stored under hashes not their keys' own,
which a move that hashed the key again would lose.

The cost is 8 bytes an entry: an entry of a 16-byte key and a 100-byte value takes 127 bytes of
the arena where it took 119, so a memtable of a fixed size holds 6.3% fewer entries and rotates
6.7% more often, and a kept memtable holds 6.3% fewer keys in the same bytes. Whether the hashes
saved pay for that is the full bench's to decide, beside §4's arm and its bases.
