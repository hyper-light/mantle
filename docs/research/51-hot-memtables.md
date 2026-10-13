# 51. Gets at memory speed: packed memtables kept readable

Sources. Measured 2026-10-13 in Docker Desktop's Linux VM on this Mac (18 cores, 63 GB, 4 KiB
pages, transparent huge pages always: 1.7 of 2.0 GB of the process's memory in huge pages during
gets), under the host's load from other sessions; the ranges path (`benches/ranges.rs`), one
shard, 16-byte keys, 100-byte values, seed 301, 64 MiB memtables, the machine's memory
(`--machine-memory`); SplinterDB and RocksDB beside it in the same container
(`benchmark-results/mantle-splinterdb-20261013`, its README).
- **[ab]** `runs/n*-r21..23-*`: 483cc0b and 1d5caa9 beside SplinterDB (256 MiB and 32 GiB cache)
  and RocksDB 11.8.1 (256 MiB and 32 GiB), three interleaved rounds at 10M and 100M.
- **[prof]** `prof/{base,mrec,mhot}-get10m-*`: `perf record -e cpu-clock` (the VM exposes no
  hardware counters) of 5M gets after a 10M fill, the shard thread's self time by symbol.
- **[hot]** `runs/n*-r31..35-*`: the diag below at 100K (three rounds) and 10M (five).
- **[sdb]** SplinterDB at `vmware/splinterdb@7d607b11` (`../splinterdb`).
- **[F2]** Kanellis, Chandramouli, Hart, Venkataraman, *From FASTER to F2*, PVLDB 18 (2025),
  arXiv 2305.01516v3 (research/37).
- **[SILT]** Lim, Fan, Andersen, Kaminsky, *SILT: A Memory-Efficient, High-Performance Key-Value
  Store*, SOSP 2011.

## 1. What a get costs [prof]

5M gets after a 10M fill, 735K a second in both builds; the shard thread's self time:

| | 483cc0b | 1d5caa9 |
|---|---|---|
| a leaf's binary search (`View::bound`) | 30.7% | 33.3% |
| clock reads | 25.7% | 8.9% |
| Bloom probes (`Filter::may_contain`) | 11.6% | 13.7% |
| the trie to the leaf (`select1`, `step`, `floor`) | 12.5% | 16.5% |
| the record cache | 6.1% | 7.1% |

1d5caa9 stopped timing the record cache's misses, and the clock's share fell by 17 points while
the rate stayed the same: on aarch64 a clock read's `isb` waits for the loads before it, so a
profile charges their misses to it (research/36 §2). The time is the misses of the trunk's
path. After the fill the root held 42 branches over 8 pivots and the leaves 9, so a get asks
about six Bloom filters (their misses overlapped, `Trunk::find`'s touch), walks one trie and
searches one leaf: about six to eight misses one after another, ~1.4 µs here.

## 2. What SplinterDB's get costs [sdb, prof]

Same VM and workload, 32 GiB cache: `btree_find_pivot` 28.1%, `btree_find_tuple` 27.4%,
`trunk_ondisk_bundle_merge_lookup` 14.2%, `key_compare` 6.5%, its clock 2.2%.
- Its B-tree search halves an offset table, each step a full key comparison through
  `data_config`'s function pointer (`btree.c:481`, `btree_find_pivot`): two dependent loads a
  step, the offset and then the key; no key heads, no prefix truncation. Mantle's leaves keep
  4-byte key heads after the page's prefix, two cache lines a halving reads.
- Its routing filters live in cache pages: a probe is a cache lookup and a page read
  (`trunk.c:6972`, `routing_filter_lookup(context->cc, ...)`). Mantle's Bloom filters stay in
  memory.

Both pay a sorted structure's dependent misses; SplinterDB's gets ran at 610K–824K a second at
10M [ab], mantle's at 596K–752K.

## 3. What the target needs [inferred]

Four times SplinterDB's gets, its 32 GiB arm, from [ab] and the first batch (r1–r3):

| keys | SplinterDB gets/s | 4× | ns a get, harness included |
|---|---|---|---|
| 1M | 1,618,629 | 6.47M | 155 |
| 10M | 824,146 | 3.30M | 303 |
| 100M | 359,828 | 1.44M | 694 |

The trunk's path at its best (filters overlapped, a trie, a leaf) is six to eight misses one
after another: at most ~1.5–2× SplinterDB. Four times needs a present key found in about two
misses: a hash of the key, then its record.

## 4. A hash path for recent records [F2, SILT]

- F2 keeps the hot log's recent records in memory, read-only once past its mutable region, and
  serves reads of them from memory through the hot log's hash index [F2 §4].
- SILT freezes a full LogStore and converts it to a HashStore in the background; the old
  LogStore serves lookups during the conversion, and a lookup searches the LogStore, the
  HashStores newest first, then the SortedStore, the youngest store's value winning [SILT §2,
  §3.2]. A HashStore's filter is the LogStore's hash table's tags, copied rather than rebuilt
  [SILT §3.2].
- Mantle's memtable is already a hash index over an arena (research/40). Once its branch is in
  the trunk, a packed memtable kept readable is F2's read-only region and SILT's HashStore: the
  index and the records exist, built by the fill, so the put path pays nothing for them.

## 5. The measured potential [hot]

A diag (`gets-hot-diag`, 3acc024): every packed memtable kept readable, each kept memtable's first
index group read before any is probed (their misses overlapped), probed newest first after the
frozen memtables and before the record cache.

| keys | arm | fill/s | gets/s | seeks/s |
|---|---|---|---|---|
| 100K | none kept | 5,963,992 | 5,556,277 | 1,023,279 |
| 100K | every one kept | 5,214,297 | 7,435,391 | 1,061,829 |
| 100K | SplinterDB, 32 GiB | 1,763,343 | 2,655,951 | 613,166 |
| 10M | none kept | 3,866,885 | 712,664 | 430,507 |
| 10M | every one kept | 3,428,196 | 1,090,464 | 436,908 |
| 10M | SplinterDB, 32 GiB | 931,459 | 749,987 | 183,768 |

- Gets at 10M: 1.49× with every memtable kept (paired rounds, 1.37–1.69×); 1.45× SplinterDB.
- At 100K, one memtable kept and in the caches: 7.4M gets a second, 134 ns a get with the
  harness, the floor a hash hit sets.
- Fill at 10M: 0.86× (0.86–0.98×). Kept memtables leave no spare: each fill starts a new
  memtable whose index grows and migrates from empty, and whose arena faults in fresh pages.
- Where the 10M gets' time went [prof, `mhot`]: probing 18 memtables' indexes 53% (the probe
  32%, the lookup's loop 21%), the absent keys' Bloom probes and trunk descent ~10%.

## 6. What follows

1. **Step 1, do gets need it:** yes; gets are 1.0–1.1× SplinterDB and the target is four times.
2. **Step 2, how much:** 3.3M gets a second at 10M, 303 ns. A present key found in a kept
   memtable's index costs the index's group and the record, two misses (134 ns at 100K, cached);
   18 indexes probed at 10M cost 53% of a get. One index over the kept memtables, two misses a
   get, is what reaches the target; the 177 memtables of 100M make it necessary.
3. **Step 3, elsewhere or later:** the index over kept memtables is built on maintenance
   workers, which hold each memtable's copy of its arena (research/40 §4) and walk every key
   to pack it; a new memtable's arena is faulted in on a worker.
4. **Step 4, fewer resources:** the kept memtables are the fill's own arenas and indexes, no
   copy; a new memtable's index is sized from the last one's entries, so it neither grows nor
   migrates (the spare's behavior).
5. **Step 5, spread:** index merges on workers.

The memory a kept memtable holds is the budget's idle memory: the page cache's share less what
it holds and the room its next chunk takes (store/cache.rs grows by chunks), so any region that
needs memory takes it from the kept memtables first, oldest first, as F2's log evicts at its
head. The machine keeps twice a memtable's bytes free before a fill starts a new one, the page
cache's own rule (`Cache::machine_spares`).

## 7. What 100M showed: one index over the kept memtables

Kept memtables as built (065cd73), paired with 1d5caa9 (`runs/n*-r61..65-*`):

| keys | gets | fill | seeks |
|---|---|---|---|
| 1M, 3 rounds | 1.72× (1.66–1.75) | 0.93× (0.87–1.07) | 0.97× |
| 10M, 5 rounds | 1.52× (1.26–1.66) | 1.01× (0.78–1.36) | 1.36× |
| 100M, first round | 0.31× | 0.95× | 0.88× |

At 100M the budget kept all 178 memtables (16.7 GB) and they answered every present key, but a
get probed 178 indexes, an absent one all of them: 120K gets a second against 390K. Probing a
memtable's own index costs a get one group's miss each; their overlap is bounded by the core's
outstanding misses, which no published figure states for this core (the Cimple paper cites 6–10
L1 miss buffers as typical, Haswell's 10 line fill buffers; arXiv 1807.01624), so the probes a get
makes must not grow with the memtables kept.

**One index over them, built on workers.** The packing worker already hashes every key of the
memtable's copy as it sorts it (the full 64-bit filter hash, of which the branch keeps 32 bits) and
walks each key's newest entry once: it emits that walk's (hash, arena offset) pairs, which the copy
shares with the arena. A worker inserts them into a table mapping a key's hash to (memtable,
offset), the newest memtable's entry replacing an older one's, and the table goes to the shard; a
get probes the few kept memtables the table does not yet cover, then the table, one group's miss
and the record's. Two tables take turns: one serves while the other is out being extended with the
memtables kept since, so the shard never lends what it reads and each entry is inserted twice. An
entry whose memtable was given back is a miss (the trunk holds its record); the table drops such
entries when it next grows or holds more of them than of live ones.
