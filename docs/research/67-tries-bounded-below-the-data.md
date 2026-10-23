# 67. Tries bounded below the data

A branch keeps two succinct tries in memory (research/35): its range filter, each key cut one
byte past what it shares with either neighbour (SuRF-Real), and its leaf index, each leaf's
shortest separator to the leaf's page. Neither compresses a key's levels, so their labels are
bounded only by the key bytes a branch stores. Keys in pairs sharing all but a last byte, each
pair parting from the one before at its first byte, made the two a third of the data at the
range's longest keys, 2,132 bytes (node.md §1.4). This note gathers what bounds them, and what
each bound costs.

The sources were read from their published copies; quotes are theirs, and lines marked
**Reading** are this note's.

## 1. SuRF has no bound, and mantle's shape is its worst case

Zhang, Lim, Leis, Andersen, Kaminsky, Keeton, Pavlo, *SuRF: Practical Range Query Filtering
with Fast Succinct Tries*, SIGMOD 2018, and *Succinct Range Filters*, ACM TODS 45(2), 2020
(article 5; pages below are its).

- The cut: "SuRF-Base only stores an additional byte for each key beyond the shared prefixes"
  (§3.1, 5:11); SuRF-Real "stores the n key bits immediately following the stored prefix of a
  key" (§3.3). Mantle's filter is this (`fst::surf`).
- No bound: "the trie shape of SuRF-Base depends on the distribution of the stored keys. Hence,
  there is no theoretical upper-bound of the size of SuRF-Base" (§3.1, 5:11). The authors' code
  takes no depth (`SuRFBuilder(include_dense, sparse_dense_ratio, suffix_type, hash_suffix_len,
  real_suffix_len)`, efficient/SuRF f2ec6e1).
- Its worst case is mantle's: 64-character keys in pairs that "differ in the last byte. [...]
  This way of constructing keys is unfriendly to SuRF, because it maximizes the trie height [...]
  and minimizes the internal node sharing"; "SuRF in the worst-case scenario takes 328 bits on
  average to encode each key, consuming memory that is equivalent to 64.1% of the dataset size"
  (§7, 5:26-27).
- The authors' tutorial: "An adversarial workload (e.g., each pair of keys produces a unique long
  prefix) can destroy SuRF's space efficiency" (Pandey, Farach-Colton, Dayan, Zhang, SIGMOD
  Companion 2024, §2.5).

## 2. A filter held below the data gives up precision, never correctness

- Lower bound: "Any data structure for answering approximate range emptiness queries on
  intervals of length up to L with false positive probability ε, must use space Ω(n lg(L/ε)) −
  O(n) bits" (Goswami, Grønlund, Larsen, Pagh, SODA 2015, abstract; Theorem 1). For keys of any
  length a range can hold unboundedly many keys: "a robust range filter cannot support
  variable-length keys" (Eslami, Bercea, Dayan, *Diva*, PVLDB 18(11) 2025, §2). **Reading:** a
  bound below the data for any keys must let more empty ranges through; it may never rule out a
  range holding a key.
- Truncation keeps order: a < b gives a cut to d bytes at most b cut to d bytes, so a filter of
  cut keys has no false negative; a filter over a monotone image of the keys has none
  ("Monotonicity [...] ensures no false negatives", Vaidya et al., *SNARF*, PVLDB 15(8) 2022,
  §2.1.3).
- Hashed-prefix filters bound bits a key whatever the key's length, but only for ranges up to a
  stated length or inside one prefix (Rosetta, SIGMOD 2020, as Proteus §2.1 and Diva §2 describe
  it; REncoder, ICDE 2023; Memento, SIGMOD 2025; Grafite, SIGMOD 2024, Lemma 3.1; RocksDB's
  prefix Bloom, its Prefix-Seek wiki). A listing's range has no such length, so they do not
  serve a LIST.

## 3. Truncating past a depth

Knorr, Lemaire, Lim, Luo, Zhang, Idreos, Mitzenmacher, *Proteus: A Self-Designing Range Filter*,
SIGMOD 2022.

- "As opposed to SuRF, the FST in Proteus does not encode a unique prefix for every key, but
  rather all unique key prefixes of a fixed length [...] a uniform depth can be adjusted to fit
  within any memory constraint" (§4.1).
- No false negative: every key's prefix of the trie's depth is in the trie, so a range meeting a
  key meets its prefix (§4.2, Algorithm 1 with no Bloom filter past the trie).
- The cost, Eq. (5): a range is let through when its keys and the stored keys share at least the
  trie's depth (`P_fp = 1` past it) and never otherwise.
- The depth from the data: "the number of unique key prefixes, |K_l|, for all possible prefix
  lengths l [...] can be done by computing the successive LCPs of the sorted key set [...] an
  O(|K|) operation", the same counts sizing the trie at each depth (§4.3).

## 4. Finding a leaf without each separator's bytes

- Partitioned index: "only top-level index is loaded into memory", at the cost of "one
  additional IO" when the rest is not cached (RocksDB wiki, Partitioned Index Filters).
- Blind search: a Patricia trie that keeps only the depth each node branches at and its
  branching byte "fits Θ(B) strings into one B-tree node independently of their length"; a
  search descends by branching bytes, loads the one string it reaches, takes its common prefix
  with the query, and finds the query's place from the hit node by the mismatching byte (Ferragina
  and Grossi, *The String B-tree*, JACM 46(2) 1999, §2.1 and Lemma 3.5). HOT keeps only
  discriminative bits, "between 11.4 and 14.4 bytes per key" with tuple ids, and "the key
  corresponding to that tuple is loaded from the database and compared to the search key"
  (Binna et al., SIGMOD 2018, §4.3, §6.3). ART skips collapsed chains past 8 bytes and checks the
  key at the leaf (Leis, Kemper, Neumann, ICDE 2013, §III.E).
- Not bounds: Masstree's layers grow with each 8 shared bytes (EuroSys 2012, §4.1); HOPE
  compresses text 1.5-2.5x, not random bytes (SIGMOD 2020, §7); shortest separators are what
  mantle keeps (Bayer and Unterauer's simple prefix B-trees, as PostgreSQL's nbtree README and
  LevelDB's `FindShortestSeparator` implement them).

## 5. What mantle's tries held

Branches of 100,000 keys at a range's floor (the page sized by its 2,132-byte keys and largest
rows: 24,576 bytes), as the engine builds them (`tests/branch_tries.rs`; release, Linux x86_64,
2026-10-23), each trie's bytes in memory against the branch's tree pages:

| keys | range filter | leaf index |
|---|---|---|
| 8-byte, no values (densest) | 16.92% | 0.11% |
| 16-byte numbers, 100-byte values (the shard benchmark's) | 2.60% | 0.08% |
| object names, 42 bytes, 100-byte values | 3.03% | 0.17% |
| paths through two random 32-hex directories, 94 bytes | 6.06% | 0.25% |
| 64-byte pairs sharing all but a last byte, no values | 61.70% | 1.81% |
| 2,132-byte pairs, no values | 68.49% | 13.90% |
| 2,132-byte pairs, largest values | 8.55% | 8.55% |

The pairs are SuRF's worst case (§1): each pair's long shared run is a label a byte that only two
keys share, and where a leaf ends inside a pair, its separator is as long. And every trie kept a
record and buffers a level, about 438 bytes a level whatever it held: two keys parting late in a
small branch made it hundreds of levels deep, the first pair of a filter alone 931 kB of levels
for 2,126 labels (`tests/trie_memory.rs`).

## 6. The bound

- **A depth past what the keys share.** A range filter's entry is cut at most
  `RANGE_FILTER_DEPTH` bytes past the bytes every key of its branch shares, which its builder is
  told: a packing its memtable's (`HashMem::skip`, kept as keys are written), a part a worker packs
  that and what its first key shares with the next cut, a compaction's part what its first key
  shares with the range's end, or with what its inputs' greatest entries share (each a prefix of
  its input's last key). Counted from the root instead, 16 bytes cut object names inside their
  agent and paths inside their first directory, the keys past that one entry each (their filters
  3.03% → 0.29% and 6.06% → 0.02% of the data, precision gone with the bytes). An entry cut short
  at the shared bytes and the depth, the filter's reach, stands for every key starting with it, a
  Proteus prefix (§3; truncation keeps order, §2), and the keys after it starting with it take
  none. Its suffix bits, which no lookup reads, are all set, and the filter keeps its reach, so it
  tells a whole entry by its length and value: a value keeps only its suffix bits, where a bit
  marking an entry whole took a bit a key from every filter (2,000,000 16-byte keys: 4,821,275
  bytes against 4,570,905 with none). An entry whose own cut falls at the reach with every suffix
  bit set, one in 2^n of those, is read as whole, its wider interval: no range holding a key is
  ruled out. Read by length alone, every entry at the reach was whole, and object names at depth
  16 let 4.35 / 5.11 / 4.10% of empty points, gaps and listings through against 3.91 / 4.38 /
  3.66% with no depth; an entry cut at the reach keeping a byte more restored that at 32.78 bits a
  key against 30.28, a quarter of their cuts falling there (§7). A key so adds at most the depth's
  labels, and the trie is at most the depth deep past the shared bytes.
- **The leaf index's groups.** A separator past `LEAF_INDEX_DEPTH` bytes beyond the shared ones
  is cut there and stands for the group of leaves whose first keys start with it; its entry is the
  group's last leaf. A key within a group reads down from the root through the branch's index
  pages, a page a level (§4: a partitioned index's top level in memory, the full separators on
  disk), in every read path: a get, sync and asynchronous (`PointRead` steps index pages as it does
  its leaf), and a seek. A merge's bounds take a group whole, from memory alone.
- **A level costs its labels.** The sparse levels are one sequence each, labels, branch and
  node-start bits, a bit a node, values, with nodes numbered across them breadth first, as SuRF
  lays them out (§1): a branch's child is the first level's nodes and the branches going on before
  it, and a value one rank over the whole part. Node starts keep their 16-bit extras a block of
  256 nodes at a time, none for a block of single-label chains, one past a block's last node, so
  a node's labels are its block's entry and two adjacent extras; and every buffer is taken once at
  its size: the levels' bits and values are joined a word at a time and the extras counted by a
  pass of selects over the node-start words, so a trie's finish takes what the per-level layout's
  did (500,000 keys: 0.47-0.54 ms for numbers, 1.31-1.54 ms for object names, against 0.44-0.51
  and 1.30-1.63).
- **Stated.** `branch::tries_bytes` states what a branch's two tries hold for a key bound and its
  entries and leaves, the depths in it, and `branch::builder_bytes` counts its builders' tries the
  same way; `tests/branch_tries.rs` holds every shape within it.

## 7. Measured

Range filters by depth (`benches/surf.rs`, 1 M keys, 50,000 empty ranges of each kind, 8 suffix
bits; fedora, Linux x86_64, engine-cap cac4acbc, 2026-10-23): the share let through, points /
gaps between keys / listings of absent prefixes, and bits a key.

| keys | depth 4 | 8 | 16 | 32 | 64 | none |
|---|---|---|---|---|---|---|
| 16-byte numbers | 0.04% all, 20.2 bits | = | = | = | = | 0.04% all, 20.2 bits |
| object names | 100% | 100% | 3.92 / 4.41 / 3.67%, 30.3 bits | = none | = none | 3.91 / 4.38 / 3.66%, 31.2 bits |
| paths through two 32-hex directories | 100% | 98 / 99 / 0.79%, 15.6 bits | 98 / 99 / 0.25%, 39.4 bits | same, 82.3 bits | 30 / 27 / 0.25%, 148.6 bits | 28 / 25 / 0.25%, 168.3 bits |

16 is the least depth at which every shape lets its listings through as with no depth; narrow
empty ranges inside a deep directory are let through more, which costs a scan a branch read, never
a result. Scans meet keys far more often than such ranges, and gets ask the point filter.

The same branches as §5 with both depths (`RANGE_FILTER_DEPTH` 16, `LEAF_INDEX_DEPTH` 256), and
how far past the shared bytes each shape's separators reach:

| keys | range filter | leaf index | separators past the shared bytes |
|---|---|---|---|
| 8-byte, no values | 16.86% | 0.03% | 3 |
| 16-byte numbers | 2.59% | 0.02% | 4 |
| object names | 2.97% | 0.08% | 16 |
| paths through two 32-hex directories | 0.01% | 0.07% | 77 |
| data-lake paths (day and hour partitions, uuid part files), 114 bytes | 0.05% | 0.09% | 25 |
| 64-byte pairs | 15.13% | 0.20% | 58 |
| 16-byte pairs a byte apart, no values | 43.87% | 0.13% | 16 |
| the same, one pair in eight parting at each depth | 83.31% | 0.12% | 16 |
| 2,132-byte pairs | 0.46% | 0.69% | 2,126 |
| 2,132-byte pairs, largest values | 0.04% | 0.68% | 2,126 |

The pairs the goal named, a third of their data in the two tries, hold 1.15% of it. Within the
depth the range filter's worst is short keys with no values, a key adding as many labels as the
bytes it shares with the next past what it shares with the one before: 16-byte pairs sharing 15
bytes, a byte apart, take 8 labels a key and 43.87% of their data. A node's start then costs most:
a block of 256 node starts keeps its 16-bit extras for every node once any node of it holds two
labels, and breadth-first numbering keeps such nodes to the levels where keys part; pairs parting
at every depth put one in every block, 2 bytes a node beside a label's 1.4, and take 83.31%, still
below the data, every one within `branch::tries_bytes`. Every
realistic shape's separators stay within the leaf index's depth, so their lookups read no index
page; 64-byte pairs, shorter than the depth past what they share, keep their cut. The leaf index
lost its levels' records (paths: 62 → 16 bytes a leaf; 64-byte pairs 454 → 49), and a filter's
whole is its labels and bits (100,000 object names: 3,580,679 bytes against 3,575,657 a level at a
time, the blocks' offsets the difference).

The work an operation adds, in instructions: on adacachy (AMD Zen 5) exact by `perf stat -e
instructions:u`, on this Mac (Apple silicon, the release gate's VM's architecture) as macOS
counts them retired (`/usr/bin/time -l`); two sizes of each bench, so the difference is the
operation's alone; engine-inflight 76e465ee against engine-cap 1ca1b83f, 2026-10-23:

| work | x86_64 before | after | | arm64 before | after | |
|---|---|---|---|---|---|---|
| a get of a branch (`benches/point_read.rs`, 1 M entries) | 2,049 | 2,068 | +0.9% | 1,667 | 1,552 | -6.9% |
| a seek and its nexts (`benches/seek_cursor.rs`) | 28,518 | 28,566 | +0.2% | 28,368 | 28,167 | -0.7% |
| a leaf lookup and its key, numbers (`benches/leaf_index.rs`) | 2,704 | 2,727 | +0.9% | 2,013 | 1,848 | -8.2% |
| a leaf lookup and its key, object names | 14,707 | 13,962 | -5.1% | 10,102 | 8,223 | -18.6% |
| a compaction's round (`benches/compaction.rs`, 2.1 M entries) | 3.404 G | 3.240 G | -4.8% | 3.571 G | 3.391 G | -5.0% |
| a range filter's build, a key (`benches/surf_build.rs`, 16-byte keys, less making them) | 314.5 | 231.1 | -26.5% | 300.1 | 202.0 | -32.7% |

The one sequence first cost a leaf lookup 7.0% (numbers) and 8.0% (object names) and a get
4.8% on x86_64: a node's labels were two node starts, each a block entry, a sentinel test and an
extra, about 50 instructions a step. A block's extras now run one past its last node, so a
node's labels are one entry and two adjacent extras, and a rank finds its block by a shift where
a division by a count read at run time was a division on every rank: lookups that rank most,
object names', and every lookup on arm64 came out below the per-level layout's. A filter's build
pays the depth, the shared bytes and the reach checked a key and a whole entry's mark: as
147f6149 built it, 11.7 instructions a key more than the base's on x86_64 and a compaction round
1.1% more (1.4% on arm64). Its first forms cost more: one that marked entries with a bit above
the suffix bits, 11.0% a key; one whose cut added an entry from two calls, which left the trie's
add out of line; one telling the mark and the refusal by one compare in `push`. What the build
had paid all along, the base's as much, was three calls left out of line, a level's lookup, a
label's push and a value's push, 77 instructions a key, and a value placed by its index in words
sized for it, 18 more. Inlined, a value written into its words in place (1ca1b83f), a filter's
build costs a quarter to a third less than the base's and a compaction round 5% less.

## 8. The release gate

On the VM lane (Docker Desktop at 96 GiB, the carryover-balanced order of `run7.sh`), each figure
an arm over the base in the same round, medians (`paired.py`); the base engine-inflight 3be9ec0e,
whose production code 0ef22265 keeps. The first build (153c1997) carried the lookup cost §7 found
and gave 1M 1.002 / 0.963 / 1.000, 10M 0.983 / 0.995 / 0.988 and 100M 0.958 / 0.977 / 0.968 for
fill / gets / seeks (four rounds each, R0 7560-7580), its gets' 99th percentile at 100M 2.37 µs
against 1.65. With the node starts and the rank fixed (45ca830c, R0 7600-7620, four rounds):

| keys | fill | gets | seeks |
|---|---|---|---|
| 1M | 1.010 | 0.972 | 0.992 |
| 10M | 0.971 | 1.011 | 0.992 |
| 100M | 0.982 | 1.022 | 0.977 |

The final build (147f6149) beside the base and the base with its layout alone shifted (af047ab5,
engine-follow's A/A recipe), one cycle of six rounds at each size (R0 7630, 7640):

| keys | final over base | layout alone over base | final over the shifted base |
|---|---|---|---|
| 10M | 1.002 / 0.987 / 0.993 | 0.999 / 1.003 / 0.992 | 0.992 / 0.975 / 1.000 |
| 100M | 0.991 / 0.673 / 0.948 | 1.030 / 1.051 / 0.881 | 0.949 / 0.849 / 1.088 |

At 100M every arm's gets fell into one of two modes, about 2.1 or 1.0 million a second, the
shifted base's as much as the others' (research/66 §6 saw the same lane run a round's ratio from
0.43 to 22): the final build drew the fast one in one round of six, the bases in three and two,
and within a mode it ran as they did (fast 2.16 against 1.71-2.13 million; slow 0.71-1.22 against
0.85-1.18). Its seeks at 100M averaged 570 thousand a second against 579 and 529 thousand. Its
fill averaged 4.588 million a second against 4.651 and 4.646 at 10M and 4.075 against 4.143 and
4.270 at 100M, 1.3-1.6% below the base with the base's two layouts 0.1% and 3.1% apart: the
compactions' added work (§7, +1.1% of a round's instructions on x86_64, +1.4% on arm64), a range
filter's build checking the shared bytes and the reach each key and marking its whole entries.

Against RocksDB 11.8.1 and SplinterDB at a fixed 32 GiB cache (45ca830c's rounds):

| keys | over SplinterDB, fill / gets / seeks | over RocksDB, fill / gets / seeks |
|---|---|---|
| 1M | 4.47 / 2.30 / 3.59 | 6.13 / 6.28 / 5.59 |
| 10M | 3.90 / 3.00 / 4.52 | 5.11 / 9.95 / 7.86 |
| 100M | 3.42 / 4.87 / 11.19 | 4.45 / 6.60 / 7.55 |

*Open:* a billion rows waits for the Mac's disk (a 1B case grows the VM's image about 115 GB;
the Mac held 108-116 GB free on 2026-10-23).
