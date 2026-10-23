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
