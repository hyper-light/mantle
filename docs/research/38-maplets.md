# 38. Maplets, measured against mantle's filters

Sources, marked as cited:
- **[arXiv]** Bender, Conway, Farach-Colton, Johnson, Pandey, *Time To Replace Your Filter: How
  Maplets Simplify System Design*, arXiv 2510.05518v1 (October 2025), §2–§4. Read in full.
- **[SIGMOD]** Conway, Farach-Colton, Johnson, *SplinterDB and Maplets: Improving the Tradeoffs in
  Key-Value Store Compaction Policy*, PACMMOD 1(1), SIGMOD 2023, DOI 10.1145/3588726. Its body is
  behind ACM's bot check from every host tried (ACM, VMware Research, the authors' pages link only
  ACM); only the abstract is read, so its numbers stay abstract-level here, as in research/34 §2.
- **[src]** SplinterDB's routing filter, `routing_filter.c`, research/34 §1: SplinterDB as shipped
  already routes a pivot's keys to its branches with one maplet.

## 1. What a maplet is [arXiv §2–§3]

- A space-efficient map, as a filter is a space-efficient set: a query of `k` returns `m[k]`,
  equal to the map's value with probability at least `1 − ε`, and never less than it under the
  value's order (one-sided error). For a set of branches, the answer is a superset of the
  branches holding `k`.
- Built from a perfect-hashing filter (quotient, cuckoo): "we simply store an array V of values
  and use the filter to map keys to slots of V", or widen the filter's own slots to hold them.
- Space: O(log 1/ε + v) bits a key, `v` the value's bits: "the bits that were used for FPR
  reduction are exactly used for the SSTable ids" [SIGMOD abstract].
- Merging: maplets over sorted fingerprints merge sequentially without reading the data they
  describe, so filter compaction is decoupled from data compaction [arXiv §4; src].
- Paging: "Maplets can be paged out to storage, and then only the blocks required to answer a
  query can be paged back in at query time. This typically incurs a single IO, which can save the
  up to g IOs required to query the SSTables" [arXiv §4].

## 2. What mantle measured (2026-10-07, benches/shard_db.rs with temporary counters)

Per get, 50/50 puts and gets, keys uniform, 16-byte keys and 100-byte values, fanout 8:

| | 10M keys | 100M keys |
|---|---|---|
| bundles a get visits | 1.80 | 2.93 |
| branch filters it probes | 3.59 | 5.01 |
| filters passed (true and false) | 0.60 | 0.65 |

After the shard's maintenance has settled, 10M keys: 2.88 probes a get, 0.003 false passes.
Filter memory, resident and outside the memory budget: 234 MB at 100M keys (2.34 B/key), beside
range filters at 2.08 B/key and REMIX views at 1.55 B/key.

## 3. What follows

- **Not a faster in-memory get.** A blocked Bloom probe reads one cache line. A maplet lookup
  reads an index and a bucket: two. With 2.9 lookups in place of 5.0 probes, a get whose filters
  are all resident reads about as many lines either way. The 5–40 filter probes SplinterDB
  replaces come from its larger bundles and its on-disk filters, neither of which mantle has at
  these sizes.
- **Filters that scale.** Resident filters grow with the keys: about 2.3 GB at a billion keys a
  shard, before the range filters and views, all outside the budget. A maplet kept in store pages
  with a small in-memory index is read through the page cache, so its pages are governed by the
  budget the memory tuner divides (research/36), and a lookup whose page is not cached costs one
  page read a bundle, not one a branch.
- **So E5 builds paged maplets.** One per bundle, values the branch's age in the bundle; a
  branch's own (value-free) maplet replaces its Bloom filter, built at its pack; a bundle's is
  merged sequentially from its branches' as they arrive. Fingerprints come from the key's xxh3
  hash a get already computes.
- **Rate.** A lookup's false-positive rate is about `load · 2^−r` for `r` remainder bits; `r = 7`
  keeps it at or under the Bloom filters' 0.8% a probe (research/34's `n·2^−h` with `h = b + r`,
  `b` the bucket bits), now once a bundle rather than once a branch.
- **To measure, not assume:** gets with every maplet page cached against today's resident
  filters (instructions and cycles a get, as research/36's footprint work measured), gets under a
  budget smaller than the filters, and the bytes a key each takes.

## 4. The maplet built and measured (benches/maplet.rs, 2026-10-07)

`crates/engine/src/maplet.rs`: fingerprints of `hash_bits(most)` bits, sorted, in pages of
64-bucket blocks (unary headers then packed remainder‖value), a table of block offsets at each
page's head; built in two streaming passes, merged from pages alone. Every key finds its
branches; false matches track `load · 2^−r` (775 against 781 expected at one key, 470 against
447 at 300,000). All pages and filters in memory, xxh3-like hashes, best of five passes:

| bundle | filters ns a route, present / absent | maplet | absent keys passing, filters / maplet |
|---|---|---|---|
| 1 branch × 580k | 2.3 / 8.3 | 37.7 / 36.2 | 0.96% / 0.43% |
| 8 × 580k | 31.3 / 60.7 | 68.9 / 52.9 | 4.76% / 0.27% |
| 8 × 70k | 25.0 / 51.3 | 43.5 / 36.3 | 4.72% / 0.27% |

- Routing in memory costs the maplet 36–69 ns where a single filter, also resident in L2, costs
  2.3: the difference is instructions (the block table, the unary scan with select, each entry's
  bits), not cache misses. With the 2.9 bundles and 5.0 filters a get meets at 100M keys, a get
  routes slower with maplets while every page is cached.
- Space: 1.80 B/key a full page; whole-page packing of 64-bucket blocks under a power-of-two page
  rule left pages half full (3.6 B/key measured), which exact page packing would fix. At the
  filters' false-pass rate (`r` of about 3) a maplet would take about what they do, 1.2 B/key.
- What it wins: 18× fewer false passes (each a page read) and, once filters no longer fit the
  budget, one page a bundle where filters read one a branch.

**Decision pending with the owner:** not on the read path at mantle's measured sizes; the
filters, range filters and views (about 6 B/key, 600 MB at 100M keys) first counted in the
memory budget; maplets revisited where a shard's filters exceed it.
