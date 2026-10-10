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

That first layout's cost was its own instructions, not the idea: §5–§6 rebuild it.

## 5. The maplet that routes as fast as a filter: vector quotient blocks

Source, read in full: **[VQF]** Pandey, Conway, Durie, Bender, Farach-Colton, Johnson, *Vector
Quotient Filters: Overcoming the Time/Space Trade-Off in Filter Design*, SIGMOD '21, DOI
10.1145/3448016.3452841, §3–§6 (the authors of the maplets work).

- **Block** [VQF §3.2, §6.1]: a mini quotient filter in a cache line: `b` buckets, `s` slots of
  `r`-bit fingerprints in bucket order, and `b + s` metadata bits giving each bucket's count in
  unary. For 8-bit fingerprints `s = 48`, `b = 80` (128 metadata bits); for 16-bit, `s = 28`,
  `b = 36`. Space is minimized at `s/b = ln 2` and flat near it (Fig. 3).
- **Two choices** [VQF §3.1, Thm. 1, Berenbrink et al.]: an item hashes to blocks `b1`, `b2` and
  goes to the emptier; with high probability no block exceeds `n/m + O(ln ln n)`, so blocks fill
  to 93% (Fig. 4) with no kicking.
- **Lookup** [VQF Alg. 2, §3.3]: select on the metadata gives the bucket's run, one vector
  compare checks its slots; two cache lines, the second independent of the first.
- **Rate and space** [VQF §5]: `ε ≤ 2 (s/b) 2^−r`; `S = (r + b/s + 1) / α` bits an item.

What mantle takes, as a maplet:
- **8-bit slots, the value inside**: a slot is the remainder and the branch's age, `r = 8 − v`.
  One branch: `r = 8`, ε ≤ 0.47% (filters: 0.96% measured). Eight: `r = 5`, ε ≤ 3.75% (eight
  filters: 4.76%). `S ≈ (8 + 1.67) / α` ≈ 10.4 bits at α = 0.93, about the filters' 10.
- **Matching by words**: a slot's remainder matched against a byte pattern in `u64` words, as the
  page cache's index matches tags (util/incmap.rs, hashbrown's portable group); the value read
  from each matching byte.
- **Built offline, both choices in one page**: bundles are immutable, so a maplet is built once,
  greedily by two choices, its second block chosen within the first's page so a lookup reads one
  page cold. A page that overflows rebuilds with more blocks: the build's outcome, not a chance.
- **Merging needs the hashes**: a two-choice block keeps the remainder and the bucket but not
  which choice placed the item, so a maplet is not rebuilt from itself at a new size. Each
  branch keeps its keys' hashes, sorted, 4 bytes a key in its own pages (about 3% of its data),
  and a bundle's maplet is built from its branches' lists by a sequential merge, never reading
  their data — the decoupling maplets promise [SIGMOD abstract; arXiv §4].

## 6. Built for the CPU: one block, one select, an index in memory (benches/maplet.rs)

The two-choice VQF blocks (§5) measured about 330 instructions a route: two blocks, two selects
over 128 bits of dense metadata each, and a select that looped in a byte (aarch64 has no PDEP).
What brought it down, each step measured:
- **Broadword select** (Vigna, WEA 2008; sux `select64`): byte counts summed by one
  multiplication and the byte found by one comparison in every byte; the bit within the byte by
  the same comparison over its bits spread a byte each (no table: the lints forbid its
  indexing). The SuRF trie keeps its halving select: on its sparse words the early exits
  measured faster (benches/surf.rs, 16-byte keys: point p50 84 ns against 125 with broadword).
- **One block a key**: bundles are immutable, so a maplet is a static quotient filter, buckets
  32 to a block in hash order; no second choice to read.
- **One select a run**: the run's start is the highest one below its end, a leading-zero count.
- **The block's place from memory**: an index word a block (page, header length, offset), about
  a bit a key, so a lookup reads its index word and its block's line, not a chain through a page
  table (that chain alone measured about 130 cycles in L2).

Instructions and cycles a route, best of five, everything in memory (2026-10-07):

| bundle | absent: filters / maplet | present: filters / maplet | absent passing | bytes/key |
|---|---|---|---|---|
| 8 × 580k | 189 ins 259 cyc / 183 ins **117 cyc** | 136 / 153 cyc vs 209 / 167 | 4.76% / **1.07%** | 1.25 / 1.38 |
| 8 × 70k | 189 / 194 cyc vs 183 / **71** | 136 / 94 vs 208 / 120 | 4.72% / **1.05%** | 1.25 / 1.39 |
| 3 × 580k | 107 / 82 vs 188 / 95 | 95 / 26 vs 210 / 121 | 1.93% / **0.86%** | 1.25 / 1.30 |
| 1 × 580k | 73 / 37 vs 185 / 107 | 84 / 11 vs 200 / 114 | 0.96% / **0.22%** | 1.25 / 1.38 |

(Each figure includes the bench loop's own 60–80 instructions.) A maplet routes an absent key
through a bundle of eight 2.2–2.7× faster than its eight filters, a present one about as fast,
with a quarter of their false passes. A single branch's Bloom probe is one line and a few
instructions and stays ahead. So bundles of several branches take a maplet and single branches
keep a blocked Bloom filter, both counted in the memory budget; the crossover near three
branches is set by measuring gets once both are wired in.
