# 39. How much memory a shard's filters take, and where

Source, read: **[Monkey]** Dayan, Athanassoulis, Idreos, *Monkey: Optimal Navigable Key-Value
Store*, SIGMOD 2017, DOI 10.1145/3035918.3064054, §2 and §4.1–§4.2 (Eq. 1–8), Appendix B cited
there for the derivation. Read for the owner's charge (2026-10-07): the filters' 10 bits a key
are RocksDB's default, not data-derived, and the filters sit outside the memory budget.

## 1. Monkey [§2, §4.1]

- A Bloom filter of `bits` over `entries` with the optimal number of hash functions has
  `FPR = e^(−(bits/entries)·ln(2)²)` (Eq. 2).
- A zero-result lookup's expected I/O is the sum of the false-positive rates of the filters it
  probes (Eq. 3): `R = Σ p_i` with leveling, `(T−1)·Σ p_i` with tiering.
- The filters' memory for rates `p_1…p_L` is `M_filters = −(N/ln(2)²)·((T−1)/T)·Σ ln(p_i)/T^(L−i)`
  (Eq. 4).
- Minimizing `R` under `M_filters` (Lagrange multipliers, Appendix B) gives each level a rate
  proportional to its entries (Eq. 5–6, Fig. 6): `p_i = p_1·T^(i−1)` until a level's rate
  reaches 1, below which it has no filter. Every store they knew uses one rate for all.
- §4.3–§4.4 co-tune `M_filters` with the buffer by modeling throughput from the workload's mix.

## 2. Mantle's tree, and what changes

- A get does not probe one run a level: it probes the branches of the node its key falls in at
  each level (docs/design/engine-structure.md §4). Branch `j` is visited by a fraction `v_j` of
  gets, which the keys' distribution and the tree's shape set and gets measure.
- The cost to minimize is the false positives a get meets: `R = Σ_j v_j·p_j`, under
  `Σ_j −n_j·ln(p_j)/ln(2)² ≤ M`. The same Lagrangian gives `v_j = λ·n_j/p_j`, so
  **`p_j = λ·n_j / v_j`** (capped at 1: such a branch keeps no filter): a branch's rate in
  proportion to its entries over its visits. Monkey's levels are the case `v_j = 1`.
- **The budget is the tuner's**: at the optimum every bit saves the same `λ·ln(2)²` false
  positives a get per bit-key, and a false positive costs a page read whose time the store
  measures. So the filters are a region of the shard's memory priced like the others
  (research/36), not a fixed 10 bits a key.

## 3. What makes it possible: filters rebuilt from hashes

- A branch keeps its keys' 32-bit hashes in its own pages (research/38 §5). If a filter's block
  and probes derive from those 32 bits, any branch's filter can be rebuilt at any size from its
  list, sequentially, without reading its entries: the tuner can then move filter memory as it
  moves the caches'.
- 32 bits of hash suffice while a filter's rate stays well above `n · 2^−32`, the rate two keys'
  hashes collide; for the shard sizes measured (10^7–10^8 keys a shard, 10^5–10^7 a branch)
  that is 10^−5 to 10^−3 of a get, far under the rates a budget sets.

## 4. Steps, each measured against dev

1. Filter blocks and probes from the 32-bit hash; false-positive rate measured unchanged.
2. Visits `v_j` counted per branch on gets; each branch's rate `p_j = λ·n_j/v_j`.
3. The filters as a region of the memory tuner, `λ` set by its budget; filters resized from
   their hash lists in idle time.
