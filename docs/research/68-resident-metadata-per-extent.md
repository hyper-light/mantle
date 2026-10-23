# 68. Resident metadata per extent, stated

A branch keeps in memory, while it lives, its point filter, its range filter, its leaf index, its
tree pages' entry counts, its extents' numbers and its record (node.md §1.4; `trunk::Memory`).
Until now the engine took these as held: `tests/engine_memory.rs` added what its branches held to
its stated buffers. This note states them from the configuration alone, a bound for each extent
a branch holds at any keys up to a stated length, and records the one part that had no bound: a
compaction's point filter.

Lines marked **Reading** are this note's.

## 1. A compaction's point filter had no bound

A filter whose key count is known only as a bound while its branch is built (a compaction's,
duplicates merging and tombstones dropped at the bottom) was made for the bound
(`Builder::new(store, Keys::AtMost(remaining.min(per)))`, `branch/merge.rs`), its blocks made even,
and halved once the count was known while half still gave its keys 10 bits each
(`Filter::fit`). Halving is the classic shrink: "Suppose that the size of the filter is a power
of 2. To halve the size of the filter, just OR the first and second halves together" (Broder and
Mitzenmacher, *Network Applications of Bloom Filters: A Survey*, Internet Mathematics 1(4), 2004,
§2.4). A count that is not a power of two halves only while it is even, so a branch whose merge
cut its keys well below the bound kept the bound's filter:

| bound, keys | blocks made, needed | kept by halving | bits a key |
|---|---|---|---|
| 800,000, 100,000 (an eighth of the inputs) | 15,626, 1,954 | 7,813 (500,032 bytes) | 40.0 |
| 400,000, 30,000 | 7,814, 586 | 3,907 (250,048 bytes) | 66.7 |
| 2,000,000, 10 (a range deleted, ten keys left) | 39,064, 1 | 4,883 (312,512 bytes) | 250,010 |

**Reading:** a branch's filter so grew with its compaction's inputs, not its keys, and a range
whose keys are mostly overwritten or deleted kept filters past any statement per extent; nothing
reclaimed them until the branch's next compaction.

**The fold.** A key's block is its hash's high 32 bits `x` scaled into the block count,
`⌊x·n / 2^32⌋` (Lemire, *Fast Random Integer Generation in an Interval*, ACM TOMACS 29(1), 2019),
and its probes within the block do not depend on `n`. For any `r` dividing `n`,
`⌊⌊x·n / 2^32⌋ / r⌋ = ⌊x·(n/r) / 2^32⌋`, since `⌊⌊y⌋ / r⌋ = ⌊y / r⌋` for a whole `r`. So OR-ing
blocks `r i .. r i + r` into block `i` makes exactly the filter its keys would have made at
`n / r` blocks: halving is the case `r = 2`, and every divisor of the count is a size the filter
can fold to, with no key lost and no bit more than building at that size sets
(`branch::filter::tests`, which compares each fold with the filter its keys make at its size).

**The count.** A filter folds to within twice any smaller count of blocks when each of its count's
divisors, in order, is at most twice the one before: for `need` blocks, the least divisor at or
above `need` follows the largest below it, which is less than `need`, so it is less than twice
`need`. These are OEIS A174973, "Numbers whose divisors increase by a factor of at most 2",
which Saias (1997) called 2-dense; a count `∏ p_j^e_j`, primes ascending, is one "if and only if
pk <= 2*Product_{j < k} p_j^e_j" (A174973). Every count `2^a m` with `m <= 2^(a+1)` is one (its
powers of two and the doublings of `m` each at most double the one before), so the least 2-dense
count at or above any bound is found by a search ending at the least such `2^a ⌈bound/2^a⌉`.
Measured over every bound from 2 to 19,999 blocks (the script below, 2026-10-23):
the least 2-dense count is 0.1% above the bound on average, 9.6% at most from 64 blocks up, and
the search tries at most 36 counts; the construction alone would be 11.6% at most. A count tried
fails at its first divisor gap, so most cost a few remainders.

A bounded filter is now made at the least 2-dense count at or above its bound, and `Filter::fit`
folds it to the least divisor that still gives its keys 10 bits each: fewer than twice the blocks
they need, so between 10 and 20 bits a key, and a filter made for exactly its keys (a memtable's
packing) is never touched. **Reading:** the alternative, RocksDB's, keeps every key's 64-bit hash
while the table is built and makes the filter at the end: "a builder will keep a set of 64-bit
key hashes and only build the filter in Finish when the final number of keys is known"
(`table/block_based/filter_policy_internal.h`, RocksDB abeebd963). That is exact, but it holds
8 bytes a key while building where mantle's builder holds the filter's 1.25, and puts every
probe of the branch on the step that seals it, where mantle's land one an entry as the merge
goes; the fold keeps both and costs the rounding.

What it changed on disk: a compaction's filter is one or two blocks larger where its bound was
not 2-dense (`tests/branch_bytes_golden.rs`: its four compactions' branches 508 to 510 blocks, 254
to 255, 49 to 50, 158 to 160; the built branches and every other byte of the descriptors the
same). The two cases above now keep 1,955 blocks (10.0 bits a key) and 782 (13.3 bits).

## 2. The bound

Everything a branch holds resident is a buffer taken at its size and the record holding them; the
bytes its drop frees, counted by the allocator, equal what `trunk::Memory` adds for it at every
shape below (`tests/resident_memory.rs`). Each part's bound, for a branch of `n` entries of keys
at most `k` bytes, `L` leaves, `P` tree pages and `E` extents (`branch::resident_bytes`):

- **Point filter**: at most twice the blocks its keys need, `2 ⌈10 n / 512⌉ · 64` bytes (§1;
  `filter::fitted_most_bytes`).
- **Range filter** (`fst::surf`): a key's entry runs one byte past the larger of what it shares
  with each neighbour, past the bytes every key shares and at most the depth `d = min(16, k)`, or
  ends at the reach or with the key. Write `s_i` for what key `i` shares with key `i + 1` so
  measured, `s_0 = s_n = 0`. Past what it shares with the key before, key `i`'s entry holds at
  most `1 + max(0, s_i - s_{i-1})` labels; a key a whole entry holds takes none. The `s` start and
  end at none, so their rises are half their steps, `R = Σ |s_i - s_{i-1}| / 2`, and the labels
  are at most `n + R`. A label leads to a node only where a later key's labels go on from it, at
  most the key's rise, so the nodes are at most `R + 1`. Each step is at most `d`, so
  `R <= n d / 2`. Within a leaf, both of key `i`'s neighbours share at least the leaf's prefix
  with it and at most the key, so its step is at most its suffix there, `σ_i`, which the leaf
  stores past the key's 11 least bytes (a slot of 6 and a fixed 5: `branch::LEAST_ENTRY`); a
  leaf's first and last key step at most `d`. So `R <= Σ σ_i / 2 + L d <= (B - 11 n) / 2 + L d`,
  `B` the leaves' payload (`branch::range_rises`). The trie's bytes follow from its labels, nodes
  and values (`fst::trie::most_bytes`): a byte and two bits a label, a bit a node, a rank entry
  each 512 bits of each, and its node starts, 8 bytes and 257 two-byte extras each block of 256
  nodes, so about 2 bytes a node; its 8 suffix bits a key; its dense levels at most a 64th of the
  sparse bits below them; its prefix at most twice `k` and 8.
- **Leaf index**: a separator a leaf, at most `min(257, k)` labels past the bytes every key
  shares (`store::LEAF_INDEX_DEPTH` and a byte), as many nodes, a 32-bit value each.
- **Counts, extents and record**: 2 bytes a tree page, 8 an extent, and the record,
  `size_of::<Branch>()`, 1,576 bytes on x86_64.

**Per extent.** An extent of `p` pages of `c` payload bytes holds at most `p ⌊(c - 5) / 11⌋`
entries (`branch::most_entries`), at most `p` leaves, `p` tree pages and payload `p (c - 5)`.
Every part above grows at most in proportion to its counts, its constants once a branch, so a
branch of `E` extents holds at most `E` times the bound for one; the rises' two bounds, one rising
with `n` and one falling, are written for `E` extents as `E` times their per-extent forms at
`n / E`, so the same holds for them. Over any count of entries the rises are most where
`n d / 2` meets `(B - 11 n) / 2 + p d`, at `n = (B + 2 p d) / (d + 11)`, and the labels, `n + R`,
are most there too; the per-extent bound takes each part at its own most
(`branch::resident_per_extent`, `shard_db::resident_per_extent`). An extent is one branch's: a
branch's extents are taken fresh by its builder and each held once (`store::Store::retain` has no
caller), so an engine's branches hold at most their extents times it.

At the range's floor, 24 KiB pages two an extent (`branch::least_page(4096, 2132, 18699)`), the
statement and its parts, in bytes an extent:

| keys at most | entries | labels | rises | point filter | range filter | leaf index | counts, extents, record | an extent |
|---|---|---|---|---|---|---|---|---|
| 8 bytes | 4,462 | 12,930 | 10,344 | 11,264 | 49,486 | 1,360 | 1,588 | 63,698 |
| 16 bytes | 4,462 | 16,388 | 14,567 | 11,264 | 62,972 | 1,392 | 1,588 | 77,216 |
| 2,132 bytes | 4,462 | 16,388 | 14,567 | 11,264 | 67,204 | 7,374 | 1,588 | 87,430 |

The range filter's nodes take 32,216 bytes of its 62,972 at 16-byte keys, their starts' extras
30,276. Counted from the entries alone, `n (1 + d/2)` labels and as many nodes, the range filter's
part was 87,932 bytes at 8-byte keys, 149,814 at 16 and 154,046 at 2,132; bounding the rises by
the suffixes the leaves store, and the nodes by the rises, takes it to 49,486, 62,972 and 67,204.

## 3. Measured

One branch of each shape at the floor (`tests/resident_memory.rs`, fedora x86_64, 2026-10-23):
what its drop freed and its record, against the branch's bound from its own counts and against
its extents times the per-extent statement, at its own longest key and at the range's 2,132.

| keys | entries | extents | held, of its data | held an extent | branch stated / held | extents stated / held, own key | at 2,132 |
|---|---|---|---|---|---|---|---|
| 8-byte, no values (densest) | 100,000 | 35 | 355,202, 26.3% | 10,149 | 2.77 | 6.28 | 8.61 |
| 16-byte numbers, 100-byte values | 100,000 | 268 | 462,394, 3.6% | 1,725 | 7.38 | 44.8 | 50.7 |
| object names, 100-byte values | 100,000 | 306 | 573,042, 4.0% | 1,873 | 6.05 | 41.3 | 46.7 |
| 64-byte pairs, no values | 20,000 | 34 | 245,446, 17.2% | 7,219 | 2.82 | 10.7 | 12.1 |
| 16-byte pairs a byte apart, no values | 20,000 | 18 | 286,304, 48.5% | 15,906 | 2.37 | 4.85 | 5.50 |
| the same, one pair in eight parting at each depth | 20,000 | 17 | 518,860, 88.0% | 30,521 | 1.31 | 2.53 | 2.86 |
| 2,132-byte pairs, no values | 4,000 | 204 | 122,642, 1.2% | 601 | 3.90 | 145 | 145 |
| 2,132-byte pairs, largest values | 1,000 | 555 | 204,876, 0.8% | 369 | 4.67 | 237 | 237 |
| densest, a compaction's last branch, an eighth of its inputs | 100,000 | 35 | 355,266, 26.3% | 10,150 | 2.77 | 6.28 | 8.61 |
| densest, 30,000 keys of a bound of 400,000 | 30,000 | 11 | 120,277, 28.8% | 10,934 | 2.40 | 5.83 | 8.00 |

**Reading:** the statement is a bound for any keys up to its length, so it sits at the shape that
fills each part most: the pairs parting at every depth hold 30.5 KB an extent against 77.2 KB
stated, while realistic shapes hold 1.7-10 KB. Its largest parts are the range filter's node
starts, about 2 bytes a node where a block of 256 nodes holds more labels than nodes, and the
record, 1,576 bytes a branch.

In an engine at the floor, filled with paced puts and a scan every 97 (`tests/engine_memory.rs`,
fedora x86_64, 2026-10-23), the branches held 5,808,443 bytes for 672 branches of 1,346 extents
at 8-byte keys and 1.28 million rows, 4,315 bytes an extent, against 85,737,508 stated; and
264,340 bytes for 15 branches of 31 extents at 2,132-byte keys and 32,000 rows, against
2,710,330. The trunk's own records, its nodes and pivots, their keys and its lists, held 517,752
and 106,992 bytes there, and are counted as held. A node over the fanout splits only once its
in-flight bundles are empty and its parent's pivot is (`trunk::upkeep`, `take_change`), so its
children's splits can add pivots meanwhile; and nodes never merge, so their count follows the
splits a range has seen, not the data it holds. Stating them waits on a bound for both.

## Appendix: the 2-dense counts measured

`python3 -I gaps.py 20000` printed "v<20000: search worst gap 35 blocks, worst rel (v>=64) 0.0959,
mean rel 0.00105, most candidates 36; construction worst rel (v>=64) 0.1163".

```python
# Smallest 2-dense number (consecutive divisors at most a factor two apart) at or above v:
# its gap from v, the candidates a search tests, and the 2^a*m construction's gap.
import math, sys
def divisors(n):
    small, large = [], []
    d = 1
    while d * d <= n:
        if n % d == 0:
            small.append(d)
            if d * d != n:
                large.append(n // d)
        d += 1
    return small + large[::-1]
def dense(n):
    ds = divisors(n)
    return all(b <= 2 * a for a, b in zip(ds, ds[1:]))
def construct(v):
    a = 0
    while True:
        m = -(-v // (1 << a))
        if m <= (1 << (a + 1)):
            return m << a
        a += 1
top = int(sys.argv[1])
worst_rel, worst_gap, worst_tests, worst_c = 0.0, 0, 0, 0.0
sum_rel = 0.0
n = 0
for v in range(2, top):
    c = v
    tests = 0
    while True:
        tests += 1
        if dense(c):
            break
        c += 1
    g = c - v
    rel = g / v
    u = construct(v)
    worst_c = max(worst_c, (u - v) / v if v >= 64 else 0)
    if v >= 64:
        worst_rel = max(worst_rel, rel)
        sum_rel += rel
        n += 1
    worst_gap = max(worst_gap, g)
    worst_tests = max(worst_tests, tests)
print(f"v<{top}: search worst gap {worst_gap} blocks, worst rel (v>=64) {worst_rel:.4f}, mean rel {sum_rel/n:.5f}, most candidates {worst_tests}; construction worst rel (v>=64) {worst_c:.4f}")
```
