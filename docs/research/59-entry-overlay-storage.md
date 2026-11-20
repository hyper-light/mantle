# 59. An entry's overlay: an ordered index kept from entry to entry

The overlay an entry's commands read and write through (docs/design/replica.md §2) holds the
keys the entry wrote in key order: a command reads a key's value (a point lookup), scans for the
first row in a range (an ordered seek), deletes and clears ranges, and the engine takes the
entry's batch in key order. It is kept by the range from one entry to the next, so it allocates
nothing for its rows once warm (`crates/meta/src/overlay.rs`). These are the sources its
structure, constants and bound rest on.

## 1. Skip lists (Pugh 1990)

W. Pugh, "Skip Lists: A Probabilistic Alternative to Balanced Trees", Communications of the ACM
33(6):668–676, June 1990.

- A skip list keeps its elements in a sorted linked list, each element also linked at a random
  number of higher levels: an element at level i is at level i+1 with probability p. A search
  walks the top level forward while the next key is below the target, then drops a level;
  insertion and deletion splice the element in or out at each of its levels, given the last
  element before it at each level (the "update" vector the search fills).
- Expected search cost is O(log n) for any fixed p; with p = 1/4 the expected number of
  comparisons is close to that of p = 1/2 and the expected pointers per element is
  1/(1 − p) = 4/3 (against 2 for p = 1/2). Pugh recommends p = 1/4 unless the variability of
  running time is a primary concern.
- MaxLevel should be L(N) = log_{1/p} N, N an upper bound on the elements: levels past it add
  nothing to the expected cost. Mantle's slots are named by `u32` links, N = 2^32, so with
  p = 1/4, MaxLevel = log_4 2^32 = 16 (`LEVELS`).
- Levels are drawn independently of the keys, so the expected costs hold for any keys and any
  order of insertion that does not depend on the levels drawn.

Why a skip list rather than the map it replaced: `BTreeMap` allocates its nodes as it grows and
frees them as it is dropped or cleared, and keeps no capacity, so a map made for each entry and
each command allocated per command and per few rows (crates/meta/tests/encoding_allocs.rs pins
the counts). A sorted vector reused across entries allocates nothing once warm, but an insertion
moves the keys after it, quadratic in an entry's rows, and an entry may write many (the largest
an entry holds, `wire::largest_entry_bytes`, is a completion of 10,000 parts). A skip list over
slots and links held in vectors keeps both: O(log n) expected per operation, links that splice
in place, and vectors that are cleared keeping their capacity. A deletion here unlinks a range
whole: the last element below the range's start at each level takes the link of the last
element below its end at that level.

## 2. Xorshift generators (Marsaglia 2003)

G. Marsaglia, "Xorshift RNGs", Journal of Statistical Software 8(14), 2003.

- A xorshift generator applies x ^= x << a, x ^= x >> b, x ^= x << c to a nonzero state; for
  64 bits the paper lists the triples that give the full period 2^64 − 1, and gives as its
  example `xor64` the triple (13, 7, 17) with the seed 88172645463325252.
- The overlay draws each new slot's level from it: one level, and one more for each pair of low
  bits that are zero, so each level holds a quarter of the one below (p = 1/4, §1).
- The draw starts at that seed at each clearing (`SEED`), so an entry's slots take the same
  levels in the order they are made, whatever their keys: an entry no larger than one held
  before needs no more links, and the allocations an entry makes are the same on every run,
  which the allocation tests pin exactly.

## 3. Contracting a table (Cormen, Leiserson, Rivest and Stein)

T. H. Cormen, C. E. Leiserson, R. L. Rivest, C. Stein, Introduction to Algorithms, 3rd ed.,
MIT Press, 2009, §17.4 "Dynamic tables".

- A table that doubles when full and halves when its load factor falls below 1/4 (not 1/2)
  has constant amortized cost per insertion and deletion: after a contraction the table is
  half full, so it can neither expand nor contract again until a number of operations
  proportional to its size has paid for it. Contracting at 1/2 instead lets an alternating
  insert and delete at the boundary cost a whole copy each.
- The overlay applies the rule per entry: as an entry ends, each vector of its storage that
  held less than a quarter of its capacity is halved. Entries of a steady size never shrink
  it (a vector grown by doubling holds more than half its capacity at its largest entry), and an
  outsized entry's capacity is given back by halves over the entries after it.
