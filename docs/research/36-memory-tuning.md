# 36. Tuning memory between write buffers and the cache

Source: Chen Luo and Michael J. Carey, *Breaking Down Memory Walls: Adaptive Memory Management
in LSM-based Storage Systems*, PVLDB 14(3), 2021, §5 (pp. 246–247), §6 (p. 248). Marked
**[paper]**. Read for E7 and E8 (docs/design/engine-structure.md §5–§6): how a shard divides the
memory its owner spares between its memtables, its page cache and its write runs.

## 1. The memory tuner [paper §5]

- **Goal.** With `M` the memory for both and `x` the write memory, minimize the I/O cost per
  operation `cost(x) = ω·write(x) + γ·read(x)` (pages per operation). `ω` and `γ` weigh writes
  against reads for the device: on SSDs a write may be made dearer than a read [paper §5.1].
- **Method.** Online gradient: the tuner estimates `cost′(x) = ω·write′(x) + γ·read′(x)` from
  statistics of the last tuning cycle and moves `x` toward `cost′(x) = 0` [paper §5.1, Fig. 5].
- **Write cost's derivative** [paper §5.2, Eq. 3–5]. From the merge writes counted in the last
  cycle, `merge(x)`: `write′(x) = −merge(x) / (x · ln(|L_N| / (a·x)))`, `|L_N|` the last level's
  size and `a` the active component's share, scaled by the share of flushes triggered by memory
  (not by log truncation), since more memory does not reduce flushes the log forces.
- **Read cost's derivative** [paper §5.3, Eq. 6]. A *simulated cache* holds the page IDs of
  pages evicted from the buffer cache, `sim` bytes of them (32 MB in the paper). A disk read of
  a page it holds is a read a larger cache would have saved: `saved_q` for queries and
  `saved_m` for merges, per operation. Then
  `read′(x) = (saved_q + saved_m) / sim + write′(x) · read_m(x) / merge_m(x)`, the second term
  for the merge reads more write memory also saves.
- **Step** [paper §5.4]. Newton–Raphson on `cost′`, approximated as linear from the last `K = 3`
  allocations: `x_{i+1} = x_i − cost′(x_i) / A`. With too few samples, or a step that does not
  reduce the cost, a fixed step of 5% of the total. A region gives up at most 10% of its memory
  a step (diminishing returns on both sides).
- **Stopping** [paper §5.4]. No change if the step is under 32 MB or the expected reduction under
  0.1% of the I/O cost.
- **Cycle** [paper §5.4]. When the log records since the last tuning pass the maximum log
  length, so log-forced flushes are counted whole; otherwise every 10 minutes.
- **Shape** [paper §5.5, Fig. 6]. `cost(x)` need not be convex (`read_m` is not monotone), but on
  YCSB write-heavy and TPC-C it had one global minimum.

## 2. What mantle takes

- A shard's memory budget, stated by its owner, is divided between its memtables, its page
  cache and its write runs; filters, leaf indexes and views are what the data needs and are
  counted, not tuned.
- The read side's marginal benefit comes from the S3-FIFO cache's own ghost queue
  (store/cache.rs), which already keeps the IDs of evicted pages: a miss on a page the ghost
  holds is the paper's `saved` read, with no second structure.
- The write side's derivative is the Bε-tree's: entries written per put as a function of the
  memtable size, measured per cycle from the trunk's `entries_written`, in place of the paper's
  leveled-LSM formula.
- The runs out at once are not the paper's: they follow from the device's measured write
  bandwidth and latency (in flight = bandwidth × latency, Little's law), the backpressure the
  10 M fill's p99.9 puts waited on (docs/design/engine-structure.md §6).
- The paper's constants (32 MB `sim`, 5% and 10% steps, 32 MB and 0.1% stops, 10-minute cycle)
  are its choices on its hardware: mantle measures its own (CLAUDE.md §4–5).
- The step is the paper's: Newton on the difference of two regions' gains a MiB, fitted by least
  squares to the last `K = 3` allocations between the pair, in exact integers; the fixed 5% step
  when there are fewer samples or the fit does not diminish, and the 10% donor bound always.
  With three regions the pair is the one each step moves between, and a new pair starts its
  samples over.
- A region's ghost must outlive its resizes: the record cache first rebuilt its ghost with its
  ring, so each step's next cycle saw ten times fewer ghost hits (16.9M ns saved, then 0.17M)
  and the tuner reversed. Measured 2026-10-07, 10M keys, skewed reads.
- Memory a step takes from a region must leave it. The page cache kept one slab and shrank it by
  moving pages down and truncating, which keeps the allocation: peak RSS 913 MiB where slots
  owning their buffers, dropped on a shrink, give 802 MiB at the same split (10M keys, 10M skewed
  reads, 320 MiB budget, 2026-10-07). Shrinking also no longer copies pages: the longest page
  cache resize fell from 3.3 ms to 0.84 ms.
