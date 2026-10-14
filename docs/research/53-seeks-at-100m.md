# 53. Seeks at 100M: the phase that consolidates, and what a merged entry costs

Sources.
- **[vm]** `benchmark-results/mantle-splinterdb-20261013`, runs `n100000000-r261..263-*`: mantle
  `c4eb1e8` with the ranges bench's `--queries-again` (`q_settled`: the gets and seeks run a second
  time once the queries' flush has left the trunk idle), RocksDB 11.8.1 with a 32 GiB block cache
  and SplinterDB with 32 GiB, three interleaved rounds at 100M in Docker Desktop's Linux VM on this
  Mac (18 cores), the ranges path (`benches/ranges.rs`), one shard, 16-byte keys, 100-byte values,
  seed 301, `--machine-memory`. RocksDB's reads follow `waitforcompaction`; its compactions run
  during its fill.
- **[spread]** the same directory's runs `n100000000-r241..253-m_*`: every hyper-rt arm of
  2026-10-14, one seek phase each.
- **[mac]** macOS, the ranges path at 100M, 1M seeks of 10 after the fill, interleaved; a
  diagnostic build of `a2266b2` (never kept) running a second seek phase over another key stream on
  the store the first consolidated.
- **[merge]** `benches/compaction.rs` at `c4eb1e8` on macOS, 10M entries, three rounds of each shape
  (research/50 §12); **[msample]** macOS `sample` of its settle rounds, 4,509 samples of the merging
  thread.

## 1. Step one: do seeks need it?

Yes, the phase that consolidates. At 100M [vm] the seek phase ran 152K, 162K and 168K seeks a
second, 2.51×, 2.53× and 2.89× RocksDB's 61K, 64K and 58K in the same rounds (3.59×, 4.34× and 4.11×
SplinterDB's 42K, 37K and 41K); the target is four to five times both. The same seeks on the trunk
they had consolidated ran 344K, 348K and 335K: 5.67×, 5.44× and 5.77× RocksDB and 8.1×, 9.3× and
8.2× SplinterDB, at a p50 of 2.75–2.88 µs against 3.58–5.12 µs. What the first phase lacks is the
consolidation it pays for.

## 2. Step two: how much?

Four times RocksDB is 232K–256K seeks in the phase that consolidates: its 5.96–6.57 s cut to about
4 s. On the consolidated trunk the same million seeks take 2.87–2.98 s; the consolidation adds
3.0–3.7 s to the phase.

## 3. What the phase consolidates

Every run starts the seek phase unconsolidated and ends it consolidated [spread]: 0 consolidations
and 365–549 branches before it, 70–91 consolidations after, one branch a pivot. The seeks' rent buys
them (research/43, research/50 §3), and the phase's rate is how soon the merging finishes on the busy
host: one binary's phase ran 94K–159K seeks a second, a paired round's ratio 0.64–1.56 [spread].

In the queries of each round [vm] the merges wrote 94M–101M entries: 31M–38M flushed down the index
(`pivot_entries`) and 63.2M settled into the leaves (`settle_entries_seeks`, every live entry), with
20–49 s of the workers' merge time while the queries ran 6.7–7.3 s. A third of the writes moved
entries one level down that the leaves' settles then rewrote.

The draft's earlier measurements on macOS [mac], one round each by consolidation mode:

| mode | first seek phase | second seek phase | sources a seek | consolidations |
|---|---|---|---|---|
| never | 48K | 64K | 9.8 | 0 |
| measured (the default) | 124K | 286K | 1.5 | 87 |
| always | 75K | 381K | 1.2 | 161 |

On the unconsolidated store a seek opens about ten sources one after another; consolidated, one. A
source's open is a chain of misses: the leaf's page header, the entry's line in the copy of its key
and value, the page's binary search, and before them the branch's trie to the leaf.

## 4. What a merged entry costs

1,930 instructions a settled entry and 2,079 a compacted one [merge], 124–138 and 132–139 ns on the
Mac, as research/50 §12 left them. Where a settle's time goes [msample]:

| share of the merging thread | what |
|---|---|
| 50% | the branch builder: an entry copied into the page's staging (`Page::push`, 12.5%), its key and value appended (`Builder::add_inner`, 12.4%; `Builder::insert`, 6.2%), the page encoded from the staging (`Page::encode`, 3.8%), and the byte copies these make (`memmove` 14.6%, `memcpy` 1.3%) |
| 9.2% | the merge's choice of its next entry (`Merge::pick` 6.4%, `next` 2.8%) |
| 9.1% | the range filter's trie and SuRF (LIST's prefix filter, research/50 §12) |
| 8.8% | `pread` and `pwrite` |
| 4.3% | key comparisons (`memcmp`) |
| 3.9% | the pages' CRC |

An entry's bytes are copied from its input page into the page's staging, from the staging into the
encoded page, into the cache's copy of the written page, and by the kernel's write: four copies of
116 bytes, where the page's layout (its shared prefix and offset table ahead of the entries, the
prefix known once the page is full) is what makes the builder stage before it encodes.

## 5. Steps three to five: the levers

1. **Fewer writes.** A consolidation's index flushes rewrite a third of its entries one level down,
   and the leaf's settle rewrites each again: one merge from every source on a leaf's path into its
   settled branch writes each entry once.
2. **Cheaper writes.** A page laid out so that entries are written once as they come, the table and
   prefix after or beside them, copies each entry once on its way out of the builder.
3. **Earlier.** Consolidating in the idle time after a fill moves the cost out of the seeks where a
   store has idle time; a bench with none only moves it.

Each is built as a variant and run against main, RocksDB and SplinterDB at 10M, 100M and 1B,
interleaved, the fill and gets beside the seeks (no lever may cost them). SuRF and the range filter
stay whole: LIST needs them.

## 6. What paces the phase

Seeks buy each pivot's consolidation once their rent for it covers its price (`Trunk::charge`; the
deterministic ski-rental rule). At 100M a phase bought 70–77 leaves and 66–104 index pivots one at a
time, in 18–35 consolidations (`consolidations_started`), and no paid flush paid the pivots below it
(`paid_below` 0: only a drain's cascades) [vm, runs `r271..273`]. A consolidation starts only with
nothing in progress and ends only when nothing is, and a pivot paid while one runs is looked at only
when the next starts: the consolidations ran one after another, each as long as its slowest settle.
In one of main's rounds the phase ended with 35 of 73 leaves paid, and ran 82K seeks a second [vm,
`r282`].

## 7. A leaf's settle taking its in-flight bundles (kept)

A leaf compacted the in-flight bundles its parent flushed into its bundle, then settled, rewriting
them: a settle due once that compaction's branch joined the bundle now takes them among its inputs
(`c8067c3`). Over seven interleaved rounds at 100M against main [vm, `r271..273`, `r281..284`] the
rates were level (the medians of the paired ratios 1.019 fill, 0.991 gets, 0.998 seeks, a round's
ratio 0.68–1.63), the seek phase's process CPU an operation 12% less (median 1.136 inverted, better in
five rounds of seven), and the fill wrote 3–7% fewer entries (302M–309M against 311M–325M at the
queries' start). In a consolidation bought by seeks a leaf is rarely paid when its parent's data
arrives, so the settles took in-flight entries during the fill, not the queries (`settle_inflight_entries`).

## 8. A drawn fraction of the price (measured, not kept)

Karlin, Manasse, McGeoch and Owicki's randomized ski rental (Algorithmica 11(6), 1994) buys once the
rent reaches `x` of the price, `x` drawn with density eˣ/(e − 1) on [0, 1): an expected cost at most
e/(e − 1) ≈ 1.58 times the best in hindsight against the deterministic rule's 2, buying at 0.58 of
the price on average. Each pivot drew its fraction as its rent began (`cd04482`, on `c8067c3`).
Over four rounds at 100M [vm, `r281..284`] it bought earlier and in fewer consolidations (7–10 against
15–30) but settled some leaves twice (76–83 settles over 73 leaves), wrote 103M–115M entries in the
queries against 94M–104M, and used 22% more process CPU a seek than `c8067c3` (the paired ratio 0.777,
worse in four rounds of four), its seeks 0.881 of `c8067c3`'s: a leaf bought before its parent's
data reached it settles again when it does.

## 9. Consolidations pipelined (measured, not kept)

A pivot paid while a consolidation ran was queued for that consolidation's next look rather than the
next consolidation's start. Over four rounds at 100M against `c8067c3` [vm, `r291..294`] the phase
started 3–23 consolidations against 18–24 and spent 9% less process CPU a seek (the paired ratio
1.087, better in four rounds of four), but its seeks were level (0.984; the phase 6.6–7.6 s against
6.4–8.1 s): the consolidations' order was not what paced it. What does is the rent itself: a leaf is
bought once seeks have paid its price, and one bought sooner is bought before its parent's data
reaches it (§8). The fill, which neither change touches, ran 0.82–1.03 of the same build in the same
rounds: three or four rounds resolve no smaller change in this phase.

## 10. Where the phase's remaining gap lies

Four times RocksDB in the phase that consolidates needs the consolidation, about 2 s of the
workers' time at 100M, to be done by the time most seeks run: started at the fill's end or the
phase's first seeks, not bought pivot by pivot as each one's rent covers its price. The rent is the
hedge against consolidating a store no seek reads (research/43, research/50 §3): spending it earlier
is a design choice about workloads that write and then seek rarely, measured against the seeks it
buys, and the phase on the consolidated trunk already runs 5.4–5.8× RocksDB (§1).

## 11. A whole consolidation once a shard falls quiet (kept: C)

§10 left open when the trunk should consolidate. Three builds of one tree, differing only in what
buys a whole consolidation (every pivot of several sources rewritten, as a drain's is), ran
interleaved with RocksDB and SplinterDB in every round [ab]:

- **A** (`95edbd8`, main's rule): nothing buys one; seeks' rent buys each pivot (§6).
- **B** (`eb64216`): a shard that falls quiet consolidates whole: its memtable empty since a flush,
  nothing packing or frozen, its trunk idle and owing nothing.
- **C** (`c3e56da`): B's rule, once the writes since the last whole consolidation reach the entries
  the trunk holds, counting the entries that entered the root and the entries maintenance wrote.
  This is global rebuilding's rule (Overmars, The Design of Dynamic Data Structures, LNCS 156,
  Springer 1983): a structure is rebuilt once the updates since its last rebuild reach its size,
  so each rebuild is paid for by at least as many writes as it rewrites. A bulk load pays for one
  at once; a burst into a large trunk does not.

Every run ends with the bench's `--trickle=2:1000000`: two bursts of a million draws, each key the
fill left present put again (1.26M puts at 100M), each burst flushed and its maintenance settled.
That is the workload that costs B most, since each burst leaves the shard quiet with its layout
unconsolidated.

[ab]: `benchmark-results/mantle-splinterdb-20261013`, the [vm] setup with `--queries-again`: runs
`r311..314` on a quiet host (A and B; four rounds at 10M, three at 100M) and `r321..323` on a busy
one, 18 busy loops in the container, one a core, from before each run to its end (A, B and C; three
rounds at 10M, one at 100M). Medians over rounds; the trickle's CPU is the process's, workers
included.

| keys, host | arm | puts/s | gets/s | seeks/s | seek p99 | trickle CPU a put | trickle wrote |
|---|---|---|---|---|---|---|---|
| 10M, quiet | A | 3.67M | 2.65M | 440K | 9.0 µs | 0.70 µs | 1.2M |
| | B | 3.92M | 2.43M | 516K | 3.3 µs | 2.69 µs | 13.8M |
| | RocksDB, SplinterDB | 690K, 954K | 167K, 706K | 91K, 185K | 31.3, 11.0 µs | | |
| 100M, quiet | A | 3.49M | 1.38M | 143K | 31.7 µs | 0.97 µs | 1.9M |
| | B | 3.66M | 1.52M | 347K | 5.5 µs | 25.4 µs | 130M |
| | RocksDB, SplinterDB | 562K, 917K | 178K, 299K | 57K, 38K | 50.2, 59.4 µs | | |
| 10M, busy | A | 1.31M | 0.86M | 188K | 11.8 µs | 1.50 µs | 1.2M |
| | B | 1.32M | 1.18M | 236K | 5.1 µs | 4.66 µs | 13.8M |
| | C | 1.34M | 1.00M | 327K | 4.4 µs | 1.16 µs | 1.2M |
| | RocksDB, SplinterDB | 301K, 543K | 93K, 257K | 48K, 86K | 48.7, 25.1 µs | | |
| 100M, busy | A | 1.46M | 0.65M | 68K | 44.0 µs | 4.57 µs | 13.7M |
| | B | 1.45M | 0.74M | 213K | 9.0 µs | 43.9 µs | 130M |
| | C | 1.32M | 0.57M | 140K | 9.7 µs | 1.48 µs | 2.1M |
| | RocksDB, SplinterDB | 250K, 514K | 54K, 191K | 35K, 20K | 75.6, 200.7 µs | | |

**Where B's and C's consolidation runs.** The bench's flush after the fill runs maintenance to its
end, so their whole consolidation ran there: 4.1–6.5 s at 100M on the quiet host against A's
0.5–1.1 s, outside every engine's measured phases. RocksDB's runs at 100M spent 4.9–5.8 s outside
theirs (`waitforcompaction`, with the open and close), and SplinterDB 4.9 s in the barrier after its
fill. A's seek phase carries its consolidation (§3); B's and C's queries ran on the trunk it left and
wrote nothing.

**What B costs.** Each burst of the trickle bought a rewrite of the whole trunk: at 100M B spent
25–31 µs of CPU a put on the quiet host and 44 µs on the busy one, against A's 0.95–1.16 µs, and
wrote 130M entries against 1.9M.

**What C does.** Its fill and queries ran as B's, the fill's writes having paid for the
consolidation after it, and its trickle as A's, no burst buying a whole consolidation: at 10M on the
busy host 1.16 µs of CPU a put against A's 1.50 µs and B's 4.66 µs, and at 100M 1.48 µs against B's
44 µs, writing 2.1M entries where A wrote 1.9M on the quiet host. Against A on the busy host at 10M
(the medians of the paired rounds) its seeks ran 1.74× (0.82–2.26), its gets 1.15× and its fill
1.15×. At 100M on the busy host A's seeks bought only part of their consolidation (43.6M entries
written in the queries, 7 leaves settled) and its trickle settled the rest (13.7M written, 4.57 µs a
put); C's seeks ran 2.08× A's in that round, its trickle 0.32× A's CPU a put. Under load A's phase
is as slow as the merging the busy host leaves it, where C's consolidation ran in the window after
the fill (21.5 s there).

Kept: C. Its quiet-host rates are measured as the control of the batches that follow, with 1M and
1B.

## 12. Measured and not kept

- **The leaves' separators as 8-byte heads** in a sorted array a branch keeps beside its trie,
  searched in place of the trie's descent: the consolidated phase ran 343K to 384K against 366K to
  390K [mac, three rounds]; the trie's descent is not what bounds it, and building the array for
  every branch a worker makes cost the fill 0.77×.
