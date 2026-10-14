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

## 6. Measured and not kept

- **The leaves' separators as 8-byte heads** in a sorted array a branch keeps beside its trie,
  searched in place of the trie's descent: the consolidated phase ran 343K to 384K against 366K to
  390K [mac, three rounds]; the trie's descent is not what bounds it, and building the array for
  every branch a worker makes cost the fill 0.77×.
