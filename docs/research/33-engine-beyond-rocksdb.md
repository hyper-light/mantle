# 33. An engine an order of magnitude past RocksDB: what the evidence supports

The question: how mantle's metadata engine beats RocksDB by about 10× on agentic, Meta-scale
traffic (billions of agents, machine-speed bursts, heavy skew), with strict durability, on
NVMe, on a busy machine, and still runs efficiently on one laptop. This note collects the
primary sources, the measured multiples and the conditions behind them, and draws only the
conclusions those support. The PDFs and their text are kept beside the session that read them;
every number below was checked against a paper's text unless it is marked otherwise.

## 1. The workload, as Meta measured it

Cao, Dong, Vemuri and Du, *Characterizing, Modeling, and Benchmarking RocksDB Key-Value
Workloads at Facebook*, FAST '20 (https://www.usenix.org/conference/fast20/presentation/cao-zhichao),
traced three production RocksDB deployments. One of them, a ZippyDB shard, holds the metadata of
Meta's object store: the workload mantle's engine serves.

- **Query mix** (§4.1): 78% Get, 13% Put, 6% Delete, 3% Iterator seek, over 420 million queries in 24 hours.
- **Sizes** (§5, Table 2, Figure 8(c, d)):
  - keys average 47.9 bytes, in two steps of 48–53 and 90–91 bytes;
  - values average 42.9 bytes, more than 90% are under 34 bytes, and about 1% are over 400.
- **Skew** (§4.2, Figure 5):
  - about 80% of the pairs read are read once in a day;
  - about 1% are read more than 100 times, and those take about half of all Gets;
  - 73% of pairs are Put only once.
- **Scans** (Figure 4(b), for UDB): more than 60% of iterators read a single pair. ZippyDB's seeks start at a few metadata keys repeatedly.

Read misses dominate the cost: most keys are cold, so a Get pays a device read and, in RocksDB,
a whole data block decompressed to return about 90 bytes.

## 2. Mechanisms, ranked by evidence for this workload

Durability matters to every comparison. Most "N× RocksDB" results run with weaker durability
than mantle requires (a write acknowledged only once flushed, §6 of CLAUDE.md):
- **F2** (VLDB '25): every system has its write-ahead log, compression and checksums disabled.
- **Haas and Leis** (VLDB '23): logging is off.
- **p²KVS** (EuroSys '22): RocksDB's asynchronous logging.
- **Tidehunter**: acknowledges before fsync.

SpanDB's baseline is RocksDB's group commit with fdatasync (its §2.4); SplinterDB runs with its
per-thread log on. Multiples measured without durable commit do not carry over to mantle.

| # | Mechanism | Source | Measured against RocksDB, and the conditions |
|---|---|---|---|
| 1 | Size-tiered Bε-tree (STBε), flush-then-compact by the writing thread, quotient filters, concurrent memtable and user-level cache | Conway et al., *SplinterDB*, ATC '20, https://www.usenix.org/system/files/atc20-conway.pdf | "6–10× on insertions and 2–2.6× on point queries, while matching RocksDB on small range queries", write amplification 2× lower. 24-byte keys, 100-byte values, Optane 905p, memory 10% of data, per-thread log on. RocksDB "able to use only 30% of the bandwidth … even when using 20 or more cores": CPU-bound on NVMe. |
| 2 | Maplets: filters compacted eagerly, data lazily | Conway et al., SIGMOD '23, https://dl.acm.org/doi/10.1145/3588726 | Up to 9× insert, up to 1.83× query. **Abstract only; conditions not verified.** |
| 3 | Parallel group commit: several WAL batches in flight, 4 KiB-aligned log pages, asynchronous request pipeline | Chen et al., *SpanDB*, FAST '21, https://www.usenix.org/system/files/fast21-chen-hao.pdf | Up to 8.8× throughput on all-write YCSB, latency 9.5–58.3% lower; RocksDB spent 68–81% of write time in group logging. Fast log device (Optane), SPDK, polling cores. |
| 4 | Shared-nothing partitions, a pinned worker each, request batching | Lu et al., *p²KVS*, EuroSys '22 | Up to 4.6× write, 5.4× read, 128-byte pairs; RocksDB's global log and memtable insert named as bottlenecks. Asynchronous log. |
| 5 | Compaction and flush scheduled against foreground work (preemption, bandwidth borrowing) | Balmau et al., *SILK*, ATC '19, https://www.usenix.org/system/files/atc19-balmau.pdf | "Up to two orders of magnitude lower 99th percentile latencies", throughput unchanged. |
| 6 | Log as level 0, skew-aware memtable, deferred L0 merges | Balmau et al., *TRIAD*, ATC '17 | Up to 2.93× throughput, write amplification up to 4× lower. |
| 7 | One sorted view over a level's runs for seeks | Zhong et al., *REMIX*, FAST '21 | Seeks 5.1× (8 tables) to 9.3× (16 tables) over a merging iterator, 16-byte keys, 100-byte values. End-to-end multiple not extracted. |
| 8 | Range filter (Fast Succinct Trie) | Zhang et al., *SuRF*, SIGMOD '18 | In RocksDB: open seeks up to 1.5×, closed seeks up to 5×. |
| 9 | Hash index over a hybrid log, hot records updated in place, read cache | Kanellis et al., *F2*, PVLDB 18 (2025), https://arxiv.org/abs/2305.01516 | 11.8× average throughput, 8-byte keys, 100-byte values, memory 10% of data, Zipf 0.99, **logging off, no range scans**. |
| 10 | Learned index per file | Dai et al., *Bourbon*, OSDI '20 | Lookups 1.23–1.78× (against a LevelDB-family baseline). |
| 11 | Compaction granularity aligned to the last level | Dayan et al., *Spooky*, PVLDB 15 (2022) | Total write amplification, SSD garbage collection included, about 2.5× lower. |
| 12 | Device data placement (ZNS, FDP) | Bjørling et al., ATC '21; Allison et al., EuroSys '25 | ZNS: p99.9 reads 2–4× lower, writes 2×; FDP: device write amplification about 1. |
| 13 | Key-value separation | WiscKey FAST '16; Tidehunter (arXiv 2602.01873) | Wins only for values of about 1 KB and up. Tidehunter at 64-byte values is about 2× **slower** than RocksDB. Not for mantle's 43-byte values. |
| 14 | Caching policy | Yang et al., *S3-FIFO*, SOSP '23; McAllister et al., *Kangaroo*, SOSP '21 | S3-FIFO: lowest miss ratio on 10 of 14 trace sets, 6× the throughput of an optimized LRU at 16 threads. Kangaroo: 29% fewer misses for about 100-byte objects on flash. |

Rejected for this workload:
- KVell (SOSP '19): an in-memory index of every key, write amplification at least 30× for 100-byte records, 11.9× slower than F2 at a 10% memory budget.
- PebblesDB: superseded by SplinterDB, which found no large write-amplification gain.

What Meta reports limits RocksDB (Dong et al., FAST '21, https://www.usenix.org/system/files/fast21-dong.pdf): the
target "migrated from write amplification, to space amplification, to CPU utilization", and for
most deployments "space utilization was far more important than write amplification". A design
that gives back space efficiency to win writes is not a win at Meta.

## 3. What the evidence supports, per operation

| Operation | Supported multiple | Basis |
|---|---|---|
| Put, durable group commit | 5–9× | SpanDB 7.6–8.8× with fdatasync; SplinterDB 6–10× inserts with its log on. Neither measured the combination, and both used Optane-class devices; on flash the bound is flush latency times groups in flight. |
| Put latency | 1.5–3× | SpanDB, SplinterDB. |
| Point get, cold, data past memory | 1.4–2.6× | SplinterDB, Bourbon. One device read per cold Get is the floor no design removes. |
| Point get, hot set | up to about 10× | F2, logging off. |
| Short scans and listings | 1× for the tree alone; 1.4–9× on seeks with a REMIX view; up to 5× on closed seeks with SuRF | No source measures the combination. |
| Write amplification | 2–4× lower | SplinterDB, TRIAD, Spooky. |
| p99 under compaction | 10–100× | SILK, SplinterDB's stall-free flush-then-compact. |
| Space | at risk with tiering | Maplets' claim (abstract only) is the mitigation. |

No primary source supports 10× on every operation. The order of magnitude is supported for durable
ingest (5–9×), tail latency (10–100×) and hot-set reads. Cold reads and scans are bounded by the
device and by data layout, at about 1.5–3×.

## 4. The block codec on this workload

zstd's own source and Meta's papers (the codec research of 2026-10-06) give the codec's part:
- RocksDB compresses one zstd frame a block. Entropy tables are rebuilt every frame unless a
  dictionary is configured (zstd 1.5.7 `zstd_ddict.c:58-86` points a frame at a dictionary's
  prebuilt tables).
- A dictionary trained per table file, as RocksDB supports in format version 7, makes most blocks
  repeat its tables (`set_repeat`). It removes table builds from most blocks, and on small records
  raises the ratio: 1 KB JSON records went from 2.8× to 6.9× in Meta's 2016 zstd announcement.
- RocksDB's block cache holds uncompressed blocks, so a block is decompressed only on a miss.
- Smaller data blocks, which a dictionary keeps compressible, cut the bytes decompressed per miss
  in proportion.

## 5. What this asks of mantle's engine

The engine today converts RocksDB 11.8.1 and keeps its file format (docs/design/engine.md). That
alone cannot reach these multiples: RocksDB's own structure is what SplinterDB and SpanDB
measured against. The evidence-backed path keeps every correctness property mantle requires
(durable commit, checksums on every record, typed corruption) and changes the structure:

1. Shared-nothing shards by key range, a worker each, requests batched per shard (p²KVS).
2. Per-shard logical log with several group commits in flight, 4 KiB-aligned pages, completions
   (SpanDB), on hyper-block's issuer; the log doubles as level 0 (TRIAD).
3. SplinterDB's STBε-tree with maplets, compaction by the shard itself, granularity aligned to
   the last level (Spooky).
4. A REMIX view and SuRF filters for listings.
5. An F2-style in-place region and read cache for the hot set, S3-FIFO for caches.
6. SILK-style scheduling of flush and compaction against foreground I/O.
7. FDP or ZNS lifetime hints where the device has them.

Whether mantle keeps RocksDB's file format is the owner's decision. The structure above writes
its own files. RocksDB compatibility would then be an import and export path, not the engine's
own format.
