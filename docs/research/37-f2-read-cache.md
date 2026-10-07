# 37. F2's read cache, for a shard's hot records

Source: Konstantinos Kanellis, Badrish Chandramouli, Ted Hart, Shivaram Venkataraman, *From
FASTER to F2: Evolving Concurrent Key-Value Store Designs for Large Skewed Workloads*, PVLDB 18
(2025), arXiv 2305.01516v3, §3–§8 (pp. 3–10). Marked **[paper]**. Read for E8
(docs/design/engine-structure.md §5): serving read-hot records from memory.

## 1. F2 [paper §4]

- **Two logs.** A hot log (FASTER's HybridLog: mutable, read-only and stable regions, write-hot
  records updated in place in the mutable region) and a cold log on disk, each with its own
  index. Live records move hot → cold by hot-cold compaction [paper §4.1–§4.2, Fig. 4–5].
- **Read cache** [paper §7]. An in-memory HybridLog of *replicas* of disk-resident records that
  are read; the originals stay in their logs. Records already in memory are never cached.
  - Records are inserted at the tail (mutable region) and evicted at the head, a page at a time.
  - **Second chance**: a cached record read again while in the read-only region is copied to the
    tail, so the most read-hot records are never evicted (Second-Chance FIFO).
  - **Invariant**: for a key, the read cache keeps its most recent record. An upsert, RMW or
    delete invalidates the cached record (a header bit) before it proceeds.
  - **Reads**: follow the key's hash chain; a valid cached record with the key is returned at
    once; a disk-resident record found is inserted into the read cache.
- **Measured** [paper §8]. The read cache alone improves throughput by up to 1.27× on
  read-heavy workloads (§8.2). The whole of F2 is 2–11.9× existing stores at memory 2.5–10% of
  the data, Zipf 0.99, 8-byte keys and 100-byte values, with write-ahead logging, compression
  and checksums disabled on every system (§8.1).

## 2. What mantle takes

- **A record cache in front of the trunk.** A get that misses both memtables looks up the
  record cache before the trunk; a hit skips the filters, the leaf index and the page search.
  A record read from the trunk is inserted. Records are replicas: the trunk keeps every
  original, so the cache needs no durability of its own (the Raft log and checkpoints are the
  shard's, docs/design/engine-structure.md §2).
- **The paper's invariant, exactly.** A put or delete drops its key's cached record before it
  returns. The memtable is read first, so a newer write wins while it is there; once flushed,
  the newer version lives in the trunk, and the dropped record cannot hide it.
- **The paper's structure.** A log of records in pages, appended at the tail and evicted at the
  head a page at a time, a record read since it was appended carried to the tail when its page
  is evicted (second chance), indexed by a table of key hashes. The evicted page is the next
  tail page: eviction allocates nothing.
- **Pages make resizing cheap.** The memory tuner resizes the cache while gets run. A single
  ring had to be rebuilt to resize, a copy of every record: 19–73 ms stalls on a 170 MiB cache
  (10M keys, 2026-10-07). With pages, growing raises the page limit and shrinking evicts head
  pages, the work eviction does anyway.
- **Write-hot records** stay in the memtable, a B-tree updated in place (§8 E3): mantle's
  analogue of the hot log's mutable region.
- **Sizing.** The cache's bytes are a third region of the shard's memory budget, divided by the
  same measured-gain rule as the page cache and write memory (research/36).
