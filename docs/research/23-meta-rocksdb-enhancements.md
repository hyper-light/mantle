# 23 — Meta's RocksDB enhancements, and the benchmark matrix for mantle's port

Research note for porting RocksDB 11.8.1 to Rust as mantle's metadata-range engine, and for
benchmarking that port against RocksDB itself. Note 12 chose the engine, described RocksDB's
design and the LSM models, and derived how a range stores its state (one instance per range
replica, the shared per-disk Raft log as the only WAL, snapshots and moves as files). This note
does not repeat that. It collects what Meta has published about the enhancements it made to
RocksDB and to the systems built on it, with each one's mechanism, how it was measured, by how
much, where it lives in the 11.8.1 source, whether it is on by default, and what it means for
the port and its benchmarks.

The note covers:

- the systems Meta built on RocksDB and what each changed or measured in the engine: ZippyDB
  (beyond note 12 §3), Tectonic's metadata, ZippyDB on Tectonic, Delos, MyRocks, MySQL Raft,
  Rocksandra, LogDevice, Laser, and where Shard Manager touches shard moves (§1);
- the engine enhancements, feature by feature (§2);
- the published workload models and the generator that implements them, with the defects found
  in it (§3);
- what the port must implement to be comparable, and what it can leave out (§4);
- the benchmark matrix (§5);
- what remains unknown (§6).

Compiled 2026-09-30. This is research input, not a decision record.

---

## How to read this document

**Citation tags.** As in note 12:

- Papers: `[KEY §section, p. N]`, where `p.` is the printed proceedings page. DISAGG and
  DONG21-TOS pages are article pages (`p. 192:N`, `p. 26:N`). Author copies without printed
  page numbers are cited as "PDF p. N": DONG17, DELOS-LSP, MYNVM, RT16.
- Web pages, wiki pages, blog posts and PR descriptions: `[KEY, "heading"]`, or
  `[RDB-BLOG "file"]` for a post in the RocksDB repository's `docs/_posts/`.
- Source code: `[RDB-SRC path:line]`, line numbers of RocksDB 11.8.1 (commit `abeebd963`).
  History: `[RDB-HIST L<line>, x.y.z]`, the line of `HISTORY.md` and the release section it
  falls under.
- Earlier notes: "note 01 §x" (Tectonic, ZippyDB), "note 06 §x" (consensus), "note 07 §x"
  (focal), "note 09 §x" (cells, Shard Manager), "note 12 §x" (RocksDB and ZippyDB), "note 21
  §x" (benchmark states).

**Quotes** follow note 12's rules: verbatim from the source's text layer or saved page text,
ligatures normalized, hyphenated words rejoined, "..." for an elision, "[sic]" for an error in
the original.

**Evidence labels.** The same as note 12:

- *(no label)*: stated in the cited **peer-reviewed** source and checked against its text.
- **NON-PEER-REVIEWED**: RocksDB wiki, blog posts, pull-request descriptions, `HISTORY.md`,
  headers and source; the Meta and Instagram engineering blogs. These are primary sources for
  what a system *does or says*, not peer-reviewed descriptions. Most of RocksDB's own
  performance numbers are in this class, and most of them state no hardware.
- **PREPRINT**: RIBBON (arXiv only; its successors are peer-reviewed, see Sources).
- **DERIVED**: arithmetic or an interpretation made by this note.
- **UNVERIFIED**: not found in any primary source consulted.
- **INFERENCE / Recommendation**: reasoning for mantle, citing the facts it rests on.

**Method.**

1. PDFs were fetched from usenix.org, vldb.org, cidrdb.org, arxiv.org, pdl.cmu.edu and the
   authors' sites. ACM Digital Library PDFs refused a non-browser client (HTTP 403); the
   Internet Archive's captures of the same URLs were used for DONG21-TOS, DISAGG, SM and
   MYNVM (the last is the Facebook-hosted author copy). Each PDF was converted with
   `pdftotext`, in both layout and reading-order modes for two-column pages, and the relevant
   sections were read in full. Page offsets were checked against page footers.
2. The RocksDB source, `HISTORY.md`, `tools/` and `docs/_posts/` were read from a clone at the
   11.8.1 release (`include/rocksdb/version.h` gives 11 / 8 / 1). Note 12 read the same version
   from the copy bundled in `librocksdb-sys`; line numbers here are from the clone.
3. RocksDB wiki pages were fetched as raw markdown on 2026-09-30. They are live documents and
   several are stale against the code; each discrepancy found is recorded where it matters.
   PR descriptions were fetched with the GitHub API.
4. Blog posts were fetched as HTML and reduced to text; the Instagram post only through the
   Internet Archive (the live site timed out).
5. Spot checks: every figure used in §0 and §5 was re-read against the saved text or source
   line by this note's author, not only by the reading pass that found it.
6. No secondary summaries were used as evidence. Mark Callaghan's personal benchmark blog,
   which the RocksDB wiki links for HyperClockCache results, was not used.

---

## Sources

### Peer-reviewed papers

Keys shared with note 12 (DONG21, DONG21-TOS, DONG17, CAO20, MYROCKS, RIBBON, SILK, TEC, SM) use the
same citations and page mappings as note 12's Sources table; only new keys are listed in full.

| Key | Full citation | Peer-reviewed | Where obtained |
|---|---|---|---|
| **DISAGG** | Siying Dong, Shiva Shankar P, Satadru Pan, Anand Ananthabhotla, Dhanabal Ekambaram, Abhinav Sharma, Shobhit Dayal, Nishant Vinaybhai Parikh, Yanqin Jin, Albert Kim, Sushil Patil, Jay Zhuang, Sam Dunster, Akanksha Mahajan, Anirudh Chelluri, Chaitanya Datye, Lucas Vasconcelos Santana, Nitin Garg, Omkar Gawde. "Disaggregating RocksDB: A Production Experience." *Proc. ACM Manag. Data* 1(2) (SIGMOD 2023), Article 192, 24 pp. DOI 10.1145/3589772. CC-BY. | Yes | Internet Archive capture of https://dl.acm.org/doi/pdf/10.1145/3589772 |
| **DELOS-VC** | Mahesh Balakrishnan, Jason Flinn, Chen Shen, Mihir Dharamshi, Ahmed Jafri, Xiao Shi, Santosh Ghosh, Hazem Hassan, Aaryaman Sagar, Rhed Shi, Jingming Liu, Filip Gruszczynski, Xianan Zhang, Huy Hoang, Ahmed Yossef, Francois Richard, Yee Jiun Song. "Virtual Consensus in Delos." *OSDI '20*, pp. 617–632. | Yes | https://www.usenix.org/system/files/osdi20-balakrishnan.pdf |
| **DELOS-LSP** | Mahesh Balakrishnan et al. "Log-structured Protocols in Delos." *SOSP '21*. DOI 10.1145/3477132.3483544. | Yes | Author copy, https://maheshba.bitbucket.io/papers/delos-sosp2021.pdf (no printed pages) |
| **CACHELIB** | Benjamin Berg et al. "The CacheLib Caching Engine: Design and Experiences at Scale." *OSDI '20*. | Yes | https://www.usenix.org/system/files/osdi20-berg.pdf |
| **KANGAROO** | Sara McAllister et al. "Kangaroo: Caching Billions of Tiny Objects on Flash." *SOSP '21*. | Yes | https://www.pdl.cmu.edu/PDL-FTP/NVM/McAllister-SOSP21.pdf |
| **MYNVM** | Assaf Eisenman, Darryl Gardner, Islam AbdelRahman, Jens Axboe, Siying Dong, Kim Hazelwood, Chris Petersen, Asaf Cidon, Sachin Katti. "Reducing DRAM Footprint with NVM in Facebook." *EuroSys '18*. DOI 10.1145/3190508.3190524. | Yes | Author copy (Internet Archive capture of research.fb.com); pages 1–13 of that copy |
| **RT16** | Guoqiang Jerry Chen et al. "Realtime Data Processing at Facebook." *SIGMOD '16*. DOI 10.1145/2882903.2904441. | Yes | Author copy (Internet Archive capture of research.fb.com) |

**Page mapping.** Printed page = PDF page + 615 (DELOS-VC), + 767 (CACHELIB), + 242
(KANGAROO). DISAGG page 192:N = PDF page N. DELOS-LSP's proceedings pages are probably 538–552
(SM, the next paper, starts at 553); **UNVERIFIED**, so it is cited by PDF page.

**RIBBON's status.** The arXiv preprint has no journal reference. Two peer-reviewed successors
exist and were not read: Dillinger, Hübschle-Schneider, Sanders, Walzer, "Fast Succinct
Retrieval and Approximate Membership Using Ribbon," SEA 2022, DOI 10.4230/LIPIcs.SEA.2022.4;
and Dietzfelbinger, Dillinger, Hübschle-Schneider, Sanders, Walzer, "Ribbon: Fast Succinct
Static Retrieval and Approximate Membership," *J. ACM* 73(1), 2026, DOI 10.1145/3785417
(Crossref). This note keeps citing the preprint and labels it PREPRINT.

### NON-PEER-REVIEWED primary sources

| Key | What it is | Where |
|---|---|---|
| **ZDB-BLOG** | Sarang Masti, "How we built a general purpose key value store for Facebook with ZippyDB", Engineering at Meta, 2021-08-06. Same key as notes 01 and 12. | https://engineering.fb.com/2021/08/06/core-infra/zippydb/ |
| **MYRAFT** | Anirban Rahut, Abhinav Sharma, Yichen Shen, Ahsanul Haque, "Building and deploying MySQL Raft at Meta", 2023-05-16. | https://engineering.fb.com/2023/05/16/data-infrastructure/mysql-raft-meta/ |
| **ROCKSANDRA** | Dikang Gu, "Open-sourcing a 10x reduction in Apache Cassandra tail latency", Instagram Engineering, 2018-03-05. | Internet Archive capture (2019-09-05) of instagram-engineering.com |
| **MYROCKS16** | Yoshinori Matsunobu, "MyRocks: A space- and write-optimized MySQL database", 2016-08-31. | https://engineering.fb.com/2016/08/31/core-infra/myrocks-a-space-and-write-optimized-mysql-database/ |
| **MYROCKS17** | Yoshinori Matsunobu, "Migrating a database from InnoDB to MyRocks", 2017-09-25. | https://engineering.fb.com/2017/09/25/core-infra/migrating-a-database-from-innodb-to-myrocks/ |
| **MESSENGER** | Thomas Georgiou, Xiang Li, "Migrating Messenger storage to optimize performance", 2018-06-26. | https://engineering.fb.com/2018/06/26/core-infra/migrating-messenger-storage-to-optimize-performance/ |
| **ZSTD-BLOG** | Felix Handte, Nick Terrell, Yann Collet, "5 ways Facebook improved compression at scale with Zstandard", 2018-12-19. | https://engineering.fb.com/2018/12/19/core-infra/zstandard/ |
| **LOGDEVICE** | Mark Marchukov, "LogDevice: a distributed data store for logs", 2017-08-31. | https://engineering.fb.com/2017/08/31/core-infra/logdevice-a-distributed-data-store-for-logs/ |
| **RDB-SRC** | RocksDB 11.8.1 source, clone at commit `abeebd963` ("Additional HISTORY.md update for 11.8.1"): `include/rocksdb/*.h`, `db/`, `cache/`, `table/`, `util/`, `tools/db_bench_tool.cc`, `tools/benchmark.sh`, `tools/regression_test.sh`, `USERS.md`. | https://github.com/facebook/rocksdb |
| **RDB-HIST** | `HISTORY.md` and `DEFAULT_OPTIONS_HISTORY.md` of the same tree. | same |
| **RDB-BLOG** | RocksDB blog posts in `docs/_posts/` of the same tree, cited by file name (e.g. `2021-12-29-ribbon-filter`). | same |
| **RDB-WIKI** | RocksDB wiki, raw markdown fetched 2026-09-30: "RocksDB-Trace,-Replay,-Analyzer,-and-Workload-Generation", "Performance-Benchmarks", "Setup-Options-and-Basic-Tuning", "RocksDB-Bloom-Filter", "Block-Cache", "BlobDB", "User-defined-Timestamp", "Wide-Columns", "Asynchronous-IO", "Partitioned-Index-Filters", "Remote-Compaction", "Tiered-Storage-(Experimental)", "Read-only-and-Secondary-instances", "Creating-and-Ingesting-SST-files", "Write-Buffer-Manager", "SST-File-Manager", "Thread-Pool", "FIFO-compaction-style", "Atomic-flush", "Subcompaction". | https://github.com/facebook/rocksdb/wiki |
| **RDB-PR** | RocksDB pull-request descriptions: #8271 (secondary cache in LRUCache), #10626 (clock cache revamp), #11738 (AutoHCC), #12141 (eviction effort cap), #13910 (parallel compression), #13964 (HCC as default), #14247 and #14383 (interpolation search). | https://github.com/facebook/rocksdb/pulls |

---

## 0. Decision-relevant summary

1. **Meta has published few option values.** Beyond note 12's set, the only production numbers
   in primary sources are: MyRocks' 16 KB blocks, 10 bloom bits per key, no last-level filter, a
   20-byte prefix bloom, size multiplier 10, a 12 GB block cache, no compression on L0–L2 then
   LZ4 then Zstandard at the last level (later LZ4 on every level but the last), and 16 KB
   dictionaries [DONG17 §2–§4; MYROCKS §3.2; ZSTD-BLOG]; TRIM-safe file deletion in about 64 MB
   chunks at about 128 MB/s [MYROCKS17]; ZippyDB's 20 GB primary plus 100 GB secondary cache
   [DISAGG §4.1.3, p. 192:9]; and the disaggregated setting's 4–8 MB compaction reads, write
   buffers of 64 MB or more and SST files of 64–256 MB [DISAGG §4.1.4, §4.1.6]. Across 39
   ZippyDB deployments the configurations differed in compaction (14 variants), SST format (7),
   plug-ins (6), I/O (4) and compression (2) [DONG21-TOS Table 5, p. 26:17], so there is no
   single "Meta setting"; §5 names one per system and source.
2. **A "stock" benchmark is not db_bench's defaults.** db_bench's flag defaults differ from the
   11.8.1 library defaults in compression (Snappy against LZ4), dynamic level sizing (off
   against on), top-level index pinning (off against on) and the LRU high-priority pool ratio
   (0.0 against 0.5) [RDB-SRC `tools/db_bench_tool.cc:1569, 970, 747, 636`;
   `include/rocksdb/table.h:289`; `cache.h:245`; RDB-HIST L61, 11.5.0]. The stock
   configuration C0 (§5.2) sets them explicitly.
3. **The published ZippyDB workload command does not model ZippyDB in 11.8.1.** The wiki's
   `mixgraph` command sets the key-range hotness parameters but not `key_dist_a`/`key_dist_b`;
   with either at 0, db_bench draws keys uniformly and ignores the key-range model, and without
   `-sine_mix_rate=true` the QPS sine is off [RDB-SRC `db_bench_tool.cc:8559–8576, 8594`]. Query
   type, value size and scan length are all derived from one uniform draw, so they are
   correlated, which CAO20 set out to avoid [§3.3]. The Cao benchmark therefore has to be
   rebuilt for the port, and RocksDB run through the same generator (§3.4).
4. **Delos is the peer-reviewed precedent for mantle's log–engine relation, and it measured the
   cost of sharing a device.** Delos keeps state in a RocksDB LocalStore, commits the applied
   log position in the same transaction as the state, flushes the LocalStore "periodically in a
   background thread", replays from the log after a reboot, and trims the log only below the
   position every replica has "applied and flushed durably" [DELOS-LSP §3.2, §4.1, PDF pp. 5,
   8]. With the log on the same SSDs as the database, latency "starts rising at 15K ops/s for
   puts due to contention between the Loglet and the database", against 150K ops/s with the
   log on other machines, and a burst to 2500 puts/s pushed p99 "to over a second" [DELOS-VC
   §5.1, pp. 627–628]. Mantle's per-disk Raft log shares a device with the engines, so the
   matrix must measure log and engine together on one device and on two (§5.2 C5).
5. **ZippyDB's published latencies are not a durability-equal baseline.** ZippyDB performed
   "rlog writes to OS page cache before acknowledgment", and on Tectonic writes "into a shared
   memory buffer before acknowledgement" [DISAGG §6.1, p. 192:15]. Mantle acknowledges only
   after a full flush on every required replica (CLAUDE.md rule 6), so a comparison against
   ZippyDB's numbers must say that, and the matrix's durable baseline is RocksDB with its WAL
   synced (C5-a).
6. **The block cache default changed, and neither cache wins everywhere.** 11.8.1 creates a
   32 MB AutoHyperClockCache when none is given [RDB-SRC
   `table/block_based/block_based_table_factory.cc:474–477`; RDB-HIST L259, 10.7.0]. HCC beat
   LRU by 5.2× at 48 threads with the database cached [RDB-PR #10626] and by 3.4× at 100
   threads, but LRU was faster at a 50% hit rate (725K against 541K ops/s at 10 threads)
   [RDB-PR #11738]. The port's cache must be benchmarked in both regimes (§5.2 C7).
7. **Some enhancements are always on in 11.8.1 and must be in the port for a fair
   comparison**: dynamic level sizing, `kMinOverlappingRatio`, compaction outputs cut at
   next-level file boundaries (no opt-out since 9.0.0), a 30-day `ttl` for block-based tables,
   `format_version` 7 with XXH3 block checksums, `optimize_filters_for_memory`, LZ4 as the
   default compression, and AutoHCC (§4.1).
8. **Most enhancements Meta measured are not needed by a metadata range, and the port can defer
   them**: integrated BlobDB (values of tens of bytes, note 12 §2.7), remote compaction and
   follower instances (built for disaggregated storage; mantle's engines are on local disks),
   time-aware tiered storage (one device class per engine), wide columns and attribute groups
   (no published use or measurement), and async I/O (its measured gain was on remote flash)
   (§4.3).
9. **The measured engine-level gains that bear on metadata**: SingleDelete with
   deletion-triggered compaction kept LinkBench QPS from collapsing under deletes [MYROCKS
   Fig. 5]; range-tombstone conversion sped scans over deleted runs 99× forward and 368×
   reverse [RDB-BLOG `2026-06-22-range-tombstone-conversion`]; cutting outputs at boundaries
   saved 12.6% of compaction writes [RDB-BLOG `2022-10-31-align-compaction-output-file`,
   DERIVED]; user-defined timestamps gave 1.2–2.0× over timestamps in keys [DONG21-TOS Table 6,
   p. 26:22]; the data-block hash index gave 10% on cached point lookups at 4.6% more space
   [RDB-BLOG `2018-08-23-data-block-hash-index`]; Ribbon filters save about 30% of filter
   memory for 3–4× filter CPU [RDB-BLOG `2021-12-29-ribbon-filter`]. All but the MyRocks and
   timestamp figures are NON-PEER-REVIEWED and name no hardware; the matrix re-measures them.
10. **Moves are file operations in every Meta system that publishes how it copies.** MyRocks
    clones by checkpoint and ingests bulk loads into Lmax (note 12 §1.9); Rocksandra streams
    into temporary SST files and ingests them [ROCKSANDRA, "Challenges"]; ZippyDB on Tectonic
    rebuilt an in-region replica by a metadata-only file copy, "from about 50 minutes down to
    within one minute" [DISAGG §6.2, pp. 192:15–16]. Shard Manager bounds concurrent moves per
    server, per application and per shard but specifies no data-copy protocol [SM §5.1,
    p. 561]. The matrix measures the file path (checkpoint, verify, transfer, open or ingest)
    as its own configuration (C9).

---

## 1. Systems Meta built on RocksDB

### 1.1 ZippyDB, beyond note 12 §3

Note 12 §3 has the replication (Data Shuttle, Multi-Paxos global scope plus asynchronous
followers), epochs and leases, consistency levels, OCC transactions, conditional writes, TTL
through periodic compaction, and 50–100 GB physical shards of tens of thousands of µshards.
Additions:

- **Scale and age** (NON-PEER-REVIEWED). In production "since we first deployed ZippyDB in
  2013"; described as "the largest strongly consistent, geographically distributed key-value
  store at Facebook" [ZDB-BLOG, introduction]. No total shard, server, QPS or byte count is
  published.
- **Tiers.** "specialized tiers for distributed filesystem metadata" sit beside a multitenant
  "wildcard" tier, which is "preferred ... because of its better utilization of hardware and
  lower operational overhead" [ZDB-BLOG "Architecture"].
- **API follows RocksDB's.** Get, put and delete with batch variants, "iterating over key
  prefixes and deleting a range of keys. These APIs are very similar to the API exposed by the
  underlying RocksDB storage engine" [ZDB-BLOG "Data model"]. The engine operations a ZippyDB
  shard exercises are therefore point reads, batched writes, prefix scans and `DeleteRange`,
  which is the set the matrix's workloads cover (§5.3).
- **Replication log placement.** ZippyDB "moved all their file operations to use RocksDB's
  storage interface", including its replication log (rlog) [DISAGG §6.1, p. 192:15]. The rlog
  acknowledgement rule is in §0 item 5. Whether ZippyDB also writes RocksDB's WAL is still not
  published: DISAGG places both a WAL and the rlog on Tectonic but never says ZippyDB disables
  the RocksDB WAL (**UNVERIFIED**; note 12 §3.8 left the same question open).
- **Three replicas, mixed storage.** In the DISAGG deployment each shard had "one replica
  stored on Tectonic and the other two on local SSD"; the local replicas skipped direct I/O
  and used the page cache; the Tectonic replica had a local flash cache "sized about 4x the
  block cache size" [DISAGG §6.6, pp. 192:17–18].
- **Secondary cache in production.** "Using the secondary cache in our production environment
  with ZippyDB results in a 50-60% improvement in read IOPS ... and a 30-40% improvement in
  ZippyDB level read latency ... The cache configuration used was 20GB of primary cache and
  100GB of secondary cache" [DISAGG §4.1.3, p. 192:9]. The secondary tier was CacheLib on
  local flash; RocksDB 11.8.1 contains no such implementation, only the DRAM
  `CompressedSecondaryCache` and a tiered wrapper that takes a user-supplied NVM tier [RDB-SRC
  `cache/compressed_secondary_cache.h`, `cache/tiered_secondary_cache.h`] (§2.3).
- **Per-terabyte load on four ZippyDB use cases** (read QPS / write QPS / read MB/s / write
  MB/s per TB): 14 / 548 / 0.4 / 0.37; 33K / 28 / 294 / 28.42; 54 / 12 / 0.77 / 5.55; 3K /
  1.7K / 0.17 / 0.27 [DISAGG Table 4, p. 192:18]. These are the only published ZippyDB load
  intensities besides CAO20's single traced shard, and they span three orders of magnitude.
- **Failover and space.** Table 3 compares Tectonic-backed against local ZippyDB: space
  utilization 75% against 35%, per-database failover 49 seconds against 51 minutes [DISAGG
  Table 3, p. 192:17]. The failover gap is the file-copy rebuild above, not an engine change.

### 1.2 Tectonic's metadata on ZippyDB

Note 01 §1.5–§1.6 and note 12 §3.7 have Tectonic's layers and guarantees. The facts that shape
an engine benchmark:

- **Hash partitioning and a Block-layer majority.** "ADLS range-partitions metadata layers
  whereas Tectonic hash-partitions layers"; "Around two-thirds of metadata operations in
  Tectonic are served by the Block layer" [TEC §3.3, p. 220]. Name-layer expansion is "a prefix
  scan over keys" [TEC Table 1, p. 220].
- **Hot shards hit the limit.** "each shard can serve a maximum of 10 KQPS ... around 1% of Name
  layer shards hit the QPS limit because they hold very hot directories" [TEC §6.3, p. 225].
- **For mantle (INFERENCE).** A Tectonic-shaped engine workload is point reads dominating
  (Block layer), with short prefix scans (Name layer) and hot key ranges. That matches CAO20's
  traced ZippyDB shard (78% Get, 3% seek, key-range locality; note 12 §1.13), which is why the
  ZippyDB model is the matrix's primary workload (§5.3 W2). Mantle's ranges are contiguous key spans (note 12, introduction),
  so its Name ranges keep a directory's entries together, which strengthens the locality CAO20
  measured.

### 1.3 RocksDB on Tectonic (DISAGG)

The engine changes Meta made to run RocksDB over a remote file system [DISAGG §3–§4, §7]:

- **A file-system plugin** behind RocksDB's `FileSystem` interface; one RocksDB instance is the
  single writer of its directory, "so the instance can cache data and metadata without
  consistency issues" [§3.1, p. 192:6]. Metadata caching "effectively bypassed almost all
  metadata lookup operations" [§4.1.2, p. 192:9].
- **Fencing.** "a process trying to own a RocksDB directory must first 'IO Fence' the directory
  using a token ... successful if the token is lexicographically greater than the previous
  fencing token" [§4.3, p. 192:12].
- **I/O sizes.** "applications usually see satisfactory performance when configuring the
  compaction read size to 4MB or 8MB and the compaction write buffer size to 64MB or larger";
  the adaptive readahead "warms up too slowly for Tectonic", so the initial size came from
  history and the maximum became configurable [§4.1.4, p. 192:10]. "unless the target SST
  file size is set to be smaller than 64MB, it has minimal impact on performance" [§4.1.6,
  p. 192:11].
- **Parallel I/O in MultiGet** within one SST file; CPU "increase when measured using
  micro-benchmarks was significant (50%), the absolute increase is quite small" [§4.1.5,
  pp. 192:10–11].
- **Error classification.** File-system statuses carry "retry-ability, scope ... and whether
  there is permanent data loss"; background flush and compaction writes are retried, WAL write
  errors stop writes to flush memtables [§4.4, p. 192:13]. The reason given is ZippyDB's
  replica removal on a write failure. Mantle fences the tablet instead (note 12 §1.12, §6.7);
  retrying a failed flush is excluded there for the fsync reason note 06 §A10.1 records.
- **db_bench measurement** (RocksDB 7.4; 20-byte keys, 400-byte values, 1 billion keys, about
  200 GB; 16 GB block cache; 8 KB blocks; ZSTD with LZ4 for newer data; "3 1.600GHz physical
  cores and 64 GB DRAM"; the local run with direct I/O) [§5, pp. 192:13–15]:
  - writes: sequential 262.4 MB/s on Tectonic against 264.1 local; random 19.2 against 26;
  - Get at 64 threads: 54.8K QPS, p50 1.1 ms, p99 2.9 ms on Tectonic against 334K, 0.19 ms,
    0.42 ms local;
  - "Tectonic I/O latency is about 5x the local SSD latency ... The parallel I/O feature can cut
    the difference to 3x."
- **Remote compaction** on one ZippyDB use case: "save more than 50% of cross data center IO
  usage and saw the average compaction time is reduced by 20.4%" [§7.2, p. 192:20].
- **For mantle.** Mantle's engines run on local disks, so the plugin, fencing and remote I/O
  sizes do not transfer. The local-SSD column of DISAGG's db_bench run is a published reference
  point with a full configuration, which is why it appears as C4 (§5.2).

### 1.4 Delos: the log is the source of truth, RocksDB holds the state

Delos is Meta's replicated control-plane store (a table store, DelosTable, and a ZooKeeper
API, Zelos) built over a shared log.

- **State in RocksDB, applied from the log.** "Each Delos server maintains a local copy of state
  in RocksDB and keeps this state synchronized via state machine replication (SMR) over the
  VirtualLog ... it executes the operation within a single thread as a failure-atomic
  transaction on its local RocksDB" [DELOS-VC §4, p. 623]. "We use RocksDB as the LocalStore"
  [DELOS-LSP §4, PDF p. 8].
- **The applied position is written with the state.** The BaseEngine "maintains a cursor in
  the LocalStore. As the BaseEngine plays each entry, it creates a LocalStore transaction
  context; updates its cursor in the store within the transaction; and then calls apply"
  [DELOS-LSP §3.2, PDF p. 5]. This is note 12 §6.2's step 1.
- **The engine is flushed periodically, and the log covers the gap.** "A committed transaction
  is visible but not immediately durable on the LocalStore, which we flush periodically in a
  background thread (we can replay the entry from the log if the server reboots)" [DELOS-LSP
  §3.2, PDF p. 5]. The paper does not say whether RocksDB's WAL is disabled (**UNVERIFIED**).
- **Trimming waits for durable application everywhere.** The ViewTrackingEngine "queries the
  LocalStore for the last log position that has been applied and flushed durably"; "when all
  servers have played the log past some point X, the log can be trimmed until X" [DELOS-LSP
  §4.1, PDF p. 8]. Backups also gate trimming [§4.2, PDF pp. 8–9]. This is note 12 §6.2's
  step 3 (truncate below the persisted applied index), with the added rule that the minimum is
  taken across replicas, since in Delos the log is shared and a lagging replica must still be
  able to replay.
- **Group commit.** "the entire batch is applied in a single LocalStore transaction", worth "a
  2X speed-up" at 20 ms p99 for 100-byte writes on five nodes [DELOS-LSP §4.4, §5.2, PDF
  pp. 10–12].
- **Where the time goes.** "a substantial amount of time is spent by the BaseEngine
  constructing the LocalStore transaction (in the beginTX call) and subsequently committing
  it" [DELOS-LSP §5.1, PDF p. 11]; high-write clusters are "bottlenecked by SSD bandwidth for
  the consensus protocol (which requires synchronous writes) rather than the apply thread", and
  "90% of the clusters are below 10% apply utilization" [ibid.].
- **Log and engine on one device.** Quoted in §0 item 4 [DELOS-VC §5.1, pp. 627–628]. On
  dedicated NVMe benchmark hardware the converged and disaggregated logs reached 139K and 190K
  ops/s [ibid.].
- **RocksDB against in-memory state.** "ZooKeeper can provide over 30K puts/sec before p99
  latency degrades beyond 15ms. In contrast, Delos+NativeLoglet manages around 26K puts/sec.
  The primary reason ... ZooKeeper stores its materialized state in memory while Delos uses
  RocksDB"; a 100 GB database gave a curve "nearly identical to the 1GB case" [DELOS-VC §5.2,
  p. 629].
- **Production shape.** "425 queries/sec and 150 puts/sec ... Write size has a median of 500
  bytes and a max of 150KB. Each deployment stores between 1GB and 10GB ... local snapshots
  every 10 minutes" [DELOS-VC §5, p. 626].
- **For mantle (INFERENCE).** Delos confirms the protocol note 12 §6.2 derived and adds two
  things. First, the periodic engine flush is a tunable with a cost on each side: flushing
  rarely lengthens replay and holds the log; flushing often adds engine writes to the device
  the log is fsyncing. Second, the measured contention says the benchmark of mantle's setting
  must put the shared Raft log and the engines on the same device, at write rates past the
  knee, and report tail latency, not only throughput.

### 1.5 MyRocks, beyond note 12

Note 12 §1.4–§1.13 already cites MyRocks' last-level filter removal, prefix bloom, SingleDelete,
deletion-triggered compaction, periodic compaction, bulk loading into Lmax, checkpoint cloning
and hybrid compression. Additions:

- **What Meta says it added to RocksDB for MyRocks.** "Among the new features we introduced in
  RocksDB were transactional support, bulk loading, and prefix bloom filters" [MYROCKS §1,
  p. 3217]; and "rate limited compaction file generations and deletions to prevent stalls"
  [§1, p. 3218].
- **Reverse key comparator.** Delta-encoded blocks and single-direction skiplists make reverse
  scans costly; storing some keys in inverse byte order "improved descending scan throughput by
  approximately 15% in UDB" [§3.2.1.2, p. 3222]. No conditions beyond "in UDB". Mantle's S3
  listing is forward-only (ListObjectsV2), so the port needs no reverse comparator (INFERENCE).
- **Mem-comparable keys.** MyRocks encodes every key bytewise-comparable because an LSM needs
  "one binary search for each sorted run ... This can lead to several times more key
  comparisons" [§3.2.1.1, p. 3221]. No number. The port compares keys bytewise, so mantle's
  key encoding has to be order-preserving under byte comparison, as MyRocks' is (INFERENCE).
- **Deletes under load.** LinkBench QPS over time with no optimization, DTC, SingleDelete, and
  both [Fig. 5, p. 3223]: "With no optimization, QPS significantly degraded ... DTC made overall
  QPS drop much less significant." Read from the chart (approximate): about 37–40K QPS at the
  dips without optimization, about 45–50K with both. No hardware or dataset is stated.
- **Bulk load.** Throughput "higher than InnoDB by 2.2 times (with one table, one concurrency)
  to 5.7 times (with 20 tables, 20 concurrency)" [§3.2.3.4, p. 3224].
- **Direct I/O.** "the Linux kernel allocated approximately 2~3GB of slab memory per 1TB of
  RocksDB SST files ... After using Direct I/O, our average slab size dropped by over 80%"; and
  "we had to make sure we did not mix buffered and direct I/O to the same file" [§6.1.1,
  p. 3228]. The 2017 migration post says the opposite of the paper's end state: "MyRocks/RocksDB
  had limited support for direct I/O so we switched to use buffered I/O" [MYROCKS17, "Other
  technical tips"]. Both are Meta's at different times.
- **Production comparison, one UDB replica set at peak** [MYROCKS Table 1, pp. 3226–3227]:

| Engine | Space | CPU s/s, writes only | CPU s/s, reads + writes | Bytes written/s |
|---|---|---|---|---|
| InnoDB (compressed) | 2187.4 GB | 0.89 | 1.83 | 13.34 MB |
| MyRocks | 824.4 GB | 0.55 | 1.65 | 3.42 MB |

  DERIVED: 37.7% of the space, 38% less CPU for writes, 10% less for reads and writes, 74% fewer
  bytes written. The paper warns that feedback-directed optimization "reduced the CPU usage of
  MyRocks instances by approximately 7~10%" and that a Zstandard-compressed, similarly built
  InnoDB would be "comparable" in CPU [§5.1, p. 3227].
- **Space mechanisms, measured** [DONG17 §3.2, §4, PDF pp. 4–5]: key prefix encoding saves
  "3% – 17%"; zeroing sequence numbers saves "0.03%" to "23%", the high end for "social graph
  edges that will have empty values"; lightweight compression reduces data to "as low as 40%"
  and strong compression to "as low as 25%"; a dictionary "an additional 3%"; Zstandard or zlib
  at the last level "an additional 15%–30%" over lightweight compression alone. Mantle's
  metadata rows are small, key-heavy and often have short values (CAO20; note 12 §1.13), the
  case where prefix encoding and sequence-number zeroing count most (INFERENCE).
- **Cache and I/O at production scale** [DONG17 appendix, PDF pp. 8–9]: block cache hit rate
  "79.3%" for data blocks and "99.97%" for index and filter blocks; page-cache hit rate "98%,
  98%, 93%, 77%, and 46% for levels L0-L4"; "Less than 10% of read queries result in a disk
  access". The body says "92%" of block-cache misses are served by L4 and Figure 11's caption
  says "(82%)"; the paper is inconsistent.
- **LinkBench** (24-core Xeon E5-2678v3, 256 GB RAM, three NVMe SSDs in RAID 0, 16 clients,
  24-hour runs, 1 billion vertices with all but 50 GB of RAM locked, a 10 GB block cache)
  [DONG17 §5, PDF pp. 5–6]: RocksDB was "3%-16% better than InnoDB" in transactions, read
  "between 10% and 22% higher" bytes per read transaction with compression, and wrote "less than
  20%" of InnoDB's bytes per transaction; p99 latencies were "an order of magnitude better"
  [Fig. 4, PDF p. 6].
- **Dictionaries in production.** "In MyRocks deployments (UDB ... and Facebook Messenger), we
  decided to use zstd in the bottommost level ... and to use LZ4 for other levels"; dictionaries
  "more than offset the compression reduction introduced by smaller blocks" [ZSTD-BLOG, "Hybrid
  compression", "Dictionary Compression"]. MYNVM chose 6 KB blocks for an NVM cache tier and
  measured database growth of "almost 20%" at 4 KB blocks, "more than 11%" at 6 KB, and 1.5%
  at 6 KB with 16 KB dictionaries [MYNVM §4, PDF pp. 6–8].

### 1.6 MySQL Raft

- **The binlog is the Raft log; the engine keeps its own log.** "From the Raft perspective, the
  binary log became the replicated log"; mysqld "runs a two-phase commit protocol between the
  engine and the replicated binlog as the participants" [MYRAFT, "Replicated log", "Crash
  recovery"]. A transaction is prepared in the engine (InnoDB or MyRocks), written to the
  binlog, waits for consensus, then commits in the engine. On recovery, prepared but
  uncommitted transactions are rolled back and, if the entry survived the election, reapplied
  "from scratch".
- **Engine changes.** None described beyond prepare, commit and rollback under 2PC, and switching
  "the engine side log from apply-log to binlog" on promotion [MYRAFT, "Raft-initiated state
  machine transitions"].
- **Numbers.** Write latency "equivalent to semisync"; failover "within 2 seconds" against 20 to
  40 seconds under semisync; heartbeats every 500 ms, election after three misses [MYRAFT,
  "Performance"]. No distribution, hardware or workload.
- **For mantle.** MySQL Raft keeps an engine log and a replicated log and couples them with 2PC.
  Mantle's design (note 12 §6.2) removes the engine log instead, as Delos and TiKV's tablets do,
  so MySQL Raft is not a model for the port. Its "Next steps" name the direction: "disentangle
  the log from the state machine ... into a disaggregated log setup".

### 1.7 Rocksandra, LogDevice, Laser, Messenger

- **Rocksandra** replaced Cassandra's storage engine with RocksDB at Instagram. Streaming for
  moves was rewritten: "we now stream data into temp sst files first, and then use the RocksDB
  ingest file API to bulk load them" [ROCKSANDRA, "Challenges"]. In production "the P99 read
  latency dropped from 60ms to 20ms", and GC stalls fell from 2.5% to 0.3%; on three
  i3.8xlarge instances with 250 million 6 KB rows, read throughput was "10X higher ... (300K/s
  for Rocksandra vs. 30K/s for C* 3.0)" at a similar 2 ms p99 [ROCKSANDRA, "Performance
  metrics"]. The gain is from leaving the JVM heap, not from an engine change; no RocksDB
  options are published.
- **LogDevice's LogsDB** is "a time-ordered collection of RocksDB column families ... sharing a
  common write-ahead log"; a partition is left alone at "typically about 10" SST files, a new
  one is started, and space is reclaimed by "deleting (or in some cases infrequently
  compacting) the oldest partition" [LOGDEVICE, "The local log store"]. It is Meta's use of
  column-family drop as bulk deletion (note 12 §1.7). Mantle's Raft log is focal-log, not
  RocksDB (note 07), so this is context only.
- **Laser** is "a high query throughput, low (millisecond) latency, key-value storage service
  built on top of RocksDB" [RT16 §2.5, PDF p. 3], built on Shard Manager, serving "nearly one
  billion queries per second at peak; 9% of those queries are prefix scans" [SM §3.1, p. 558].
  No configuration or engine change is published.
- **Messenger** moved from HBase to MyRocks on flash: replication factor from six to three,
  storage "reduced ... by 90 percent", read latency "50 times lower" [MESSENGER, "Benefits of
  the new system"]. A storage-system migration, not an engine enhancement.

### 1.8 Shard Manager, where it touches moves

Note 09 §7.3 covers Shard Manager. For the engine:

- **Graceful primary migration** is five calls (`prepare_add_shard`, `prepare_drop_shard`,
  `add_shard`, discovery update, `drop_shard`), with the old primary forwarding writes until
  the new one takes over; "no client request is dropped" [SM §4.3, p. 560].
- **Moves are bounded.** "Cap the number of concurrent shard moves per server and per
  application ... cap the number of a shard's replicas that can be moved concurrently" [SM §5.1,
  p. 561]. An upgrade of 10,000 shards on 60 servers kept success "≈100%" with graceful
  migration against "below 90%" without it [SM §8.2, p. 564].
- **No storage contract.** SM leaves state transfer to the application (not in SM).
- **For mantle (INFERENCE).** The bound on concurrent moves per node is a CLAUDE.md rule 2
  bound; the engine benchmark supplies its input: the time and I/O one move costs (C9).

---

## 2. Engine enhancements, feature by feature

Each entry: mechanism; published measurement with conditions; where it lives in 11.8.1; default;
what it means for the port and its benchmarks. Features note 12 already covers are cited there
and only the new facts are given.

### 2.1 Filters: Ribbon, and filter placement

- **Mechanism.** A static filter solving a banded linear system over GF(2) ("Rapid Incremental
  Boolean Banding ON the fly") [RIBBON abstract, §3, p. 4]. RocksDB uses the Standard
  (non-homogeneous) variant with 128-bit coefficient rows and no "smash" [RDB-SRC
  `table/block_based/filter_policy.cc`, `Standard128RibbonRehasherTypesAndSettings`: "kHomogeneous
  = false ... kUseSmash = false"]; it falls back to Bloom after 256 failed re-seeds.
  `NewRibbonFilterPolicy(bits, bloom_before_level)` chooses per file: Bloom for flushes (and
  levels below `bloom_before_level`), Ribbon deeper [RDB-SRC `include/rocksdb/filter_policy.h:
  184–188, 209–210`].
- **Measured** (PREPRINT) [RIBBON §7 Table 2, p. 11], Xeon D-2191, ns per key, 1% FP rate, n =
  10⁶ and 10⁸ keys: RocksDB's Bloom constructs in 21 and 72 ns and queries in 10 and 36 ns at
  49.8% space overhead; Standard Ribbon w = 128 constructs in 166 and 235 ns, queries in 58 and
  140 ns, at 6% and 8% overhead. DERIVED: 3.3–7.9× construction and 3.9–5.8× query time for
  about 35% less space. RocksDB's own cost model (NON-PEER-REVIEWED; `filter_bench`, no hardware
  named) prices the trade: filters that live longer than about 3235 s (an hour) should be Ribbon
  [RDB-BLOG `2021-12-29-ribbon-filter`]. Construction needs "~230 bits per key for 128-bit Ribbon
  vs. ~75 bits per key for Bloom" of temporary memory [ibid.].
- **Production context** (PREPRINT). "roughly 10% of memory and roughly 1% of CPU used in blocked
  Bloom filters. The size-weighted average age of a live filter is about three days" [RIBBON §1
  fn. 5, p. 1]. Three days is well past the one-hour break-even (DERIVED).
- **Default.** No filter at all: `filter_policy = nullptr` [RDB-SRC `table.h:590`];
  `optimize_filters_for_memory = true` [`table.h:574`]; `optimize_filters_for_hits = false`
  [`advanced_options.h:816`]. Ribbon since 6.15.0 (experimental), production 6.22.0, hybrid 6.24.0
  [RDB-HIST L2011, L1806, L1757].
- **db_bench.** `--bloom_bits` (default −1, no filter) and `--use_ribbon_filter` [RDB-SRC
  `db_bench_tool.cc:850, 854`]; db_bench always passes `bloom_before_level = 0`, so other
  placements need an options file.
- **For the port.** Ribbon construction is the one filter path where the port's CPU cost can
  differ most from C++ (128-bit arithmetic, banding); the matrix measures construction time per
  key and flush/compaction throughput with Ribbon on, not only query time. Filter memory per
  level remains note 12 §2.4's question; C6 measures the placements RocksDB can express.

### 2.2 Block cache: HyperClockCache and its default

- **Mechanism.** Lock-free counting-CLOCK eviction with each slot's metadata in one atomic word:
  "Lookup was previously about 4 atomic updates, now just 1 atomic update" [RDB-PR #10626].
  FixedHCC uses open addressing and needs `estimated_entry_charge`; AutoHCC (charge 0) grows by
  linear hashing inside a reserved anonymous mapping and uses chaining [RDB-PR #11738; RDB-SRC
  `cache/clock_cache.cc:3616–3620`]. HCC's minimum shard is 32 MB, so the default cache has one
  shard, where LRU at 32 MB has 64 [RDB-SRC `clock_cache.cc:3610`; `sharded_cache.h:318–320`].
- **Measured** (NON-PEER-REVIEWED), db_bench readrandom on a 30-million-key database in
  `/dev/shm`, 48-thread Skylake [RDB-PR #10626]: with the database cached, LRU 310.5K and HCC
  1604.9K ops/s at 48 threads (DERIVED 5.2×), 47.8K and 53.9K at one thread; "With partitioned
  instead of full filters, the maximum speed-up vs. base is more like 2.5x rather than 5x". On
  a 48-core machine [RDB-PR #11738]: 100 threads, LRU 438,613, FixedHCC 1,651,310, AutoHCC
  1,505,875 ops/s; at about 50% hit rate and 10 threads, LRU 725,231, FixedHCC 638,620, AutoHCC
  541,018. With a nearly fully pinned cache, AutoHCC fell "more than 10x slower" than LRU until
  `eviction_effort_cap` (default 30) was added [RDB-PR #12141]. Creating a million 32 MB caches
  took about 25 KB each for LRU and 5 KB for AutoHCC [RDB-PR #13964].
- **Default.** AutoHCC, 32 MB, when `block_cache` is null [RDB-SRC
  `block_based_table_factory.cc:474–477`; RDB-HIST L259, 10.7.0]; "HYPERCLOCKCACHE IS NOW
  GENERALLY RECOMMENDED OVER LRUCACHE" [RDB-SRC `include/rocksdb/cache.h:378`]. The wiki's
  "Block-Cache" page still says LRU; it is stale.
- **Production.** "AutoHCC running fully in production for a while on a very large service"
  [RDB-PR #13964]; the service is not named.
- **For the port.** The cache is shared by every tablet on a node (note 12 §6.1), so it sees the
  node's full thread count; lock-free lookup is the property that matters, and the churn regime
  where LRU wins must be measured too (C7). Meta's own numbers were taken with the database in
  RAM, which hides I/O; mantle's runs use real disks (CLAUDE.md rule 8).

### 2.3 Secondary and tiered caches, and memory charging

- **Mechanism.** A `SecondaryCache` receives blocks evicted from the primary cache and returns
  them on a miss [RDB-BLOG `2021-05-27-rocksdb-secondary-cache`]. In-tree implementations: a
  DRAM `CompressedSecondaryCache` (LZ4, filters not compressed) and `NewTieredCache`, which
  splits one `total_capacity` between a primary, a compressed secondary and an optional
  user-supplied NVM tier [RDB-SRC `include/rocksdb/cache.h:302–315, 526–558`, marked
  EXPERIMENTAL].
- **Measured.** Production, ZippyDB, CacheLib on flash: 50–60% more read IOPS served, 30–40%
  lower read latency, 20 GB primary and 100 GB secondary [DISAGG §4.1.3, p. 192:9]. Earlier
  prototype (NON-PEER-REVIEWED): "a 15% gain with the local flash cache over no local cache,
  and a ~25-30% reduction in network reads" on mixgraph [RDB-BLOG
  `2021-05-27-rocksdb-secondary-cache`]. MYNVM's NVM tier for MyRocks (16 GB DRAM plus 140 GB
  NVM against 96 GB DRAM): mean and p99 latency 10% and 20% higher, QPS 8% lower, and 45% lower
  latency than 16 GB DRAM alone [MYNVM §6, PDF pp. 2, 10–11]; blocks were admitted to NVM only
  on a second hit in a simulated LRU, to respect NVM endurance [§4.3, PDF p. 8]. No measurement
  is published for `CompressedSecondaryCache` or `NewTieredCache`.
- **CacheLib and RocksDB.** In 2020, RocksDB "wanted to use CacheLib to implement its internal
  page buffer", but CacheLib's C++ reference counting "prevented programmers from integrating
  CacheLib with RocksDB's C-style code base" [CACHELIB §6, p. 781]; the secondary-cache
  interface was the way around it [RDB-BLOG `2021-05-27-rocksdb-secondary-cache`]. RocksDB as a
  flash cache under FIFO compaction reached a 53% hit ratio against CacheLib's 76% and used 50%
  more CPU [CACHELIB §5.1, p. 778]. KANGAROO mentions RocksDB only as a flash cache Netflix had to
  over-provision by 67% [§2.3, p. 246].
- **Memory charging.** Memtables (through the write buffer manager), table readers, file
  metadata, dictionary buffers and filter construction can be charged to the block cache, so its
  capacity becomes one memory cap [RDB-BLOG `2025-09-24-unified-memory-tracking`]. Defaults: only
  dictionary-building buffers are charged; filter construction, table readers and file metadata
  are not [ibid.].
- **Default.** Off.
- **For the port (INFERENCE).** Mantle's metadata lives on local disks, where a flash secondary
  cache only helps if the flash is faster than the engine's device; the port does not need
  one. Memory charging does matter: note 12 §6.1 wants one node-wide memory bound, and charging
  every role to the shared cache is how RocksDB gives it. The port should charge every role by
  construction, and the C5 comparison runs RocksDB with every role charged.

### 2.4 Integrated BlobDB and its garbage collection

- **Mechanism.** Values at least `min_blob_size` go to blob files written by flush and
  compaction; SSTs hold a reference. Garbage collection happens in compaction: live blobs in the
  oldest `blob_garbage_collection_age_cutoff` fraction of blob files are rewritten, and
  `blob_garbage_collection_force_threshold` schedules compactions when those files' garbage
  ratio passes it [RDB-SRC `include/rocksdb/advanced_options.h:1082–1200`]. Blob direct write
  (11.2.0) separates values on the write path instead [ibid. `:1228–1257`].
- **Measured** (NON-PEER-REVIEWED) [RDB-BLOG `2021-05-26-integrated-blob-db`], 18-core Skylake DE
  at 1.6 GHz, 64 GB RAM, two 1.88 TB M.2 SSDs in RAID 0, RocksDB ≈ 6.18.1, 1 TB of values from
  1 KB to 1 MB: write amplification 1.0–1.02 on initial load against 1.6; under overwrite 1.4–1.7
  against leveled 6.1–6.8 and universal 5.7–6.2; overwrite throughput 2.1–3.5× leveled; reads
  equal or better "except for workloads involving range scans at the two smallest value sizes
  tested (1 KB and 4 KB)". No measurement is published for the GC parameters or the blob cache.
- **Default.** Off (`enable_blob_files = false`, `advanced_options.h:1082`).
- **For the port.** Excluded. Metadata values average 43–127 B [CAO20 Table 2, p. 215], below
  BlobDB's smallest tested size, and range scans at small values are the case it loses (note
  12 §2.7). The matrix has no BlobDB configuration.

### 2.5 User-defined timestamps

- **Mechanism.** Note 12 §1.8. New: `persist_user_defined_timestamps = false` keeps timestamps
  in the memtable and WAL and strips them at flush [RDB-SRC `advanced_options.h:1320–1323`, whose
  comment still begins "UNDER CONSTRUCTION -- DO NOT USE"]. It first shipped in 8.3 (PR #11362);
  no system is named as its motivation.
- **Measured.** The full table behind note 12's "1.2X or better": fill_seq then read_random 1.2×,
  fill_seq then read_while_writing 1.9×, fill_random then read_random 1.9×, fill_random then
  read_while_writing 2.0×, against timestamps encoded in keys, at the cost that "the database
  would consume more disk space" [DONG21-TOS §7.1 Table 6, pp. 26:22–23]. No measurement exists
  for the memtable-only mode.
- **Default.** Off (bytewise comparator); `db_bench --user_timestamp_size` (default 0).
- **For the port.** Not needed now (note 12 §1.8). If mantle adds reads at a log index, the
  memtable-only mode fits a design where the Raft log, not the engine, keeps history.

### 2.6 Wide columns

- **Mechanism.** `PutEntity` stores named columns under one key as one value type; attribute
  groups map column groups to column families [RDB-SRC `include/rocksdb/wide_columns.h`,
  `attribute_groups.h`; since 7.9.0, RDB-HIST L1105].
- **Measured.** Nothing published. No Meta system is named.
- **For the port.** Excluded: a range stores one value per key (note 12 §6.1), and nothing
  in Meta's sources measures what wide columns would buy.

### 2.7 Ingestion, export and import, for moves

Note 12 §1.9 has ingestion's write blocking, `allow_db_generated_files` and export/import. New:

- **Options that matter for a move.** `link_files` (9.7.0), `move_files`, `fill_cache` (9.8.0),
  `verify_checksums_before_ingest` (default false), `verify_file_checksum` (default true),
  `atomic_replace_range` (experimental, first in 10.1.3; its comment says
  `snapshot_consistency = true` "is not yet supported" and "BUG: the upper bound of the range
  may be interpreted as inclusive or exclusive"), and `ClipColumnFamily` (8.3.0) [RDB-SRC
  `include/rocksdb/options.h:2834–3044`; `db.h:2359`]. 11.5.0 split ingestion into a prepare
  phase outside the database mutex and a commit phase (`PrepareFileIngestion`) [RDB-HIST L49].
- **Import bugs fixed late.** Export and import did not carry range tombstones until 8.1.0
  [RDB-HIST L1009]; import could miss the file from the flush it triggered until 10.4.0
  [L333]. These are the paths mantle would exercise, so the port's tests should replay them.
- **Measured.** Nothing for ingestion in RocksDB's own sources. System-level: MyRocks bulk load
  2.2–5.7× InnoDB [MYROCKS §3.2.3.4]; ZippyDB rebuild 50 minutes to under one [DISAGG §6.2]; the
  2017 post lists "Migrating shards between machines by dumping key-range in SST File and
  loading the file in a different machine" as a use case [RDB-BLOG
  `2017-02-17-bulkoad-ingest-sst-file`].
- **db_bench.** Benchmark `ingestexternalfile` with batch-size, batch-count and opening-thread
  flags [RDB-SRC `db_bench_tool.cc:1377–1402, 4389`].
- **For the port.** With one instance per range (note 12 §6.1), a move is checkpoint, transfer,
  verify, open; ingestion is used for merges and cross-cell copies (note 12 §6.4–§6.5). The port
  needs ingestion into an empty instance and `link_files`; `atomic_replace_range` is excluded
  while its documented bug stands.

### 2.8 Remote compaction, secondary and follower instances

- **Mechanism.** With `DBOptions::compaction_service` set, the primary picks a compaction and
  hands a serialized input to `Schedule`; a worker runs `DB::OpenAndCompact` as a secondary
  instance, writes outputs to its own directory, and the primary installs them by rename, so the
  worker's output must be on the same file system [RDB-SRC `options.h:460–534, 1755, 3167–3206`;
  RDB-WIKI "Remote-Compaction", "4. Install & Purge"]. Resumable mode checkpoints progress after
  each output file [RDB-BLOG `2026-05-19-resumable-remote-compaction`]. Secondary instances tail
  the MANIFEST and replay WAL files; follower instances keep hard links to the leader's files
  [RDB-SRC `db.h:257–345`].
- **Measured.** Only DISAGG's single use case (§1.3). RocksDB's own posts publish no numbers.
- **Default.** Off (`compaction_service = nullptr`, marked EXPERIMENTAL).
- **For the port.** Deferred. Mantle's compaction runs on the node that owns the disk. One
  consequence of mantle's design is worth recording: with the engine WAL off, a secondary or
  follower sees only flushed files, since memtables are rebuilt "by replaying the log entries
  in the WAL files" [RDB-WIKI "Read-only-and-Secondary-instances"] (DERIVED). Read replicas in
  mantle are Raft followers, not engine secondaries.

### 2.9 Time-aware tiered storage and per-key placement

- **Mechanism.** Flushes record sampled (sequence number, time) pairs in each SST, at most 100
  per file [RDB-SRC `db/seqno_to_time_mapping.h:28, 38`]. With
  `preclude_last_level_data_seconds` set, a compaction into the last level writes keys newer than
  the cut to a second output at the level above (the "proximal" level), so hot data never reaches
  the cold last level [RDB-SRC `db/compaction/compaction_job.cc:410–420`;
  `subcompaction_state.cc:109–115`]. The last level can be given a file temperature that the file
  system maps to a device.
- **Measured.** "In production, **we found the majority of the compaction load is actually major
  compaction (more than 80%)**" [RDB-BLOG `2022-11-09-time-aware-tiered-storage`]; the overhead is
  "far less than 1KB per SST". No throughput or cost figure. MYROCKS independently reports that
  "approximately 80% of compaction bytes are completed in non-Lmax levels" [§3.3.4, p. 3224];
  the two statements count different things.
- **Discrepancy.** The header says `last_level_temperature` is "Currently only compatible with
  universal compaction" and the wiki says tiered storage supports only universal, but
  `Compaction::EvaluateProximalLevel` enables per-key placement for leveled as well [RDB-SRC
  `advanced_options.h:1007–1011`; `db/compaction/compaction.cc:1020–1030`]. The wiki warns that
  under leveled "it may cause infinite auto compaction if majority of data is hot".
- **Default.** Off.
- **For the port.** Deferred: each engine sits on one device (note 12 §6.1). Tiering in mantle is
  a data-layer concern (chunk store), not a metadata one.

### 2.10 Shared budgets across instances

Note 12 §1.10 has DONG21's case for sharing memory, compaction bandwidth, threads, disk and
deletion rate across instances. The mechanisms in 11.8.1 and their measured behaviour:

- **Rate limiter.** Token bucket, refill every 100 ms, fairness 10, flush charged at high and
  compaction at low priority; one object shareable across databases [RDB-SRC
  `include/rocksdb/rate_limiter.h:166–170`]. The auto-tuner re-evaluates every 100 refills,
  lowers the rate when the bucket drained in under 50% of intervals and raises it above 90%, in
  5% steps, within [max/20, max] [RDB-SRC `util/rate_limiter.cc:214, 435–460`]. Measured
  (NON-PEER-REVIEWED): ingest at 10 MB/s with a 1000 MB/s ceiling settled "around 125MB/s"
  [RDB-BLOG `2017-12-18-17-auto-tuned-rate-limiter`]; no hardware. It still covers only flush
  and compaction: "RocksDB does not enforce rate limit for anything other than flush and
  compaction, e.g. write to WAL" [`rate_limiter.h:144–146`]. MYROCKS added compaction rate limits
  "to mitigate their effect on user query I/O" without publishing the rates [§3.2.3.2,
  p. 3223]. Default off.
- **Write buffer manager.** Shared across databases; flushes the writing database when mutable
  memory exceeds 7/8 of the budget, or when total memory reaches it and mutable memory is at
  least half; can charge memtables to the block cache; `allow_stall` stalls every writer of
  every sharing database [RDB-SRC `include/rocksdb/write_buffer_manager.h:50–126`]. The wiki's
  "about 90%" and 1 MB dummy entries are stale; the code uses 7/8 and 256 KB [RDB-HIST L2464].
  No measurement. Default off.
- **SST file manager.** Shared; enforces a space limit (the database that crosses it goes
  read-only with `Status::SpaceLimit()`); deletes through a trash directory at
  `rate_bytes_per_sec`, truncating large files in `bytes_max_delete_chunk` pieces (default
  64 MB) [RDB-SRC `include/rocksdb/sst_file_manager.h:38–137`]. Meta's setting: "~64MB chunks
  ... around 128MB per second", because TRIM stalls "might take seconds to tens of seconds"
  [MYROCKS17, "Other technical tips"]. Default off. The header warns that secondaries "cannot
  read shared SST files that have been truncated" [`db.h:250–256`].
- **Thread pools.** One process-wide `Env` with LOW, HIGH and BOTTOM pools; "Without a HIGH pool,
  long running major compaction jobs could potentially block memtable flush jobs of other db
  instances, leading to unnecessary Put stalls" [RDB-WIKI "Thread-Pool"]. The bottom pool dates
  from 2017 [DONG21 App. A, p. 44].
- **Prioritization.** "we learned it is important to support prioritization among RocksDB
  instances" [DONG21-TOS §4.1, p. 26:11]; 11.8.1 has I/O priorities per request but no priority
  between databases sharing one limiter (DERIVED from the rate-limiter interface).
- **db_bench.** `--num_multi_db=N` opens N databases with one `Options` object, so the block
  cache, write buffer manager, rate limiter and thread pools are shared by construction
  [RDB-SRC `db_bench_tool.cc:415, 5854–5866`]. There are flags for the rate limiter, the write
  buffer manager and cache charging, but none for the SST file manager or `allow_stall`.
- **For the port.** These are the mechanisms behind note 12 §6.1's node budgets and behind
  CLAUDE.md rule 2. The port implements them as node-level objects from the start; C5 runs
  RocksDB with the same sharing. `allow_stall` is excluded: mantle refuses at the door with a
  typed `Busy` (note 12 §1.10).

### 2.11 SST partitioner

- **Mechanism.** A factory can force a new output file between two keys during compaction and
  veto trivial moves across a boundary; the built-in one cuts when a fixed-length prefix changes
  [RDB-SRC `include/rocksdb/sst_partitioner.h`; `options.h:346`, "THE FEATURE IS STILL
  EXPERIMENTAL"]. The stated purpose is to "lower the write amplification during SST file
  promote to higher level".
- **Origin.** Contributed in 6.12 (PR #6957); the commit message's reason: "so that promotion of
  some SST file does not cover huge key space on next level". No Meta system and no measurement
  are published.
- **Default.** Off; no db_bench flag.
- **For the port (INFERENCE).** With one instance per range the partitioner has no range
  boundaries to respect. It would matter under note 12 §6.1's fallback (one instance per device,
  one column family per range) or to make split points coincide with file boundaries so a split
  hard-links whole files (note 12 §6.4). Deferred until a split benchmark shows straddling files
  cost enough.

### 2.12 Periodic and TTL compaction

- **Mechanism.** Note 12 §1.5. The "RocksDB controls" sentinel resolves at open: `ttl` becomes 30
  days for block-based tables under every compaction style; `periodic_compaction_seconds`
  becomes 30 days under leveled compaction only when a compaction filter is installed, and under
  universal always [RDB-SRC `db/column_family.cc:418–470`].
- **For the benchmark.** A stock leveled database therefore runs TTL compaction on files older
  than 30 days. A benchmark shorter than that never triggers it; a long-running soak test will
  (DERIVED). `--use_keep_filter` in db_bench installs a no-op filter and so turns on periodic
  compaction as a side effect [RDB-SRC `db_bench_tool.cc:907`].
- **Meta's use.** ZippyDB's TTL [ZDB-BLOG]; MyRocks' Lmax cleanup, "if it was older than a
  settable threshold" (value not published) [MYROCKS §3.2.3.3]; the deletion deadline guarantee
  that tombstones reach the oldest level within a threshold [DONG21-TOS §6.3.2, p. 26:19].
- **For the port.** Must match the 30-day default to be comparable in soak tests, and mantle sets
  both from the grace period (note 12 §6.6).

### 2.13 Deletion enhancements

- **SingleDelete and deletion-triggered compaction.** Note 12 §1.6 and §1.5 above for the
  measurement. DTC is a table-properties collector,
  `NewCompactOnDeletionCollectorFactory(window, trigger, ratio)`, off unless installed [RDB-SRC
  `include/rocksdb/utilities/table_properties_collectors.h:98`].
- **Range-tombstone conversion** (11.3.0). An iterator crossing at least N contiguous point
  tombstones with no live key inserts one range tombstone over the run
  [`min_tombstones_for_range_conversion`, RDB-SRC `advanced_options.h:1442`, default 0, off].
  Measured (NON-PEER-REVIEWED; 1 million keys, 8 threads, `--seek_nexts=100`, no hardware named):
  forward scans 2,685 → 266,733 ops/s (~99×), reverse 519 → 191,119 (~368×), no change without
  deletes [RDB-BLOG `2026-06-22-range-tombstone-conversion`]; "This has become a common problem
  within Meta".
- **DeleteRange v2** (NON-PEER-REVIEWED; 5 million keys, 10,000 range tombstones, cached, 10 runs):
  point lookups 0.61 µs against 1.32 µs for v1; short scans 2.82 against 6.23 µs [RDB-BLOG
  `2018-11-21-delete-range`]. Users named: MyRocks, Rocksandra, Marketplace.
- **For the port.** An S3 prefix delete produces a run of point tombstones in the Name layer,
  which later listings scan across (note 12 §1.6). The port needs SingleDelete (Name rows are
  put once, then deleted), DTC, and a tombstone-run mitigation; C8 measures scans over deleted
  runs with each.

### 2.14 Compaction output alignment, and other always-on changes

- **Output alignment.** An output file is cut at a next-level file boundary once it reaches 50%
  of the target size plus 5% per boundary crossed, capped at 90%; non-last-level outputs may grow
  to twice the target [RDB-SRC `db/compaction/compaction_outputs.cc:340–357`]. Measured
  (NON-PEER-REVIEWED; `db_bench fillrandom,readrandom`, 400 million keys, 32 MB target files,
  12 background jobs, no hardware named): cumulative compaction writes 285.90 GB → 249.97 GB and
  time 2926.7 s → 2534.9 s [RDB-BLOG `2022-10-31-align-compaction-output-file`]; DERIVED 12.6%
  fewer bytes and 13.4% less time. On since 7.8.0; the opt-out was removed in 9.0.0 [RDB-HIST
  L1139, L708].
- **Compaction readahead** 2 MB by default since 8.7.0 [RDB-SRC `options.h:1281`].
- **Subcompactions** default 1 [`options.h:926`]; no published measurement.
- **For the port.** Alignment changes write amplification by about an eighth on RocksDB's own
  benchmark, so a port without it would look 12–13% worse on compaction bytes for a reason
  unrelated to Rust. It is in the port's required set (§4.1).

### 2.15 Read-path enhancements

- **Async I/O and parallel MultiGet.** Seek issues reads at every level before waiting; MultiGet
  runs per-file reads in parallel as C++20 coroutines, on io_uring on Linux [RDB-BLOG
  `2022-10-07-asynchronous-io-in-rocksdb`; RDB-WIKI "Asynchronous-IO"]. Measured on Meta's
  remote Warm Storage flash, 5 million keys of 32 + 512 bytes, batches of 8, four threads:
  MultiGet 1292 → 508 µs/op (2.55×), p99 29.3 → 10.9 ms; long scans 158 → 326 MB/s; CPU "may
  increase 6-15%" [ibid.]. Default off (`async_io = false`, `options.h:2234`), needs a folly
  build for coroutines.
- **Data-block hash index.** A byte array per block maps key hashes to restart intervals.
  "throughput is increased by 10% at an overhead of 4.6% more space" on a cached workload, and
  helps while "the workload has a cache miss ratio smaller than 40%" [RDB-BLOG
  `2018-08-23-data-block-hash-index`]. Default off (`table.h:362`).
- **Interpolation search in index blocks** (11.0.0). +9.2% readrandom on uniform keys with
  `index_shortening_mode=1`; −40.1% on skewed keys with forced interpolation, +2.6% with the
  adaptive mode [RDB-BLOG `2026-05-04-interpolation-search`; RDB-PR #14383]. Default off
  (`index_block_search_type = kBinary`, `table.h:354`; `uniform_cv_threshold = −1`, `table.h:764`,
  although HISTORY says 0.2). Files written with it are "a corruption error" to readers older
  than 11.0.0.
- **Partitioned index and filters** (note 12 §1.4). New measurement: on LinkBench over 300 GB of
  SSD, reducing memory from 6 GB to 2 GB dropped throughput from 38K to 23K tps without
  partitioning and to 30K with it [RDB-WIKI "Partitioned-Index-Filters", "Success stories"].
- **For the port.** Mantle's keys are prefixed by directory or file id, so they are far from
  uniform; interpolation search is excluded. The hash index suits the Block layer's point reads
  (TEC's two-thirds). Async I/O is deferred: its measured gain was against remote flash latency.

### 2.16 Write-path and format enhancements

- **Parallel compression** (revamped in 10.7.0): ZSTD at default level with three threads went
  from 38% more throughput for 73% more CPU to 58% for 25% [RDB-BLOG
  `2025-10-08-parallel-compression-revamp`; RDB-PR #13910 gives the `/dev/shm` fillseq loop].
  Ignored for LZ4, Snappy and fast ZSTD since 11.5.0 [RDB-HIST L62]. Default off
  (`parallel_threads = 1`).
- **Dictionary compression**: off by default (`max_dict_bytes = 0`); the blog's guidance is 16 KB
  dictionaries trained on 100× that many bytes, with benefit "in use cases with data block size
  up to 16KB", and no numbers [RDB-BLOG `2021-05-31-dictionary-compression`]. The measured
  figures are DONG17's 3% and MYNVM's 11% → 1.5% growth (§1.5).
- **Per-key-value protection**: note 12 §6.7; the blog publishes no cost, only that "one per
  thousand machines in our fleet will at some point experience a hardware error that is exposed
  to an application" [RDB-BLOG `2022-07-18-per-key-value-checksum`].
- **format_version**: 7 by default since 10.11.0 [RDB-SRC `table.h:739`; RDB-HIST L188]; version 4
  "significantly reduces the index block size, in some cases around 4-5x" [RDB-BLOG
  `2019-03-08-format-version-4`].
- **Online validation**: `force_consistency_checks = true`, `flush_verify_memtable_count = true`,
  `verify_sst_unique_id_in_manifest = true` by default, at "two extra CPU cycles per million on a
  major production workload" [RDB-HIST L2023].

### 2.17 Behaviour with the WAL off

Mantle's configuration, per note 12 §6.2. What 11.8.1 does and tests:

- A crash loses the memtables; recovery rebuilds from the MANIFEST. RocksDB's crash tests check
  that what survives is a prefix: "**Process crash with WAL disabled** (`WriteOptions::disableWAL=1`),
  which loses writes since the last memtable flush"; they do not cover "alternating writes with
  WAL disabled and WAL enabled" [RDB-BLOG `2022-10-05-lost-buffered-write-recovery`].
- On close, "By default RocksDB will flush all memtables on DB close if there are unpersisted
  data (i.e. with WAL disabled)" [RDB-SRC `options.h:1535–1541`].
- A retryable flush error with the WAL off is a soft error (6.13) [RDB-HIST L2072]; mantle turns
  automatic resume off (note 12 §6.7).
- 11.7.0: checkpoint and backup "now flush WAL-disabled unpersisted data" under `LockWAL` [RDB-HIST
  L29]. Before that, a checkpoint of a WAL-off database could miss the memtable (DERIVED); note 12
  §6.3 flushes before every checkpoint, which the port must keep.
- **For the port.** The prefix property is what note 12 §6.2 step 5 relies on. The port's crash
  tests must check it on real disks, as RocksDB's do, and the C5 benchmark measures flush
  frequency against Raft-log replay length.

---

## 3. Workload models and their generator

### 3.1 What CAO20 publishes

- **Model families, not fitted values.** Values and scan lengths fit a Generalized Pareto
  distribution; per-key access counts a power law; key-range hotness a two-term power model; QPS
  a sine with a 24-hour period; models are chosen by the smallest fit standard error [CAO20
  §7.2, §7.4, pp. 218–220]. The paper publishes no fitted parameter values, no hardware, and no
  RocksDB options except a cache "configured with the same value as the production setup"
  [§7.3, p. 219]. "We do not consider deletions in our current models" [§7, p. 217].
- **Key and value sizes** [CAO20 Table 2, p. 215], bytes, mean (SD): UDB keys 27.1 (2.6), values
  126.7 (22.1); ZippyDB keys 47.9 (3.7), values 42.9 (26.1); UP2X keys 10.45 (1.4), values 46.8
  (11.6). ZippyDB keys are "fixed at either 48 or 90 bytes" in the model [§7.2, p. 218]. UDB's
  Assoc_count column family has keys and values of "exactly 20 bytes"; its secondary-index
  families have values under 8 bytes [pp. 214–215].
- **Key-range size** should be "close to the average number of KV-pairs in an SST file" to keep
  locality [§7.2, p. 218].
- **Accuracy.** On 50 million preloaded pairs with production cache size, three runs each:
  YCSB-zipfian caused "at least 500% higher" block reads, the other YCSB distributions "1000% or
  more", and the key-range model (Prefix_dist) "only 40% higher" read bytes than replay [§7.3,
  p. 219]; the introduction gives 43% and "about 77% of the cache hits" [§1, p. 210]. The paper
  is internally inconsistent on the first figure.

### 3.2 What the wiki publishes

Two parameter sets, both NON-PEER-REVIEWED [RDB-WIKI "RocksDB-Trace,-Replay,-Analyzer,-and-Workload-Generation",
"Synthetic Workload Generation based on Models"]:

- **Social graph (column family not named).** Value size σ = 226.409, k = 0.923, θ = 0; key
  access a = 0.001636, b = −0.7094, c = 3.217×10⁻⁹; QPS A = 147.9, B = 8.3×10⁻⁵, C = −1.734,
  D = 1064.2; scan length σ = 1.747, k = 0.0819, θ = 0. DERIVED: value mean 2940 B and median
  220 B before db_bench's clamp. The unit of B is not stated.
- **ZippyDB.** The command:

```
./db_bench --benchmarks="mixgraph" -use_direct_io_for_flush_and_compaction=true
  -use_direct_reads=true -cache_size=268435456 -keyrange_dist_a=14.18
  -keyrange_dist_b=-2.917 -keyrange_dist_c=0.0164 -keyrange_dist_d=-0.08082
  -keyrange_num=30 -value_k=0.2615 -value_sigma=25.45 -iter_k=2.517 -iter_sigma=14.236
  -mix_get_ratio=0.85 -mix_put_ratio=0.14 -mix_seek_ratio=0.01
  -sine_mix_rate_interval_milliseconds=5000 -sine_a=1000 -sine_b=0.000073 -sine_d=4500
  --perf_level=2 -reads=420000000 -num=50000000 -key_size=48
```

  DERIVED: the value distribution has mean 34.5 B and median 19.3 B (CAO20 reports a 42.9 B
  mean); the sine period is 23.9 hours; the mix 0.85/0.14/0.01 differs from CAO20's trace
  without deletes, 0.830/0.138/0.032; the scan-length distribution has k > 1 and so no finite
  mean, and is cut by `mix_max_scan_len`.

### 3.3 What db_bench 11.8.1 does with them

Read from the source; not published claims [RDB-SRC `tools/db_bench_tool.cc`]:

- **The key-range model is bypassed unless the per-key model is also set.** `if (FLAGS_key_dist_a
  == 0 || FLAGS_key_dist_b == 0) use_random_modeling = true;` and random modeling takes
  `key_rand = ini_rand`, a uniform key [`:8559–8576`]. The wiki's ZippyDB command sets neither,
  so its `keyrange_*` parameters have no effect and keys are uniform. db_bench implements
  key-range hotness as a two-term *exponential* (`f(x)=a*exp(b*x)+c*exp(d*x)`, `:1765–1776`)
  where CAO20 fitted a two-term *power* model.
- **The QPS model is off by default.** Rate control runs only with `-sine_mix_rate=true`
  [`:1817–1818, 8594`], which the command omits.
- **Correlated draws.** Query type, value size and scan length are all computed from the same
  uniform number `u` [`:8569, 8583, 8644, 8678`]; CAO20 checked its variables for "very low
  correlations" [§3, p. 212] and models them independently. `u = 0` is possible and gives an
  infinite Pareto draw [`:8295–8302`].
- **Clamps.** Values below 10 B become 10 B; values above `mix_max_value_size` (1024) wrap modulo
  it [`:8646–8649`].
- **Defaults.** `value_k = 0.2615` and `value_sigma = 25.45` are the ZippyDB values, labelled "Use
  reasonable defaults based on the mixgraph paper"; `mix_get_ratio = 1.0` [`:1787–1811`].

### 3.4 Consequences for the benchmark (INFERENCE)

- The published command is a uniform-key, constant-rate, small-value workload. It is still worth
  running, labelled as such, because it is the configuration anyone reproducing Meta's ZippyDB
  benchmark would run (W2-a).
- A workload that models ZippyDB as CAO20 describes needs a generator that applies key-range
  hotness with per-key power-law access inside each range, independent draws, and the sine rate.
  The port's harness implements that generator (W2-b), and RocksDB is driven by the same
  generator through its C API, so both engines see identical operation streams. ZippyDB's
  per-key parameters were never published; W2-b uses the social-graph `a`, `b` as a stated
  substitute until mantle has its own trace.
- Every model-generated workload gets replaced by mantle's own recorded metadata trace once one
  exists (note 12 §6.6, W5).

---

## 4. What the port must implement to be comparable

### 4.1 Required: on by default in 11.8.1, or needed by mantle's setting

| Feature | Why | Source |
|---|---|---|
| Leveled compaction, dynamic level sizing, `kMinOverlappingRatio` | defaults; note 12 §1.5 | `advanced_options.h:666, 725` |
| Output cut at next-level boundaries (50% + 5%/boundary, ≤ 90%) | default, no opt-out; ~12.6% of compaction bytes | `compaction_outputs.cc:340–357` |
| 30-day `ttl` sentinel resolution; periodic compaction with a filter | defaults | `column_family.cc:418–470` |
| Block-based format v7, XXH3 block checksums, `optimize_filters_for_memory` | defaults; on-disk parity for the comparison | `table.h:374, 574, 739` |
| LZ4 default compression; per-level compression; ZSTD with dictionaries | default and Meta's production setting | RDB-HIST L61; §1.5 |
| AutoHCC-class lock-free shared cache, with LRU-like behaviour under churn measured | default; §2.2 | `block_based_table_factory.cc:474–477` |
| Bloom and Ribbon filters, per-level choice, optional last-level omission, prefix filters | MyRocks and ZippyDB settings; note 12 §2.4 | `filter_policy.h:184–210` |
| Partitioned index and filters; data-block hash index | Meta settings; Block-layer point reads | `table.h:326, 362, 519` |
| WAL off, `kPersistedTier` reads, flush-on-close, prefix recovery | mantle's setting | note 12 §6.2; §2.17 |
| Shared rate limiter (auto-tuned), write buffer manager with cache charging, SST file manager with rate-limited deletion, shared thread pools | many instances per node | §2.10 |
| SingleDelete, DeleteRange, deletion-triggered compaction, tombstone-run mitigation | metadata deletes | §2.13 |
| Checkpoint, ingestion into an empty instance with `link_files`, CRC-32C file checksums | moves and snapshots | note 12 §1.9, §6.3 |
| Statistics that match RocksDB's (bytes read and written per level, stall time, cache hits, compaction bytes) | the benchmark compares them | `include/rocksdb/statistics.h` |

### 4.2 Required for measurement only

The port's harness must report write amplification as RocksDB defines it for DONG21 Table 3 —
flush and compaction bytes over flushed bytes, "WAL writes are not included" (note 12 §1.5) — and
separately the Raft log's bytes, so C5's comparison counts every byte written to the device.

### 4.3 Deferred or excluded, with reasons

| Feature | Decision | Reason |
|---|---|---|
| Integrated BlobDB, blob direct write | excluded | values of 43–127 B; loses small-value scans (§2.4) |
| Wide columns, attribute groups | excluded | no use in mantle's encoding; nothing measured (§2.6) |
| Remote compaction, secondary and follower instances | deferred | built for disaggregated storage; WAL-off secondaries see only flushed data (§2.8) |
| Time-aware tiered storage, per-key placement | deferred | one device per engine (§2.9) |
| Flash or NVM secondary cache | deferred | not in 11.8.1's tree; local-disk engines (§2.3) |
| Async I/O and coroutine MultiGet | deferred | measured against remote flash (§2.15) |
| Interpolation search | excluded | prefixed keys are skewed; −40% on skew (§2.15) |
| User-defined timestamps | deferred | note 12 §1.8 |
| SST partitioner | deferred | per-range instances (§2.11) |
| `atomic_replace_range` | excluded | documented bound bug (§2.7) |
| Reverse comparator | excluded | forward-only listing (§1.5) |
| `allow_stall`, `unordered_write` | excluded | typed refusal instead (note 12 §1.10); snapshot semantics (note 12 §1.3) |

---

## 5. Benchmark matrix

### 5.1 Structure

A run is one engine × one configuration × one workload × one scale, on one host.

- **Engines.** RocksDB 11.8.1 (C++; db_bench where it can express the configuration, otherwise
  mantle's harness through the C API) and mantle's port (mantle's harness). The same harness drives
  both wherever db_bench cannot, so generator differences cannot masquerade as engine differences
  (§3.4).
- **Reported per run.** Throughput; latency p50, p99, p99.9 per operation type; write
  amplification (engine) and device bytes written (engine plus Raft log); space amplification;
  bytes read per operation; block-cache hit ratio; CPU seconds per operation; peak RSS; stall or
  `Busy` time. These are the quantities Meta reported for its enhancements (MYROCKS Table 1,
  DONG21 Table 3, DISAGG Tables 1–2, CAO20 §7.3), so each published figure has a counterpart.
- **Statistics.** Rounds, steady-state detection and intervals follow note 21 §9; runs are long
  enough to reach compaction steady state, because "it is essential to run performance tests for
  an extended amount of time, lest these issues go undetected" [SILK §4.7, p. 758, via note 12
  §2.6]. CAO20's three runs per test are not enough by note 21's criteria.
- **Hardware.** Every run on mantle's own hosts, devices identified and probed (CLAUDE.md rule 5).
  The published numbers are not reproduced absolutely; the claim is the ratio of port to RocksDB
  on the same host, and, where a configuration reproduces a published setup, whether RocksDB on
  mantle's host shows the same direction and rough size of effect that Meta reported.

### 5.2 Configurations

| ID | Configuration | Settings (11.8.1 option names; db_bench flags where they exist) | Source |
|---|---|---|---|
| **C0** | Stock 11.8.1 library defaults | Library defaults, with db_bench's differing defaults overridden: `--compression_type=lz4 --level_compaction_dynamic_level_bytes=true --cache_type=auto_hyper_clock_cache --cache_size=33554432 --pin_top_level_index_and_filter=true`; no filter (`--bloom_bits=-1`); WAL on, `sync=false` | §0 item 2; RDB-SRC defaults (note 12 §1.2) |
| **C1** | RocksDB's published benchmark setting | `tools/benchmark.sh` defaults: 20 B keys, 400 B values, 8 KB blocks, 128 MB write buffers and target files, 1 GB L1, fanout 8, 8 levels, L0 4/20/30, ZSTD, 10 bloom bits, `pin_l0_filter_and_index_blocks_in_cache`, 16 background jobs, `bytes_per_sync` 1 MB, `open_files=-1`; the Performance-Benchmarks page used 900 M keys, a 6 GB cache, with and without direct I/O, on an m5d.2xlarge (8 vCPU, 32 GB, one NVMe) | RDB-SRC `tools/benchmark.sh:169–352`; RDB-WIKI "Performance-Benchmarks" (newest results: 7.2.2) |
| **C2** | MyRocks / UDB production | 16 KB blocks; leveled, dynamic, multiplier 10, L0 trigger 4; 10-bit Bloom with `optimize_filters_for_hits=true`; prefix extractor of 20 B (for mantle: the parent-id prefix of a Name key); no compression L0–L2, LZ4 to Lmax−1, ZSTD at Lmax (C2-a, 2017) or LZ4 on all but Lmax (C2-b, 2018–2020); `max_dict_bytes=16384`, `zstd_max_train_bytes=1638400`; `cache_index_and_filter_blocks` with L0 pinned; block cache 12 GB scaled to the host's memory ratio; direct I/O; shared rate limiter; SST file manager at 128 MB/s in 64 MB chunks; DTC installed; SingleDelete for put-once rows | DONG17 §2–§4; MYROCKS §3.2, §6.1; ZSTD-BLOG; MYROCKS17; RDB-WIKI "Setup-Options-and-Basic-Tuning" |
| **C3** | ZippyDB | C3-a: the wiki command exactly (§3.2), with direct I/O and a 256 MB cache. C3-b: the same engine settings under the CAO20-faithful generator (W2-b). C3-c: C3-b plus a secondary cache in the 20 GB : 100 GB ratio, using the in-tree compressed secondary as the only available implementation, labelled as a substitute for CacheLib on flash. TTL through periodic compaction with a no-op filter | RDB-WIKI (Trace page); CAO20 §7; DISAGG §4.1.3; ZDB-BLOG "Data model" |
| **C4** | DISAGG's local-SSD reference | 20 B keys, 400 B values, 1 B keys (scaled to the host), 16 GB cache, 8 KB blocks, ZSTD bottom and LZ4 above, direct I/O, `compaction_readahead_size` 4–8 MB, write buffer ≥ 64 MB, target file ≥ 64 MB | DISAGG §4.1.4, §4.1.6, §5 |
| **C5** | Mantle's setting | N instances per node (N from note 12 §6.6's bound; `--num_multi_db=N`), one shared AutoHCC cache with every memory role charged, one write buffer manager charged to it, one auto-tuned rate limiter per device, one SST file manager per device, shared HIGH/LOW thread pools, `max_bgerror_resume_count=0`, CRC-32C file checksums; engine WAL off. Durability variants: **C5-a** RocksDB with each instance's WAL on and `sync=true` (N WALs; the durable stock baseline); **C5-b** engine WAL off plus one shared group-committing log per device on the *same* device as the engines, with the platform's full flush (mantle's design; Delos NativeLoglet analogue); **C5-c** as C5-b with the log on a separate device (Delos LDLoglet analogue); **C5-d** engine WAL off and no log (upper bound, not durable) | note 12 §6.1–§6.2, §6.7; DELOS-VC §5.1; DELOS-LSP §3.2, §4.1; DONG21 §4 |
| **C6** | Filter variants on C5 | 10-bit Bloom everywhere; Ribbon hybrid with `bloom_before_level=0`; Ribbon everywhere (`-1`); Bloom without the last level (`optimize_filters_for_hits`); measure memory, zero-result lookup I/O and flush/compaction CPU | RIBBON; RDB-BLOG ribbon; MYROCKS §3.2.3.1; note 12 §2.4 |
| **C7** | Cache variants on C5 | AutoHCC, FixedHCC, LRU (`high_pri_pool_ratio` 0.5); threads 1, 10, cores, 100; cache sized for ~97% and ~50% hit rates; plus a nearly-pinned case for the eviction cap | RDB-PR #10626, #11738, #12141 |
| **C8** | Deletion-heavy, on C5 | none / SingleDelete / DTC / both / plus tombstone-run conversion (`min_tombstones_for_range_conversion=8`); scans across deleted runs forward | MYROCKS Fig. 5; RDB-BLOG range-tombstone conversion; DONG21-TOS §6.3 |
| **C9** | Move path | flush, checkpoint, list with CRC-32C, transfer over QUIC, verify, open (per-range instance); and SST ingestion into an empty instance with `link_files` and `verify_checksums_before_ingest=true`; and the write-block duration of ingesting into a live instance (`ingestexternalfile`) | note 12 §6.3–§6.5; §2.7; ROCKSANDRA; DISAGG §6.2 |

C0, C1 and C4 measure the port against RocksDB on settings anyone can reproduce. C2 and C3 measure it
on Meta's published production settings. C5 is the configuration mantle will ship, and C6 to C9
isolate the enhancements whose value for mantle is still an open question. C5-a against C5-b
measures what the shared log buys over per-instance WALs. C5-b against C5-c measures the
device contention Delos reported. C5-d bounds both.

### 5.3 Workloads

| ID | Workload | Parameters | Source |
|---|---|---|---|
| **W1** | RocksDB standard suite | `benchmark.sh` sequence: bulkload (WAL off, auto-compaction off, then compact), readrandom, fwdrange, overwrite, readwhilewriting at 2 MB/s writes, fwdrangewhilewriting; plus `regression_test.sh`'s multireadrandom and seekrandom | RDB-SRC `tools/benchmark.sh`, `tools/regression_test.sh:133–148` |
| **W2** | ZippyDB model | W2-a: wiki command as published (uniform keys, constant rate). W2-b: CAO20-faithful generator: 30 key ranges with the wiki's hotness curve, per-key power law inside ranges (social-graph a, b as the stated substitute), independent value and scan draws (GPD k = 0.2615, σ = 25.45; k = 2.517, σ = 14.236, capped), keys of 48 or 90 B, mix 0.83/0.14/0.03 Get/Put/Seek, sine rate with a 24-hour period; 50 M keys preloaded | §3; CAO20 §4–§7 |
| **W3** | Social-graph model | wiki Set A parameters, same generator | RDB-WIKI (Trace page) |
| **W4** | Size sweep | (key, value) means and SDs of CAO20 Table 2: UDB (27.1, 126.7), ZippyDB (47.9, 42.9), UP2X (10.45, 46.8), and UDB Assoc_count (20, 20) | CAO20 Table 2, p. 215 |
| **W5** | Mantle trace | recorded metadata operations replayed with key-range locality preserved | note 12 §6.6; CAO20 §7 |
| **W6** | Tectonic-shaped | per-node mix with Block-layer point reads two-thirds of operations, Name-layer prefix scans, hot ranges near a 10 KQPS-per-range cap | TEC §3.3, §6.3 (DERIVED from the shares) |
| **W7** | Delete-heavy listing | create a directory's entries, delete them all, list the prefix repeatedly | DONG21-TOS §6.3.1; note 12 §1.6 |

### 5.4 Which runs are required

| | W1 | W2-a | W2-b | W3 | W4 | W5 | W6 | W7 |
|---|---|---|---|---|---|---|---|---|
| C0 | ● | ● | ● | | ● | | | |
| C1 | ● | | | | | | | |
| C2 | ● | | ● | ● | ● | ○ | | ● |
| C3 | | ● (C3-a) | ● (C3-b, C3-c) | | ● | ○ | | |
| C4 | ● | | | | | | | |
| C5 | ● | | ● | | ● | ○ | ● | ● |
| C6–C8 | | | ● | | | ○ | ● | C8 only |
| C9 | move of a tablet at the sizes note 12 §6.6 bounds | | | | | | | |

● required; ○ required once mantle's trace exists. Scale for C5: tablet sizes and counts per
node from note 12 §6.6's bounds; ZippyDB's 50–100 GB per shard [ZDB-BLOG] and "tens or hundreds"
of shards per host [DONG21 §4, p. 38] set the upper reference.

---

## 6. What remains unknown

- **Meta's actual option values** for ZippyDB and Tectonic's metadata tier: compaction style
  variants, block size, filters, cache sizes, memtable budget. DONG21-TOS Table 5 counts the
  distinct configurations but gives none of them.
- **Whether ZippyDB or Delos disable RocksDB's WAL.** Both papers imply the log carries
  durability (DISAGG §6.1; DELOS-LSP §3.2) without saying so.
- **ZippyDB's per-key access parameters** (`key_dist_a`, `key_dist_b`) and whether the wiki's
  ZippyDB command was run with a db_bench that honoured the key-range model; the current one does
  not (§3.3).
- **Where Meta's figures for the NON-PEER-REVIEWED enhancements were measured.** Ribbon's cost
  model, HCC, output alignment, range-tombstone conversion, interpolation search and parallel
  compression name no hardware, and several ran on `/dev/shm`.
- **The CacheLib secondary cache implementation** that ZippyDB ran is not in RocksDB 11.8.1; its
  admission policy and flash layout are not published in the sources read.
- **Whether per-key placement is safe under leveled compaction** despite the header and wiki
  (§2.9); not needed now.
- **The cost of Delos-style periodic flushing on a shared device.** Delos gives the flush policy
  but not its interval or its effect on log latency; C5-b measures it for mantle.
- **Whether the port can match C++ on Ribbon construction and AutoHCC lookup** without `unsafe`
  outside the files `scripts/check-contracts.py` allows (CLAUDE.md rule 7); only C6 and C7 will
  tell.
- **How RocksDB is driven through the same generator as the port.** The C API covers the
  operations but not every option in C5 (note 12 §5.2 on the bindings); a C++ shim may be needed
  for the benchmark only, which is a build decision outside this note.
- **The successors of RIBBON** (SEA 2022, JACM 2026) were not read; their numbers may differ from
  the preprint's.

---

## Appendix A: quantitative quick reference

| Fact | Value | Source |
|---|---|---|
| ZippyDB secondary cache in production | +50–60% read IOPS, −30–40% read latency; 20 GB + 100 GB | [DISAGG §4.1.3, p. 192:9] |
| ZippyDB replica rebuild, local copy vs Tectonic file copy | ~50 min vs <1 min | [DISAGG §6.2, pp. 192:15–16] |
| ZippyDB per-DB failover, local vs Tectonic | 51 min vs 49 s | [DISAGG Table 3, p. 192:17] |
| Remote compaction, one ZippyDB use case | >50% less cross-DC I/O; −20.4% compaction time | [DISAGG §7.2, p. 192:20] |
| Tectonic vs local SSD Get, 64 threads (RocksDB 7.4) | 54.8K vs 334K QPS; p99 2.9 vs 0.42 ms | [DISAGG §5 Table 2, pp. 192:13–15] |
| Delos converged log, put knee | ~15K ops/s vs 150K disaggregated | [DELOS-VC §5.1, p. 627] |
| Delos burst on shared SSD | p99 > 1 s at 2500 puts/s | [DELOS-VC §5.1, p. 628] |
| Delos vs ZooKeeper puts at 15 ms p99 | ~26K vs >30K/s | [DELOS-VC §5.2, p. 629] |
| Delos group commit | 2× at 20 ms p99, 100 B writes | [DELOS-LSP §4.4, §5.2, PDF pp. 10–12] |
| MyRocks vs compressed InnoDB, one UDB replica set | 37.7% space; −38% write CPU; −74% bytes written | [MYROCKS Table 1, pp. 3226–3227] (DERIVED ratios) |
| Reverse key comparator | +15% descending scans | [MYROCKS §3.2.1.2, p. 3222] |
| MyRocks bulk load vs InnoDB | 2.2–5.7× | [MYROCKS §3.2.3.4, p. 3224] |
| Direct I/O slab memory | −80% (was 2–3 GB per TB of SST) | [MYROCKS §6.1.1, p. 3228] |
| Key prefix encoding / seqno zeroing / dictionary | 3–17% / 0.03–23% / 3% | [DONG17 §3.2, PDF p. 4] |
| Strong last-level compression over lightweight only | 15–30% | [DONG17 §4, PDF p. 5] |
| Block cache hit rate, data / metadata (MyRocks prod) | 79.3% / 99.97% | [DONG17 appendix, PDF p. 8] |
| User-defined timestamps vs timestamps in keys | 1.2–2.0× | [DONG21-TOS Table 6, pp. 26:22–23] |
| Dynamic vs LevelDB-style leveling space overhead at 1 B keys | 12.4% vs 22.4% | [DONG21-TOS Table 4, p. 26:7] |
| Ribbon w=128 vs RocksDB Bloom, 1% FP | 6–8% vs 49.8% space overhead; 3.3–7.9× build, 3.9–5.8× query time | [RIBBON Table 2, p. 11] (PREPRINT; DERIVED ratios) |
| Filter memory and CPU at Meta; mean filter age | ~10% RAM, ~1% CPU; ~3 days | [RIBBON §1 fn. 5, p. 1] (PREPRINT) |
| HCC vs LRU, DB cached, 48 threads | 1604.9K vs 310.5K ops/s | [RDB-PR #10626] (NON-PEER-REVIEWED) |
| AutoHCC vs LRU at ~50% hit rate, 10 threads | 541K vs 725K ops/s | [RDB-PR #11738] (NON-PEER-REVIEWED) |
| MyNVM (16 GB DRAM + 140 GB NVM) vs 96 GB DRAM | +10% mean, +20% p99 latency, −8% QPS | [MYNVM §6, PDF pp. 2, 11] |
| RocksDB as FIFO flash cache vs CacheLib | 53% vs 76% hit ratio; +50% CPU | [CACHELIB §5.1, p. 778] |
| Output alignment | −12.6% compaction bytes, −13.4% time | [RDB-BLOG align-compaction-output-file] (DERIVED) |
| Range-tombstone conversion | 99× forward, 368× reverse scan | [RDB-BLOG range-tombstone-conversion] (NON-PEER-REVIEWED) |
| Data-block hash index | +10% throughput, +4.6% space | [RDB-BLOG data-block-hash-index] (NON-PEER-REVIEWED) |
| Async MultiGet on remote flash | 2.55× lower µs/op; +6–15% CPU | [RDB-BLOG asynchronous-io] (NON-PEER-REVIEWED) |
| Integrated BlobDB overwrite write amplification | 1.4–1.7 vs 6.1–6.8 leveled | [RDB-BLOG integrated-blob-db] (NON-PEER-REVIEWED) |
| Auto-tuned rate limiter, 10 MB/s ingest, 1000 MB/s cap | settles ~125 MB/s | [RDB-BLOG auto-tuned-rate-limiter] (NON-PEER-REVIEWED) |
| MyRocks TRIM-safe deletion | ~64 MB chunks, ~128 MB/s | [MYROCKS17] (NON-PEER-REVIEWED) |
| Rocksandra vs Cassandra 3.0 reads at 2 ms p99 | 300K vs 30K/s | [ROCKSANDRA] (NON-PEER-REVIEWED) |
| MySQL Raft failover | ~2 s vs 20–40 s semisync | [MYRAFT] (NON-PEER-REVIEWED) |
| CAO20 key / value means (UDB, ZippyDB, UP2X) | 27.1/126.7, 47.9/42.9, 10.45/46.8 B | [CAO20 Table 2, p. 215] |
| YCSB vs key-range model, extra read bytes over replay | ≥500% vs 40% (43% in §1) | [CAO20 §7.3, p. 219; §1, p. 210] |
