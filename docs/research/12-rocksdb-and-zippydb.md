# 12 — RocksDB and ZippyDB: the storage engine under a metadata range

Research note for mantle's metadata *ranges*. A range is one Raft group (focal's `focal-raft`
core) over a contiguous span of one metadata layer (Name, File or Block). Its state has to live
in an on-disk engine, because focal's durable shell caps a snapshot at 8 MiB and keeps the
retained log in RAM (note 07 §2.4, §2.10).

The note covers:

- the LSM-tree and RocksDB's design (§1);
- the analytical models that let an LSM engine's parameters be calculated rather than chosen (§2);
- what ZippyDB, the store under Tectonic's metadata, adds on top of RocksDB (§3);
- TiKV and CockroachDB, the other production multi-Raft systems built on LSMs (§4);
- the engine choice for mantle, evaluated the way note 04 Part B evaluated erasure-coding crates
  (§5);
- what all of this means for how a range stores its state, relates to its Raft log, snapshots,
  splits and moves, and which parameters come from which model (§6).

Compiled 2026-09-28. This is research input, not a decision record. §6 proposes decisions for
`docs/design/`. It builds on, and in two places revises, note 06 §C.b–§C.c (§6.1, §6.2).

---

## How to read this document

**Citation tags.**

- Papers: `[KEY §section, p. N]`, where `p.` is the printed proceedings page.
  - Author copies without printed page numbers are cited as "PDF p. N": DONG17, MONKEY,
    DOSTOEVSKY, PEBBLES.
  - ONEIL96 is read from the authors' preprint and cited by its own page numbers ("preprint
    p. N"); the journal pagination (351–385) could not be mapped onto it.
  - DONG21-TOS pages are "p. 26:N" (article 26 of the issue).
- Web pages, wiki pages, design documents and issues: `[KEY, "heading"]`.
- Source code: `[KEY path:line]`, with line numbers of the version named in the Sources table.
- Earlier notes: "note 01 §x" (Tectonic, ZippyDB), "note 02 §x" (OS storage APIs), "note 03
  §x" (I/O and persistence), "note 06 §x" (consensus and range metadata), "note 07 §x"
  (focal's consensus stack), "note 09 §x" (cells, Shard Manager, Akkio).

**Quotes.** Quotes are verbatim from each source's text layer or saved page text. Ligatures
are normalized, words hyphenated across line breaks are rejoined, bracketed reference numbers
are dropped, "..." marks an elision, and "[sic]" marks an error in the original.

**Evidence labels.**

- *(no label)*: stated in the cited **peer-reviewed** source and checked against its text.
- **NON-PEER-REVIEWED**: the RocksDB wiki, headers and source; the Meta engineering blog; TiKV
  and CockroachDB RFCs, tech notes, documentation, source and issues; crate source, READMEs and
  registry metadata. These are primary sources for what a system *does or says*, not
  peer-reviewed descriptions.
- **PREPRINT**: a paper whose only public version is an arXiv preprint (RIBBON).
- **DERIVED**: arithmetic or an interpretation made by this note from stated facts.
- **UNVERIFIED**: not found in any primary source consulted.
- **INFERENCE / Recommendation**: design reasoning for mantle, citing the facts it rests on.

**Method.**

1. PDFs were downloaded from usenix.org, vldb.org, cidrdb.org, arxiv.org and the authors'
   sites. Where the publisher or author site refused a non-browser client (MONKEY,
   DOSTOEVSKY, DONG21-TOS, SM), the Internet Archive's capture of the same URL was used.
   Each was converted with `pdftotext -layout` (plus a reading-order pass for two-column
   pages) and the relevant sections were read in full. Page numbers were checked page by page.
2. Web pages, wiki pages (fetched as raw markdown from the RocksDB wiki repository), RFCs and
   source files were fetched on 2026-09-28. Repository files are pinned: TiKV at commit
   `548812e1ef57`, CockroachDB at `d30c905fff79`.
3. Crates were downloaded from `static.crates.io`; RocksDB's C++ source is the copy bundled in
   `librocksdb-sys 0.19.0+11.8.1` (RocksDB 11.8.1, `include/rocksdb/version.h:14–16`).
4. Every quoted fragment of four or more words was checked by machine against the saved texts,
   with both sides reduced to lowercase letters and digits. Mismatches were corrected.
5. No secondary summaries (blogs about papers, lecture notes, third-party articles) were used
   as evidence.

---

## Sources

### Peer-reviewed papers

| Key | Full citation | Peer-reviewed | Where obtained |
|---|---|---|---|
| **ONEIL96** | Patrick O'Neil, Edward Cheng, Dieter Gawlick, Elizabeth O'Neil. "The Log-Structured Merge-Tree (LSM-Tree)." *Acta Informatica* 33(4):351–385, 1996. DOI 10.1007/s002360050048 (Crossref). | Yes (journal). Read from the authors' preprint marked "To be published: Acta Informatica", 32 pp.; wording may differ from the journal version. | https://www.cs.umb.edu/~poneil/lsmtree.pdf |
| **DONG21** | Siying Dong, Andrew Kryczka, Yanqin Jin, Michael Stumm. "Evolution of Development Priorities in Key-value Stores Serving Large-scale Applications: The RocksDB Experience." *19th USENIX Conference on File and Storage Technologies (FAST '21)*, pp. 33–49. | Yes | https://www.usenix.org/system/files/fast21-dong.pdf |
| **DONG21-TOS** | Siying Dong, Andrew Kryczka, Yanqin Jin, Michael Stumm. "RocksDB: Evolution of Development Priorities in a Key-value Store Serving Large-scale Applications." *ACM Transactions on Storage* 17(4), Article 26, October 2021, 32 pp. DOI 10.1145/3483840. CC-BY 4.0. The extended version of DONG21; its §9 lists what it adds: column families, compaction filters and merge operators, deletions, memory management, column support, and lessons from failed initiatives. | Yes | Internet Archive capture (2024-04-15) of https://dl.acm.org/doi/pdf/10.1145/3483840 |
| **DONG17** | Siying Dong, Mark Callaghan, Leonidas Galanis, Dhruba Borthakur, Tony Savor, Michael Stumm. "Optimizing Space Amplification in RocksDB." *8th Biennial Conference on Innovative Data Systems Research (CIDR '17)*, 9 pp., no printed page numbers. | Yes | https://www.cidrdb.org/cidr2017/papers/p82-dong-cidr17.pdf |
| **CAO20** | Zhichao Cao, Siying Dong, Sagar Vemuri, David H.C. Du. "Characterizing, Modeling, and Benchmarking RocksDB Key-Value Workloads at Facebook." *FAST '20*, pp. 209–223. Same key as note 01. | Yes | https://www.usenix.org/system/files/fast20-cao_zhichao.pdf |
| **MYROCKS** | Yoshinori Matsunobu, Siying Dong, Herman Lee. "MyRocks: LSM-Tree Database Storage Engine Serving Facebook's Social Graph." *PVLDB* 13(12):3217–3230, 2020. DOI 10.14778/3415478.3415546. | Yes | https://www.vldb.org/pvldb/vol13/p3217-matsunobu.pdf |
| **LIM16** | Hyeontaek Lim, David G. Andersen, Michael Kaminsky. "Towards Accurate and Fast Evaluation of Multi-Stage Log-structured Designs." *FAST '16*, pp. 149–166. | Yes | https://www.usenix.org/system/files/conference/fast16/fast16-papers-lim.pdf |
| **MONKEY** | Niv Dayan, Manos Athanassoulis, Stratos Idreos. "Monkey: Optimal Navigable Key-Value Store." *SIGMOD '17*, pp. 79–94. DOI 10.1145/3035918.3064054. | Yes | Author copy; Internet Archive capture of https://stratos.seas.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf (direct: HTTP 403) |
| **DOSTOEVSKY** | Niv Dayan, Stratos Idreos. "Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree Based Key-Value Stores via Adaptive Removal of Superfluous Merging." *SIGMOD '18*, pp. 505–520. DOI 10.1145/3183713.3196927. | Yes | Author copy; Internet Archive capture of https://stratos.seas.harvard.edu/files/stratos/files/dostoevskykv.pdf |
| **SILK** | Oana Balmau, Florin Dinu, Willy Zwaenepoel, Karan Gupta, Ravishankar Chandhiramoorthi, Diego Didona. "SILK: Preventing Latency Spikes in Log-Structured Merge Key-Value Stores." *USENIX ATC '19*, pp. 753–766. | Yes | https://www.usenix.org/system/files/atc19-balmau.pdf |
| **WISCKEY** | Lanyue Lu, Thanumalayan Sankaranarayana Pillai, Hariharan Gopalakrishnan, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. "WiscKey: Separating Keys from Values in SSD-conscious Storage." *FAST '16*, pp. 133–148. | Yes | https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf |
| **PEBBLES** | Pandian Raju, Rohan Kadekodi, Vijay Chidambaram, Ittai Abraham. "PebblesDB: Building Key-Value Stores using Fragmented Log-Structured Merge Trees." *SOSP '17*, pp. 497–514. DOI 10.1145/3132747.3132765. | Yes | Author copy: https://www.cs.utexas.edu/~vijay/papers/sosp17-pebblesdb.pdf |
| **RIBBON** | Peter C. Dillinger, Stefan Walzer. "Ribbon filter: practically smaller than Bloom and Xor." arXiv:2103.02515v2, 8 March 2021, 14 pp. The arXiv record lists no journal reference (checked 2026-09-28). | **No (PREPRINT)** | https://arxiv.org/abs/2103.02515 |
| **TEC** | Pan et al. "Facebook's Tectonic Filesystem: Efficiency from Exascale." *FAST '21*, pp. 217–231. Same key as notes 01 and 09. | Yes | https://www.usenix.org/system/files/fast21-pan.pdf |
| **AKKIO** | Annamalai et al. "Sharding the Shards: Managing Datastore Locality at Scale with Akkio." *OSDI '18*, pp. 445–460. Same key as notes 01 and 09. | Yes | https://www.usenix.org/system/files/osdi18-annamalai.pdf |
| **SM** | Lee et al. "Shard Manager: A Generic Shard Management Framework for Geo-distributed Applications." *SOSP '21*, pp. 553–569. DOI 10.1145/3477132.3483546. Same key as note 09. | Yes | Internet Archive capture (2025-04-17) of the ACM open-access PDF |
| **TIDB** | Dongxu Huang et al. "TiDB: A Raft-based HTAP Database." *PVLDB* 13(12):3072–3084, 2020. DOI 10.14778/3415478.3415535. | Yes | https://www.vldb.org/pvldb/vol13/p3072-huang.pdf |
| **CRDB** | Rebecca Taft et al. "CockroachDB: The Resilient Geo-Distributed SQL Database." *SIGMOD '20*, pp. 1493–1509. DOI 10.1145/3318464.3386134. | Yes | https://www.cockroachlabs.com/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf |

**Page mapping.** Printed page = PDF page + 31 (DONG21), + 207 (CAO20), + 3216 (MYROCKS),
+ 147 (LIM16), + 751 (SILK), + 131 (WISCKEY), + 443 (AKKIO), + 552 (SM), + 215 (TEC),
+ 3071 (TIDB), + 1492 (CRDB). Crossref page ranges were checked for the papers with a DOI.

### NON-PEER-REVIEWED primary sources

| Key | What it is | Where |
|---|---|---|
| **ZDB-BLOG** | Sarang Masti, "How we built a general purpose key value store for Facebook with ZippyDB", Engineering at Meta, 6 Aug 2021. Same key as note 01. Cited by section heading. | https://engineering.fb.com/2021/08/06/core-infra/zippydb/ |
| **RDB-WIKI** | RocksDB wiki pages, raw markdown fetched 2026-09-28: "Leveled-Compaction", "Universal-Compaction", "FIFO-compaction-style", "DeleteRange", "DeleteRange-Implementation", "User-defined-Timestamp", "Column-Families", "Checkpoints", "Creating-and-Ingesting-SST-files", "Rate-Limiter", "Direct-IO", "Full-File-Checksum-and-Checksum-Handoff", "Pipelined-Write", "Write-Ahead-Log-(WAL)", "WAL-Recovery-Modes", "Partitioned-Index-Filters", "RocksDB-Bloom-Filter", "BlobDB", "Write-Stalls", "Write-Buffer-Manager", "Background-Error-Handling", "Atomic-flush", "Choose-Level-Compaction-Files". | https://github.com/facebook/rocksdb/wiki |
| **RDB-SRC** | RocksDB 11.8.1 C++ source as bundled in `librocksdb-sys 0.19.0+11.8.1` (headers `include/rocksdb/{options.h, advanced_options.h, table.h, filter_policy.h, c.h}`, `env/io_posix.cc`, `port/port_posix.h`, `port/win/io_win.cc`, `db/error_handler.cc`, `db/builder.cc`, `db/version_set.cc`, `db/flush_job.cc`, `CMakeLists.txt`, `build_tools/build_detect_platform`, `README.md`). | https://static.crates.io/crates/librocksdb-sys/ |
| **RRDB** | `rocksdb` 0.25.0 (2026-08-16) and `librocksdb-sys` 0.19.0+11.8.1, the upstream Rust binding; repository rust-rocksdb/rust-rocksdb, CI workflow `rust.yml`. | https://github.com/rust-rocksdb/rust-rocksdb |
| **RRDB-1105** | rust-rocksdb issue #1105, "macOS/iOS builds miss HAVE_FULLFSYNC: WAL/Sync() fall back to fdatasync instead of fcntl(F_FULLFSYNC)", opened 2026-08-27, open; and PR #1106, "librocksdb-sys/build.rs: define HAVE_FULLFSYNC on Apple targets", opened 2026-08-27, open. | https://github.com/rust-rocksdb/rust-rocksdb/issues/1105 |
| **ZRDB** | `rust-rocksdb` 0.53.0 and `rust-librocksdb-sys` 0.48.1+11.8.1 (2026-09-05), a separately published fork maintained at zaidoon1/rust-rocksdb. | https://github.com/zaidoon1/rust-rocksdb |
| **FJALL**, **LSMT**, **REDB** | `fjall` 3.1.10, `lsm-tree` 3.1.10 (2026-08-30), `redb` 4.3.0 (2026-09-15), crate source. Issue states re-checked 2026-09-28 (fjall #311, lsm-tree #306 and #321). Evaluated in depth in note 06 §B4. | crates.io; GitHub |
| **RUST-STD** | Rust 1.98.0 (the toolchain mantle pins) `library/std/src/sys/fs/unix.rs` and `windows.rs`, fetched at tag `1.98.0`. | https://github.com/rust-lang/rust |
| **TIKV-RFC-0093** | TiKV RFC "Physical isolation between Region" (`text/0093-rocksdb-per-region.md`). | https://github.com/tikv/rfcs |
| **TIKV-RFC-0082** | TiKV RFC "Dynamic size region" (`text/0082-dynamic-size-region.md`). | same |
| **TIKV-RFC-0067** | TiKV RFC "Substitute rocksdb write stall" (`text/0067-substitute-rocksdb-write-stall.md`). | same |
| **TIKV-SRC** | TiKV `components/raftstore/src/store/snap.rs`, `store/snap/io.rs` and `store/worker/region.rs` at `548812e1ef57`. | https://github.com/tikv/tikv |
| **TIKV-DOCS** | PingCAP documentation (stable channel, fetched 2026-09-28): "RocksDB Overview", "Partitioned Raft KV", "TiKV Configuration File". | https://docs.pingcap.com/tidb/stable/ |
| **CRDB-TN-SNAP** | CockroachDB tech note "Raft snapshots and why you see them when you oughtn't" (`docs/tech-notes/raft-snapshots.md`). | https://github.com/cockroachdb/cockroach |
| **CRDB-RFC-RAFT** | CockroachDB RFC "Dedicated storage engine for Raft" (`docs/RFCS/20170605_dedicated_raft_storage.md`), status "postponed". | same |
| **CRDB-SEP** | CockroachDB issues #16624 "kvserver: separate raft log" (opened 2017-06-20, open), #97610 (opened 2023-02-24, open) and #173897 (opened 2026-08-26, open). | same |
| **CRDB-SRC** | CockroachDB `pkg/kv/kvserver/replica_raftstorage.go` and `snapshot_apply_prepare.go` at `d30c905fff79`. | same |
| **PEBBLE** | Pebble `README.md`. | https://github.com/cockroachdb/pebble |

---

## 0. Decision-relevant summary

1. **Meta runs one RocksDB instance per shard, tens to hundreds of them per host.** "In our
   context, a separate RocksDB instance is used to service each shard" [DONG21 §4, p. 38], and
   CAO20 says the same of ZippyDB: "each shard is supported by one RocksDB instance" [CAO20
   §2.2, p. 211]. The RocksDB team's lessons from that deployment are to manage memory, compaction
   bandwidth, threads, disk and file-deletion rate across instances, to keep the on-disk format
   compatible across versions, and to support replicas built by copying files (§1.10, §1.9).
2. **A consensus log makes the engine's WAL unnecessary.** "distributed systems often have
   their own replication logs (e.g., Paxos logs), in which case RocksDB WAL are not needed at
   all" [DONG21 §4, p. 38]. TiKV's per-region design disables the engine WAL
   [TIKV-RFC-0093]; CockroachDB measured the cost of *not* separating them in 2017 and has
   been separating its Raft log since (§4.2). For mantle: the Raft log is the only WAL, each
   apply is one engine write batch that carries the applied index, and log truncation waits for
   the *persisted* applied index, which RocksDB can read directly (`kPersistedTier`, §6.2).
3. **Snapshots and moves should be files, not keys.** RocksDB supports copying a replica by
   hard-linked checkpoint and by ingesting SST files [DONG21 §4, p. 40; MYROCKS §3.3.1,
   p. 3224]. Ingestion into a *shared* engine blocks writes to the whole database
   [RDB-WIKI "Creating-and-Ingesting-SST-files"], which is why TiKV patched its RocksDB fork
   and CockroachDB's Pebble added "excise" (§4). With one engine per range, a snapshot is a
   checkpoint's file list, small enough for focal's 8 MiB message (§6.3).
4. **The upstream Rust binding does not make RocksDB durable on macOS.**
   `librocksdb-sys 0.19.0` never defines `HAVE_FULLFSYNC`, so RocksDB's `Sync()` compiles to
   `fdatasync`, which RocksDB maps to `fsync` on macOS; neither flushes the drive cache (note
   02 §3.8). Confirmed from the source [RDB-SRC `env/io_posix.cc:1826–1851`,
   `port/port_posix.h:74–78`; RRDB `build.rs:239–242`] and by an open upstream issue and PR
   [RRDB-1105]. The fork ZRDB defines it. This violates CLAUDE.md rule 6 unless fixed.
5. **RocksDB fences itself on write errors by default, but also retries.** With
   `paranoid_checks = true` (the default) a flush I/O error is fatal and the database turns
   read-only [RDB-SRC `db/error_handler.cc:139–205`], which matches the chunk store's rule, "A
   failed write or flush fences the volume" (chunk-store.md §4). Automatic resume after
   *retryable* errors is on by default (`max_bgerror_resume_count = INT_MAX`) and must be
   turned off. The upstream Rust binding exposes neither that setting nor the background-error
   listener; ZRDB exposes both.
6. **Corruption is measured, and RocksDB's defenses are partly off by default.** At Meta,
   RocksDB-level corruption appeared "roughly once every three months for each 100PB of data",
   and "in 40% of those cases, the corruption had already propagated to other replicas"
   [DONG21 §5, p. 40]. Block checksums are verified on every read (XXH3 by default), but
   whole-file checksums and per-key-value protection are off by default [RDB-SRC]. Mantle
   should enable CRC-32C file checksums and verify them on every transfer (§6.7).
7. **Meta's metadata workload says: leveled compaction, no key-value separation, no
   fragmented LSM.** The traced ZippyDB shard holding object-store metadata served 78% Gets
   and 3% range scans, with strong key-space locality, 48–53 B or 90–91 B keys and values
   mostly under 34 B [CAO20 §4–§6, pp. 212–217]. WiscKey-style separation loses 12× on range
   queries of 64 B pairs [WISCKEY §4.1.2, p. 142]; PebblesDB costs 30% on small range queries
   of a compacted store [PEBBLES §1, PDF p. 2].
8. **Every engine parameter has a model.** Level size ratio and level sizes (O'Neil's
   theorem, Lim's redundancy model, the space-amplification bound 1 + 1/(T−1)), filter memory
   per level (Monkey: at T = 10 and four levels, 36% of the uniform design's wasted I/O, or
   2.1 fewer bits per key; DERIVED; neither Rust binding can vary bits by level, §2.4), merge
   policy (Dostoevsky), flush and compaction bandwidth
   (SILK), and the benchmark itself (Cao: key-range locality, not YCSB). §6.6 maps each to the
   measurements mantle already takes or must add.
9. **TiKV has moved away from one shared RocksDB, slowly.** One kvdb plus one raftdb, then Raft
   Engine as the default log, then "Partitioned Raft KV", one RocksDB per region, which is
   still "an experimental feature" [TIKV-DOCS "Partitioned Raft KV"]. Its RFC gives the
   reasons: SST-count lock contention, compactions that mix regions, and snapshot and replica
   removal reshaping the tree [TIKV-RFC-0093].
10. **Engine choice.** RocksDB is still the only candidate with production evidence and the
    file-level operations mantle needs (checkpoint, ingestion, DeleteRange, file checksums,
    rate limiting, shared memory budgets). The binding is the open question: upstream
    `rocksdb` has more users but misses the macOS flush, the error listener and file-checksum
    control; ZRDB has them but is essentially one maintainer. fjall and redb flush correctly
    on all three OSes through `std` but lack checkpoint and ingestion and carry the open
    issues note 06 §B4 lists. A mantle-built LSM is bounded but large (§5.6).
11. **The gates change if RocksDB comes in.** Every CI runner needs a C++20 compiler and
    libclang, and `scripts/check-targets.sh`, which lints all six targets from one machine,
    would have to compile RocksDB's C++ for Windows on a non-Windows host, which rust-rocksdb
    does not support today (its Linux-to-Windows cross-compilation issue, #1016, is open).
    §5.2 lists the consequences (INFERENCE from the scripts).

---

## 1. The LSM-tree and RocksDB's design

### 1.1 O'Neil's LSM-tree

- **What it is.** The LSM-tree "defers and batches index changes, cascading the changes from a
  memory-based component through one or more disk components in an efficient manner
  reminiscent of merge sort" [ONEIL96 abstract, preprint p. 1]. Component C0 is in memory;
  C1…CK are on disk; a "rolling merge" moves entries from each component into the next
  [§2–§3.3, preprint pp. 4–16].
- **Recovery comes from a log the LSM does not own.** In O'Neil's design the history rows are
  recovered from the transaction system's log, "treated as logical logs", and the lost C0
  entries are recreated from them [§2, preprint p. 4]. This is the relationship mantle wants
  between a Raft log and the state engine (§6.2).
- **Trade-off stated at the origin.** "indexed finds requiring immediate response will lose I/O
  efficiency in some cases" [abstract, preprint p. 1]. Inserts are cheap, reads pay.
- **Theorem 3.1.** For K+1 components with fixed largest size S_K, insert rate R and memory
  component size S_0, "the total page I/O rate H to perform all merges is minimized when the
  ratios ri = Si/S i-1 are all equal to a common value r" [§3.4, preprint p. 17], and then
  H = (2R/S_p)·(K·(1+r) − 1/2), with S_p the page size.
- **Memory versus disk.** With cost terms for memory, disk space and disk arms, the two-component
  minimum is "twice the geometric mean" of the cost of holding all data in memory and the cost
  of the multi-page I/O the inserts need [§3.4, preprint p. 19].
- **How many components.** Adding components shrinks S_0 until r reaches e, but each adds merge
  CPU, buffer memory and a probe per lookup, so "three components are probably the most that
  will be seen in practice" [§3.4, preprint p. 21]. RocksDB instead fixes the ratio and lets
  the number of levels grow (§1.5; DONG17 §3.1 footnote 4, PDF p. 4).

### 1.2 RocksDB's structure

- **Writes** go to "an in-memory write buffer called MemTable, as well as an on-disk Write
  Ahead Log (WAL)". The memtable is a skiplist. "The WAL is used for recovery after a failure,
  but is not mandatory" [DONG21 §2.2, p. 35]. A full memtable becomes immutable, a new one and
  a new WAL are allocated, and the immutable memtable is flushed to an SSTable, after which it
  and its WAL are discarded [ibid.].
- **SSTables** are sorted, divided into blocks, and carry an index block; once written they are
  immutable [DONG21-TOS §2.2, p. 26:5].
- **Levels.** Level-0 files come straight from flushes and overlap; each deeper level is one
  sorted run partitioned across files. Compaction merges files of level L with the overlapping
  files of L+1 [DONG21 §2.2, p. 35].
- **Reads** search the memtables, every L0 file, then one file per deeper level. "Hot SSTable
  blocks are cached in a memory-based block cache" and Bloom filters skip most files
  [DONG21-TOS §2.2, p. 26:5]. "Scans require that all levels be searched" [DONG21 §2.2,
  p. 35].
- **Defaults in RocksDB 11.8.1** [RDB-SRC `options.h`, `advanced_options.h`, `table.h`;
  NON-PEER-REVIEWED]:

| Option | Default | Where |
|---|---|---|
| `write_buffer_size` / `max_write_buffer_number` | 64 MiB / 2 | `options.h:191`; `advanced_options.h:271` |
| `level0_file_num_compaction_trigger` / slowdown / stop | 4 / 20 / 36 files | `options.h:255`; `advanced_options.h:547, 554` |
| `max_bytes_for_level_base` / multiplier | 256 MiB / 10 | `options.h:303`; `advanced_options.h:671` |
| `level_compaction_dynamic_level_bytes` | true | `advanced_options.h:666` |
| `compaction_pri` | `kMinOverlappingRatio` | `advanced_options.h:725` |
| `target_file_size_base` | 64 MiB | `advanced_options.h:568` |
| soft / hard pending-compaction limit | 64 GiB / 256 GiB | `advanced_options.h:709, 717` |
| `max_background_jobs` | 2 | `options.h:900` |
| block size / block checksum / `format_version` | 4 KiB / `kXXH3` / 7 | `table.h:400, 374, 739` |
| `WriteOptions::sync` / `disableWAL` | false / false | `options.h:2526, 2534` |
| `paranoid_checks` / `use_fsync` | true / false | `options.h:625, 841` |
| `wal_recovery_mode` | `kPointInTimeRecovery` | `options.h:1480` |
| `max_bgerror_resume_count` | `INT_MAX` | `options.h:1707` |
| `memtable_protection_bytes_per_key` / `WriteOptions::protection_bytes_per_key` | 0 / 0 (off) | `advanced_options.h:1273`; `options.h:2585` |
| `periodic_compaction_seconds` / `ttl` | `0xfffffffffffffffe` ("RocksDB controls") | `advanced_options.h:942, 894` |

None of these is a measured value for mantle's hardware or workload; §6.6 says what replaces
each.

### 1.3 The write path: WAL modes, group commit, pipelining

- **What `sync` means.** With `sync = false`, a write "has similar crash semantics" to
  `write()`; with `sync = true`, to `write()` followed by `fdatasync()` [RDB-SRC
  `options.h:2507–2526`]. On macOS the second half of that sentence is false for the upstream
  Rust build (§5.2).
- **Three WAL modes.** Meta "introduced differentiated WAL operating modes: (i) synchronous WAL
  writes, (ii) buffered WAL writes, and (iii) no WAL writes at all" [DONG21-TOS §4.3,
  p. 26:13]. The third is `WriteOptions::disableWAL`: "writes will not first go to the write
  ahead log, and the write may get lost after a crash" [RDB-SRC `options.h:2528–2534`].
- **Group commit is built in.** "By default, a single write thread queue is maintained. The
  thread gets to the head of the queue becomes write batch group leader and responsible for
  writing to WAL and memtable for the batch group" [RDB-SRC `options.h:1385–1388`]. This is the
  same leader-batched pattern as mantle's chunk-store writer and focal-log (note 07 §3.2).
- **Pipelined writes** keep separate queues for WAL and memtable writes. The wiki reports "20%
  write throughput improvement with concurrent writers and WAL enabled" on ramfs, and about 30%
  on tmpfs with 8 writers [RDB-WIKI "Pipelined-Write"]. Neither number is on persistent media.
- **Unordered writes** trade "higher write throughput with relaxing the immutability guarantee
  of snapshots" [RDB-SRC `options.h:1400–1424`]. A Raft state machine needs consistent
  snapshots for checkpoints and reads at an applied index, so this mode is excluded (INFERENCE).
- **Column families share one WAL** [RDB-WIKI "Write-Ahead-Log-(WAL)"]. With the WAL disabled,
  consistency across column families needs `atomic_flush`: "This option is useful when there
  are column families with writes NOT protected by WAL" [RDB-SRC `options.h:1580–1593`].
- **WAL recovery modes.** `kTolerateCorruptedTailRecords` is described as "a heuristic mode,
  the system cannot differentiate between corruption at the tail of the log and incomplete
  write"; `kPointInTimeRecovery` (the default since 6.6) stops at the first error and "is
  ideal for systems with replicas" [RDB-WIKI "WAL-Recovery-Modes"]. With the WAL disabled
  (§6.2) none of these apply to mantle; the Raft log's own torn-tail rule does (note 07 §3.4).

### 1.4 The read path: block cache, filters, partitioned indexes

- **Filters.** A Bloom filter "(typically) requires 10 bits per key" [DONG17 §4, PDF p. 5];
  the wiki gives the curve: 9.9 bits per key for a 1% false-positive rate, 15.5 bits for 0.1%
  [RDB-WIKI "RocksDB-Bloom-Filter"].
- **No filter at the last level.** At Facebook, "we do not use a Bloom filter at the last
  level" because it is "∼9X as large as all lower-level Bloom filters combined" [DONG17 §4,
  PDF p. 5]. MyRocks measured the effect: "the total bloom filter size was reduced by 90%,
  while the bloom filter is still effective", at the price that "empty key lookups, such as
  unique key check by INSERTs, become more expensive" [MYROCKS §3.2.3.1, p. 3223]. Mantle's
  name creation is exactly such a lookup (`key_not_present`, note 01 §5.3 Z1), so this setting
  needs Monkey's arithmetic (§2.4), not a copy.
- **Prefix Bloom filters** are built over a key prefix, so a scan of one prefix can skip files
  that hold none of it. They cut read amplification of such range queries "by up to 64%" in
  Facebook's systems [DONG17 §4, PDF p. 5]. Mantle's directory listings are prefix scans (note
  01 §1.5). MyRocks' validation found "bugs in the prefix bloom filter where some range scans
  with equal predicates returned fewer rows than expected" [MYROCKS §4.3, p. 3225]: a filter bug
  is a silent correctness bug, which is an argument for differential testing against a model
  engine (note 06 §C.b.3).
- **Ribbon filters** (PREPRINT). Bloom filters "use at least 44% more space" than the
  information-theoretic bound; Ribbon "can achieve space overhead below 10% with some additional
  CPU time" [RIBBON abstract and §1, p. 1]. The authors report that in large RocksDB
  deployments "we observe roughly 10% of memory and roughly 1% of CPU used in blocked Bloom
  filters" [RIBBON §1 footnote 5, p. 1]. The wiki's summary of the RocksDB implementation:
  "saving about 30% of Bloom filter space (most importantly, memory) but using about 3-4x as
  much CPU on filters", and a Ribbon policy at 9.9 "has the same 1% FP rate as Bloom but only
  uses around 7 bits per key" [RDB-WIKI "RocksDB-Bloom-Filter"; NON-PEER-REVIEWED].
- **Partitioned index and filters** split a file's index and filter into cache-sized pieces
  under a top-level index, so a cache miss loads one partition instead of the whole block
  [RDB-WIKI "Partitioned-Index-Filters"]. The wiki's example: a 256 MB SST carries index and
  filter blocks of about 0.5 MB and 5 MB [ibid.].
- **Reading only what is persisted.** `ReadOptions::read_tier = kPersistedTier` returns
  persisted data; "When WAL is disabled, this option will skip data in memtable" (Get and
  MultiGet only) [RDB-SRC `options.h:1934–1942`]. §6.2 uses this to read the flushed applied
  index.

### 1.5 Compaction styles and their amplification

**Measured trade-offs (RocksDB 5.9)** [DONG21 Table 3, p. 35; conditions from DONG21-TOS
Table 3, p. 26:6]: 16-byte keys, 100-byte values (about 50 bytes compressed), 500 million keys,
random overwrites at 2 MB/s, direct I/O, block cache 10% of the compacted size. Write
amplification counts SSTable writes against memtable bytes flushed, "WAL writes are not
included".

| Style | Write amplification | Max space overhead | Avg space overhead | I/O per Get, with filter | I/O per Get, no filter | I/O per iterator seek |
|---|---|---|---|---|---|---|
| Leveled | 16.07 | 9.8% | 9.5% | 0.99 | 1.7 | 1.84 |
| Tiered (universal, 12 runs) | 4.8 | 94.4% | 45.5% | 1.03 | 3.39 | 4.80 |
| FIFO (20 filter bits/key) | 2.14 | N/A | N/A | 1.16 | 528 | 967 |

- **Leveled** is RocksDB's default. It "usually exhibits write amplification between 10 and
  30"; tiered compaction "brings write amplification down to the 4–10 range" with lower read
  performance [DONG21 §3, p. 36]. The SSD adds its own: "by our observations between 1.1 and
  3" [ibid.].
- **Dynamic level sizing.** Levels are sized from the actual size of the last level, which
  holds "90% of data" [RDB-WIKI "Leveled-Compaction"; default since 8.4]. It "limits space
  overhead to 13%" where static leveling can add "more than 25%", and "can be as high as 90%"
  in the worst case [DONG21 §3, p. 36]. The mechanism: "capping the ratio between the sizes of
  the newer levels and the oldest level tends to limit space overhead" [DONG21-TOS §3.2,
  p. 26:8]. §2.2 turns this into a formula.
- **Universal (tiered).** A full compaction "will be temporarily double the disk space usage"
  [RDB-WIKI "Universal-Compaction"]. Its space-amplification trigger
  (`max_size_amplification_percent`) bounds steady-state space, not the transient doubling.
- **FIFO** "simply discards old files once the DB hits a size limit" and "targets in-memory
  caching applications" [DONG21 §2.2, p. 35]. It drops data by design, so it cannot hold
  metadata.
- **File picking.** LIM16 found that RocksDB 4.0's rule, which "picks the largest SSTable in a
  level for compaction", raised write amplification, and that LevelDB-style selection lowered it
  "by up to 32.0%" [LIM16 §7, pp. 149, 159]. RocksDB's default is now `kMinOverlappingRatio`
  [RDB-SRC `advanced_options.h:725`], which picks by overlap. Whether that closes LIM16's gap is
  **UNVERIFIED**.
- **Time-bounded compaction.** `ttl` compacts files older than a threshold, and
  `periodic_compaction_seconds` sends every file through the compaction filter within a period
  [RDB-WIKI "Leveled-Compaction"]. MyRocks built periodic compaction because old deletes never
  reached the last level, so "the row images containing the data remained in Lmax"; the fix
  "triggered compactions until it reached Lmax" [MYROCKS §3.2.3.3, p. 3223]. RocksDB can also
  bound how long a tombstone takes to reach the last level [DONG21-TOS §6.3.2, p. 26:19].

### 1.6 Deletes, range deletions and tombstones

- **Tombstones persist.** "a tombstone for a given key cannot be removed from an SSTable during
  compaction unless it is certain that the key is not present in any SSTable at one of the
  older levels" [DONG21-TOS §6.3, p. 26:18]. Meta has "observed queries having to iterate over
  millions of tombstones, just to return a few KV-pairs", and their example is file-system
  paths: deleting a directory leaves a run of tombstones [ibid. §6.3.1, p. 26:19].
- **Mitigations Meta built** [DONG21-TOS §6.3.1, p. 26:19; MYROCKS §3.2.2, pp. 3222–3223]:
  - compaction likelihood rises when tombstones pass 50% of a file's entries;
  - Deletion Triggered Compaction, which compacts again when flush or compaction finds dense
    tombstones;
  - `SingleDelete`, which "can immediately be dropped when removing a matched Put" but "does not
    work when multiple Puts occur to the same key" [MYROCKS §3.2.2.2, pp. 3222–3223];
  - a per-scan tombstone budget that returns an incomplete result.
- **Range deletion.** `DeleteRange` writes one range tombstone instead of a scan of point
  deletes [RDB-WIKI "DeleteRange"]. In the memtable, "Range tombstones are not fragmented in
  the memtable directly, but are instead fragmented each time a read occurs" [RDB-WIKI
  "DeleteRange-Implementation"], so many range tombstones in a memtable cost every read.
  Meta still calls efficient, frequent range deletion "perhaps one of the more challenging
  issues RocksDB faces today" [DONG21-TOS §6.3.1, p. 26:19].
- **Whole-file deletion.** `DeleteFilesInRange` drops SSTables entirely inside a range. TiKV
  uses it, then `DeleteRange`, then point deletes, depending on the case [TIKV-SRC
  `worker/region.rs` `DeleteStrategy::{DeleteFiles, DeleteByRange, DeleteByKey,
  DeleteByWriter}`]. Pebble adds "Delete-only compactions that drop whole sstables that fall
  within the bounds of a range deletion" [PEBBLE].
- **Implication for mantle (INFERENCE).** Tectonic deletes lazily and garbage-collects between
  layers (note 01 §1.6), and S3 deletes of whole prefixes produce long runs of tombstones in the
  Name layer. The engine choice must make range removal cheap (§6.4–§6.5), and the scan path
  needs a tombstone budget with a typed "partial" result, as Meta added.

### 1.7 Column families

- Each column family "has its own set of MemTables and SSTables, but they share the WAL", so
  "the shared WAL enables atomic writes to different column families", and a column family "can
  be removed at the appropriate time without having to explicitly delete the KV pairs contained
  therein" [DONG21-TOS §2.2, p. 26:6].
- A flush of one column family starts a new WAL for all of them, and an old WAL is deleted
  only when every column family has flushed past it [RDB-WIKI "Column-Families"].
- Write stalls are per database: "if one column family triggers write stall, the whole DB will
  be stalled" [RDB-WIKI "Write-Stalls"].
- ZippyDB's traced metadata shard used one column family [CAO20 §4.1, p. 212].

### 1.8 User-defined timestamps

- RocksDB's 56-bit sequence numbers are per instance and cannot be chosen by the application,
  so versioned reads across shards are impossible without encoding timestamps into keys or
  values [DONG21 §6, p. 42]. User-defined timestamps store an application timestamp between the
  user key and the sequence number, and gave "a 1.2X or better throughput gain" over timestamps
  in keys in DB-Bench [ibid., pp. 42–43].
- The timestamp must be monotone with the sequence number for any key [RDB-WIKI
  "User-defined-Timestamp"], and `DB::OpenAndTrimHistory()` removes the newest data after
  recovery for applications whose recent writes "may not be reliably replicated" [ibid.].
- **For mantle (INFERENCE).** A range applies in Raft-log order, so the log index is a
  monotone timestamp; it would give reads as of a chosen log index. Nothing in the design
  requires it today (note 06 §A4.2 lists the closed-timestamp work it would support).

### 1.9 Checkpoints, SST ingestion, export and import: the basis for snapshots and moves

- **Physical copying is a supported way to build a replica.** "RocksDB assists physical
  copying by identifying existing database files at a current point in time, and preventing
  them from being deleted or mutated" [DONG21 §4, p. 40]. On the source side of a *logical*
  copy, "snapshots ensure a consistent view of the source data" [DONG21-TOS §4.2.1, p. 26:12].
- **Checkpoints** create a consistent copy in a new directory: "If the snapshot is on the same
  filesystem as the original database, the SST files will be hard-linked, otherwise SST files
  will be copied" [RDB-WIKI "Checkpoints"].
- **MyRocks clones replicas this way**, incrementally: "A RocksDB checkpoint creates hard links
  of the SST files", and to bound catch-up "we periodically re-create checkpoints during
  cloning, and continuously send newly hard linked SST files to the destination" [MYROCKS
  §3.3.1, p. 3224].
- **SST ingestion** loads externally built, sorted files. MyRocks' new files "are directly
  ingested into Lmax, automatically and atomically updating the RocksDB Manifest", and "Bulk
  loading requires that ingested key ranges never overlap with existing data" [MYROCKS
  §3.2.3.4, pp. 3223–3224].
- **What ingestion does to a live database** [RDB-WIKI "Creating-and-Ingesting-SST-files";
  NON-PEER-REVIEWED]:
  - it will "block (not skip) writes to the DB because we have to keep a consistent db state";
  - "If file key range overlap with memtable key range, flush memtable";
  - files built by another live database can be ingested since 10.6: "The only requirement is
    that the files being ingested don't already overlap with existing files". The option,
    `allow_db_generated_files`, is marked "EXPERIMENTAL, SUBJECT TO CHANGE" [RDB-SRC
    `options.h:2934–2976`].
- **Verification at ingestion is opt-in.** `verify_checksums_before_ingest` defaults to false,
  and `verify_file_checksum` (default true) only has an effect when the database has a file
  checksum generator [RDB-SRC `options.h:2887–2918`].
- **Column-family export and import** (`Checkpoint::ExportColumnFamily`,
  `CreateColumnFamilyWithImport`) move one column family's files between databases. They are
  in the C API [RDB-SRC `c.h:527–530, 594`], in ZRDB (`checkpoint.rs:183`, `db.rs:5435`), and
  not in upstream RRDB 0.25.0 (no matches in its source).
- **Raft's own view.** Ongaro's dissertation already observed that transferring an LSM state
  machine means sending immutable runs, which cannot change during the transfer (note 06 §A1.6).

### 1.10 Many instances on one host: shared budgets, stalls, TRIM

- **Shards bound the unit of copy.** "The size of shards is limited, because a shard is the
  unit for load balancing and replication, and because shards are copied between nodes
  atomically for this purpose. As a result, each server node will typically host tens or
  hundreds of shards" [DONG21 §4, p. 38].
- **What must be shared.** Memory for write buffers and block cache, compaction I/O
  bandwidth, compaction threads, total disk usage and file-deletion rate, "potentially needed on
  a per-I/O device basis" [DONG21 §4, p. 38]. For example "a compaction rate limiter that, say,
  limits the total compaction rate to 100 MB/s can be passed to multiple instances"
  [DONG21-TOS §4.1, p. 26:11], and "Threads doing similar type of work (e.g., background
  flushes) should be in a pool that is shared across all similar instances" [ibid.].
- **The shared pool has its own failure mode.** "In extreme cases, delaying background work
  for a long period of time can cause the LSM-tree to become bloated", or trip the limits that
  cause write stalls [DONG21-TOS §4.1, p. 26:12].
- **Mechanisms** [RDB-WIKI; NON-PEER-REVIEWED]:
  - the write buffer manager "helps users control the total memory used by memtables across
    multiple column families and/or DB instances", and can charge memtables to the block cache
    so one limit covers both ["Write-Buffer-Manager"];
  - the rate limiter covers flush and compaction only: "Currently, RocksDB does not enforce
    rate limit for anything other than flush and compaction, e.g. write to WAL"
    ["Rate-Limiter"];
  - stalls: memtable count, L0 file count and pending-compaction bytes slow or stop writes; a
    write with `no_slowdown` returns `Status::Incomplete()` instead of blocking
    ["Write-Stalls"].
- **TRIM.** File deletion after compaction can issue TRIM storms; "we introduced rate
  limiting for file deletion to prevent multiple files from being deleted simultaneously"
  [DONG21 §4, p. 39; MYROCKS §3.2.3.2, p. 3223].
- **TiKV replaced RocksDB's stalls.** "we need to turn off the write stall mechanism of
  RocksDB, and add a limiter in the very beginning of TiKV to throttle write flow more smoothly.
  We can tolerate long-time higher request duration, while latency spike is not what we want"
  [TIKV-RFC-0067 "Motivation"]; with flow control on, TiKV disables the engine's stall
  mechanism and answers `ServerIsBusy` [TIKV-DOCS "TiKV Configuration File"]. This is mantle's
  admission-control model (architecture.md §8): refuse with a typed `Busy` / 503 at the door,
  never block inside the engine.

### 1.11 Direct I/O

- `use_direct_reads` and `use_direct_io_for_flush_and_compaction` open SST files with `O_DIRECT`
  (Linux), `F_NOCACHE` (macOS) or `FILE_FLAG_NO_BUFFERING` (Windows); they "will only be
  applied to SST file I/O but not WAL I/O or MANIFEST I/O" [RDB-WIKI "Direct-IO"].
- DONG21's Table 3 ran with direct I/O [p. 35]. Whether direct I/O helps depends on the device
  and on the block cache taking over from the page cache [RDB-WIKI "Direct-IO"]; mantle decides
  it per device from identification plus measurement, as the chunk store does (CLAUDE.md
  rule 5; chunk-store.md §1).

### 1.12 Integrity and failure handling

- **Measured corruption at Meta** [DONG21 §5, p. 40]:
  - RocksDB-level corruption (including CPU and memory faults), measured by comparing MyRocks
    primary and secondary indexes: "roughly once every three months for each 100PB of data",
    and "in 40% of those cases, the corruption had already propagated to other replicas";
  - a storage-system bug during network failures produced "roughly 17 checksum mismatches for
    every petabyte of physical data transferred".
- **Checksum layers** [DONG21 §5, pp. 40–41; DONG21-TOS §5.2–§5.3, pp. 26:14–26:16]:
  - block checksums on every SSTable block and WAL fragment, verified on every read, unlike "the
    file checksum that is verified only when the file is moved";
  - whole-file checksums, "added in 2020", recorded in the MANIFEST and checked when files are
    copied, backed up or ingested;
  - handoff checksums from RocksDB to the file system (for remote storage);
  - per-key-value checksums in memtables and write batches.
- **Defaults** [RDB-SRC]: block checksum `kXXH3`; `ReadOptions::verify_checksums = true`
  (`options.h:2211`); file checksums off until `file_checksum_gen_factory` is set (the built-in
  generator is CRC-32C); per-key-value protection off. Without a file checksum, "If a wrong SST
  file is transferred to a RocksDB SST file directory, all block checksum will match, but it
  doesn't contain the data we want" [RDB-WIKI "Full-File-Checksum-and-Checksum-Handoff"].
- **Replica comparison is an open problem.** Meta lists "Can we develop an efficient way of
  comparing two replicas to ensure they contain the same data?" as future work [DONG21 §8,
  p. 43]. Shard Manager says a consensus store "needs to handle complex issues such as
  continuous data-consistency auditing to guard against bit rot" [SM §2.4, p. 558].
- **Error severity.** Early RocksDB treated "all non-EINTR filesystem errors the same", freezing
  all writes on any write-path error [DONG21-TOS §5, p. 26:14]; its "design choices revisited"
  include "It’s OK to panic when seeing any I/O error" [DONG21 App. C, p. 45]. Today:
  - with `paranoid_checks = true`, an I/O error during flush (WAL on or off) or a MANIFEST
    write maps to `kFatalError`, and a corruption to `kUnrecoverableError` [RDB-SRC
    `db/error_handler.cc:139–205`]; the database becomes read-only;
  - *retryable* I/O errors, and ENOSPC, are resumed automatically, including "Errors during WAL
    sync, recovery is done only if 2PC is not in use and manual_wal_flush is not true" [RDB-WIKI
    "Background-Error-Handling"]; `max_bgerror_resume_count` bounds the attempts, and "If this
    value is 0 or negative, DB::Resume() will not be called automatically" [RDB-SRC
    `options.h:1699–1707`].
- **For mantle (INFERENCE).** Rebello et al. showed a retried fsync can report success for
  pages already marked clean (note 06 §A10.1). Mantle sets `max_bgerror_resume_count = 0`,
  treats any background error as fencing that engine, and rebuilds from durable state or peers,
  the same rule as the chunk store (chunk-store.md §4).

### 1.13 Workloads at Meta that resemble mantle's metadata

- **The traced ZippyDB shard** stored "the metadata of ObjStorage", "an object storage system
  at Facebook" [CAO20 §2.2, p. 211]; whether ObjStorage is Tectonic is **UNVERIFIED** (note 01
  §5.1). Over 24 hours [CAO20 §4–§6, pp. 212–217]:
  - about 420 million queries: 78% Get, 13% Put, 6% Delete, 3% forward iterator;
    DERIVED average: 4,861 queries/s per shard;
  - about 1% of pairs got more than 100 Gets and took about half of all Gets, while "about 73%
    of the KV-pairs are Put only once";
  - keys 48–53 B or 90–91 B; values averaged 42.9 B and "More than 90% of the value sizes are
    smaller than 34 bytes, which is even smaller than the key sizes";
  - "the ZippyDB workload is read-intensive and has very good key-space locality".
- **Benchmarks mislead without locality.** Replaying the same shard, YCSB configured to match
  it needed "at least 7.7x" the block reads and got 0.17× the cache hits [CAO20 §7.1, p. 218];
  overall "YCSB causes at least 500% more read-bytes and delivers only 17% of the cache hits"
  [§1, p. 210]. Their fix models hotness per key range, with a range size close to "the
  average number of KV-pairs in an SST file" [§7.2, p. 218].
- **MyRocks' tuning lesson.** An LSM "was much more workload dependent and harder to tune
  correctly", and tombstone-heavy applications "affected performance more severely than
  InnoDB" [MYROCKS §6, p. 3227]. Its validation tools found "some RocksDB compaction bugs that
  did not handle Delete/SingleDelete correctly" [§4.3, p. 3225].
- **Space dominates.** Of 42 surveyed ZippyDB and MyRocks deployments, "Most of the workloads
  are space constrained", and some were CPU-heavy [DONG21 §3, p. 37]; 39 ZippyDB deployments
  used "over 25 distinct configurations" [DONG21 §4, p. 39]; jemalloc overheads across 40
  ZippyDB clusters "vary from between 2% and 25%" [DONG21-TOS §6.4, p. 26:21].

---

## 2. The LSM design space: models that calculate an engine's parameters

Each model below is described by the question it answers, its inputs, its outputs, its
assumptions, and what it means for a metadata range. §2.10 summarizes.

### 2.1 O'Neil: equal size ratios, and memory against disk arms [ONEIL96 §3.3–§3.4]

- **Question.** How big should each component be, and how much memory should C0 get?
- **Inputs.** Insert rate R (bytes/s), page size S_p, size of the largest component S_K, cost
  of memory per byte, of disk per byte and of disk-arm I/O per second.
- **Outputs.** Equal ratios r between adjacent components minimize merge I/O for fixed S_0 and
  S_K (Theorem 3.1). Merge page I/O is H = (2R/S_p)(K(1+r) − 1/2). The memory size then
  follows from balancing memory cost against the disk arms H needs.
- **Assumptions.** Steady inserts; entries live until the last component; deletes balance
  inserts at C_K; I/O striped evenly; no lookups in the cost function ("a full analysis should
  minimize total cost over the workload, including both updates and retrievals" [ONEIL96 §3.4,
  preprint p. 21]).
- **For mantle.** It justifies a single size ratio across levels as the starting point, and
  frames memtable size as a memory-versus-I/O trade. LIM16 (§2.3) shows equal ratios are not
  optimal once key skew is modeled.

### 2.2 Dong et al.: the space-amplification bound of dynamic leveling [DONG17 §3]

- **Question.** How much space does leveled compaction waste, and how does the size
  multiplier trade space against writes?
- **Stated facts.** With the last level full and each level 10× the previous one, "in the
  worst case, LSM-tree space amplification will be 1.111" [§3, PDF p. 3]. With static targets
  the last level can be barely larger than the one above, and "space amplification would be
  larger than 2"; sizing each level from the next removes that [§3.1, PDF p. 4]. "The larger
  the size multiplier is, the lower the space amplification and the read amplification, but the
  higher the write amplification" [ibid.].
- **DERIVED generalization.** With dynamic leveling and multiplier T, the levels above the last
  hold at most Σ_{j≥1} T^−j = 1/(T−1) of it, so worst-case space amplification ≈ 1 + 1/(T−1):
  1.25 at T = 5, 1.143 at T = 8, 1.111 at T = 10, 1.067 at T = 16. DOSTOEVSKY states the same
  bound as O(1/T) and reports that "RocksDB uses leveling and a size ratio of 10 to bound
  space-amplification to ≈ 10%" [DOSTOEVSKY §3, PDF p. 5].
- **Facebook's values.** Most installations use 10, "although there are a few instances that
  use 8" [DONG17 §3.1, PDF p. 4]. The same paper cites the RUM conjecture: "one can optimize for
  any two of space, read, and write amplification, but at the cost of the third" [DONG17 §4,
  PDF p. 4].
- **Assumptions.** Worst case: every entry above the last level overwrites a distinct last-level
  entry. Compression per level changes the ratio of bytes, which DONG17 leaves open [§3.1
  footnote 5, PDF p. 4].
- **For mantle.** The space budget of a node (a capacity-plan input) gives the largest
  acceptable space amplification, and so the smallest T; write amplification (§2.3) gives the
  largest.

### 2.3 Lim et al.: write amplification from the key distribution [LIM16 §3–§6]

- **Question.** What write amplification will a given LSM configuration really have, and what
  level sizes minimize it?
- **Primitives.** Unique(p) = N − Σ_k (1 − f_X(k))^p is the expected number of distinct keys in
  p requests; its inverse and Merge(u, v) = Unique(Unique⁻¹(u) + Unique⁻¹(v)) give the size of a
  merged table [§3.3–§3.4, pp. 152–153]. "Asymptotic analyses in prior studies ignore
  redundancy" [§3.1, p. 152].
- **Inputs.** Key popularity f_X over N unique keys (from a trace), item size, write-buffer
  size, L0 trigger, level sizes.
- **Outputs.** Per-level and total write amplification; with a nonlinear solver, optimized level
  sizes. The model matched LevelDB within 3.0% where "the conventional worst-case analysis gives
  1.8–3.5X higher estimates", and optimized sizes cut LevelDB's insert cost "by up to
  9.4–26.2%" [abstract and §4.5, pp. 149, 156]. An optimization over 100 million keys took
  2.63 s (uniform) and 79 s (Zipf 0.99) [§6.4, p. 159].
- **Findings.** "it is suboptimal to use fixed level sizes for different workloads" [§6.3,
  p. 158]; LevelDB's per-level write amplification is "only up to 4–6" against the 11–12 that
  worst-case analysis predicts for a growth factor of 10 [§9, p. 160].
- **Assumptions.** Independent, identically distributed keys with "no spatial locality";
  fixed-size items (relaxed by a weighted variant); no tombstones [§3.2, §8, §9, pp. 152, 160].
  The authors warn that "Assumptions such as independence and no spatial locality in requested
  keys may not hold" [§9, p. 160].
- **For mantle.** CAO20 shows metadata has strong key-space locality (§1.13), which breaks the
  independence assumption. LIM16's model is a fast first estimate; the value that ships is
  checked by replaying a trace (§2.10).

### 2.4 Monkey: how much filter memory each level gets [MONKEY §3–§4]

- **Question.** Given a memory budget for filters, what false-positive rate should each level
  have?
- **Key result.** Worst-case lookup cost "is proportional to the sum of the false positive
  rates of the Bloom filters across all levels" [abstract, PDF p. 1]. Setting each filter's
  false-positive rate "proportional to the number of entries in the run that it corresponds to"
  minimizes that sum [§1, PDF p. 2]; so "the optimal FPR at Level i is T times higher than the
  optimal FPR at Level i − 1" [§4.1, PDF p. 6], which "shaves a factor of O(L) from the
  worst-case lookup cost" [§1, PDF p. 2].
- **Inputs.** Entries N, entry size E, buffer size, size ratio T, merge policy, filter memory
  M, and the workload mix (zero-result lookups, lookups that find a key, range lookups,
  updates), plus the storage's read/write cost ratio [§4.2–§4.4].
- **Outputs.** Per-level false-positive rates (possibly no filter at the deepest levels),
  closed-form lookup and update costs, and an allocation of memory between buffer and filters.
- **Assumptions.** Fence pointers in memory give one I/O per probed run; Bloom filters use the
  optimal number of hash functions; zero-result lookups are the worst case, and "they are very
  common in practice" [§2, PDF p. 4]; fixed entry size unless the iterative variant is used.
- **Result.** Lookup latency fell "50% − 80%" on their LevelDB-based implementation [abstract,
  PDF p. 1]. Monkey notes that all implementations it knew "use 10 bits per entry for their
  Bloom filters by default" [§2, PDF p. 4].
- **DERIVED example (leveling).** Minimizing Σp_i subject to M·ln²2 = −Σ N_i ln p_i gives
  p_i ∝ N_i and R_Monkey ≈ e^(−(M/N)·ln²2) · T^(T/(T−1)) / (T−1), against
  R_uniform = L · e^(−(M/N)·ln²2) for the same bits per key everywhere. The ratio is
  T^(T/(T−1)) / ((T−1)·L):

| T | L = 3 | L = 4 | L = 5 | L = 6 |
|---|---|---|---|---|
| 8 | 0.51 (1.4 bits/key saved) | 0.39 (2.0) | 0.31 (2.5) | 0.26 (2.8) |
| 10 | 0.48 (1.5) | 0.36 (2.1) | 0.29 (2.6) | 0.24 (3.0) |
| 16 | 0.43 (1.8) | 0.32 (2.4) | 0.26 (2.8) | 0.21 (3.2) |

  Each cell is Monkey's wasted I/O per zero-result lookup as a fraction of the uniform design's
  at equal memory; in brackets, the bits per key Monkey saves at equal wasted I/O,
  ln(L·(T−1)/T^(T/(T−1))) / ln²2. At 10 bits per key the uniform design wastes
  L × 0.0082 I/O per zero-result lookup.
- **How RocksDB can express it.** The C API, which both Rust bindings wrap, builds four filter
  policies, each with one bits-per-key value for every level: two Bloom forms, Ribbon, and a
  hybrid that uses Bloom for flushes and for levels numbered below `bloom_before_level`, and
  Ribbon from there down [RDB-SRC `c.h:3066–3074`, `filter_policy.h:175–188`]. Ribbon keeps
  Bloom's false-positive rate for the given bits per key and "saves about 30% space compared
  to Bloom filters" [RDB-SRC `filter_policy.h:169–173`], so the hybrid saves memory in the deep
  levels without varying the false-positive rate by level. The one per-level control is
  `optimize_filters_for_hits`, which lets RocksDB "not store filters for the last level"
  [RDB-SRC `advanced_options.h:802–816`]: Monkey's allocation at its extreme, paid by every
  zero-result lookup that reaches the last level. Monkey's allocation proper needs a C++
  `FilterPolicy` that sets bits per key from the `level_at_creation` RocksDB passes to it
  [RDB-SRC `filter_policy.h:70–73`]. The C API has no constructor for a custom policy and the
  fork's C API extensions add none, so neither binding can install one (INFERENCE from the API
  surface) [RRDB `db_options.rs:648, 674, 696, 1949`; ZRDB `db_options.rs:584, 610, 632, 2943`,
  `rust-librocksdb-sys` `c-api-extensions/`].
- **For mantle.** Zero-result lookups are common in mantle's metadata: every
  create-if-absent (`key_not_present`) and every probe of a name that does not exist. That is
  the case where dropping the last-level filter hurts (§1.4) and where Monkey's allocation
  gains most; the table above prices what uniform bits per key, the only allocation the
  bindings offer, gives up.

### 2.5 Dostoevsky: which merge policy [DOSTOEVSKY §3–§4]

- **Question.** Where on the tiering-to-leveling continuum should each level sit?
- **Analysis.** Update cost comes equally from every level: "while merge operations at larger
  levels do exponentially more work, they are also exponentially less frequent" [§3, PDF p. 5].
  With Monkey's filters, "most point lookup I/Os target the largest level" [§1, PDF p. 2]; long
  range lookups and space amplification also come mostly from the largest level. Space
  amplification is O(1/T) with leveling and O(T) with tiering [§3, PDF p. 5].
- **Lazy leveling** "removes merge operations from all levels of LSM-tree but the largest"
  [abstract, PDF p. 1]: tiering above, leveling at the bottom. It keeps leveling's point-lookup,
  long-range and space bounds and lowers update cost to O((L+T)/B), but a short range lookup
  costs O(1 + (L−1)·T) I/Os [§4.1, PDF p. 8].
- **Fluid LSM-tree** bounds runs per level by K (upper levels) and Z (last level): "K = T − 1
  and Z = 1 give Lazy Leveling", K = Z = 1 is leveling, K = Z = T − 1 is tiering [§4.2,
  PDF p. 8]. Dostoevsky searches T, K and Z against the monitored proportions of updates,
  zero-result and non-zero-result lookups and range lookups, weighted by the storage's
  sequential-to-random and write-to-read costs, under a space-amplification constraint [§2
  Table 1, PDF p. 3; §4.3, PDF p. 10].
- **Conclusion.** "no single design dominates the others universally" [§4.1, PDF p. 8].
- **For mantle.** RocksDB offers leveled and universal, not a per-level K/Z. Directory listings
  are short range scans, where lazy leveling pays (L−1)·T extra probes. With CAO20's 78% Gets
  and small scans, the model favors leveling unless measured writes dominate (INFERENCE; to be
  run on mantle's trace).

### 2.6 SILK: compaction bandwidth and latency spikes [SILK §3–§5]

- **Question.** How should flush and compaction I/O be scheduled so client tail latency stays
  flat?
- **Finding.** "The main reason for high tail latency is the fact that writes get blocked by Cm
  filling up": L0 fills when L0→L1 compaction falls behind, or flushes starve behind
  concurrent compactions. "Simply limiting bandwidth for internal operations does not solve
  the problem ... and can in fact exacerbate it in the long run" [§4.7, p. 758].
- **Mechanism.** SILK measures client bandwidth C and "continuously adjusts the internal
  operation bandwidth to I = T −C−ε B/s", with T the device's total [§5.2.1, p. 758]. Flushes
  get a dedicated pool and a minimum bandwidth "sufficient to be able to flush the immutable
  memory component before the active one fills up"; L0→L1 compactions come second and can
  preempt deeper compactions [§5.2.2, p. 759].
- **Measured settings, with their reasons.** Monitoring every 10 ms; limit changes only above
  10 MB/s ("We empirically set this threshold"); the number of compaction threads "should
  instead depend on the total drive I/O bandwidth and the amount of I/O bandwidth required by
  individual compaction operations", for example four threads on a 200 MB/s drive
  [§5.2.1–§5.2.2, pp. 758–759].
- **Result.** "up to two orders of magnitude lower 99th percentile latencies than RocksDB and
  TRIAD" [abstract, p. 753]. And a testing rule: "it is essential to run performance tests for
  an extended amount of time, lest these issues go undetected" [§4.7, p. 758].
- **Assumptions.** Client load varies over time, leaving idle bandwidth to exploit; one
  device.
- **For mantle.** T is measured by `mantle disk probe`; C is measured at run time; the flush
  floor follows from the peak ingest rate. §6.6 applies it per node across all ranges.

### 2.7 WiscKey and BlobDB: key-value separation [WISCKEY §3–§4]

- **Idea.** "Compaction only needs to sort keys, while values can be managed separately"
  [§3.2, p. 137]: values go to a log, the LSM stores pointers. Loading was "2.5×–111× faster than
  LevelDB", random lookups "1.6×–14× faster" [abstract, p. 133].
- **Cost for small values.** On a range query over a randomly loaded store, "WiscKey performs
  12× worse than LevelDB for 64-B key-value pairs" [§4.1.2, p. 142].
- **Crash consistency rests on a file-system property**: "if a value X in the vLog is lost in a
  crash, all future values (inserted after X) are lost too", because appends recover as a
  prefix [§3.3.3, p. 139]. Mantle does not rely on that property; it checksums records
  (CLAUDE.md rule 6).
- **BlobDB** is RocksDB's integrated version: "BlobDB is essentially RocksDB for large-value
  use cases" [RDB-WIKI "BlobDB"], with a `min_blob_size` threshold and garbage collection folded
  into compaction.
- **For mantle.** Metadata values are tens of bytes (§1.13). Separation is not a fit.

### 2.8 PebblesDB: fragmented LSM [PEBBLES]

- **Idea.** Guards partition each level; "FLSM introduces the notion of guards to organize
  logs, and avoids rewriting data in the same level". It cut write amplification "2.4-3×
  compared to RocksDB, while increasing write throughput by 6.7×" [abstract, PDF p. 1].
- **Cost.** "On a fully compacted key-value store, PebblesDb incurs a 30% overhead for small
  range queries", and FLSM "is not the best fit for workloads which involve a lot of range
  queries after an initial burst of writes" [§1, PDF p. 2].
- **For mantle.** Listing is a short range query; the trade goes the wrong way. PebblesDB is
  also a research store, not a supported engine.

### 2.9 Ribbon: filter bits per key (PREPRINT) [RIBBON]

- **Inputs.** Target false-positive rate f and acceptable CPU for construction and query.
- **Output.** Space per key close to log₂(1/f): below 10% overhead where Bloom needs at least
  44% [abstract, §1, p. 1].
- **Assumptions.** Static sets (an SST's filter is built once and never changes), which is the
  LSM case the authors target; "an intended application is optimizing accesses to the largest
  levels of an LSM-tree" [§1, p. 1].
- **For mantle.** A per-level choice: Bloom for small, short-lived upper-level filters,
  Ribbon for the large last-level filter, as the preprint suggests. RocksDB exposes this as a
  hybrid policy (`NewRibbonFilterPolicy` with `bloom_before_level` [RDB-SRC
  `filter_policy.h:175–188, 209–210`]), which both bindings reach (§2.4). DERIVED scale: a 50–100 GB shard of 91-byte metadata rows holds 0.55–1.1
  billion keys; at 10 bits per key its filters take 0.69–1.37 GB, at 7 bits 0.48–0.96 GB.

### 2.10 Summary: model, inputs, outputs

| Model | Decides | Inputs mantle must supply | Assumption to check |
|---|---|---|---|
| O'Neil Thm 3.1 [ONEIL96] | equal level ratios; memtable versus I/O | insert rate; page size; memory and I/O budgets | no lookups in the cost; steady inserts |
| Space bound [DONG17] | smallest T for a space budget | capacity plan's space-amplification limit | worst case: upper levels are all overwrites |
| Unique/Merge [LIM16] | write amplification; level sizes | key-popularity distribution from a trace; item size; buffer size | i.i.d. keys, no spatial locality (false for metadata: CAO20) |
| Monkey [MONKEY] | filter bits per level; buffer versus filters | filter memory; zero-result lookup share; N; T | one I/O per probed run |
| Dostoevsky [DOSTOEVSKY] | merge policy (T, K, Z) | full op mix; device cost ratios; space bound | worst-case (uniform) costs |
| SILK [SILK] | flush/compaction bandwidth split; thread count | device bandwidth T; client bandwidth C; peak ingest | load varies; one device |
| Key-range model [CAO20] | how to benchmark | a trace of mantle's own metadata operations | ranges near SST size preserve locality |

---

## 3. What ZippyDB adds on top of RocksDB

Note 01 §5 holds the ZippyDB facts known before this note; note 09 §7.3 and §7.8 cover Shard
Manager and Akkio. This section collects what bears on the storage engine and the replication
around it.

### 3.1 Peer-reviewed facts

- **One Paxos group and one RocksDB instance per shard.** "KV-pairs are divided into shards,
  and each shard is supported by one RocksDB instance" [CAO20 §2.2, p. 211]; "each replica
  participates in a shard-specific Paxos group" [AKKIO §3, p. 449].
- **Primary-driven writes and reads.** "The primary shard processes all the writes to a
  certain shard"; strongly consistent reads go only to the primary [CAO20 §2.2, p. 211; TEC
  §3.3, p. 220].
- **Roles and replica count.** "Each ZippyDB shard has a primary serving as the Paxos leader
  and proposer, and multiple secondaries serving as acceptors and learners"; "most ZippyDB
  deployments use one primary plus only two secondaries per shard" [SM §2.5, p. 558].
- **Placement.** "ZippyDB’s Shard Manager assigns each shard replica to a specific ZippyDB
  server while obeying the specified policy rules", and is responsible for "load balancing, by
  migrating shards if necessary" [AKKIO §3, p. 450]. Load balancing uses "multiple metrics,
  including CPU, storage, and shard count" [SM §2.5, p. 558].
- **Configuration is per deployment.** Across 39 ZippyDB deployments, "over 25 distinct
  configurations" [DONG21 §4, p. 39].
- **Consensus as a library had one user.** Meta's Paxos library "eventually had only one use
  case, i.e., ZippyDB" [SM §2.4, p. 557].

### 3.2 From the Meta blog (all NON-PEER-REVIEWED) [ZDB-BLOG]

- **Construction.** ZippyDB combined "a reusable and flexible data replication library called
  Data Shuttle" with RocksDB, on top of Shard Manager and a ZooKeeper-based configuration service
  ["History of ZippyDB"].
- **Tiers.** Among a handful of tiers are "specialized tiers for distributed filesystem
  metadata" ["Architecture"].
- **Replication.** Each shard is replicated "using Data Shuttle, which uses either Paxos or
  async replication to replicate data". A subset of replicas, the "global scope", replicates
  synchronously with Multi-Paxos; the rest are followers, "similar to learners in Paxos
  terminology", receiving data asynchronously ["Architecture"].
- **Epochs and ordering.** "Each epoch has a unique leader, whose role is assigned using an
  external shard management service called ShardManager"; the leader holds a lease for the
  epoch, renewed by heartbeats. Within an epoch the leader assigns "a monotonically increasing
  sequence number" to each write; writes go to "a replicated durable log using Multi-Paxos",
  and "Once the writes have reached consensus, they are drained in-order across all replicas"
  ["Data model"].
- **Pipelining.** The post does not describe how Data Shuttle batches or pipelines log
  appends. **UNVERIFIED**; note 06 §A1.9 and §A9 cover the published Raft and Paxos techniques
  mantle uses instead.

### 3.3 Consistency levels (NON-PEER-REVIEWED) [ZDB-BLOG "Consistency"]

- **Writes.** "By default, a write involves persisting the data on a majority of replicas’
  Paxos logs and writing the data to RocksDB on the primary before acknowledging the write to
  the client". A fast-acknowledge mode acknowledges when a write is "enqueued on the primary for
  replication", with weaker guarantees.
- **Reads.**
  - "eventual": replicas lagging beyond a threshold do not serve, so these reads "are closer to
    bounded staleness consistency in literature";
  - read-your-writes: "the clients cache the latest sequence number returned by the server for
    writes" and read at or after it;
  - strong: served by the primary, which "relies on owning the lease to ensure that there is no
    other primary before serving reads", with a quorum check as the fallback.
- **For mantle (INFERENCE).** The engine side of each level: strong reads at the leaseholder
  read the engine directly; read-your-writes and bounded-staleness reads at a follower need
  the follower's applied index, which the state engine stores with every batch (§6.2). The
  fast-ack mode is excluded for metadata (note 01 §5.3 Z2).

### 3.4 Transactions and conditional writes (NON-PEER-REVIEWED) [ZDB-BLOG "Transactions and conditional writes"]

- Serializable per shard; optimistic concurrency control at the primary, which checks the read
  and write sets against recently admitted writes; "Transactions spanning epochs are rejected";
  a minimum tracked version bounds the snapshots it accepts.
- Conditional writes reuse the transaction path with preconditions `key_present`,
  `key_not_present`, `value_matches_or_key_not_present`.
- **Engine requirement (INFERENCE).** Evaluating a precondition needs a consistent read at the
  leader's applied index and an atomic batch for the effect. RocksDB gives both (snapshot and
  `WriteBatch`); a range's apply loop runs single-threaded per range, so no engine-level
  transaction is needed.

### 3.5 TTL (NON-PEER-REVIEWED) [ZDB-BLOG "Data model"]

- "We piggyback on RocksDB’s periodic compaction support to clean up all the expired keys
  efficiently while filtering out dead keys on the read side in between compaction runs."
- The engine mechanism is a compaction filter; Meta warns that "improperly using compaction
  filters can break some basic data consistency guarantees" [DONG21-TOS §6.2.1, p. 26:18].
- **For mantle (INFERENCE).** A compaction filter decides per replica at compaction time, which
  differs between replicas. Deletions that change replicated state (lazy delete after the grace
  period, chunk-store.md §8) must be Raft commands; a compaction filter may only drop data every
  replica already considers deleted.

### 3.6 µshards, placement and moves

- **µshards** (NON-PEER-REVIEWED): "A typical physical shard has a size of 50–100 GB, hosting
  several tens of thousands of μshards" [ZDB-BLOG "Data model"]. A µshard is mapped to a
  physical shard either statically ("compact mapping", changed on split) or by Akkio.
- **Akkio** moves µshards between replica-set collections by a logical copy with the source set
  read-only through ACLs (note 09 §7.8.4). Akkio puts datastore shards at "one to a few tens of
  gigabytes" and µshards at "a few hundred bytes to a few megabytes" [AKKIO §1, p. 446].
- **Shard Manager** moves whole shard replicas and migrates primaries gracefully (note 09
  §7.3.4): "ShardManager is responsible for monitoring servers for load imbalance, failures,
  and initiating shard movement between servers" [ZDB-BLOG "Data model"].
- **How a moved or new replica gets its data is not published.** RocksDB supports both logical
  and physical copies for exactly this purpose [DONG21 §4, p. 40], but no source reviewed says
  which ZippyDB uses. **UNVERIFIED.**

### 3.7 How Tectonic uses it

- Tectonic's Name, File and Block layers live in ZippyDB, "a linearizable, fault-tolerant,
  sharded key-value store"; "The key-value store nodes internally run RocksDB"; "Shards are
  replicated with Paxos"; there are no cross-shard transactions [TEC §3.3, p. 220] (note 01
  §1.5).
- "Shards are sized so that each metadata node can host several shards. This allows shards to
  be redistributed in parallel to new nodes in case a node fails, reducing recovery time", and
  "the key-value store will transparently move shards to control load on each node" [TEC §3.3,
  p. 220].
- Each metadata shard can serve at most 10 KQPS [TEC §6.3, p. 225] (note 01 §1.12). DERIVED
  comparison: the traced metadata shard in CAO20 averaged 4.9 K queries/s.

### 3.8 What Meta has not published

- Data Shuttle's batching, pipelining and log storage format.
- Whether ZippyDB writes RocksDB's WAL at all. DONG21 says Paxos logs make it unnecessary
  [p. 38] but does not say what ZippyDB configures.
- How a replica is bootstrapped (logical or physical copy) and how shard splits happen at the
  RocksDB level.
- The shard size of the filesystem-metadata tier.
- The "storage-compute disaggregation" and membership changes the post lists as in progress
  ["The future of ZippyDB"].

---

## 4. Other production multi-Raft engines on LSMs

### 4.1 TiKV

**Peer-reviewed facts** [TIDB; note 06 §A4.4]:

- "On each TiKV server, data and metadata are persisted to RocksDB" [TIDB §4.1, p. 3074].
  Regions were 96 MB by default in 2020 [§4.1, p. 3074].
- A split is a Raft command: "The log only includes a split command, instead of modifying
  actual data", and "The overhead of region split is low as only metadata change is needed"
  [§4.1.4, p. 3076]. After a partition, "the group of nodes with the most recent epoch wins"
  [ibid.].

**Design documents and source (all NON-PEER-REVIEWED):**

- **Two instances, then a separate log.** "All data in a TiKV node shares two RocksDB
  instances", one for the Raft log (raftdb) and one for data (kvdb) [TIKV-DOCS "RocksDB
  Overview"]. Raft Engine, a shared append-only log for all groups (note 06 §B5), is the default
  log store: `raft-engine.enable` "Determines whether to use Raft Engine to store Raft logs.
  When it is enabled, configurations of raftdb are ignored", default true [TIKV-DOCS "TiKV
  Configuration File"].
- **Region size grew.** `region-split-size` is 256 MiB since v8.4.0, 96 MiB before
  [TIKV-DOCS]. The dynamic-region RFC explains why: "We have observed a lot of regressions when
  the count of regions increases" (more RPCs per transaction, per-region resource cost, tools
  that loop over regions); it proposes 512 MiB for hot and 10 GiB for cold regions, with
  buckets of about 128 MiB for statistics and scan parallelism [TIKV-RFC-0082 "Motivation",
  "Detailed design"].
- **Snapshots by SST, with checksums.** The sender writes SST files per column family,
  re-reads them to verify block checksums ("use sst reader to verify block checksum, it would
  detect corrupted SST due to memory bit-flip"), syncs them and records a CRC32 and size per
  file; the receiver checks both and ingests [TIKV-SRC `snap/io.rs:132–200, 355–395`;
  `snap.rs:261–312`]. Snapshot I/O is capped by `snap-io-max-bytes-per-sec`, default 100 MiB
  [TIKV-DOCS].
- **Ingestion needed a fork.** TiKV ingests with RocksDB's `IngestExternalFileOptions.allow_write`
  set, so foreground writes to other regions continue; the source comment explains why no
  concurrent write can overlap the ingested range [TIKV-SRC `snap/io.rs:355–395`]. Upstream
  RocksDB 11.8.1's `IngestExternalFileOptions` has no such field [RDB-SRC
  `options.h:2834–2980`], so it comes from TiKV's RocksDB fork (note 06 §B4.1; INFERENCE).
  Upstream ingestion blocks the database's writes (§1.9).
- **Removing a replica** uses whole-file deletion where it can, then range deletion or point
  deletes [TIKV-SRC `worker/region.rs`].
- **One RocksDB per region, experimental.** RFC 0093 proposes "Make every region stores its own
  data in an isolated LSM tree, in our case RocksDB" [sic] [TIKV-RFC-0093 "Summary"]. Its
  reasons:
  - "Larger size means more unnecessary compaction. Data is written and read per region, so
    compaction across region is meaningless in our case";
  - "Apply snapshot and delete peer needs to change the shape of LSM tree, which easily brings a
    lot of compactions";
  - lock contention with many SST files ["Motivation"].
- **How the per-region design works** [TIKV-RFC-0093 "Detailed design"]:
  - "Tablets share env, blockcache, statics, rate limiters and compaction filters";
  - the KV WAL is disabled ("Every tablet writes its own WAL can bring random writes, so we
    better disable WAL"), and apply state moves into the Raft engine;
  - "To generate a snapshot, just use the checkpoint API from rocksdb"; because hard-linked
    files may not be in the page cache, "we need to do flow control with sending"; applying a
    snapshot is a rename;
  - a split clones the tablet by hard links plus a frozen memtable (a new RocksDB API the RFC
    names `FreezeAndClone`), then trims each side with `DeleteFilesInRange`, a compaction filter
    and manual compaction;
  - "The maximum size of a tablet is supposed to be 10GiB, its file count can usually be 20 ~
    60";
  - measured in a prototype: "dynamic regions can scale in a 1TiB node in 4 hours and scale out
    in 7 hours. In v5.x, these operations need more than 16 hours and 2 days respectively";
  - drawback: "This RFC may be easier to OOM compared to existing architecture".
- **Status.** "Partitioned Raft KV is an experimental feature. It is not recommended that you
  use it in the production environment" [TIKV-DOCS "Partitioned Raft KV"]; it arrived in v6.6.0
  and cannot be switched on or off after a cluster is created [ibid.].

### 4.2 CockroachDB

- **Peer-reviewed.** In 2020 CockroachDB used RocksDB, "which we treat as a black box
  throughout the paper"; a lagging replica catches up by "sending a snapshot of the full Range
  data" or the missing log entries, chosen by how many writes it missed [CRDB §2.1.5, §2.2.2,
  p. 1495]. Ranges were about 64 MiB (note 06 §A4.3).
- **Pebble** (NON-PEER-REVIEWED): "Pebble was made the default storage engine in CockroachDB
  v20.2 (released Nov 2020)". It keeps RocksDB's file formats and the subset CockroachDB uses,
  and warns that it "may silently corrupt data or behave incorrectly if used with a RocksDB
  database that uses a feature Pebble doesn't support" [PEBBLE].
- **Snapshots are ingested and excise the old range** (NON-PEER-REVIEWED): "By default, the
  snapshot is applied to Pebble as a single ingestion", or as a write batch when small; the
  ingestion calls `IngestAndExciseFiles` over the range's key span [CRDB-SRC
  `replica_raftstorage.go:539–544, 648`; `snapshot_apply_prepare.go:201–205`].
- **The Raft log shares the engine, and separating it has been open since 2017**
  (NON-PEER-REVIEWED):
  - the 2017 RFC found that, with one engine, each synced Raft-log write also flushed the
    previous, unsynced state-machine write, so the synced and unsynced workloads could not be
    kept apart; the RFC's status is "postponed" [CRDB-RFC-RAFT "Summary", "Motivation"];
  - #16624 "kvserver: separate raft log" is open; the code now has "separated engines" in which
    "the raft engine batch will contain a WAG node that guarantees durability of the state
    machine write" [CRDB-SRC `snapshot_apply_prepare.go:181–190`]; exposing it to operators
    (#97610) is open, and its test fallout was being fixed in August 2026 (#173897)
    [CRDB-SEP].
- **Raft snapshots come from interactions** (NON-PEER-REVIEWED) [CRDB-TN-SNAP]:
  - a fresh 15-node cluster importing TBs in 2018 requested about 15,000 Raft snapshots, each
    "~64mb of data over the network (closer to ~32mb in practice)";
  - causes: log truncation cutting off an in-flight snapshot (fix: "track the index of
    in-flight snapshots and don’t truncate them away"), replica GC removing a preemptive
    snapshot, and splits racing replica removal and snapshots;
  - "A split shouldn’t require a Raft snapshot", and "a zero rate of Raft snapshots is a good
    indicator of cluster health for production and stability testing".

### 4.3 Side by side

| | ZippyDB | TiKV (default) | TiKV Partitioned Raft KV | CockroachDB | focal today (note 07) |
|---|---|---|---|---|---|
| Engine instances | one RocksDB per shard [CAO20] | one kvdb per node [TIKV-DOCS] | one RocksDB per region [TIKV-RFC-0093] | one Pebble per store [PEBBLE; CRDB-SRC] | application-owned |
| Consensus log | Multi-Paxos log, format unpublished [ZDB-BLOG] | Raft Engine, shared [TIKV-DOCS] | Raft Engine, shared | same Pebble; separation in progress [CRDB-SEP] | shared focal-log WAL |
| Engine WAL | not published | on (per RFC 0093's description) | off [TIKV-RFC-0093] | n/a (one engine) | n/a |
| Shard/range size | 50–100 GB physical [ZDB-BLOG] | 256 MiB [TIKV-DOCS] | ≤10 GiB [TIKV-RFC-0093] | ~64 MiB [CRDB] | ≤8 MiB checkpoint |
| Split | not published | Raft command, metadata only [TIDB] | clone by hard links, trim [TIKV-RFC-0093] | split trigger; data stays [CRDB-TN-SNAP] | not supported |
| Snapshot | not published | SSTs built, CRC-checked, ingested [TIKV-SRC] | checkpoint, send files, rename | SSTs ingested with excise [CRDB-SRC] | one ≤8 MiB message |
| Status | production | production | experimental [TIKV-DOCS] | production | — |

---

## 5. Engine choice for mantle

**Method.** Versions, dates and licenses from the crates.io API (2026-09-28); source from the
published `.crate` tarballs; issue and repository state from the GitHub API. Panic counts are
the same heuristic scan as note 06 §B4.5 (skip `#[cfg(test)]` modules and `//` lines), so treat
them as approximate. Nothing was built or run.

### 5.1 Requirements, from mantle's rules and notes

1. Ordered keys, prefix and range scans, atomic batches that carry the applied index (note 06
   §C.b).
2. Consistent point-in-time images and file-level transfer for snapshots, splits and moves
   (§1.9; note 07 §2.10).
3. Cheap removal of a key range, and cheap removal of a whole replica.
4. Checksums verified on every read and on every transfer; typed corruption errors
   (CLAUDE.md rule 6).
5. The platform's full flush on every OS: `fdatasync` (Linux), `F_FULLFSYNC` (macOS),
   `FlushFileBuffers` (Windows), plus directory flushes (CLAUDE.md rule 6).
6. No panics across the boundary; failed writes and flushes fence, never retry (rule 1;
   chunk-store.md §4).
7. Every resource bounded across many instances: memory, threads, compaction bandwidth, disk,
   open files (rule 2).
8. Builds and passes the gates on all six targets (rule 7); licenses within `deny.toml`.
9. Testable in deterministic simulation, or replaceable there by a model engine (note 06
   §C.b.3).

### 5.2 RocksDB through upstream `rocksdb` 0.25.0 (RocksDB 11.8.1)

Note 06 §B4.1 already covers build requirements, cross-compilation issues, the options that
matter to Raft and the wrapper's panic sites. New findings:

- **macOS durability.** RocksDB flushes a file with `fcntl(F_FULLFSYNC)` only when compiled with
  `HAVE_FULLFSYNC`, and otherwise with `fdatasync` (`Sync`) or `fsync` (`Fsync`) [RDB-SRC
  `env/io_posix.cc:1826–1851`, directory flush at `:2146–2156`]; on macOS `fdatasync` is defined
  as `fsync` [RDB-SRC `port/port_posix.h:74–78`]. RocksDB's own CMake and Makefile builds detect
  `F_FULLFSYNC` and define it [RDB-SRC `CMakeLists.txt:606–608`;
  `build_tools/build_detect_platform:792–802`]. `librocksdb-sys`'s `build.rs` compiles RocksDB
  with `cc` and, for darwin, defines only `OS_MACOSX`, `ROCKSDB_PLATFORM_POSIX` and
  `ROCKSDB_LIB_IO_POSIX` [RRDB `librocksdb-sys/build.rs:239–242`]. So every sync RocksDB issues on
  macOS through this crate is a plain `fsync`, which does not flush the drive's cache (note 02
  §3.8). The upstream issue says the same: "On Apple targets, `librocksdb-sys/build.rs` never
  defines `HAVE_FULLFSYNC`", and "This is a bug in the wrapper's build script, not in RocksDB
  itself"; the fix PR notes it "makes `sync = true` writes noticeably slower on macOS, because
  the drive cache is now actually flushed" [RRDB-1105; both open on 2026-09-28].
- **Windows durability.** File `Sync` and `Fsync` call `FlushFileBuffers` [RDB-SRC
  `port/win/io_win.cc:897–907, 985–991`]; directory `Fsync` returns OK without doing anything
  [`:1085–1088`], the same as focal-log and fjall on Windows (note 07 §3.3). Whether NTFS needs a
  directory flush for a create or rename to be durable is open (note 02 §4.8).
- **Linux durability.** `fdatasync` for data, `fsync` when `use_fsync` is set, `fsync` on
  directories [RDB-SRC `env/io_posix.cc`]; a flush syncs the new SST, the MANIFEST and the
  directory [RDB-SRC `db/builder.cc:454`; `db/version_set.cc:6034`; `db/flush_job.cc:1120`].
- **API gaps in the safe wrapper** (0 matches in `rocksdb-0.25.0/src` for each): the
  background-error listener, `max_bgerror_resume_count`, the file-checksum generator,
  `VerifyFileChecksums`, column-family export and import. All exist in RocksDB's C API [RDB-SRC
  `c.h:2101–2116, 5163, 2245–2250, 477, 527–530, 594`], so reaching them means calling
  `librocksdb-sys` directly. That is `unsafe` FFI to a C++ library, which CLAUDE.md rule 7 and
  `scripts/check-contracts.py` allow only in listed files that bind an *OS* interface: an owner
  decision.
- **Targets.** Upstream CI tests on `ubuntu-latest`, `macos-latest` and `windows-latest`, with
  LLVM installed through Chocolatey on Windows [RRDB `rust.yml`]: at most three of mantle's six
  targets. Linux-to-Windows cross-compilation is an open issue (#1016, opened 2025-07-18)
  [RRDB].
- **Gates (INFERENCE from the scripts).** `scripts/check-targets.sh` runs `cargo clippy --target`
  for all six targets from one machine. Clippy runs build scripts, and `librocksdb-sys`'s build
  script compiles 343 C++ files for the *target* and runs bindgen against the target's headers
  (note 06 §B4.1), so linting the Windows targets from macOS needs an MSVC-compatible C++
  toolchain and headers there. The native CI jobs are unaffected apart from installing a C++20
  compiler and libclang on each runner.
- **Licenses.** Crate license fields: `rocksdb` Apache-2.0; `librocksdb-sys`
  MIT/Apache-2.0/BSD-3-Clause; build dependencies `bindgen` BSD-3-Clause, `cc` MIT OR
  Apache-2.0, `clang-sys` Apache-2.0; default compression crates `zstd-sys` BSD-3-Clause,
  `lz4-sys` MIT, `libz-sys` MIT OR Apache-2.0, `bzip2-sys` MIT/Apache-2.0. All are in
  `deny.toml`'s allow list. The bundled RocksDB source "is dual-licensed under both the GPLv2
  ... and Apache 2.0 License" [RDB-SRC `README.md`], and ships `LICENSE.leveldb` for the LevelDB
  parts. How `cargo deny` treats C sources bundled inside a `-sys` crate beyond its license field
  is **UNVERIFIED**.
- **Maintenance.** 0.25.0 was released 2026-08-16, the last commit on the default branch; 189
  open issues and pull requests; 55.7 M downloads [crates.io; GitHub].

### 5.3 The fork: `rust-rocksdb` 0.53.0 / `rust-librocksdb-sys` 0.48.1+11.8.1 [ZRDB]

- **Same RocksDB (11.8.1), different build script.** It defines `HAVE_FULLFSYNC` for macOS and
  iOS, with the comment "Enable true on-disk durability via fcntl(F_FULLFSYNC)" [ZRDB
  `rust-librocksdb-sys/build.rs:1022–1046`].
- **The missing API is present**: `EventListener::on_background_error`
  (`event_listener.rs:1300–1310`), `set_max_bgerror_resume_count` (`db_options.rs:6088`),
  `set_file_checksum_gen_factory` (`db_options.rs:2772`), `verify_file_checksums`
  (`db.rs:1700`), `export_column_family` (`checkpoint.rs:183`),
  `create_column_family_with_import` (`db.rs:5435`), plus an SST file manager and write buffer
  manager.
- **Heuristic panic scan** (library code): fork 2 `panic!`, 1 `todo!`/`unimplemented!`, 45
  `.unwrap()`, 11 `.expect(`, 61 `assert*!`; upstream 1, 1, 46, 3, 45. Neither is panic-free;
  both need the unwind boundary that mantle already uses for `reed-solomon-simd` (CLAUDE.md
  rule 1; note 06 §B4.1 lists the upstream sites). An unwind boundary catches Rust panics only;
  whether RocksDB's release build has C++ abort paths was not audited (note 06 Appendix Z).
- **Targets.** CI tests Linux x86_64, Linux arm64, `macos-latest` and `windows-latest`
  [ZRDB `rust.yml`]; neither Windows arm64 nor macOS x86_64 is in the matrix. It still needs
  libclang (bindgen).
- **Maintenance.** Published 2026-09-05; 122 commits in 2026 (many dependency bumps); 45 stars;
  8 open issues. Excluding history inherited from upstream, one maintainer
  (`zaidoon1`, 219 commits). Licenses: Apache-2.0; MIT OR Apache-2.0 OR BSD-3-Clause
  [crates.io].

### 5.4 fjall 3.1.10 / lsm-tree 3.1.10

- **Durability is correct on all three OSes by construction.** Journal and table writes call
  `File::sync_all` or `sync_data` [FJALL `journal/writer.rs:138, 171, 220–228`; LSMT
  `table/writer/mod.rs:519`, `version/persist.rs:41`], and directories are flushed on Unix
  (no-op on Windows) [FJALL `file.rs:17–33`]. In Rust 1.98.0, `sync_all` and `sync_data` are
  `F_FULLFSYNC` on Apple targets, `fsync`/`fdatasync` on Linux and `FlushFileBuffers` on Windows
  [RUST-STD `unix.rs:1413–1452`; `windows.rs:400–407`].
- **What it has.** A bulk-load builder that writes sorted pairs straight into new tables
  [LSMT `ingestion.rs:97`], `drop_range`, which "Drops tables that are fully contained in a given
  range" [LSMT `abstract_tree.rs:163–171`], and `delete_keyspace` [FJALL `db.rs:420`].
- **Missing for mantle.** No checkpoint, no ingestion of table files built elsewhere (so no
  file-level snapshot or move), and no filesystem abstraction for simulation (lsm-tree #306,
  open) [note 06 §B4.2].
- **Open correctness issues re-checked 2026-09-28** [GitHub]. fjall #311 (now titled "add a
  'strict' recovery mode to distinguish mid-journal corruption from a torn tail") and lsm-tree
  #321 ("MVCC GC in CompactionStream drops the version the oldest snapshot must read") are
  open. The 3.0.0–3.1.5 releases remain yanked [crates.io].
- **License** MIT OR Apache-2.0. One maintainer (note 06 §B4.2).

### 5.5 redb 4.3.0

- **Durability** goes through `File::sync_data` [REDB `file_backend/optimized.rs:350–351`],
  hence the same per-OS primitives as fjall. No parent-directory flush after creating the
  database file was found in `src/` (a grep, not a proof); mantle's platform layer would do it.
- **Missing for mantle.** A copy-on-write B-tree with one writer: no ingestion, removals are
  O(n) over the range, space returns only on explicit compaction (note 06 §B4.3). A file-level
  snapshot would be a copy of the whole database file.
- **Strength.** A pluggable storage backend and no background threads, which fits deterministic
  simulation (note 06 §B4.0).
- **License** MIT OR Apache-2.0. One maintainer; 2026 crash-recovery fixes (note 06 §B4.3).

### 5.6 A mantle-built LSM

- **What mantle already has** (chunk-store.md; STATUS.md): a log-structured volume with
  segments; group commit with one full flush per batch and fencing on failure; CRC-32C per
  record; superblocks; checkpointed in-memory index; roll-forward recovery that tells a torn tail
  from corruption; cost-benefit cleaning; scrubbing; and a simulated device with power-loss
  semantics. About 4,900 lines in `crates/chunk/src`.
- **What an LSM adds.** A sorted memtable; an immutable sorted-table format with block index,
  filters and per-block checksums; a manifest of table versions; leveled compaction with file
  picking, trivial moves and range tombstones; merging iterators with snapshots; checkpoints;
  ingestion; rate limiting and shared memory budgets across instances.
- **Size reference (DERIVED from line counts, not a plan).** `lsm-tree` 3.1.10 is about 30,100
  lines of Rust and `fjall` 12,600 more; RocksDB's non-test C++ is about 240,000 lines.
- **What the evidence says about building one.** Meta's argument for a shared engine is that
  "Even simple applications need to protect against media corruption using checksums, guarantee
  data consistency after crashes, issue the right system calls in the correct order to guarantee
  durability of writes, and handle errors returned from the file system in a correct manner"
  [DONG21 §1, p. 33]. MyRocks' validation still found compaction and filter bugs in RocksDB
  itself (§1.13), and fjall's 2026 history shows the same classes of bug in a young engine
  (note 06 §B4.2). A mantle-built engine would run under mantle's simulator from the first line,
  which RocksDB cannot (note 06 §B4.0).

### 5.7 Evaluation matrix

| Requirement (§5.1) | RocksDB via upstream `rocksdb` | RocksDB via fork (ZRDB) | fjall / lsm-tree | redb | mantle-built LSM |
|---|---|---|---|---|---|
| 1. Order, scans, atomic batches | yes | yes | yes | yes (single writer) | to build |
| 2. File-level snapshot, split, move | checkpoint, ingest; no CF export/import | checkpoint, ingest, CF export/import | no (bulk-load builder only) | whole-file copy only | to build |
| 3. Cheap range and replica removal | `DeleteRange`, `DeleteFilesInRange`; drop directory per instance | same | `drop_range` (whole tables); `delete_keyspace` | O(n) | to build |
| 4. Checksums read + transfer | block XXH3 on read; file CRC-32C needs FFI | both, through the API | xxh3 (since 3.0) | Merkle checksums; read-path verification unclear (note 06) | CRC-32C, as the chunk store |
| 5. Full flush per OS | **Linux, Windows; not macOS** | **all three** | all three | all three | all three (std or mantle-disk) |
| 6. Fence, don't retry | fatal on flush errors; resume setting needs FFI | listener + resume count in API | `Error::Poisoned` | `PreviousIo` after an I/O error | by design |
| 7. Shared bounds across instances | write buffer manager, rate limiter, cache, pools, SST file manager | same, plus more exposed | not across instances | no | to build |
| 8. Six targets, gates, licenses | C++20 + libclang; cross-lint breaks; licenses OK | same | pure Rust | pure Rust | pure Rust |
| 9. Simulation | model engine stands in | model engine stands in | no FS abstraction | yes | yes |
| Production evidence | Meta, TiKV, CockroachDB (until Pebble) | same engine; binding younger | little | little | none |

### 5.8 Verdict (Recommendation)

1. **RocksDB stays the engine.** It alone meets rows 2, 3 and 7 with production evidence behind
   it (§1, §3, §4). This confirms note 06 §C.b.1.
2. **Fix the binding before anything depends on it.** Either adopt the fork (ZRDB), which
   already has the macOS flush, the error listener, the resume setting, file checksums and CF
   export/import; or stay on upstream once PR #1106 is released and add a mantle-owned FFI file
   for the rest, which needs the owner to widen CLAUDE.md rule 7's "OS interface" wording. The
   fork is one maintainer: pin it by version, keep its API behind mantle's engine trait (note 06
   §C.b.3), and keep switching back to upstream a one-file change.
3. **Change the cross-target lint, not the rule.** Keep native lint, test and release jobs on all
   six runners; restrict `check-targets.sh` to crates without C++ build scripts, or give it the
   cross toolchains. The owner chooses; either way the change is to the gate script, and every
   target is still linted on its own runner.
4. **Keep the pure-Rust candidates on the watch-list** with note 06's criteria, and keep a
   mantle-built LSM as the long-term option if RocksDB's build or behavior cannot meet the
   gates.

---

## 6. Implications for mantle

Everything in this section is **INFERENCE** unless a line cites a source or says DERIVED.

### 6.1 How a metadata range stores its state

**One RocksDB instance per range replica (a "tablet"), with node-level shared budgets.** This
revises note 06 §C.b.1, which proposed one engine per node with a metadata-only split.

- **Why.** It is ZippyDB's layout [CAO20 §2.2; DONG21 §4], the direction TiKV's own measurements
  pushed it [TIKV-RFC-0093], and it turns the operations mantle does most often into file
  operations:
  - snapshot: a checkpoint's file list (§6.3);
  - move: copy files, verify checksums, open (§6.5);
  - replica removal: delete a directory, with no tombstones and no compaction of other ranges;
  - isolation: a write stall [RDB-WIKI "Write-Stalls"] or a fatal background error (§1.12) stops
    one range, not every range on the device, which is the blast-radius rule of note 06 §C.a.3.
- **Shared per node or per device:** one write buffer manager (memtable bytes), one block cache,
  one rate limiter per device, shared flush and compaction thread pools, one SST file manager
  (disk budget and deletion rate) [DONG21 §4, p. 38; DONG21-TOS §4.1, p. 26:11; TIKV-RFC-0093].
  Each is a bound in the sense of CLAUDE.md rule 2, and exceeding it is a typed refusal
  (`no_slowdown` → `Busy`), never an unbounded wait.
- **Range size follows.** Per-instance cost (files, MANIFEST, memtables, filters) means ranges
  are few and large per node: Meta's "tens or hundreds of shards" per host [DONG21 §4, p. 38],
  TiKV's ≤10 GiB tablets [TIKV-RFC-0093]. §6.6 derives the bounds; mantle measures the fixed
  cost of one idle instance before fixing them (**UNVERIFIED** today).
- **Heat splits stay possible.** Split creates a new instance (§6.4). TiKV's answer to the cost
  of many small regions is dynamic size: small when hot, large when cold [TIKV-RFC-0082].
  ZippyDB's is µshards inside large physical shards [ZDB-BLOG].
- **Fallback, decided by measurement.** If the measured per-instance cost times the needed
  range count exceeds the memory budget, use one instance per device with a column family per
  range (cheap drop [DONG21-TOS §2.2]; export/import for moves [§1.9]) and accept DB-wide stalls
  and a shared failure domain.
- **Inside a tablet.** One column family; keys un-prefixed by range (the range is the instance);
  reserved keys for the applied index, the range descriptor and epoch, and the client-session
  table (note 06 §A1.8), all written in the same batch as the data they describe.

### 6.2 How the Raft log and the state engine relate

**The Raft log (focal-log, shared by all ranges on a device) is the only WAL; the engine runs
with its WAL disabled.**

- **Evidence.** "distributed systems often have their own replication logs (e.g., Paxos logs),
  in which case RocksDB WAL are not needed at all" [DONG21 §4, p. 38]; TiKV's tablets disable
  the KV WAL [TIKV-RFC-0093]; CockroachDB's shared engine forced unrelated state-machine writes
  into every synced log write [CRDB-RFC-RAFT]; Spanner called its double log "expedient" (note
  06 §A4.2). ZippyDB's default acknowledgement already means "persisting the data on a
  majority of replicas’ Paxos logs and writing the data to RocksDB on the primary" [ZDB-BLOG]:
  the log carries durability, the engine carries state.
- **Protocol.**
  1. Apply a committed entry as one `WriteBatch` with `disableWAL = true` containing the
     mutations and the new applied index (and descriptor or epoch changes). The batch is atomic
     in the memtable.
  2. On restart, open the engine, read the applied index it persisted, replay the Raft log from
     the next entry. Apply must be deterministic and idempotent at an index (note 06 §C.b.2).
  3. Truncate the Raft log only up to the **persisted** applied index, read with
     `ReadOptions::read_tier = kPersistedTier`, which skips the memtable when the WAL is
     disabled [RDB-SRC `options.h:1934–1942`].
  4. On macOS without `HAVE_FULLFSYNC`, RocksDB's flush is only `fsync`ed; the Apple man page
     guarantees that "data that had been fsync'd on the same device before is guaranteed to be
     persisted" when an `F_FULLFSYNC` returns (note 02 §3.7). So a focal-log group commit on the
     same device after the flush would make the flushed tables durable before truncation.
     Caveat: RocksDB's own ordering (tables before MANIFEST) still rests on plain `fsync` until
     then, so a power loss in between can leave a MANIFEST naming a table that never reached
     the platter; the engine then fails its checksums and the replica is rebuilt from peers.
     No acknowledged write is lost, because the log was not yet truncated, but recovery is
     costly. The binding fix (§5.8) removes the window.
  5. With one column family per tablet, a flush persists a prefix of the applied batches, so the
     persisted applied index names exactly the state on disk. More than one column family needs
     `atomic_flush` for the same property (§1.3).
- **Bounds.**
  - Unflushed log: force a flush when the entries or bytes between the persisted and the
    delivered index exceed what the node can replay within its restart-time budget (DERIVED:
    bytes ≤ measured replay rate × target restart time).
  - Memory: the same entries sit in focal's `RamLog` until a checkpoint (note 07 §2.4), so the
    memtable budget and the log budget are one budget.
- **focal changes this needs** (adds to note 07 §7.2 B). `begin_checkpoint` requires
  `index == delivered_index` (note 07 §2.4). Mantle needs to compact the log up to an index
  *below* the delivered index (the engine's persisted applied index) and keep the entries above
  it, or else force an engine flush at every checkpoint.
- **Failure.** A fatal engine error fences that range's replica; mantle reopens the engine from
  its files and replays the log, or rebuilds the replica from a peer. No automatic resume
  (§1.12).

### 6.3 Snapshots

- **Build.** At an applied index i: flush the tablet, take a checkpoint (hard links) [RDB-WIKI
  "Checkpoints"], and list its files with size and CRC-32C (the chunk store's checksum). The
  Raft snapshot payload is the list plus i and the range descriptor: kilobytes, within focal's
  8 MiB cap (note 07 §2.4).
- **Transfer.** Stream the files out of band over QUIC (note 07 §7.2 C), rate-limited per
  device: hard-linked files may not be in the page cache, which is why TiKV adds flow control
  [TIKV-RFC-0093]. The receiver verifies size and CRC-32C for every file, as TiKV does
  [TIKV-SRC `snap.rs:288–312`]; a mismatch is a typed corruption error that feeds repair
  (CLAUDE.md rule 6), and DONG21's 17 mismatches per PB transferred is the reason [p. 40].
- **Catch-up during large transfers.** Re-checkpoint and send only newly linked files, as MyRocks
  does [MYROCKS §3.3.1, p. 3224]; then Raft log entries after the last checkpoint.
- **Install.** Write into a staging directory, flush files and directory, rename into place,
  flush the parent (CLAUDE.md rule 6), then persist the new applied index in the Raft state. No
  ingestion into a shared tree, so no database-wide write block [RDB-WIKI
  "Creating-and-Ingesting-SST-files"].
- **Hold the log.** Do not truncate the source's log past an in-flight snapshot's index
  [CRDB-TN-SNAP], and bound the number of snapshots in flight per node, as CockroachDB's
  snapshot queue and reservations do [ibid.].
- **Measure snapshots.** CockroachDB treats a nonzero rate of Raft snapshots outside failures as
  a health signal [CRDB-TN-SNAP]; mantle's simulator and real-cluster tests should count them.

### 6.4 Range splits and merges

- **Split (per-range instances).** The split is a Raft command in the parent's log that bumps
  the descriptor generation (architecture.md §6; [TIDB §4.1.4]). At apply, every replica:
  1. flushes the parent and checkpoints it into the child's directory (hard links, O(files));
  2. in the parent, `DeleteRange` the child's span and `DeleteFilesInRange` it; in the child,
     the reverse;
  3. persists both descriptors and applied indexes, then acknowledges.
  Space is shared through hard links until compaction rewrites the straddling files (DERIVED:
  at most one straddling file per level, about 2–3 levels per tablet in TiKV's estimate
  [TIKV-RFC-0093]). This is TiKV's design without the `FreezeAndClone` API, which costs a flush
  per split.
- **No snapshot for a split.** Both halves stay on the same replicas until the directory update
  (architecture.md §6); "A split shouldn’t require a Raft snapshot" [CRDB-TN-SNAP].
- **Merge.** Rare (architecture.md §6). Co-locate replicas, freeze the right-hand range, then
  copy its keys into the left tablet by ingestion of SSTs written from a snapshot (non-overlapping
  by construction [MYROCKS §3.2.3.4]); or, experimentally, ingest the right tablet's own files
  with `allow_db_generated_files` [RDB-SRC `options.h:2934–2976`].

### 6.5 Moves between nodes and between cells

- **Within a cell** (new replica, repair, rebalancing): add a learner, send a snapshot (§6.3),
  catch up from the log, run joint consensus (architecture.md §6). The data moves as verified
  files.
- **Between cells** (architecture.md §6.1): rows are *re-created* in the target because Block
  and File rows refer to cell-local placements (note 09 §9.6). That is a logical copy: scan the
  source at a snapshot with fill-cache off, as RocksDB supports for this purpose [DONG21-TOS
  §4.2.1, p. 26:12], transform, and bulk-load the target through SST ingestion into an empty
  tablet [MYROCKS §3.2.3.4]. Akkio's µ-shard moves are logical for the same reason (note 09
  §7.8.4).
- **Leaving a range behind.** Delete the tablet directory after the grace period (architecture.md
  §6.1 step 8), through the SST file manager's rate-limited deletion [DONG21 §4, p. 39].

### 6.6 Parameters: calculated, from which model, with which measured inputs

None of RocksDB's defaults (§1.2) is taken as-is. Each row names the model, the measured inputs
and the rule; "trace" means a recorded mantle metadata workload replayed with key-range
locality preserved [CAO20 §7].

| Parameter (RocksDB option) | Model | Measured inputs | Rule |
|---|---|---|---|
| Compaction style | Dostoevsky cost model [§2.5]; DONG21 Table 3 [§1.5] | op mix from the trace (point, zero-result, short and long scans, writes); device read/write costs from `mantle disk probe` | leveled unless the model, run on the trace, says writes dominate |
| Size ratio T (`max_bytes_for_level_multiplier`) | space bound 1 + 1/(T−1) [§2.2]; Lim [§2.3]; O'Neil [§2.1]; Dostoevsky/Monkey costs [§2.4–§2.5] | space budget from the capacity plan; write budget from device endurance and measured write rate; op mix from the trace | the space bound gives a lowest T, the write budget (Lim's model) a highest T; the cost model picks within that interval on the trace's op mix; an empty interval means the budgets conflict and the capacity plan changes |
| Level sizes (`level_compaction_dynamic_level_bytes`, base) | DONG17 [§2.2]; Lim's optimizer [§2.3] | key-popularity distribution from the trace | dynamic on; base from Lim's optimizer, confirmed by trace replay |
| Filter bits per key, Bloom or Ribbon, last-level filter | Monkey [§2.4]; Ribbon [§2.9] | filter memory per tablet (node budget ÷ tablets); zero-result share from the trace; CPU headroom | one bits-per-key value per tablet from Monkey's cost equations with uniform allocation, since neither binding can vary it by level [§2.4]; the hybrid Ribbon policy where memory binds and CPU does not; keep a last-level filter while zero-result lookups are common |
| Memtable size and count (`write_buffer_size`, `max_write_buffer_number`), node total (write buffer manager) | Lim (buffer versus write amplification) [§2.3]; O'Neil memory-versus-I/O [§2.1]; replay bound [§6.2] | node memory budget; Raft-log replay rate; target restart time | node total = min(memory budget, replay rate × restart budget); per-tablet share of it enforced by the write buffer manager |
| L0 triggers, stall thresholds | SILK [§2.6] | flush and L0→L1 bandwidth measured on the device; peak ingest per node | stop never reached at measured peak; stalls replaced by admission control returning `Busy` [TIKV-RFC-0067] |
| Compaction bandwidth and threads (rate limiter, pools) | SILK: I = T − C − ε; threads from device and per-compaction bandwidth [§2.6] | device sequential bandwidth (`mantle disk probe`); client bandwidth at run time | dynamic, per device, shared by all tablets on it |
| File-deletion rate (SST file manager) | DONG21 TRIM finding [§1.10] | read-latency change during deletions, measured per device class | the highest rate that leaves measured read p99 unchanged |
| Direct I/O (`use_direct_reads`, `use_direct_io_for_flush_and_compaction`) | device identification + calibration (CLAUDE.md rule 5) | the probe's direct-I/O support and alignment; cache hit rates from the trace | per device, as the chunk store decides |
| Block size, block cache capacity | CAO20 locality [§1.13] | hit-ratio curve from trace replay | smallest cache on the flat part of the curve |
| `periodic_compaction_seconds`, `ttl` | MYROCKS [§1.5] | lazy-deletion grace period (chunk-store.md §8) | at most the grace period, so deleted metadata leaves the disk within it |
| Tablet (range) size bounds | TEC parallel recovery [§3.7]; move time; per-instance cost [§6.1] | network and disk bandwidth for snapshots; measured idle-instance cost; recovery-time target; per-range QPS cap | max size ≤ bandwidth × move budget; count per node ≤ memory budget ÷ per-instance cost; enough ranges per node to rebuild a failed node in parallel |

### 6.7 Failure handling and integrity

- Set `paranoid_checks = true` (default), `max_bgerror_resume_count = 0`, and register the
  background-error listener; any background error fences the tablet (§1.12).
- Enable the CRC-32C file-checksum generator; set `verify_checksums_before_ingest` and verify
  file checksums on every transfer, ingestion and backup (§1.9, §1.12).
- Measure per-key-value protection (`protection_bytes_per_key`, memtable protection) before
  enabling it: DONG21's CPU and memory faults are what it catches (§1.12), and its cost on
  mantle's hardware is **UNVERIFIED**.
- Compare replicas: a periodic digest of each tablet at an applied index, compared across
  replicas, answers DONG21's open question for mantle and SM's "continuous data-consistency
  auditing" [SM §2.4, p. 558]. Paxos Made Live's checksum-at-index (note 06 §C.d.3) is the
  model.
- Deterministic simulation runs a model engine behind the engine trait (note 06 §C.b.3); RocksDB
  itself is covered by real-disk crash and fault-injection tests, including a macOS power-cut
  test that would have caught §5.2's missing flush.

### 6.8 What remains unknown

- ZippyDB: Data Shuttle's pipelining and log format; whether RocksDB's WAL is on; how replicas
  are bootstrapped and shards split; the metadata tier's shard size (§3.8).
- Whether `kMinOverlappingRatio` closes the file-picking gap LIM16 measured (§1.5).
- Whether uniform filter bits cost as much lookup I/O on mantle's trace as the §2.4 table
  predicts; neither binding can install a per-level filter policy (§2.4), so a large loss
  would mean carrying a C++ filter policy in the build.
- The fixed memory and file cost of an idle RocksDB 11.8.1 instance, which sets the tablet count
  per node (§6.1).
- The cost of per-key-value protection (§6.7).
- How `cargo deny` treats C and C++ sources bundled in `-sys` crates (§5.2).
- Whether NTFS needs a directory flush for create or rename durability (note 02 §4.8), which
  RocksDB, focal-log and fjall all skip on Windows.
- When rust-rocksdb PR #1106 is released, and whether the fork stays maintained.
- Every quantitative claim in §2 was measured on hardware and workloads other than mantle's;
  §6.6 lists the measurements that replace them.

---

## Appendix A: quantitative quick reference

| Fact | Value | Source |
|---|---|---|
| Leveled / tiered / FIFO write amplification (RocksDB 5.9 benchmark) | 16.07 / 4.8 / 2.14 | [DONG21 Table 3, p. 35] |
| Leveled / tiered max space overhead | 9.8% / 94.4% | [DONG21 Table 3, p. 35] |
| Typical leveled write amplification | 10–30 | [DONG21 §3, p. 36] |
| SSD internal write amplification | 1.1–3 | [DONG21 §3, p. 36] |
| Dynamic vs static leveling space overhead | ≤13% vs >25% (worst case 90%) | [DONG21 §3, p. 36] |
| Worst-case space amplification, T = 10 | 1.111 | [DONG17 §3, PDF p. 3] |
| Facebook size multiplier | 10 (a few use 8) | [DONG17 §3.1, PDF p. 4] |
| Last-level filter size vs all others | ~9× | [DONG17 §4, PDF p. 5] |
| Bloom filter memory saved without last-level filter | 90% | [MYROCKS §3.2.3.1, p. 3223] |
| Prefix Bloom read-amplification cut on range queries | up to 64% | [DONG17 §4, PDF p. 5] |
| Bloom space over information bound / Ribbon | ≥44% / <10% | [RIBBON §1, p. 1] (PREPRINT) |
| Filter memory and CPU in large RocksDB deployments | ~10% of memory, ~1% of CPU | [RIBBON §1 fn. 5, p. 1] (PREPRINT) |
| Ribbon in RocksDB | ~30% less filter memory, 3–4× filter CPU | [RDB-WIKI "RocksDB-Bloom-Filter"] (NON-PEER-REVIEWED) |
| RocksDB-level corruption rate | ~1 per 3 months per 100 PB; 40% already on other replicas | [DONG21 §5, p. 40] |
| Checksum mismatches in transfer (storage bug) | ~17 per PB transferred | [DONG21 §5, p. 40] |
| ZippyDB metadata shard op mix (24 h) | ~420 M queries: 78% Get, 13% Put, 6% Delete, 3% Iterator | [CAO20 §4.1, p. 212] |
| ZippyDB metadata key / value size | 47.9 B / 42.9 B average; >90% of values <34 B | [CAO20 Table 2, §5, p. 215] |
| YCSB vs replay on that shard | ≥7.7× block reads, 0.17× cache hits | [CAO20 §7.1, p. 218] |
| ZippyDB distinct configurations | >25 across 39 deployments | [DONG21 §4, p. 39] |
| ZippyDB replicas per shard (most deployments) | 1 primary + 2 secondaries | [SM §2.5, p. 558] |
| ZippyDB physical shard | 50–100 GB, tens of thousands of µshards | [ZDB-BLOG] (NON-PEER-REVIEWED) |
| Shards per host at Meta | tens or hundreds | [DONG21 §4, p. 38] |
| jemalloc overhead across 40 ZippyDB clusters | 2%–25% | [DONG21-TOS §6.4, p. 26:21] |
| MyRocks vs compressed InnoDB size | 62.3% smaller | [MYROCKS abstract, p. 3217] |
| LZ4 above Lmax vs Zstandard everywhere | compaction time to one third | [MYROCKS §3.3.4, p. 3224] |
| LSM model error vs worst-case analysis | ≤3.0% vs 1.8–3.5× overestimate | [LIM16 §4.5, p. 156] |
| Monkey lookup latency reduction | 50%–80% | [MONKEY abstract, PDF p. 1] |
| Monkey vs uniform filters, T = 10, L = 4 | 0.36× wasted I/O; 2.1 bits/key saved | DERIVED (§2.4) |
| SILK p99 improvement | up to 100× | [SILK abstract, p. 753] |
| WiscKey range query, 64 B pairs | 12× worse than LevelDB | [WISCKEY §4.1.2, p. 142] |
| PebblesDB small-range-query overhead (compacted) | 30% | [PEBBLES §1, PDF p. 2] |
| TiKV region split size | 256 MiB (96 MiB before v8.4.0) | [TIKV-DOCS] (NON-PEER-REVIEWED) |
| TiKV tablet max size; scale-in / scale-out of 1 TiB | 10 GiB; 4 h / 7 h (vs >16 h / 2 days) | [TIKV-RFC-0093] (NON-PEER-REVIEWED) |
| TiKV snapshot I/O cap (default) | 100 MiB/s | [TIKV-DOCS] (NON-PEER-REVIEWED) |
| CockroachDB snapshot size in 2018 import | ~64 MB (~32 MB in practice); ~15,000 snapshots | [CRDB-TN-SNAP] (NON-PEER-REVIEWED) |
| ZippyDB metadata shard average rate | ~4,861 queries/s | DERIVED from [CAO20 §4.1] |
| Filter memory for a 50–100 GB shard of 91 B rows | 0.69–1.37 GB at 10 bits/key | DERIVED (§2.9) |
