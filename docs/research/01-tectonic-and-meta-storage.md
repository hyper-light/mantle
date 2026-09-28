# 01 - Tectonic and Meta/LinkedIn blob storage: primary-source research

Permanent design record for **mantle** (Rust, Tectonic-style distributed on-disk filesystem and object store behind an S3-compatible API).
Compiled 2026-09-28. Covers: Tectonic (FAST '21), f4 (OSDI '14), Haystack (OSDI '10), Ambry (SIGMOD '16), and ZippyDB (no paper exists).

---

## How to read this document

**Citation tags.** Every factual bullet carries a tag of the form `[KEY §section, p. N]`. `p.` is the printed proceedings page number.

- TEC, F4, TSHIFT, BALEEN, CAO and AKKIO: page numbers are printed in the PDF footers.
- HAY: the USENIX legacy PDF has no page footers. The OSDI '10 page range 47-60 (from the Semantic Scholar/DBLP record `conf/osdi/BeaverKLSV10`) is mapped one-to-one onto the PDF's 14 pages.
- AMB: the author-hosted copy has no page footers. The ACM page range 253-265 (from Crossref, DOI 10.1145/2882903.2903738) is mapped one-to-one onto its 13 pages.

**Quotes.** Quotes are verbatim from each PDF's text layer. Ligatures are normalized, words hyphenated across line breaks are rejoined, bracketed reference numbers such as "[26]" are omitted, "..." marks elisions, and "[sic]" marks typos in the original.

**Evidence labels:**

- *(no label)*: stated in the cited source. We checked it against the full text.
- **DERIVED**: arithmetic or an interpretation made by this note from stated facts. The source does not state it.
- **UNVERIFIED**: not found in any primary source we consulted. Do not rely on it without new evidence.
- **NON-PEER-REVIEWED**: from Meta's engineering blog. The owner allows this for ZippyDB only.
- **Recommendation / INFERENCE**: design reasoning for mantle. Each one cites the facts it rests on.

**Method.** PDFs were downloaded from usenix.org, plus one author-hosted copy for Ambry, and read in full from their text layers. No secondary summaries (blogs, lecture notes, slide decks) were used as evidence for any fact.

---

## Sources

| Key | Full citation | Peer-reviewed | Where obtained |
|---|---|---|---|
| **TEC** | Satadru Pan, Theano Stavrinos, Yunqiao Zhang, Atul Sikaria, Pavel Zakharov, Abhinav Sharma, Shiva Shankar P, Mike Shuey, Richard Wareing, Monika Gangapuram, Guanglei Cao, Christian Preseau, Pratap Singh, Kestutis Patiejunas, JR Tipton, Ethan Katz-Bassett, Wyatt Lloyd. "Facebook's Tectonic Filesystem: Efficiency from Exascale." *19th USENIX Conference on File and Storage Technologies (FAST '21)*, Feb 23-25, 2021, pp. 217-231. ISBN 978-1-939133-20-5. | Yes | https://www.usenix.org/conference/fast21/presentation/pan (PDF: https://www.usenix.org/system/files/fast21-pan.pdf) |
| **F4** | Subramanian Muralidhar, Wyatt Lloyd, Sabyasachi Roy, Cory Hill, Ernest Lin, Weiwen Liu, Satadru Pan, Shiva Shankar, Viswanath Sivakumar, Linpeng Tang, Sanjeev Kumar. "f4: Facebook's Warm BLOB Storage System." *11th USENIX Symposium on Operating Systems Design and Implementation (OSDI '14)*, Oct 6-8, 2014, Broomfield, CO, pp. 383-398. ISBN 978-1-931971-16-4. | Yes | https://www.usenix.org/conference/osdi14/technical-sessions/presentation/muralidhar (PDF: https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-muralidhar.pdf) |
| **HAY** | Doug Beaver, Sanjeev Kumar, Harry C. Li, Jason Sobel, Peter Vajgel. "Finding a needle in Haystack: Facebook's photo storage." *9th USENIX Symposium on Operating Systems Design and Implementation (OSDI '10)*, Oct 2010, Vancouver, BC, pp. 47-60. | Yes | https://www.usenix.org/conference/osdi10/finding-needle-haystack-facebooks-photo-storage (PDF: https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf) |
| **AMB** | Shadi A. Noghabi, Sriram Subramanian, Priyesh Narayanan, Sivabalan Narayanan, Gopalakrishna Holla, Mammad Zadeh, Tianwei Li, Indranil Gupta, Roy H. Campbell. "Ambry: LinkedIn's Scalable Geo-Distributed Object Store." *Proceedings of the 2016 International Conference on Management of Data (SIGMOD '16)*, San Francisco, pp. 253-265. DOI 10.1145/2882903.2903738. | Yes | https://doi.org/10.1145/2882903.2903738 (text read from the author copy: https://assured-cloud-computing.illinois.edu/files/2014/03/Ambry-LinkedIns-Scalable-GeoDistributed-Object-Store.pdf) |
| **ZDB-BLOG** | Sarang Masti. "How we built a general purpose key value store for Facebook with ZippyDB." Engineering at Meta blog, Aug 6, 2021. | **No (NON-PEER-REVIEWED)** | https://engineering.fb.com/2021/08/06/core-infra/zippydb/ |
| **TSHIFT** | Mark Zhao, Satadru Pan, Niket Agarwal, Zhaoduo Wen, David Xu, Anand Natarajan, Pavan Kumar, Shiva Shankar P, Ritesh Tijoriwala, Karan Asher, Hao Wu, Aarti Basant, Daniel Ford, Delia David, Nezih Yigitbasi, Pratap Singh, Carole-Jean Wu, Christos Kozyrakis. "Tectonic-Shift: A Composite Storage Fabric for Large-Scale ML Training." *2023 USENIX Annual Technical Conference (ATC '23)*, pp. 433-449. (Supplementary: gives Tectonic block and chunk sizes. Several authors are also Tectonic authors.) | Yes | https://www.usenix.org/conference/atc23/presentation/zhao |
| **BALEEN** | Daniel Lin-Kit Wong, Hao Wu, Carson Molder, Sathya Gunasekar, Jimmy Lu, Snehal Khandkar, Abhinav Sharma, Daniel S. Berger, Nathan Beckmann, Gregory R. Ganger. "Baleen: ML Admission & Prefetching for Flash Caches." *22nd USENIX Conference on File and Storage Technologies (FAST '24)*, pp. 347-371. (Supplementary.) | Yes | https://www.usenix.org/conference/fast24/presentation/wong |
| **CAO** | Zhichao Cao, Siying Dong, Sagar Vemuri, David H.C. Du. "Characterizing, Modeling, and Benchmarking RocksDB Key-Value Workloads at Facebook." *18th USENIX Conference on File and Storage Technologies (FAST '20)*, pp. 209-223. (Supplementary: peer-reviewed description of ZippyDB and measured metadata key/value sizes.) | Yes | https://www.usenix.org/conference/fast20/presentation/cao-zhichao |
| **AKKIO** | Muthukaruppan Annamalai, Kaushik Ravichandran, Harish Srinivas, Igor Zinkovsky, Luning Pan, Tony Savor, David Nagle, Michael Stumm. "Sharding the Shards: Managing Datastore Locality at Scale with Akkio." *13th USENIX Symposium on Operating Systems Design and Implementation (OSDI '18)*, Oct 8-10, 2018, Carlsbad, CA, pp. 445-460. ISBN 978-1-939133-08-3. (Supplementary: peer-reviewed description of ZippyDB replication.) | Yes | https://www.usenix.org/conference/osdi18/presentation/annamalai |

Terminology differs between these systems, so read the glossary before comparing sizes.

| Concept | Tectonic | f4 | Haystack | Ambry |
|---|---|---|---|---|
| Unit stored on one disk | **chunk**: a file on XFS [TEC §3.2, p. 219] | **block**: typically 1 GB of a volume's data file [F4 §5.3, p. 389] | **physical volume**: one ~100 GB file [HAY §3.4, p. 51] | **partition replica**: a 100 GB preallocated append-only file [AMB §2.2, p. 255] |
| Unit of durability/encoding | **block**: RS or replicated chunks [TEC §3.2, p. 219] | **stripe**: n data + k parity blocks [F4 §5.3, pp. 388-389] | **logical volume**: the set of physical volumes (replicas) [HAY §3.1, p. 50] | **partition**: replicated [AMB §2.2, p. 255] |
| User object | file (files contain blocks) or blob (packed in log-structured files) [TEC §5.2, p. 224] | BLOB inside a locked volume [F4 §4, p. 386] | needle [HAY §3.4, p. 52] | blob; large blobs are split into 4-8 MB chunks [AMB §4.2.1, p. 257] |

Note that BALEEN uses "block" for the unit mapped to an HDD location, which is what Tectonic calls a *chunk* (see §1.14).

---

## 0. Decision-relevant summary (for mantle)

1. **Metadata layers.** Metadata goes in three separately hash-partitioned layers over a sharded key-value store:
   - **Name** layer, sharded by `dir_id`.
   - **File** layer, sharded by `file_id`.
   - **Block** layer, sharded by `blk_id`.

   The KV store provides per-shard linearizability and atomic in-shard read-modify-write transactions. It does *not* provide cross-shard transactions [TEC §3.3, Table 1, pp. 220-221]. A directory's immediate listing lives in one shard, and listing it is a prefix scan. Hash partitioning, rather than range partitioning, was chosen specifically to avoid hotspots [TEC §3.3, §6.3, pp. 220, 225-226].
2. **Consistency without cross-shard transactions.** Tectonic guarantees read-after-write consistency for:
   - data operations,
   - single-object file/directory operations,
   - moves within the same parent directory.

   Cross-directory moves are **non-atomic**: a two-phase link-then-unlink with a parent backpointer. Garbage collectors between layers clean up the resulting "acceptable" inconsistencies [TEC §3.3, §3.5, pp. 221-222]. The authors report "These limitations have not been a problem in practice" [TEC §7, p. 228].
3. **Two write paths, chosen per call:**
   - **(a) Full-block RS-encoded writes** for large write-once files. The client encodes. For RS(9,6), it sends reservation requests to 19 nodes, writes to the first 15, and acknowledges at 14 of 15 [TEC §5.1, p. 224].
   - **(b) Partial-block quorum appends** for small objects. These are 3-way replicated and acknowledged after 2 on-disk writes. The post-append block size and checksum are committed to block metadata *before* the acknowledgement. Sealed blocks are later re-encoded to RS(10,4) [TEC §5.2, pp. 224-225].
4. **Sizes.**
   - Tectonic blocks are "typically 72 MiB" and chunks "typically 8 MiB" [TSHIFT §2, p. 435]. The Tectonic paper itself uses 72 MB blocks only in its hedging experiment [TEC Fig. 3a, p. 224].
   - Ambry independently settled on 4-8 MB chunks for large blobs [AMB §4.2.1, p. 257].
   - f4 used 1 GB EC blocks [F4 §5.3, p. 389].
   - Haystack, f4 and Ambry all use ~100 GB append-only containers [HAY §3.4, p. 51; F4 §4, p. 386; AMB §2.2, p. 255].
5. **Single writer per file.** Enforced with a **write token** stored in the file metadata. A new opener overwrites the token and seals the previous writer's open blocks. No lease or expiry mechanism is described [TEC §3.4, p. 221].
6. **Contiguous RS layout.** Most reads are smaller than a chunk, so they are one disk IO. Reconstructed reads cost 10x the IOs under RS(10,4) and are **capped at 10% of reads** to prevent "reconstruction storms" [TEC §6.4, p. 226].
7. **Explicit placement.**
   - Chunk-to-disk locations are stored in the Block layer, not computed by hashing as in Ceph [TEC §7, p. 228].
   - Copysets are drawn from ~100 consistent shuffles of all disks [TEC §3.5, p. 222].
   - Grouping blocks for fewer metadata entries ("block groups") was tried and abandoned: with 5% of nodes down, 80% of groups were unwritable [TEC §6.6, p. 227].
8. **Multitenancy.**
   - Storage capacity uses static per-tenant quotas.
   - Ephemeral resources (IOPS/disk time and metadata QPS) are shared through ~50 TrafficGroups per cluster in 3 TrafficClasses (Gold/Silver/Bronze).
   - Enforcement combines a client-side distributed-counter leaky bucket with node-side weighted round-robin plus protections for Gold requests [TEC §4.1, pp. 222-223].
   - Disk usage is accounted in **disk time**, not IOPS or bytes [TEC §6.2, p. 225].
9. **Metadata hotspots.** Tectonic handles them with:
   - hash partitioning,
   - sealing, so sealed metadata can be cached at clients and metadata nodes,
   - a per-shard QPS cap of 10 KQPS with retry after backoff,
   - a list API that returns file IDs.

   About 1% of Name-layer shards hit the cap during data-warehouse spikes [TEC §3.3, §6.3, pp. 220, 225-226].
10. **Inherited limitations that matter for S3.** Tectonic has no recursive list API, no `du`, and higher metadata latency than an in-memory NameNode [TEC §6.5, p. 227]. The flat/recursive prefix listing S3 clients expect is therefore the #1 open design risk if mantle adopts hash-partitioned directories (see §6.2).
11. **Integrity.** Checksums are carried end to end, both between and within processes. In-memory transforms such as RS encoding are verified by applying the inverse transform. The reason: "in-memory data corruption is a regular occurrence" at this scale [TEC §6.6, p. 227].
12. **Deleting packed small objects.** Three designs exist:
    - flag plus copy-compaction (Haystack) [HAY §3.4.3, §3.6.1, pp. 52-54];
    - delete-entry plus in-place compaction (Ambry) [AMB §2.2, p. 255];
    - crypto-shredding with per-BLOB keys and no compaction, which left 6.8% dead space (f4) [F4 §5.3, §6.5, pp. 389, 394].
13. **ZippyDB sources.** Everything about ZippyDB beyond sharding, Paxos, RocksDB, primary-served strong reads and "no cross-shard transactions" is **NON-PEER-REVIEWED** (see §5).

---

## 1. Tectonic (Pan et al., FAST '21) - priority source

### 1.1 Scope and context

- Tectonic is Facebook's exabyte-scale distributed filesystem. It serves "around ten tenants", including blob storage and data warehouse, and each of those two stores exabytes [TEC §1, p. 217].
- It replaced Haystack and f4 for blob storage and HDFS for the data warehouse. Before Tectonic, "Data warehouse was spread across many HDFS instances" [TEC §1, p. 217].
- Tectonic ran in single-tenant clusters for several years. At writing time, "Multitenant clusters are being methodically rolled out" [TEC §1, p. 217].
- Moving the data warehouse from HDFS to Tectonic cut the number of warehouse clusters by 10x [TEC §1, p. 217].
- Blobs are immutable and opaque, ranging from several KB (small photos) to several MB (HD video chunks), and need low latency [TEC §2.1, p. 218]. Data-warehouse reads average multiple MB and writes average tens of MB, and the workload prioritizes throughput over latency [TEC §2.2, p. 218].
- Effective replication factors [TEC §2.1, p. 218]:
  - Haystack's *ideal* was 3.6x (3x replication times 1.2x for RAID-6). Being IOPS-bound pushed it to **5.3x**.
  - f4 was 2.8x, using RS(10,4) in two datacenters.
  - Moving blob storage to Tectonic gave "~2.8x".
  - **DERIVED:** 2 x 1.4 (RS(10,4)) = 2.8, which fits blob storage's second copy in another datacenter [TEC §5.2, p. 224].
- The system was previously described in talks under the name "Warm Storage" [TEC §7, p. 228].

### 1.2 Architecture [TEC §3.1, Fig. 2, pp. 218-219]

- A **cluster** is the top-level deployment unit. It is datacenter-local and "resilient to host, rack, and power domain failures". Tenants build geo-replication on top [TEC §3.1, p. 218].
- **Components:**
  - **Chunk Store**: storage nodes.
  - **Metadata Store**: a scalable KV store plus *stateless* metadata services that build filesystem logic over it.
  - **Client Library**: orchestrates RPCs to both stores.
  - **Stateless background services**: garbage collectors, rebalancer, stat service, disk inventory, block repair/scan, storage node health checker (Fig. 2).
  - "Apart from the Chunk and Metadata Stores, all components are stateless" (Fig. 2 caption) [TEC p. 219].
- **Namespaces.** A cluster supports "any number of arbitrarily-sized namespaces". Each tenant typically owns one, and namespace size is limited only by cluster size [TEC §3.1, p. 219].
- **API.** Applications see "a hierarchical filesystem API with append-only semantics, similar to HDFS". Unlike HDFS, the APIs are configurable at runtime per call, not preconfigured per cluster or per tenant [TEC §3.1, p. 219; §5, p. 223].
- **Client-driven microservices.** "The Chunk and Metadata Stores each run independent services... orchestrated by the Client Library" [TEC §3.1, p. 219].

### 1.3 Chunk Store [TEC §3.2, p. 219]

- It is "a flat, distributed object store for chunks, the unit of data storage in Tectonic. Chunks make up blocks, which in turn make up Tectonic files."
- It is flat: the number of chunks grows linearly with storage nodes. It is also **oblivious to blocks and files**, which are "constructed by the Client Library using the Metadata Store".
- "Individual chunks are stored as files on a cluster's storage nodes, which each run a local instance of XFS."
- **Storage-node API:** get, put, append to, and delete chunks, plus APIs for **listing** and **scanning** chunks. Each node is responsible for sharing its *local* resources fairly among tenants.
- **Hardware:**
  - "Each storage node has 36 hard drives for storing chunks" (cites the Bryce Canyon platform).
  - Each node also has "a 1 TB SSD, used for storing XFS metadata and caching hot chunks".
  - Storage nodes run an XFS variant that stores local XFS metadata on flash. This "is particularly helpful for blob storage, where new blobs are written as appends, updating the chunk size."
  - The hot-chunk cache is managed by a flash-endurance-aware cache library (CacheLib).
- **HDD capacity is not stated in TEC. UNVERIFIED.**
  - **DERIVED** from Table 2: 1590 PB / 4208 nodes = ~378 TB per node, and / 36 drives = ~10.5 TB per HDD. This assumes "Capacity" counts raw HDD bytes only.
  - BALEEN, citing TEC, states "Each node has 378 TB in HDDs, 400 GB in flash cache, and 10 GB in DRAM cache" [BALEEN §2.1, p. 348]. Because it cites TEC, this is not independent corroboration; it matches the derived ~378 TB.

### 1.4 Blocks, durability, erasure-coding schemes

- A block is "a logical unit that hides the complexity of raw data storage and durability". Tectonic provides **per-block durability** so tenants can trade off capacity, fault tolerance and performance. Blocks are either Reed-Solomon encoded or replicated [TEC §3.2, p. 219].
- **Notation.** "For RS(r, k) encoding, the block data is split into r equal chunks (potentially by padding the data), and k parity chunks are generated from the data chunks." That is, the first number is data chunks and the second is parity chunks. "For replication, data chunks are the same size as the block and multiple copies are created." Chunks of a block are stored in different fault domains, such as different racks [TEC §3.2, p. 219].
- **Schemes in production:**

| Use | Scheme | Source | Storage overhead (DERIVED) |
|---|---|---|---|
| Data warehouse, long-lived data | RS(9,6) | [TEC §5.1, p. 223] | 15/9 = 1.67x |
| Data warehouse, short-lived data (e.g., map-reduce shuffles) | RS(3,3) | [TEC §5.1, p. 223] | 2.0x |
| Blob storage, new appends | replicated; "e.g., two nodes for three-way replication" acknowledge | [TEC §5.2, p. 224] | 3.0x |
| Blob storage, after the block is sealed | re-encoded to RS(10,4) | [TEC §5.2, p. 225] | 1.4x |

- **Configuration granularity.** Durability is configured per block write. By contrast, HDFS configures it per directory [TEC §5, p. 223].
- **Contiguous, not striped, layout.** Tectonic uses contiguous RS encoding. "The majority of reads are smaller than a chunk size", so reads are usually direct: "a single disk IO" [TEC §6.4, p. 226].

### 1.5 Metadata Store: schema, partitioning, KV guarantees [TEC §3.3, pp. 220-221]

**Table 1, reproduced exactly [TEC Table 1, p. 220]:**

| Layer | Key | Value | Sharded by | Mapping |
|---|---|---|---|---|
| Name | (dir_id, subdirname) | subdir_info, subdir_id | dir_id | dir -> list of subdirs (expanded) |
| Name | (dir_id, filename) | file_info, file_id | dir_id | dir -> list of files (expanded) |
| File | (file_id, blk_id) | blk_info | file_id | file -> list of blocks (expanded) |
| Block | blk_id | list<disk_id> | blk_id | block -> list of disks (i.e., chunks) |
| Block | (disk_id, blk_id) | chunk_info | blk_id | disk -> list of blocks (expanded) |

Caption: "dirname and filename are application-exposed strings. dir_id, file_id, and block_id are internal object references. Most mappings are expanded for efficient updating."

**What each layer maps.** "The Name layer maps each directory to its sub-directories and/or files. The File layer maps file objects to a list of blocks. The Block layer maps each block to a list of disk (i.e., chunk) locations. The Block layer also contains the reverse index of disks to the blocks whose chunks are stored on that disk, used for maintenance operations." [p. 220]

**Partitioning.** "Name, File, and Block layers are hash-partitioned by directory, file, and block IDs, respectively." [p. 220]

**DERIVED observation.** The reverse index `(disk_id, blk_id)` is sharded by **blk_id**, not by disk_id. A block's forward entry and all of its reverse entries therefore live in the *same* shard and can be updated in one in-shard transaction. The flip side is that finding everything on one disk means visiting every Block-layer shard. That matches the repair service working "on a per-Block layer shard, per-disk basis" [TEC §3.5, p. 221].

**Expanded keys.** "A key mapped to a list is expanded by storing each item in the list as a key, prefixed by the true key." Example: directory d1 with files foo and bar is stored as keys (d1, foo) and (d1, bar) "in d1's Name shard". Expansion avoids read-then-write of whole lists, which matters because "directories may contain millions of files". Contents are listed "by doing a prefix scan over keys" [p. 220].

**Not specified in TEC. UNVERIFIED:**
- the fields inside `subdir_info`, `file_info`, `blk_info` and `chunk_info`;
- the key under which per-file metadata lives. The write token (§1.6) and the file "owners" (rename step R2) are stored in "file metadata", but Table 1 shows no per-file header key;
- how block order within a file is encoded;
- where a moved directory's "backpointer" is stored.

**The KV store (ZippyDB), as described in TEC [p. 220]:**
- "Tectonic delegates filesystem metadata storage to ZippyDB [6], a linearizable, fault-tolerant, sharded key-value store."
- "all operations are scoped to a shard, and shards are the unit of replication."
- Nodes run RocksDB (SSD-based) for shard replicas, and shards are replicated with Paxos.
- "Any replica can serve reads, though reads that must be strongly consistent are served by the primary."
- "**The key-value store does not provide cross-shard transactions**, limiting certain filesystem metadata operations."
- "Shards are sized so that each metadata node can host several shards", which allows parallel redistribution after a node failure and granular load balancing. The KV store "will transparently move shards to control load".
- Reference [6] is a 2015 video talk, not a paper. See §5 for everything else known about ZippyDB.
- Related-work summary: "Tectonic metadata is built on a sharded key-value store, which only provides within-shard strong consistency and no cross-shard operations. These limitations have not been a problem in practice." Colossus, by contrast, uses Spanner [TEC §7, p. 228].

### 1.6 Consistency guarantees and how Tectonic copes without cross-shard transactions [TEC §3.3, p. 221]

**What is guaranteed.** Tectonic relies on "the key-value store's strongly-consistent operations and atomic read-modify-write in-shard transactions". It guarantees **read-after-write consistency** for:
- data operations (appends, reads);
- file and directory operations involving a single object (create, list);
- move operations whose source and destination are in the same parent directory.

"Files in a directory reside in the directory's shard (Table 1), so metadata operations like file create, delete, and moves within a parent directory are consistent."

**Non-atomic operations:**
- "Tectonic provides non-atomic cross-directory move operations."
- **Directory move to a parent on a different shard** is two-phase: "First, we create a link from the new parent directory, and then delete the link from the previous parent. The moved directory keeps a backpointer to its parent directory to detect pending moves. This ensures only one move operation is active for a directory at a time."
- **Cross-directory file move** is a copy followed by a delete from the source directory. "The copy step creates a new file object with the underlying blocks of the source file, avoiding data movement."

**Race handling example, quoted step by step.** The scenario: rename f1 to f2 in directory d while a concurrent create reuses the name f1 (creates overwrite existing files).

- Rename:
  - R1: get file ID fid for f1 (Name, shard(d)).
  - R2: add f2 as an owner of fid (File, shard(fid)).
  - R3: create f2 -> fid and delete f1 -> fid "in an atomic transaction" (Name, shard(d)).
- Create with overwrite:
  - C1: create new file ID fid_new (File, shard(fid_new)).
  - C2: map f1 -> fid_new; delete f1 -> fid (Name, shard(d)).

If C1 and C2 run between R1 and R3, then R3 would erase the new mapping. The fix: "Rename step R3 uses a within-shard transaction to ensure that the file object pointed to by f1 has not been modified since R1." This is a compare-and-swap guard inside the directory's shard.

**Garbage collectors.** "A garbage collector between each metadata layer cleans up (acceptable) metadata inconsistencies" left by failed multi-step Client Library operations and by **lazy object deletion**, "a real-time latency optimization that marks deleted objects at delete time without actually removing them" [TEC §3.5, p. 221].

### 1.7 Client Library and single-writer semantics [TEC §3.4, p. 221]

**Client Library:**
- It "executes reads and writes at the chunk granularity, the finest granularity possible in Tectonic".
- It "replicates or RS-encodes data and writes chunks directly to the Chunk Store". It "reads and reconstructs chunks from the Chunk Store", consults the Metadata Store to locate chunks, and updates the Metadata Store for filesystem operations.

**Single writer per file:**
- "Tectonic simplifies the Client Library's orchestration by allowing a single writer per file." This lets the library write replicas in parallel and hedge writes. "Tenants needing multiple-writer semantics can build serialization semantics on top of Tectonic."
- **Enforcement:** "Tectonic enforces single-writer semantics with a write token for every file."
  - "Any time a writer wants to add a block to a file, it must include a matching token for the metadata write to succeed."
  - The token is added to the file metadata when a process opens the file for appending.
  - "If a second process attempts to open the file, it will generate a new token and overwrite the first process's token, becoming the new, and only, writer for the file. The new writer's Client Library will seal any blocks opened by the previous writer in the open file call."
- **Appends:** "Blocks can only be appended to by the writer that created the block" [TEC §5.2, p. 224].
- **Not described in TEC. UNVERIFIED:** any lease, timeout or expiry on write tokens, and how an abandoned writer's unsealed blocks are found if no new writer ever opens the file. The mechanism is *token-stealing*, not a lease.

### 1.8 Data path A: data warehouse full-block RS writes and hedged quorum writes [TEC §5.1, pp. 223-224]

**Visibility.** "the file is visible to readers only once the creator closes the file. The file is then immutable for its lifetime."

**Full-block, RS-encoded asynchronous writes:**
- Applications "buffer writes up to the block size", "RS-encode blocks in memory and write the data chunks to storage nodes".
- Why RS instead of replication: it saves space, network and disk IO. "More IOPS are needed to write chunks to 15 disks in RS(9,6), but each write is small and the total amount of data written is much smaller than with replication... block sizes are large enough that disk bandwidth, not IOPS, is the bottleneck for full-block writes."
- Blocks of a file are written "asynchronously in parallel". "Once the blocks of the file are written, the file metadata is updated all together. There is no risk of inconsistency with this strategy because a file is only visible once it is completely written."

**Hedged quorum writes with reservation requests:**
- "Instead of sending the chunk write payload to extra nodes, Tectonic first sends reservation requests ahead of the data and then writes the chunks to the first nodes to accept the reservation." This avoids sending data to nodes that would reject it for lack of resources or because the requester exceeded its resource share on that node.
- **Exact numbers for RS(9,6):**
  - "the Client Library sends a reservation request to **19** storage nodes in different failure domains, four more than required for the write."
  - It writes data and parity chunks "to the first **15** storage nodes that respond".
  - "It acknowledges the write to the client as soon as a quorum of **14 out of 15** nodes return success. If the 15th write fails, the corresponding chunk is repaired offline."
- **Result:** "~20% improvement in 99th percentile latency for RS(9,6) encoded, **72 MB** full-block writes, in a test cluster with 80% throughput utilization" (Fig. 3a). "The hedging step is more effective when the cluster is highly loaded."
- **DERIVED:** a block acknowledged at 14/15 chunks can still lose 5 more chunks before becoming unrecoverable under RS(9,6). RS(9,6) needs any 9 of 15.
- **UNVERIFIED (not stated):**
  - hedging parameters for other codes (extra reservations; quorum for RS(10,4), RS(3,3) or replication);
  - how the reservation fan-out interacts with the copyset the Block layer hands out (§1.10);
  - whether chunk locations are recorded in the Block layer before or after the writes complete.

### 1.9 Data path B: blob storage quorum appends and re-encoding [TEC §5.2, pp. 224-225]

**Log-structured blob files.** Facebook stores "tens of trillions of blobs". "Tectonic manages the size of blob storage metadata by storing many blobs together into log-structured files, where new blobs are appended at the end of a file. Blobs are located with a map from blob ID to the location of the blob in the file."

**UNVERIFIED:** where this blob-ID -> location map is stored and how it is structured. TEC does not say. It appears to belong to the blob-storage tenant, not to Tectonic.

**Why partial-block appends.** "Blobs are usually much smaller than Tectonic blocks", so new blobs are written "as small, replicated partial block appends for low latency". These appends "need to be read-after-write consistent so blobs can be read immediately after successful upload."

**Quorum append.** "the Client Library acknowledges a write after a subset of storage nodes has successfully written the data to disk, e.g., two nodes for three-way replication." The temporary drop in durability is acceptable "because the block will soon be reencoded and because blob storage writes a second copy to another datacenter."

**Consistency rule** (this is the core protocol):
- The problem: "straggler appends could leave replica chunks at different sizes."
- "Blocks can only be appended to by the writer that created the block."
- "Once an append completes, Tectonic commits the post-append block size and checksum to the block metadata **before** acknowledging the partial block quorum append."
- The resulting invariant: "If block metadata reports a block size of S, then all preceeding [sic] bytes in the block were written to at least two storage nodes. Readers will be able to access data in the block up to offset S."

**Performance.** Blob read and write latency is "comparable to Haystack" (Figs. 3b and 3c). The text gives figures only, no numeric values.

**Re-encoding.**
- "Directly RS-encoding small partial-block appends would be IO-inefficient... (e.g, 14 IOs with RS(10, 4) instead of 3)."
- "the Client Library reencodes the block from replicated form to RS(10,4) encoding once the block is sealed. Reencoding is IO-efficient... requiring only a single large IO on each of the 14 target storage nodes."

**UNVERIFIED:** the blob-storage block size and seal trigger, and which process runs re-encoding. TEC only says "the Client Library".

### 1.10 Background services, repair and copysets [TEC §3.5, pp. 221-222]

**Structure.** Background services "maintain consistency between metadata layers, maintain durability by repairing lost data, rebalance data across storage nodes, handle rack drains, and publish statistics about filesystem usage". They "are layered similar to the Metadata Store, and they operate on one shard at a time."

**Named services** [Fig. 2, p. 219]:
- garbage collectors
- rebalancer
- stat service
- disk inventory
- block repair/scan
- storage node health checker

**Only the GC, the rebalancer, repair and copysets are described in the text.** How disk inventory, the block scanner, the health checker and the stat service work is **UNVERIFIED** (named only).

**Rebalancer and repair:**
- "A rebalancer and a repair service work in tandem to relocate or delete chunks."
- The rebalancer identifies chunks to move "in response to events like hardware failure, added storage capacity, and rack drains".
- "The repair service handles the actual data movement by reconciling the chunk list to the disk-to-block map for every disk in the system." It scales out by working "on a per-Block layer shard, per-disk basis, enabled by the reverse index".

**Copysets:**
- A copyset is the set of disks holding one block's chunks. For an RS(10,4) block it is 14 disks. The concept comes from Cidon et al., ATC '13, which this note did not review.
- The tradeoff: too many copysets raises unavailability risk when disk failures spike; too few raises reconstruction load on peer disks.
- The Block layer and the rebalancer "each keep in memory about one hundred consistent shuffles of all the disks in the cluster".
- "The Block Layer forms copysets from contiguous disks in a shuffle. On a write, the Block Layer gives the Client Library a copyset from the shuffle corresponding to that block ID."
- The rebalancer tries to keep a block's chunks in that copyset. "Copysets are best-effort, since disk membership in the cluster changes constantly."

**Placement philosophy.** "Tectonic explicitly maps chunks to storage nodes, allowing controlled migration." This is contrasted with Ceph and FDS, which locate data by hashing. There, failures force "frequent updates to the hash-to-location map", and Ceph "lacks support for controlled data migration" [TEC §7, p. 228].

### 1.11 Multitenancy [TEC §4, pp. 222-223]

**Resource types:**
- **Non-ephemeral: storage capacity.** Managed per tenant with "a predefined capacity quota with strict isolation". There is "no automatic elasticity". Reconfiguration is manual but causes no downtime. Tenants distribute capacity among their own applications [§4.1, p. 222].
- **Ephemeral: "Storage IOPS capacity and metadata query capacity"** [§4.1, p. 222].

**TrafficGroups and TrafficClasses** [§4.1, p. 222]:
- Ephemeral resources are managed "within each tenant at the granularity of groups of applications", called TrafficGroups. The goal is to reduce the cardinality of the sharing problem: managing hundreds of individual applications would be "too complex and resource-intensive".
- "Tectonic supports around **50 TrafficGroups per cluster**."
- Each TrafficGroup has a **TrafficClass**: **Gold, Silver or Bronze**, corresponding to latency-sensitive, normal and background applications.
- Each tenant gets a guaranteed quota of ephemeral resources, subdivided among its TrafficGroups.
- **Surplus order:**
  1. The tenant's own TrafficGroups, by descending TrafficClass.
  2. Then TrafficGroups of other tenants, by descending TrafficClass.
- "When one TrafficGroup uses resources from another TrafficGroup, the resulting traffic gets the minimum TrafficClass of the two TrafficGroups." This keeps the per-class traffic ratio stable on each node.

**Global enforcement, client side** [§4.1, p. 223]:
- The Client Library rate limiter uses "high-performance, near-realtime distributed counters to track the demand for each tracked resource in each tenant and TrafficGroup in the last small time window" and "implements a modified leaky bucket algorithm".
- It checks for spare capacity in order: own TrafficGroup, then other TrafficGroups of the same tenant, then other tenants, respecting TrafficClass priority.
- Otherwise the request "is delayed or rejected depending on the request's timeout". Throttling at the client puts backpressure on clients "before they make a potentially wasted request".

**Local enforcement, storage and metadata nodes** [§4.1, p. 223]:
- A weighted round-robin (WRR) scheduler "provisionally skips a TrafficGroup's turn if it will exceed its resource quota".
- Storage nodes protect Gold latency three ways:
  1. A lower-class request may cede its turn to a higher class "if the request will have enough time to complete after the higher-TrafficClass request".
  2. A per-disk limit on in-flight non-Gold IOs; non-Gold traffic is blocked when Gold requests are pending and the limit is reached.
  3. Scheduling of non-Gold requests to a disk stops once a Gold request has been pending there for a threshold time. This compensates for the disk reordering IOs itself.
- **UNVERIFIED (not given):** the window length, counter implementation, in-flight limits and threshold values.

**Access control** [§4.2, p. 223]:
- Authorization is token-based and capability-like. Tokens carry the resources they grant, following Lewi et al., IACR ePrint 2018/413.
- "An authorization service authorizes top-level client requests (e.g., opening a file), generating an authorization token for the next layer in the filesystem; each subsequent layer likewise authorizes the next layer."
- Verification happens "entirely in memory; verification can be performed in tens of microseconds". Tokens are piggybacked on existing protocols.

### 1.12 Production numbers [TEC §6, pp. 225-226]

**Table 2** (one representative multitenant cluster) [p. 225]:

| Capacity | Used bytes | Files | Blocks | Storage nodes |
|---|---|---|---|---|
| 1590 PB | 1250 PB | 10.7 B | 15 B | 4208 |

- The text says used bytes are "~70% of the cluster capacity". **DERIVED:** 1250/1590 = 78.6%. **The paper is internally inconsistent here.**
- **DERIVED:**
  - ~1.4 blocks per file (15 B / 10.7 B).
  - ~83 MB of "used bytes" per block. Whether "used bytes" are logical or physical is not stated, so treat this as an order of magnitude only.
  - ~378 TB of capacity per node.

**Tenant mix and consolidation** [§6.2, p. 225]:
- Blob storage uses ~49% of used space and data warehouse ~51%.
- Data warehouse has "large, regular load spikes"; blob storage traffic is "smooth and predictable".
- **Disk time is the bottleneck resource**, because "neither IOPS nor bandwidth can fairly account for disk IO usage". Worked example: 10 IOs at 50 ms each means the disk was busy 500 of 1000 ms.
- Table 3 (normalized disk-time demand vs supply):

| | Supply | Peak 1 | Peak 2 | Peak 3 |
|---|---|---|---|---|
| Warehouse | 0.51 | 0.60 | 0.54 | 0.57 |
| Blob storage | 0.49 | 0.12 | 0.14 | 0.11 |
| Combined | 1.00 | 0.72 | 0.68 | 0.68 |

- Handling warehouse peaks alone "would have needed ~17% overprovisioning". Consolidation instead absorbs them with blob storage's stranded disk time.

**Metadata hotspots** [§6.3, pp. 225-226]:
- "each shard can serve a maximum of **10 KQPS**". The limit is imposed by the isolation mechanism on metadata nodes.
- All File and Block shards stayed below it. "around 1% of Name layer shards hit the QPS limit because they hold very hot directories". The unhandled requests "are retried after a backoff".
- "Each higher layer has a larger distribution of QPS per shard because it colocates more of a tenant's operations."
- Range partitioning (as in ADLS) "would colocate many more of a tenant's operations together and result in much larger load spikes". Warehouse jobs "often read many similarly-named directories".
- **Co-design with the warehouse:** "Tectonic's list-files API returns the file IDs along with the file names in a directory". Workers can then open files by ID "without querying the directory shard again". This avoids the anti-pattern where an orchestrator lists a directory and many workers then open its files concurrently.
- About two-thirds of metadata operations are served by the Block layer, and hash partitioning spreads them evenly [TEC §3.3, p. 220].

**Caching sealed metadata** [TEC §3.3, p. 220]:
- "Tectonic allows blocks, files, and directories to be sealed." Directory sealing is not recursive; it only prevents adding objects in the immediate level.
- Sealed metadata "can be cached at metadata nodes and at clients without compromising consistency". The exception is the block-to-chunk mapping, because "chunks can migrate among disks". "A stale Block layer cache can be detected during reads, triggering a cache refresh."

### 1.13 Tradeoffs, lessons learned and limitations [TEC §6.4-§6.7, pp. 226-228]

**Reconstruction storms** [§6.4, p. 226]:
- Reconstruction reads need 10x the IOs of direct reads under RS(10,4). The reconstructed fraction is hard to predict because it is triggered by both failures and overload. Overloaded nodes cause direct reads to fail, which triggers more reconstructions, and the cascade is a "reconstruction storm".
- Striped RS would avoid storms but make normal reads expensive.
- "We instead prevent reconstruction storms by **restricting reconstructed reads to 10% of all reads**." This is "typically enough to handle disk, host, and rack failures".

**Direct access vs proxies** [§6.4, pp. 226-227]:
- Direct client-to-storage-node access "is vastly more network- and hardware resource efficient than a proxy design, avoiding an extra network hop for terabytes of data per second". The cost: "bugs in the library become bugs in the application binary".
- For geographically remote clients, "remote requests get forwarded to a stateless proxy in the same datacenter as the storage nodes."

**Higher metadata latency** [§6.5, p. 227]:
- HDFS keeps metadata in memory on one node. In Tectonic, "a file open operation will interact with the Name and File layers".
- Compute engines had to parallelize previously sequential per-file renames.

**Hash-partitioning limits** [§6.5, p. 227]:
- "listing directories recursively involves querying many shards. In fact, **Tectonic does not provide a recursive list API**; tenants need to build it as a client-side wrapper over individual list calls."
- "Tectonic does not have du". Per-directory usage is aggregated periodically and "can be stale".

**Lessons** [§6.6, p. 227]:
- **Block groups abandoned:**
  - The first Chunk Store grouped blocks with the same redundancy scheme, RS-encoded them as one unit, and mapped each group to a set of storage nodes to cut metadata.
  - "with only 5% of storage nodes unavailable, 80% of the block groups became unavailable for writes."
  - Block groups also "precluded optimizations like hedged quorum writes and quorum appends".
- **Name and File layers were originally combined**: "clients consulted the same shards for directory lookups and for listing blocks in a file", which "resulted in unavailability from metadata hotspots".
- **Memory corruption "is a regular occurrence"**:
  - Checksums are enforced "within and between process boundaries".
  - For a transform D' = F(D), integrity is checked by computing G(D'), the inverse transform, and comparing its checksum with C_D. G may be expensive (e.g., RS decoding or decryption) but "it is an acceptable cost".
  - "All API boundaries involving moving, copying, or transforming data had to be retrofitted to include checksum information."

**Non-users** [§6.7, pp. 227-228]:
- Bootstrap services, which must have no dependencies; Tectonic depends on the KV store, config management and so on.
- Graph storage, because "Tectonic is not yet optimized for key-value store workloads which often need the low latencies provided by SSD storage".
- Geo-replication "is a separate problem that Tectonic delegates to its large tenants".

### 1.14 Supplementary peer-reviewed sources on Tectonic (block/chunk sizes)

**TSHIFT §2 [p. 435]** (peer-reviewed; several authors are Tectonic authors):
- "Files are divided into blocks (**typically 72 MiB**) representing a logical array of bytes. Tectonic further divides blocks into smaller chunks (**typically 8 MiB**) and durably encodes each via replication or Reed-Solomon (RS) encoding."
- "The Client Library obtains chunk mappings and any directory and file metadata (e.g., directory ls) via queries to a hash-sharded Metadata Layer built on ZippyDB."
- Warehouse and ML readers "coalesce reads into large O(1MB)-sized IOs" to limit HDD seeks.
- **DERIVED:** 72 MiB / 9 data chunks = 8 MiB, which is consistent with RS(9,6).

**TSHIFT §3.1 [p. 435]:** it mentions "1.6EB of disks assuming RS(9, 6)" for the training-data tier. This confirms RS(9,6) as the default warehouse code.

**TSHIFT §4.1-§4.2 [p. 438]:**
- Training data "is stored in immutable, sealed blocks", which lets the flash cache avoid invalidation.
- "Blocks are typically 72 MiB".
- The read path decomposes a `pread` into block reads "by querying the Tectonic File Layer".

**BALEEN §2.1 [p. 348]:** "(Tectonic has 8 MB blocks and 128 kB segments.)" In BALEEN's own definition, a block is the unit "mapped to a location on backing HDDs" and segments are cacheable sub-units.
- **Terminology conflict (DERIVED interpretation).** BALEEN's "8 MB block" matches TSHIFT's "8 MiB chunk", the unit on one disk, not TSHIFT's 72 MiB block.
- **Resolution for this record:** logical block = 72 MiB, chunk = 8 MiB, cache segment = 128 kB.

### 1.15 Tectonic facts we could NOT verify from primary sources

**Not stated anywhere in TEC:**
- the default block size (only the 72 MB experiment; TSHIFT supplies "typically 72 MiB");
- the chunk size (TSHIFT supplies "typically 8 MiB");
- HDD capacity (only derived, about 10.5 TB);
- blob-storage block size and seal policy;
- the location and format of the blob-ID index;
- ZippyDB shard sizes for Tectonic;
- the fields of `*_info` values;
- write-token lease/timeout semantics;
- disk inventory, block scanner and health checker behavior;
- rate-limiter parameters;
- hedging parameters for codes other than RS(9,6);
- whether copyset choice constrains the 19-node reservation set.

### 1.16 Implications for mantle (Tectonic)

**Metadata model:**

- **Recommendation M1: adopt three separately hash-partitioned layers.** Use Name, File and Block layers. Do not start with a combined Name+File layer: Tectonic's first version did, and it caused hotspot unavailability [TEC §6.6]. Do not range-partition the Name or File layer [TEC §3.3, §6.3].
- **Recommendation M2: mantle logical key schema.** This is an adaptation of TEC Table 1. Rows marked *mantle choice* fill gaps TEC leaves unspecified.

| Layer | Logical key | Shard key | Value (minimum fields) | Grounding |
|---|---|---|---|---|
| Name | (dir_id, child_name) | dir_id | kind (dir/object), child_id; for sealed objects, immutable listing attributes (size, checksum/ETag, timestamps) | TEC Table 1; §6.3 (list returns IDs); §3.3 (sealed metadata is cacheable) |
| File header (*mantle choice*) | (file_id) | file_id | owners (name back-references), write_token, state (open/sealed), length, durability policy | TEC §3.3 R2 ("owner"), §3.4 (token "in the file metadata"); exact key UNVERIFIED in TEC |
| File blocks | (file_id, block_index) | file_id | blk_id, logical offset, length, checksum | TEC Table 1 `(file_id, blk_id) -> blk_info`; block_index for ordered prefix scans is a *mantle choice* |
| Block forward | (blk_id) | blk_id | encoding (RS(k,m) or Rep(n)), committed size S, checksum, sealed flag, chunk locations in chunk order | TEC Table 1; §5.2 (size and checksum committed before ack) |
| Block reverse | (disk_id, blk_id) | **blk_id** | chunk_info: chunk index, length, checksum | TEC Table 1; §3.5 (per-shard, per-disk repair) |

- **Recommendation M3: physical key encoding.** *INFERENCE.* "Sharded by X" but "prefix-scanned by Y" needs the shard to be chosen independently of the key's sort prefix, as with the `(disk_id, blk_id)` row sharded by blk_id. Prefix every physical key with a virtual-shard number `vshard = H(shard_key) mod V`, then a layer tag, then the logical key. Physical shards own vshard ranges.
  - This reproduces hash partitioning, keeps co-sharded rows together, and supports in-shard prefix scans on any KV store.
  - It allows resharding without rehashing. The same idea appears as ZippyDB microshards [ZDB-BLOG, NON-PEER-REVIEWED], Akkio's µ-shards [AKKIO §3, p. 449] and the logical/physical splits in Haystack and Ambry [HAY §3.1, p. 50; AMB §2.2, p. 254].
- **Recommendation M4: required KV contract.** This is what Tectonic assumes of ZippyDB [TEC §3.3]:
  - per-shard linearizable reads and writes;
  - **atomic multi-key read-modify-write transactions within a shard**, including compare-and-swap guards such as rename step R3;
  - ordered prefix scans;
  - Paxos- or Raft-replicated shards small enough that "each metadata node can host several shards" (parallel recovery);
  - transparent shard moves for load balancing.

  Cross-shard transactions are **not required** if mantle accepts Tectonic's non-atomic cross-directory operations plus inter-layer GC [TEC §3.3, §3.5, §7].
- **Recommendation M5: make an overwriting PUT a Tectonic "create with overwriting".** *INFERENCE* from TEC §3.3 C1/C2 and §3.5:
  1. Create a new file_id (File shard).
  2. Write data.
  3. In one Name-shard transaction, map name -> new file_id and drop the old mapping.
  4. Lazy-GC the old file object and its blocks.

  This makes the last committed mapping win without torn state. Any multi-step name operation, such as a copy-then-delete "move", must use R3's guard: an in-shard transaction checking that the name still points to the file_id read earlier.
- **Recommendation M6: seal on completion and cache aggressively.** *INFERENCE* from TEC §3.3. Seal files (objects) when the upload completes, then cache Name and File metadata at gateways and metadata nodes. Treat cached block-to-chunk locations as hints; on a stale read, refresh from the Block layer.
- **Recommendation M7: plan for per-shard QPS caps** (Tectonic uses 10 KQPS) with client retry and backoff [TEC §6.3]. Keep the Name layer's per-directory listing as the unit of hotness. Return object IDs and immutable attributes in list responses so follow-up reads skip the directory shard [TEC §6.3].

**Data path:**

- **Recommendation D1: two write paths selected per request** [TEC §5]:
  - Large or streaming objects use full-block RS; see D2.
  - Small objects use quorum appends into log-structured files; see D3.
  - The size cut-over is **UNVERIFIED**. No source gives one, so determine it by benchmarking. For context, Ambry found reads under ~200 KB are seek-dominated [AMB §6.1.4, p. 260].
- **Recommendation D2: full-block write protocol** (after TEC §5.1; ordering details are *INFERENCE*):
  1. The client or gateway buffers one logical block (default **72 MiB** [TSHIFT §2]).
  2. It RS(9,6)-encodes the block into **8 MiB** chunks [TSHIFT §2; TEC §5.1].
  3. It sends reservation requests to k+m+4 storage nodes in distinct failure domains, writes to the first k+m that accept, and acknowledges the block at k+m-1 successes [TEC §5.1: 19/15/14 for RS(9,6)].
  4. It records chunk locations and reverse entries in the Block layer. Both live in the block's own shard [Table 1].
  5. After all blocks are written, it commits the File-layer block list in one File-shard transaction that checks the write token [TEC §3.4, §5.1].
  6. It commits the Name mapping (M5).

  Blocks are written in parallel. The object is invisible until step 6 [TEC §5.1]. Pad a short final block to equal chunks, as TEC §3.2 does, or send small objects through D3.
- **Recommendation D3: small-object append protocol** (after TEC §5.2):
  1. Each gateway process owns its own open log file and open block. This gives single-writer semantics with no contention [TEC §3.4, §5.2].
  2. Append a self-describing record (see the Haystack needle format, §3.4) to 3 replica chunks, and wait for 2 durable on-disk acknowledgements.
  3. Commit the new block size S and checksum to the Block layer.
  4. Only then commit the object's Name entry with its locator (blk_id, offset, length).
  5. Readers never read past S.
  6. When the block fills, seal it and re-encode to RS(10,4) with one large IO per target node [TEC §5.2].

  *INFERENCE:* a crash between steps leaves only unreferenced bytes (garbage) and never a dangling name.
- **Recommendation D4: layout and reconstruction.** Use a contiguous (not striped) RS layout so sub-chunk reads are one IO. Enforce a global reconstruction-read budget; Tectonic uses 10% [TEC §6.4]. Online reconstruction should decode only the requested byte range, as f4 does [F4 §5.3, p. 389].
- **Recommendation D5: block and chunk sizes.** Default to a 72 MiB logical block with 8 MiB chunks for RS(9,6) [TSHIFT §2]. Keep block size and encoding **per-write configurable**, as Tectonic does per call [TEC §5]. Two independent systems chose ~8 MB transfer and storage units: Tectonic's chunks and Ambry's 4-8 MB chunks [AMB §4.2.1]. f4's 1 GB blocks traded smaller metadata against larger rebuild cost [F4 §5.3].

**Chunk store:**

- **Recommendation C1: storage nodes store chunks only.** A node stores each chunk as one file on a local filesystem (XFS in Tectonic). Its API is get, put, append, delete, list and scan. It is oblivious to blocks and files and enforces local fair sharing [TEC §3.2]. Put filesystem metadata and a hot-chunk cache on SSD if available [TEC §3.2].
  - *DERIVED sizing:* 10.5 TB / 8 MiB = ~1.25 M chunk files per full HDD.
  - Haystack's warning about per-object files applies to KB-sized photos [HAY §2.3], not to MB-sized chunks.
- **Recommendation C2: explicit placement and copysets.**
  - Store chunk locations explicitly in the Block layer; no CRUSH-style computed placement [TEC §7].
  - Use copysets drawn from about 100 consistent disk shuffles, keyed by block ID [TEC §3.5].
  - Do not group blocks into fixed node sets [TEC §6.6].
  - Place each chunk of a block in a distinct fault domain [TEC §3.2, §5.1]. The 19-way reservation implies at least 19 fault domains; f4 required at least n+k racks [F4 §5.5, p. 390].
- **Recommendation C3: stateless background services, run per metadata shard** [TEC §3.5]:
  - inter-layer GC (including lazy deletion);
  - rebalancer plus repair, working per Block shard and per disk through the reverse index;
  - disk inventory;
  - block scan (scrubbing);
  - node health checker;
  - usage-statistics aggregation. S3 bucket usage can be a periodic aggregate, as Tectonic does for directories [TEC §6.5].

  Run all background IO in the lowest TrafficClass [TEC §4.1].

**Tenancy, security, integrity:**

- **Recommendation T1: implement Tectonic's resource model** [TEC §4.1, §6.2]:
  - tenant = account, with a static capacity quota;
  - tens of TrafficGroups;
  - Gold/Silver/Bronze TrafficClasses;
  - client/gateway-side distributed-counter leaky bucket with the surplus-borrowing order and minimum-class rule;
  - node-side WRR with the three Gold protections;
  - IO accounted in disk time.

  Parameter values must be chosen by mantle; they are **UNVERIFIED** in TEC.
- **Recommendation T2: layer-chained capability tokens.** The S3 gateway verifies the S3 request, then mints per-layer capability tokens that metadata and storage nodes verify in memory [TEC §4.2].
- **Recommendation T3: end-to-end checksums from day one.** Carry checksums across every API boundary. Verify EC encoding (and any encryption) by decoding before acknowledging [TEC §6.6]. Tectonic had to retrofit this.
- **Recommendation T4: gateways as in-datacenter proxies.** mantle's S3 gateways *are* the in-datacenter stateless proxies that TEC §6.4 uses for remote clients. Co-locate the "client library" logic in the gateway. Consider a native Rust client for internal bulk consumers who can reach storage nodes directly [TEC §6.4].

---

## 2. f4 (Muralidhar et al., OSDI '14)

### 2.1 Context and workload facts

- **Scale.** f4 "currently stores over 65PBs of logical BLOBs and reduces their effective-replication-factor from 3.6 to either 2.8 or 2.1" [F4 Abstract, p. 383]. It had been in production "for over 19 months" and "saves over 53PB" [F4 §1, p. 384]. Facebook stored over 400 billion photos as of Feb 2014 [F4 §1, p. 383].
- **Temperature.**
  - "the request rate for week-old BLOBs is an order of magnitude lower than for less-than-a-day old content for eight of nine examined types" [F4 §1, p. 383].
  - "content less than one day old receives more than 100 times the request rate of one-year-old content" [F4 §3, p. 385].
- **Disk throughput.** "The 4TB disks used in f4 can deliver a maximum of 80 Input/Output Operations Per Second (IOPS) while keeping per-request latency acceptably low". This gives the warm-storage ceiling of **20 IOPS/TB** [F4 §3, p. 385].
- **Warm cutoff.**
  - One month is the hot/warm boundary for all but two types: Profile Photos are never moved and Photos use three months [F4 §3, p. 386].
  - In production, "an approximately 3-month cutoff for all types" is used for simplicity [F4 §6.2 footnote 4, p. 393].
  - More than 89% of objects were warm in the most recent interval [F4 §3, p. 386].
- **Deletes.** Deletion rate is correlated with age: "most deletes are for young BLOBs" [F4 §3, p. 386].

### 2.2 Volumes: locking (sealing) and file structure [F4 §4, pp. 386-387]

- "Volumes are initially unlocked and support reads, creates (appends), and deletes. Once volumes are full, at around **100GB** in size, they transition to being **locked** and no longer allow creates. Locked volumes only allow reads and deletes."
- Each volume has three files:
  - a **data file** (each BLOB "along with associated metadata such as the key, the size, and checksum");
  - an **index file** ("a snapshot of the in-memory lookup structure", used to rebuild in-memory indexes after reboot);
  - a **journal file** (tracks deleted BLOBs; new relative to the published Haystack, which updated the data and index files directly).
- For locked volumes the data and index files are read-only and the journal is read-write.
- **Controller:** provisions store machines, maintains a pool of unlocked volumes, ensures logical volumes have enough physical volumes, and runs compaction and GC [F4 §4.1, p. 387].
- **Router tier:**
  - Stateless machines with soft-state copies of the logical-to-physical volume mapping. The canonical copy is in a separate database.
  - **Reads:** the router extracts the logical volume id from the BLOB id and picks a physical volume, usually the closest; on timeout it tries the next.
  - **Creates:** the router "sends the BLOB out to all physical volumes for that logical volume". On any error, partial data "is ignored to be garbage collected later, and a new logical volume is picked for the create".
  - **Deletes:** issued to all replicas and retried until complete [F4 §4.1, p. 387].
- **Haystack as described in 2014:**
  - Only "~100" files per host.
  - Typically 3 physical volumes per logical volume, each holding "up to millions of immutable BLOBs" and growing to ~100GB.
  - A read is one in-memory lookup plus "a single I/O request to the data file".
  - A create "synchronously appends a record ... updates the in-memory hash tables, and synchronously updates the index and journal files".
  - Fault tolerance: two replicas on different racks in a primary datacenter, a third in another datacenter, plus RAID-6. That gives 3 x 1.2 = 3.6 [F4 §4.1, pp. 387-388].

### 2.3 How Haystack volumes become f4 volumes (what is and is not stated)

**Stated:**
- Haystack "aggregates newly created BLOBs into volumes and stores them until their request and delete rates have cooled off enough to be migrated to f4" [F4 §1, p. 383].
- f4 cells hold only **locked** volumes [F4 §5.3, p. 388].
- "When a volume is migrated from the hot storage system to the warm storage system it temporarily resides in both while the canonical mapping is updated and then client operations are transparently directed to the new storage system" [F4 §4.1, p. 387].
- In f4:
  - the data file is RS-encoded;
  - the index file is triple-replicated;
  - "The haystack journal files that track deletes are not present in f4". Deletes use per-BLOB encryption keys instead (§2.5) [F4 §5.3, pp. 388-389].
- Volumes mix BLOB ages and types, so "the volume may still be migrated to f4" if its overall temperature is low enough [F4 §5.6, p. 391].

**UNVERIFIED, not described in F4:**
- the mechanics of conversion: which component RS-encodes, and whether data is streamed or copied;
- whether BLOBs already deleted in Haystack are dropped or compacted before encoding;
- how Haystack's journal state is reconciled with the per-BLOB key store;
- how the location-map is built.

### 2.4 f4 cell design [F4 §5.2-§5.3, pp. 388-390]

**Cell shape:**
- A cell is one datacenter of homogeneous hardware. "Current cells use **14 racks of 15 hosts** with **30 4TB drives per host**." A cell is the unit of acquisition and deployment [F4 §5.2, p. 388]. **DERIVED:** 14 x 15 x 30 x 4 TB = 25.2 PB raw per cell.
- **Notation.** "A Reed-Solomon(n, k) code encodes n bits of data with k extra bits of parity, and can tolerate k failures, at an overall storage size of n + k." This is data-then-parity, the same convention as Tectonic [F4 §5.2, p. 388].

**Index and data files:**
- **Index files are triple-replicated within a cell.** They "are small enough that the storage gain from encoding them is too small to be worth the added complexity" [F4 §5.3, p. 388].
- **Data file encoding:**
  - "Recent f4 cells use n = 10 and k = 4."
  - "The file is logically divided up into contiguous sequences of n blocks, each of size b. For each such sequence of n blocks, k parity blocks are generated, thus forming a logical stripe of size n + k blocks." The other blocks of a stripe are its "companion blocks".
  - A non-multiple file is zero-padded.
  - "A subset of a block, corresponding to a BLOB, can also be decoded from only the equivalent subsets of any n of its companion and parity blocks" [F4 §5.3, pp. 388-389].
- **Block size:** "The block-size for encoding is chosen to be a large value—**typically 1 GB**—for two reasons. First, it decreases the number of BLOBs that span multiple blocks and thus require multiple I/O operations to read. Second, it reduces the amount of per-block metadata that f4 needs to maintain. We avoid a larger block size because of the larger overhead for rebuilding blocks it would incur." [F4 §5.3, p. 389]

**Components:**
- **Name node:** maps data and parity blocks to the storage nodes holding them. Uses a primary-backup setup [F4 §5.3, p. 389].
- **Storage nodes:**
  - Expose an **Index API** (existence and location) and a **File API** (data).
  - Keep the index (BLOB -> data file, offset, length) and a per-volume **location-map** (data-file offset -> physical block), both "pinned in memory to avoid disk seeks".
  - **Two-part reads:** validate existence, then redirect the caller to the node holding the data block [F4 §5.3, p. 389].
  - Footnote 2: each volume is owned by exactly one storage node at a time.
- **Backoff nodes:**
  - "storage-less, CPU-heavy nodes that handle the online reconstruction of request BLOBs".
  - They read the same offsets from the n-1 companions and k parity blocks and decode after n responses.
  - "This online reconstruction rebuilds only the requested BLOB, it does not rebuild the full block" ("e.g., 40KB instead of 1GB") [F4 §5.3, pp. 389-390].
- **Rebuilder nodes:**
  - Detect failures by probing, report to coordinators, and rebuild blocks from n companion or parity blocks.
  - "Rebuilder nodes throttle themselves to avoid adversely impacting online user requests" [F4 §5.3, p. 390].
- **Coordinator nodes:** schedule rebuilds and run a **placement balancer** that fixes stripes whose blocks share a failure domain. Rebalancing is also throttled [F4 §5.3, p. 390].

### 2.5 Deletes via per-BLOB encryption keys (no compaction) [F4 §5.3, p. 389; §6.5, p. 394; §7, pp. 394-395]

- "Each BLOB in f4 is encrypted with a per-BLOB encryption key. Deletes are handled outside of f4 by deleting a BLOB's encryption key that is stored in a separate key store, typically a database. This renders the BLOB unreadable and effectively deletes it without requiring the use of compaction in f4."
- The router tier fetches the key in parallel with the read and decrypts, so decryption scales independently of storage.
- **Cost:** f4 "does not reclaim the space of deleted BLOBs". The measured deleted fraction was **6.8%** [F4 §6.5, p. 394].
- **Lesson:**
  - An early f4 kept Haystack-style journal files. "This single read-write file was at odds with the rest of the f4 design, which is read-only." Combined with HDFS's at-most-one-writer requirement and inevitable failures, it "was the foremost source of production issues for f4".
  - Moving delete tracking to another system "simplified f4 by making it fully read-only and fixed the production issues" [F4 §7, pp. 394-395].

### 2.6 Geo-replication with XOR and effective replication factors [F4 §5.4, p. 390]

- **Initial approach:** double-replicated cells in two datacenters, giving 3.6 -> **2.8**.
- **XOR scheme:** "storing the XOR of blocks from two different volumes primarily stored in two different datacenters in a third datacenter". Each block's counterpart in the other volume is its "buddy block". The XOR volumes keep normal triple-replicated index files.
- **Formula:** "The 2.1 replication factor comes from the 1.4X for the primary single cell replication for each of two volumes and another 1.4X for the geo-replicated XOR of the two volumes: (1.4*2+1.4)/2 = 2.1."
- **Reads during a datacenter failure:** a geo-backoff node fetches the region from the local XOR block and the remote XOR-companion block, then reconstructs.
- **Tradeoff:** this is accepted "with the tradeoff of decreased throughput for BLOBs stored at the failed datacenter".
- **Code properties:** the local RS code is MDS, "though the combination with XOR is not" [F4 §8, p. 395].

### 2.7 Placement and fault-tolerance facts [F4 §5.5, pp. 390-391; §6.4, pp. 393-394]

- **Placement:**
  - "A rack is the largest failure domain and is our primary concern."
  - Blocks of a stripe go on different racks, "and at least on a different node". This "requires that a cell have at least n + k racks". Initial placement is best-effort, and the placement balancer corrects violations.
- **Table 1 (component fault tolerance):**

| Component | Strategy |
|---|---|
| Name | primary-backup with 2 backups on different racks |
| Coordinator | same as Name |
| Backoff | soft state only |
| Rebuilder | soft state only |
| Storage index | 3x in the local cell, 3x in the remote cell |
| Storage data | Reed-Solomon in the local cell, XOR in the remote cell |

- **Failure experience:**
  - "an Annualized Failure Rate (AFR) of ~1% for our disks", replaced "in less than 3 business days" [F4 §6.4, p. 394].
  - Worst event: a drill that "rebuilt 2 hosts worth of data (240 TB) in the background over 3 days", raising p99 latency to 500 ms [F4 §6.4, p. 394].
- **Correlated failure:** a bad disk batch plus high temperatures raised AFR "to an AFR over 60% for a period of weeks". There was no data loss because buddy and XOR blocks lived in other, unaffected cells. The lesson: use hardware heterogeneity [F4 §7, p. 395].

### 2.8 Performance and savings [F4 §6, pp. 392-394]

- **Caching stack:** reduces the request rate to ~30% overall, and to ~55% for BLOBs 3+ months old [F4 §6.2, p. 392].
- **Haystack absorbs load:**
  - more than 50% of reads;
  - "over 70% of deletes excluding auto-expiry, and over 80% of deletes including auto-expiry" [F4 §6.2, p. 393].
- **Peak load:** 3.5 (rack), 4.1 (machine) and 8.5 (disk) IOPS/TB, all below the 20 IOPS/TB maximum [F4 §6.3, p. 393].
- **Latency:** median read latency is 14 ms for Haystack vs 17 ms for f4. f4 is "less than 30 ms for 80% of them and 80 ms for 99% of them" [F4 §6.3, p. 393].
- **Savings:**
  - Reduction = (repl_hay - repl_f4 x 1/(1 - del_f4)) x logical = (3.6 - repl_f4 x 1.07) x 65 PB.
  - The prose states savings of "over 39 PB" at 2.8 and "over 87 PB" at 2.1, and "over 53PB" currently.
  - **Discrepancy in the paper:** the equation's own result line reads "30PB at 2.8, 68PB at 2.1". **DERIVED:** evaluating the formula gives 39.3 PB and 87.9 PB, which matches the prose. The equation line appears to be a typo [F4 §6.5, p. 394].
- **Hardware:**
  - Older hosts: 12 x 1/2/3 TB drives.
  - Newer hosts: "Open Vault 2U chassis holding 30 x 3TB/4TB SATA drives".
  - "Haystack uses Hardware RAID-6 with a NVRAM write-back cache while f4 uses these machines in a JBOD" [F4 §6.1, p. 392].
- **Software/hardware co-design:** "The f4 software is designed so the weekly peak load on any drive is less than the maximum IOPS it can deliver" [F4 §5.6, p. 392].
- **Implementation note:** f4 was built on HDFS. Proxied HDFS reads had poor throughput because of thread-per-request IO scheduling; the fix was the two-part read that bypasses proxying [F4 §7, p. 395].

### 2.9 Implications for mantle (f4)

- **Recommendation F1: seal, then re-encode in place.** Use age or seal status as the tiering trigger: write new data replicated and convert sealed containers to wide RS. Tectonic did this inside one system with sealed-block re-encoding [TEC §5.2], removing f4's separate warm cluster. f4's data shows most reads and deletes hit young data [F4 §3, §6.2]. mantle should not need a separate warm system.
- **Recommendation F2: keep small metadata replicated.** Replicate indexes and metadata (3x). Erasure-code only bulk data [F4 §5.3].
- **Recommendation F3: reconstruct only what is needed.** Online: decode only the requested byte range, which works with contiguous RS [F4 §5.3; TEC §6.4]. Offline: rebuild whole chunks in throttled background services [F4 §5.3; TEC §3.5].
- **Recommendation F4: failure domains.** Enforce one chunk per failure domain (rack) per block, with at least k+m racks. Run a placement checker/balancer that repairs violations after failures [F4 §5.5, §5.3].
- **Recommendation F5: deletes on immutable, EC-encoded data.** Consider crypto-shredding with per-object keys to avoid rewriting RS-encoded blocks. The costs: a key store on the read path, decrypt CPU and unreclaimed space (6.8% in f4) [F4 §5.3, §6.5]. Otherwise, reclaim space by rewriting sealed blocks whose live fraction is low. That design is **UNVERIFIED**: no source reviewed here describes it for Tectonic.
- **Recommendation F6: never put one mutable file inside otherwise immutable data.** Keep every mutable piece of state (delete tracking, indexes) in the metadata store [F4 §7].
- **Recommendation F7: capacity planning.** Use ~1% AFR as the baseline, but design repair and placement for correlated batch failures (>60% AFR observed). Diversify hardware batches across failure domains [F4 §6.4, §7].
- **Recommendation F8: geo-replication (future).** XOR-of-buddies across three regions gives 2.1x vs 2.8x for full copies, at the cost of degraded throughput during a region outage [F4 §5.4]. Tectonic leaves geo-replication to tenants [TEC §6.7].

---

## 3. Haystack (Beaver et al., OSDI '10)

### 3.1 Problem and goal

- **Scale:** "over 260 billion images, which translates to over 20 petabytes of data. Users upload one billion new photos (~60 terabytes) each week and Facebook serves over one million images per second at peak." Each upload is stored in four sizes [HAY Abstract, §1, p. 47].
- **NFS/NAS predecessor:**
  - With thousands of files per directory, "it was common to incur more than 10 disk operations to retrieve a single image".
  - With hundreds per directory, it "would still generally incur 3 disk operations": directory metadata, inode, then file contents [HAY §2.2, p. 49].
  - Caching file handles in memcache helped only slightly [HAY §2.2, p. 49].
- **Core insight:** "each file requires at least one inode, which is hundreds of bytes large". Holding all filesystem metadata in memory was not cost-effective, so Haystack "reduces the amount of filesystem metadata per photo" [HAY §2.3, p. 49].
- **Goal:** "Haystack achieves high throughput and low latency by requiring at most one disk operation per read. We accomplish this by keeping all metadata in main memory" [HAY §1, p. 47].
- **Cost:** ~28% less per usable TB and ~4x more reads per second per usable TB than NAS [HAY §1, p. 48].

### 3.2 Architecture [HAY §3.1-§3.3, pp. 50-51]

- **Components:** Store (persistent storage; "the only component that manages the filesystem metadata"), Directory and Cache [HAY §3.1, p. 50].
- **Volumes:**
  - "we can organize a server's 10 terabytes of capacity into 100 physical volumes each of which provides 100 gigabytes of storage".
  - Physical volumes on different machines form a **logical volume**, and a photo written to a logical volume goes to all its physical volumes [HAY §3.1, p. 50].
- **URL:** `http://<CDN>/<Cache>/<Machine id>/<Logical volume, Photo>` [HAY §3.1, p. 50].
- **Upload:** the web server gets a write-enabled logical volume from the Directory, assigns a unique id, and uploads to each physical volume [HAY §3.1, p. 51].
- **Directory:**
  - maps logical to physical volumes;
  - load-balances writes across logical volumes and reads across physical volumes;
  - chooses CDN vs Cache;
  - marks volumes read-only, at machine granularity;
  - is stored in "a replicated database accessed via a PHP interface that leverages memcache" [HAY §3.2, p. 51].
- **Cache:**
  - a DHT keyed by photo id;
  - caches only when (a) the request comes directly from a user, not the CDN, and (b) the photo lives on a **write-enabled** Store machine;
  - rationale: shelter write-enabled machines, since "filesystems for our workload generally perform better when doing either reads or writes but not both" [HAY §3.3, p. 51];
  - hit rate is about 80% [HAY §4.3, p. 55].

### 3.3 Needle and volume layout [HAY §3.4, Fig. 5, Table 1, pp. 51-52]

- A physical volume is "a very large file (100 GB) saved as '/hay/haystack_<logical volume id>'". The machine keeps open file descriptors for every volume [HAY §3.4, p. 51].
- A volume file is "a superblock followed by a sequence of needles". Each needle is one photo [HAY §3.4, p. 52].
- **Needle fields (Table 1):**

| Field | Meaning |
|---|---|
| Header | "Magic number used for recovery" |
| Cookie | "Random number to mitigate brute force lookups" |
| Key | "64-bit photo id" |
| Alternate key | "32-bit supplemental id" (the photo's size/type: 'n', 'a', 's', 't') |
| Flags | "Signifies deleted status" |
| Size | "Data size" |
| Data | "The actual photo data" |
| Footer | "Magic number for recovery" |
| Data Checksum | "Used to check integrity" |
| Padding | "Total needle size is aligned to 8 bytes" |

- **In-memory structure:** per volume, it maps (key, alternate key) -> (flags, size in bytes, volume offset). "After a crash, a Store machine can reconstruct this mapping directly from the volume file before processing requests" [HAY §3.4, p. 52].

### 3.4 Read, write, delete; why one disk op per read [HAY §3.4.1-§3.4.5, pp. 52-53]

- **Read:**
  - The Cache supplies the logical volume id, key, alternate key and cookie.
  - The Store looks up the in-memory map; if the photo is not deleted, it "seeks to the appropriate offset in the volume file, reads the entire needle from disk (whose size it can calculate ahead of time), and verifies the cookie and the integrity of the data" [§3.4.1, p. 52].
- **Write:**
  - Each machine "synchronously appends needle images to its physical volume files and updates in-memory mappings".
  - Overwrites are forbidden. A modification appends a new needle with the same (key, alternate key), and "the latest version of a needle within a physical volume is the one at the highest offset" [§3.4.2, p. 52].
- **Delete:**
  - Sets the delete flag "in both the in-memory mapping and synchronously in the volume file".
  - Space "is for the moment lost" until compaction [§3.4.3, pp. 52-53].
- **Why one IO per read** [§3.4.5, p. 53]:
  - All location metadata (offset, size) is in RAM, with open FDs per volume.
  - The needle size is known, so the whole needle is read in one IO.
  - XFS keeps blockmaps for large contiguous files small enough to cache, and XFS "provides efficient file preallocation, mitigating fragmentation".
  - "There exists corner cases where the filesystem requires more than one disk operation when photo data crosses extents or RAID boundaries. Haystack preallocates **1 gigabyte extents** and uses **256 kilobyte RAID stripe sizes** so that in practice we encounter these cases rarely."

### 3.5 Index file and recovery [HAY §3.4.4, Table 2, p. 53; §3.5, p. 54]

- **Index file:**
  - A per-volume "checkpoint of the in-memory data structures".
  - Layout: a superblock plus one record per needle, "in the same order as the corresponding needles appear in the volume file".
  - **Record fields (Table 2):** Key (64-bit), Alternate key (32-bit), Flags ("Currently unused"), Offset (needle offset in the Store), Size (needle data size).
- **Async update:**
  - "When we write a new photo the Store machine synchronously appends a needle to the end of the volume file and asynchronously appends a record to the index file. When we delete a photo, the Store machine synchronously sets the flag in that photo's needle without updating the index file."
  - Consequences: **orphan** needles (with no index record) exist, and index records "do not reflect deleted photos".
- **Recovery:**
  - Orphans are found quickly because "the last record in the index file corresponds to the last non-orphan needle in the volume file". They are scanned and appended to the index.
  - In-memory maps are then built from the index.
  - A deleted photo is detected after reading the needle's flag, and the in-memory map is corrected.
- **Health checking:**
  - The "pitchfork" background task checks connectivity, volume availability and readability.
  - On consistent failure it marks all of that machine's logical volumes read-only.
  - "Bulk sync" (reset from a replica) happens "a few each month". It takes hours because it is bottlenecked by the NIC [HAY §3.5, p. 54].

### 3.6 Compaction, memory footprint, batching [HAY §3.6, p. 54; §4, pp. 54-58]

- **Compaction** [§3.6.1]:
  - "Compaction is an online operation that reclaims the space used by deleted and duplicate needles."
  - The machine copies needles into a new file, skipping duplicates and deleted entries. "During compaction, deletes go to both files. Once this procedure reaches the end of the file, it blocks any further modifications to the volume and atomically swaps the files and in-memory structures."
  - "Over the course of a year, about 25% of the photos get deleted."
- **Memory** [§3.6.2]:
  - Deleted photos are marked by setting offset = 0, which removes the in-memory flags. Cookies are not kept in memory; they are checked after the disk read. Together these save 20%.
  - "Currently, Haystack uses on average **10 bytes of main memory per photo**." The breakdown: 4 scaled images share a 64-bit key, with 32-bit alternate keys and 16-bit sizes ("32 bytes"), plus about 2 bytes per image of hash-table overhead, so 40 bytes per 4 images. For comparison, "an xfs_inode_t structure in Linux is 536 bytes".
  - **Note (DERIVED):** the itemization (8 + 4x4 + 4x2 = 32 bytes) does not list the offset field explicitly. Treat 10 B/photo as the paper's reported average, not a complete layout.
- **Batching** [§3.6.3; §4.4.2; §4.4.3]:
  - Workloads that group 1, 4 or 16 writes into one "multi-write" show that "amortizing the fixed cost of writes over 4 and 16 images improves throughput by 30% and 78%" [p. 57].
  - Production averages "9.27" images per multi-write [p. 57].
  - Multi-write latency is 1-2 ms [p. 57]. "the NVRAM allows us to write needles asynchronously and then issue a single fsync to flush the volume file once the multi-write is complete" [p. 58].
- **Hardware** [§4.4.1, p. 56]:
  - 2U blade with 2 hyper-threaded quad-core Xeons, 48 GB RAM, a hardware RAID controller with 256-512 MB NVRAM, and 12 x 1 TB SATA drives.
  - About 9 TB usable as RAID-6. NVRAM is reserved for writes and disk caches are disabled.
- **Benchmark:** with random 64 KB reads on 201 volumes, Haystack delivers 85% of raw device throughput at 17% higher latency [§4.4.2, p. 56].
- **Traffic** [Table 3, p. 55]:
  - ~120 M photos uploaded per day.
  - ~1.44 B Haystack photos written per day (12x = 4 sizes x 3 locations).
  - 80-100 B photos viewed.
  - 10 B Haystack photos read.
  - Haystack serves ~10% of CDN-originated photo requests.

### 3.7 Implications for mantle (Haystack)

- **Recommendation H1: never one filesystem object per small object.** Pack small objects into large append-only containers so no per-object inode or metadata IO sits on the read path [HAY §2.3, §3]. In mantle's Tectonic-style design the per-object locator lives in the metadata KV (Name layer). Storage nodes keep only MB-sized chunk files [TEC §3.2; §5.2].
- **Recommendation H2: self-describing record format** for packed small objects: header magic, key/id, flags, length, data, footer magic, checksum, 8-byte alignment [HAY Table 1]. Even with an authoritative metadata store, this makes chunks scannable. That supports scrubbing, GC verification and disaster reconstruction, as Haystack rebuilds its index from the volume file [HAY §3.4, §3.4.4].
- **Recommendation H3: one IO per small read.** Keep each small object inside one chunk; never let it straddle a chunk boundary. Preallocate chunk files to their final size when known (full-block writes know it) to avoid extent fragmentation [HAY §3.4.5]. Precise XFS preallocation behavior should be checked against XFS primary documentation: **UNVERIFIED** here.
- **Recommendation H4: group commit on storage nodes.** Batch appends and share a single flush; Haystack saw +30% throughput at 4 writes per batch and +78% at 16 [HAY §4.4.2]. Acknowledge only after durable persistence: Tectonic's quorum append acks after data is "written... to disk" [TEC §5.2]. Haystack relied on NVRAM-backed RAID for fast fsync [HAY §4.4.3].
- **Recommendation H5: asynchronous indexes must be recoverable.** Any node-local index (for example a chunk inventory) should be an asynchronously updated checkpoint whose tail can be rebuilt by scanning, using the orphan-scan technique [HAY §3.4.4].
- **Recommendation H6: compaction for replicated, unsealed containers.** Use Haystack's copy-live-then-atomic-swap compaction, with concurrent deletes applied to both copies [HAY §3.6.1]. Sealed RS blocks need a different policy (see F5).
- **Recommendation H7: repair must be many-to-many.** Haystack's single-source bulk sync was NIC-bound and took hours of MTTR [HAY §3.5]. Tectonic's per-shard, per-disk repair and copyset spreading solve this [TEC §3.5].
- **Recommendation H8: separate read-mostly from write-hot devices where possible.** Haystack found filesystems do better at pure reads or pure writes, and used a cache to shelter write-enabled machines [HAY §3.3]. mantle's SSD hot-chunk cache plays that role [TEC §3.2].

---

## 4. Ambry (Noghabi et al., SIGMOD '16)

### 4.1 Context

- Ambry had been in production "for the past 24 months, across four datacenters, serving more than 400 million users" [AMB §1, p. 254].
- **Load:** up to 10K req/s [AMB Abstract, p. 253]. Request rate grew from 5k to 9.5k req/s over 12 months. There were "more than 800 million put and get operations per day (over 120 TB in size)" [AMB §1, p. 253].
- **Workload:** blob sizes range from tens of KB to a few GB, with ">95% read traffic" [AMB §1, pp. 253-254].
- **Results:** up to 88% of network bandwidth, under 50 ms for a 1 MB object, and 8x-10x better request-rate balance across disks [AMB Abstract, p. 253].

### 4.2 Architecture [AMB §2.1, p. 254; §4.1-§4.2, pp. 256-258]

- **Components:** Frontends (receive and route requests), Datanodes (store data) and Cluster Managers (cluster state). Each datacenter runs its own set, and Cluster Managers are synchronized via ZooKeeper [§2.1, p. 254].
- **Cluster Manager state:** "very small (less than a few MBs in total)" [§4.1, p. 256]. It holds:
  - the hardware layout (datacenters, Datanodes, disks, capacity, UP/DOWN);
  - the logical layout (partition -> replica placement, and read-write vs read-only state).
- **Frontends:** stateless. They perform optional security checks and push events to Kafka for change capture [§4.2, p. 257].
- **Router Library:** holds "all the core logic". Clients can embed it and bypass the Frontends [§4.2.1, p. 257].

### 4.3 Partitions as append-only logs; blob IDs [AMB §2.2, pp. 254-255]

- **Decoupling:** "Instead of directly mapping blobs to physical machines, e.g., Chord and CRUSH, Ambry randomly groups blobs together into virtual units called partitions." Separating logical from physical placement "enables transparent data movement ... and avoids immediate rehashing of data during cluster expansion" [p. 254].
- **Structure:**
  - "A partition is implemented as an append-only log in a pre-allocated large file."
  - Partitions are fixed-size.
  - "We use **100 GB** partitions". "even 100 GB partitions can be rebuilt in a few minutes" because rebuilds pull from multiple replicas in parallel [p. 255].
- **Entries:**
  - Blobs are written sequentially as **put** and **delete** entries. Each carries a header (offsets of fields) and the blob id.
  - Put entries also carry the blob size, TTL, creation time, content type, an optional map of user properties, and then the blob [p. 255].
- **Blob id:**
  - "This id consists of the partition id in which the blob is placed (**8 Bytes**), followed by a **32 Byte** universally unique id (UUID) for the blob."
  - Collision probability "< 2^-320". Collisions are handled at Datanodes "by failing the late put request" [p. 255].
- **Replica placement:** greedy, choosing "the disk with the most unallocated space", subject to at most one replica per Datanode and replicas in multiple datacenters. The replica count is set by the administrator [p. 255].
- **Future work stated in the paper:** "use erasure coding for cold data" [p. 255].
- **Lifecycle:**
  - A partition is read-write until it reaches its capacity threshold, then read-only.
  - "The capacity threshold should be slightly less than the max capacity (**80-90%**) of the partition", because replicas may need to catch up and deletes still append entries [p. 255].

### 4.4 Operations, consistency and replication [AMB §2.3, p. 255; §5, pp. 258-259; §6.2, pp. 260-262]

- **API:** only put, get and delete.
  - Put chooses a partition at random for balance.
  - Get and delete extract the partition from the blob id [§2.3, p. 255].
- **Multi-master policies:**
  - "one, k, majority, all", similar to Cassandra consistency levels.
  - Puts and deletes go to all replicas, and the policy sets how many acknowledgements are needed. Gets contact that many randomly chosen replicas.
  - "In practice, we found that for all operations the k = 2 replica policy gives us the balance we desire" [§2.3, p. 255].
- **Async geo-writes:** "puts are performed synchronously only in the local datacenter". Other datacenters are replicated later [§2.3, p. 255].
- **Read-after-write across datacenters:** achieved with **proxy requests** to another datacenter, which "happen infrequently (less than 0.001 % of the time)" [§2.3, p. 255]. During datacenter partitions, unreplicated data can be unavailable [§4.2.1, p. 258].
- **Replication protocol** [§5, pp. 258-259]:
  - Decentralized all-to-all, **pull-based** and two-phase:
    1. Fetch the blob ids written since the last synced offset and filter out those present locally.
    2. Fetch only the missing blobs and append them.
  - Each replica keeps an in-memory **journal**: "an in-memory cache of recent blobs ordered by their offset".
  - Optimizations: separate thread pools for intra- and inter-datacenter replication, batching, and prioritizing lagging replicas.
- **Measured:**
  - Replication lag: more than 85% of values are 0, and "The 95th percentile is less than 1 KB for 100 GB partitions" [§6.2.1, p. 261].
  - Inter-datacenter replication latency: median under 150 ms [§6.2.3, p. 261].
  - Intra-datacenter replication is 6x slower because of "a pre-existing and prefixed artificial added delay of 1 second, intended to prevent incorrect blob collision detections" [§6.2.3, p. 262].

### 4.5 Small vs large objects: chunking [AMB §4.2.1, p. 257]

- Large blobs "create load imbalance, block smaller blobs, and inherently have high latency".
- "Ambry splits large blobs into smaller equal-size units called chunks... we found the sweet spot for the chunk size to be in the range of **4 to 8 MB**." Footnote 3: "Chunk size is not fixed and can be adapted".
- **Put:**
  - Each chunk is put as an independent blob, "most likely being placed on a different partition", with a chunk id in blob-id format.
  - A **metadata blob** stores "the number of chunks and chunk ids in order". Its blob id is returned as the id of the large blob.
  - "If the put fails before writing all chunks, the system will issue deletes for written chunks and the operation has to be redone."
- **Get:**
  - Fetch the metadata blob, then use a **sliding buffer** of s chunks fetched in parallel.
  - The response starts streaming as soon as the first chunk arrives.
- The benchmark stops at 5 MB because "blobs are chunked beyond that point" [§6.1.2, p. 259].

### 4.6 Per-partition index, deletes, compaction [AMB §2.2, p. 255; §4.3, p. 258]

- **Datanode techniques** [§4.3, p. 258]:
  - a per-replica index of blob offsets;
  - reliance on the OS page cache;
  - "Batched writes, with a single disk seek" with a configurable flush period that "trades off latency for durability";
  - "Keeping all file handles open" (a Datanode holds "a few hundred" 100 GB partition replicas);
  - zero-copy gets.
- **Index** [§4.3.1, p. 258]:
  - "light-weight in-memory indexing per replica... sorted by blob id, mapping the blob id to the start offset of the blob entry". It also stores a delete flag and an optional TTL, checked before reading data.
  - "Similar to SSTables, Ambry limits the size of the index by splitting it into segments, storing old segments on disk, and maintaining a Bloom filter for each on-disk segment."
  - "the indexing does not contain any additional information affecting the correctness of the system... the whole indexing can be reconstructed from the partition."
- **Memory policy** [§4.3.2, p. 258]:
  - Only the latest index segment stays in memory. It is flushed as read-only when it exceeds a maximum size, so only it needs rebuilding after failure.
  - Lookups run in reverse chronological order, so "a delete entry will be found before a put entry".
  - Bloom filters mean "with high probability, it incurs only one disk seek".
- **Deletes:**
  - "deletes result in appending a delete entry (with the delete flag set) for the blob (soft delete). Deleted blobs are periodically cleaned up using an in-place compaction mechanism. After compaction, read-only partitions can become read-write if enough space is freed-up" [§2.2, p. 255].
  - **UNVERIFIED:** the compaction algorithm itself is not described.

### 4.7 Failure detection and load balancing [AMB §3, pp. 255-256; §4.2.1, pp. 257-258]

- **Zero-cost failure detection:**
  - No heartbeats. The Router counts consecutive failed requests per Datanode or disk. Past `MAX_FAIL` (2 in the example), the target is marked temporarily down for a wait period.
  - After the wait it becomes temporarily available: one failure sends it back down, and one success restores it [§4.2.1, pp. 257-258].
- **Static balance:** chunking plus random partition choice keeps imbalance "as low as 5%" among Datanodes [§3, pp. 255-256].
- **Expansion hotspot:** new Datanodes hold only read-write partitions, which get the writes and most reads. "the average request rates of new Datanodes were up to 100x higher than old Datanodes" [§3, p. 256].
- **Rebalancing (Algorithm 1):**
  - Compute ideal counts per disk: read-write partitions, read-only partitions and bytes used.
  - Phase 1 moves extras to a pool: read-write partitions with the least data, and random read-only ones.
  - Phase 2 places pooled partitions round-robin onto shuffled below-ideal disks.
  - A replica moves by creating the new replica, syncing it via replication, then deleting the old one.
  - Imbalance of request rate and disk usage drops 6-10x and 9-10x [§3, p. 256].

### 4.8 Performance facts [AMB §6.1, pp. 259-260]

- **Setup:** one Datanode with 24 cores, 64 GB RAM, 14 x 1 TB HDDs and 1 Gb/s networking [§6.1.2, p. 259].
- **Throughput:** saturation at 75%-88% of network bandwidth, except for reads of small blobs [§6.1.3, p. 259].
- **Seek cost:** "when reading a 50 KB blob, more than 94% of latency is due to disk seek (6.49 ms for disk seek, and 0.4 ms for reading the data)" [§6.1.4, p. 260].
- **Page cache effect for 50 KB blobs** (Table 3):
  - Disk reads: avg 17 ms, max 67 ms.
  - Cached reads: avg 3 ms, max 5 ms.
  - About 2100 req/s cached vs 540 req/s from disk [§6.1.6, p. 260].

### 4.9 Implications for mantle (Ambry)

- **Recommendation A1: decouple logical from physical placement.** This is how Ambry rebalances without rehashing [AMB §2.2]. mantle achieves it more finely with Tectonic's explicit chunk -> disk map [TEC §7].
- **Recommendation A2: an ~8 MiB transfer/storage unit is corroborated.** Ambry's 4-8 MB chunks [AMB §4.2.1] and Tectonic's typical 8 MiB chunks [TSHIFT §2] agree. mantle's large-object layout (a File layer with an ordered block list) is Ambry's "metadata blob with ordered chunk ids". mantle should also stream GET responses with a sliding window of parallel chunk reads [AMB §4.2.1].
- **Recommendation A3: node-local index design, if ever needed.** If mantle adds node-local indexes (for example a packed small-object store outside the metadata KV), use Ambry's design:
  - sorted segments with the latest in memory;
  - Bloom filters for on-disk segments;
  - newest-first lookup so tombstones shadow puts;
  - full rebuildability from the log [AMB §4.3.1-§4.3.2].
- **Recommendation A4: handle the post-expansion hotspot explicitly.** Allocating all new writable containers to new disks concentrates the hottest traffic there (up to 100x) [AMB §3]. Spread new blocks across all disks with capacity-aware weights, and let the rebalancer move sealed data onto new disks. Tectonic's rebalancer reacts to "added storage capacity" [TEC §3.5].
- **Recommendation A5: zero-cost failure detection in the client library.** Mark a node or disk temporarily down after N consecutive failures and probe it again with live traffic [AMB §4.2.1]. This complements a central health checker [TEC Fig. 2].
- **Recommendation A6: cache hot data in memory or flash.** The OS page cache or an SSD tier cuts small-read latency 5.5x on average in Ambry's measurement [AMB §6.1.6]. Tectonic uses an SSD hot-chunk cache [TEC §3.2].
- **Recommendation A7: geo-replication (future).** Ambry's local-synchronous, remote-async model with proxy reads [AMB §2.3, §5] is a proven pattern. Beware of replication racing a slow initial put; Ambry needed a 1 s intra-datacenter delay to avoid false collision detection [AMB §6.2.3].

---

## 5. ZippyDB (Tectonic's metadata KV store)

**No peer-reviewed design paper exists.** TEC cites a 2015 video talk (M. Annamalai, "ZippyDB - A Distributed key value store") [TEC ref. 6]. The sources are ranked here.

### 5.1 Peer-reviewed facts about ZippyDB

**From TEC [§3.3, p. 220; §7, p. 228]:**
- linearizable, fault-tolerant, sharded;
- all operations are scoped to a shard, and shards are the unit of replication;
- replicas are RocksDB on SSD, replicated with Paxos;
- strongly consistent reads go to the primary, and any replica can serve other reads;
- **no cross-shard transactions**; "only provides within-shard strong consistency and no cross-shard operations";
- "atomic read-modify-write in-shard transactions" [§3.3, p. 221];
- shards are sized so each node hosts several;
- shards move transparently for load.

**From AKKIO §3 [pp. 449-450]:**
- ZippyDB data "is partitioned horizontally, with each partition assigned to a different shard". Each shard has a primary and secondaries, and "each replica participates in a shard-specific Paxos group".
- "A write to a shard is directed to its primary replica, which then replicates the write to the secondary replicas, using Paxos to ensure that writes are processed in the same order at each replica."
- "Reads that need to be strongly consistent are directed to the primary replica. If eventual consistency is acceptable then reads can be directed to a secondary."
- Replication configurations (replica count and placement across datacenters/racks) are customizable per use case, and several configurations can coexist in one deployment.
- "ZippyDB's Shard Manager assigns each shard replica to a specific ZippyDB server". Shard Manager also migrates shards for load balancing and monitors server liveness.
- ZippyDB "is used as the database service for hundreds of use cases at Facebook" (footnote 4).

**From CAO §2.2 [p. 211] and §5 / Table 2 [p. 215]:**
- "ZippyDB was developed based on RocksDB and relies on Paxos to achieve data consistency and reliability. KV-pairs are divided into shards, and each shard is supported by one RocksDB instance. One of the replicas is selected as the primary shard... The primary shard processes all the writes... If strong consistency is required for reads, read requests (e.g., Get and Scan) are only processed by the primary shard."
- The traced shard "stores the metadata of ObjStorage, which is an object storage system at Facebook. In this shard, a KV-pair usually contains the metadata information for an ObjStorage file or a data block with its address information." CAO does *not* identify ObjStorage as Tectonic, so that link is **UNVERIFIED**.
- **Measured sizes for that metadata shard:**
  - average key **47.9 B** (SD 3.7) and average value **42.9 B** (SD 26.1);
  - "Nearly all of the key sizes are in the two size ranges: [48, 53] and [90, 91]";
  - about 1% of values exceed 400 B.

**From TSHIFT §2 [p. 435]:** Tectonic's Metadata Layer is "hash-sharded" and "built on ZippyDB".

### 5.2 NON-PEER-REVIEWED facts (ZDB-BLOG, Masti 2021; cited by post section)

- **History** ("History of ZippyDB"):
  - First deployed in 2013.
  - Built by combining a replication library, **Data Shuttle**, with RocksDB, on top of Shard Manager and a ZooKeeper-based configuration service.
- **Architecture** ("Architecture"):
  - Deployed as **tiers**, including "specialized tiers for distributed filesystem metadata" alongside the multitenant "wildcard" tier.
  - Data is split into **shards**, replicated across regions with Data Shuttle using either Paxos or async replication.
  - A subset of replicas forms the Paxos quorum ("global scope") with synchronous Multi-Paxos. The rest are **followers**, similar to Paxos learners, that receive data asynchronously.
  - Optional region "stickiness constraints", a caching layer, and pub-sub on mutations.
- **Data model** ("Data model"):
  - get/put/delete and batch variants, prefix iteration and range delete;
  - a test-and-set API, transactions and conditional writes;
  - TTL, cleaned up via RocksDB periodic compaction;
  - "A typical physical shard has a size of **50-100 GB**, hosting several tens of thousands of μshards";
  - μshard-to-physical-shard mapping is either "compact" (static, changed on split) or managed by Akkio.
- **Leadership:**
  - "time is subdivided into units known as **epochs**. Each epoch has a unique leader", assigned by ShardManager with a **lease** kept alive by heartbeats.
  - "Within each epoch, the leader generates a total ordering of all writes to the shard, by assigning each write a monotonically increasing sequence number."
  - Writes go to a replicated durable log via Multi-Paxos and are then applied in order on all replicas.
  - Failure detection happens outside the replication layer (ShardManager); moving it in-band is future work.
- **Consistency** ("Consistency"):
  - **Default write:** persisted on a majority of replicas' Paxos logs *and* written to RocksDB on the primary before acknowledgement. A "fast-acknowledge mode" acknowledges once the write is enqueued on the primary, with weaker guarantees.
  - **Read levels:**
    - "eventual" is really bounded staleness: lagging replicas beyond a threshold do not serve reads;
    - read-your-writes uses client-cached sequence numbers;
    - strong reads go to the primary, "The primary relies on owning the lease". In outlier cases where the primary hasn't heard about lease renewal, they fall back to a quorum check.
- **Transactions** ("Transactions and conditional writes"):
  - "All transactions are serializable by default on a shard". They use optimistic concurrency control: read and write sets are sent to the primary and checked against recently admitted writes.
  - "Transactions spanning epochs are rejected". The write history is purged, and a minimum tracked version bounds acceptable snapshots.
  - Conditional writes are server-side transactions with preconditions `key_present`, `key_not_present` and `value_matches_or_key_not_present`.
- **Future** ("The future of ZippyDB"): "distributed transactions" were in progress as of 2021, implying none existed then. This is consistent with TEC.

### 5.3 Implications for mantle (metadata store)

- **Recommendation Z1: the minimal KV feature set is exactly Tectonic's assumptions** (M4):
  - per-shard consensus log with a total write order;
  - leader lease for primary-served linearizable reads;
  - in-shard multi-key transactions or conditional writes. These implement R3-style guards and "create with overwrite". The `key_not_present` and `value_matches_or_key_not_present` preconditions [ZDB-BLOG] map directly onto name creation and mapping swaps;
  - prefix scans;
  - no cross-shard transactions needed.
- **Recommendation Z2: metadata writes use the default durable mode.** Acknowledge after a majority log commit plus apply on the leader. Never use a fast-ack mode for filesystem metadata [ZDB-BLOG]. *INFERENCE:* the Tectonic ordering guarantees (§1.9) require the metadata commit to be durable before the client is acknowledged.
- **Recommendation Z3: shard granularity.** Use many small virtual shards (μshards) mapped onto fewer physical shards, so resharding and load balancing move only mappings [ZDB-BLOG; AKKIO §3; TEC §3.3]. The ZippyDB physical shard size (50-100 GB) is non-peer-reviewed guidance only. Size mantle's shards so each node hosts several and recovery parallelizes [TEC §3.3].
- **Recommendation Z4: a dedicated metadata tier.** Meta runs filesystem metadata on a dedicated ZippyDB tier [ZDB-BLOG], and shard QPS is capped for isolation (10 KQPS) [TEC §6.3]. mantle's metadata cluster should be isolated from any other KV tenants.
- **Recommendation Z5: capacity estimate.** Use about 50 B keys and 45 B values per metadata row as a planning prior [CAO Table 2], until mantle has its own measurements. **DERIVED:** Table 1 implies (1 + k + m) Block-layer rows per block (the forward entry plus k+m reverse entries), for example 16 rows for RS(9,6), plus File and Name rows. The reverse index dominates Block-layer row counts.

---

## 6. Cross-paper synthesis: proposed mantle baseline

### 6.1 Baseline decisions and their grounding

| Decision | Baseline | Grounding |
|---|---|---|
| Metadata partitioning | 3 layers (Name/File/Block), each hash-partitioned by parent ID; reverse disk->block index co-sharded with blk_id | [TEC §3.3, Table 1, §6.3, §6.6] |
| KV contract | per-shard linearizable + in-shard atomic transactions/conditional writes + prefix scan; no cross-shard transactions | [TEC §3.3, §7; AKKIO §3; CAO §2.2; ZDB-BLOG] |
| Namespace consistency | read-after-write for data, single-object and same-parent ops; non-atomic cross-parent moves with backpointer; inter-layer GC | [TEC §3.3, §3.5] |
| Object overwrite | new file_id + guarded atomic name swap in the directory shard; old object GC'd lazily | [TEC §3.3 (C1/C2, R3), §3.5] |
| Writers | single writer per file via write token; token steal seals the old writer's blocks; appends only by the block's creator | [TEC §3.4, §5.2] |
| Large-object write | 72 MiB blocks, RS(9,6), 8 MiB chunks, client-side encode, reservation-hedged (k+m+4 asked, ack at k+m-1), file metadata committed at close | [TEC §5.1; TSHIFT §2] |
| Small-object write | log-structured files; 3x replicated partial-block quorum appends (ack at 2); commit block size + checksum before ack; seal -> RS(10,4) | [TEC §5.2] |
| Read path | contiguous RS; direct single-IO reads; range-only online reconstruction; reconstruction budget of about 10% of reads | [TEC §6.4; F4 §5.3] |
| Placement | explicit chunk->disk map; copysets from ~100 shuffles; one chunk per failure domain; no block groups | [TEC §3.5, §6.6, §7; F4 §5.5] |
| Storage node | chunks as files on local FS (XFS); API get/put/append/delete/list/scan; SSD for FS metadata + hot cache; JBOD | [TEC §3.2; F4 §6.1] |
| Background | stateless per-shard services: GC, rebalancer, repair, disk inventory, block scan, health checker, stats; throttled, lowest TrafficClass | [TEC §3.5, Fig. 2, §4.1; F4 §5.3] |
| Multitenancy | static capacity quotas; TrafficGroups x {Gold, Silver, Bronze}; client-side distributed leaky bucket; node WRR + Gold protections; disk-time accounting | [TEC §4.1, §6.2] |
| Integrity | end-to-end checksums across and within processes; verify transforms by inversion | [TEC §6.6; HAY Table 1] |
| Deletes | lazy delete + GC for metadata; for packed small objects choose Haystack-style compaction (unsealed), rewrite-sealed-block, or crypto-shredding | [TEC §3.5; HAY §3.6.1; AMB §2.2; F4 §5.3, §6.5] |
| Geo-replication | out of scope for v1 (Tectonic delegates it); future options: async + proxy reads (Ambry) or XOR buddies (f4) | [TEC §6.7; AMB §2.3; F4 §5.4] |

### 6.2 Open design risks surfaced by this research

1. **S3 listing vs hash-partitioned directories.** Tectonic has **no recursive list** and needs a multi-shard client-side walk [TEC §6.5]. Three options:
   - **(A) Tectonic-faithful.** Split object keys on `/` into directory entries. Delimiter listings become single-shard prefix scans; full recursive listings need a merge-walk across shards.
   - **(B) Flat per-bucket index, range-partitioned by key.** Cheap ordered listing, but exposed to the hotspot behavior TEC measured against ADLS-style range partitioning [TEC §3.3, §6.3].
   - **(C) Hybrid.** Hash-partitioned directory shards plus an asynchronously maintained ordered index. This needs its own consistency argument.

   The choice depends on S3 listing semantics, which must be sourced from AWS primary documentation in a separate note and are **not verified here**. Baseline recommendation: (A), because it is the only option validated at exabyte scale in these sources. Revisit after the S3 research note.
2. **Write-token liveness.** TEC defines no lease or timeout for writers [TEC §3.4]. mantle must define how abandoned uploads' unsealed blocks are detected and sealed or collected. This is a design gap, not a sourced fact.
3. **Where the small-object locator lives.** In Tectonic, blob locations are kept by the blob tenant [TEC §5.2] and the location is **UNVERIFIED**. For mantle, the natural place is the Name-layer value of each S3 object (*INFERENCE*), because S3 objects already need a name entry.
4. **Space reclamation for sealed RS blocks** that hold packed small objects. No reviewed source describes Tectonic's approach. **UNVERIFIED**; it needs its own design and research.
5. **Hedging parameters for other codes** (RS(10,4), replication) and how reservation sets interact with copysets: **UNVERIFIED** [TEC §5.1, §3.5].

### 6.3 Suggested follow-up primary sources (not reviewed in this note)

- Cidon et al., "Copysets: Reducing the Frequency of Data Loss in Cloud Storage", USENIX ATC '13. Tectonic's copyset design reference [TEC ref. 20].
- Ramakrishnan et al., "Azure Data Lake Store", SIGMOD '17. The range-partitioned layered-metadata alternative [TEC ref. 42].
- Niazi et al., "HopsFS", FAST '17. Metadata in a NewSQL database [TEC ref. 35].
- Huang et al., "Erasure Coding in Windows Azure Storage", USENIX ATC '12. LRC vs RS, cited by f4.
- Berg et al., "The CacheLib Caching Engine", OSDI '20. Tectonic's flash cache library [TEC ref. 13].
- Lewi et al., "Scaling backend authentication at Facebook", IACR ePrint 2018/413. Tectonic's token scheme [TEC ref. 31]; not peer-reviewed as a conference paper.
- AWS S3 API reference: listing order, consistency, multipart and conditional writes. Required before finalizing §6.2 item 1.

---

## 7. Consolidated list: facts NOT verified

| Item | Status |
|---|---|
| Tectonic default block size, as stated in the Tectonic paper itself | Not stated. Only the 72 MB experiment [TEC Fig. 3a]. "Typically 72 MiB" comes from TSHIFT §2 |
| Tectonic chunk size | Not in TEC. "Typically 8 MiB" from TSHIFT §2. BALEEN §2.1 says "8 MB blocks and 128 kB segments" (terminology conflict, see §1.14) |
| Tectonic HDD capacity per drive | UNVERIFIED. DERIVED ~10.5 TB from Table 2. BALEEN states 378 TB HDD per node (citing TEC) |
| Tectonic "~70%" utilization | Conflicts with Table 2 arithmetic (78.6%) |
| Blob-ID -> location index location/format in Tectonic | UNVERIFIED |
| Blob-storage block size and seal policy; which process re-encodes | UNVERIFIED ("the Client Library") |
| Write-token lease/timeout semantics | UNVERIFIED (none described) |
| Disk inventory, block scanner, health checker, stat service behavior | UNVERIFIED (named in Fig. 2 only) |
| Rate-limiter window, in-flight caps, Gold starvation threshold | UNVERIFIED (values not given) |
| Hedging parameters for RS(10,4)/RS(3,3)/replication; copyset vs 19-node reservation interplay | UNVERIFIED |
| Contents of subdir_info/file_info/blk_info/chunk_info; the per-file header key | UNVERIFIED |
| ZippyDB shard size used by Tectonic | UNVERIFIED (blog gives generic 50-100 GB, non-peer-reviewed) |
| ZippyDB epochs/leases/OCC/μshards/Data Shuttle | NON-PEER-REVIEWED only (ZDB-BLOG) |
| CAO's "ObjStorage" being Tectonic | UNVERIFIED |
| f4 conversion mechanics (who encodes; purge of deleted needles; journal reconciliation) | UNVERIFIED |
| f4 savings equation line (30/68 PB) | Paper typo. Prose and formula give 39/87 PB |
| Haystack 10 B/photo breakdown includes offset | Not itemized in the paper |
| Ambry compaction algorithm | UNVERIFIED ("in-place compaction" only) |
| Small/large object cut-over size for mantle | No source. Decide by benchmark |

---

## Appendix A: quantitative quick reference

| Fact | Value | Source |
|---|---|---|
| Tectonic tenants per cluster | ~10 | [TEC §1, §3.1] |
| TrafficGroups per cluster | ~50 | [TEC §4.1, p. 222] |
| Storage node hardware | 36 HDD + 1 TB SSD | [TEC §3.2, p. 219] |
| Representative cluster | 1590 PB capacity, 1250 PB used, 10.7 B files, 15 B blocks, 4208 nodes | [TEC Table 2, p. 225] |
| Metadata shard QPS cap | 10 KQPS | [TEC §6.3, p. 225] |
| Name shards hitting the cap | ~1% over 3 days | [TEC §6.3, p. 225] |
| Share of metadata ops served by the Block layer | ~2/3 | [TEC §3.3, p. 220] |
| Warehouse EC | RS(9,6) long-lived; RS(3,3) short-lived | [TEC §5.1, p. 223] |
| Blob EC | 3x replicated appends (ack 2) -> RS(10,4) after seal | [TEC §5.2, pp. 224-225] |
| Hedged write RS(9,6) | reserve 19, write 15, ack 14 | [TEC §5.1, p. 224] |
| Hedging benefit | ~20% p99 improvement, 72 MB blocks, 80% utilization | [TEC §5.1, p. 224] |
| Reconstruction read cap | 10% of reads | [TEC §6.4, p. 226] |
| Copyset shuffles | ~100 | [TEC §3.5, p. 222] |
| Block groups failure | 5% nodes down -> 80% groups unwritable | [TEC §6.6, p. 227] |
| Overprovisioning avoided | ~17% | [TEC §6.2, p. 225] |
| Warehouse cluster count reduction | 10x | [TEC §1, p. 217] |
| Effective RF | Haystack 3.6x ideal / 5.3x actual; f4 2.8x or 2.1x; Tectonic blob ~2.8x | [TEC §2.1, p. 218; F4 §5.4, p. 390] |
| Tectonic typical block / chunk | 72 MiB / 8 MiB | [TSHIFT §2, p. 435] |
| Tectonic node HDD capacity | 378 TB | [BALEEN §2.1, p. 348] (citing TEC) |
| f4 cell | 14 racks x 15 hosts x 30 x 4 TB | [F4 §5.2, p. 388] |
| f4 EC block size | typically 1 GB | [F4 §5.3, p. 389] |
| f4 disk IOPS | 80 IOPS per 4 TB -> 20 IOPS/TB | [F4 §3, p. 385] |
| f4 deleted-space overhead | 6.8% | [F4 §6.5, p. 394] |
| f4 read latency | median 17 ms (Haystack 14 ms); p80 < 30 ms; p99 80 ms | [F4 §6.3, p. 393] |
| Disk AFR | ~1% normal; >60% bad batch | [F4 §6.4, §7, pp. 394-395] |
| Haystack volume | ~100 GB | [HAY §3.1, §3.4, pp. 50-51] |
| Haystack RAM per photo | ~10 B | [HAY §3.6.2, p. 54] |
| Haystack XFS tuning | 1 GB preallocated extents; 256 KB RAID stripe | [HAY §3.4.5, p. 53] |
| Haystack batching | +30% (4/batch), +78% (16/batch) | [HAY §4.4.2, p. 57] |
| Haystack yearly deletes | ~25% of photos | [HAY §3.6.1, p. 54] |
| Ambry partition | 100 GB, read-only at 80-90% | [AMB §2.2, p. 255] |
| Ambry blob id | 8 B partition id + 32 B UUID | [AMB §2.2, p. 255] |
| Ambry chunk | 4-8 MB | [AMB §4.2.1, p. 257] |
| Ambry quorum policy | k = 2 | [AMB §2.3, p. 255] |
| Ambry proxy reads | < 0.001% | [AMB §2.3, p. 255] |
| Ambry replication lag | p95 < 1 KB (100 GB partitions) | [AMB §6.2.1, p. 261] |
| Ambry seek share | 50 KB read: 6.49 ms seek vs 0.4 ms transfer | [AMB §6.1.4, p. 260] |
| ZippyDB metadata KV size | avg key 47.9 B, avg value 42.9 B | [CAO Table 2, p. 215] |
| ZippyDB physical shard | 50-100 GB (NON-PEER-REVIEWED) | [ZDB-BLOG, "Data model"] |
