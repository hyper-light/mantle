# 22 — Garbage collection: lazy deletion, sweeps between layers, grace periods and pacing

Research note for mantle's collector (docs/design/metadata.md §2, "Lazy deletion" and "Creating
and deleting a bucket"). Its §6 lists two items as open: the collector's schedule, and the sweep
that finds files a stopped gateway never handed over. mantle releases a file into its Name
range's queue in the transaction that stops referencing it, and a collector reclaims the queue
oldest first after a grace period, bottom up and resumable (`crates/meta/src/reclaim.rs`). This
note covers what the queue cannot do, which is to find objects no range ever referenced and to
decide when a stalled bucket create or delete is taken over. It also covers what published
systems do about grace periods and about pacing deletion against foreground I/O:

- GFS's lazy deletion, orphan scan and stale-replica rule; Tectonic's collectors between
  metadata layers;
- HDFS's block reports, deletion limits, trash and leases; Ceph RGW's garbage collector, orphan
  tools and bucket-index timeouts; Windows Azure Storage and Giza;
- grace periods set by another process (Cassandra, Spanner, Riak, Dynamo) and sweeps that race
  writes in flight (Bigtable, git, RocksDB, Ceph, Giza, S3 multipart, Haystack and f4);
- pacing: HDFS, Ceph, RocksDB, fstrim, Tectonic's traffic classes, and the literature on
  background work (freeblock scheduling, Aqueduct, mClock, and the idle-time work note 11
  already records);
- coordinators that fail part way: HDFS lease recovery, Percolator's lock cleanup, Ceph's
  pending bucket-index entries.

All sources were fetched on 2026-09-29 (UTC). Labels: **peer-reviewed** for papers in refereed
venues; **primary** for vendor documentation, man pages, a project's own issue tracker, and
source code at a pinned commit (a vendor's blog is primary for what the vendor says of its own
system, but is not peer-reviewed); **third-party** for another party's account; **DERIVED** for
an inference of this note; **UNVERIFIED** where nothing was found.

## 0. Sources

### 0.1 Citations

`[KEY §section, p. N]` gives the printed page of the copy read. Papers were read from their PDF
text layers (pdftotext). Ligatures are normalized, words hyphenated across line breaks are
rejoined, bracketed reference numbers such as "[25]" are omitted, "..." marks an elision and
"[sic]" a typo in the original.

- GGL03: the Google copy prints no page numbers. Crossref gives SOSP '03 pp. 29–43 for DOI
  10.1145/945445.945450, and the PDF has 15 pages, so PDF page n is p. 28 + n.
- HAY: the legacy PDF prints no page numbers; its 14 pages are mapped onto OSDI '10 pp. 47–60 as
  note 01 does.
- AQ: PDF page 1 is a USENIX cover, and the paper's first page states "pp. 219–230", so PDF page
  n is p. 217 + n (DERIVED mapping).
- PERC, FBS00, MCLK, WASEC: no printed page numbers; cited as "PDF p."
- TEC, WAS, GIZA, BT06, F4, DYN: printed page numbers.
- Source code is cited by file and line at the pinned commit; configuration by option name.

### 0.2 Table

| Key | Source | Label | How obtained |
|---|---|---|---|
| GGL03 | S. Ghemawat, H. Gobioff, S.-T. Leung. "The Google File System." *SOSP '03*, pp. 29–43. doi:10.1145/945445.945450 | peer-reviewed | https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf (SHA-256 prefix 108ced8f084131ae) |
| TEC | S. Pan et al. "Facebook's Tectonic Filesystem: Efficiency from Exascale." *FAST '21*, pp. 217–231 (authors in note 01) | peer-reviewed | https://www.usenix.org/system/files/fast21-pan.pdf (18e1ab6268ae8cb1) |
| TEC-TALK | FAST '21 talk slides for TEC | primary | https://www.cs.princeton.edu/~wlloyd/papers/tectonic-fast21-talk-public.pdf (86d75bb1cc251d91); lists "Garbage collectors" only |
| TEC-BLOG | Engineering at Meta, "Consolidating Facebook storage infrastructure with Tectonic file system", 2021-06-21 | primary, not peer-reviewed | https://engineering.fb.com/2021/06/21/data-infrastructure/tectonic-file-system/ ; searched for "garbage", "delet", "lazy": no match |
| WAS | B. Calder et al. "Windows Azure Storage: A Highly Available Cloud Storage Service with Strong Consistency." *SOSP '11*, pp. 143–157. doi:10.1145/2043556.2043571 (authors in note 09) | peer-reviewed | sigops.org printable PDF, URL in note 09 (a9d462c13228385e) |
| WASEC | C. Huang, H. Simitci, Y. Xu, A. Ogus, B. Calder, P. Gopalan, J. Li, S. Yekhanin. "Erasure Coding in Windows Azure Storage." *USENIX ATC '12* | peer-reviewed | https://www.usenix.org/system/files/conference/atc12/atc12-final181_0.pdf |
| GIZA | Y. L. Chen, S. Mu, J. Li, C. Huang, J. Li, A. Ogus, D. Phillips. "Giza: Erasure Coding Objects across Global Data Centers." *USENIX ATC '17*, pp. 539–551 | peer-reviewed | https://www.usenix.org/system/files/conference/atc17/atc17-chen_yu_lin.pdf (ea390e2717102d0b) |
| BT06 | F. Chang et al. "Bigtable: A Distributed Storage System for Structured Data." *OSDI '06*, pp. 205–218 | peer-reviewed | https://www.usenix.org/legacy/event/osdi06/tech/chang/chang.pdf (0afd4caeddd8b6a1) |
| PERC | D. Peng, F. Dabek. "Large-scale Incremental Processing Using Distributed Transactions and Notifications." *OSDI '10* | peer-reviewed | https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Peng.pdf (7a9e42fc3e58da94) |
| HAY | D. Beaver, S. Kumar, H. C. Li, J. Sobel, P. Vajgel. "Finding a needle in Haystack: Facebook's photo storage." *OSDI '10*, pp. 47–60 | peer-reviewed | URL in note 01 (fb643a578af63544) |
| F4 | S. Muralidhar et al. "f4: Facebook's Warm BLOB Storage System." *OSDI '14*, pp. 383–398 | peer-reviewed | URL in note 01 (d605da32c2127cef) |
| DYN | G. DeCandia et al. "Dynamo: Amazon's Highly Available Key-value Store." *SOSP '07*, pp. 205–220 | peer-reviewed | https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf (5cacd624cd7bfd37) |
| FBS00 | C. R. Lumb, J. Schindler, G. R. Ganger, D. F. Nagle, E. Riedel. "Towards Higher Disk Head Utilization: Extracting Free Bandwidth From Busy Disk Drives." *OSDI 2000* | peer-reviewed | https://www.usenix.org/legacy/events/osdi2000/full_papers/lumb/lumb.pdf (b7e7bbb90ed37333) |
| AQ | C. Lu, G. A. Alvarez, J. Wilkes. "Aqueduct: online data migration with performance guarantees." *FAST '02*, pp. 219–230 | peer-reviewed | https://www.usenix.org/legacy/event/fast02/full_papers/lu/lu.pdf (b0ec0dde331c2e5e) |
| MCLK | A. Gulati, A. Merchant, P. J. Varman. "mClock: Handling Throughput Variability for Hypervisor IO Scheduling." *OSDI '10* | peer-reviewed | https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Gulati.pdf (7bbf6bb7f5efcc96) |
| HDFS | Apache Hadoop `rel/release-3.5.0`, commit `dbcc7cd797100e6b32cd84f85b53a5193a5f9af0`: `hdfs-default.xml` (e43c26678cb6317f), `core-default.xml` (aecc613c77518c78), `BlockManager.java` (85493792a73505c5), `InvalidateBlocks.java` (1f6f83ecb97f0c05), `DatanodeManager.java` (d4e779bc221ffff9), `LeaseManager.java` (d7c5d71e8f1adfcf), `FSNamesystem.java` (5bc1642af6ace1d3), `HdfsConstants.java` (af7159c6d3f7aa6d), `HdfsClientConfigKeys.java` (1a4173e4414d808e), `ClientProtocol.java` (92ec38483965fa24), `FsDatasetAsyncDiskService.java` (c2e41ed3cb942bb9), `HdfsDesign.md` (0dde16f64ae8bd88) | primary | raw.githubusercontent.com at the commit |
| HDFS-3.2 | `HdfsConstants.java` at `rel/release-3.2.0`, commit `e97acb3bd8f3befd27418996fa5d4b50bf2e17bf` | primary | raw.githubusercontent.com (a20723b29309ac7b) |
| HDFS-14758 | Apache JIRA HDFS-14758, "Decrease lease hard limit"; resolved Fixed 2020-02-11; fix versions 3.3.0, 2.8.6, 2.9.3, 3.1.4, 3.2.2, 2.10.1 | primary | https://issues.apache.org/jira/rest/api/2/issue/HDFS-14758 |
| CEPH | Ceph `v20.2.4`, commit `7f793731f1b39eb4f465e960113d2363c311b964`: `src/common/options/rgw.yaml.in` (90155141cd45a43e), `src/common/options/osd.yaml.in` (96e00ff1c8bbef68), `doc/radosgw/config-ref.rst` (ad7606b327859195), `doc/radosgw/orphans.rst` (7be0290da19a4259), `doc/man/8/rgw-orphan-list.rst` (c2c4545749244bfe), `doc/man/8/radosgw-admin.rst` (8289a252babf4f8e), `src/rgw/rgw-orphan-list` (c62a07638466e9a4), `doc/rados/configuration/mclock-config-ref.rst` (9f84818f80458313) | primary | raw.githubusercontent.com at the commit |
| CEPH-18 | Ceph `v18.2.4`, commit `e7ad5345525c7aa95470c26863873b581076945d`: `src/rgw/rgw_orphan.cc`, the `orphans find` implementation (absent from `src/rgw` at v20.2.4) | primary | raw.githubusercontent.com (f2604e4d4c798fe7) |
| CASS | Apache Cassandra `cassandra-5.0.9`, commit `b5f2a54210d541339c2e7c17a794195cac0e67c2`: `TableParams.java` (976e45f860351067), `doc/.../compaction/tombstones.adoc` (1136d3eaf162cc84), `doc/.../operating/repair.adoc` (63c12739977c584a) | primary | raw.githubusercontent.com at the commit |
| ROCKS | RocksDB `v11.8.1`, commit `abeebd9630f11bd08c28b7bd43c7bdfc62050654`: `db/db_impl/db_impl.h` (c2474a6042e584b7), `include/rocksdb/sst_file_manager.h` (596797ef0c5838be), `file/delete_scheduler.h` (ee1aafc4f771fdce) | primary | raw.githubusercontent.com at the commit |
| ROCKS-WIKI | RocksDB wiki pages "Slow Deletion" and "SST File Manager" (unversioned) | primary, unpinned | https://raw.githubusercontent.com/wiki/facebook/rocksdb/Slow-Deletion.md |
| GIT | Git `v2.56.0`, commit `a018953688f1b10bddf91bff8747068f5f4746a4`: `Documentation/git-gc.adoc` (e3dbc4081095faf8), `Documentation/config/gc.adoc` (0ab75f6f40ebdbda) | primary | raw.githubusercontent.com at the commit |
| FSTRIM | util-linux `v2.42.4`, commit `d76cbf8f13e65ff657344f7f6a90042cf755ba59`: `sys-utils/fstrim.8.adoc` | primary (man page) | raw.githubusercontent.com (014ffa1de823ba5b) |
| S3 | AWS S3 User Guide `mpuoverview` (debe2173d490015d), `mpu-abort-incomplete-mpu-lifecycle-config` (1ba0353a79bb268f), `abort-mpu` (6e113972dbf16f51), `lifecycle-expire-general-considerations` (88d8c618cc9ef297); API Reference `API_AbortMultipartUpload` (40bdd6065e211bb5) | primary | `.md` renditions under https://docs.aws.amazon.com/AmazonS3/latest/userguide/ and .../API/ |
| SPANNER | Google Cloud Spanner documentation, "Timestamp bounds" (d7d010ec335e9f1a) and "Point-in-time recovery" (4fe0c4befb5f5111) | primary, unversioned | https://cloud.google.com/spanner/docs/timestamp-bounds , https://cloud.google.com/spanner/docs/pitr |
| COLOSSUS | Google Cloud blog, "A peek behind Colossus, Google's file system", April 2021 | primary, not peer-reviewed | https://cloud.google.com/blog/products/storage-data-transfer/a-peek-behind-colossus-googles-file-system (3fdc2865cc853a20) |
| RIAK | Riak KV documentation, "Object Deletion Reference" (latest, unversioned) | primary | https://docs.riak.com/riak/kv/latest/using/reference/object-deletion/index.html (2ed26c3e5638d17f) |
| RO92, BHS95, GBS95, AOS12, MRZ+09 | Rosenblum and Ousterhout (TOCS 1992); Blackwell, Harris, Seltzer (USENIX 1995); Golding et al. (USENIX 1995); Amvrosiadis, Oprea, Schroeder (DSN 2012); Mi et al. (ACM TOS 2009) | peer-reviewed | quoted as read and recorded in note 11 §10.2 and §12.3 |

Third-party posts found while searching (blogs summarizing Tectonic and Colossus) were not used
as evidence.

## 1. GFS: lazy deletion, the orphan scan, stale replicas

### 1.1 Mechanism [GGL03 §4.4]

- "After a file is deleted, GFS does not immediately reclaim the available physical storage. It
  does so only lazily during regular garbage collection at both the file and chunk levels. We
  find that this approach makes the system much simpler and more reliable." [GGL03 §4.4, p. 36]
- Files: "the file is just renamed to a hidden name that includes the deletion timestamp. During
  the master's regular scan of the file system namespace, it removes any such hidden files if
  they have existed for more than three days (the interval is configurable). Until then, the file
  can still be read under the new, special name and can be undeleted by renaming it back to
  normal." Removing the hidden file's in-memory metadata "effectively severs its links to all its
  chunks." [GGL03 §4.4.1, p. 36]
- Chunks: "In a similar regular scan of the chunk namespace, the master identifies orphaned
  chunks (i.e., those not reachable from any file) and erases the metadata for those chunks. In a
  HeartBeat message regularly exchanged with the master, each chunkserver reports a subset of the
  chunks it has, and the master replies with the identity of all chunks that are no longer present
  in the master's metadata. The chunkserver is free to delete its replicas of such chunks."
  [GGL03 §4.4.1, p. 36]
- Why the sweep is easy there: "We can easily identify all references to chunks: they are in the
  file-to-chunk mappings maintained exclusively by the master. We can also easily identify all
  the chunk replicas: they are Linux files under designated directories on each chunkserver. Any
  such replica not known to the master is “garbage.”" [GGL03 §4.4.2, p. 36]
- The scan is cheap because the state is in memory: "it is easy and efficient for the master to
  periodically scan through its entire state in the background. This periodic scanning is used to
  implement chunk garbage collection, re-replication in the presence of chunkserver failures, and
  chunk migration to balance load and disk space" [GGL03 §2.6.1, p. 31].

### 1.2 Why lazy [GGL03 §4.4.2, p. 37]

"First, it is simple and reliable in a large-scale distributed system where component failures
are common. Chunk creation may succeed on some chunkservers but not others, leaving replicas that
the master does not know exist. Replica deletion messages may be lost, and the master has to
remember to resend them across failures, both its own and the chunkserver's. Garbage collection
provides a uniform and dependable way to clean up any replicas not known to be useful. Second, it
merges storage reclamation into the regular background activities of the master, such as the
regular scans of namespaces and handshakes with chunkservers. Thus, it is done in batches and the
cost is amortized. Moreover, it is done only when the master is relatively free. The master can
respond more promptly to client requests that demand timely attention. Third, the delay in
reclaiming storage provides a safety net against accidental, irreversible deletion."

### 1.3 Costs and knobs

- "In our experience, the main disadvantage is that the delay sometimes hinders user effort to
  fine tune usage when storage is tight. Applications that repeatedly create and delete temporary
  files may not be able to reuse the storage right away. We address these issues by expediting
  storage reclamation if a deleted file is explicitly deleted again. We also allow users to apply
  different replication and reclamation policies to different parts of the namespace. For
  example, users can specify that all the chunks in the files within some directory tree are to be
  stored without replication, and any deleted files are immediately and irrevocably removed from
  the file system state." [GGL03 §4.4.2, p. 37]
- Repair skips what is being deleted: "we prefer to first re-replicate chunks for live files as
  opposed to chunks that belong to recently deleted files (see Section 4.4)." [GGL03 §4.3, p. 36]
- What the grace holds: Table 2 gives cluster A 735 k files and 22 k dead files, cluster B 737 k
  files and 232 k dead files; "B has a larger proportion of dead files, namely files which were
  deleted or replaced by a new version but whose storage have not yet been reclaimed."
  [GGL03 §6.2, §6.2.1, p. 39]. **DERIVED:** about 3% of A's files and 31% of B's were dead; the
  space a grace period holds depends on the workload's churn, not on the period alone.

### 1.4 Stale replicas [GGL03 §4.5, p. 37]

- "Whenever the master grants a new lease on a chunk, it increases the chunk version number and
  informs the up-to-date replicas. ... This occurs before any client is notified and therefore
  before it can start writing to the chunk."
- "The master will detect that this chunkserver has a stale replica when the chunkserver restarts
  and reports its set of chunks and their associated version numbers."
- "The master removes stale replicas in its regular garbage collection. Before that, it
  effectively considers a stale replica not to exist at all when it replies to client requests for
  chunk information."

### 1.5 Why GFS's sweep does not race writes in flight

- "Each chunk is identified by an immutable and globally unique 64 bit chunk handle assigned by
  the master at the time of chunk creation." [GGL03 §2.3, p. 30]
- "The master does not keep a persistent record of which chunkservers have a replica of a given
  chunk. It simply polls chunkservers for that information at startup." [GGL03 §2.6.2, p. 32]
- Leases bound a primary that loses contact, not a write's length: "A lease has an initial timeout
  of 60 seconds. However, as long as the chunk is being mutated, the primary can request and
  typically receive extensions from the master indefinitely." "Even if the master loses
  communication with a primary, it can safely grant a new lease to another replica after the old
  lease expires." [GGL03 §3.1, p. 33]
- **DERIVED:** every chunk is named in the master's metadata before its data is written, so a
  chunk being written is never "not known to the master". GFS needs no time bound between
  creating a chunk and referencing it, and publishes none.

## 2. Tectonic: collectors between metadata layers

### 2.1 What the collectors clean [TEC §3.5, p. 221]

- "Background services maintain consistency between metadata layers, maintain durability by
  repairing lost data, rebalance data across storage nodes, handle rack drains, and publish
  statistics about filesystem usage. Background services are layered similar to the Metadata
  Store, and they operate on one shard at a time."
- "A garbage collector between each metadata layer cleans up (acceptable) metadata
  inconsistencies. Metadata inconsistencies can result from failed multi-step Client Library
  operations. Lazy object deletion, a real-time latency optimization that marks deleted objects at
  delete time without actually removing them, also causes inconsistencies."
- The chunk level is reconciled by repair: "The repair service handles the actual data movement by
  reconciling the chunk list to the disk-to-block map for every disk in the system. To scale
  horizontally, the repair service works on a per-Block layer shard, per-disk basis, enabled by the
  reverse index mapping disks to blocks (Table 1)." Storage nodes expose "APIs for listing chunks
  and scanning chunks" [TEC §3.2, p. 219].

### 2.2 Multi-step operations and how a pending one is detected

- "The key-value store does not support consistent cross-shard transactions, so Tectonic provides
  non-atomic cross-directory move operations." "The moved directory keeps a backpointer to its
  parent directory to detect pending moves. This ensures only one move operation is active for a
  directory at a time." "Rename step R3 uses a within-shard transaction to ensure that the file
  object pointed to by f1 has not been modified since R1." [TEC §3.3, p. 221]
- Writers are fenced by a token, not by time: "Tectonic enforces single-writer semantics with a
  write token for every file. Any time a writer wants to add a block to a file, it must include a
  matching token for the metadata write to succeed." "If a second process attempts to open the
  file, it will generate a new token and overwrite the first process's token, becoming the new,
  and only, writer for the file. The new writer's Client Library will seal any blocks opened by the
  previous writer in the open file call." [TEC §3.4, p. 221]

### 2.3 Scheduling and pacing

- Collection traffic is a class of its own: "blob storage includes production traffic from
  Facebook users and background garbage collection traffic." "The TrafficClasses are Gold, Silver,
  and Bronze, corresponding to latency-sensitive, normal, and background applications. Spare
  resources are distributed according to TrafficClass priority within a tenant." [TEC §4.1,
  p. 222]
- At the storage node: "Nodes provide fair sharing and isolation with a weighted round-robin (WRR)
  scheduler that provisionally skips a TrafficGroup's turn if it will exceed its resource quota."
  "Second, we limit how many non-Gold IOs may be in flight for every disk. Incoming non-Gold
  traffic is blocked from scheduling if there are any pending Gold requests and the non-Gold
  in-flight limit has been reached." "Tectonic stops scheduling non-Gold requests to a disk if a
  Gold request has been pending on that disk for a threshold amount of time." At the client, a
  request without spare capacity "is delayed or rejected depending on the request's timeout."
  [TEC §4.1, p. 223]

### 2.4 Not published

The collectors' period, pacing, grace before deleting lazily deleted objects, and how they tell a
failed multi-step operation from one still running: **UNVERIFIED**. The values of the non-Gold
in-flight limit and the Gold pending threshold are not given. TEC-TALK names "Garbage
collectors" among background services without detail, and TEC-BLOG does not mention them.

## 3. HDFS

### 3.1 Blocks are allocated before they are written

- `ClientProtocol.addBlock`: "addBlock() allocates a new block and datanodes the block data should
  be replicated to." (`ClientProtocol.java:400`)
- "The NameNode makes all decisions regarding replication of blocks. It periodically receives a
  Heartbeat and a Blockreport from each of the DataNodes in the cluster. ... A Blockreport contains
  a list of all blocks on a DataNode." "When a DataNode starts up, it scans through its local file
  system, generates a list of all HDFS data blocks that correspond to each of these local files,
  and sends this report to the NameNode." (`HdfsDesign.md`)
- Full block reports every `dfs.blockreport.intervalMsec` = `21600000` (6 h); heartbeats every
  `dfs.heartbeat.interval` = `3` s (`hdfs-default.xml:839`, `:941`).

### 3.2 Orphaned and excess replicas found from reports

- A reported replica with no block in the namespace is deleted: "If blocksMap does not contain
  reported block id, The replica should be removed from Datanode" (`BlockManager.java:3448–3449`,
  `processReportedBlock`).
- Excess replicas: "When the replication factor of a file is reduced, the NameNode selects excess
  replicas that can be deleted. The next Heartbeat transfers this information to the DataNode. The
  DataNode then removes the corresponding blocks and the corresponding free space appears in the
  cluster." (`HdfsDesign.md`, "Decrease Replication Factor")
- An inventory that may lag is not acted on. On a standby: "the node may receive block reports
  from datanodes before receiving the corresponding namespace edits from the active NameNode. Thus,
  it will postpone them for later processing, instead of marking the blocks as corrupt."
  (`BlockManager.java:433–438`). Deleting a corrupt replica is postponed while any replica is on
  "nodes with potentially out-of-date block reports" (`BlockManager.java:2033–2040`), and
  excess-replica processing is postponed while a storage "does not yet have up-to-date
  information" (`processExtraRedundancyBlockWithoutPostpone`).
- Nothing is deleted in safe mode: "Blocks should not be replicated or removed if in safe mode."
  (`BlockManager.java:5425`)

### 3.3 Deleting: per-iteration and per-heartbeat limits

- `dfs.namenode.invalidate.work.pct.per.iteration` = `0.32f`: "This determines the percentage
  amount of block invalidations (deletes) to do over a single DN heartbeat deletion command. The
  final deletion count is determined by applying this percentage to the number of live nodes in the
  system." (`hdfs-default.xml:2252`). In code it sets how many DataNodes get deletion work per
  iteration: `nodesToProcess = (int) Math.ceil(numlive * this.blocksInvalidateWorkPct)`
  (`BlockManager.java:5436`).
- `dfs.block.invalidate.limit` = `1000`: "The maximum number of invalidate blocks sent by namenode
  to a datanode per heartbeat deletion command. This property works with
  "dfs.namenode.invalidate.work.pct.per.iteration" to throttle block deletions."
  (`hdfs-default.xml:4395`). The limit used is the larger of this and `20 *` the heartbeat interval
  in seconds (`DatanodeManager.java:2215–2227`).
- The iteration runs every `dfs.namenode.redundancy.interval.seconds` = `3` s
  (`hdfs-default.xml:1237`; `RedundancyMonitor`, `BlockManager.java:5381–5395`).
- On the DataNode, deletes run on per-volume thread pools,
  `dfs.datanode.fsdatasetasyncdisk.max.threads.per.volume` = `4`: "These threads consume I/O and
  CPU at the same time. This will affect normal data node operations." (`hdfs-default.xml:3091`).
  The pool's queue is an unbounded `LinkedBlockingQueue` (`FsDatasetAsyncDiskService.java:120–123`).
- **DERIVED:** with defaults, a DataNode is told to delete at most 1000 blocks per 3 s heartbeat,
  about 333 blocks/s; the limit counts blocks, not bytes.

### 3.4 The namespace side: a lock budget

Removing a deleted file's blocks from the namespace is asynchronous (`markedDeleteQueue`,
`MarkedDeleteBlockScrubber`, `BlockManager.java:5308–5331`). The scrubber holds the lock at most
`dfs.namenode.block.deletion.lock.threshold.ms` = `50` ("The limit of single time lock holding
duration for the block asynchronous deletion thread.") and then sleeps
`dfs.namenode.block.deletion.unlock.interval.ms` = `10` ("The sleep interval for yield lock.")
(`hdfs-default.xml:6374`, `:6383`).

### 3.5 Trash and the startup delay

- `fs.trash.interval` = `0`: "Number of minutes after which the checkpoint gets deleted. If zero,
  the trash feature is disabled." (`core-default.xml:1094`). With trash on, "files removed by FS
  Shell is not immediately removed from HDFS. Instead, HDFS moves it to a trash directory ... The
  file can be restored quickly as long as it remains in trash." and "Note that there could be an
  appreciable time delay between the time a file is deleted by a user and the time of the
  corresponding increase in free space in HDFS." (`HdfsDesign.md`, "File Deletes and Undeletes")
- `dfs.namenode.startup.delay.block.deletion.sec` = `0`: "The delay in seconds at which we will
  pause the blocks deletion after Namenode startup. By default it's disabled. In the case a
  directory has large number of directories and files are deleted, suggested delay is one hour to
  give the administrator enough time to notice large number of pending deletion blocks and take
  corrective action." (`hdfs-default.xml:3563`; `InvalidateBlocks.java:271–278`)

### 3.6 Leases: soft limit and hard limit

- Soft limit, 60 s: "Until the soft limit expires, the writer has sole write access to the file. If
  the soft limit expires and the client fails to close the file or renew the lease, another client
  can preempt the lease." `LEASE_SOFTLIMIT_PERIOD = 60 * 1000` (`HdfsConstants.java:163–172`). A
  new writer whose predecessor "has not renewed in the last SOFTLIMIT period" starts lease recovery
  (`FSNamesystem.java:2982–2986`).
- Hard limit, 1 hour through 3.2: `LEASE_HARDLIMIT_PERIOD = 60 * LEASE_SOFTLIMIT_PERIOD`; "If after
  the hard limit expires and the client has failed to renew the lease, HDFS assumes that the client
  has quit and will automatically close the file on behalf of the writer, and recover the lease."
  (HDFS-3.2 `HdfsConstants.java:114–124`)
- Hard limit since 3.3.0: `dfs.namenode.lease-hard-limit-sec` = `1200`, "Determines the namenode
  automatic lease recovery interval in seconds." (`hdfs-default.xml:6501`;
  `HdfsClientConfigKeys.java:275–276`). HDFS-14758's reasoning: "The hard limit is currently
  hard-coded to be 1 hour. This also determines the NN automatic lease recovery interval. Something
  like 20 min will make more sense." and "However, there is one risk in reducing the hard limit.
  E.g. Reduced to 20 min. If the NN crashes and the manual failover takes more than 20 minutes,
  clients will abort." The issue's text speaks of a "5 min soft limit"; the constant in both
  versions read is 60 s.
- Recovery rolls forward to what every replica holds: "p computes the minimum block length" and
  "Namenode updates the BlockInfo", then "removes f from the lease" (`LeaseManager.java:64–79`).
- **DERIVED:** a lease that is renewed can be held indefinitely, so neither limit bounds how long
  a live writer keeps a file open. HDFS does not need that bound for garbage collection (§3.1).

## 4. Ceph RGW

### 4.1 Deferred deletion of tail data, and why [CEPH]

- "The Ceph Object Gateway allocates storage for new objects immediately. The Ceph Object Gateway
  purges the storage space used for deleted and overwritten objects in the Ceph Storage cluster
  some time after the gateway deletes the objects from the bucket index. The process of purging the
  deleted object data from the Ceph Storage cluster is known as Garbage Collection or GC."
  (`config-ref.rst`, "Garbage Collection Settings")
- `rgw_gc_obj_min_wait`, default `2_hr`: "The length of time (in seconds) that the RGW collector
  will wait before purging a deleted object's data. RGW will not remove object immediately, as
  object could still have readers. A mechanism exists to increase the object's expiration time when
  it's being read. The recommended value of its lower limit is 30 minutes" (`rgw.yaml.in`)

### 4.2 The collector's schedule and knobs (`rgw.yaml.in` at v20.2.4)

| Option | Default | Documented meaning |
|---|---|---|
| `rgw_gc_processor_period` | `1_hr` | "The amount of time between the start of consecutive runs of the garbage collector threads. If garbage collector runs takes more than this period, it will not wait before running again." |
| `rgw_gc_max_objs` | `32` | "Number of shards for garbage collector data" |
| `rgw_gc_processor_max_time` | `1_hr` | "Garbage collection thread in RGW process holds a lease on its data shards. ... RGW takes a lease in order to prevent multiple RGW processes from handling the same objects concurrently. ... In the case where RGW goes down uncleanly, this is the amount of time where processing of that data shard will be blocked." |
| `rgw_gc_max_concurrent_io` | `10` | "The maximum number of concurrent IO operations that the RGW garbage collection thread will use when purging old data." |
| `rgw_gc_max_trim_chunk` | `16` | "Max number of keys to remove from garbage collector log in a single operation" |
| `rgw_gc_max_queue_size` | `131068_K` | "The maximum allowed size of each gc queue" |
| `rgw_lifecycle_work_time` | `00:00-06:00` | "Local time window in which the lifecycle maintenance thread can work." |

- "Garbage collection is a background activity that may execute continuously or during times of
  low loads, depending upon how the administrator configures the Ceph Object Gateway. By default,
  the Ceph Object Gateway conducts GC operations continuously." "Some workloads may temporarily or
  permanently outpace the rate of garbage collection activity. This is especially true of
  delete-heavy workloads, where many objects get stored for a short period of time and then
  deleted." For those, the suggested first step is `rgw_gc_max_concurrent_io = 20` and
  `rgw_gc_max_trim_chunk = 64`, then: "please monitor for performance of the cluster during
  Garbage Collection to verify no adverse performance issues due to the increased values."
  (`config-ref.rst`)

### 4.3 Finding orphans

- "Orphans are RADOS objects that are left behind after their associated RGW objects are removed.
  Normally these RADOS objects are removed automatically, either immediately or through a process
  known as "garbage collection". Over the history of RGW, however, there may have been bugs that
  prevented these RADOS objects from being deleted" (`orphans.rst`)
- `radosgw-admin orphans find` is deprecated: "the confidence that these subcommands can accurately
  identify true orphans is presently low", and they "store intermediate results on the cluster
  itself" (`orphans.rst`). Its guard for writes in flight: `--orphan-stale-secs`, "Number of
  seconds to wait before declaring an object to be an orphan. The efault [sic] is 86400 (24
  hours)." (`radosgw-admin.rst`). The implementation skips any object whose modification time is
  within that window of the search's start: `time_threshold = search_info.start_time.sec() -
  stale_secs` and `if (stale_secs && (uint64_t)mtime >= time_threshold)` it logs "skipping"
  (CEPH-18 `rgw_orphan.cc:748`, `:795–797`). It also skips a bucket that is being resharded, logging
  "reshard in progress. Skipping" (`:522–527`).
- Its replacement, `rgw-orphan-list`: "Behind the scenes it runs `rados ls` and `radosgw-admin
  bucket radoslist ...` and produces a list of those entries that appear in the former but not the
  latter. Those entries are presumed to be the orphans." (`rgw-orphan-list.rst`). "The list of
  orphans produced should be "sanity checked" before being used for a large delete operation."
  Unindexed buckets are listed falsely as orphans (`orphans.rst`). The script: "False positives are
  possible. False positives would likely appear as objects that were never deleted and are fully
  intact. All results should therefore be verified." It lists the pool before the indexes
  (`rgw-orphan-list`, `rados_ls` then `radosgw_radoslist`).
- In-flight uploads: the v20.2.4 documents say nothing about them. **DERIVED:** an object whose
  data was listed by `rados ls` but whose index entry was not yet written when the index listing
  reached its bucket appears as an orphan; `orphans find` guarded against that with its 24-hour
  window, and `rgw-orphan-list` leaves it to the administrator.

### 4.4 Timeouts on multi-step operations

- `rgw_pending_bucket_index_op_expiration`, default `120`: "Number of seconds a pending operation
  can remain in bucket index shard before it expires. Used for transactional bucket index
  operations, and if the operation does not complete in this time period, the operation will be
  dropped."
- `rgw_mp_lock_max_time`, default `10_min`: "Time length to allow completion of a multipart upload
  operation. This is done to prevent concurrent completions on the same object with the same upload
  id."

### 4.5 Pacing deletion in the OSDs

- Legacy sleeps between removal transactions (`osd.yaml.in`): `osd_delete_sleep_hdd` = `5`,
  `osd_delete_sleep_ssd` = `1`, `osd_delete_sleep_hybrid` = `1` s ("Time in seconds to sleep
  before the next removal transaction. This throttles the PG deletion process."); snapshot
  trimming `osd_snap_trim_sleep_hdd` = `5`, `_ssd` = `0`, `_hybrid` = `2`. Each: "This setting is
  ignored when the mClock scheduler is used."
- mClock classes: the class "Background best-effort" covers "Internal backfill, scrub, snap trim
  and PG deletion requests". The default *balanced* profile gives client 50% reservation, weight
  1, no limit; background recovery 50%, weight 1, no limit; background best-effort 5%, weight 2,
  limit 90% (`mclock-config-ref.rst`). Capacity is measured: "The OSD capacity in terms of total
  IOPS is determined automatically during OSD initialization. This is achieved by running the OSD
  bench tool", with a fallback to a default when the result exceeds
  `osd_mclock_iops_capacity_threshold_hdd` = 500 or `_ssd` = 80000.

## 5. Windows Azure Storage and Giza

### 5.1 WAS: extents are collected by the Stream Manager

- "A stream is an ordered list of pointers to extents which is maintained by the Stream Manager."
  "A new stream can be constructed by concatenating extents from existing streams, which is a fast
  operation since it just updates a list of pointers." [WAS §4, p. 146]
- The SM is responsible for "(e) garbage collecting extents that are no longer pointed to by any
  stream". "The SM periodically polls (syncs) the state of the ENs and what extents they store."
  [WAS §4.1, p. 146]. "When an extent is no longer referenced by any stream, the SM garbage
  collects the extent and notifies the ENs to reclaim the space." "The SM does not know anything
  about blocks, just streams and extents." [WAS §4.1, p. 147]
- Garbage inside extents is the partition layer's: after a timed-out append is retried, "For the
  row data and blob data streams, for duplicate writes, only the last write will be pointed to by
  the RangePartition data structures, so the prior duplicate writes will have no references and
  will be garbage collected later." [WAS §4.2, p. 147]. "the partition server will periodically
  combine the checkpoints into larger checkpoints, and then remove the old checkpoints via garbage
  collection." [WAS §5.4, p. 151]
- Cost: "An append-based system comes with certain costs. An efficient and scalable garbage
  collection (GC) system is crucial to keep the space overhead low, and GC comes at a cost of extra
  I/O." [WAS §8, p. 155]. "garbage collect a RangePartition" is one of the operations the stress
  tests trigger [WAS §8, p. 156].
- **DERIVED:** the stream layer learns an extent is unreferenced because the SM holds both the
  extents and the only pointers to them, in one Paxos state machine, and allocates every extent
  before it is written. The partition layer's garbage within extents is reclaimed by rewriting and
  dropping extents from streams; the paper gives no detail. A grace period before extent GC:
  **UNVERIFIED** (not stated).

### 5.2 WAS: throttling maintenance, with a floor [WASEC §4.4, PDF pp. 8–9]

"The stream layer handles a large mix of I/O types at a given time: on-demand open/close, read,
and append operations from clients, create, delete, replicate, reconstruct, scrub, and move
operations generated by the system itself, and more. Letting all these I/Os happen at their own
pace can quickly render the system unusable. To make the system fair and responsive, operations
are subject to throttling and scheduling at all levels of the storage system. Every EN keeps track
of its load at the network ports and on individual disks to decide to accept, reject, or delay I/O
requests." The SM decides "when to initiate replication, erasure coding, deletion, and various
other system maintenance operations". And: "it is also important to make sure erasure coding is
keeping up with the incoming data rate from customers as well as internal system functions such as
garbage collection. ... Therefore, the erasure coding needs to be scheduled such that it keeps up
with the incoming rate of data".

### 5.3 Giza [GIZA]

- Orphans from a write whose data landed and whose metadata did not: "In one uncommon case, the
  data path succeeds, while the metadata path fails. Now, the fragments stored in the cloud blobs
  become orphans. Giza will eventually delete these fragments and reclaim storage through a
  cleaning process, which first executes Paxos to update the current version to no-op, discovers
  the orphan fragments as not being referenced in the metadata store, and then removes the fragments
  from the corresponding blob storage in all the DCs." [GIZA §3.3, p. 545]
- Order of reclaiming, for the same reason as mantle's: "1) fetching the metadata corresponding to
  the version to be garbage collected, 2) deleting the fragments in the blob storage, and 3)
  removing the columns of the deleted version from the metadata table row. The second step has to
  occur before the third one in case that the garbage collection process is interrupted and the
  fragments may become “orphans” without proper metadata pointing to them in the table storage."
  [GIZA §3.4, p. 545]
- Removing an object's last rows races a new put, so Giza uses two phases: "In the first phase, it
  marks the rows in all the DCs as confined. After this any other get or put operations are
  temporarily disabled for this object. In the second phase, all the rows are actually removed from
  the table storage. The disadvantage of this approach is obvious. It requires all the data centers
  to be online." [GIZA §3.4, p. 545]
- Workload: objects deleted within a year of creation "account for 26.5% of the total consumed
  storage capacity" in OneDrive [GIZA §2.2, p. 541]; garbage collection is one of the three design
  challenges named [GIZA §3.1, p. 543].

Azure papers other than WAS, WASEC and Giza were not checked.

## 6. Grace periods set by another process

- **Cassandra.** "To prevent the reappearance of zombies, Cassandra gives each tombstone a grace
  period. The grace period for a tombstone is set with the table property `WITH
  gc_grace_seconds`. Its default value is 864000 seconds (ten days), after which a tombstone
  expires and can be deleted during compaction." "The purpose of the grace period is to give
  unresponsive nodes time to recover and process tombstones normally." "If a node remains down or
  disconnected for longer than `gc_grace_seconds`, its deleted data will be repaired back to the
  other nodes and reappear in the cluster." (`tombstones.adoc:37–45`, `:67`; the ten-day default is
  `gcGraceSeconds = 864000` at `TableParams.java:360`). Repair cadence, which the grace must
  cover: "running an incremental repair every 1-3 days, and a full repair every 1-3 weeks is
  probably reasonable. If you don't want to run incremental repairs, a full repair every 5 days is
  a good place to start." (`repair.adoc:70–75`). **DERIVED:** the grace is sized from two other
  bounds: the longest a replica stays down, and the repair period.
- **Spanner.** "Version garbage collection reclaims versions after they expire past a database's
  version_retention_period, which defaults to 1 hour, but can be configured up to 1 week. This
  restriction also applies to in-progress reads or SQL queries with timestamps that become too old
  while executing. Reads and SQL queries with too-old read timestamps fail with the error
  FAILED_PRECONDITION." (SPANNER, "Timestamp bounds"). PITR: "By default, your database retains all
  versions of its data and schema for one hour. You can increase this time limit to as long as
  seven days" (SPANNER, "Point-in-time recovery"). A read that outlives the retention window is
  refused.
- **Riak.** `delete_mode`: "How long to wait until the tombstone is removed, expressed in
  milliseconds. The default is 3000, i.e. to wait 3 seconds". "we recommend setting the
  delete_mode parameter to keep if you plan to delete and recreate objects under the same key. This
  protects against failure scenario cases in which a deleted object may be resurrected." (RIAK).
  **DERIVED:** the 3-second default rests on no bound on another process, and the documentation
  itself advises against reaping where resurrection matters.
- **Dynamo.** "Using this reconciliation mechanism, an “add to cart” operation is never lost.
  However, deleted items can resurface." [DYN §4.4, p. 210]. Versions subsumed by a descendant "can
  be garbage collected" [DYN §4.4, p. 211]. The paper gives no tombstone grace.

## 7. Sweeps and writes in flight

### 7.1 One authority holding every root: Bigtable

"The master is responsible for ... garbage collection of files in GFS." [BT06 §5, p. 208]. "Since
SSTables are immutable, the problem of permanently removing deleted data is transformed to garbage
collecting obsolete SSTables. Each tablet's SSTables are registered in the METADATA table. The
master removes obsolete SSTables as a mark-and-sweep garbage collection over the set of SSTables,
where the METADATA table contains the set of roots." [BT06 §6, p. 212]. How the sweep avoids an
SSTable a compaction has written but not yet registered: **UNVERIFIED** (not in the paper).

### 7.2 A time threshold without a bound on the writer: git, Ceph `orphans find`

- git: "when 'git gc' runs concurrently with another process, there is a risk of it deleting an
  object that the other process is using but hasn't created a reference to. This may just cause the
  other process to fail or may corrupt the repository if the other process later adds a reference
  to the deleted object. Git has two features that significantly mitigate this problem". The two
  items of the list that follows: "Any object with modification time newer than the `--prune` date
  is kept, along with everything reachable from it." and "Most operations that add an object to
  the database update the modification time of the object if it is already present so that #1
  applies." Then: "However, these features fall short of a complete solution, so users who run
  commands concurrently have to live with some risk of corruption (which seems to be low in
  practice)." (`git-gc.adoc:153–169`). The window: `gc.pruneExpire` defaults to `2.weeks.ago`;
  "This feature helps prevent corruption when 'git gc' runs concurrently with another process
  writing to the repository" (`config/gc.adoc:96–105`).
- Ceph `orphans find`: 24 hours by modification time (§4.3).
- **DERIVED:** both are mitigations. Nothing stops a writer from taking longer than the window and
  then publishing a reference to an object the sweep removed.

### 7.3 Registering outputs in flight: RocksDB

"For each background job, pending_outputs_ keeps the current file number at the time that
background job started. FindObsoleteFiles()/PurgeObsoleteFiles() never deletes any file that has
number bigger than any of the file number in pending_outputs_. Since file numbers grow
monotonically, this also means that pending_outputs_ is always sorted. After a background job is
done executing, its file number is deleted from pending_outputs_, which allows
PurgeObsoleteFiles() to clean it up." (`db_impl.h:3380–3389`). "This will protect any file with
number `file_num` or greater from being deleted while <do something> is running."
(`db_impl.h:2282–2283`). **DERIVED:** the fence is exact because the sweeper and the writers share
one process and one monotone counter.

### 7.4 Fencing the reference: Giza

The cleaner writes a no-op into the version slot the stalled put would have committed, then deletes
the fragments nothing references (§5.3). **DERIVED:** the no-op and the stalled put contend for the
same Paxos instance, so only one of them can be chosen, and no time bound on the put is needed.

### 7.5 S3 multipart uploads

- "After you initiate a multipart upload, Amazon S3 retains all the parts until you either complete
  or stop the upload. Throughout its lifetime, you are billed for all storage, bandwidth, and
  requests for this multipart upload and its associated parts." (S3 `mpuoverview`)
- An abort races parts in flight: "if any part uploads are currently in progress, those part
  uploads might or might not succeed. As a result, it might be necessary to abort a given multipart
  upload multiple times in order to completely free all storage consumed by all parts."
  (S3 `API_AbortMultipartUpload`); "To make sure you free all storage consumed by all parts, you
  must stop a multipart upload only after all part uploads have completed." (S3 `mpuoverview`)
- The bound S3 offers is a lifecycle rule: "Amazon S3 supports a bucket lifecycle rule that you can
  use to direct Amazon S3 to stop multipart uploads that aren't completed within a specified number
  of days after being initiated." The example uses `<DaysAfterInitiation>7</DaysAfterInitiation>`
  (S3 `mpu-abort-incomplete-mpu-lifecycle-config`).
- S3's own deletion is queued: "Amazon S3 queues the object for removal and removes it
  asynchronously"; "There may be a delay between the expiration date and the date at which Amazon
  S3 removes an object. You are not charged for expiration or the storage time associated with an
  object that has expired." Eligibility is checked again when the action runs: "At execution time,
  Amazon S3 re-evaluates the object's current tags." (S3 `lifecycle-expire-general-considerations`)

### 7.6 Haystack and f4

- Haystack: "A Store machine sets the delete flag in both the in-memory mapping and synchronously in
  the volume file." "Note that the space occupied by deleted needles is for the moment lost."
  [HAY §3.4.3, pp. 52–53]. "Compaction is an online operation that reclaims the space used by
  deleted and duplicate needles ... During compaction, deletes go to both files. Once this procedure
  reaches the end of the file, it blocks any further modifications to the volume and atomically
  swaps the files and in-memory structures." "Over the course of a year, about 25% of the photos get
  deleted." [HAY §3.6.1, p. 54]
- f4's router: "In case of any errors, any partially written data is ignored to be garbage
  collected later, and a new logical volume is picked for the create. For deletes, the router
  issues deletes to all physical replicas of a BLOB. Responses are handled asynchronously and the
  delete is continually retried until the BLOB is fully deleted in case of failure." [F4 §4.1,
  p. 387]. How the partially written data is found: **UNVERIFIED** (not described).
- f4 deletes by key: "Deleting the encryption key for a BLOB in f4 logically deletes it by making it
  unreadable." [F4 §5.3, p. 388]; "This renders the BLOB unreadable and effectively deletes it
  without requiring the use of compaction in f4." [F4 p. 389]. Deletes concentrate on new data:
  "most deletes are for young BLOBs" [F4 p. 386]; for expiry-driven content, "The hot storage system
  copes with the high delete rate by running compaction frequently to reclaim the now available
  space." [F4 p. 388]

### 7.7 Colossus

Google's post names "background storage managers called Custodians" that handle "tasks like disk
space balancing and RAID reconstruction" (COLOSSUS). It says nothing of garbage collection or
grace periods: **UNVERIFIED**. Third-party posts that attribute garbage collection to custodians
were not used.

### 7.8 What bounds the time from creating an object to referencing it

| System | Bound or fence | How the sweep uses it |
|---|---|---|
| GFS | chunk handle assigned by the master at creation | only replicas "not known to the master" are garbage (§1.5) |
| HDFS | `addBlock()` allocates the block before data | unknown reported blocks are deleted; lagging inventories postpone (§3.1–§3.2) |
| WAS | SM creates and assigns extents | extents no stream points to (§5.1) |
| Giza | Paxos no-op in the version slot | fence first, then delete what is unreferenced (§5.3) |
| RocksDB | `pending_outputs_` file-number floor | never delete at or above the smallest pending number (§7.3) |
| Ceph `orphans find` | 24 h by mtime | skip younger objects; writers unbounded (§4.3) |
| git | 2 weeks by mtime | "fall short of a complete solution" (§7.2) |
| S3 multipart | lifecycle `DaysAfterInitiation` | parts billed until then; abort races parts in flight (§7.5) |
| Spanner | version retention, 1 h default | too-old readers fail `FAILED_PRECONDITION` (§6) |
| Tectonic, Bigtable, f4 | not published | **UNVERIFIED** |

## 8. Pacing background deletion against foreground I/O

### 8.1 Published limits

| System | Limit | Default |
|---|---|---|
| HDFS | blocks per DataNode per heartbeat (`dfs.block.invalidate.limit`) | 1000 per 3 s |
| HDFS | DataNodes given deletion work per iteration | ⌈0.32 × live⌉ every 3 s |
| HDFS | NameNode lock per asynchronous deletion pass, then yield | 50 ms, then 10 ms |
| HDFS | DataNode deletion threads per volume | 4 |
| Ceph RGW | GC cycle; shards; concurrent IOs; keys per trim | 1 h; 32; 10; 16 (20 and 64 suggested for delete-heavy loads) |
| Ceph RGW | lifecycle window | 00:00–06:00 local |
| Ceph OSD (legacy) | sleep between removal transactions | 5 s HDD, 1 s SSD, 1 s hybrid |
| Ceph OSD (mClock, *balanced* profile) | background best-effort (PG deletion, snap trim, scrub, backfill) | 5% reservation, weight 2, 90% limit of measured capacity |
| RocksDB | deletion rate `rate_bytes_per_sec` | 0 (off); `max_trash_db_ratio` 0.25; `bytes_max_delete_chunk` 64 MiB |
| Tectonic | background class (Bronze), WRR, per-disk non-Gold in-flight cap | values not published |

GFS gives no numbers; its collector runs in batches "only when the master is relatively free"
(§1.2).

### 8.2 SSD discards

- RocksDB: "File deletions can be rate limited in order to prevent the resultant IO from
  interfering with normal DB operations." "A common reason for doing this is to prevent latency
  spikes on flash devices. A high rate of file deletion can cause excessive number of TRIM commands
  issued to the device, which can cause read latencies to spike." `max_trash_db_ratio` "specifies
  the upper limit on trash size to live DB size ratio that can be tolerated before files are
  immediately deleted, overriding the rate limit"; `bytes_max_delete_chunk` "limits the size of
  TRIM command requests sent to the flash device and helps read latencies." (ROCKS-WIKI). The
  scheduler "apply sleep penalty between deletes if they are happening in a rate faster than
  rate_bytes_per_sec" (`delete_scheduler.h`); defaults at `sst_file_manager.h:120–128`.
- fstrim(8): "Running fstrim frequently, or even using mount -o discard, might negatively affect
  the lifetime of poor-quality SSD devices. For most desktop and server systems a sufficient
  trimming frequency is once a week. Note that not all devices support a queued trim, so each trim
  command incurs a performance penalty on whatever else might be trying to use the disk at the
  time." (`fstrim.8.adoc:25`)

### 8.3 Peer-reviewed treatments

- **Freeblock scheduling.** "By filling rotational latency periods with useful media transfers,
  20–50% of a never-idle disk's bandwidth can often be provided to background applications with no
  effect on foreground response times." "Free segment cleaning often allows an LFS file system to
  maintain its ideal write performance when cleaning overheads would otherwise reduce performance
  by up to a factor of three." [FBS00 abstract, PDF p. 1]. It depends on rotational positioning, so
  it applies to disks with heads only (DERIVED).
- **Aqueduct.** "Aqueduct, uses a control-theoretical approach to statistically guarantee a bound
  on the amount of impact on foreground work during a data migration, while still accomplishing the
  data migration in as short a time as possible." [AQ abstract, p. 219]. The controller samples
  each store's latency over a period W and sets the next period's rate by an integral law,
  `Rm(k) = Rm(k-1) + K∗Emin(k)`, aiming at a reference `P∗LC` just below the latency contract `LC`
  [AQ §3, p. 223]. Results: "Sub-store violates the latency contract in 70% (±0%) of all sampling
  periods during migration, while Aqueduct only violates 17% (±5%)" at the price of time: "1219
  (±43) sec on average to complete the migration plan, while Sub-store only needs 556 (±3) sec"
  [AQ §5.3–§5.4, p. 226]. "Aqueduct reduces the average I/O latency experienced by client
  applications by as much as 76% with respect to the traditional method" and "reduces the violation
  fraction from 100% to only 12%" [AQ §7, p. 229].
- **mClock.** "Our algorithm, mClock, supports proportional-share fairness subject to minimum
  reservations and maximum limits on the IO allocations for VMs." [MCLK abstract, PDF p. 1]. Ceph's
  profiles in §4.5 are this algorithm applied to background classes.
- **Idle time.** Recorded in note 11 and not repeated at length: Sprite LFS starts cleaning below
  "a few tens of segments" and stops above "50-100 clean segments", and its performance "does not
  seem to be very sensitive to the exact choice of the threshold values" [RO92 §3.4]; "With a
  simple heuristic of cleaning whenever the disk has been idle for two seconds, we can virtually
  eliminate any user-perceived cleaning latency" [BHS95 Conclusions];
  "if idle times have low variability, then idle waiting is not necessary. Only if idle times are
  highly variable does idle waiting become necessary" [MRZ+09 abstract]; the Waiting policy of
  AOS12 and GBS95's idleness detectors (note 11 §10.2, §12.3).

## 9. Multi-step operations left by a failed coordinator

- **Percolator.** "Percolator takes a lazy approach to cleanup: when a transaction A encounters a
  conflicting lock left behind by transaction B, A may determine that B has failed and erase its
  locks. It is very difficult for A to be perfectly confident in its judgment that B is failed; as a
  result we must avoid a race between A cleaning up B's transaction and a not-actually-failed B
  committing the same transaction. Percolator handles this by designating one cell in every
  transaction as a synchronizing point for any commit or cleanup operations." [PERC §2.2, PDF p. 5].
  Direction: "if the primary lock has been replaced by a write record, the transaction which wrote
  the lock must have committed and the lock must be rolled forward, otherwise it should be rolled
  back". Timing only affects cost: "Since cleanup is synchronized on the primary lock, it is safe to
  clean up locks held by live clients; however, this incurs a performance penalty since rollback
  forces the transaction to abort. So, a transaction will not clean up a lock unless it suspects
  that a lock belongs to a dead or stuck worker." Suspicion: "Running workers write a token into the
  Chubby lockservice to indicate they belong to the system", and "we additionally write the wall
  time into the lock; a lock that contains a too-old wall time will be cleaned up even if the
  worker's liveness token is valid. To handle long-running commit operations, workers periodically
  update this wall time while committing." [PERC §2.2, PDF p. 6]. The cost of laziness: "This lazy,
  simple-to-implement approach potentially delays transaction commit by tens of seconds." [PERC §2,
  PDF p. 2]. The "too-old" threshold is not published: **UNVERIFIED**.
- **HDFS.** Another writer may preempt after the 60 s soft limit; the NameNode recovers after the
  hard limit, 20 min since 3.3.0 and 1 h before, rolling forward to the shortest replica (§3.6).
- **Ceph RGW.** A pending bucket-index operation is dropped after 120 s; a multipart completion lock
  lasts 10 min; a GC shard lease 1 h, during which a crashed collector blocks its shard (§4.2,
  §4.4).
- **GFS.** A lease that loses its holder is regranted after it expires, 60 s initially (§1.5).
- **Tectonic.** A backpointer detects a pending move and a collector repairs failed multi-step
  operations (§2.2). Timeouts: **UNVERIFIED**.
- **Giza.** The two-phase row removal waits rather than times out: "Data center failure or network
  partition may pause the process and make the row unavailable (but can still continue after data
  center recovers or network partition heals)." [GIZA §3.4, p. 545]
- **S3.** A conflicting operation in progress answers `OperationAborted`, and a deleted bucket is
  queued for deletion with no published time bound (note 05 §10.5, §11).

## 10. Consequences for mantle

### 10.1 What the sweep between layers rests on

mantle writes data first and gives every object one referrer: chunks, then Block rows, then File
rows, then the Name commit, with 128-bit file IDs the gateway chooses (metadata.md §2;
`crates/meta/src/file.rs`). So, unlike GFS, HDFS and WAS (§7.8), an object below the Name commit
is known to no range above it while it is in flight, and the sweep cannot tell "in flight" from
"abandoned" by references alone. The published choices are a fence (Giza, RocksDB) or a time
threshold, and a threshold that nothing enforces on the writer is only a mitigation (git, Ceph).
Each item below is **DERIVED** from the facts cited in it.

1. **Stamp and back-reference.** Each object below the Name layer should record the range's time
   when its row committed and the one referrer it was made for. Chunk records already do both:
   `block id` and `write time (Unix ns)` (chunk-store §3.1). `FileHeader` holds only length and
   extent count, and `BlockHeader` only length, `k`, `m`, chunk length and CRC
   (`crates/meta/src/record.rs`). A file would name the write it was made for (bucket incarnation,
   key, and the version or upload part it is to become); a block would name its file. Then the sweep
   asks one range per object instead of marking a whole namespace, which only a single authority
   can do (GFS, Bigtable §7.1).
2. **A handover deadline the referrer enforces.** The Name range refuses a handover whose file
   stamp is more than `D_f` older than the range's time at commit, and releases the file into its
   queue as it releases any refused write's file (metadata.md §2). The File range applies `D_b` to
   blocks a file write names, and the Block range `D_c` to chunks a block write names. This is
   Spanner's rule for readers (§6) applied to writers: an operation older than the horizon is
   refused, never half-applied.
3. **The sweep fences through the referrer's log.** The sweep does not delete. It sends the
   referrer range a command that releases the object if it is still unreferenced and older than `D`
   at that range's time. The range's log orders it against the handover: whichever commits first
   wins, and a later handover fails item 2. This is Giza's no-op in the version slot (§5.3) built
   from mantle's time rule, and it reuses the queue and the reclaimer, whose steps are idempotent
   and resumable (`reclaim.rs`). The File and Block ranges need the same release path for orphan
   blocks, and the Block range for orphan chunks. A linearizable read that returns both the
   reference state and the range's last assigned time decides the same question, since every
   later write takes a later time; the command has the advantage of putting the object into the
   queue the reclaimer already works from.
4. **Only monotone range time is needed for safety.** Both the refusal and the sweep's test compare
   the stamp with the same referrer range's time, which never moves backward (`clock.rs`: the later
   of the proposal and the last time plus one). So a clock offset `ε` between the stamping range and
   the referrer cannot make the sweep take a live object. It only makes an honest handover fail
   when `h + ε > D` for handover time `h`. A split must give each side the parent's clock and a
   merge the later of the two.
5. **No decision on a lagging view.** A reference check that decides anything runs in the
   referrer's log (item 3) or as a linearizable read at its leader (ReadIndex, note 06). A
   follower's or a cache's view may only pick candidates. HDFS postpones deletion for the same
   reason when a report may lag the namespace (§3.2).
6. **The chunk level.** Reconcile each volume's chunks with the Block layer's reverse rows for that
   volume, as Tectonic's repair reconciles "the chunk list to the disk-to-block map" (§2.1) and GFS
   and HDFS reconcile heartbeats and block reports. Take chunks that no Block row names and whose
   write time is older than `D_c`. The same pass removes chunks of blocks reclaimed or moved while
   their disk was away, and chunks whose record `epoch` is older than the block's layout generation,
   the counterpart of GFS's version numbers (§1.4).
7. **Part files.** A part adopted by a completion is referenced from the object's file, not from a
   Name row, so the check for a part file follows the completed version to its file (one more read).

### 10.2 What mantle must bound

**DERIVED**, except where a value is cited.

| Bound | Covers | Enforced by | Value from |
|---|---|---|---|
| `D_f` | File-row commit to Name handover | Name range refuses, file released | measured: the gateway's retry budget for the step × a high quantile of one try's latency, plus `ε` |
| `D_b` | Block-row commit to File-row commit | File range refuses | measured, as `D_f` |
| `D_c` | chunk write to Block-row commit | Block range refuses | measured; buffering a block before writing its chunks (note 01, D2) keeps this a storage-node write, not a client upload |
| `ε` | offset between the clocks compared | clock monitoring | measured; note 06 §A4.3 records CockroachDB's 500 ms default maximum offset and self-termination |
| `R` | longest read of chunks after the Name read that chose them | gateway deadline on GetObject and HeadObject | measured |
| `P` | an attempt's longest time without progress | collector takes over | per-step deadline × step retry budget, plus `ε` (§10.5) |
| `L` | a collector's hold on one range's queue | a lease row in the range | measured time of one batch, with margin |
| `Q` | released files eligible but not yet reclaimed | pacing floor (§10.3) | capacity headroom |
| sweep page and period | the orphan sweep's scan | budget per page; period | leaked-bytes bound and the measured orphan rate |

- Stamping at commit, after the object's data is durable, keeps each `D` to one or two range
  commits, so no `D` grows with object size or client bandwidth. A multipart upload's lifetime is
  bounded separately, by lifecycle rules, as in S3 (§7.5).
- Published values for comparable windows between a prepared step and its completion: Ceph's
  pending bucket-index entry, 120 s (§4.4); HDFS's soft limit, 60 s (§3.6). They show the order
  of magnitude others chose. mantle's values are measured on the running system.
- The deadlines are absolute. A renewable lease would not bound a handover, since GFS's and HDFS's
  leases can be extended indefinitely (§1.5, §3.6).
- mantle's design already avoids the multipart race S3 documents (§7.5): an UploadPart for an
  aborted upload is refused at the Name range and its file released, so no part outlives an abort
  and no second abort is needed.

### 10.3 Scheduling and pacing the collector

**DERIVED** throughout.

1. **When.** The queue is keyed by release time (`name.rs`, `release`), so the next eligible row
   says when work is due: the oldest row's time plus the grace. The collector can wait until then
   instead of polling on a period, as GFS and Ceph must because they scan (§1.1, §4.2). The orphan
   sweep scans, so it has a period: HDFS takes a full inventory every 6 h (§3.1). mantle's comes
   from a bound on leaked bytes (leak rate × (period + `D`)) with the leak rate measured by the
   sweep itself, since orphans arise only from failed gateways and nodes.
2. **Who.** One collector per Name range at a time, holding a lease in the range, as Ceph leases
   GC shards (§4.2) and Tectonic's services work one shard at a time (§2.1). Overlap is harmless
   because every step is idempotent, so the lease only prevents duplicate work, and `L` can be
   short: Ceph's 1-hour lease blocks a shard for an hour after an unclean crash (§4.2).
3. **How much per step.** Pages from `released(before, max)`, oldest first, as GFS batches (§1.2)
   and Ceph trims 16 keys per operation (§4.2). A page is bounded by the largest entry a range
   takes (metadata.md §6). Chunk deletes are grouped per volume.
4. **How fast.** The ceiling comes from foreground latency: an integral controller in the manner
   of Aqueduct (§8.3) on the measured latency of the ranges and volumes the collector touches, with
   its I/O classed as background at the storage node, as Tectonic's Bronze class and mClock's
   best-effort class are (§2.3, §4.5). The floor comes from `Q`: when the backlog of eligible rows
   passes it, the floor rises until the queue drains. WAS schedules erasure coding so that it
   "keeps up with the incoming rate of data" (§5.2), RocksDB deletes at once past a trash ratio
   (§8.2), and Ceph documents workloads that "outpace the rate of garbage collection" (§4.2).
   Without a floor the queue has no bound; without a ceiling, deletion bursts reach reads (§8.2).
5. **What it costs.** A reclaim is range commands (Block, File, Name) and chunk `Delete` records in
   volume index logs. The space comes back later through the segment cleaner, which frees and
   discards whole segments (chunk-store §8). So the collector's rate is paced in range commands and
   chunk deletes per volume, both measured. Discards are already batched per segment, which is the
   batching RocksDB and fstrim advise (§8.2).
6. **Idle time.** On devices whose measured idle intervals are variable enough, run in idle periods
   (MRZ+09; note 11 §12.4). Freeblock scheduling applies to rotating disks only, and to the cleaner
   more than to the collector (§8.3).
7. **Repair.** Repair ranks blocks of released files below live ones, as GFS does (§1.3).

### 10.4 The grace period

- Three days by default, after GFS (§1.1; metadata.md §2), is a recovery-point policy, not a
  safety bound. GFS names its cost, "the delay sometimes hinders user effort to fine tune usage
  when storage is tight", and its knobs: expedite a second explicit delete, and per-subtree policy
  (§1.3). Its Table 2 shows the grace can hold a third of a cluster's files (§1.3).
- **DERIVED:** the floor for any configured grace is `R + ε`. Ceph waits because "object could
  still have readers" and recommends at least 30 minutes (§4.1); Spanner fails readers older than
  its window (§6). mantle should refuse a configured grace below `R + ε`, and a read that outlives
  it should fail with a typed error.
- **DERIVED:** the grace is measured in range time. A leader clock ahead by `x` pushes range time
  ahead for good (`clock.rs`), which shortens every pending grace by `x`. Offset monitoring of the
  kind note 06 §A4.3 records bounds `x`.
- **DERIVED:** Cassandra's reason for its grace (§6) does not carry over to the queue, because a
  release is a row in a Raft range rather than a message to replicas. It does carry over to chunks,
  which sit on several volumes without consensus. A volume that was away when its chunk was
  deleted still holds the chunk, and item 6 of §10.1 removes it. It is never referenced again,
  because references live only in Block rows.

### 10.5 Taking over a stalled bucket create or delete

**DERIVED**, from Percolator, HDFS and Ceph (§9) and the design's attempts (metadata.md §2).

1. **Safety does not depend on the threshold.** Every step names its attempt, and a range refuses
   a step older than the attempt that last moved the row or gate, so taking over early cannot break
   a bucket. It only aborts a slow but live attempt, whose request learns it was "taken over by a
   later attempt" (metadata.md §2, "The coordinator"). This is Percolator's position: cleanup is
   safe against live clients and costs only an abort (§9). The threshold trades that abort against
   how long a name answers `OperationAborted`, or how long a stalled delete keeps gates closed.
2. **Direction.** The Bucket row plays the part of Percolator's primary lock
   (`BucketState` in `crates/meta/src/record.rs`). Once it is `Deleted`, the collector rolls
   forward and finishes the cleanup, as the design has it. A create still `Creating` is rolled back
   by taking it over as a delete, as the design has it. A delete still `Deleting` is taken over as
   a new attempt that runs the delete again: it deletes if the bucket is empty and reopens the
   gates if not, as a live coordinator would.
3. **When.** Progress, not start time. A delete's emptiness probe goes in pages and may pass many
   in-progress uploads, so a deadline from the start would abort live deletes. The attempt should
   record the range time of its last step where the collector can read it: on the Bucket row every
   few steps, as Percolator's workers "periodically update this wall time while committing". The
   collector takes over after `P = (per-step deadline × step retry budget) + ε` without progress,
   with the per-step deadline the same measured quantity as for `D_f`. A client's retry still takes
   over at once, as HDFS lets another client preempt a lapsed lease (§3.6).
4. **Upper side.** `P` bounds how long a bucket stays blocked after its coordinator dies. HDFS
   chose 20 minutes for its analogous window and names the risk of a pause longer than that (§3.6).
   S3 publishes no bound for its own queued bucket deletion (note 05 §10.5): **UNVERIFIED**.
5. **Finding stalled attempts.** The collector pages the Bucket range for rows in `Creating` or
   `Deleting`, with a period no longer than `P`.

### 10.6 Unverified and open

- Tectonic's collector schedule, pacing, grace, and failed-operation detection (§2.4).
- How Bigtable's sweep spares an SSTable written but not yet registered (§7.1).
- WAS's grace before extent collection; how f4 finds partially written data (§5.1, §7.6).
- Colossus's garbage collection (§7.7).
- Percolator's "too-old" wall-time threshold (§9).
- Tectonic's non-Gold in-flight limit and Gold pending threshold (§2.3).
- S3's internal reclamation timing and bucket-deletion propagation time (§7.5, §10.5).
- HDFS-14758's description speaks of a "5 min soft limit"; the code constant at both pinned
  versions is 60 s (§3.6).
- Page numbers for PERC, FBS00, MCLK and WASEC: their PDFs print none.
