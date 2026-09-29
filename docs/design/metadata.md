# Metadata: how mantle records buckets, objects and where their bytes live

Status: design, 2026-09-28. Sources: docs/research/01 (Tectonic, cited as "01 §x" and
[TEC]), 05 (S3 semantics), 06 (consensus and range-partitioned metadata), 07 (focal's
consensus stack), 09 (cells), 12 (RocksDB and ZippyDB), 18 (Object Lock), 22 (garbage
collection); docs/design/architecture.md.

The metadata service records what exists: buckets, the versions of every object, the
multipart uploads in progress, which blocks hold an object's bytes, and which chunk stores
hold each block. Bytes themselves live in chunk stores (chunk-store.md). This record states
how the metadata is laid out, how an S3 operation becomes metadata writes, and how a range
keeps its rows.

## 1. Three layers

Tectonic splits file-system metadata into a Name layer (names to files), a File layer
(files to blocks) and a Block layer (blocks to chunk locations), each partitioned and
replicated on its own, with every list "expanded" into one key per item so no update
reads and rewrites a whole list [01 §1.5, TEC Table 1]. mantle keeps the three layers and
fits the Name layer to S3.

| Layer | Key | Value | Partitioned by |
|---|---|---|---|
| Bucket | bucket | owner, creation time, location, versioning state, lifecycle state | bucket |
| Bucket | (OWNER, owner) | how many buckets the owner has | bucket |
| Bucket | (OWNER, owner, bucket) | reverse entry: the owner's bucket, its creation time and location | bucket |
| Name | (GATE, bucket), in each range | the bucket's incarnation, and whether the range admits its writes | (bucket, key) |
| Name | (bucket, key, NULL) | order of the key's null version | (bucket, key) |
| Name | (bucket, key, VERSION, order) | a version: object or delete marker | (bucket, key) |
| Name | (bucket, key, UPLOAD, upload) | a multipart upload in progress | (bucket, key) |
| Name | (bucket, key, UPLOAD, upload, part) | an uploaded part | (bucket, key) |
| Name | (RELEASED, time, file), in each range | a file nothing references any more, for the collector | the range's |
| File | (file, HEADER) | length, extent count | file |
| File | (file, EXTENT, end) | a block, or another file, and its length | file |
| Block | (block, HEADER) | length, code, chunk size, checksum | block |
| Block | (block, CHUNK, index) | the chunk store volume and chunk key holding chunk `index` | block |
| Block | (ON, volume, block) | reverse entry: which of the block's chunks `volume` holds | block |

- **Everything for one object key is contiguous.** Its null-version pointer, its versions,
  its uploads and their parts sort together under `(bucket, key)`, so a Name range splits
  only between object keys and every operation on one key stays in one range, as
  Tectonic keeps a directory's files in the directory's shard so creates, deletes and
  renames within it are consistent [01 §1.6]. Architecture decision 1 orders the Name
  layer by `(bucket, key)` for listing [architecture §11].
- **Versions sort newest first.** `order` is the version's time inverted, so the first
  version under a key is its current one: a read or a listing takes one seek, and a
  listing passes the rest of the key's versions with one more (the seek in list.rs). The
  time is the commit's, except that a completed multipart upload takes its initiation time,
  since "the current version of the object is determined by which upload started most
  recently" [05 §4.4].
- **A version ID names its place.** The ID is `order` in a URL-safe alphabet with no `+`,
  `/` or `=` [05 §7.1], so a read by version ID is one key, and IDs sort as the versions
  do. The null version, which each key has at most one of and which moves when
  overwritten [05 §7.2], is found through its pointer.
- **Files and blocks are named by random 128-bit IDs,** so ranges over them take uniform
  shares of the load, which is what Tectonic's hash partitioning buys [01 §1.5], and two
  IDs collide with probability about `n²/2^129`.
- **A file is a list of extents,** each a block or another file, keyed by where each ends,
  so one seek from any offset lands on the extent holding it. An object written by one PUT
  is a file of blocks. A completed multipart upload is a file whose extents are its parts'
  files: completing an upload of 10,000 parts writes 10,000 extents and moves no bytes and
  no block records.
- **Reverse Block entries sort by volume and live with their block.** Tectonic keys its
  reverse index `(disk_id, blk_id)` and shards it by block, so a block and its reverse
  entries change in one shard's transaction and repair works "per Block layer shard, per
  disk" [01 §1.5, TEC Table 1]. A mantle Block range spans an interval of block IDs and
  holds, under a prefix of their own, the reverse entries of its blocks keyed by volume and
  then block, so one scan lists the range's blocks on a volume. A split cuts the forward
  rows at a block ID and each volume's reverse entries at the same ID. Research note 01
  (M3) proposes virtual shards for such rows instead, which fixes at creation how many
  ranges the layer can have and makes every range seek once per virtual shard it owns;
  cutting per volume costs only when a range splits.
- **A block's chunks are on distinct volumes,** since chunks on one volume are lost
  together. The Block layer refuses a block, or a move, that would put two on one.
- **Bucket rows are few and change rarely,** at most 10,000 a tenant by default [05 §10.4].
  A cell keeps them in its own Bucket ranges, and gateways cache them. A write carries the
  bucket's incarnation and the versioning state it was made under. S3 lets a versioning
  change take "up to 15 minutes" to reach every write [05 §7.1]; mantle bounds the cache's
  staleness below that. A stale cache never lets a write into a deleted bucket, since each
  Name range admits a bucket's writes through its gate (§2).
- **An owner's buckets are counted and listed** from reverse rows sorted by owner and kept
  in the range of each bucket, as the Block layer's are by volume. A create past the
  owner's quota, 10,000 by default as in S3 [05 §10.4], is refused with `TooManyBuckets`.
  While the Bucket layer is one range, the count and the listing are exact.

## 2. Operations

Every transaction runs inside one range, as ZippyDB's do [01 §1.5]: no operation spans
ranges atomically. An operation that touches several layers writes them bottom up, blocks
before files before names, so a name is committed only once everything it points to
exists. A failure between steps leaves rows nothing points to; the collector between
layers removes them, as Tectonic's does [01 §1.6].

- **PutObject.** The gateway writes the chunks (chunk-store.md), then the Block rows, then
  the File rows, then commits the version in the Name range. The Name commit is the write's
  linearization point: preconditions (`If-Match`, `If-None-Match`) are evaluated there,
  against the key's current version at commit (05 §2.2; conditional.rs), and the version's
  order is assigned there.
- **GetObject and HeadObject** read the current version, or the one a version ID names,
  from the Name range, then the File and Block rows, then the chunks.
- **DeleteObject** writes in the Name range only: a delete marker, the null version's
  removal, or a version's removal, by the bucket's versioning state [05 §7.3]. A version
  removed leaves its file and blocks unreferenced.
- **Object Lock** (docs/research/18; s3-protocol §11). A version's retention and legal hold
  live in its row, so the step that would remove a version reads its lock in the same
  transaction. A write carries the retention and legal hold its request names and the
  bucket's default retention as the gateway read it; the default runs from the version's
  creation, a year as 365 days [18 §2.4], and a completed upload takes the lock its creation
  named. Removing a version by ID, or replacing a null version, is refused while its legal
  hold is on or its retention's date is ahead, a GOVERNANCE retention yielding to bypass
  [18 §2.2, §2.3]; a delete marker still stacks over a locked version [18 §2.1]. A
  retention may be extended by anyone who may set one, and shortened, removed or moved out of
  GOVERNANCE only under bypass; a COMPLIANCE one never changes while it lasts. A lock's expiry
  is judged at the range's time for the entry, the later of the leader's proposal and the
  range's clock, so every replica judges it alike. The null version is checked though Object
  Lock keeps versioning enabled: a gateway whose view of the bucket predates Object Lock could
  otherwise replace a locked one. The Bucket range keeps a bucket's Object Lock configuration
  in its row, set only while versioning is enabled, after which versioning cannot be
  suspended [18 §5]. The lifecycle worker removes versions with Name-range deletes that never
  bypass, so no locked version expires [18 §2.5].
- **Lazy deletion.** Unreferenced files and blocks are removed by the collector after a
  grace period, so a deletion by mistake can still be undone by a metadata change, as GFS
  keeps a deleted file three days and Tectonic deletes lazily between layers (chunk-store
  §8; 01 §1.6).
  - *Every file has one referrer:* the version or part whose row names it, or, once an
    upload completes, the object's file that takes it as an extent. A write hands the Name
    range a file the gateway made for it alone. If the write goes ahead, the file is
    referenced; if it is refused, the range releases it, and the gateway makes a new file
    for any retry, never reusing one. Copies must keep the rule: a copy that took the source's
    blocks without moving data, as Tectonic's does (01 §1.6), would give them two referrers,
    and would need Tectonic's owner rows (R2) before the collector could tell when they are
    free.
  - *Releasing.* Whatever removes a reference writes, in the same transaction, a row in the
    range's queue of released files, keyed by the range's time and then the file: a version
    removed by ID, a null version replaced, a part uploaded again, an upload's parts that
    its completion did not list, an aborted or collected upload's parts, and a refused
    write's file. A completed upload's listed parts are not released: they are the object's
    extents. So no file is forgotten by the step that stops referencing it, and none is
    released while something still names it. A property test runs random histories of puts,
    deletes, part uploads, completions and aborts under every versioning state and checks
    after each step that every file handed to the range is held in exactly one place;
    removing any one release, or releasing a listed part, fails it.
    - *Reclaiming.* The collector takes the queue oldest first, once a file has been released
    longer than the grace period, and removes what it holds bottom up: each block's chunks
    from their volumes, then the block's rows, then an adopted part's file, then the file,
    and last the queue row. A reclaimer (`crates/meta/src/reclaim.rs`) names each read and
    command and moves on with its answer, doing no I/O, as the coordinator does. Every step
    can be repeated, since a chunk or row already gone stays gone and a removed file reads as
    one with no extents, so a collector that stops is resumed from the queue. A property test
    stops reclaimers after random steps over random files of parts and blocks, resumes them
    from the queue, and checks that every row, block and chunk of the file goes and nothing
    else does; dropping the queue row first fails it.
  - *When.* The grace period is a recovery-point policy, not a safety bound: three days by
    default, as GFS keeps a deleted file (chunk-store §8; 22 §1.1, §10.4). A released file
    comes due a grace after its release, in the range's clock, and the collector waits on the
    queue's own times rather than polling: it next looks when the oldest row comes due, or a
    grace from now when the queue is empty, since nothing released after now can come due
    sooner (22 §10.3; `crates/meta/src/collector.rs`). The collector runs beside each range's
    leader; two at once, across a change of leader, only repeat steps that can be repeated.
  - *What the queue cannot see:* a gateway that stops between writing a file and handing it
    over leaves a file no range ever held. Finding those needs the sweep between layers that
    Tectonic runs (01 §1.6), from files to the writes they were made for (§6).
- **Multipart.** CreateMultipartUpload writes the upload row. UploadPart writes the part's
  chunks, blocks and file, then replaces the part row. CompleteMultipartUpload writes the
  object's file of part extents in its File range, then, in the Name range, checks the
  parts against the list sent (numbers ascending, ETags and checksums matching;
  body.rs), commits the version, and removes the upload and its part rows. A retried
  complete with the same parts finds the version it made and answers as before (05 §4.4).
- **Creating and deleting a bucket** touch the Bucket range and every Name range the
  bucket's keys fall in. Each is a sequence of range transactions, each guarded by what the
  one before it wrote, with the collector finishing what a failure leaves, as Tectonic moves
  a directory between shards [01 §1.6].
  - *Gates.* Each Name range holds a gate for every bucket whose keys it may hold. The gate
    records the bucket's incarnation, the attempt that last moved it, and whether it is
    open, closed or condemned. The incarnation is the bucket's creation time in the Bucket
    range, which a bucket re-created under the same name never repeats, since a range's
    clock only moves forward. A write names the incarnation it was made under, and a range
    applies it only through that incarnation's open gate, so a gateway whose cache is stale
    is refused with `NoSuchBucket`, with no lease or clock bound needed. A range that
    splits gives each side its floor and the gates of the buckets whose keys it can hold.
  - *Attempts.* Each create and delete is an attempt, numbered by the Bucket range's clock
    when it began. A request that finds another attempt in progress, such as a client's
    retry after its gateway stopped answering, takes it over under a new number. Every
    step, in the Bucket range and at the gates, names its attempt, and a step older than the
    attempt that last moved the row or gate is refused, as a Raft proposal carrying an old
    lease sequence is refused at apply [06 §A4.3]. A Name range also keeps a floor, the
    attempt of the last gate it removed, below which no attempt may place a gate, so a
    coordinator left behind cannot reopen a gate for a bucket that is gone, and the range
    keeps no row for the buckets it has removed. A create refused by the floor that another
    bucket's removal raised starts again under a new number.
  - *Create.* The Bucket range records the bucket as being created and counts it against
    the owner's quota. Each Name range the bucket's keys fall in opens a gate for it, and
    the Bucket range then marks it active and lists it. The collector deletes a create left
    unfinished: it takes the create over as a delete, placing closed gates where none was
    opened.
  - *Delete.* The Bucket range marks the bucket as being deleted. Each Name range closes
    its gate and is then read, in pages, for any version or delete marker: S3 deletes a
    bucket only once "all object versions and delete markers" are gone [05 §10.5]. If one
    is found, the gates reopen and the delete answers `BucketNotEmpty`. Otherwise the
    Bucket range marks the bucket deleted, and it answers `NoSuchBucket` from then on. The
    gates are condemned, and the collector removes the bucket's in-progress uploads, which
    do not block deleting a general purpose bucket [05 §10.5], then its gates. Only then
    does the Bucket range forget the name. Until it does, a create of that name answers
    `OperationAborted` [05 §11], much as S3 "queues the bucket for deletion" [05 §10.5].
  - *Why no write is lost.* A delete reads each range only after closing its gate under
    its own attempt, so every write the range admits comes before the read, which sees it,
    and every later one is refused, including those through a gate an older attempt had
    reopened. The Bucket range accepts the delete only from the attempt that closed the
    gates. An acknowledged write therefore either stops the delete or was itself deleted
    first. A new incarnation never sees a condemned bucket's uploads, since the name is not
    reused until they and the gates are gone. `crates/meta/tests/bucket_lifecycle.rs`
    checks these properties after every step of 2,000 generated schedules. The schedules
    have concurrent creates and deletes, coordinators that stall and are taken over,
    collectors that resume, and writers with stale views.
  - *The coordinator* (`crates/meta/src/coordinator.rs`) runs one attempt, and does no I/O
    itself. It names each read and command and the range it goes to, and moves on with the
    answer. It tells the request that started it what that request learns: created,
    deleted, not empty, or taken over by a later attempt. The simulation drives this
    coordinator, stepping any attempt between any two of its reads and commands.
  - *Attempts left behind.* A gateway that stops leaves its attempt where it was. The Bucket
    range keeps an index of the buckets whose create or delete is in progress, and each
    bucket's row the range time its attempt last showed progress: when it began, and each
    `Progress` step its driver sends while the attempt works through the Name ranges. The
    collector pages the index, and takes over an attempt that has gone its patience without
    progress: a create by abandoning it as a delete, a delete by running it again under a new
    attempt, and a deleted bucket's cleanup by resuming it from its row. Measuring from
    progress rather than from the start keeps a long delete, paging through many uploads,
    from being taken over while it works (22 §10.5). Taking over early is safe, since every
    range refuses the steps of an older attempt; it only aborts a slow one. So the patience
    trades that abort against how long a stalled create keeps its name answering
    `OperationAborted`; it is the gateway's per-step deadline times its retry budget, plus
    the clock offset between ranges, measured once the gateway runs (22 §10.5).
  - *Resuming a cleanup.* Every step of a deleted bucket's cleanup can be repeated from its
    start: condemning a gate the cleanup already dropped is done, as the range's floor shows
    the gate was removed by this attempt or a later one, and collecting where the gate is
    gone is done, since a gate is dropped only once its range holds no row of the bucket.
    The simulation's schedules include gateways that stop for good mid-attempt, and after
    every schedule the faults stop, the collector acts on its schedule, and every create and
    delete must end with the bucket active or its name forgotten. Before the rule for
    condemning a dropped gate, a cleanup that stopped after its first drop could never be
    resumed: the simulation found such a schedule within 2,000.
- **Listing** scans the Name range in key order. ListObjects takes the first version of each
  key and passes keys whose first version is a delete marker (05 §6.4); ListObjectVersions
  takes every version, newest first; ListMultipartUploads takes uploads, reaching a key's
  with one seek past its versions. Each scan passes at most a budget of keys that hold nothing
  it lists, and pauses between keys, naming the last one passed so the next page starts just
  after it (s3-protocol §3).

## 3. Ranges

A range is one Raft group over a contiguous span of one layer, with `2f + 1` replicas one
to a failure domain (architecture §1, §11). Its rows live in a state engine on each
replica; its log is the node's shared write-ahead log.

- **Consensus is focal-raft,** the sans-io Raft core with pre-vote, check-quorum, learners,
  joint consensus, ReadIndex, snapshots and Fast Raft's fast track, tested step for step
  against raft-rs and model-checked in TLA+ (07 §1). mantle depends on it by pinned revision
  and does not write another core.
- **The log is mantle's,** one per disk, shared by every range with a replica there: one
  flush commits every group's appends, as Bigtable's single commit log per tablet server
  does, where per-tablet logs weakened group commit (06 §C.c). focal's own shell rewrites
  its whole log for each group's checkpoint and caps snapshots at 8 MiB (07 §2), which does
  not scale to thousands of ranges a node. The log reuses the chunk store's techniques:
  checksummed frames, group commit with one flush a batch, fencing on a failed write or
  flush, torn tails told apart from corruption.
- **The Raft log is the only write-ahead log.** A range applies committed entries to its
  engine with the engine's own log off, each batch carrying the applied index; the Raft log
  is truncated only below the index the engine has made durable, so a crash replays from
  there (06 §C.b.2; 12 §6.2). Applying is deterministic and idempotent at an index.
- **Reads are linearizable** through ReadIndex, which focal provides with a quorum round and
  no lease (07 §1.1); a lease, if added, is bounded by the clock drift the deployment
  states (06 §A1.5).
- **Snapshots, splits and moves** transfer engine files over QUIC, checksummed, and never as
  one Raft message (12 §6.3–§6.5). Splits, merges and moves between cells are modeled in
  TLA+ before they are built (architecture §6.1, §10).
- **Splits and merges under a create and a delete.** `docs/models/RangeSplit.tla` models
  Name ranges splitting and merging while a bucket is created, written and deleted. A split is one command in the
  parent's log, and the new child takes the parent's gate. Writers route by cached
  descriptors, and a range answers a stale one with its own descriptor and its children's.
  Each step of a create or delete attempt names the descriptor generation it read; a range
  refuses another generation, and the attempt learns the answer and starts its phase again.
  A merge is one command in the lower range's log: it takes the higher range's span and keys
  at a generation past both and keeps its own gate, and the higher range ends, answering a
  stale request with the lower's descriptor. The two gates need not agree: an attempt that
  held either range's descriptor is refused at the new generation and starts its phase
  again, so it moves the joined gate from wherever the lower one was. TLC checks
  72,007,924 distinct states of three keys, two splits, a merge, a create and two delete
  attempts (`scripts/check-model.sh`). In every one of them:
  - the live ranges divide the keys between them;
  - no acknowledged write is lost to a delete;
  - an active bucket's every range has its gate open.

  Without the generation check, TLC breaks each property in a few steps. In each case the
  directory has not yet learned of a split. A delete then closes and reads only the parent's
  half, and deletes the bucket while the child holds a version (eight steps). A create opens
  only the parent's gate and activates the bucket, and the child's gate never opens (five
  steps). So before ranges split, the coordinator (§2) will carry each range's descriptor
  and generation, where today it counts ranges.
- **Transport:** QUIC for snapshots and other bulk transfers, and a separate UDP datagram
  plane for Raft's messages, including Fast Raft's, as the hecate specification lays out
  (07 §4.7). A fast-track proposal carries its entry, so an entry travels as datagrams only
  while it fits the path MTU (07 §7).

## 4. The state engine

A range reaches its rows through a narrow engine interface: apply a batch with its applied
index, get, scan a range of keys in order, take a consistent snapshot, delete a key range,
export and ingest files, and report the applied index that is durable (06 §C.b.3). Two
engines implement it:

- **A model engine** for deterministic simulation: an ordered map with explicit durable and
  not-yet-durable state, the latter dropped on a simulated crash (06 §C.b.3, §C.d.3).
- **A production engine,** one instance per range with shared memory, compaction and
  file-deletion budgets per node, as ZippyDB runs one RocksDB per shard (12 §6.1). Research
  note 12 recommends RocksDB, with leveled compaction and the models of §6.6 setting its
  parameters. Which binding carries it, the upstream crate (whose macOS build does not flush
  the drive cache) with mantle-owned FFI, a maintained fork, or a mantle-built LSM, is the
  owner's decision (12 §5.8), and the interface keeps it one crate's change.

## 5. Testing

The metadata service is done when a linearizability checker accepts histories recorded
under network partitions, process crashes and disk faults, in deterministic simulation and
with real processes (STATUS item 1). Simulation runs the real Raft core, log, apply path and
range manager on simulated network, disk and clock, with the model engine; the checker is
per-key, P-compositional WGL in the manner of Porcupine (06 §A6.8, §C.d). Real-process tests
cover the production engine, which the simulator cannot.

## 6. Open

- The production engine and its binding (§4).
- An owner's quota and ListBuckets once the Bucket layer outgrows one range.

- How an entry larger than a datagram reaches the replicas: over QUIC, or fragmented on the
  UDP plane. A completion of 10,000 parts is an entry of hundreds of kilobytes in the Name
  range and another in the File range.
- The sweep that finds files a stopped gateway made and never handed over (§2), and the
  collector's pacing against foreground latency, with a floor that keeps its backlog bounded
  (22 §10.1–§10.3). The patience after which an attempt is taken over, and the floor under
  the grace period, the longest read's deadline plus clock offset (22 §10.4), are measured
  once the gateway runs.
- The bound on a cached bucket row's staleness, and how a versioning change reaches
  gateways within it.
- Where a bucket's lifecycle and tag configurations live. A lifecycle configuration at its
  largest, 1,000 rules with the longest IDs, prefixes and tags, is about 13 MB unescaped
  (docs/design/s3-protocol.md §7). That is too large for the bucket's row, which every
  request to the bucket reads, and larger than one entry should be.
