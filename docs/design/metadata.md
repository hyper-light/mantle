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
| Name | (MARKS, bucket, key, file) | a file the range took, until the sweep settles it | (bucket, key) |
| Name | (RELEASED, time, file), in each range | a file nothing references any more, for the collector | the range's |
| Name | (LINEAGE), in each range | the range's descriptor, and the child of its last split | the range's |
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
    range's queue of released files, keyed by the range's time and then the file and naming
    the object key the file was held under: a version
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
    then any mark a stopped sweep left on it, at the Name range that holds its key by then,
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
  - *Files never handed over.* A gateway that stops between writing a file and handing it
    over leaves a file no range ever held, and the queue cannot see it. Tectonic runs a
    collector between layers for such leftovers (01 §1.6); scanning every file for a
    reference, as GFS scans its namespace, costs a pass over the whole layer each period
    (22 §1.1). Instead every file is registered while its handover is in flight, as HDFS
    allocates a block before it is written and RocksDB registers a job's outputs while it runs
    (22 §3.1, §7.3), and the sweep reads only those.
    - *The deadline.* A file's row records the File range's time it was written, the Name-range
      write it was made for (bucket, incarnation and key), and a deadline by which that write
      must take it, the writing gateway's handover time added to the write's. The file waits
      in the File range's queue of unsettled files, keyed by its deadline. A handover names
      the deadline, and a Name range whose time has passed it refuses the write and releases
      the file, as Spanner fails reads older than its version window (22 §6).
    - *The mark.* A Name range that takes a file within its deadline, referencing it or
      releasing it for a refused write, marks it. The mark is keyed by the object key the
      file was made for, in a space of its own that listings never read, so a split cuts a
      range's marks at the same key as its rows and a file's mark stays with the rows that
      may reference it. A mark keyed by the file alone would stay behind in the old range, and
      the sweep, asking the range that holds the key now, would release a referenced file.
    - *The sweep* (`crates/meta/src/sweep.rs`) takes the queue as deadlines pass and asks the
      Name range of each file's key, in one command, whether it took the file. A marked file
      was taken. An unmarked one past its deadline, at the Name range's time, is released and
      marked there and then. The check records its time as a write does, so every later
      entry reads a time at least as late, even one proposed by a leader whose clock runs
      behind, and a handover that comes after finds the deadline passed and is refused: the
      check and the handover are ordered by the range's log, as Giza's no-op and a stalled put
      contend for one Paxos slot (22 §7.4). Judged at the entry's proposed time without
      recording it, a handover from a lagging leader could take a file the check had just
      released.
      The sweep then settles the files in the File range and removes their marks, in that
      order: a mark removed first would read, to a sweep resumed after a stop, as a file never
      handed over. Like the reclaimer, the sweep names each read and command and does no I/O,
      and every step can be repeated.
    - *What it costs.* A file handed over past its deadline is released unmarked, since it may
      come after the sweep settled the file, and a mark would stay behind; a file whose mark
      went with its release before the sweep came by is released again. Either way the file is
      in the queue twice, and the reclaimer's second pass finds it gone. A mark left by a sweep
      that stopped between settling a file and unmarking it goes when the file is released or
      reclaimed, so every mark is on a file a version or part references or on one released.
    - *The deadline's length* is the gateway's measured handover time: the per-step deadline
      times its retry budget, plus the clock offset between ranges, as Ceph's 120 s and HDFS's
      60 s bound the same window (22 §10.2). It bounds how long a leftover file waits, not
      correctness: the File range and the Name range may disagree about the time, which only
      makes an honest handover late (22 §10.1).
    - *Blocks never named.* One layer down, a gateway that stops between writing a block and
      writing its file leaves a block no file names. A block is made for one file, which alone
      may name it, and records that file, its time and its deadline, and waits in the Block
      range's queue of unsettled blocks. A file's write names the soonest deadline of its
      blocks, and a File range whose time has passed it refuses the write. A PUT's first block
      is written while the rest of its body streams in, and its file only once the body ends,
      so no fixed deadline serves a body that arrives slowly: the gateway renews each block
      it wrote, a handover past the Block range's time, while its body streams, as an HDFS
      writer renews its lease (22 §3.6). The block sweep (`BlockSweep`) asks the File range of
      each block's file, one file at a time, which answers from the file itself: a file is
      written once, whole, so one written names what it ever will, and one not written by a
      block's deadline never will. Nothing is marked. A block named is settled. One never to
      be named is released in the Block range only if its deadline is still the one the File
      range judged, and a released block renews no more, so a renewal and a release are
      ordered by the Block range's log: a renewal first keeps the block, and after a release
      the writer's file names a deadline the File range has passed and is refused. A
      released block is taken apart there and then, its chunks first and its rows and place
      in the queue last, with no grace period, since no reference ever reached it and there
      is no deletion by mistake to undo.
    - *Checked.* `crates/meta/tests/orphan_sweep.rs` runs 2,000 generated schedules across a
      Block range, a File range and two Name ranges. Gateways write a chunk and its block,
      renew it, then write the file, then hand it over, stopping at any stage or coming late
      to the next;
      deletes release files; both sweeps stop between any two steps; and leaders whose clocks
      run behind propose entries at earlier times than entries already applied. After every
      step, no file a version references is released, every file is referenced, released or
      unsettled, and every block a file names keeps its rows and chunk. Once the faults stop,
      nothing is left unsettled in either layer, every file is referenced or released and not
      both, and no block or chunk is left that nothing names. With the Name range's deadline
      check removed, the File range's, either check's recorded time, the release's deadline
      check, or the refusal to renew a released block, the simulation loses a file or a
      block's chunk.
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
  Name ranges splitting and merging while a bucket is created, written and deleted. A split
  is one command in the parent's log, and the new child takes the parent's gate. A merge
  spans the two ranges' logs: the higher range freezes for a merge into the lower one at the
  generation its driver read, and the lower range decides the merge once, at that
  generation, moving its generation on whether it takes the frozen range or refuses; a taken
  merge ends the frozen range and is then resolved, and one never to be taken thaws it.
  Drivers stop and are replaced at any point, and their commands arrive late. Writers route
  by cached descriptors, and a range answers a stale one with its own descriptor, unless it
  has ended, and those of the child its last split made and of the range it is merging or
  merged into; one whose descriptors then no longer cover the keys reads the directory.
  Each step of a create or delete attempt names the descriptor generation it read; a range
  refuses another generation, and the attempt learns the answer, keeping each range's
  newest descriptor only, and starts its phase again. The merged range keeps its own gate:
  an attempt that held either range's descriptor is refused at the new generation and starts
  its phase again, so it moves the joined gate from wherever the lower one was. TLC checks
  every state of four configurations of three keys (`scripts/check-model.sh`): two splits, a
  create and two delete attempts (1,607,127 states); two splits, a merge, a create and a
  delete (13,081,532); a split, a merge, a create and two deletes, one taking over from
  the other (9,457,818); and two splits, two merges and a create (93,864,489). In
  every one of them:
  - the ranges that own their spans, every serving range and every frozen one whose merge
    was not taken, divide the keys between them;
  - no acknowledged write is lost to a delete;
  - an active bucket's every range has its gate open.

  Without the generation check, TLC breaks each property in a few steps. In each case the
  directory has not yet learned of a split. A delete then closes and reads only the parent's
  half, and deletes the bucket while the child holds a version (eight steps). A create opens
  only the parent's gate and activates the bucket, and the child's gate never opens (five
  steps). So the coordinator (§2) routes each step by a descriptor and names its
  generation, and relearns as the model's attempts do. Two more controls justify the merge's
  rules. A refusal that leaves the lower range's generation where it was, a driver thawing on
  the answer, lets a late decision take a range that thawed and serves: two ranges own one
  key (five steps). A range frozen while holding a merge it took can be taken and end, and a
  driver reads the merge it held as never taken and thaws its frozen range: two ranges own
  one key (eight steps).

  Checking merges found two rules the attempts must keep. An attempt keeps each range's
  newest descriptor only: an older one beside it can span keys the range no longer holds,
  and once the newer one's step is done the attempt counts them as covered, activating a
  bucket whose child range never opened its gate. And a range that ended answers with where
  its span went, never with its own descriptor, which an attempt would take for a live
  range and step forever.
- **Splits, as built** (`crates/meta/src/name.rs`). A Name range's lineage records its
  descriptor, an ID, a span and a generation, and the child its last split made. A span runs
  between two routing keys (`key::route`): the bucket, then the key, each escaped and ended.
  Every row of an object key, its versions and uploads and its marks alike, is its space's
  byte, the key's routing key and a suffix whose first byte is below 0xFF, so a span bounds
  its keys' rows by bytes in every space; a mark carries a byte of its own before the file
  ID, which may begin with 0xFF and would otherwise read, after a key ending in an escaped
  zero byte, as part of a later key.
  - *The split* is one command in the parent's log, naming the generation the splitter read
    and a routing key inside the span. The parent keeps the keys before it, its queue of
    released files and its sessions. The child takes the keys from it with their rows and
    marks, and the gate of every bucket whose keys it can hold: a bucket the cut falls
    inside keeps its gate on both sides, and one wholly past the cut moves its gate. It
    takes the gate floor, so an attempt the parent refused stays refused and a resumed
    cleanup finds the gates it dropped, and the clock, so a time a check recorded carries
    over: a handover a lagging leader proposes behind the check that released its file is
    refused by the child as the parent would have refused it. Both take the next
    generation, and the parent clears what the child took with one range delete a space
    (§4). `name::child` reads the child's rows from the parent before the parent applies the
    split, so the replicas can make the child on the parent's replicas from the parent's
    rows as they stood.
  - *Fences.* A command for an object key outside the range's span, a write or the sweep's
    check or unmark of a file made for such a key, is not the range's, and neither is a
    coordinator's step or read routed by another generation. The range answers with its
    lineage and takes nothing, not even a file the command carries, which the sender hands
    on to the range that holds the key. A range keeps one child, so its lineage is bounded;
    a sender whose descriptors no longer cover the keys it needs reads the directory.
  - *Checked.* `crates/meta/tests/bucket_lifecycle.rs` splits ranges at seven cuts,
    among them the bucket's first routing key and the next bucket's, while its 2,000
    schedules create and delete the bucket; writers route by descriptors they learn of
    late, and the directory learns of splits when the schedule says. After every step it
    checks the model's properties, that each range holds rows, marks and gates only of its
    own keys and buckets, and that a forgotten bucket leaves no gate.
    `crates/meta/tests/orphan_sweep.rs` splits ranges while gateways hand files over and the
    sweep checks them by late descriptors. Each rule removed on purpose fails a
    simulation: without the generation check, the delete of the model's eight-step schedule
    loses its version and a create activates a bucket whose child never opened its gate;
    without the span check, writes land outside the span; a child without its gates
    leaves an active bucket's range closed; marks left behind, or checks answered outside
    the span, release files that versions reference; a child without the floor leaves a
    resumed cleanup unable to finish. A child without the clock passed 2,000 schedules,
    which never reached the one interleaving that needs it, so a test of its own spells
    that interleaving out.
- **Merges, as built** (`crates/meta/src/name.rs`, `crates/meta/src/merge.rs`). A merge
  joins a range to the range just below it across the two ranges' logs, in the steps the
  model takes.
  - *Freeze.* A driver reads both descriptors and freezes the higher range for a merge into
    the lower one at the generation it read. The frozen range takes nothing but the merge's
    own steps, and its generation rises, so a step routed by what it was is refused after it
    thaws. A range holding a merge not yet resolved is not frozen: it could otherwise end
    with the merge unresolved, and a driver would read the merge as never taken.
  - *Decision.* Once every replica of the frozen range has applied the freeze, the lower
    range decides, in its own log and only at the generation the merge names. It takes the
    frozen range if it holds no merge not yet resolved, the frozen range begins where it
    ends, and the frozen range holds no more rows than the merge may take, which bounds the
    entry; otherwise it refuses. Either way its generation moves on, so the merge is decided
    once, and a command for it that comes later, however late, is routed by a generation the
    range no longer has. A driver abandons a merge not yet decided the same way, by moving
    the lower range's generation on. The generation is the record of the decision: no merge
    counter or watermark is kept.
  - *What moves.* Each replica of the lower range reads its own copy of the frozen range,
    which every replica holds frozen alike, and takes its rows and marks, its queue of
    released files, and the gates of the buckets the lower range holds none of, keeping its
    own gate of a bucket both hold keys of; the higher of the two floors and the later of the
    two clocks, so a time a check recorded carries over as in a split; and none of its
    sessions. A replica whose copy is not frozen for the merge stops rather than take other
    rows than its peers.
  - *Ending.* A taken merge ends the frozen range, which then answers every request with the
    lower range as the merge left it and never with its own descriptor, and the lower range
    lets go of the merge; a refused one thaws the frozen range a generation on. A driver
    thaws only once the lower range shows the merge will never be taken: its generation past
    the merge's and the merge not the one it holds, or the range ended. Once the lower range
    has let go of a taken merge, a driver still judging it reads it as never taken, but the
    frozen range has ended by then and its thaw is refused. A lower range read while frozen
    for a merge of its own decides nothing until that merge ends it or thaws it a generation
    on, and the merge waiting on it is then never taken. The driver asks whether the lower
    range took the merge before whether it ended, so it judges an ended range right from its
    lineage even without the rule against freezing a range that holds a merge; the rule
    keeps the judgment from resting on that lineage staying readable.
  - *Routing through a merge.* Until the merge is decided nothing serves the frozen span,
    and writes to it wait, as architecture §6 accepts for a merge. Between the lower range
    taking the merge and the frozen range ending, the frozen range names the lower range as
    the driver read it; a sender asks that range next, which holds the span once the merge
    is taken. A merge that takes back the lower range's child forgets the child, which no
    longer holds any of its span.
  - *Checked.* Both simulations merge ranges while buckets are created and deleted and files
    are handed over and swept, with drivers that stop anywhere, resume from either range,
    give up, and whose last command arrives late. Each rule removed on purpose fails one of
    them or a test: marks or the queue left behind release files versions reference or lose
    track of one; a refusal that leaves the generation where it was, with the replica's own
    check of the frozen copy removed, lets two ranges own one key, and with the check the
    replica stops first. A merge that keeps the lower range's clock passed the simulations,
    so a test spells out the interleaving that needs the later clock.
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
- Chunks a gateway wrote for a block it never recorded: each volume's chunks reconciled with
  the Block layer's reverse rows, taking those past a deadline that no block names, as GFS
  and HDFS reconcile chunk reports (22 §10.1, item 6), once storage nodes run. And the
  collector's pacing against foreground latency, with a floor that keeps its backlog bounded
  (22 §10.3). The patience after which an attempt is taken over, and the floor under
  the grace period, the longest read's deadline plus clock offset (22 §10.4), are measured
  once the gateway runs.
- The bound on a cached bucket row's staleness, and how a versioning change reaches
  gateways within it.
- Where a bucket's lifecycle and tag configurations live. A lifecycle configuration at its
  largest, 1,000 rules with the longest IDs, prefixes and tags, is about 13 MB unescaped
  (docs/design/s3-protocol.md §7). That is too large for the bucket's row, which every
  request to the bucket reads, and larger than one entry should be.
