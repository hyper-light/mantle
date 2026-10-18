# The metadata engine's structure: past RocksDB by its own design

Status: design, 2026-10-06. It supersedes the structure of engine.md, which converted RocksDB
11.8.1. The owner decided on 2026-10-06, on docs/research/33, to give the engine "a new structure
[and its] own format". RocksDB's format remains an import and export path (§9), and the pieces
P1–P4 built (codecs, hashes, coding, blocks) remain where they serve. Sources:
- docs/research/33: the evidence and the multiples it supports;
- docs/research/34: the mechanisms in implementable detail;
- docs/research/12 (RocksDB and ZippyDB), metadata.md (ranges and operations), raft-log.md (the
  per-device log), CLAUDE.md (the rules).

## 1. The bar, and what never moves

**The workload** is object-store metadata as Meta measured it (research/33 §1): 78% gets, 13%
puts, 6% deletes, 3% short seeks; 48-byte keys and 43-byte values; most keys cold and a hot 1%
taking half the reads; machine-speed bursts; always a busy machine.

**What every structure here must keep, without exception:**
- A write is acknowledged only once durable on every replica the protocol requires (CLAUDE.md §6).
  Here that is the Raft log's flush (raft-log.md); the engine adds no weaker path.
- Every record and page on disk carries a checksum verified on read. A mismatch is a typed
  corruption that feeds repair.
- No panic in production, every structure bounded, no thread per unit of work, completion I/O
  through hyper-block's issuer.
- Recovery from any crash point, proved by deterministic simulation with injected faults and by
  kill tests on real processes and disks.

**What it must beat**, each measured against stock RocksDB running the same workload under the
same load, both with durable commit (research/33 §3):
- puts per second at durable commit;
- p99 and p99.9 of every operation while flush and compaction run;
- get latency, hot and cold;
- seek and short-scan latency;
- write, space and read amplification;
- allocations, page faults and energy per operation.

A design step lands only with its measurement (CLAUDE.md §8).

## 2. Where the engine sits

A range replica is a Raft group (metadata.md §3) whose state machine is this engine. Its
entries are made durable by the device's shared Raft log, one flush committing every group's
writes (raft-log.md). The engine therefore keeps **no write-ahead log of its own**:
- It applies committed entries into its memtable.
- It makes its state durable by checkpoints that record the last applied index.
- On recovery it opens its last checkpoint and replays the Raft log from that index.

This is SplinterDB's design with mantle's log as its log (research/34 §1, checkpoint). It is
also the role the RocksDB conversion gave RocksDB with its WAL off (engine.md).

Group commit is therefore the Raft log's to make faster, and SpanDB's measured gain (7.6–8.8× at
fdatasync with several batches in flight, research/33 row 3) is hyper-log's work. It is listed
here because the engine's put throughput depends on it: §8, step E0.

Every range replica's engine is its own instance with its own files: shared-nothing by
construction (p²KVS, research/33 row 4). A process runs many under shared budgets (memory,
device bandwidth) and runs them on hyper-rt's shards.

### Shards on hyper-rt (step E2), as designed

Measured first: with every replica's maintenance on the thread its puts run on, one engine
reached RocksDB's put tail at moderate load and fell behind it at load average 21 (put p99.99
358–489 µs against 42–107 µs, throughput 0.49–0.57 against 0.71–0.92 M/s, 2026-10-07, §6),
because RocksDB spreads its flushes and compactions over background threads and our one thread
was preempted mid-slice. The design's answer is not a shared compaction pool (§4 excludes one)
but its own first part: the work spread over shards, one per core, each owning whole replicas
(p²KVS, research/33 row 4), with SILK's scheduling inside each (§6).

- **Runtime.** A node runs one hyper-rt runtime (hyper-raft `docs/runtime.md`): a shard is an
  OS thread with its own task arena, run queue, timing wheel and completion driver; a task lives
  on its shard for its life and is never stolen; values move between tasks and threads through
  bounded channels, and no state is shared (runtime.md §3, §8). The shard count is the cores the
  node is given (runtime.md §10), not chosen here.
- **Ranges own engines.** A replica's engine (`ShardDb`, its store file, its page cache, its
  device attachment) is created on the shard that will own it and never leaves it. Keys reach
  it by range: a node's keyspace is cut into ranges (metadata.md §3), each placed on a shard. A
  standalone engine (the bench, the laptop) cuts its keyspace into one range a shard, by key, as
  p²KVS partitions an instance's keys across its workers.
- **Requests batched per shard.** A caller (a connection's task on any shard, or a plain
  thread) sends its operation to the owning shard's request channel, and the result comes back
  on a one-value channel. The shard's range task drains every request waiting, up to its bound,
  and applies them together: one wake and one channel round a batch, not an operation (p²KVS's
  batching, research/33 row 4). The channel's bound is the shard's admission bound; past it the
  caller is refused `Full` to retry or shed, never queued without limit.
- **Maintenance as the shard's tasks, scheduled by SILK.** Each range's maintenance (packing,
  cascades, filter pages, the cache's forgetting) runs on its own shard as work the range task
  takes in the time requests leave: a batch served, then maintenance until requests wait again.
  Packing and the root's compaction are never paused, since a full memtable stops puts; deeper
  compactions run only in that idle time. The debt pacing of §6 stays as the backstop: when a
  debt's room is running out, puts pay their share as now, so a saturated shard still cannot
  fall behind. A shard never runs another shard's maintenance: its replicas' state never leaves
  it.
- **I/O.** Each range's store attaches to its device's issuer (hyper-block), shared by every
  range on the device, so a device keeps its measured depth whatever the range count. A demand
  page the OS holds in memory is read on the range's own shard, through the attachment's own
  duplicate of the file (`Attached::read_resident_at`: `RWF_NOWAIT` on Linux, `mincore` over a
  kept mapping on macOS); only a read that would wait goes to the issuer, since each handoff to it
  is a wake the scheduler grants, tens of microseconds at the median and milliseconds at the tail
  on a busy machine (research/41).
- **Active memtable order.** A range's paced idle slices finish the sorts and counter merges
  already admitted by puts (`HashMem::debt`, `pay`; research/40). They do not sort each new
  partial tail and merge it with a larger run: that repeats the larger run's rebuild as writes
  continue. Scans still seal their tail, and explicit maintenance, tidying and packing retain
  their full ordering rules. With no admitted job, a partial tail does not keep the range awake.
- **Bounds.** Requests a channel holds; ranges a shard owns (its tasks bound,
  `tasks_per_shard`); batches out at the issuer; the page cache by bytes. Each is a stated bound
  with a typed refusal.

**Same-shard clients.** Measured per operation over the same engine
(`benchmark-results/mantle-boxed-request-panel-20261010`, 1M fills, 100K gets and seeks): a put
cost 2,995 instructions on the direct path and 9,340 through a range on the runtime; a get 4,833
and 7,600 warm; a seek 14,822 and 21,160. The difference is the request's round trip, which a
client on the range's own shard does not need: two channel hops, two wakes, three or four polls of
two tasks (each reading the clock), the serve loop's bookkeeping and a timed maintenance slice. So
a client on its range's shard makes a put, deletion or get inline when the range is idle and the
operation finishes without waiting, as an event loop runs a call made on its own thread at once
(trantor's `EventLoop::runInLoop`):
- the range's task lends its engine (`Lend`) at the awaits that borrow none of it: its idle wait
  for a request, the yields between maintenance slices, and a client's turn before it waits for
  its workers or the device, the wait made only once a turn passes with no operation made on
  the engine (a full memtable is out on a worker for most of a fill, and a range that held its
  engine through that wait sent 57% of a same-shard client's puts through its channel, each
  with a timed maintenance slice). The engine is boxed once and moves
  into a slot on its shard's thread and back, so ownership moves and no reference is shared (no
  Arc, no RefCell); a lend dropped mid-await (the task's future dropped) takes the engine back
  and drops it there, as the task's local would have been;
- a range lends only with no fault, no mutation waiting to be applied (their arrival order is
  kept) and no request in hand; a put `put_paced` cannot apply now, a get or a scan needing a
  page neither cached nor in the OS's memory (`ShardDb::get_now`, `ShardDb::scan_now`: the store
  refuses a read that would wait, `Store::begin_no_wait`, and a refused scan leaves its rows, its
  next key and its counters as they were), a barrier and every request from another shard take
  the request path unchanged;
- an inline operation runs to its end in the poll that starts it, or not at all: canceled
  before, nothing of it happened; nothing of it is ever outstanding. `RangesConfig::inline` off
  gives every client the request path's semantics alone (a canceled mutation published and kept),
  which its tests use;
- an inline operation never waits, so a client making them in a loop yields its shard after
  about the shard's quantum of them, at the rate measured over its last turn (two clock reads a
  turn), and the first operation to leave the range maintenance owed wakes the range's task once,
  which runs when the client yields.

Lending the engine for a client's turn before a wait on the workers, measured in a 10M fill with
memtables sorted on workers (benches/ranges.rs, `rangesmcpdiag-bin` against `rangesmcqdiag-bin`):
puts made inline went from 4.34 to 10 million of 10 million, and the range's timed maintenance
slices from 0.57 to 0.06 a put; puts a second, three interleaved rounds, from 2.84 to 4.93
million at 10M and from 2.39 to 4.38 million at 100M (research/40 §4).

Measured with one binary, inline against `--no-inline` (every operation through the range's
channel), three interleaved rounds, 1M fills, 1M gets and 100K seeks of 10, seed 301
(`benchmark-results/mantle-inline-final-ab-20261010`), medians: fill 1.32 to 1.98 million puts a
second, 9.85 to 6.80 thousand instructions a put; gets 1.10 to 1.61 million a second, CPU 0.91 to
0.61 us a get, p99 3.33 to 2.75 us. Seeks take the request path in both arms: 0.41 and 0.39
million a second, within the rounds' spread.

An idle range that reaches a required trunk read yields its work before opening or claiming
that read. Its task then waits for either a request or an issuer answer through hyper-rt's
existing bounded channel. The answer channel holds the attachment's admitted batch count,
claimed once at attachment setup; waiting creates no new channel or per-I/O allocation.
The channel's register-then-recheck receive and publish-then-wake send are the existing
hyper-rt handoff protocol (`vendor/hyper-rt/src/sync/mod.rs`, `handoff.rs`; upstream runtime
design §8). Canceling a borrowed receive consumes no answer, batch credit or buffer. Store
accounts the numbered answer through the same path as its synchronous consumer, and a later
consumer can take an interrupted wait's answer. A runtime range requires its device attachment
and admitted maintenance workers before it starts. Every runtime I/O wait, including cold queries,
packing, metadata, both durability flushes and retirement, must yield to the shard. A synchronous
fallback is an implementation defect and prevents acceptance of the integration.
Native held-I/O tests cover same-shard request progress, cancellation,
failed reads and writes, and exact checkpoint recovery (`tests/ranges_async_reads.rs`,
`ranges_owning_cancel.rs`, `ranges_paced_stall.rs`, `store_issuer.rs`).

The retained checkpoint keeps its applied index through preparation and image pages,
allocator map pages, the first numbered flush, the alternating superblock write and the second
numbered flush. Only the second successful flush publishes durability. Once image saving begins,
idle maintenance cannot change the snapshot. Canceling a borrowed wait retains its current page,
cursor, buffers and receipt; it cannot replay a completed write. A cold Get or Scan keeps its cursor
and page ownership while awaiting the issuer, with resident pages borrowed in place. A range is an
admitted cooperative service: owning-task cancellation enters closing once, then retains the Db,
active and queued requests, reply buffers and client leases until worker EOF, owner drain and
attachment retirement. Closing is never raced against the already-ready cancellation level.
Before actor admission, the original file moves to the native reaper with the attachment's
physical-retirement watch. The reaper joins the workers, waits for that watch and closes the
original file off the shard. The range awaits the original-close receipt before returning or
dropping its retained requests and leases. The Store retains the first observed
physical fence error through later drains, so terminal cleanup does not replace it with a generic
refusal. This private candidate has native fault, cancellation and crash-cut proofs; final shared
consumer, platform, stress and performance gates remain required before acceptance.

What it is held to: puts and gets per second against RocksDB's `db_bench` at the same thread
count (`--threads=N`, N shards against N threads), and every operation's p99, p99.9 and p99.99
under load, at moderate load and saturated.

## 3. Pages, extents, checksums

- **Units.** The engine's files are written in pages of the device's alignment (`B`, raft-log.md
  §2: the largest of 4 KiB and the device's block and write unit), grouped in extents of a
  measured size.
- **Checksums.** Every page carries a header checksum (CRC-32C, as the chunk store's frames)
  over its contents and its own address, so a page read from the wrong place fails like a
  corrupt one.
- **Immutability.** Pages of branches and trunk nodes are immutable once written:
  copy-on-write, reference-counted per extent.
- **Superblock.** Two alternating copies at fixed places, each with a generation and a
  checksum, written only after the data it names is durable; open takes the newest valid copy
  (research/34 §1, atomicity).
- **The allocator's reference counts are rebuilt or checkpointed.** SplinterDB invalidates its
  allocator map at every checkpoint and cannot rebuild it after a crash (research/34 §0). Mantle
  persists the map with each checkpoint, behind the superblock's generation, and can also rebuild
  it by walking from the root. A test compares the two after every simulated crash.
- **The file grows by segments.** While a file is first-written, every other read and write of it
  waits, on APFS and NTFS, and a first-writer in another file holds up none (research/48 §2, §4). So
  the store's file is kept as segments (hyper-block's `SegmentedFile`): each created whole at its
  final length and, on those, written whole with zeros by a maintenance worker, past the OS's cache,
  before any of its extents is handed out, so every transfer to a segment in use is an overwrite. On
  Linux a segment is allocated whole and not written: ext4's, XFS's and btrfs's writes take the
  inode's lock the same way whether they overwrite or not, and the zeros, a write of every new byte,
  were what a 100M fill waited on (research/58 §7). The allocator stops at the extents admitted; the
  shard books the next segment when fewer are left than a segment, as large as the largest grant
  asked, and a lead, the most the allocations took while a growth was due (research/48 §6). Reads
  made from memory keep one reclaim epoch for the whole file (research/46). Opened, the segments
  holding an extent ever handed out are admitted, and a segment past them, which a crash may have
  left unwritten, is written whole again before use where segments are written whole.

## 4. The tree: SplinterDB's size-tiered Bε-tree

The structure is research/34 §1–§2.
- **Branches** are immutable B-trees of 4 KiB pages, built by packing sorted data. Their index
  entries carry subtree counts, so a range's size is estimated without a scan.
- **Trunk nodes** carry pivots, a pivot bundle each and inflight bundles shared by the pivots.
- **Flush-then-compact.** A memtable becomes a branch, and a flush hands bundles down by
  reference. Compaction folds one pivot's inflight bundles into a single branch.
- **Filters.** SplinterDB keeps one maplet per pivot (its routing filter), merged as branches
  arrive, instead of per-branch filters. Mantle keeps a blocked Bloom filter per branch: maplets
  were built two ways, A/B'd against it to 1B keys, and removed (below).

What mantle changes, and why:
- **Who compacts.** Bundle compaction runs as tasks of the replica's own shard, scheduled by §6.
  There are no shared compaction pools, and so no thread per unit.
- **Bounds.** Every queue and map has a stated bound with a typed refusal:
  - the pivot-state map (SplinterDB's has 1024 buckets and no bound on entries);
  - the task queue;
  - the branches a range iterator collects (SplinterDB's cap is 256; mantle derives it from `F`
    and the height).
- **Parameters by measurement.** The fanout `F` and the memtable size `m` are measured on the
  running hardware and against mantle's key counts. SplinterDB chose 8 and 24 MiB without a
  measurement (research/34 §1).
- **Checksums everywhere** (§3), where SplinterDB has none on data pages.

**Maplets were built, measured and removed** (2026-10-11; research/43 §3–4). Two designs, A/B'd
in interleaved rounds against per-branch filters with no maplets:
- bought by gets' measured rent: the read phase carries the builds it buys (reads 198K at 100M
  against 438K);
- SplinterDB's paged routing filters: no Bloom filters, each branch's own maplet in its pages,
  bundle maplets merged on workers at every bundle change and read through the cache. A get
  reads its maplet's page where a filter answers from memory: fill 1.90M, reads 354K, seeks 59K
  at 100M against 2.10M, 438K, 63K. At 1B it fills 29% slower even with its block indexes in
  memory (the trunk image cannot name the extents they take; SplinterDB keeps them in pages, a
  page read more a probe). Its 1B seeks were twice none's, from the trunk its slower fill left
  (seeks read no maplet; research/43 §4).
Each branch also kept its keys' sorted 32-bit hashes, 4 bytes a key, for maplets to merge from:
gone with them. The filters' resident memory (10 bits a key) is what paged filters would save; a
filter kept in pages and read through the cache, as RocksDB's partitioned filters are, bounds it
when it binds, and is measured then.

## 5. Reads

1. **Memtables.** The active and the immutable ones not yet incorporated.
2. **The hot region.** F2's in-memory region and read cache (research/33 row 9) hold the hot 1%
   that takes half the gets, in memory with in-place updates. Its size and admission are
   measured, and S3-FIFO governs the cache (row 14). It never holds a value the tree does not
   also have durably by checkpoint or Raft log.
   - **Kept memtables** (research/51), built: a packed memtable whose branch is in the trunk
     stays readable, newest last, and one index over them answers a get in one probe: each
     key's newest entry among the kept memtables, by its 64-bit filter hash, as (memtable,
     offset) (`kept::KeptIndex`). The packing worker emits each memtable's (hash, offset) pairs
     from the walk it builds the branch with (the shard makes them from the memtable's own index
     for one it packed itself); a worker adds them to an index the shard hands it, the newer
     memtable's entry replacing an older one's, and hands it back. Two indexes take turns, one
     serving gets while the other is out taking the memtables kept since, so the shard never
     lends the one it reads and a get never waits for one. A get probes, after the frozen
     memtables, the few kept memtables the serving index does not yet cover, through their own
     indexes, then the index, whose entry is checked: its memtable still kept, its key the one
     asked. Every memtable packed after a kept one is kept, frozen, packing or active,
     so the first version found is the newest, and a key none holds is read from the tree. They
     hold the memory budget's idle memory: the budget less what the page cache, write memory
     and the record cache hold and the most a get grows the caches by (a cache chunk and a
     record page), looked at after every put's pacing and before every get, so the regions and
     the kept memtables never hold more than the budget after an operation. Any region that
     grows takes memory from them, oldest first, as F2's log evicts at its head; one goes back
     when the page cache is refused the machine's memory, and before a rotation would take a new
     memtable while the machine has not twice its bytes free (the cache's rule). A fresh
     memtable's index is sized for the entries the one before it held, so it neither grows nor
     migrates as it fills. Without a memory budget, or without workers to build the index (a get's
     probes would grow with the memtables kept), none is kept; reopened, none is. An index drops
     the entries of memtables given back when they are more than half of what it holds.
3. **The tree.** Root to leaf; at each node each branch's filter is asked before its leaf is
   read. With the filters and branch interiors in memory, a cold get costs about one device read
   (research/34 §1).
4. **Seeks and listings.** A scan merges a cursor per branch on its path; a SuRF-style range
   filter answers "nothing in this prefix" without reading (research/33 row 8). REMIX views were
   built and measured, and removed (below).

### Seeks and listings (step E6), as designed

Scans as built (2026-10-07) merge, at each leaf a scan reaches, a cursor per branch a key there
probes: the pending branches, the in-flight and pivot bundles on the path, and the leaf's own
bundle. A scan finds its leaf in one descent (`Trunk::segment_at`) and the next only when that
one runs dry, so a seek's cost does not grow with the data. It allocates nothing per row and
about ten times a seek (the merge's cursors), and runs at 1.6–2.0× RocksDB's seekrandom
throughput at 3 M keys (benches/shard_db.rs, 2026-10-07: 151–165 k seeks a second, p50 5.9–6.4 µs,
against 84–94 k at p50 9.5–11.2 µs). What remains is the merge: a seek descends every branch of
the leaf's bundle, and every `next` compares the heads of all of them.

A 10M seek's placing is 72% of a seek of ten (research/61, 2026-10-18). Landing on its leaf reads
the cache index's control word and bucket together and starts the page's first line with the slot,
and loads only the entry the leaf's table search finds; where a leaf segment holds one source and
no memtable holds a key before its next, its rows go out in a run, with no comparison of sources a
row (`ScanMerge::push_run`). Seeks 1.20× at 10M and 1.25× at 100M. The trie's floor and the table's
search are the placing's steps left.

**REMIX views were built, measured and removed** (2026-10-11; research/43). A view per pivot
bundle of two runs or more (Zhong et al., FAST 2021; research/34 §4) was built in the shard's idle
time and, on the async path, from every run each time its bundle gained one (a rebuild's
merge-point reads could not be readied ahead): a root pivot's view was rebuilt after every
memtable rotation, quadratic in its runs. Measured on the async ranges path:
- **Cost.** Views took 18-63 s of a 100M-key fill (22-62% with maplets), while nothing reads them
  during a fill.
- **Gain.** A view seek still positions a cursor in each run, so it reads as many pages as the
  merge; it saves comparisons, and a 100M seek spends ~62% of its time on page access (research/43
  §1). Microbenchmarked, a view seek gains nothing at 2-4 runs and 1.8× at 8.
- **A/B** (three rounds, 1M-100M, against RocksDB; research/43 §3): with no views and no maplets,
  100M fills ran 2.10M a second, reads 438K and seeks 63K, against 1.19M, 285K and 43K with them
  built speculatively, and against views bought by seeks' measured rent (2.04M, 430K, 53K) or by
  one account choosing a view or a consolidation (2.25M, 449K, 47K): building a view in the phase
  that wants it costs that phase more than the view saves it.
RemixDB and SplinterDB build their structures eagerly because there they are mandatory (REMIX
§4.1: tables carry no index or filter); here every bundle keeps its branches' indexes and filters,
so nothing requires a view, and none is built. Seeks are measured against the page-access cost
they pay instead.

**Whole consolidations paid for by writes** (2026-10-14; research/53 §11). Seeks' rent buys each
pivot's consolidation once it covers the pivot's price (research/50 §3, §10), so after a bulk load
the seek phase carried the whole trunk's consolidation: at 100M it ran 2.5–2.9× RocksDB, where the
same seeks on the trunk they had consolidated ran 5.4–5.8× (research/53 §1). A shard that falls quiet
(its memtable empty since a flush, nothing packing or frozen, its trunk idle and owing nothing) now
consolidates every pivot of several sources at once, as a drain does, once the writes since the
last whole consolidation reach the entries the trunk holds. That is global rebuilding's rule
(Overmars, The Design of Dynamic Data Structures, LNCS 156, Springer 1983): each rebuild is paid for
by at least as many writes as it rewrites, however often the shard falls quiet. A/B'd against the
rent alone and against consolidating at every quiet moment, with bursts of puts after the queries:
- consolidating once quiet ran 100M seeks at 347K a second against the rent's 143K on a quiet host,
  its consolidation in the window after the fill (4.1–6.5 s), where RocksDB's compactions and
  SplinterDB's barrier take about 5 s;
- at every quiet moment, each burst rewrote the whole trunk: 25–44 µs of CPU a put at 100M against
  the rent's 1 µs;
- paid for by writes, the queries ran as at every quiet moment (on a busy host at 10M, seeks 1.74×
  the rent's) and the bursts as under the rent alone (1.16 µs of CPU a put against 1.50).
The rent still buys pivots wherever a shard never falls quiet or its writes have not paid for a
rebuild.

A whole consolidation is a pass down from the root, writing each leaf once (2026-10-15;
research/57 §2). Its flushes came down a trunk of several index levels in waves, and a leaf settled
before a wave reached it was paid again and settled again: at 100M every leaf of the drain's
consolidation settled twice, the first settles 60.8M of its 127.2M entries. In a whole
consolidation a pivot's flush and a paid leaf's settle now each wait while a pivot above them on
their path will still flush data down (held in its bundle, in flight or in a compaction out), so
each leaf settles once, after the last data lands, and a pivot whose path holds nothing to come
goes at once. Leaves alone waiting (the first form of the change) let a consolidation end with a
leaf unsettled or leave data above the leaves; waiting for pivots that held nothing slowed a 10M
drain (research/57 §5).

**SuRF range filters** (Zhang et al., SIGMOD 2018; research/33 row 8, research/35 §2) let a
bounded seek pass over a source with nothing in its range without reading it (closed seeks up to
5× in RocksDB). As built (2026-10-07):
- **Per branch, deletions included.** Each branch keeps a filter of its keys, built as the keys
  are added (`fst::surf::SurfBuilder`, its buffers from the store's pool, so a warm builder adds a
  key without allocating) and written after the leaf index in the branch's filter stream. A
  deletion must stay visible to a scan, since it hides older versions, so it is filtered like a
  put.
- **SuRF-Real, exact about intervals.** A key is cut one byte past what it shares with either
  neighbour, and keeps the next `n` bits as the trie's value (`RANGE_FILTER_BITS = 8`, measured).
  Where the paper answers with a false-positive flag, an entry is treated here as the interval of
  keys it can stand for: from the prefix followed by the suffix bits, to before the prefix
  followed by the next suffix. `may_hold(from, end)` checks the stored prefixes of `from` (one
  descent reports them) and the ceiling against `[from, end)`. No range holding a key is ruled
  out.
- **The trie under it.** The trie is built in one streaming pass with packed values. Only
  S-LOUDS keeps select samples, as the paper says; dense labels are found by word scans. The
  sparse part is kept a level at a time, as the builder lays it out, not assembled into one
  sequence as the paper does. A level's nodes are the branches going on above it, so rank and
  select run within the level. Finishing copies words, not labels: 0.17 ms for 500 k keys
  against 3.3 ms assembled, which was the put latency maximum once a flush.
- **Consulted on bounded scans.** At a segment's open, a branch is passed over when its filter
  rules out `[from, min(end, segment end))`. An open scan does not check, since a leaf's end alone
  almost never rules out a branch above it.
- **Measured** (benches/surf.rs, 1 M keys): 16-byte random-number keys at 18.8 bits a key let
  0.03% of empty ranges through, checked in 84–167 ns. 42-byte object names at 21.9 bits let
  3.6–4.4% through, checked in about 0.63 µs. That descent goes through about 25 sparse levels and
  is bound by memory latency (`select1` is 36% of samples). Interleaving each label's byte and
  bits, and compressing single-child chains, are the measured next steps.
- **End to end** (benches/shard_db.rs against db_bench 11.8.1's `--max_scan_distance`, 3 M keys,
  300 k seeks reading 10, 2026-10-07): seeks bounded to 10 and 100 keys ran at p50 2.62–3.00 µs
  against RocksDB's 7.66–7.85, and open ones at 2.54 against 7.77–7.94. No source was skipped. A
  trunk whose maintenance has caught up holds one branch a leaf and nothing above it, so a seek
  opens one source, and a range 10 keys wide almost always holds keys there. The check cost
  0.1–0.4 µs a bounded seek. The filter pays where several sources overlap a seek (bundles
  above the leaf while writes flow) or where ranges are empty. Under writes (benches/shard_db.rs
  mixgraph, 3 M fill then 4 M operations half puts half seeks, 2026-10-07):
  - seeks bounded to 1 key passed over 0.68 of 1.77 sources each, at p50 2.42 µs against 3.88
    open, 637 k operations a second against 445 k;
  - seeks bounded to 10 keys passed over 0.10, and paid 0.1 µs for the checks.
  A check costs about 0.12 µs and a source passed over saves about 2.1 µs, so checks pay
  once about 6% of sources are ruled out; ten-key bounds sit at that break-even, and a gate
  would win at most about 3% there, so checks stay on for every bounded scan.
- **Seek maxima** (2026-10-07): under the mixed workload, a seek now and then takes 3 to 165 ms
  where p99.99 is 32–40 µs (RocksDB's: 59–81 µs). Each slow seek was attributed to its page
  reads (`pread`), and the maxima stay at 19–29 ms with direct I/O, where the OS cache's
  writeback is out of the path: they are the device's read stalls on a busy machine, not the
  engine's work. They are answered by caching, and above the engine by reads hedged across
  replicas.

Held to: seekrandom and short listings against RocksDB's `db_bench` at the same key count,
under load, every percentile, with allocations, reallocations and page faults a seek.

### The page cache, as built (step E8's first part)

With uncached I/O, which keeps the OS's dirty-page throttle off the put path (§6), the device
serves every read the engine does not. The store keeps its own cache of verified node pages
(`crates/engine/src/store/cache.rs`, `Store::set_cache`), off unless the owner sizes it:
- **S3-FIFO** (Yang et al., SOSP 2023), with the reference implementation's parameters: a small
  queue of 10%, a ghost of 90%, promotion at two reads, a count capped at 3. A page read once
  leaves through the small queue, so a burst of new pages does not flush the pages read again.
- **Written through.** A node page enters the cache as it is queued or written, as SplinterDB
  writes through its cache (research/34 §1) and as the OS's cache keeps a buffered write.
  Without it, 1 M reads after a 10 M fill missed 223,975 times at 1 and 2 GiB alike: pages
  compaction had just written, first touched.
- **Live pages only.** An extent whose last reference is released has its pages dropped, so
  the cache holds what nodes name, not the inputs of compactions done. A page written at an
  address replaces what the cache held there, and a failed write drops the cache until the
  owner reopens, so a read never returns a page its address no longer holds
  (`tests/shard_db_paced.rs`: a cache larger than the store serves every read and none
  stale; it fails with the replacement on write removed).
- **Scans look, never touch.** A compaction's cursor takes a page the cache holds without
  counting a read or moving it (`Cache::peek`), and reads the rest through its span (§4), which
  never enters the cache: a scan neither promotes, fills nor evicts. Attributing each put past
  p99.9 to the work inside it (`benches/shard_db.rs` `attribute`) found 7,815 of 10,001 waiting
  on a span reading from the device pages written moments before; with the look, no fill read
  the device (248,163 scan pages from the cache), put p99.9 went from 52.7–53.3 µs to
  33.1–34.3 µs, p99.99 from 221–226 µs to 51.5–53.8 µs, and the fill from 0.76–0.79 to 1.03–1.08
  M puts a second (10 M, uncached, 1.5 GiB, this Mac). What now waits at p99.9 is the extent
  write: 9,874 of 10,011 of those puts made one, about 40 µs each, synchronously (§6).

10 M puts then 1 M reads, uncached I/O, fanout 8, this Mac, 2026-10-07:

| cache | read p50 | read p99 | read p99.9 | misses | RSS |
|---|---|---|---|---|---|
| none (buffered I/O, the OS's cache) | 3.29 µs | 5.29 µs | 10.5 µs | — | 265 MB |
| 1 GiB | 2.12 µs | 141 µs | 178 µs | 40,985 | 1.37 GB |
| 1.5 GiB | 2.08 µs | 4.04 µs | 5.50 µs | 0 | 1.61 GB |
| 2 GiB | 1.92 µs | 2.96 µs | 4.25 µs | 0 | 1.62 GB |

The buffered row's memory is the OS's, outside the process and unbounded; the cache's is
the owner's and bounded. The uncached fills' tail beside them stayed steady: p99.9 67–74 µs,
p99.99 230–235 µs at 1.5 and 2 GiB, where buffered fills on this busy machine reached 2.4–5.3
ms. How the cache is sized against a node's memory, and the F2 hot region, are E8's next part.

## 6. Scheduling flush and compaction

Each shard schedules its replicas' flushes and compactions against foreground I/O, as SILK does
(research/34 §5):
- A memtable flush and the root's compaction are never paused.
- Deeper compactions take the bandwidth the foreground leaves and pause when it needs more.
- The device's bandwidth `T` is measured (CLAUDE.md §5).
- SILK's unstated constants (`ε`, the flush minimum, the pause granularity) are measured, not
  chosen.

### Paced maintenance, as built (step E7's first part)

A shard's flush and compaction ran inline on the put that filled the memtable: at 10 M puts one
put waited up to 6.0 s (uncached) or 2.9 s (buffered) while every compaction the flush set off
ran. The shard now does that work a slice at a time on its own worker
(`crates/engine/src/shard_db.rs`, `trunk/mod.rs`):
- **Two memtables.** A full one goes to packing, still read, and a cleared one takes the puts
  (RocksDB's `max_write_buffer_number` of 2 by default). With maintenance workers, the memtable
  kept a copy of its arena as it was written, and a worker sorts that copy and builds the branch
  (research/40 §4); the shard orders a memtable only for the scans that read it. Without
  workers, or with none free when a rotation must finish it, the shard packs it itself, its
  order and walk paid a slice at a time.
- **Pending branches.** A packed memtable waits as pending, read before the tree. The next
  cascade takes every pending branch into the root together, so `k` waiting memtables are
  merged in one compaction instead of `k`.
- **Cascades in steps.** The flush-then-compact recursion is an explicit stack of frames. A
  compaction (`branch/merge.rs` `Compaction`) merges a budget of keys a step and changes its
  node only when done. Nothing else changes a node while a cascade runs, so every read between
  steps sees the trunk whole (`tests/trunk_test.rs`, `tests/shard_db_paced.rs`, each failing
  when reads skip pending branches or the packing memtable).
- **Pacing, derived.** A put of `b` bytes pays `debt · b / room` of each debt: the packing
  memtable's entries over the arena bytes the new memtable has left, and the trunk's known
  compaction work over that room plus a memtable for each of the `fanout` pending slots still
  free. Paid at that rate, a debt is paid when its room runs out. A compaction is owed only once
  planned, so a put whose memtable fills with debt left pays it whole and the engine counts a
  stall; 10 M puts at fanout 8 counted none. `ShardDb::maintain` pays debt in the worker's idle
  time.
- **Writer runs.** Each branch builder queues its pages in its own extent buffer
  (`store::Run`). With one buffer in the store, the packing builder and a compaction's builder
  taking turns wrote each other's runs a page at a time: 352,084 write calls a fill against
  15,829.
- **Write reservations.** `ShardDb::set_write_budget` still caps queued whole runs. The total
  budget (`set_memory`) first reserves issuer workspace for the owner and every admitted
  worker: each attached store's batch slots, active replacement and submit handoff, since
  `Store::submit` takes its replacement before waiting for a full attachment. The owner's
  depth peak and queued buffers returned to its warm pool stay charged after admission
  shrinks. Worker capacity is limited by the budget left after these reservations; fixed or
  retained write memory that cannot fit is refused before changing the configuration.
  A worker seat stays warm and charged until terminal retirement. The measured `want`
  limits active jobs, while `workers` counts those retained seats; a reduced need does not
  detach a Store and reuse its seat before the old Store has finished retirement.
  `memory_held` reports this reservation alongside the caches, whose existing gradual trim
  remains visible. This is not RSS: additional Builder working runs, input spans, point/page
  buffers, memtables and their arena copies, branch metadata, channel metadata and thread stacks are
  separate. Only the remainder moves by the marginal-gain rule (research/36).
- **Filters as keys arrive.** A builder adds each key to its filter as it adds the entry, so
  finishing a branch no longer builds a filter over millions of keys in one slice. A
  compaction's count is only bounded, so its filter starts at a power of two of blocks for the
  bound and halves exactly once the count is known (`branch/filter.rs` `fit`).

At 10 M puts (`benches/shard_db.rs`, fanout 8, this Mac, 2026-10-06). The paced rows are six
runs, four fill-only and two alternated with RocksDB 11.8.1's `db_bench` (same keys, values,
64 MiB write buffer, no WAL, no compression, `--histogram=1`):

| put latency | p50 | p99 | p99.9 | p99.99 | max | fill |
|---|---|---|---|---|---|---|
| inline, buffered (before) | 0.50 µs | 1.08 µs | 1.58 µs | 9.79 µs | 2.9 s | 12.9–45.6 s |
| paced, buffered | 0.83–1.17 µs | 1.62–2.67 µs | 12.5–19.1 µs | 44.6–2,996 µs | 5.1–160.6 ms | 9.5–18.0 s |
| paced, uncached | 1.00–1.21 µs | 2.46–2.96 µs | 106–138 µs | 238–3,073 µs | 6.2–6.9 ms | 16.9–20.6 s |
| RocksDB (alternated) | 0.62–0.76 µs | 2.64–3.00 µs | 3.99–7.74 µs | 18.9–132 µs | | 10.6–15.5 s |

Reads after the fill's owed maintenance is paid (`maintain`, 0.53–1.25 s): p50 3.33–3.62 µs and
p99 5.67–6.50 µs, against RocksDB's 4.29–4.30 µs and 10.0–14.0 µs. The buffered run's 160.6 ms
and 2,996 µs came with 6.59 s inside write calls, the OS's dirty-page throttle again.

The median rises by the work each put now carries: about two merged keys a put, which the inline
design paid all at once on one put. The uncached tail past p99.9 is the extent writes and span
reads a put still issues synchronously, one call in about 600 puts. Submitting them to the
device's issuer and going on, once hyper-block's issuer takes submissions without waiting, is the
next part of E7.

### Flat maintenance (CLAUDE.md §10), as designed

The cascade above is recursion on an explicit stack: a node's frame flushes one pivot, then waits
for that child's whole frame (its compactions, its flushes, its children's) before the next
pivot, and incorporation waits for the whole of it once `fanout` branches are pending. At 100 M
puts (`benches/ranges.rs`, seed 301, this Mac, 2026-10-10) one cascade in each run was a split
wave: 26 pivot compactions, 12 leaf compactions, 93–107 splits and about 100 M entries written in
15–17.7 s, while rotations filled `fanout` pending slots behind it, so a put waited 11.7–14.4 s.
Its single-job leaf compactions ran one after another, 9.6–11.8 s of each run, and the async
path admitted each node's jobs at the pool's measured need, one in 44 of the 58 slowest nodes,
beside 17 idle warm workers (benchmark-results/mantle-stall-trace-100m-20261010,
mantle-stall-urgent-ab-20261010). Admitting a stalled cascade's jobs within the cores changed
neither the stall nor the fill: the work was a chain, not a queue.

SplinterDB keeps no such chain (research/34 §2, trunk.c): a flush copies references only, so the
recursive flush ends before any compaction, and each changed pivot's bundle compaction is a task
the workers run in parallel and apply later, discarded if a flush or split changed the pivot.
SILK (research/34 §5) gives the work that blocks clients first claim: flush and L0→L1 are never
paused. The trunk becomes a set of per-node tasks, each with a trigger, run by one loop with a
counted budget; no task waits on another node's:
- **Incorporation** moves pending branches into the root's in-flight list whenever the root takes
  no structural change. A packing on a worker ends a branch at each of the root's pivot keys
  as they were when it was handed out, and where the root's pivots are still those and no pivot
  taking a branch has an in-flight bundle, a compaction out or a child to split, each branch
  becomes its pivot's newest bundle branch as it is: that pivot's compaction would have had it
  alone for input and written it again, 78.8M of the 374.0M entries a 100M fill wrote, all in
  the fill (2026-10-15; research/57 §7). Such a packing keeps its pages in room lent by the
  shard's cache, adopted with its branches when they are placed and given back when they go in
  flight, so a placed branch is cached where it was written, as a compaction's is. A compaction plans over the in-flight bundles up to `covered`, so later
  ones land past it. Puts wait only when the root holds more than `fanout` uncompacted in-flight
  bundles, the bound pending has now, so a read probes no more sources than today; this is the
  one stall left. Root compactions free it, so they are first among ready tasks, and while a put
  or a pending branch waits, every ready task is admitted within the cores (`Pool::can_take`'s
  rule for a wait).
- **Compaction**, one task per pivot with live in-flight bundles: it merges them, clipped to the
  pivot's range, into one branch, on a worker or inline a budget at a time, and its apply puts
  the branch at the front of the pivot bundle and moves the pivot's start to `covered`.
- **Timed where it runs.** A compaction's merging is timed where it runs, an inline task's steps on
  the shard or a worker's job on the worker, and its merged units are counted, so a compacted key's
  measured cost, which seek-paid consolidations are priced at, covers every compaction. Timing the
  shard's idle slices instead measured nothing once the compactions were on workers: the cost stayed
  unmeasured and no consolidation was ever priced. 1M seeks over 10M keys on the async path spent
  53% of the phase, 7.9 s, on rent, opening 3.07 sources a seek, and bought none
  (benchmark-results/mantle-async10m-diag-20261011, run `rent`).
- **Flush**, one task per pivot whose bundle passes `fanout`: its branches move to the child's
  in-flight list by reference, no entry rewritten, which makes the child's pivots compaction
  tasks. A pivot bundle has one owner, so a branch is still named by one node at a time.
- **Settle and split**, one structural task per node: a leaf past `fanout` branches or
  `leaf_entries` entries is compacted whole and split by entries; an index node past `fanout`
  pivots splits. The apply splices the parts into the parent, which may make the parent a split
  task in turn: a trigger per level, never a frame waiting on a frame.
- **Exclusivity, not discard.** A node with a structural task out takes no flush and plans no
  compaction until it applies, so no work is thrown away and no branch is shared; every other
  node proceeds.
- **Bounds.** At most one compaction and one flush per pivot and one structural task per node:
  the queue is bounded by the trunk's size, and a step ends on its budget. The debt is the known
  sum of the queued tasks, so pacing no longer meets work planned only once a chain reaches it.

Reads are unchanged: every apply runs on the shard's thread between steps, so a read sees each
node either before or after a task, never half of one. A checkpoint saves the nodes as they are;
an output not yet applied is named by no saved node and is freed at recovery, as a job out is now.

### Writes handed to the device's issuer (E7's second part)

The tail left after pacing was the extent write a put issued synchronously: attributed by
`benches/shard_db.rs` `attribute`, 9,874 of the 10,011 puts past p99.9 made one. The store now
hands each full run to the device's issuer (`Store::attach`, hyper-block `Attached::submit`)
with a stated number out at once, and goes on; a read or span over pages in flight waits for
their run, a flush answers every run first, the file's end moves only when a run is answered,
and a failed answer fences the store (`tests/store_issuer.rs`, `tests/shard_db_paced.rs`).

On a quiet device, uncached, two out: put p99.9 43.8–48.9 µs → 5.6–6.7 µs, p99.99 72.4–73.8 →
17.5–20.6 µs. On a contended one (this Mac minutes after boot, load average 7–12, the disk
busy with other processes), uncached writes leave the device to absorb every byte at fill
time: two out waited 3,709–8,367 times a fill, and even 256 out waited 8,653 times, since a
bounded queue cannot hide a sustained deficit. Buffered writes through the issuer let the OS
absorb bursts in memory while its throttling falls on the issuer's workers, never a put; with
the page cache serving compaction's reads, alternated with RocksDB 11.8.1's `db_bench` in that
load (10 M puts, 16 out, 1.5 GiB cache, 2026-10-07):

| put latency | p50 | p99 | p99.9 | p99.99 | max | fill |
|---|---|---|---|---|---|---|
| buffered, issuer, cache | 1.00–1.04 µs | 2.58–2.79 µs | 5.12–6.00 µs | 17.6–20.8 µs | 3.5–4.5 ms | 0.88–0.91 M/s |
| uncached, issuer, cache | 1.08–1.17 µs | 3.29–3.62 µs | 10.3–11.9 µs | 25.9–1,696 µs | 12–22 ms | 0.60–0.80 M/s |
| RocksDB | 0.60–0.69 µs | 2.54–2.98 µs | 4.19–9.64 µs | 18.3–30.6 µs | 1.4–3.5 ms | 0.83–1.01 M/s |

The batches out at once are the owner's to state: the extent buffers it spares. How they and
the caching mode derive from the device's measured latency and the node's memory is the next
part.

Measured again after the memtable, scan and trie work (10 M puts, 16 out, 1.5 GiB cache, this
Mac under load, 2026-10-07; benches/shard_db.rs `attribute`):
- put p50 0.54 µs (RocksDB 0.52), p99 1.8–1.9 µs (1.74–1.79), p99.9 4.0–4.3 µs (2.83–3.05),
  p99.99 12–14 µs (9.8–10.1);
- 1.4–1.6 M puts a second against RocksDB's 1.40–1.46 M;
- reads 5.6–5.9× RocksDB's and seeks 1.4–1.6×, both afterwards.
- Past p99.9, a put is the device's backpressure: those past p99.9 averaged 646 µs, 629 of it
  waiting for one of the 16 runs to be answered. Merging and building in the same puts took
  9–15 µs.
- RocksDB lets compaction lag behind a memtable and its level-0 files, hundreds of MiB, before
  it slows writes. A shard buffers its maintenance's output in its runs alone, 2 MiB, so its
  puts feel the OS's dirty-page throttle sooner.
- More runs out did not stop the waits: 64 out still waited 691–974 times a fill. A put that
  deferred its share while every run was out bunched the shares (1,116 entries in one put, 5
  stalls) and raised p99.9 to 5.3–5.8 µs.
- So the remedy is the part named above: runs out sized from the device's measured write
  latency and the memory the node spares the shard, not a larger constant.

## 7. Crash and recovery

- **Order of a checkpoint.**
  1. Write the new pages.
  2. Platform full flush.
  3. Write the superblock copy naming the new root, the allocator map and the applied index.
  4. Full flush.
  5. Release the old root's extents.
- **Recovery.** Open the newest valid superblock, check its root and map, and replay the Raft log
  from the applied index.
- **Log holes.** A Raft log with several batches in flight must not acknowledge a batch while a
  lower one is not durable, so its recovery stops at the first hole and loses nothing
  acknowledged (research/34 §3). That is hyper-log's rule to keep when it pipelines (step E0).
- **Proof.** The simulator crashes at every write and flush boundary and checks that the
  recovered state is a prefix of the applied entries containing every acknowledged one. Kill
  tests on real disks repeat it.

## 8. Order of work

The structure is research/33 §5's seven parts, each a step below with the evidence it is held to:
1. shared-nothing key-range shards, a worker each, requests batched per shard (p²KVS);
2. a log with several group commits in flight (SpanDB), and the log as level 0 (TRIAD);
3. SplinterDB's size-tiered Bε-tree, compacted by its shard, the compaction's
   granularity aligned to the last level (Spooky);
4. SuRF range filters for listings (REMIX views measured and removed, research/43);
5. an F2-style in-place hot region and read cache, S3-FIFO for the caches;
6. SILK-style scheduling of flush and compaction;
7. FDP or ZNS lifetime hints where the device has them.

Each step is gated, measured against its RocksDB counterpart under load with durable commit on
both sides, and landed before the next:

| Step | What | Part | Expected (research/33 §3) |
|---|---|---|---|
| E0 | hyper-log: several group-commit frames in flight, the stop-at-the-first-hole rule; measured at fdatasync / F_FULLFSYNC per device class | 2 | durable put throughput 5–9× on devices whose writes complete durably in parallel; none on a flush-bound device (measured on Apple's SSD, research/log-pipeline.md §5) |
| E1 | Pages, extents, checksums, the double superblock and the persisted allocator, with crash simulation (§3, §7; `crates/engine/src/store`) | 3 | correctness base |
| E2 | Shards: a range replica's engine on one hyper-rt shard, its requests batched, no state shared across shards | 1 | writes up to 4.6×, reads up to 5.4× (p²KVS, asynchronous log there; measured here with the durable one) |
| E3 | Memtable (concurrent B-tree) and branch packing | 3 | flush cost |
| E4 | Trunk, flush-then-compact, bundle compaction on the shard's tasks, granularity aligned to the last level | 3 | inserts 6–10×, write amplification 2× lower (SplinterDB); total write amplification with the device's GC about 2.5× lower (Spooky) |
| E5 | Per-branch blocked Bloom filters: a read probes only branches that may hold its key. Maplets routing a key to its pivot's branches were built, measured and removed (research/43) | 3 | queries up to 1.8×, space overhead 15–61% (maplets, abstract-level evidence); measured with filters: reads 1.2–1.7× RocksDB at 10 M (benches/shard_db.rs), 1.6× at 1B (research/43 §4) |
| E6 | SuRF range filters for listings (REMIX views built, measured and removed: research/43) | 4 | closed seeks up to 5× |
| E7 | SILK scheduling: paced maintenance on the shard's worker (landed, §6 as built), then I/O submitted without waiting | 6 | p99 10–100× under compaction; measured so far: the longest put at 10 M from 2.9 s to 5.1–49.5 ms |
| E8 | Hot region and S3-FIFO caches | 5 | hot gets up to about 10× |
| E9 | The log as level 0, if the Raft log's ownership of entries allows it (§10) | 2 | throughput up to 2.9×, write amplification up to 4× lower (TRIAD) |
| E10 | FDP placement hints, ZNS zones, where the device reports them (CLAUDE.md §5) | 7 | device write amplification about 1 (FDP); p99.9 reads 2–4× lower (ZNS) |
| E11 | Import and export of RocksDB files (§9); range split, snapshot and move | — | parity with what note 12 required of RocksDB |

### E1 as built

- **Page** (`store/page.rs`): a 20-byte header (CRC-32C, kind, format, payload length, the
  checkpoint generation), then the payload, zeros to the page's end. The CRC covers bytes 4 on
  and then the page's address, so a misdirected read fails as corruption does.
- **Superblock** (`store/superblock.rs`): copies at pages 0 and 1, generation `g` at `g mod 2`;
  the payload names the page and extent size, generation, applied index, root, the extents the
  file holds and the extents of the allocator map. Open takes the newest copy that verifies and
  whose slot matches its generation.
- **Allocator** (`store/alloc.rs`): a reference count an extent, persisted copy-on-write with
  each checkpoint. An extent the durable checkpoint names whose count reaches zero is reused only
  once a checkpoint that does not name it is durable; one it does not name, allocated since it was
  taken, is read by no recovery and is free again at once, so a file holds about its live extents
  rather than everything written since the last checkpoint (a 100M-key fill checkpointed only at
  its end held 54-64 GB for 11.6 GB of entries). The old map is released before the new counts are
  written, so the counts persisted never hold it.
- **Proof** (`tests/store_crash.rs`): power is cut after every write and flush of a workload of
  checkpoints, each crash losing all, none, or a random half of the unflushed sectors. Recovery
  always lands on the last acknowledged checkpoint or the one in flight, every page it names
  reads back exactly, the allocator holds exactly what it names, and the store goes on from it.
  Each step also writes and releases an extent no checkpoint names and reuses it at once for its
  data. Removing the deferred free (freeing a named extent at once), the flush before the
  superblock, or the old map's early release each makes the test fail.

## 9. RocksDB's format

The P1–P4 conversion's reader and writer of RocksDB tables stay:
- as **import**, to bring an existing RocksDB database in;
- as **export**, to hand a range to RocksDB's tools;
- as the **differential oracle** where formats overlap. The blocks and the codecs are reused
  directly.

The engine's own files are its own format, versioned and checksummed (§3). RocksDB's tools no
longer read them, and that was the owner's trade.

## 10. Open, to be decided by measurement

- `F`, `m`, the page and extent sizes.
- The hot region's size and admission rule.
- Whether the log serves as level 0 (TRIAD, research/33 row 6), given the Raft log's ownership of
  entries: step E9 decides it.
- Range split and move over a copy-on-write trunk: by references, exported as branches.
- The space-amplification bound, which no source gives (research/34 §1).
