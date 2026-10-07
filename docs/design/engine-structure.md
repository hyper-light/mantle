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

## 3. Pages, extents, checksums

- **Units.** The engine's files are written in pages of the device's alignment (`B`, raft-log.md
  §2: the largest of 4 KiB and the device's block and write unit), grouped in extents of a
  measured size.
- **Checksums.** Every page carries a header checksum (CRC-32C, as the chunk store's frames)
  over its contents and its own address, so a page read from the wrong place fails like a
  corrupt one.
- **Immutability.** Pages of branches, maplets and trunk nodes are immutable once written:
  copy-on-write, reference-counted per extent.
- **Superblock.** Two alternating copies at fixed places, each with a generation and a
  checksum, written only after the data it names is durable; open takes the newest valid copy
  (research/34 §1, atomicity).
- **The allocator's reference counts are rebuilt or checkpointed.** SplinterDB invalidates its
  allocator map at every checkpoint and cannot rebuild it after a crash (research/34 §0). Mantle
  persists the map with each checkpoint, behind the superblock's generation, and can also rebuild
  it by walking from the root. A test compares the two after every simulated crash.

## 4. The tree: SplinterDB's size-tiered Bε-tree with maplets

The structure is research/34 §1–§2.
- **Branches** are immutable B-trees of 4 KiB pages, built by packing sorted data. Their index
  entries carry subtree counts, so a range's size is estimated without a scan.
- **Trunk nodes** carry pivots, a pivot bundle each and inflight bundles shared by the pivots.
- **Flush-then-compact.** A memtable becomes a branch, and a flush hands bundles down by
  reference. Compaction folds one pivot's inflight bundles into a single branch.
- **Maplets.** One per pivot maps keys to the branches that may hold them, merged sequentially
  as branches arrive.

What mantle changes, and why:
- **Who compacts.** Bundle compaction runs as tasks of the replica's own shard, scheduled by §6.
  There are no shared compaction pools, and so no thread per unit.
- **Bounds.** Every queue and map has a stated bound with a typed refusal:
  - the pivot-state map (SplinterDB's has 1024 buckets and no bound on entries);
  - the task queue;
  - the branches a range iterator collects (SplinterDB's cap is 256; mantle derives it from `F`
    and the height).
- **Parameters by measurement.** The fanout `F`, the memtable size `m` and the maplet hash width
  are measured on the running hardware and against mantle's key counts. SplinterDB chose 8,
  24 MiB and 26 bits without a measurement (research/34 §1). The false-positive rate per query
  follows from the width and the count (`n·2^−h`), so `h` is derived from the leaf size `F·m`
  and a target rate the owner signs off.
- **Checksums everywhere** (§3), where SplinterDB has none on data pages.

## 5. Reads

1. **Memtables.** The active and the immutable ones not yet incorporated.
2. **The hot region.** F2's in-memory region and read cache (research/33 row 9) hold the hot 1%
   that takes half the gets, in memory with in-place updates. Its size and admission are
   measured, and S3-FIFO governs the cache (row 14). It never holds a value the tree does not
   also have durably by checkpoint or Raft log.
3. **The tree.** Root to leaf; at each node the maplet names the branches to probe. With the
   filters and branch interiors in memory, a cold get costs about one device read (research/34
   §1).
4. **Seeks and listings.** A REMIX view per leaf gives seeks `log2(H·N)` comparisons and `next`
   no comparisons at all (research/34 §4). It is built when a leaf's branches change, by merging
   the old view with the new branch. A SuRF-style range filter answers "nothing in this prefix"
   without reading (research/33 row 8).

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
- **Scans bypass it.** A compaction's cursor reads through its span (§4), so a scan neither
  fills the cache nor evicts from it.

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
- **Two memtables.** A full one becomes the packing one, still read, and a cleared one takes
  the puts (RocksDB's `max_write_buffer_number` of 2 by default).
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
3. SplinterDB's size-tiered Bε-tree with maplets, compacted by its shard, the compaction's
   granularity aligned to the last level (Spooky);
4. a REMIX view and SuRF range filters for listings;
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
| E5 | Per-branch blocked Bloom filters (landed first: a read probes only branches that may hold its key), then maplets routing a key to its pivot's branches with one lookup in less space | 3 | queries up to 1.8×, space overhead 15–61% (maplets, abstract-level evidence); measured with filters: reads 1.2–1.7× RocksDB at 10 M (benches/shard_db.rs) |
| E6 | REMIX views and SuRF range filters for listings | 4 | seeks 1.4–9×, closed seeks up to 5× |
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
  each checkpoint. An extent whose count reaches zero is reused only once a checkpoint that does
  not name it is durable; the old map is released before the new counts are written, so the
  counts persisted never hold it.
- **Proof** (`tests/store_crash.rs`): power is cut after every write and flush of a workload of
  checkpoints, each crash losing all, none, or a random half of the unflushed sectors. Recovery
  always lands on the last acknowledged checkpoint or the one in flight, every page it names
  reads back exactly, the allocator holds exactly what it names, and the store goes on from it.
  Removing the deferred free, the flush before the superblock, or the old map's early release
  each makes the test fail.

## 9. RocksDB's format

The P1–P4 conversion's reader and writer of RocksDB tables stay:
- as **import**, to bring an existing RocksDB database in;
- as **export**, to hand a range to RocksDB's tools;
- as the **differential oracle** where formats overlap. The blocks and the codecs are reused
  directly.

The engine's own files are its own format, versioned and checksummed (§3). RocksDB's tools no
longer read them, and that was the owner's trade.

## 10. Open, to be decided by measurement

- `F`, `m`, the maplet width, the page and extent sizes.
- The hot region's size and admission rule.
- Whether the log serves as level 0 (TRIAD, research/33 row 6), given the Raft log's ownership of
  entries: step E9 decides it.
- Range split and move over a copy-on-write trunk: by references, exported as branches.
- The space-amplification bound, which no source gives (research/34 §1).
