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

## 6. Scheduling flush and compaction

Each shard schedules its replicas' flushes and compactions against foreground I/O, as SILK does
(research/34 §5):
- A memtable flush and the root's compaction are never paused.
- Deeper compactions take the bandwidth the foreground leaves and pause when it needs more.
- The device's bandwidth `T` is measured (CLAUDE.md §5).
- SILK's unstated constants (`ε`, the flush minimum, the pause granularity) are measured, not
  chosen.

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
| E7 | SILK scheduling | 6 | p99 10–100× under compaction |
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
