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

Each step is gated, measured against its RocksDB counterpart and landed before the next:

| Step | What | Expected (research/33 §3) |
|---|---|---|
| E0 | hyper-log: several group-commit batches in flight, with the stop-at-the-first-hole rule; measured at fdatasync / F_FULLFSYNC | durable put throughput 5–9× on fast devices |
| E1 | Pages, extents, checksums, superblock and allocator, with crash simulation | correctness base |
| E2 | Memtable (concurrent B-tree) and branch packing | flush cost |
| E3 | Trunk, flush-then-compact, bundle compaction on shard tasks | inserts 6–10×, write amplification 2× lower |
| E4 | Maplets | queries up to 1.8×, space overhead 15–61% (abstract-level evidence) |
| E5 | REMIX views and a range filter for listings | seeks 1.4–9× |
| E6 | Scheduling | p99 10–100× under compaction |
| E7 | Hot region and S3-FIFO cache | hot gets up to about 10× |
| E8 | Import and export of RocksDB files (§9); range split, snapshot and move | parity with what note 12 required of RocksDB |

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
  entries.
- Range split and move over a copy-on-write trunk: by references, exported as branches.
- The space-amplification bound, which no source gives (research/34 §1).
