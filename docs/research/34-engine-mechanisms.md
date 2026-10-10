# 34. The engine's mechanisms in implementable detail

The detail behind docs/research/33's recommendation: how SplinterDB's tree, maplets, SpanDB's
group commit, REMIX and SILK actually work. Sources:
- SplinterDB's source at `vmware/splinterdb@938676b` (2026-07-31), marked **[src]**.
- The papers' text, marked **[paper]**: SplinterDB ATC '20, SpanDB FAST '21, REMIX FAST '21,
  SILK ATC '19.
- The maplets paper (Conway, Farach-Colton, Johnson, SIGMOD '23, DOI 10.1145/3588726) has no open
  copy. Its part rests on the authors' arXiv 2510.05518 and the abstract, and is marked so.

## 0. What SplinterDB does not give mantle

Three facts about SplinterDB's current source decide what mantle takes from it:
1. **No durable acknowledgement.** An insert returns after a cache-page append; nothing is
   flushed per write. `docs/limitations.md`: it "does not expose an API to force the latest
   write to be durable".
2. **No crash recovery.** `core_mount` returns `STATUS_INVALID_STATE` unless the last unmount was
   clean. Its comment: "crash recovery (log replay + allocator rebuild) is not wired yet".
3. **No checksums on data pages.** The log pages and superblock carry checksum128; B-tree, trunk
   and filter pages carry none.

SplinterDB is the source of the tree's shape and its compaction policy. Mantle's commit, log and
recovery are its own (§3, §6), and every page mantle writes carries a checksum (CLAUDE.md §6).

## 1. SplinterDB's size-tiered Bε-tree [src: trunk.h, trunk.c]

**Pieces.**
- **Branch.** An immutable B-tree, referenced by root address and reference-counted per
  128 KiB extent (4 KiB pages, 32 to an extent). Each level is a linked list of pages. Index
  entries carry the subtree's `{num_kvs, key_bytes, message_bytes}`, so counts over a range are
  estimated without a scan (`btree_count_in_range`).
- **Bundle.** `{maplet; branches[]}`, oldest branch first. A bundle with no maplet holds one
  branch, which every lookup probes.
- **Trunk node.** `{height; pivots[]; pivot_bundles[] (one per child); inflight_bundles[]}`.
  - A pivot is `{key, child, inflight_bundle_start, stats}`.
  - A leaf is a node with two pivots and no children.
  - A node fits one extent.
- **Inflight bundles.** Data that reaches a node lands as inflight bundles, shared by all its
  pivots. Each pivot's `inflight_bundle_start` marks the oldest still live for it.
- **Bundle compaction.** A background step folds a pivot's inflight bundles into one branch,
  clipped to the pivot's range, and appends it to the pivot bundle. The pivot bundle's one maplet
  maps key → branch.

**Defaults** (`splinterdb.c:127-165`), and how the paper chose them (§2.3):
- **Page size:** 4 KiB pages.
- **Memtable size `m`:** 24 MiB, "comfortably large enough" that scanning a branch runs at disk
  bandwidth, and small against RAM.
- **Fanout `F`:** 8, from "typically 8 to 16". No measurement is given for 8.
- **Filter hash:** 26 bits.
- **Branches a maplet may name:** at most 32 (a u64 found-set).
- **Memtables:** four.
- **Query budget:** with RAM about 10% of the data, the filters (1–2 B/key) and branch interiors
  fit in memory, so a query costs about one I/O.

**Insert** [src: core.c, memtable.c, trunk.c]:
1. The memtable insert goes into a concurrent B-tree in the cache. The log record follows, under
   a shared insert lock.
2. A full memtable (2·m of extents) is rotated by one thread under an exclusive lock, and its
   generation is bumped.
3. A task packs the memtable into a static branch.
4. Incorporation runs strictly in generation order under a single modification claim on the
   root. The branch becomes a one-branch inflight bundle and is run through flush-then-compact,
   which builds a new copy-on-write root.
5. The new root is published by a pointer swap under the root lock, the old root released, and
   the collected bundle compactions queued.

**Flush-then-compact** [src: trunk.c:5253–5860]:
- **Receiving.** A node receives its parent's pivot bundle and inflight bundles as new inflight
  bundles; per-child statistics come from `btree_count_in_range`.
- **When a child is flushed:**
  - when its eventual branch count would pass `F`;
  - when its tuples pass the maplet's limit (`2·(extent/8)·2^9 − 1`, about 16.7 M);
  - and the fullest remaining child, if it holds more than `m` bytes.
- **What a flush does.**
  - It copies references only, raising branch reference counts. No data is rewritten, so the
    whole recursive flush to the leaves finishes before any compaction.
  - The child's split result replaces the pivot.
  - An index node over more than `F` children splits into `ceil(children/F)` nodes. Each copy
    keeps all inflight bundles, and liveness is the per-pivot cursor.
- **Leaf splits.**
  - The target leaf count is `round(estimated unique bytes / m)`. Unique keys are estimated from
    the maplet's fingerprints, sampling 1/16 of its index blocks.
  - Split keys come from the pivots' statistics, cut at even byte boundaries.
  - A single leaf over `F` branches is rewritten in place, which is a full compaction.

**Compaction** [src: trunk.c:2779–4255]:
- **Bundle compaction.**
  - Each changed pivot gets a `bundle_compaction` in a 1024-bucket state map. Workers run it, or
    foreground threads do once the queue passes the worker count.
  - It merges [pivot, next pivot) of the captured branches into one new branch: a full merge,
    dropping tombstones, only for an empty leaf's first compaction; an intermediate merge
    otherwise.
  - The pack hashes each key's fingerprint as it goes. No trunk lock is held.
- **Maplet compaction.**
  - It applies completed bundle compactions in FIFO order per pivot. Each adds the new
    fingerprints to the old maplet in one sequential merge, then path-copies root → node under
    the modification claim, appends the branch, and advances the cursor.
  - If a flush or split changed the pivot in between, the work is discarded.
- **Concurrency.** Compactions at different pivots run in parallel. Only the short path copy is
  serialized.

**Point query** [src: core.c:1259–1340, trunk.c:5937–6415]:
1. Memtables, newest first.
2. Then, from the root down: binary-search the pivots. For each live inflight bundle (newest
   first) and then the pivot bundle, the maplet gives a bitmask of candidate branches; probe them
   newest first.
3. Stop at a final value or tombstone; update messages accumulate.
4. A maplet lookup reads two pages: an index page, then the bucket's header and remainders.

**Range query.**
- Collect every live branch along the start key's root-to-leaf path, at most 256, and run a merge
  iterator over them and the memtables.
- At the leaf's end key, rebuild from there.
- Branch iterators prefetch the next extent within a 1 MiB budget.

**Maplet / routing filter format** [src: routing_filter.c]:
- **Fingerprint.** `fp = hash(key, seed) >> (32 − 26)`. For `n` fingerprints there are
  `max(floor(log2 n), 9)` bucket bits, the rest are the remainder, plus `bits(max branch)` of
  value.
- **Layout.** Buckets are grouped 512 to an index, each a header with a unary bucket encoding,
  then the packed remainder‖value array.
- **Build.** Radix-sort the new fingerprints, then one sequential merge with the old filter. No
  data is read and there are no random updates.
- **Lookup.** Popcount to the bucket, scan its entries, OR in `1 << value`.
- **False positives (derived from the format).** Expected false-positive branches per query are
  about `n·2^−26`:
  - about 0.4% at one 24 MiB incorporation of mantle's 91-byte pairs (about 276 K fingerprints);
  - about 3% at a full leaf (`F·m`, about 2.2 M).
  
  Mantle tunes the hash width for its key counts.

**Memtable** [src: btree.c:60–100]. A B-tree locked level by level (read → claim → write), one
lock at a time, restarting from the root on a claim conflict. A leaf split locks parent, child
and the next leaf in order. A node with half its space dead is defragmented instead of split.

**Cache** [paper §5.2, src: clockcache.c]:
- A direct-mapped lookup array over the device's pages.
- Per-(entry, thread) reference counts striped over cache lines; readers bump their count, then
  check the write bit.
- Threads take free pages from private batches of 64, and a CAS'd clock hand cleans 512 batches
  ahead of eviction.
- A dirty-generation fence drains writes, then `fdatasync`.

**Log** [src: shard_log.c]:
- A page per thread, records `{memtable_generation, leaf_generation, tuple}`. A page header holds
  a checksum128, a magic and the next extent.
- Full pages are written back asynchronously, **not fsynced**.
- The leaf generation, bumped on each insert and inherited across splits, orders a key's
  updates. Recovery sorts by (memtable, leaf) generation; it is not wired in.

**Checkpoint** [src: core.c:347–660]. Two logs:
1. Swap in a new log at a memtable rotation.
2. Seal the old one: write back dirty pages, `fdatasync`, record the log cut in the superblock,
   make it durable.
3. Once the cut's generations are incorporated: snapshot the root, write it back, barrier, record
   the root and first unincorporated generation in the superblock, durable.
4. Free the sealed log.

**Atomicity.** Every trunk change is copy-on-write and published by a pointer swap. Branches and
maplets are immutable, reference-counted per extent. On disk, atomicity is the double superblock:
two alternating copies, each with a generation and checksum, written after the data's
`fdatasync`, then synced. A checkpoint invalidates the allocator map, so a crash would need
reference counts rebuilt from the root, which is not implemented.

**Space amplification.** No bound is given. The paper concedes size-tiering "temporarily
increase[s] space usage". The only number is SplinterDB's 137% on update-heavy work, from the
maplets abstract.

## 2. Maplets (SIGMOD '23; abstract and arXiv 2510.05518 only)

- **What changes.** One maplet per pivot, a one-sided-error map from key to the set of branches
  holding it, replaces a filter per branch.
  - At the same false-positive rate it costs the same memory: "the bits that were used for FPR
    reduction are exactly used for the SSTable ids".
  - A query costs one maplet probe per bundle instead of 5–40 filter probes.
  - A maplet can stay paged out; one I/O fetches the block needed.
- **Compaction.** A new branch's fingerprints are merged into the maplet sequentially, without
  reading the other branches.
  - Filter compaction is decoupled from data compaction: branches within a pivot stay
    size-tiered, at most `F`.
  - Data is merged once per level, when flushed down.
- **Reported (abstract only, not verified against the body).**
  - Inserts match SplinterDB and beat RocksDB by up to 9×.
  - Queries beat SplinterDB by up to 89% and RocksDB by up to 83%.
  - Space overhead on update-heavy work is 15–61%, against RocksDB's 80–117% and SplinterDB's up
    to 137%.

## 3. SpanDB's group commit [paper §2.3, §4.1–4.2]

- **Path.**
  1. A worker builds the WAL entry.
  2. A spinning logger takes every queued entry and packs them into as few 4 KiB blocks as
     possible.
  3. SPDK write.
  4. A worker applies the memtable insert, which makes the write visible, and completes it.
- **Loggers.** One to three, adapted to write intensity, each with several requests outstanding:
  - 3 loggers × 3 outstanding peak on Optane;
  - 2 × 4 on a P4610 — up to 8 WAL groups in flight.
- **Layout.**
  - A raw page area (10 GB in evaluation). Every 4 KiB page starts with its memtable's log tag
    number, then entries carrying RocksDB's per-entry checksums.
  - Fixed page groups, one per memtable, recycled whole after it flushes.
  - A metadata page records each group's range and the active tag. It is not written per commit.
- **Ordering.** The only synchronization is the CAS that allocates pages. A write that observed
  another was issued after it completed, so it has higher pages and sequence numbers. Recovery
  replays in sequence order.
- **Recovery.** Read the metadata page, then scan the active group. A page with a stale tag marks
  unwritten space, so nothing is zeroed on recycle. Measured 10.25 s against RocksDB's 10.27 s
  for a 4 GB database.
- **Unstated in the paper, and decided by mantle:**
  1. Whether a completed SPDK write is on media (no FUA, flush or volatile-cache handling is
     mentioned). Mantle acknowledges only after the platform's full flush (CLAUDE.md §6).
  2. What recovery does at a hole. Concurrent batches complete out of order, so a stale-tag page
     can sit below valid ones. Mantle's rule: a commit is acknowledged only once every lower page
     is durable, so recovery stops at the first hole and loses nothing acknowledged.

## 4. REMIX [paper §3–4]

- **Structure.** For `H` sorted runs, the merged view is cut into segments of at most `D` keys.
  Each segment holds:
  - an **anchor key**, its smallest. The anchors form a sparse index.
  - `H` **cursor offsets**: per run, the first key ≥ the anchor, as a 16-bit block and 8-bit
    in-block index (256 MB per run).
  - `D` **run selectors**, one byte per key in view order:
    - 0x80 marks an old version, 0x40 a tombstone, 0x3f a placeholder;
    - at most 63 runs;
    - a key's versions stay in one segment, newest first, so `D ≥ H`.
- **Space.** `(L̄ + S·H)/D + ⌈log2 H⌉/8` bytes a key. For Zippy's 47.9-byte keys (mantle's
  shape) with `H = 8`:
  - 5.4, 2.9 and 1.6 B/key at `D` = 16, 32, 64 — 3.16% of the data at `D = 32`;
  - against 1.2 B/key for an SSTable block index, 2.4 with a 10-bit Bloom filter.
- **Seek.** Binary-search the anchors, set every cursor from the segment's offsets, then
  binary-search within the segment. To reach the j-th key, count its selector's occurrences
  before j (popcount) and advance that run's cursor. Cost is `log2(H·N)` comparisons, not
  `H·log2 N`.
- **Next.** Follow the next selector. No comparisons, no heap; old versions skip by their bit.
- **Get.** A seek and an equality check. No Bloom filters.
- **Build at compaction.** The existing view is one run, merged with the new run by generalized
  binary merging (Hwang). Merge points come from the anchors and at most `log2 D` keys per
  segment. Old selectors and offsets follow without I/O; one key is read per new segment's anchor.

## 5. SILK [paper §5.2]

- **Bandwidth.**
  - A thread samples client I/O bandwidth `C` every 10 ms and sets the internal limit `T − C − ε`
    through a token bucket (`T` the device's).
  - The limit changes only by 10 MB/s or more.
  - A minimum is reserved for flush and L0→L1, sized so the immutable memtable flushes before the
    active one fills.
- **Priorities.**
  1. Flush: its own pool, always the internal bandwidth.
  2. L0→L1: never paused. With all compaction threads busy, it preempts a random deeper
     compaction, whose partial work is discarded. At most one runs.
  3. Deeper compactions: the leftover bandwidth, paused and resumed individually or as a pool.
- **Pool size.** 4 threads with flush, "for a 200 MB/s drive": sized by device bandwidth over a
  compaction's need, not by core count.
- **Unstated:** `ε`, the minimum flush bandwidth, and the pause granularity. Mantle measures them
  (CLAUDE.md §4–5).
