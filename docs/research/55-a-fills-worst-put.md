# 55. A fill's worst put: one merge while the cores sat idle

Sources.
- **[probe]** `benchmark-results/mantle-splinterdb-20261013`, runs `stallprobe-quiet-r1..2` and
  `stallprobe-busy-r1..2`: fill-only runs (100M puts on the range's shard, `fill_inside.sh`'s) of
  main `f3deb05` with a diagnostic never kept (`stall_probe.sh`): at each new worst put the bench
  measures, the shard thread's CPU time, faults and context switches since a reading at most a
  millisecond before it and the range task's phases inside it; each rotation stall with the
  trunk's tasks at its start and end; and the VM's reclaim, fault and pressure counters every
  100 ms. Docker Desktop's Linux VM on this Mac (18 cores), quiet and with 18 busy loops,
  2026-10-14. An earlier build of the probe (`stallprobe-v1-*`) timed every put and wrote to the
  mount shared with the Mac: one write blocked the shard thread 33 ms and the fill ran at half
  its rate, so none of its numbers is used.
- **[batch]** the same directory's runs `n100000000-r421`, `-r423`: the fill latency of mantle's
  arms, RocksDB 11.8.1 (32 GiB cache) and SplinterDB (32 GiB), 100M, interleaved.
- RocksDB 11.8.1 (`../rocksdb` @ `abeebd963`): `include/rocksdb/options.h:921-926`
  (`max_subcompactions`), `db/compaction/compaction_job.cc:542-700`
  (`CompactionJob::GenSubcompactionBoundaries`). SplinterDB (`../splinterdb` @ `7d607b11`):
  `src/memtable.c:191`.

## 1. What the worst put waited on

Up to p99.99 the fill's puts are faster than either engine's [batch]: 21–34 µs against
SplinterDB's 39–41 µs and RocksDB's 108 µs. Its worst put is not: 0.3–1.2 s against SplinterDB's
98–135 ms (db_bench's maximum is an overflowed value and unusable). The probe [probe]:

| run | puts/s | p99.99 | worst put | rotation stalls | the worst stall waited for |
|---|---|---|---|---|---|
| quiet 1 | 2.47M | 30.7 µs | 1.05 s | 19 | the root's settle: one task, 5.03M entries |
| quiet 2 | 3.09M | 26.6 µs | 215 ms | 3 | the root's pivot compactions: four tasks, 12.4M entries |
| busy 1 | 1.01M | 1.1 ms | 2.11 s | 4 | the root's settle: one task, 5.05M entries |
| busy 2 | 1.36M | 0.70 ms | 2.49 s | 3 | the root's settle: one task, 5.05M entries |

Every worst put is a rotation that stalled because the root had no room: its pending branches
and the most live in-flight branches a pivot reads reached `fanout` (`Trunk::has_room`). The shard
thread was off its CPU for nearly all of it, parked for a worker: 12.8 ms of CPU in the 1.05 s,
37 ms in the 2.49 s. The VM reclaimed nothing in those windows (no direct reclaim or compaction
stall, 46 GB free); its run queue held up to 38 runnable threads on 18 cores.

The longest is the root's settle. Early in a fill the root is still a leaf; once its branches pass
the bound (nine memtables, 5.05M entries) it settles whole into leaves of `leaf_entries`, one merge
on one worker: about 1 s quiet, 2.1–2.5 s busy. The pool held 2–3 of its 17 workers meanwhile.
The root takes no pending branch while it changes, so the pending ones fill its room and every put
waits for the merge. The other stalls are the root's pivot compactions after a root split leaves
it two pivots: one or two tasks of 1.7–3.1M entries, 0.4–0.9 s, once behind a child's settle (a
pivot never flushes into a child whose change has started).

## 2. Step one: does a put need to wait?

The root's bound holds what a read probes above the pivots' bundles to `fanout` sources; while it
holds, a put waits when the root is full. SplinterDB's writers do not: an insert waits only for
the next memtable (`memtable.c:191`), and its root takes more bundles as its reads probe more.
Keeping the bound, the wait is needed; its length is not.

## 3. Step two: this much?

The wait is the remaining time of one merge. Its work is fixed, each entry rewritten once; its
time is not. The settle's output is already cut into leaves, ranges no other overlaps, each the
merge of the inputs' entries in its range alone, and nothing makes one worker make them in turn.

## 4. The settle as one task a leaf (design)

- **How many tasks.** As many as the leaves the settle makes, at most one a core the workers may
  use (the pool's bound: the cores the OS reports less the shard's). Both are facts of the
  settle and the host.
- **Where they part.** At keys that cut the inputs' entries into equal shares, the entries below
  a key counted through each input's leaf index (the entries in the pages before the leaf the key
  falls in): exact to a leaf an input. A cut is built a byte at a time from the range ends'
  shared prefix, each byte the greatest whose keys are still below the share: the bytes every
  key in the range holds in two lookups each (the least key the inputs hold names them), the
  others by a binary search over a byte's values, until the keys between one byte and the next
  are no more than the lookups can tell apart.
- **What each makes.** Its share of the settle's own leaves: the `j`th cut ends where
  `floor(j * parts / tasks)` of the settle's `parts` leaves lie below it, so the tasks make the
  `parts` leaves between them, each task cutting its share evenly; no more leaves than the one
  merge would make, however the cuts fall. A first version let each task round its own count
  from an over-count of its entries (the leaves its ends fall in counted whole): at 10M it made a
  ninth leaf where the merge made eight, the root passed its fanout and split, and the tree grew
  a level, 32% more entries written and gets and seeks 10% slower (runs `n10000000-r441..443`).
  `TrunkStats::settle_leaves_made` never passes `settle_leaves_planned`.
- **How they apply.** Together, once all are done, their parts in key order: the one settle they
  are. A failure abandons all of them, as any task's does.
- **Their room.** Each task's grant is the extents the pages its range spans fill
  (`Branch::pages_between`), not the extents of its inputs whole, which every task of the settle
  shares.

RocksDB splits a compaction the same way, into key ranges run at once (`max_subcompactions`). It
is an option, off by default (1), and its boundaries come from 128 anchors a file
(`GenSubcompactionBoundaries`); a range is at least one output file. Here it is on by structure,
the task count comes from the settle and the host, the cuts are exact to a leaf, and a task is at
least one leaf.

## 5. Measured

To follow: the arm against main, fill-only at 100M, quiet and busy, interleaved; then the
release gate against RocksDB and SplinterDB.
