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
- **What each makes.** Its range's entries cut at the leaf size of the settle's one merge,
  `per`; once every task is back, the parts are gathered into the leaves that merge would make,
  `ceil(entries / per)` of the entries they hold, each part in the leaf its first entry's share
  falls in, so a task's short last part may share a leaf with the next task's first, their ranges
  apart. The leaf count is the merge's from what was written: the count a settle plans from is an
  upper bound, its inputs sharing keys, and the merge makes fewer leaves when they do. A first
  version made at least one leaf a task: at 10M (3.9M distinct keys among 5.06M entries) it made 9
  where the merge made 7, the root passed its fanout and the tree grew a level, 32% more entries
  written and seeks 3-10% slower (runs `n10000000-r441..443`, `-r461..463`).
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

## 5. The root's merge (design)

With the settle split, the worst puts left waited on the root's pivot compactions once a root
split left the root two pivots: one or two tasks of 1.7-2.25M entries (`stallprobe-ps2-*`), the
shard thread parked through 0.2-1.85 s. A compaction's job measured on its worker
(`stallprobe-job-quiet-*`, 795 jobs past 200K entries in one quiet 100M fill) spent 825 ns of
wall time an entry and 214 ns of the worker's CPU: three quarters of a compaction is waiting,
and the slowest jobs, two inputs of 0.8-0.9M entries, waited over 90% of 3-4.5 s, about 4.5 ms an
extent read on a device queue every worker and the shard share.

- **Step one: is the root's merge needed?** Every entry is merged once at the root before it
  moves down: about 100M of the 417-464M entries a 100M fill writes. The merge makes one branch a
  root pivot, so a pivot's bundle can flush to its child by reference, and holds the sources a
  read probes at the root to `fanout`. The packing worker writes each memtable in key order
  already; cut there at the root's pivot keys, each piece is one pivot's alone and enters that
  pivot's bundle as its newest branch, the same bound on sources, no entry written twice. The
  root's room is then freed by flushes, which wait on no merge. So the merge is not needed.
- **The design.** A packing carries the root's pivot keys past its first, as they stand when it is
  handed out; the worker counts the sorted entries below each (a binary search of the sorted
  order) so each piece's filter is sized to its own keys, and builds one branch a range. A piece
  enters its pivot's bundle when its range lies in that pivot's and nothing older is live for the
  pivot (an in-flight bundle, or a compaction of one, is read before the bundle and must not be
  newer); a piece whose range a later pivot change crossed, or one behind older in-flight
  bundles, goes through the root's in-flight list and its compactions as every branch did. The
  root's room counts a bundle's excess past `fanout` (pieces entered while its child changes and
  it may not flush), so the sources a read probes at the root keep their bound.

## 6. Measured

The full bench (`inside3.sh`'s mantle arms: fill, gets, seeks, tombstones, the settled second
queries) at 10M and 100M, three quiet rounds interleaved, runs `n10000000-r481..483` and
`n100000000-r481..483`: main `f3deb05`, the settle split `84af82c`, the pieces on it `f4f7e5f`,
RocksDB 11.8.1 and SplinterDB, each with a 32 GiB cache. Rates and percentiles are the rounds'
medians, the maximum the worst round's. Two 100M runs ran beside a local build and test on the
Mac, whose cores the VM shares (`r482`'s pieces and RocksDB, the pieces' fill 2.82M against
3.09M and 3.42M in their other rounds): they are not counted. db_bench's fill maximum is a
wrapped negative interval; the lower edge of the highest bucket its histogram fills below an
hour stands in, marked ≥.

At 10M:

| arm | fill | p50 | p99.9 | max | gets | p50 | p99.9 | max | seeks | p50 | p99.9 | max |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| main | 3.42M | 0.25 µs | 4.35 µs | 540 ms | 2.25M | 0.42 µs | 10.2 µs | 0.29 ms | 501K | 1.89 µs | 17.9 µs | 0.64 ms |
| settle split | 2.92M | 0.25 µs | 5.63 µs | 9.1 ms | 2.18M | 0.42 µs | 11.3 µs | 0.22 ms | 474K | 1.98 µs | 21.5 µs | 5.1 ms |
| split + pieces | 3.73M | 0.21 µs | 3.58 µs | 332 ms | 2.42M | 0.42 µs | 5.63 µs | 0.13 ms | 527K | 1.82 µs | 18.9 µs | 0.69 ms |
| RocksDB | 485K | 1.26 µs | 28.8 µs | 43.2 ms | 212K | 3.75 µs | 31.5 µs | 10.1 ms | 100K | 8.89 µs | 53.9 µs | 9.4 ms |
| SplinterDB | 932K | 0.85 µs | 13.6 µs | 101 ms | 688K | 1.70 µs | 14.8 µs | 0.21 ms | 173K | 5.50 µs | 31.7 µs | 1.8 ms |

At 100M:

| arm | fill | p50 | p99.9 | max | gets | p50 | p99.9 | max | seeks | p50 | p99.9 | max |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| main | 2.97M | 0.29 µs | 5.12 µs | 1.16 s | 1.04M | 0.93 µs | 17.4 µs | 0.90 ms | 272K | 3.46 µs | 39.9 µs | 2.2 ms |
| settle split | 3.29M | 0.25 µs | 5.38 µs | 329 ms | 1.17M | 0.80 µs | 19.4 µs | 0.26 ms | 266K | 3.39 µs | 39.9 µs | 0.65 ms |
| split + pieces | 3.25M | 0.27 µs | 5.05 µs | 338 ms | 1.27M | 0.78 µs | 13.9 µs | 2.5 ms | 287K | 3.17 µs | 33.5 µs | 0.35 ms |
| RocksDB | 427K | 1.35 µs | 33.5 µs | ≥2.9 s | 165K | 5.62 µs | 64.0 µs | 9.3 ms | 62K | 15.0 µs | 145.5 µs | 28.6 ms |
| SplinterDB | 702K | 0.93 µs | 19.4 µs | 423 ms | 230K | 3.84 µs | 32.3 µs | 25.0 ms | 28K | 29.2 µs | 200.7 µs | 173 ms |

- **Writes.** Entries written at 100M: main 436M, the split 432M, the pieces 352M, the root's
  merge gone. The data disk's bytes over a whole run (`/proc/diskstats`, sampled each second):
  main 53-58 GB, the split 48 GB, the pieces 49-52 GB, RocksDB 90 GB; about 14 GB of mantle's are
  each new segment written once with zeros as the file grows (research/48). SplinterDB's 11.6 GB
  is the data written once: its cache holds the whole store at 100M and its compactions never
  reach the device, so its writes are not comparable here.
- **The worst put.** At 100M the pieces' worst round waited 338 ms, the other 58 ms, below
  SplinterDB's 423 ms; at 10M one round of three waited 332 ms in two rotation stalls (main 540
  ms in the same round), against SplinterDB's 101 ms. The settle split alone stalled in none of
  its 10M rounds.
- **Against the bound.** The pieces fill 4.6× SplinterDB's rate at 100M and 7.6× RocksDB's,
  gets 5.5× and 7.7×, seeks 10× and 4.6×.

## 7. The merge's pick: a tree of losers

Sources: D. E. Knuth, *The Art of Computer Programming* vol. 3, §5.4.1 (the tree of losers);
runs `n10000000-r501..503` and `n100000000-r501..503` (the full bench, quiet, interleaved).

- **Step one: is the pick's work needed?** A merge emits its inputs' least key, the newest of
  those tied, at every step. The pick scanned every cursor for the least and the runner-up, up
  to two key comparisons a cursor, and skipped the scan while the winner's next key stayed below
  the runner-up's. A root pivot's compaction merges its bundle and live in-flight branches, up
  to `fanout + 1` inputs (nine), its keys interleaved, so the skip rarely holds: up to 16
  comparisons a key. A tree of losers finds the next winner by playing the moved cursor's path
  again, one comparison a level, four at nine inputs.
- **A first version cost more than the scan.** Each match checked both cursors against the
  range's end besides their keys, three comparisons a match, and every step walked the winner's
  path a second time for the runner-up. Its merge took 170 ns a unit at 10M against the scan's
  144 (the workers' wall time over the units they merged, three rounds each), and 465 against
  332 at 100M in the first round, whose fill's worst put grew from 140 ms to 1.41 s: slower
  merges hold the tree's changes longer.
- **The second.** A cursor's state (spent: past its entries or the end) is noted once, when it
  moves, so a match compares keys alone; the runner-up is not kept; a move is ready to take
  without a read unless a cursor holding the key stands on its leaf's last entry, which a count
  of those cursors answers at once when there are none.
