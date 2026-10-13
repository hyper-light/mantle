# 52. What a put pays beyond its insert

Sources.
- **[ab]** `benchmark-results/mantle-splinterdb-20261013`, runs `n*-r111..115-*`: 3e6378f (kept
  memtables, research/51) beside 1d5caa9 and SplinterDB with a 32 GiB cache in Docker Desktop's
  Linux VM on this Mac, five interleaved rounds at 10M and two at 100M, the ranges path
  (`benches/ranges.rs`), one shard, 16-byte keys, 100-byte values, seed 301, `--machine-memory`.
- **[sample]** macOS `sample` of a 10M fill of the lead build (9e53a6c), 2.5 s from 0.3 s into
  the fill, 1,215 samples of the shard thread `hyper-rt-shard-0`; inclusive samples by caller
  chain. The fill ran 4.27M puts a second, 234 ns a put, the shard on CPU in 96% of its samples
  (`kevent`, its parks, 3.5%).

## 1. Step one: does the fill need it?

Yes. Against SplinterDB [ab] the fill runs 3.32× at 10M (3.84M puts a second against 1.16M) and
2.86× at 100M (3.44M against 1.20M); the target is four times.

## 2. Step two: how much?

Four times SplinterDB is 4.64M puts a second at 10M and 4.81M at 100M: a put's 260 ns cut by 44 ns
(17%) at 10M and its 291 ns by 83 ns (29%) at 100M, on the shard thread that the fill keeps busy.

## 3. What a put pays [sample]

`apply_paced` is 73% of the shard thread: the insert into the memtable 27%, `pace` 38%. What
`pace` spends:

| | share of the shard | needed per put? |
|---|---|---|
| polling the pool for packed branches (`take_packs`: `retire_ready`, `Pool::take`, the channel) | 11.4% | no: the range's task takes them at each turn |
| polling the pool for a growth and answering workers' needs (`grow`) | 4.8% | no: as above |
| trunk steps (`Trunk::run`: dispatching tasks 2.7%, taking results, steps) | 10.9% | the share is; the per-call costs are paid per put |
| the kept memtables' memory look (`kept_crowded`) | 1.8% | only after a step that grew a region |
| `u128` division in `share` | 1.8% | no: `debt · bytes` fits 64 bits below 1.6·10^17 units of debt |
| `pace`'s own | 6.2% | |

Outside `pace`: the record cache's invalidation hashes the key again (0.9%, the insert's hash);
hyper-rt's thread clock and per-poll clock (6.8%, `__thread_selfusage` and
`mach_continuous_time`, fixed upstream in 4cbea75 and 9959b40); the bench's per-put latency clock
(4.7%, `benches/ranges.rs`, as `db_bench --histogram` times RocksDB's).

A turn of the range's task comes between the client's turns, every shard quantum (2.3 µs here,
about nine puts): `serve` reclaims the lent engine and steps maintenance there
(`ShardDb::idle_paced_step`), and with requests rather than inline puts it yields after each batch
of them. A branch taken at the task's turn rather than at the next put delays its retirement by
one turn of the task (2.3 µs here, one client on the shard), of a packing that takes about 100 ms.
The polls' 16.2% and the second hash need not be paid per put; the `u128` division need not be paid
below its overflow.

## 4. The trunk's pass, a put at a time [sample]

A put whose share of the trunk's debt is a unit or more runs one pass of the trunk's maintenance
(`Trunk::run`), as does every put while compactions are out on the workers: 11.9% of the shard.
The pass at 10M did no compaction work on the shard; its time was the pass's own:

| `Trunk::run`'s callees | samples of 1,215 |
|---|---|
| taking the workers' results (`tend_tasks`, `Pool::take`) | 57 |
| handing out jobs (`dispatch_tasks`) | 32 |
| walking the inline tasks, none running (`step_inline`) | 16 |
| applying finished tasks (`apply_done`) | 13 |
| moving pending branches to the root, looking at queued nodes, sweeping | 15 |

The compactions run on the workers; a pass takes their results and hands out what they free,
which a pass at each turn of the range's task does as well as one at each put. A put's share of
the debt, inline compactions' work when there are any, is paid as before, summed over the turn:
each share is the debt by the turn's bytes over the room left, as the puts' shares summed are.

The kept memtables' look (`kept_crowded`, 1.8%) follows the steps that may have grown a region, all
at the turn now. `trim` (1.4%) asked `trimming`, whose kept-memtable part computes the kept
memtables' bytes and limit while their index is out: that part is the index's to give back, not
`trim`'s, which brings down the page cache and the record cache.

All told, 16.2% of polls, 11.9% of the trunk's pass, the 1.8% look, the 1.4% `trim` check, the
`u128` division's 1.8% and the second hash's 0.9% are 34% of the shard's time per put, about the
29% 100M needs, and hyper-rt's clocks fixed upstream are 6.8% more.

## 5. Measured

Interleaved on macOS, 10M keys, five rounds, beside 9e53a6c on the same busy host
(`mac-ab.sh`): the polls at the turn (1e5fee7) filled at 1.17× (0.77–1.71), and with the turn's
shares paid at the turn (a3a840c) at 1.43× (1.27–2.21), 6.38M puts a second against 4.58M; the
puts' p99.9 1.9–2.4 µs against 2.3–3.7. Instructions a put across the process fell from 8,110 to
5,715.

In the comparison's container (`runs/n*-r141..145-*`, each build compiled from a fresh copy of its
tree), paired with 9e53a6c: the polls at the turn filled at 1.17× at 10M (0.99–1.41, five rounds)
and 1.27× at 100M (two), the turn's shares at the turn 1.25× and 1.10×, and with the seeks' rent
fixed (a388aa8, research/50 §14) 1.30× (1.10–1.58) and 1.28× (1.25–1.32), against SplinterDB with a
32 GiB cache 3.83× at 10M and 4.01× at 100M. The shard was on CPU for 88% of a 10M fill there
(`threads_sched.sh`: 2.60 s of 2.94 s, no run-queue wait), about 260 ns a put against about 150 on
macOS.

The shard thread after them [sample, a3a840c, 1,181 samples]: the insert 33% (the arena's copy
and its first-touch faults 10.5%, the copy for the packing worker 3.9%, the head's two varints
1.9%); the bench's clock read after each put 33%, which on aarch64 waits at its `isb` for the
loads before it, the insert's misses among them (research/36 §2), and its key generator 7%. The
head is written in one append now (`put_varint32s`, its operation's byte a varint of its own).

## 6. A new memtable's first touch, at 100M [sample100]

[sample100]: macOS `sample` of a 100M fill of a3a840c, 5 s from 12 s into it, 3,102 samples of
the shard thread; the fill ran 4.36M puts a second, kept all 178 memtables (19.7 GB).

A put copies its entry twice: into the memtable's arena and into the copy kept for the packing
worker (research/40 §4). The bytes are the same; the arena is new each fill, since the memtable
before it is kept for gets (research/51), while the copy's buffer comes back from the worker that
packed with it. The arena's copy took 437 samples (14.1%), the copy's 55 (1.8%): the difference,
12.3% of the shard, is the arena's first touch, a page fault for each of its pages (4,096 of 16 KiB
a 64 MiB arena) taken on the shard as puts reach them. At 10M it was 124 samples against 33 (7.7%
of the shard).

1. **Does the fill need it?** It needs the arena's pages mapped before its puts write them, not
   mapped by the shard: whichever thread writes a page first takes its fault.
2. **How much?** A memtable's arena and index, once a fill: 64 MiB and its index's tables,
   178 times in a 100M fill.
3. **Elsewhere:** a maintenance worker made the next memtable while the current one filled, its
   arena and index written once (`HashMem::ready`, `Work::Memtable`), and the next rotation took
   it (8ba0336).
4. **Fewer resources:** none beyond the spare memtable the shard kept before kept memtables took
   its place, made only while the machine had twice a memtable free.

Measured against a388aa8, interleaved: in the comparison's container (`runs/n*-r141..145-*`) the
fill ran 0.97× at 10M (0.77–1.12, five rounds) and 0.99× at 100M (0.97–1.00, two), gets 0.93× and
0.82×; on macOS at 100M, 0.97× and gets 0.88× (two rounds). The VM backs the arena with
transparent huge pages, 32 faults a 64 MiB arena; on macOS the first touches the profile charged
to the arena's copy were not what bounded the fill. Not kept: reverted, the cost measured and
named here.
