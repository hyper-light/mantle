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
one turn of the task (2.3 µs here, one client on the shard), of a packing that takes about 100 ms. The polls' 16.2% and the second hash need not be
paid per put; the `u128` division need not be paid below its overflow. Together with the clocks
fixed upstream they are 25.6% of the shard's time, about the 17% 10M needs and short of 100M's 29%:
the trunk's per-call costs come next.
