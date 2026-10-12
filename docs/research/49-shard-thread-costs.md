# 49. What a fill costs the shard thread

Sources.
- **[sample]** `benchmark-results/mantle-shardcost-20261012`: macOS `sample` of a 10M fill on the
  ranges path (segments 38ea487, `--segments`), 3 s from 1 s into the fill, 2,077 samples of the
  shard thread `hyper-rt-shard-0`; self samples by function (`selftime.py`) and callers of each
  (`callers.py`). The fill ran 1.71 M puts/s on a host at load average 10–15.
- **[copy]** A 64 MiB copy on the same host: 1.0 ms at best of five, 69 GB/s.

## 1. Where the shard's time goes

The shard thread was on-CPU in every sample: at 10M the fill is the shard's to give. By what the
samples were doing:

| work | share of the shard | what |
|---|---|---|
| feeding a packing worker | ~29% | `feed_one`: the packing memtable's runs walked through a heap merge (`sift` 10%, `cmp`), each entry encoded for the worker (`encode` 5%, `memmove`) |
| ordering the memtable as it fills | ~20% | `merge_step` 10%, `sort_step` 5%, `schedule` 3%: a put's share (`order_put`) and idle slices' (`pay`) |
| the puts themselves | ~15% | `insert_hashed`, `parse`, the hash index |
| clock reads | ~18% | the bench client timing each put (8.5%); `serve` timing each maintenance slice (3.8%); the runtime's shard loop (4%); `__thread_selfusage` (1.3%) |
| the rest | ~18% | serving requests, dispatching tasks, pacing, growth |

## 2. Step one: is the work needed at all?

- **Ordering as the memtable fills.** A get needs no order: the hash index answers it. A scan
  needs it, and seals the tail at once when it does (`HashMem::seal`). The packing needs it, on a
  worker. So in a fill no scan reads, the 20% buys order that nothing reads until the packing,
  which a worker could make.
- **Feeding.** The worker needs the entries. The memtable has one owner (no shared ownership in
  this codebase), so they must travel: today as encoded entries a walk at a time, in order.
- **Timing every slice.** A slice is timed to size the next by its rate, and most of a fill's
  slices do nothing (research/48's slice count: 99% of about 9 M): those need no clock.
- **The client's clock.** The bench's own measurement, as `db_bench --histogram` makes RocksDB's.

## 3. Step two: how much of it?

- **Ordering.** The tail is sorted and runs merged level by level (the logarithmic method): an
  entry is moved once per level it climbs, about log₂ of the runs, on the shard. One sort at
  packing moves it a radix pass a byte of its prefix, on a worker.
- **Feeding.** Each entry is merged through a heap of the runs (log of the runs, a `sift` each),
  encoded and copied once. The arena itself, frozen, is one 64 MiB copy: about 1 ms here [copy],
  against the ~49% of the shard's time the order and the feed take over the memtable's fill
  (about 140 ms of a 64 MiB memtable's 0.28 s at 2 M puts/s).

The order a scan needs is bounded on purpose: the active memtable leaves at most `ORDER_BOUND`
(2,048) entries unsorted, so a scan after a burst of puts sorts at most two such chunks, about
50 µs; unbounded, the first seek after a 10M-put burst took 3.15 ms against 107 µs (research/40).
So the chunk sorts stay. What a fill no scan reads does not need is the rest: chunks merged level
by level as they come (about eight merges an entry at 2,048 a chunk and some 550 K entries a
memtable, `merge_step` ~10%), and the heap over the runs left when the memtable is fed (`sift`
~10%).

## 4. Steps three to five

- **Later.** Runs merge only once scans have paid for the merge, as the active memtable's tidy
  already is (`HashMem::tidy`, rent paid by scans), and a frozen memtable is put in one order at
  once: its runs' entries in one radix sort, by the prefix bytes that vary, a slice at a time, so
  its feed walks one run with no heap. A memtable of 550 K entries is 8.8 MB of 16-byte keyed
  entries a pass. (a)
- **Elsewhere.** The frozen arena copied whole to the packing worker (1 ms here [copy]), which
  sorts and builds; the shard keeps its memtable for reads until the branch is in. It takes the
  order and the feed off the shard, about half its time, for a second copy of a memtable while it
  packs. (b)
- **Fewer resources.** Each entry sorted once, by radix, rather than merged once a level.

(a) keeps the memory budget and the worker protocol; (b) takes more off the shard for 64 MiB more a
packing in flight. Both are built as variants and measured against this tree and RocksDB.

## 5. Later is not less

The ceiling on (a) was measured first, with a build never kept: the same tree with `ORDER_BOUND`
unbounded, so the memtable is put in order only once it closes, in one sort of every entry
(`benchmark-results/mantle-orderbound-20261012`, 10M keys, five rounds interleaved). It filled at
2.34–2.60 M puts/s against 2.50–2.69 M, the whole process's instructions a put 12.4–13.0 K against
12.6–12.9 K. Deferred, the order work is the same work: one radix sort of 550 K entries, 8.8 MB a
pass, is no cheaper than chunks of 2,048 sorted and merged while they are in cache. Neither
deferring nor merging less takes it off the shard; only moving it does, so (a) is dropped and (b)
is built.
