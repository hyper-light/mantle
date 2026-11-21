# 100. A page cache's floor: memory of its own, never lent

The owner's decision of 2026-11-20: each range's engine's page cache holds a floor of its own
that it never lends, moves only once a turn, and keeps its structure at none past the floor. It
replaces a cache that only borrowed the operations' idle bytes (node.md §1.4). The notes below are
what the decision and its sizing rest on.

## 1. Pinning what every lookup passes

RocksDB, ground truth (the RocksDB wiki, "Block Cache", its section "Caching index, filter, and
compression dictionary blocks"; `include/rocksdb/table.h`, `BlockBasedTableOptions`):

- By default a table's index and filter blocks live outside the block cache, in the table reader's
  memory, for as long as the table is open. `cache_index_and_filter_blocks = true` puts them in the
  block cache beside the data blocks, so their memory is bounded by the cache's.
- Once they share the cache, data blocks can evict them, and a point lookup that finds its index
  or filter evicted reads it again before it reads its data block. RocksDB's answer is to pin them:
  `pin_l0_filter_and_index_blocks_in_cache` keeps level 0's index and filter blocks in the cache
  for the table's life (level 0 is read by every lookup), and `pin_top_level_index_and_filter`
  keeps a partitioned index's top level; `cache_index_and_filter_blocks_with_high_priority`
  places them in the cache's high-priority pool. Production setups pin them (research/23, C2:
  MyRocks runs `cache_index_and_filter_blocks` with level 0 pinned).

The rationale carried over: what every lookup passes on its way to the row is kept resident, so
a lookup costs at most the data block it ends on, and the cache's churn cannot add reads to it.

Mantle's engine already keeps each branch's filter and leaf index in its resident metadata,
charged to the engine's metadata share (`mantle_engine::branch::resident_bytes`): a point read
"reads its leaf alone" (`mantle_engine::branch::point`). That is RocksDB's pinned index and filter
by construction. What remains in the page cache is the leaf a read ends on, RocksDB's data block.

## 2. What borrowing alone did at many clients

Measured on the Mac Studio (M3 Ultra, 96 GiB, 2026-11-20), native GET on the closed ladder:
11,502 a second at 64 clients, 5,724 at 256 and 6,195 at 512, with the server's CPU and context
switches per GET doubling from 64 to 512 clients.

The cause in the code as built before: each cache borrowed only the operations' bytes no running
operation held, shrank at once as each operation was admitted or widened, grew back once a turn,
and a cache shrunk to none was dropped whole (`Store::shrink_cache`). At 512 clients the operations
hold almost every byte, so each admission emptied the caches and each turn rebuilt them: a leaf a
GET loaded was dropped before its retry took it, and read again, a resident `pread` on the range
leader's shard or a worker's load and its wake, every branch's filter probed again on each try.

## 3. The floor's size

A page for each read the node's operations ask at once (one an operation, `derive::asks`), spread
over its ranges and rounded up (`derive::cache_floor`): the leaf each read in flight loaded stays
until its retry takes it. It is the working set §1 leaves the cache: the data block each lookup in
flight ends on. It comes out of the node's memory in the ranges' class, so the node admits fewer
operations at once by its size: on the Mac Studio's 96 GiB, 60,913 where it admitted 61,960, a
page of 24,576 bytes an operation (`derive::tests`, macOS aarch64).

Counted (`derive::tests::a_read_s_pages_stay_in_its_floor_while_the_operations_hold_the_memory`,
macOS aarch64): 32 reads at once over a range opened as the node opens it, each tried and tried
again past the operations admitted meanwhile, for 20 turns, the operations holding every byte. Borrowing
alone, 1,280 pages were read for 1,280 gets, 1.00 a get; at a floor of 32 pages, none.

## 4. Admission stays exact

A cache moves only once a turn (`node::host::lend`), so admission cannot take bytes back from it
mid-turn. Instead it counts them held: an operation is admitted, and a running one takes more,
only into bytes neither the running operations nor the caches hold (`admit::Admission::admits`,
`holds`). The running operations' charges and the caches' bytes past their floors are within the
operations' memory at every moment, the floors are outside it, and what a cache's bytes kept out is
wanted at the next lend, which leaves it idle (`Admission::take_wanted`), the turn due at once for
the request it let in (`Node::door_due`, `Node::parked_due`).
