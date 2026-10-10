# Engine block reads and builds against RocksDB 11.8.1, 2026-10-06

The port's data blocks against RocksDB's: building them as a flush adds entries, scanning
every entry, seeking to every key, and point lookups through `SeekForGet`. Both sides build
the same blocks byte for byte (`tests/block_builder_golden.rs`), from SplitMix64-ordered keys
with 100-byte values, cut at RocksDB's default 4096 bytes with restart interval 16.

## Setup

- Apple M5 Max, macOS 26.4.1, rustc 1.94.1, release profile.
- RocksDB 11.8.1 static library, `crates/engine/benches/p4_block_bench.cc` built with
  `clang++ -std=c++20 -O2 -DNDEBUG -march=armv8-a+crc+crypto`.
- The port: `crates/engine/benches/block_read.rs`. The binary-search rows and every read row are
  at 56d15ad. The hash-indexed build rows are at a80b7e4, which changed only
  `data_block_hash_index.rs`.
- The machine was busy, as it is in service: load averages 39, 42 and 48 when the 59 runs
  began, 5, 11 and 27 when they ended (08:57 to 09:20 PDT).

## Method

59 runs of each workload (Wilks: the extremes of 59 bound the 95th percentile at 95%), the
two sides alternating which goes first. Each run builds the blocks, then scans, seeks and gets
once each. For each row:

- *ns* is the median per entry (per operation for seek and get), with its distribution-free 95%
  interval, order statistics 22 and 38 of 59 (Conover, *Practical Nonparametric Statistics*,
  §3.2).
- *ratio* is the median of the 59 per-run port/RocksDB ratios; below 1 the port is faster.
- *allocs* and *faults* are counted over the workload: global `operator new` on the C++ side,
  hyper-measure's counting allocator on the port's, `getrusage` minor faults on both.

`index` 0 is binary search (RocksDB's default), 1 adds the data block hash index. `key` is the
user key's length: 16 is db_bench's default, within RocksDB's 39-byte inline `IterKey` buffer;
48 is past it.

## Results

| Workload | N | Index | Key | RocksDB ns [95%] | Port ns [95%] | Ratio | Allocs RocksDB/port | Faults RocksDB/port |
|---|---|---|---|---|---|---|---|---|
| build | 10,000 | 0 | 16 | 64.5 [63.4–68.8] | 59.3 [57.6–61.7] | 0.91 | 582/287 | 80/80 |
| build | 10,000 | 0 | 48 | 74.0 [71.9–78.0] | 62.9 [61.8–65.0] | 0.87 | 600/296 | 83/83 |
| build | 10,000 | 1 | 16 | 71.9 [71.0–72.4] | 63.4 [62.9–63.8] | 0.88 | 875/288 | 81/80 |
| build | 10,000 | 1 | 48 | 85.0 [84.6–85.8] | 69.0 [68.4–69.7] | 0.81 | 902/297 | 84/83 |
| scan | 10,000 | 0 | 16 | 8.52 [8.4–8.9] | 6.34 [6.1–6.8] | 0.74 | 1/1 | 0/1 |
| scan | 10,000 | 0 | 48 | 8.78 [8.4–9.0] | 6.25 [6.1–6.8] | 0.74 | 2/1 | 0/1 |
| scan | 10,000 | 1 | 16 | 8.57 [8.4–9.0] | 6.21 [6.1–6.7] | 0.73 | 1/1 | 0/1 |
| scan | 10,000 | 1 | 48 | 8.54 [8.4–9.0] | 6.24 [6.1–6.6] | 0.74 | 2/1 | 0/1 |
| seek | 10,000 | 0 | 16 | 142 [135–150] | 106 [102–116] | 0.76 | 0/0 | 1/0 |
| seek | 10,000 | 0 | 48 | 149 [141–163] | 122 [112–127] | 0.81 | 0/0 | 1/0 |
| seek | 10,000 | 1 | 16 | 144 [135–152] | 106 [101–114] | 0.76 | 0/0 | 1/0 |
| seek | 10,000 | 1 | 48 | 150 [141–167] | 121 [115–132] | 0.81 | 0/0 | 1/0 |
| get | 10,000 | 0 | 16 | 154 [147–166] | 106 [100–112] | 0.69 | 0/0 | 0/0 |
| get | 10,000 | 0 | 48 | 176 [168–187] | 115 [110–127] | 0.66 | 182,340/0 | 2/0 |
| get | 10,000 | 1 | 16 | 161 [155–171] | 120 [116–129] | 0.75 | 0/0 | 0/0 |
| get | 10,000 | 1 | 48 | 201 [188–216] | 146 [142–166] | 0.74 | 182,340/0 | 2/0 |
| build | 1,000,000 | 0 | 16 | 63.1 [61.5–66.4] | 58.4 [57.0–63.9] | 0.93 | 57,154/28,573 | 7,374/7,629 |
| build | 1,000,000 | 0 | 48 | 73.1 [70.5–76.7] | 60.8 [59.9–65.4] | 0.86 | 58,834/29,413 | 7,592/7,846 |
| build | 1,000,000 | 1 | 16 | 70.2 [69.6–70.9] | 62.9 [62.8–63.0] | 0.90 | 85,733/28,574 | 7,374/7,629 |
| build | 1,000,000 | 1 | 48 | 82.9 [82.4–83.5] | 68.9 [68.8–69.1] | 0.83 | 88,253/29,414 | 7,592/7,845 |
| scan | 1,000,000 | 0 | 16 | 9.29 [9.0–9.7] | 6.68 [6.5–7.3] | 0.72 | 1/1 | 0/1 |
| scan | 1,000,000 | 0 | 48 | 9.16 [8.9–9.6] | 6.74 [6.6–7.1] | 0.74 | 2/1 | 0/1 |
| scan | 1,000,000 | 1 | 16 | 9.22 [9.0–9.7] | 6.63 [6.5–7.0] | 0.71 | 1/1 | 0/1 |
| scan | 1,000,000 | 1 | 48 | 9.04 [8.9–9.8] | 6.57 [6.4–7.1] | 0.73 | 2/1 | 0/1 |
| seek | 1,000,000 | 0 | 16 | 474 [449–533] | 477 [410–515] | 0.92 | 0/0 | 1/0 |
| seek | 1,000,000 | 0 | 48 | 478 [444–548] | 449 [426–546] | 0.96 | 0/0 | 1/0 |
| seek | 1,000,000 | 1 | 16 | 481 [449–536] | 429 [405–492] | 0.91 | 0/0 | 1/0 |
| seek | 1,000,000 | 1 | 48 | 470 [449–530] | 452 [421–502] | 0.95 | 0/0 | 1/0 |
| get | 1,000,000 | 0 | 16 | 486 [450–550] | 449 [400–503] | 0.89 | 0/0 | 0/0 |
| get | 1,000,000 | 0 | 48 | 521 [490–575] | 463 [417–527] | 0.88 | 911,765/0 | 2/0 |
| get | 1,000,000 | 1 | 16 | 600 [580–644] | 565 [540–613] | 0.95 | 0/0 | 0/0 |
| get | 1,000,000 | 1 | 48 | 625 [613–664] | 602 [575–634] | 0.96 | 911,765/0 | 2/0 |

The port is faster in every workload. A seek or get in a million-entry set is dominated by the
cache miss on the block, which both sides take, so the ratio there is nearer 1. At 48-byte keys
RocksDB's `Get` allocates once a lookup, because the key no longer fits `IterKey`'s inline
buffer; the port's lookups allocate nothing at either length.

The port's builds take 3.5% more minor faults at a million entries (7,629 against 7,374). Both
sides keep each finished block in one allocation of its own, so the cause is not yet known; it
is open, to be found and closed in the table builder, which owns the block buffers.

## Tails

Each operation timed alone, in nanoseconds; the scan per block. RocksDB / port.

| Workload | N | Index | Key | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| get | 10,000 | 0 | 16 | 166/84 | 292/208 | 375/292 | 834/583 |
| get | 10,000 | 0 | 48 | 167/125 | 333/209 | 416/292 | 1,584/583 |
| seek | 10,000 | 0 | 16 | 125/84 | 250/208 | 334/291 | 1,167/1,000 |
| scan (block) | 10,000 | 0 | 16 | 333/250 | 417/375 | 625/1,375 | 625/1,375 |
| get | 1,000,000 | 0 | 16 | 500/417 | 875/791 | 1,375/1,250 | 45,000/59,375 |
| get | 1,000,000 | 0 | 48 | 500/458 | 875/833 | 1,291/1,125 | 78,000/59,334 |
| seek | 1,000,000 | 0 | 16 | 459/417 | 834/791 | 1,292/1,042 | 89,583/65,416 |
| scan (block) | 1,000,000 | 0 | 16 | 333/209 | 500/375 | 875/583 | 13,709/10,708 |

The port's tails are at or below RocksDB's through p99.9, with one exception. The whole-block
scan's p99.9 at 10,000 entries, which over 286 blocks is the slowest block, is higher on the
port's side; its cause is open. The maximum of a single run is one
preemption on a busy machine on either side and decides nothing.
