# 47. A fill held to its packing workers' pace

Sources. The measurements below are ground truth; the papers are cited for the one result each
is named for.
- **[bLSM]** Sears, Ramakrishnan, *bLSM: A General Purpose Log Structured Merge Tree*, SIGMOD 2012,
  §4 (the "gear" scheduler): each merge's progress is tied to the fill of the level that feeds it,
  so writes slow in step with merging instead of stopping when a level is full.
- **[rocksdb-wc]** RocksDB, `db/write_controller.h` and the wiki's "Write Stalls": writes are first
  delayed, at a rate set from compaction's progress, once level-0 files or pending compaction bytes
  pass the slowdown limits, and stopped only at the stop limits.
- **[xnu-apfs]** Measured here (`benchmark-results/mantle-fill-ioload-20261012`, `wlat.c`,
  `wpar.c`), not read from APFS's source, which is closed.

## 1. What a fill stalled on

10M-key fills of final (3d2a59d) on the ranges path are bimodal: in 4 of 9 runs of the residency
A/B a put waited 0.56 to 4.3 s, three memtable rotations stalling, and merging ran 4 to 16 times
slower an entry (`mantle-residency-20261012`). Profiled (`mantle-fill-bimodal-20261012`): the
issuer's device workers spent 5.4 times as long in `pwrite(2)` and 4 times in `pread(2)` in a slow
fill as in a fast one, for less data. Beside the fills, `iostat` showed the disk busy with another
process's small I/O (about 6,500 a second, 5 to 12 KB each) throughout the slow fill and idle
through the fast one.

A controlled load on the same disk names the cause (`mantle-fill-ioload-20261012`). One thread of
8 KiB writes each followed by `F_FULLFSYNC`, about 100 to 230 a second, stalled every 10M fill of
mantle (two or three stalls, puts waiting 1.1 to 9.6 s) and slowed RocksDB's by about 10% (worst put
7.6 to 87 ms). The same writes with `fsync(2)`, which does not flush the device's cache, or 6 GB/s
of buffered writes with no sync, did not. Under the flushes, a buffered `pwrite(2)` that writes
blocks for the first time, growing a file or into blocks reserved with `F_PREALLOCATE`, ran 8 to 10
times fewer a second with a p99 of 4.3 ms; one overwriting blocks already written was unchanged
(p99 12 µs) [xnu-apfs]. First writes into one file did not scale with threads (0.64 to 0.68 GB/s for
one to four); into separate files they did (1.91 GB/s for four).

## 2. Why mantle stalled where RocksDB slowed

Each put pays a share of its packing memtable's work over the room the active memtable has left,
so packing ends before the next rotation (`ShardDb::pace`). With maintenance workers, that share is
the feed: entries encoded into a worker's two buffers (`pool::FEED_BUFFERS`). When the worker is
behind, both buffers out with it, the feed took nothing and the put went on, the share dropped.
The fill then ran ahead of the workers' writes, and the rotation that found more packings out than
the measured overlap allows waited for the oldest one's whole rest, seconds when its writes were
slow. RocksDB delays writes as its backlog grows before it stops them [rocksdb-wc].

## 3. The change

The rotation leaves the measured overlap's packings out (`ceil(pack_ns / fill_ns)`, Little's
law) and waits for the older ones whole. Those, and only those, are what a fill must not leave to
the rotation: before a put's entry goes in, the unfed entries of the packings the next rotation
would wait for are compared with the entries the active memtable's room can still hold, and the
excess is fed first. On the async path the put yields until a worker gives a buffer back, on the
blocking path it waits for one, never for a whole packing's result. A worker gives a buffer back
once it has built its entries, which waits for its writes when the issuer's depth is full, so the
last stretch of a fill keeps to the workers' writes within two buffers and a write queue [bLSM].

Packings inside the overlap are never waited for, however starved: the room absorbs them, as
before. The rule was drawn there by a test: `a_starved_oldest_feed_never_holds_a_younger_packing_back`
holds the oldest packing's writes indefinitely and requires puts to go on until a younger packing
is out on another worker; pacing every unfed entry over the room, as first built, made those puts
wait on the held worker. The device still sets the fill's pace; what changes is that the wait for
a late packing comes a buffer at a time as the room runs out instead of a packing at a time at the
rotation. Stalls are counted by cause (a packing no worker took, packings past the overlap, a full
root) to show which remain.

The other half, writes slower on first write than on overwrite under another process's flushes, is
the store's: §1's measurements are what a fix there is held to.
