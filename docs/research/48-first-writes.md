# 48. First writes: what stalls a fill, and what a file's growth costs its readers

Sources. Measured on this Mac (APFS, the internal SSD) and on CI's runners for every target,
2026-10-12; APFS's and NTFS's sources are closed, so their behavior is what these measurements show.
- **[prewrite]** `benchmark-results/mantle-prewrite-20261012`: final 3d2a59d with diagnostics,
  its store file as the engine leaves it or written whole with zeros to 8 GiB before the fill.
- **[wmix]** `benchmark-results/mantle-wmix-20261012` (`wmix.c`): two threads overwriting or
  reading a written, cached 1 GiB region of a file while one thread makes first writes.
- **[wlat]** `benchmark-results/mantle-fill-ioload-20261012` (`wlat.c`, `wpar.c`): research/47 §1.
- **[ci]** `benchmark-results/mantle-firstwrite-20261012/ci` (CI run 38301221490):
  `crates/disk/benches/firstwrite.rs` on each target's runner, two rounds of every mode, buffered
  and direct, beside nothing and beside a writer making full flushes, the first-writer unpaced and
  at 0.3 GB/s: macOS 15 (APFS) on aarch64 and on x86_64 under Rosetta 2, Ubuntu 24.04 (ext4) on
  x86_64 and aarch64, Windows 2025 and 11 (NTFS) on x86_64 and aarch64. The runners' disks are
  virtual and slow (direct writes 0.1–0.36 GB/s), so only one mode against another on the same
  runner is read here.
- **[ext4]** Linux `fs/ext4/file.c`: `ext4_buffered_write_iter` takes the inode's lock exclusively
  for every buffered write; a direct write takes it shared when aligned and not extending,
  "regardless of extent state", and exclusively, completing synchronously, when it extends.
- **[xfs]** Linux `fs/xfs/xfs_file.c`: buffered reads take the iolock shared and buffered writes
  exclusively; aligned direct writes shared; extending direct writes exclusively.
- **[pg]** PostgreSQL, `wal_init_zero`: "new WAL files [are] filled with zeroes. On some file
  systems, this ensures that space is allocated before we need to write WAL records"; and
  `wal_recycle`, segments reused by renaming them.
- **[ntfs]** Microsoft, `SetFileValidData`: a write past a file's valid data length makes NTFS fill
  the space before it with zeros; setting the length without the zeros needs the volume-maintenance
  privilege and is not for general use.
- **[writevol]** `benchmark-results/mantle-writevol-20261012`: bytes through `write(2)` from
  `proc_pid_rusage` (`RUSAGE_INFO_V4`), every device-file write counted by size and by whether
  it landed on a page the process had written before, and RocksDB 11.8.1's own counters.

## 1. A fill's stalls are its first writes

A first write writes blocks of a file that were never written: past its end, into a hole inside
it, or into blocks reserved with `F_PREALLOCATE` [wlat]. The same 10M fills on the ranges path,
three rounds each, beside no added load and beside another process's `F_FULLFSYNC` writes
[prewrite]:

| load | store file | worst puts (ms) | stalls | fill/s (median) |
|---|---|---|---|---|
| none added | as the engine leaves it | 1,997, 24, 23 | 2, 0, 0 | 1,676,432 |
| none added | written whole first | 38, 0.4, 11 | 0, 0, 0 | 1,834,440 |
| `F_FULLFSYNC` | as the engine leaves it | 330, 1,328, 1,832 | 1, 1, 1 | 1,068,038 |
| `F_FULLFSYNC` | written whole first | 1.5, 0.4, 13 | 0, 0, 0 | 2,016,389 |

Two ordering fixes did not remove the stalls (`benchmark-results/mantle-urgency-20261012`, the
same loads): holding puts to the packing workers' pace (research/47; worst puts 13.0 s and 3.8 s)
and dispatching the device's queued transfers by urgency, packings first (SILK's order; 4.1 s
and 3.2 s); final's were 5.4 s and 1.2 s and RocksDB's 142 ms and 31 ms.

## 2. While a file is first-written, its other reads and writes wait

[wmix], three rounds each (six for overwrites beside none and past the end), p50 / p99 / max:

| the other thread's first writes | overwrites, no load added | overwrites, `F_FULLFSYNC` | cached reads, no load | cached reads, `F_FULLFSYNC` |
|---|---|---|---|---|
| none | 9–10 µs / 16–24 µs / 0.2–5.3 ms | 9–10 µs / 19–26 µs / 0.1–3.8 ms | 7 µs / 12–20 µs / 1–6 ms | 7–8 µs / 12–17 µs / 1.4–2.5 ms |
| into another file | 10–11 µs / 24–26 µs / 1.3–3.6 ms | 8–11 µs / 21–31 µs / 0.2–18 ms | 8–9 µs / 19–21 µs / 1.2–9.9 ms | 6–8 µs / 11–17 µs / 0.7–1.1 ms |
| past the same file's end | 12–85 µs / 0.9–3.9 ms / 0.66–1.01 s | 100–173 µs / 4.6–7.6 ms / 10–44 ms | 9–81 µs / 2.7–10.8 ms / 19–96 ms | 9 µs / 5.6–7.7 ms / 16–18 ms |
| into holes inside its end | 11 µs / 31–103 µs / 0.8–1.1 s | 12–16 µs / 4.0–4.4 ms / 8–13 ms | — | — |
| either, the first-writer `F_NOCACHE` | 103–127 µs / 0.4–6.3 ms / 15–36 ms | 18 µs–4.1 ms / 6.8–12.8 ms / 12–33 ms | 8–121 µs / 0.3–6.1 ms / 10–29 ms | 13 µs–4.1 ms / 6.3–16 ms / 12–25 ms |

A file's own first writes hold up every other transfer to it, reads included, whether they grow
it or fill holes inside it, cached or not, with or without another process's flushes; a
first-writer in another file costs nothing measurable.

## 3. How much a fill first-writes

A 10M fill on the ranges path made 3.92 GB of `write(2)` calls, nearly all one 128 KiB extent
each (29.9 K of 30.0 K calls); 2.37 GB of them landed on pages the process had already written
[writevol]. The 1.55 GB that did not is 1.5 times the 1.0 GB of extents its branches named at the
end; fills that stalled first-wrote 1.7–2.1 times, and grew the file to 3.7–5.4 times. RocksDB's
fill of the same keys wrote 2.73 GB (1.18 GB flushed, 1.55 GB compacted), each byte a first write
into a file of its own.

Of the 10M fill's compaction writes, 28% were entries a leaf's compaction wrote and a settle in the
same fill wrote again (13% at 100M; each branch tagged by what wrote it). Settling a leaf's in-flight
branches directly instead halved the entries a 10M fill wrote, but a started settle refuses its
parent's flushes, and starting it before those branches were merged held them off longer: 4 of 6
fills stalled where the merge tree stalled 1 of 6, and those ended with 5 splits where 11–13 were
due, their seeks 69–80 K/s against 130–320 K/s (`benchmark-results/mantle-settleflight-20261012`,
`mantle-twosteps-20261012`; branch `settleflight`, not merged). The write done twice stays; SplinterDB
splits a leaf by reference and rewrites only a leaf over `F` branches (research/34).

## 4. On every target

Each target's runner [ci], buffered, two threads' overwrites beside one more thread's writes, p99.9
in µs (the medians of two rounds; on the runners' disks a few milliseconds is a device's tail):

| target | nothing else | first-writer, same file | first-writer, another file | overwriter, same file |
|---|---|---|---|---|
| macOS aarch64, unpaced | 273 | 71,116 | 612 | 1,082 |
| macOS aarch64, 0.3 GB/s | 524 | 2,238 | 399 | 619 |
| macOS x86_64, unpaced | 136 | 71,020 | 303 | 280 |
| macOS x86_64, 0.3 GB/s | 158 | 2,461 | 102 | 311 |
| Linux x86_64, unpaced | 36 | 16,784 | 39 | 507 |
| Linux x86_64, 0.3 GB/s | 35 | 316 | 96 | 139 |
| Linux aarch64, unpaced | 39 | 14,687 | 45 | 196 |
| Linux aarch64, 0.3 GB/s | 39 | 250 | 40 | 158 |
| Windows x86_64, unpaced | 15,813 | 83,059 | 18,764 | 105,397 |
| Windows aarch64, unpaced | 14,763 | 15,659 | 43,691 | 17,368 |

A file's own first writes stall its other writes on APFS, ext4 and NTFS alike, and a first-writer in
another file does not; on ext4 every buffered write takes the inode's lock exclusively [ext4], so
writers of one file serialize even when they overwrite, which direct I/O's shared lock for aligned
writes does not [ext4] [xfs]. Reads: on APFS and on Windows x86_64 a first-writer in the same file
stalls them too (APFS unpaced p99.9 58.6 ms against 0.2 ms on aarch64, 81.2 ms against 0.14 ms on
x86_64); ext4's buffered reads take no lock and are untouched. The direct rows are the runners'
disks saturating, every mode alike, and decide nothing here.

## 5. The store grows by segments

So the store's file grows by segments (hyper-block `SegmentedFile`): a segment is created whole at
its final length, written whole with zeros by a maintenance worker while no other transfer uses it,
and only then are its extents handed out, so every transfer to a segment in use is an overwrite
(the zeros as PostgreSQL's WAL segments take them [pg], and as NTFS's valid data length needs
[ntfs]). The allocator stops at the extents admitted; the shard books the next segment when fewer
are left than a segment, which starts at a packing's grant and doubles whenever a grant had to wait
for one; a packing, a compaction or a worker's ask that does not fit waits for the growth. Opened,
the segments holding an extent ever handed out are admitted, and a segment past them, which a crash
may have left unwritten, is written again before use.
