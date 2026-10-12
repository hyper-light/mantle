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
its final length, written whole with zeros by a maintenance worker, past the OS's cache (§6), while
no other transfer uses it,
and only then are its extents handed out, so every transfer to a segment in use is an overwrite
(the zeros as PostgreSQL's WAL segments take them [pg], and as NTFS's valid data length needs
[ntfs]). The allocator stops at the extents admitted; the shard books the next segment when fewer
are left than a segment and a lead: a segment is as large as the largest grant asked, at least a
packing's, and the lead is the most the allocations took between a growth coming due and its
segment's admission, the wait for a free worker included (§6); a packing, a compaction or a
worker's ask that does not fit waits for the growth. Opened,
the segments holding an extent ever handed out are admitted, and a segment past them, which a crash
may have left unwritten, is written again before use.

## 6. Writing a segment whole

The first A/B of segments (`benchmark-results/mantle-segments-20261012`: segments 0906dfe against
the merge tree 5317963 and RocksDB, interleaved) found three costs, each work the design did not
need:

- Every segment kept its own reclaim epoch (research/46), so a read refreshed the epoch of the
  segment it reached: 473–637 K refreshes during a 10M run's queries against 3.7–62 K for one file,
  and reads 0.74 times the merge tree's. One epoch now serves all of a file's segments (39 K).
- The zeros went through the page cache, memory no read wants: those runs had 126–298 K file pages
  reclaimed each, against 0–148 K beside the merge tree, and reads probed residency 6–8 times as
  often. They now go through a handle past the cache.
- A segment doubled at every look while a grant waited, not once a growth: beside F_FULLFSYNC every
  10M fill, and one 100M fill, asked APFS for a segment past the free space (ENOSPC). A segment now
  doubles at most once a booking, and never past the file's size.

The second (`mantle-segments2-20261012`, 8dcf196) found the writing itself too slow under load:
one 128 KiB write at a time, each waiting for the device. Beside F_FULLFSYNC its fills ran at 652 K
puts/s, 0.76 times RocksDB, with 3–4 stalls each and a slowest put of 6.4 s, where the merge tree's
stalled 2–3 times each, its slowest 1.84 s. A 256 MiB segment written whole
(`benchmark-results/mantle-zfill-20261012`, `zfill.c`; 3 to 6 rounds each, interleaved, the host
as it was):

| written | beside F_FULLFSYNC | beside nothing added |
|---|---|---|
| 128 KiB a write, past the cache | 7.5–9.9 s | 1.3–2.1 s |
| 128 KiB a write through the cache, then one flush | 4.1–4.5 s | 0.42–0.85 s |
| 1 MiB a write, past the cache | 1.0–1.4 s | 0.23–0.58 s |
| 8 MiB a write, past the cache | 0.12–0.33 s | 0.03–0.15 s |
| 64 MiB a write, past the cache | 0.06–0.20 s | 0.04–0.07 s |
| pwritev(2), 1,024 vectors all one 128 KiB buffer, past the cache | 61–66 ms | 33–50 ms |

The device has in flight what a call hands it at once: XNU's cluster layer issues a direct write's
pieces asynchronously and goes on to a vectored write's next vector without waiting
(`bsd/vfs/vfs_cluster.c`, `cluster_write_direct`, `next_dwrite`). So a segment's copies go in
pwritev(2) calls of as many vectors as `IOV_MAX`, every one the same buffer
(`DeviceFile::write_copies_at`, Linux and macOS): as fast as one 64 MiB write, from 1/512 of its
memory. Beside the same flushes a 10M fill then ran at 2.52 M puts/s with no stall, its slowest put
572 µs (`benchmark-results/mantle-segments3-20261012/probe`).

On every target's runner (`benchmark-results/mantle-wholefill-20261012`, CI run 38310978631,
`crates/disk/benches/wholefill.rs` on branch `iobench/wholefill`; median ms of three rounds, the
same 256 MiB, beside nothing added / beside a writer making the platform's full flushes):

| target | 128 KiB a write | 1 MiB | 8 MiB | 64 MiB | `write_copies_at` |
|---|---|---|---|---|---|
| Linux x86_64 | 645 / 744 | 641 / 648 | 648 / 648 | 649 / 648 | 695 / 648 |
| Linux aarch64 | 854 / 944 | 764 / 783 | 792 / 646 | 646 / 645 | 803 / 795 |
| macOS aarch64 | 245 / 427 | 78 / 96 | 73 / 52 | 40 / 31 | 31 / 26 |
| macOS x86_64 (Rosetta 2) | 90 / 63 | 55 / 50 | 76 / 51 | 64 / 51 | 52 / 45 |
| Windows x86_64 | 963 / 1,677 | 962 / 1,066 | 960 / 981 | 955 / 967 | 963 / 1,686 |
| Windows aarch64 | 3,779 / 7,141 | 1,526 / 1,531 | 1,534 / 1,547 | 1,541 / 1,526 | 3,840 / 7,295 |

The Linux runners' disks write about 0.4 GB/s whatever the way, so every way there is the disk;
pwritev(2) keeps up with the largest writes on Linux and leads on macOS. On Windows, where
`write_copies_at` still wrote a copy at a time, one 128 KiB write at a time took 1.7–4.7 times as
long as writes of 1 MiB or more. NTFS's own gather write, `WriteFileGather`, writes from a list of
pages, each page-aligned and ended by a null, through a handle opened past the cache for
overlapped I/O (Microsoft, "WriteFileGather function"); every element may name a page of the same
buffer. So on Windows a file past the cache takes gather writes of a 128 KiB fill's copies, as
many a call as an element list no larger than the fill names (64 MiB), hyper-block's `gather`.
On the runners again (CI run 38313980358, `mantle-wholefill-20261012/gather`), `write_copies_at`
then took 1,555 / 1,548 ms on Windows aarch64, where a write a copy took 5,122 / 12,505, and 649 /
707 on Windows x86_64 against 653 / 1,154: the large writes' plateau, from one 128 KiB buffer.
On Linux and macOS it was again the fastest way or within a tenth of it.

The third (`mantle-segments3-20261012`, e44d9e5) still read less than the merge tree wherever the
host reclaimed file pages during a run, and segments runs saw reclaim far more often: 8 of 11
against 2 of 11. The cause was the handles, not the writes. Vectored writes past the cache leave
none of a 256 MiB segment's 16,384 pages in memory (`mantle-zfill-20261012/zres.c`), and both
arms' stores held the same pages at the end of their fills and their queries (1,444–1,474 MiB and
1,791–1,816 MiB, `mantle-reclaim-20261012`). But every handle made a slot for each segment its
bound allowed: about 30 K slots of some 500 bytes, the bench's bound being the open-file limit
over the handles a file takes, made again by every worker that opened one. A 10M fill's setup
touched 4.9 GB (313,692 page faults and 520 bytes a put, against 344 and 7.4 for one file). Slots
now sit in chunks of 1, 2, 4 and on, each made when a segment reaches it (Dechev, Pirkelbauer and
Stroustrup's bucket layout, OPODIS 2006): the same setup touches 473 pages.
