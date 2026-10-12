# 48. First writes: what stalls a fill, and what a file's growth costs its readers

Sources. Measured on this Mac (APFS, the internal SSD), 2026-10-12; APFS's source is closed, so
its behavior is what these measurements show, not what its code says.
- **[prewrite]** `benchmark-results/mantle-prewrite-20261012`: final 3d2a59d with diagnostics,
  its store file as the engine leaves it or written whole with zeros to 8 GiB before the fill.
- **[wmix]** `benchmark-results/mantle-wmix-20261012` (`wmix.c`): two threads overwriting or
  reading a written, cached 1 GiB region of a file while one thread makes first writes.
- **[wlat]** `benchmark-results/mantle-fill-ioload-20261012` (`wlat.c`, `wpar.c`): research/47 §1.
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
