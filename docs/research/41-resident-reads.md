# 41. Reads the OS holds in memory, made where they are asked for

Sources. The man pages and the measurements below are ground truth; the two papers are cited for
the one result each is named for, not re-read for this note.
- **[preadv2]** preadv2(2), Linux man-pages: `RWF_NOWAIT` (since Linux 4.14) "do not wait for
  data which is not immediately available"; `EAGAIN` when no byte is, a short count when only some
  are; `EOPNOTSUPP` for a flag the kernel or file system does not take; BUGS: Linux 5.9 and 5.10
  may answer 0 instead of `EAGAIN`.
- **[mincore]** mincore(2), macOS man page: one byte a page of a mapped range, `MINCORE_INCORE`
  set when the page is resident.
- **[mmap-db]** Crotty, Leis, Pavlo, *Are You Sure You Want to Use MMAP in Your Database
  Management System?*, CIDR 2022: reading through a mapping turns an I/O error or a truncated
  file into a signal, and an evicted page into a fault the caller cannot schedule around.
- **[CLRS]** Cormen, Leiserson, Rivest, Stein, *Introduction to Algorithms*, 3rd ed., §17.4: a
  table that doubles when full costs constant amortized time a growth.

## 1. Why

An async range must not block its shard on the device, so its demand reads went to the device's
issuer (hyper-block) and the range waited for the answer. Each such read is two to four handoffs
between threads, and each handoff is a wake of a parked thread that the OS's scheduler grants.

Measured on this host (Apple M5 Max, 18 cores, load average 76 to 98, 2026-10-10;
`benchmark-results/wake-latency-c-20261010`): a dispatch-semaphore ping between two threads, no
mantle code, wakes the waiter in 7 to 58 µs at the median, 217 to 986 µs at p90 and 3 to 4.4 ms
at p99. A thread's QoS class made no consistent difference.

A read the OS answers from its page cache takes microseconds (below). So the range spent nearly all
of a cache-missing get waiting for the scheduler: 19,151 reads in 100,000 gets at 10 to 25 thousand
gets a second (`benchmark-results/mantle-resident-reads-panel-20261010`, async arms), where
RocksDB, reading on its calling thread, served 438 to 763 thousand.

## 2. Telling, without waiting, whether the OS holds a range

- **Linux** [preadv2]. One call reads and tells at once: `RWF_NOWAIT` returns what is in memory and
  never waits. A short or zero count, `EAGAIN` or `EINTR` hands the read to the issuer. A refused
  flag (`EOPNOTSUPP`, `ENOSYS` before 4.6, `EINVAL` from an old kernel) stops the handle asking.
- **macOS** [mincore]. No such flag. `mincore(2)` over a read-only shared mapping of the file
  reports the unified buffer cache's residency. Measured on this host
  (`scratchpad resident.c`, `grow.c`, kept in `benchmark-results/mantle-resident-reads-smoke-20261010`):
  - exact: every page of a 64 MiB file written through the cache resident, none of one written
    with `F_NOCACHE`, and after one `pread(2)` exactly that page;
  - pages the mapping never touched, and pages written after the file grew past the end it had
    at mapping time, are reported, so one mapping serves the file's life;
  - costs under that load: `mincore` on a kept mapping 2.0 to 2.7 µs, with its own `mmap` and
    `munmap` 10 to 12 µs, `pread` of a resident 4 KiB page 2.6 to 4.7 µs.

  So the mapping is kept, and remapped twice as long when a read reaches past it [CLRS]. No byte
  is read through it [mmap-db]: a resident range is read with `pread(2)`. A page evicted between the
  probe and the read costs one device read on the shard; a byte read is never wrong.
- **Windows**. No interface here yet: every read goes to the issuer. The candidate to research is an
  overlapped `ReadFile` on a cached handle, which the cache manager completes synchronously when it
  holds the range.

## 3. Where the handle comes from

A runtime range's original file belongs to its native retirement owner, which closes it off the
shard once the issuer's attachment has retired (`engine-structure.md`, the range's retirement). So
the issuer's attachment keeps its submitter one more duplicate, for reads from memory only, made
when the file can tell (`BlockFile::reads_resident`). It is closed before the attachment signals
its retirement: the original is open until that retirement is answered, so the duplicate's close
releases a descriptor and is never the file's last. No file lock rides on these handles.

## 4. Measured

The same panel's async arm, 100,000 gets after 1,000,000 fills, seed 301: every one of the 19,151
reads made from memory on the shard (`resident_reads 19151`), at a mean of 2.8 µs each; 479
thousand gets a second, p50 1.21 µs, p99 4.42 µs, p99.9 11.25 µs; the shard's cross-thread wakes
in the run fell from 19 thousand to 355 (`benchmark-results/mantle-resident-reads-smoke-20261010`,
`reader-raw.txt`). The panel against RocksDB is `benchmark-results/mantle-resident-reader-panel-20261010`.
