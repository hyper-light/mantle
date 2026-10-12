# 46. Probing a page once while the kernel reclaims nothing

Sources. XNU's source and the measurements below are ground truth.
- **[xnu-pageout]** XNU `osfmk/vm/vm_pageout.c` (apple-oss-distributions/xnu `main`, read
  2026-10-12): `vm_pageout_scan`'s `reclaim_page` frees a page and counts it by the queue it left,
  `vm_pageout_freed_speculative` (the speculative queue), `vm_pageout_freed_cleaned` (the inactive
  cleaned queue), `vm_pageout_freed_internal` (anonymous) and otherwise
  `vm_pageout_freed_external`; `vps_object_cache_evict` frees the pages of objects in the VM object
  cache, which holds only objects nothing references.
- **[xnu-sysctl]** XNU `bsd/vm/vm_unix.c`: `vm.pageout_freed_external`,
  `vm.pageout_freed_speculative` and `vm.pageout_freed_cleaned` read those counters, monotonic
  `unsigned long`s.
- **[xnu-purge]** XNU `bsd/vfs/vfs_syscalls.c`: `vfs_purge`, refused to all but the superuser,
  invalidates every vnode's cached pages through `ubc_msync(..., UBC_PUSHALL | UBC_INVALIDATE)`,
  "for simulating cold starts", outside the pageout scan and its counters.
- **[41]** `docs/research/41-resident-reads.md`: why a read the OS holds is made on the shard, and
  `mincore(2)` over a kept mapping as macOS's way to tell.

## 1. Why

A range reads a page its cache misses on its own shard when the OS holds it, after asking
`mincore(2)` whether it does [41]. Profiled on this host at 10M keys (`sample`, every thread,
final 3d2a59d, `benchmark-results/mantle-compaction-20261012`), the shard spent 19.3% of the read
phase and 21.2% of the seek phase in `mincore`, as much as in the `pread(2)` that follows it:
about 1.7 µs a probe in the seek phase against about 1.5 µs a read. Linux asks and reads in one
call (`RWF_NOWAIT`), so this cost is macOS's alone, and it doubles every miss's syscall time
against RocksDB, which reads without asking.

The answers repeat. The 10M tree held 194–242 thousand pages while its two query phases made 1.84
million resident reads, so at least 87% of the probes asked about a page already found resident.

## 2. When an answer stays true

A page found resident leaves the unified buffer cache only when something frees it. For a file
mantle holds open and never shrinks:
- **Memory pressure**: `vm_pageout_scan` reclaims it and counts it in one of the three counters
  above [xnu-pageout]. Their sum, read before a probe's answer is used, is an epoch: while it is
  unchanged no clean file-backed page anywhere has been reclaimed, so every page found resident
  since it was read still is.
- **The object cache's eviction**: only for objects nothing references [xnu-pageout]; an open
  file's object is referenced.
- **Truncation, invalidation, punched holes, `F_NOCACHE` writes**: mantle's own acts; it truncates
  and invalidates nothing it reads, writes through the cache, and punches only extents no branch
  names (a punch must forget those pages' answers).
- **`vfs_purge`** [xnu-purge]: the superuser's cold-start simulation, uncounted. After one, pages
  believed resident are read from the device on the shard until the epoch next moves. Its cost is
  measured, not assumed (§4).

## 3. Design

- One answer a 4 KiB page, the pages a read's probe already covers: a probe is never wider than
  today's. Measured here (`scratchpad/prof/probe.c`, a 256 MiB file, kept mapping): `mincore` of
  1 page 457 ns, of 8 pages 682 ns, of 32 pages 1,997 ns. A probe of a whole 128 KiB extent pays
  off only past about four further reads of that extent in one epoch; at 1B keys, with 1.2 million
  extents under pressure, it would cost up to four times today's probes.
- The epoch is read once per batch of requests a range serves, the runtime's existing unit of a
  shard's work: one read of the three counters, 942 ns each through `sysctlbyname` (measured here,
  100,000 reads), fewer through a resolved MIB. A changed epoch forgets every answer.
- Forgetting costs O(1): each 64-page word of answers carries the generation it was set in, and a
  new epoch is a new generation. The words grow with the file as its mapping does [41], 12 bytes
  for each 256 KiB of file.
- Measured on this host with 49 GB free and the load of the other sessions: none of the three
  counters moved in 10,000 one-millisecond intervals (`scratchpad/prof/reclaim.c`). Under
  pressure they move, answers are forgotten each batch, and every read probes as it does today.

## 4. What is measured with it

A read made on an answer, not a probe, could wait for the device on the shard and hold every task
there: the change counts probes made, probes skipped, epochs read and epochs changed; a shadow
mode probes anyway before each read made on an answer and counts the answers that were wrong; and
the A/B runs at 1M, 10M and 100M as the host is, under a memory hog that makes the kernel reclaim
file pages during the run, and at 1B, where the data exceeds memory.
