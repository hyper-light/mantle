# 90. Memory another process holds, and where a merge's inputs lie

Status: research note, 2026-11-15. For two of the conditions Ada set fin's line (2026-11-14, through
the lead): beside memory another program holds, mantle's 100M fills, gets and seeks slow no more
than RocksDB's and SplinterDB's ("we do not accept that slowdown"); and fin's fills, every file read
directly, come back to the current build's. Every number below was measured on the VM lane
(Docker Desktop's Linux VM on the MacBook Pro, transparent huge pages `enabled` set to `always`,
`defrag` to `madvise`), by the ranges bench with each phase's `/proc/vmstat` counters differenced,
perf's samples of CPU time and of page faults (one in eight) over each phase, and the diagnostic
build `g17-fin-diag` (fin with counters of where each compaction's inputs lie as it goes out, and of
each get's kept-index probes; never merged). "Held" is a second container holding 40 GiB of
anonymous memory, touched, for the whole case; "quiet" is the same case without it. Sources: [1]
the kernel's `Documentation/admin-guide/mm/transhuge.rst`; [2] proc(5) on `/proc/vmstat`; the
rest is mantle's code at the commits named.

## 1. Huge pages asked for beside held memory

`HugeZeroed` advised every kept record table `MADV_HUGEPAGE` before populating it
(`crates/engine/src/port/mmap.rs`, fin `d53dad11`). Under `defrag` set to `madvise`, the default,
memory so advised "will enter direct reclaim like always", reclaiming and compacting before a huge
page's fault maps, while memory not advised takes a free huge page where there is one and a base
page where not [1, `defrag`]. With `enabled` set to `always`, the advice adds nothing but the
stall: the kernel gives huge pages to every mapping [1, `enabled`].

| 100M fill, uncapped, fin diagnostic build | quiet (D1q, 2 cases) | held (D1h) |
|---|---|---|
| fill puts/s | 4,031,751 and 4,064,298 | 3,084,676 (0.76x) |
| gets/s after it | 3,260,050 and 3,262,128 | 1,990,754 (0.61x) |
| page faults over the fill | 1,498,179 and 1,500,592 | 3,623,309 |
| `compact_stall` | 0 | 4,577 |
| `thp_fault_fallback` | 0 | 4,577 |
| `pgmigrate_success` | 0 | 681,531 |
| `pgsteal_direct` | 0 | 170,411 |

Every compaction stall was a huge page's fault that then fell back (the two counters equal [1,
"Monitoring usage"]), and a region that falls back is mapped a base page at a time, 512 faults
where the huge page took one: 4,577 fallbacks are at most 2.34M more faults, and the held fill
faulted 2.12M more than quiet. The compactions moved 682 thousand of the process's own pages to
make room. The gets phase after the fill faulted only 16,951 times; its loss is §2's.
Asked for only where `enabled` is `madvise`, as a page buffer already asked (`g17-fin-hz`
`1cafbcae`), the held fills stalled in compaction never (D2h, two cases: 0 and 0), their faults
taking a base page where no huge page was free (901 and 3,239 fallbacks), and filled at 4,044,266
and 4,246,113 puts/s against the same build's 3,660,024 and 4,186,720 quiet.

## 2. The kept index split in two beside held memory

With the advice gone the held gets were still 0.80-0.83x of quiet (D2h, hz build: 2,601,741 and
2,673,094 gets/s against 3,114,038 and 3,353,429). The diagnostic counters gave the cause: held,
the kept memtables' index was two generations (`kept_generations 1`) and 1,712,172 and 1,735,104
probes answered 1,000,000 gets, 71% and 74% of gets probing both; quiet it was one generation and every get
probed one (999,999 and 1,000,000 probes). The open generation closed once its index held a third of
what the kept memtables may hold, and that limit follows the machine's free memory down, so 40 GiB
held elsewhere closed it with every record still kept and nothing given back. A generation now
closes only where the kept memtables cannot take the open one's next growth, or must give memory
back (`g17-fin-hzg` `5398fb2a`).

## 3. Where a merge's inputs lie when it starts

With every file read directly, a worker's compaction reads its inputs from the device; through the
kernel's page cache the current build read most of them from memory. The diagnostic build counted,
as each job went out, its input leaf pages in the merged range, those the shard's page cache held,
and the cache chunks holding them with each chunk's live pages (D3, one case each):

| case | jobs | input pages | cached at dispatch | chunks (summed over jobs) | their live pages | beyond the inputs |
|---|---|---|---|---|---|---|
| 100M, equal cap | 1,099 | 10,073,585 | 8,686,216 (86.2%) | 21,321 | 10,618,276 | 22.2% |
| 100M, uncapped | 1,234 | 10,072,915 | 8,883,205 (88.2%) | 21,992 | 10,869,615 | 22.4% |
| 10M, uncapped | 23 | 664,336 | 369,527 (55.6%) | 741 | 378,303 | 2.4% |

The cached inputs lie in chunks a worker's loan brought back, about 500 live pages each of a loan
chunk's 512 (one 2 MiB huge page of 4 KiB pages, `loan_chunk_pages`), so what a job reads is
already in mantle's memory when it starts, in 18 to 19 chunks a job at 100M. Lending those chunks whole to the job moves their memory, as an
output loan's does, with no copy and nothing shared between threads; their other pages, 22% beyond
the inputs at 100M, cannot be read in the shard while the chunk is out and are read from it again
once it is back (`Cache::lend_reads`).

## 4. The oracle's check inside the timed loops

The ranges bench checked each get's and seek's answer against its oracle inside the timed loop, so
the check's time counted against mantle alone (RocksDB's and SplinterDB's harnesses check
nothing). Timed apart (`g17-cur-bench` `dc2585fa`, on fin's line `76606287`), it was 0.133 s of a
100M case's 1.286 s gets phase and 0.169 s of its 1.618 s seeks phase (D3, capped), about a tenth.
Batch 5 measured what taking it out of the rate changes, the current build with the fix over without,
interleaved, eight rounds each at 1M: gets 1.04x [0.97-1.10] capped and 1.02x [0.97-1.07] uncapped,
seeks 1.07x [0.95-1.14] and 1.07x [0.57-1.10], fills unchanged (they check nothing).

## 5. 1M gets against SplinterDB: no regression

1M gets measured 2.75x SplinterDB's on 2026-10-19 (4.78M against 1.74M a second) and 2.55-2.65x on
fin's line. Five points of the current build's history (10-20 `ad440fd3`, 10-22 `3cc71771`, 10-23
`d9d74adf` and `300bef68`, 11-13 `ceb241c6`) beside the current build and its A/A, interleaved, eight
rounds each, capped and uncapped (B4): the current build's gets are 1.02x to 1.09x every earlier
point's, its A/A 1.02x [0.96-1.08] and 0.99x [0.93-1.04]; 10-20's own build measures 2.43x [2.26-2.76]
and 2.46x [2.15-2.92] SplinterDB's in the same rounds. Mantle's 1M gets are 4.71M and 4.89M a
second, at or above 10-19's 4.78M; SplinterDB's are 1.78M and 1.87M, above its 1.74M then. The
ratio fell because SplinterDB's rate in this VM rose, not because mantle's fell.
