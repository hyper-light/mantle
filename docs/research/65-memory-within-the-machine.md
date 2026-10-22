# 65. Memory within the machine

A range sized to the machine (`ShardDb::follow_machine`) takes one of `shares` of the machine's
effective capacity, its physical memory clamped to any OS, job or cgroup bound (hyper-rt
`effective_capacity`), as the budget research/36's tuner divides among the page cache, queued
writes and the record cache, with the kept memtables holding what the regions leave of it
(`kept_limit`). Free memory as the OS reports it (hyper-rt `Facts::memory_available_now`,
`MemAvailable` on Linux) gates each growth: the cache takes a chunk while the machine has twice
the chunk free, and a rotation keeps its memtables while it has twice a memtable's arena.

Machine-sized, the engine was OOM-killed at a billion rows on the Docker VM lane at 64 GiB
(r7061-r7062, about 52 GB written) and at 96 GiB (r7081-r7082, 37 GB written, after 1:53). This
note traces why and measures what the machine needs left to it.

## 1. Killed at its own budget

A diagnostic build (branch `diag-oom`, never merged) prints the shard's account at each rotation:
its budget, what its regions and kept memtables hold, the memtables in flight, the branches'
metadata as held (`ShardDb::memory`), the page cache's parts, the free memory the gates read and
the process's resident memory. Beside it, the VM's `/proc/meminfo` every second
(`memsample2.sh`). One machine-sized 1B fill, m_ flags, the VM at 96 GiB (`MemTotal` 94.1 GiB),
`diag-runs/n1000000000-d2-m_oomdiag`, GiB:

| rotation | s | page cache | kept memtables (count) | memtables | metadata | free read | resident |
|---|---|---|---|---|---|---|---|
| 1 | 0 | 0.0 | 0.0 (0) | 0.16 | 0.00 | 89.4 | 0.3 |
| 101 | 17 | 10.9 | 10.0 (100) | 0.20 | 0.26 | 69.4 | 21.3 |
| 201 | 35 | 19.3 | 19.9 (200) | 0.22 | 0.51 | 50.7 | 39.5 |
| 301 | 55 | 26.2 | 32.0 (300) | 0.22 | 0.71 | 32.2 | 58.3 |
| 401 | 77 | 33.0 | 39.9 (400) | 0.25 | 0.89 | 18.2 | 72.5 |
| 451 | 85 | 36.7 | 43.8 (450) | 0.25 | 1.01 | 11.3 | 80.0 |
| 492 | 95 | 39.4 | 50.1 (474) | 0.50 | 1.09 | 0.0 | 91.1 |

The budget was 94.1 GiB at every rotation: the machine's whole memory. The page cache held the
pages the workers wrote (its loans: 32.3 GiB live, 6.6 GiB of dead slots at the end) and the kept
memtables every memtable the fill rotated, each region growing about half a GiB a second; their
sum, with the memtables in flight and the metadata, was the process's resident memory within a
GiB at every rotation. Nothing outside the shard's account grew. The VM beside it (GiB):

| s | free read (`MemAvailable`) | OS page cache (`Cached`) | dirty | anonymous | swap free |
|---|---|---|---|---|---|
| 30 | 76.8 | 5.8 | 0.00 | 14.6 | 3.9 |
| 46 | 56.4 | 11.7 | 5.14 | 34.8 | 3.9 |
| 61 | 42.7 | 16.9 | 3.68 | 48.9 | 3.9 |
| 76 | 30.9 | 21.6 | 1.94 | 60.7 | 3.9 |
| 91 | 21.0 | 20.5 | 1.75 | 70.7 | 3.9 |
| 106 | 9.5 | 9.3 | 0.05 | 83.0 | 3.9 |
| 110 | 0.4 | 0.6 | 0.05 | 92.4 | 3.9 |
| 126 | 4.3 | 0.3 | 0.00 | 88.0 | 0.0 |

The free memory the gates read counts the OS's page cache, so it stayed above two memtables until
the kernel had given the engine its page cache too: the reading fell to nothing only once the OS
page cache was 0.3 GiB and swap was full. The kernel's OOM killer then killed the process at 91.6
GiB anonymous (`anon-rss:96026548kB`, `pgtables:202004kB`), first killing coredns of the kind
cluster in the same VM.

So the budget, the whole machine, left nothing to the kernel, to its page cache or to the
machine's other processes, and the gates gave way only when there was nothing left to give.

## 2. A budget that follows the machine

The budget was fixed at the shard's share of the capacity: the machine's free memory only stopped
growth, and only once nothing was left. The same defect was measured, and a design for it built
and measured, on the unlanded branch `memlim` (2026-10-19, its research note 64, "Memory within
a limit"): under a container's 24 GiB limit and a 64 GB systemd scope, main of that day was
killed in every run, and the budget that follows the machine survived eight runs of eight at
100M in 24 GiB (fill 5.08M-5.32M a second, gets 619K-799K) and completed 1B in the 64 GB scope.
Its mechanism is taken here, re-measured on this engine:

- **When.** A shard looks at the machine at each rotation, at the end of each get cycle
  (research/36's cycle, a memtable's worth of operations), in a range task's turns once a fill's
  time (`fill_ns`) has passed since the last look, and in idle time whenever a worker's message
  has come back since the last look: the flush after a fill has no rotations, and its
  consolidations grow on every worker.
- **What.** The budget becomes what its regions and kept memtables hold plus its share of the
  machine's free memory, less what it leaves free, never above its bound and never below what
  it cannot give back (its write memory and the caches' indexes and ghosts, `floor_held`).
- **What it leaves free.** The most the process's memory outside the shard's account grew
  between two looks (`grew_most`): what may grow before the next look with no gate to ask. The
  gates keep their own steps' room as they admit them; kept again by the budget, the cache's
  next chunk (0.5-0.9 GiB) swung the budget by itself from look to look and left 4.7 GiB of the
  24 GiB unused, so that the first gets after the fill read the device 1.61M times (memlim
  §7). And the machine's reserve (§3), which is the kernel's.
- **Giving back.** A budget that falls has the kept memtables give memory back at once
  (`fit_kept`) and the page cache and record cache trimmed to their shares at once, rather than a
  few pages an operation; a trim's ghost entries are charged to its cache, so after a look's
  trims the budget is raised to what the shard then cannot give back.

`tests/shard_db_follow.rs` drives the machine's reading through memory to spare, none, and
memory again, every read checked against an oracle: with none free the next look lowers the
budget below its bound, it never rises while nothing is free, the regions and the kept memtables
hold no more than it, the kept memtables give memory back, idle maintenance alone lowers it with
no rotation, and when memory returns so does the budget. A second test holds the machine's free
memory at the reserve: the budget falls as with none free (a build that leaves no reserve fails
it: "the budget rose ... with only the reserve free").

## 3. What the machine's free memory counts, and what the kernel keeps

Linux v6.12 sources (torvalds/linux, tag v6.12), by file and function; the VM runs 6.12.76.

**`MemAvailable` counts dirty pages as free.** proc(5)'s `MemAvailable`: "An estimate of how much
memory is available for starting new applications, without swapping. Calculated from MemFree,
SReclaimable, the size of the file LRU lists, and the low watermarks in each zone"
(Documentation/filesystems/proc.rst). The estimate (mm/show_mem.c, `si_mem_available`) is free
pages less `totalreserve_pages`, plus the whole file LRU (`NR_ACTIVE_FILE + NR_INACTIVE_FILE`)
less `min(pagecache / 2, wmark_low)`, plus half the reclaimable slab at most. Every page-cache folio
is on that LRU (mm/filemap.c `filemap_add_folio` → `folio_add_lru`), dirty and under writeback
alike, and nothing subtracts `NR_FILE_DIRTY` or `NR_WRITEBACK`; reclaim skips those folios until
they are written (mm/vmscan.c `shrink_folio_list`: "waiting for disk writes is more expensive than
potentially causing reloads down the line"). So the reading fell to nothing only once the page
cache was nearly gone (§1): it estimates what reclaim could take, not a margin. The kernel's own
domain for dirty memory inside a cgroup counts only clean file pages as room (mm/page-writeback.c
`mdtc_calc_avail`: "clean = filepages - min(filepages, mdtc->dirty)"), the rule the follow's
reading takes on Linux: `MemAvailable` less `Dirty` and `Writeback`.

**The kernel keeps little for itself.** `min_free_kbytes = sqrt(lowmem_kbytes * 16)`, clamped to
128 KiB-256 MiB (mm/page_alloc.c `calculate_min_free_kbytes`), the watermarks' gaps
`max(min / 4, watermark_scale_factor / 10000 of the zone)` with a default factor of 10, 0.1 %
(`__setup_per_zone_wmarks`), and `totalreserve_pages` the high watermarks and each zone's
largest lowmem reserve (`calculate_totalreserve_pages`): under 1 % of memory. Raising it is no way
to keep a share: "Setting this too high will OOM your machine instantly"
(Documentation/admin-guide/sysctl/vm.rst, `min_free_kbytes`).

**The engine's own memory lowers the kernel's dirty limits.** Dirtyable memory is free pages less
the reserve plus the file LRU (`global_dirtyable_memory`); anonymous memory is not in it, and the
background and foreground thresholds are 10 % and 20 % of it (`dirty_background_ratio`,
`vm_dirty_ratio`; `domain_dirty_limits`), so the more the engine holds the sooner its own buffered
writes are written back and, past the midpoint of the two, throttled (`balance_dirty_pages`).

**The OOM killer.** With swap full, only file pages count as reclaimable
(mm/vmscan.c `zone_reclaimable_pages`, `can_reclaim_anon_pages`); the allocator gives up after
`MAX_RECLAIM_RETRIES` (16) rounds in a row without progress, or when no zone could reach its
watermark even with every reclaimable page freed (mm/page_alloc.c `should_reclaim_retry`), and the
victim is the task with the most rss, swap and page tables (mm/oom_kill.c `oom_badness`).

**What engines on the page cache leave to it.** RocksDB, whose reads and compactions go through the
page cache: the block cache "should be about 1/3 of your total memory budget. The remaining free
memory can be left for the OS (Operating System) page cache" (RocksDB wiki, "Setup Options and
Basic Tuning"); its memtables, index and filter blocks lie outside the block cache unless charged
to it ("Memory usage in RocksDB", "Write Buffer Manager"). PostgreSQL: shared_buffers "25% of the
memory in your system", rarely past 40 %, "because PostgreSQL also relies on the operating system
cache" (PostgreSQL 18 §19.4.1). InnoDB's 75-80 % (MySQL 8.4 §17.8.12, `innodb_buffer_pool_size`)
comes with `O_DIRECT` as the Unix default (`innodb_flush_method`), its data files not also in the
page cache, so it does not carry over. Db2's self-tuning memory manager, the nearest precedent for
a budget that follows free memory, keeps 5.5-7.8 % of memory free, 5 % of it an extra buffer
(`DB2_MEM_TUNING_RANGE`'s minfree), re-examined each tuning interval of 30 s to 10 min, a heap
growing at most 50 % and shrinking at most 20 % an interval (Storm et al., "Adaptive
Self-Tuning Memory in DB2", VLDB 2006, §3, §3.3.1, §3.5). Meta sizes a workload's memory by its
pressure: Senpai reclaims `current_mem × 0.0005 × max(0, 1 − PSI_some / 0.1%)` every six seconds,
at most 1 % a period, and growth back is immediate (Weiner et al., "TMO: Transparent Memory
Offloading in Datacenters", ASPLOS 2022, §3.3).

No peer-reviewed work sizes an LSM or Bε store's memory against the OS page cache under buffered
I/O. LSbM (Teng et al., ICDCS 2017) shows compaction reads through the OS cache evicting the data
queries read; Wong and Wilkes ("My cache or yours?", USENIX ATC 2002) that two inclusive cache
levels act only as large as the larger; Luo and Carey (PVLDB 14(3), 2020) tune one budget between
memtables and a buffer cache without the OS page cache. The vendors' shares are set for their own
two tiers (RocksDB's page cache holds compressed blocks its block cache holds uncompressed); this
engine keeps the pages its workers write in its own cache, and the share it leaves the kernel is
measured on the host (§4).

## 4. Followed, and moved to swap

E1 (branch `diag-follow`, never merged: the follow of §2 with no reserve, its reading
`MemAvailable` less `Dirty` and `Writeback`, the account line of §1) ran machine-sized at 1B on the
box (Fedora, Linux 7.2.9, 124 GiB, pinned to eight CPUs, other sessions' builds beside it),
`~/Projects/bench/diag-runs/e1-20261022T220849`, sampled every second (s; stall in s, the rest
GiB; swap is the box's zram, compressed in memory, `vm.swappiness` 150):

| s | memory stall ("some") | reading | free | page cache | anonymous | swap used | file refaults |
|---|---|---|---|---|---|---|---|
| 0 | 0.00 | 87.8 | 12.7 | 74.9 | 18.8 | 0.0 | 0 |
| 60 | 0.15 | 81.2 | 15.4 | 65.5 | 28.2 | 0.0 | 144,625 |
| 100 | 1.35 | 62.2 | 35.4 | 28.1 | 44.8 | 8.0 | 1,477,345 |
| 140 | 2.62 | 45.3 | 18.1 | 27.2 | 62.1 | 16.3 | 2,957,474 |
| 180 | 4.01 | 40.6 | 0.6 | 39.7 | 65.1 | 29.8 | 4,180,491 |
| 241 | 7.57 | 31.2 | 0.6 | 30.3 | 71.8 | 52.6 | 5,652,252 |

The budget rose with the reading, from 89.6 GiB to the machine's whole 124.8 GiB by 207 s, as the
shard's regions grew (page cache 46.6 GiB and kept memtables 62.6 GiB in its account at the last
rotation) and the kernel moved anonymous memory to swap: the process's own `VmSwap` was 41.5 GiB
beside 59 GiB resident when it was stopped at 4:06, at 2.5 minutes of fill. Every page the kernel
moved out of the engine reads as available, so the reading never fell: the budget grew by what the
kernel took from it. The memory stall grew to 3 % of the time and file pages were read back after
eviction 5.8 million times (22 GiB). The run was stopped rather than let the box's other sessions
be pushed into swap with it.

So a budget that follows the reading must count the process's own pages in swap as taken from it
(`port::memory::room_now`: the estimate less dirty and writeback pages and less `VmSwap`), and the
reserve it leaves is measured where the engine runs alone, the VM lane: E1b.

## 5. The page cache under a budget that falls

E1b (branch `diag-follow` 909c5075, never merged: the follow of §2, its reading
`port::memory::room_now`, no reserve, the account line of §1) completed the machine-sized 1B run on
the VM at 96 GiB, the first to (`diag-runs/n1000000000-e1b-1-m_e1b`, recovery exact over
632,113,920 live keys): fill 2,494,509 a second, gets 13,973, seeks 6,923. The engine with a
fixed 32 GiB cache and no budget, which leaves the OS about 50 GiB, ran 3.16M-3.98M, 20.2K-24.5K
and 10.8K-13.8K the same day. More memory for the engine's cache lost on every axis, and its own
account says why (pages of 4 KiB):

| E1b | cache bytes | live | dead | spare (loans' chunks holding no page) | cache hits in the gets | device reads in the gets |
|---|---|---|---|---|---|---|
| after the fill | 80.9 GiB | 2,081,005 | 2,888,559 | 16,230,751 | | |
| after the gets | 80.9 GiB | 0 | 0 | 21,200,315 | 0 | 630,916 of 632,595 found |

The cache held its whole limit, 80.9 GiB, and almost none of it pages: three quarters spare loan
memory after the fill, all of it after the gets, and every found key read from the device. These
are the page cache's defects that a falling limit exposes, each found, measured and fixed on the
unlanded branches `memlim`, `retire` and `hollow` (their research/64, §8, §11, §12 and §13), taken
here:

- **Spare loan memory had no bound** (memlim §8; 3f85185b). A loan's chunk whose pages are all
  forgotten is kept for the next loan and counts against the limit, never a read slot: 10 GiB at
  100M with no limit beside 0.9 GiB the loans ever had out at once, all but 50K pages of a falling
  limit in a 24 GiB container. `Cache::shed_spare` gives back spare memory past the most the loans
  have had out at once.
- **A retirement outlived its limit** (retire §11; 0fdf9c1f, 5282fe75). While a chunk retires the
  cache lends nothing, gives up each loan that comes back and makes no read slot; a retirement
  begun under a lowered limit ended only at a trim or an idle reservation, so with a limit that fell
  and rose on a busy shard the cache stopped taking pages for the rest of a fill and the gets drained
  it to none (box 1B r761, 100M in 8 GB r771). `resize`, `trim` after its spare and `shed_spare`
  now end it at once.
- **A loan came back as one chunk** (hollow §12; 1bf4b0ca). A chunk is free only with its last
  page, and a job's pages leave an extent at a time, so a whole job's chunk sat a third dead at 1B.
  Fresh loan memory comes in buffers of whole extents, a huge page at least on Linux.
- **Hollow chunks' slots froze the reads** (hollow §13; f0d2976f). A chunk given back kept its slots,
  and the reads' and idle time's slot making counted the table against the limit: 7.6M live pages
  froze from the fill's end, 17.5 GiB under the limit, and every loan's fresh pages added slots
  without bound. The limit now holds the chunks' room, and fresh loan memory refills hollow chunks
  before any new one, the table within twice the largest limit.

Each comes with its tests (`spare_loan_memory_holds_no_more_than_the_loans_had_out_at_once`,
`a_retirement_ends_once_a_raised_limit_holds_the_room`,
`a_retirement_ends_once_spare_memory_given_back_brings_the_room_within_the_limit`,
`reads_take_the_room_hollowed_loan_chunks_gave_back`,
`loans_that_come_and_go_reuse_the_slots_of_memory_given_back`, and the 40,000-step
`the_fields_the_slot_table_and_the_hollow_list_hold_through_every_step`); the hollow one fails on a
cache whose reads count the slot table again. The kernel's share is measured on top of them (E1c).
