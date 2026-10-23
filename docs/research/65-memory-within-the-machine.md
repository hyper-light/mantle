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
reading took on Linux at first: `MemAvailable` less `Dirty` and `Writeback`. It now holds back the
background threshold those pages build to before the flusher writes them out, or the pages when
they run past it, since the count at each moment follows the flusher's cycle (§9).

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

## 6. Memory the allocator keeps

E1c (`diag-follow` on the follow with §5's fixes, never merged; `diag-runs/n1000000000-e1c-1-m_e1c`)
held the page cache's pages through the fill: at its end 73.2 GiB live, 1.6 dead, none spare, where
E1b's had 7.9 GiB live and 62 spare. But at rotation 1,434 the budget fell from 82.8 GiB to 49.3 in
five seconds, and its own account says where the memory went (GiB):

| rotation | s | budget | cache | room read | resident | outside the account |
|---|---|---|---|---|---|---|
| 1428 | 312 | 82.8 | 77.7 | 4.0 | 83.2 | 0.62 |
| 1434 | 313 | 66.0 | 61.7 | 3.6 | 82.9 | 16.62 |
| 1446 | 316 | 53.9 | 49.1 | 3.7 | 83.4 | 28.98 |
| 1452 | 318 | 49.3 | 43.8 | 4.5 | 83.1 | 34.37 |

The cache gave up 34 GiB of its chunks and the process's resident memory did not move: the memory
went back to glibc, not to the OS. glibc's malloc maps each block of at least its mmap threshold
and unmaps it when freed, but each mapped block up to 32 MiB that is freed raises the threshold to
its size, so later blocks of that size come from its heap and stay in it once freed (mallopt(3),
`M_MMAP_THRESHOLD`); the cache's loan buffers, whole huge pages since §5, are such blocks. The room
the follow read stayed at 4 GiB, so each look lowered the budget to what the shard then held, and
the cache shrank to give memory that never left the process. The unlanded branch memlim measured
the same at 100M, 1.3-2.8 GiB of freed record lists held in glibc's arenas (its research/64 §6),
and kept the threshold at its initial value, which mallopt(3) says turns the adjustment off
(`port::mmap::return_freed_blocks`), called at the start of the node's process and the bench.
## 7. What the kernel's page cache does for the fill

E1d (`diag-follow` d4f35279: the follow with §5's and §6's fixes, its room read by
`port::memory::room_now`, nothing left to the kernel by rule; `diag-runs/n1000000000-e1d-1-m_e1d`)
completed the machine-sized 1B run on the VM at 96 GiB with recovery exact over 632,113,920 live
keys. Its budget held 83 to 92 GiB from the first minute to the fill's end, and the page cache
82.1 GiB, 79.7 of it live pages: the memory the allocator kept now came back (§6). Its gets and
seeks ran at twice and three times the fixed-cache arm's, its fill at two thirds:

| 1B, VM 96 GiB | fill | gets | seeks |
|---|---|---|---|
| E1d, machine-sized | 2,421,422 | 48,404 | 32,028 |
| fixed 32 GiB cache (r7111, r7151, r7191, r7192) | 3,162,508-3,976,777 | 20,238-24,096 | 10,767-13,420 |
| E1f, fixed 32 GiB cache, E1d's binary | 3,796,764 | 24,911 | 15,396 |
| E1h, fixed 4.5 GiB cache, E1d's binary | 3,770,619 | 33,704 | 19,452 |

E1f ran E1d's binary with the fixed arm's flags (`inside-fixdiag.sh`: a 32 GiB cache, no budget),
so the two accounts and the process's I/O counters (`/proc/self/io`) stand side by side. Over the
fill (GB, decimal):

| 1B fill | seconds | read through the page cache (`rchar`) | of it from the device (`read_bytes`) | written (`wchar`) | pages turned dirty (`write_bytes`) | VM page cache from 100 s, mean (GiB) | dirty, mean (GiB) |
|---|---|---|---|---|---|---|---|
| E1d | 413 | 464.9 | 142.6 (30.7%) | 575.2 | 480.2 | 8.3 | 0.47 |
| E1f | 263 | 465.0 | 12.5 (2.7%) | 576.0 | 307.0 | 50.9 | 4.23 |

The engine asked the same of the kernel in both, to within 0.2% each way. What the kernel did
with it differed by the page cache it had. `write_bytes` counts a page as it turns dirty
(`folio_account_dirtied`, mm/page-writeback.c); a write to a page already dirty is not counted and
is not written again. So with 51 GiB of page cache 269 GB of the 576 written landed on pages still
dirty and never reached the device as writes of their own, and the workers read 97.3% of their
inputs from memory; with 8 GiB, 95 GB were absorbed and 30.7% of the reads went to the device. The
device moved 623 GB in E1d against 320 GB in E1f.

E1d's own account says when (per 150 rotations, `compare-fill.sh`): while the VM had room the
workers read nothing from the device and 150 rotations took 23 to 29 s; once the page cache fell
to 6-12 GiB at 100 s the device's share of their reads rose to 20-56% and 150 rotations took 40 to
54 s, recovering to 29-37 s only as the kept memtables gave their 44 GiB back. In E1f the share
stayed under 1% until the store outgrew the page cache at 200 s (5-12% after), and 150 rotations
took 17 to 28 s throughout.

So the kernel's page cache does two things for the fill that the engine's own cache cannot, both
of them reuse of what the engine has just written. The workers read their inputs through it
(their files are buffered; the page cache's loans of a job's written pages serve the shard's reads,
not the workers'), as LSbM found of compactions (Teng et al., ICDCS 2017, §2). And it absorbs the
writes over short-lived extents: an extent a compaction frees is reused at once (`Allocator`'s
free list is a stack), and its pages, if still dirty, are written once. How long a page stays
dirty is the kernel's rule: background writeback starts at `dirty_background_ratio` (10%) of the
dirtyable memory, the free pages and the file LRU (vm.rst; `global_dirtyable_memory`), and
anonymous memory the engine holds is no part of that, so a cache grown into the page cache
shrinks the window in which a rewrite is absorbed along with the page cache itself. The two caches
also hold the same pages (every written page enters both), so together they act only as large as
the larger (Wong and Wilkes, USENIX ATC 2002): E1d's 82 GiB served its gets and seeks, and the
page cache's 8 GiB beside it served the workers only what the engine's cache already held.

How far back the workers reach is the engine's own fact. A diagnostic build (`diag-reuse`
e7727605, never merged) stamps each branch with the trunk's count of extents taken when the trunk
takes it, and counts each compaction's and settle's input pages by the extents taken since their
branch was (`reusediag`); E1e ran it with the fixed arm's flags (`diag-runs/n1000000000-e1e-f-f_e1e`).
By the fill's rotation 1,600, 383 GiB of inputs, by distance in bytes written since:

| written since (GiB) | < 0.5 | < 1 | < 2 | < 4 | < 8 | < 16 | < 32 | < 64 | < 128 | < 256 |
|---|---|---|---|---|---|---|---|---|---|---|
| share of input bytes | 29.2% | 37.8% | 45.7% | 55.0% | 65.3% | 72.5% | 82.6% | 88.2% | 93.6% | 99.8% |

Read as a least-recently-used cache of the bytes written, which serves a read at a reuse distance
short of its size (Mattson, Gecsei, Slutz and Traiger, *Evaluation techniques for storage
hierarchies*, IBM Systems Journal 9(2), 1970, the stack property), it predicts 65.7%
of the inputs from memory at E1d's 8.3 GiB, where 69.3% were, and 86.4% at E1f's 50.9 GiB, where
97.3% were: the larger the page cache, the more of the writes land on extents it already holds
(an extent a compaction freed is reused at once, its pages read as inputs just before), so it
reaches further back than the bytes written. Each doubling from 1 GiB to 32 GiB serves another 8
to 10% of the inputs, so a byte of page cache serves the most at the smallest sizes and its worth
falls about as one over its size, which is what the tuner's steps between regions assume
(research/36 §1).

The two caches hold the same pages, so the larger alone counts. E1h ran E1d's binary with a fixed
cache of 4.5 GiB, one tuning step of a 90 GiB budget, everything else left to the kernel
(`diag-runs/n1000000000-e1h-1-f_e1d`, recovery exact): its fill matched E1f's (265 s, 1.4 GB of
the 465 GB its workers read from the device, 276 GB dirtied, the VM's page cache 80 GiB), and its
gets and seeks ran 1.35 and 1.26 times E1f's: with the page cache as the cache of record, a get the
engine's cache misses is a copy from the kernel's memory more often than a device read. E1d's 82
GiB of its own cache served its gets and seeks faster still, the engine's cache costing no system
call, but starved its fill.

A budget that follows the machine therefore leaves the kernel the page cache the engine's own
files reuse, by what it saves against what the engine's own cache would: §8.

## 8. The kernel's share, tuned

The tuner (research/36) divides a budget among regions by what each saved a byte in its last
cycle. Luo and Carey's read cost counts the merge reads a larger buffer cache would have saved
beside the queries' (`saved_m`, research/36 §1); mantle's merges run on workers whose stores have no
cache (`Store::worker`) and read through the kernel's page cache, so that term is the gain of the
memory the shard leaves the kernel, and the engine's own cache's ghost keeps the queries'. While a
budget follows the machine, the kernel's page cache is a fourth region (`ShardDb::kernel_share`):
the budget's share no region takes.

- **The ghost.** The trunk counts the extents of every branch it takes and stamps the branch with
  the count (`Branch::taken`): packed memtables, compactions' and settles' results. A job a worker
  is handed counts each input's pages it reads by the extents taken since its branch was (§7's
  measure, `Trunk::take_reuse`): those at or past the share, in extents, which a page cache of the
  share would not hold (`past`), and of them those short of the share and a tuning step, which one
  a step larger would (`within`). A branch loaded at open has no stamp and is past every window.
- **The cost.** A worker times the waits of its reads the OS did not hold
  (`IoStats::read_wait_ns`) and reports them with each job (`Output::read_wait_ns`). Before it
  hands an extent's read to the issuer it asks whether the OS holds the extent's first page
  (`read_resident_at`: `preadv2(2)` with `RWF_NOWAIT` on Linux, `mincore(2)` on macOS), and only a
  read it did not hold is timed from its claim until its answer; a read made in the file at once
  is first tried from memory. A read the OS held is a copy from memory, which no larger page cache
  would spare, so a fill whose inputs all stay cached (100M rows in 96 GiB) earns the kernel
  nothing, though its workers still wait on the issuer's answers. A file that cannot tell, direct
  I/O or a platform without either call, counts no wait, and the share stays none. A job taken back
  adds its waits as many times over as its `within` is of its `past`, both counted when it was
  planned (`Trunk::note_reuse`), and a cycle's gain is their sum, against a step's bytes: of what
  the reads the share missed cost, what a step would have saved. Counted at a cycle's end instead,
  the reads of the jobs planned in it against the waits of those taken back, most cycles held one
  without the other, a job outlasting several, and the share stood still while the workers missed
  (§9). The price is measured,
  not modeled: a share past what the kernel's page cache reaches finds no waits to save (§7: it
  reaches further than the bytes written).
- **A step.** In a fill with no gets the page cache saves nothing and the workers wait on what the
  share misses: the page cache gives the share the paper's step a cycle, 5% of the budget within
  10% of its own, keeping one step itself, as the record cache does, so its ghost gathers the
  evidence to grow back. That is E1h's shape at a billion rows. Gets that the page cache's ghost
  names outweigh the workers' waits and take memory back the same way.
- **What it gives way to.** The share is the budget's last claim: no region grants into it, the
  kept memtables leave it, and it gives way to what the caches hold that no trim returns, their
  indexes and ghosts, which a trim's ghost grows (`grant`; the test
  `the_kernels_share_gives_way_to_what_the_regions_cannot_give_back`). A budget given
  (`set_memory`) stands as given and leaves the kernel nothing of it.
- **Open.** The gain prices the reads alone. The writes the page cache absorbs (§7: 269 of 576 GB
  at 51 GiB) come with the memory the reads earn, unpriced; a rewrite's ghost would count each
  extent reused by its age against the dirty window, `dirty_background_ratio` of the dirtyable
  memory.
## 9. The share measured at a billion rows

Each case below is one machine-sized 1B run on the VM at 96 GiB through `inside-oomdiag.sh`, a
diagnostic build with the account line at each rotation (`diag-kernel`, never merged), recovery
exact over 632,113,920 live keys in each. Single runs on a host whose other sessions' load varies
(the Mac's load average was 13-20 through E1j): they place the effects, the interleaved release
gate weighs them.

| 1B, VM 96 GiB | build | fill | gets | seeks | fill s |
|---|---|---|---|---|---|
| E1d | the follow, nothing left to the kernel | 2,421,422 | 48,404 | 32,028 | 413 |
| E1g | the share, priced cycle by cycle (ef42fd49, 6ec7ac90) | 2,865,640 | 23,880 | 12,433 | 349 |
| E1i | E1g, no memtable kept (`MANTLE_DIAG_KEPT0`) | 3,166,911 | 25,021 | 14,338 | 316 |
| E1j | the share priced job by job (123df270), the cap (5c5f8085) | 2,617,850 | 29,203 | 15,252 | 382 |
| E1l | E1j with the cache's churn counted | 2,603,854 | 28,801 | 13,930 | 384 |
| E1m | the room holding back the background threshold (bb3d5f7f) | 2,539,431 | 26,564 | 15,458 | 394 |
| E1n | the budget moving by a tuning step (fa294e80) | (1,732 of 1,773 rotations in 347 s) | | | |
| E1f | fixed 32 GiB cache | 3,796,764 | 24,911 | 15,396 | 263 |
| E1h | fixed 4.5 GiB cache | 3,770,619 | 33,704 | 19,452 | 265 |
| E1k | fixed 4.5 GiB cache, E1j's binary | 3,621,038 | 36,823 | 19,183 | 276 |

- **The share.** In E1g the share reached 51-56 GiB and the VM's page cache 58-61 GiB: device reads
  5.5% of the workers' (E1d 30.7%), 333 GB dirtied (E1d 480). But it grew in fits, standing still
  for 75 rotations while the workers missed: a job's reads were counted when it was planned and its
  waits when it came back, often cycles later, so most cycles held one without the other. Priced
  job by job (E1j), the share began at rotation 400 and held 32 GiB by 550, where E1g's held 18.6 at
  600.
- **Kept memtables.** Before any read missed, the kept memtables took the budget's idle part, 41
  GiB by 84 s in E1g, each a fresh arena faulted in and indexed on the workers, and gave it back as
  the share grew. Without them (E1i) the first 600 rotations took 96 s, E1g's 112, the fixed arms'
  82-88, and the fill as a whole 316 s against 349.
- **Faults.** The account counts the process's minor faults (`/proc/self/stat`): 22-37K a
  rotation through E1i's fill, 50 million in all, 69.5 million in E1j's, against 4-6K a rotation
  with E1j's binary on a fixed 4.5 GiB cache (E1k). The fixed cache's limit never moves; the
  follow's moved with every look. E1j's budget changed by 0.29 GiB a rotation on average after its
  share settled, a sawtooth: the room jumped 2 GiB every 18 rotations or so and fell 0.1-0.2 GiB a
  rotation between, and the page cache's share and the cache with it, which gave chunks back on
  every fall and took fresh memory for its loans after every rise. The room read the dirty and
  writeback pages at each look, and they follow the kernel's writeback cycle: dirty pages build
  toward the background threshold, the flusher cleans a few GiB at once, and they build again.
  That cycle is the kernel's, not a change in what the machine can give, so the room now holds back
  the background threshold itself (`nr_dirty_background_threshold`, /proc/vmstat), or the dirty
  pages when they run past it (`port::memory`; mm/page-writeback.c `balance_dirty_pages`).
- **Churn counted.** E1l counted the page cache's fresh loan memory and the spare memory it gave
  back on the account line: through E1j's build's fill the cache took 159 GiB fresh, though it never
  held more than 44 GiB, and gave back 60 GiB as spare and the rest by retiring chunks, their pages
  dropped. E1m, the room holding the threshold back, took the sawtooth away (the budget's change a
  rotation 0.12 GiB, from 0.29) but not the churn: 10-11 GiB fresh every 150 rotations with the
  share settled, about 30 thousand faults a rotation. Its budget still moved with every look, by
  what lies between the shard's account and the machine's reading taken a moment later: kept
  memtables swapped one for another, loans out, the kernel's reclaim (its change a rotation had a
  standard deviation of 0.21 GiB, correlated with none of them alone). A cache whose limit falls
  retires chunks, dropping their pages, and takes fresh memory for loans when it rises again; a
  fixed limit does neither. So the budget moves only by a tuning step or more, the granularity the
  tuner moves memory by, or at once when its target falls below what the shard holds, the machine
  needing memory back (`follow_step`): E1n. Its budget changed at 2 of 231 looks after rotation
  500, against every look before, and once the share settled the cache took no fresh memory and
  gave none back for hundreds of rotations, 6 thousand faults a rotation (E1k's fixed cache: 4-6
  thousand). The fill as a whole took 29.6 million faults and 43 GiB of fresh loan memory, where
  E1l took 64.5 million and 159 GiB, and reached rotation 1,732 at 347 s, where E1l and E1m took
  384 and 394 s for all 1,773 (Docker Desktop stopped the VM at 02:03:49 UTC, and E1n's gets and
  seeks never ran: kiln's agent, between this session's turns, stopped what its commands had left
  running, Docker Desktop among them, which the scripts had started with `open`; its log at that
  instant, "stopping what agents' commands left running, now that they're between turns"). What it still lacks of
  the fixed arms' 263-276 s is mostly the kept memtables' (E1i).
- **At 64 GiB.** The release build of the same code (engine-follow 1ba145a5, `ranges-follow`, md5
  04986845, no account line), machine-sized at a billion rows with the VM at 64 GiB (`MemTotal`
  62.7 GiB), where data-layer was OOM-killed at about 52 GB written: it completed
  (`diag-runs/n1000000000-e64-1-m_follow`), 3,269,119 puts a second over 306 s, 16,189 gets and
  9,704 seeks a second, recovery exact over 632,113,920 live keys. The VM's anonymous memory
  peaked at 50.0 GiB 66 s in, the kept memtables at their fullest, with 11.4 GiB still available;
  the kernel swapped out 770 pages in all and stalled on memory 12.9 s of the run's 681
  (`/proc/pressure/memory` full). The final build (`ranges-follow` md5 a1780568, the production
  code of 3cc71771, which later commits keep) completed at 64 GiB too
  (`diag-runs/n1000000000-e64-4-m_follow`, 2026-10-23): 3,489,672 puts a second, 18,174 gets and
  10,207 seeks a second, recovery exact over 632,113,920 live keys. Two attempts before it ended
  when kiln's agent stopped Docker Desktop at the end of a turn (14:11:16 and 14:14:07 UTC); the
  batches start Docker as a child of their own shell now, which a background task keeps.
- **Seeks and the cache's memory.** The first release gate (R0 7210-7234, n=4) put the follow's
  seeks at 0.941, 0.960 and 0.929 of data-layer's at 1M, 10M and 100M. Two costs the follow had
  added to the shard's own work went first: looks at the machine paced by the clock, a clock read
  at every turn of the range's task (one a seek with one client), and in idle steps a look after
  any worker's message, each reading three /proc files and walking every branch's metadata; and a
  residency probe and a timed read on every store, where only the workers' waits are priced.
  Looks are paced by work now (`look_when_due`: a cycle's worth of scans and merged entries; an idle
  shard looks as its workers report, `look_when_idle`) and only workers' stores probe (768371bc,
  dae63d2b, 69e010cb): a 1M run looked 6 times, and fills at 10M ran 1.064 of data-layer's (R0
  7260-7264). The seeks did not move (1M 0.904, 10M 0.930). Every build did the same work a seek
  (10M, R0 7291-7296): one source opened, no read, 1.31 span hits on the same 199,522 live cache
  pages. Where those pages lay differed. The VM's anonymous memory peaked 86% on transparent huge
  pages under data-layer and 51% under the follow (`diag-runs` n10000000-thp-1): since 1fc3342a a
  loan's fresh memory comes in buffers of one huge page each, so its pages leave the cache an
  extent at a time (§5), but a buffer of a huge page from the C library's allocator lies on none,
  its block beginning past the allocator's header and its mapping a page longer than a huge page,
  which the VM's 6.12 kernel aligns for huge pages only when its length is a whole number of them
  (mm/mmap.c `__get_unmapped_area`). One buffer a loan, as data-layer lent it, put the memory back
  at 86% and the seeks at data-layer's (1.015, R0 7301-7306); glibc's mmap threshold left to
  adjust itself did not (0.874). The cache's buffers are mapped on huge pages' boundaries now
  (`port::mmap::PageBuf`, 99443a82): 87% on huge pages, and at 10M seeks 1.002, fills 1.004 and
  gets 0.988 of data-layer's (R0 7321-7326). At 1M the seeks stayed 2-4% below (0.959-0.982 in
  three batches; 0.956-0.974 with fixed 32 GiB caches, so not the follow's choices), though layout
  alone moved them 1.003 (an A/A, data-layer with an unused function shifting its code, R0
  7331-7336), and neither the mmap threshold (0.965) nor the scans' look check (1.002 without it)
  accounted for it. perf on the VM put the time in the cursors' loads and copies out of the cache's
  pages: data-layer's cache held them in glibc's blocks, 16 bytes past a boundary, and ours began on
  one. With the buffer's bytes 16 bytes into its mapping (`PAGE_OFFSET`, 573cf167) the seeks came
  back, 64 bytes in they did not: at 1M 0.986 of data-layer's at 16, 0.963 at 0 and 0.961 at 64
  (eight rounds, R0 7381-7388), at 10M 1.000 at 16 and 0.963 at 0 (R0 7391-7396). The page's place
  within a cache line, not its alignment to a page, is what the decode met.
- **The final build at a billion rows.** engine-follow 3cc71771 (`ranges-follow`, md5 a1780568,
  the production code of 072fed1a), interleaved with data-layer bbc73a15 (`ranges-dl`, md5
  7dd33ec0) on the VM at 96 GiB. Machine-sized, against data-layer with a fixed 32 GiB cache as
  rocks32's and sdb32's (R0 7500, n=2), every case completed with recovery exact over 632,113,920
  live keys: the follow's gets 1.24 and seeks 1.26 of the fixed cache's (1.23-1.24, 1.21-1.31),
  its fill 0.85 (0.83-0.87). Its budget gave the page cache 5.4-14.9 GB, the record cache 5.1 GB
  and the kept memtables 5.0 GB where the fixed cache held 34.4 GB, and moved memory between them
  through the fill: 9,819-12,376 loans against 8,098-9,566, and 19.7-22.3 million faults against
  5.1-8.6 million, the kept memtables' cost to the fill (E1i). The same build with the fixed 32 GiB
  cache against data-layer's (R0 7520, n=2): fill 1.056 (1.054-1.058), gets 1.341 (1.306-1.377),
  seeks 1.129 (1.084-1.173), every case exact: the code both run lost nothing, and the fill's cost
  is the follow's own choice of memory. The fixed cache's build still took twice the faults
  (14.7-15.3 million against 6.1-7.6 million) without losing throughput.
- **Windows.** A look's room is never below none, so a budget falls below what the shard holds
  only by what grew outside its account between two looks (`grew_most`), which the process's
  resident memory says (`Facts::resident_now`: `/proc/self/statm` on Linux, the task's resident
  size on macOS). hyper-rt has no such reading on Windows (`resident_bytes` is none there), so a
  shard there leaves nothing free for that growth: with no room its budget falls to what it holds
  and grows no further, and what it gives back is only what the budget sets aside within it (the
  kernel's share, a get's step), never the growth outside its account. The tuning-step
  test had leaned on whatever the process's other allocations grew to take its target below
  what the shard held (1,187,783 bytes on the Mac, none on Windows), and now states that growth
  (072fed1a). The process's working set, which Windows gives (`GetProcessMemoryInfo`), is the
  reading to add to hyper-rt.
- **The final build's seeks at 1M, after landing.** Its release gate left seeks at 0.967 of
  data-layer's at 1M (four rounds; 0.986 over eight) and 0.980 at 10M with no A/A beside them.
  With one (R0 7770 and 7790, twelve rounds, the base shifted by an unused function as its third
  arm): at 1M the final's seeks 0.983 of the base's (0.965-0.993, every round below one) and the
  shifted base's 1.000 (0.981-1.013); at 10M 0.996 and 1.007, within the shift's movement. Two
  bisects over the follow's 35 commits at 1M (R0 7810 and 7830, sixteen rounds each) put the loss
  on one: 1fc3342a, a loan's fresh memory in buffers of at most a huge page, 0.924 where the commit
  before it ran 0.996. The VM's kernel gives huge pages to all anonymous memory (`[always]`, a 2
  MiB page), and a buffer of 2 MiB from glibc's allocator lies on none, so the cache's pages lost
  them; the commits after it won most back (on huge pages, 99443a82; 16 bytes into a line,
  573cf167: 0.980). Arms on the final (R0 7850-7924) measured what remains: its buffers kept off
  huge pages 0.954 at 1M and 0.906 at 10M; a loan in buffers of 32 huge pages 0.990 and 0.993 at
  1M, 1.015 and 1.002 at 10M; glibc's buffers with those 64 MiB loans 0.984 and 0.997; pages 32
  bytes into a line 0.985, 48 bytes 0.949. Three shifts of the final's own layout (R0 7930,
  eighteen rounds) ran 0.965, 0.971 and 0.978 where the final ran 0.975 and the shifted base
  0.996: the loss is not the layout's. The engine counted the same work in both (1,314,285 cache
  hits, a source a scan, no device read). On bare-metal x86_64 (adacachy, interleaved, four pairs,
  `perf record -e cycles:u -k mono` cut to the seek phase by the `profile-phases` stamps) the
  final drew 0.93 of the base's samples: the loss shows on the VM, an arm64 guest on Apple
  silicon, and not on bare-metal x86_64. A loan's buffers of 2, 8 and 32 huge pages (R0 7950 and
  7980) ran 0.985, 0.985 and 0.986 at 1M where the final ran 0.979 (twenty rounds), and 1.026,
  1.002 and 1.009 at 10M where it ran 0.979 (ten rounds, 0.95-1.28 apart): at 1M within a round's
  spread. A cache hit's page touch finds its page's chunk by a search over the chunks' first slots,
  so its overlap with the slot's read needs no slot (`Cache::touch_page`): without the touch the
  seeks ran 0.993 of the final's, with the chunk taken from the slot 0.989 (R0 8000, twenty
  rounds), so neither the search nor its depth over the chunks is the cost, and the touch is worth
  keeping. *Open:* the 1M seeks on the VM run 1.5-2.5% below data-layer's, the same work in the
  engine's own counts, and no arm here took it back.
