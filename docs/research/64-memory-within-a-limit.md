# 64. Memory within a limit

Mantle sizes a shard to the machine (`ShardDb::follow_machine`, research/50 §9). Its budget is
the machine's effective capacity: the smallest of its memory and any cgroup, job or rlimit bound
(hyper-rt `effective_capacity`), shared among its ranges. research/36's tuner divides the budget
among the page cache, queued writes and the record cache, and the kept memtables hold the rest
(engine-structure §5). The capacity is the stable bound. Free memory, read before each growth,
gates the regions: the cache takes a chunk only while the machine has twice the chunk free, the
kept memtables' index grows a part only while the machine has the growth and two memtables free,
and a rotation that needs a new memtable first gives kept memtables back until the machine has
two memtables free.

A limit set on the process is a fact of where it runs: a container's `--memory`, a systemd
scope's `MemoryMax` or `MemoryHigh`, a job object. This note measures what the engine did under
one, why it was killed, and the change that keeps it inside.

## 1. Killed at its own limit

| where | limit | main63 / land8 | SplinterDB (32 GiB cache) |
|---|---|---|---|
| VM container, 100M (`dirty/memlimit-*`) | 24 GiB, no swap | killed in 13-19 s | |
| box scope, 1B | 64 GB, no swap | killed in 4-7 min, both arms twice | completed |
| box scope, 100M (`memacct/raw-24g`) | 24 GB, no swap | killed at rotation 135 | |
| box scope, 100M (`memacct/raw-48g`) | 48 GB, no swap | completed, 37 GB resident at its peak | |

The scope's memory under 24 GB, read every second (`memwatch/`, land8): anonymous memory rose to
the limit in 36 s, the page cache was squeezed to 21 MB, and dirty pages stayed near none; then
the kernel killed the process. Documentation/admin-guide/cgroup-v2.rst, `memory.max`: "If a
cgroup's memory usage reaches this limit and can't be reduced, the OOM killer is invoked in the
cgroup."

## 2. The accounting held

Each rotation's accounting beside the process's resident memory, from the `memacct` diagnostic
(15202158, never merged), in GB:

| 24 GB scope | rotation 132 | rotation 135 |
|---|---|---|
| budget (the scope's `memory.max`) | 24.00 | 24.00 |
| page cache | 12.87 | 13.00 |
| kept memtables, their index and parts out | 9.75 | 9.50 |
| what kept memtables may hold (`kept_limit`) | 9.98 | 9.85 |
| queued writes, record cache | 0.02 | 0.02 |
| memtables in flight: active, packing, frozen | 0.47 | 0.62 |
| resident | 23.78 | 23.84 |

The kept memtables never held more than the budget left them. The budget was the whole limit,
though, and four things lay outside it: the memtables in flight, about 0.7 GB the engine does
not account (workers' scratch, the runtime, page tables), the page cache the writes go through,
and the kernel's own charges. Together they brought the cgroup to its limit.

## 3. The gates read the host

Every gate reads free memory through hyper-rt's `Facts::memory_available_now`. On Linux this
was `MemAvailable` from `/proc/meminfo`, which is the host's (proc(5)). In the 24 GB scope on the
box (128 GB, about 82 GB available), every gate passed until the kill. Inside a cgroup, the
memory a process may take is its cgroup's room, not the host's free memory.

## 4. A cgroup's room, as the kernel measures it

The kernel measures a cgroup's headroom for its own dirty-memory bound (mm/memcontrol.c
`mem_cgroup_wb_stats`, Linux 7.2-rc4). At each level from the cgroup up to the root, the ceiling
is the lower of `memory.max` and `memory.high`, and the headroom is the ceiling less the usage,
none past it. The least headroom over the levels is the cgroup's. The memory the kernel lets a
cgroup dirty (mm/page-writeback.c `mdtc_calc_avail` and `domain_dirty_avail`) adds the cgroup's
clean file pages to its headroom. Clean pages are the file LRU's pages less the dirty ones and
those under writeback; reclaim takes them back before a charge fails, while dirty pages must be
written first. `memory.high` counts: past it the kernel throttles the cgroup's processes "under
heavy reclaim pressure" (cgroup-v2.rst), so a process sized to `memory.max` above a lower
`memory.high` is throttled before it is ever killed.

hyper-rt (hyper-raft branch `rt-cgroup-headroom`) now reads, on Linux:

- **room** for each cgroup level with a ceiling: ceiling − `memory.current` (v1:
  `memory.usage_in_bytes`) + (`active_file` + `inactive_file` − `file_dirty` −
  `file_writeback`) from its `memory.stat` (v1: the `total_` counts);
- **available**: the least of `MemAvailable` and every level's room;
- **limit**: the least ceiling, `memory.high` included (`MemoryFacts::limit`).

Readings in the box's scopes (`hrcg.log`; the scratch program allocates and touches 512 MiB at a
time, then writes 1 GiB to a file and syncs it):

| step | `MemoryMax=3G`, room MiB | `MemoryHigh=3G`, room MiB |
|---|---|---|
| start (limit read as) | 3,071 (3 GiB) | 3,071 (3 GiB) |
| 512 MiB anonymous | 2,558 | 2,558 |
| 1,024 | 2,044 | 2,045 |
| 1,536 | 1,532 | 1,532 |
| 2,048 | 1,019 | 1,019 |
| 1 GiB written, unsynced | 917 | 874 |
| synced | 955 | 955 |

Each 512 MiB the process touches lowers the room by 512 MiB. A file's pages are room once they
are clean.

## 5. The reading alone is not enough

With the change, memlimd (ad9c9d0e: land9, the change, and the diagnostic) completed the 100M run
in the box's 24 GB scope. At the same rotations, in GB:

| 24 GB scope, box | rotation 25 | 50 | 75 | 100 | 125 | 150 | 175 |
|---|---|---|---|---|---|---|---|
| page cache | 2.08 | 3.92 | 5.64 | 10.39 | 13.05 | 15.35 | 16.67 |
| kept memtables | 3.65 | 6.35 | 7.02 | 10.62 | 7.43 | 5.99 | 5.34 |
| memtables kept | 22 | 47 | 73 | 98 | 54 | 40 | 34 |
| resident | 6.66 | 11.31 | 13.87 | 21.81 | 20.92 | 21.99 | 22.45 |
| room as the gates read it | 17.32 | 12.63 | 10.09 | 2.14 | 3.03 | 1.97 | 1.50 |

It was not enough elsewhere. In the VM, in a container of 24 GiB, the change and its diagnostic
builds were killed in four runs of seven (`dirty/memlimit-*`, `dirty/memtrace-*`): mid-fill, or
in the flush after it. On the box, the 1B run in its
64 GB scope was killed after five minutes, with 63.9 GiB of anonymous memory charged
(`journalctl -k`, the memcg OOM record). A trace of the cgroup every 20 ms beside each rotation's
account (`dirty/memtrace.sh`) shows where the room went (GiB):

| VM, 24 GiB | rotation 20 | 100 | 120 | 172 | 173 |
|---|---|---|---|---|---|
| resident | 5.78 | 23.23 | 20.38 | 23.79 | 20.19 |
| the engine's account (regions, kept, in flight) | 5.66 | 21.81 | 18.53 | 22.36 | 18.52 |
| outside it | 0.12 | 1.42 | 1.85 | 1.43 | 1.67 |
| room | 16.41 | 0.61 | 3.05 | 0.13 | 3.34 |

The gates let the room fall to 0.1-0.6 GiB, two memtables or a growth and two memtables above
none. The kept memtables give memory back only at a rotation, and nothing gives memory back
between rotations or during the flush after the fill, when there are none. Meanwhile the memory
outside the engine's account moved by up to 0.8 GiB between rotations, and after the fill
anonymous memory rose 2.5 GiB in five seconds with no rotation to answer it.

## 6. What the engine does not account

A diagnostic allocator kept each thread class's live heap (memlimd2-4, never merged). The
workers' live heap was 0.6-1.7 GiB, mostly blocks of 4-8 MiB. bpftrace's uprobe on libc's
`malloc` (`dirty/alloc4m.bt`) names them: the kept index's record lists, which a packing worker
builds (`kept_table::PartedList::reserve`, `RecordList::push`), 5,614 of them over the run. The
lists are in the account while the shard holds them. What is not in it is what glibc keeps once
they and other blocks are freed. `mallinfo2(3)`'s free bytes track the memory outside the account
closely:

| VM, 24 GiB | rotation 37 | 100 | 109 | 127 | 146 |
|---|---|---|---|---|---|
| outside the account | 1.13 | 1.08 | 2.42 | 2.02 | 1.97 |
| glibc's free bytes, held | 1.28 | 1.32 | 2.54 | 2.43 | 2.28 |

`malloc_info(3)` at 21 GiB resident: 37 arenas holding 1.33 GiB free, 0.38 GiB of it in chunks of
4-8 MiB and 0.67 GiB in chunks of 64 KiB-1 MiB. glibc's mmap threshold is dynamic: "when blocks
larger than the current threshold and less than or equal to DEFAULT_MMAP_THRESHOLD_MAX are freed,
the threshold is adjusted upward to the size of the freed block" (mallopt(3), `M_MMAP_THRESHOLD`).
After the first record list is freed, blocks of that size come from the arenas and stay there
once freed. With the threshold set (`GLIBC_TUNABLES=glibc.malloc.mmap_threshold=131072`, which
mallopt(3) says disables the adjustment), the same runs:

| 100M, VM | glibc free held, most | outside the account, most | room, least | fill/s | faults in the fill |
|---|---|---|---|---|---|
| default, no limit | 2.78 | 2.45 | 21.25 | 5,519,464 | 1,269,911 |
| threshold set, no limit | 0.11 | 0.45 | 23.70 | 5,477,413 | 3,094,142 |
| threshold set, 24 GiB | 0.13 | 1.04 | 0.47 | 4,977,290 | 5,475,269 |

The allocator holds the larger part of what the engine misses. Even without it, the room fell to
0.47 GiB.

## 7. The budget follows the room

The engine already divides a budget among its regions and has them give memory back to it:
research/36's tuner, the kept memtables' limit, the caches' trims. The budget was fixed at the
shard's share of the machine's capacity, so under a limit it never fell, and the machine's free
memory only stopped growth. Now a shard that follows the machine looks at it, and its budget
becomes what its regions and kept memtables hold plus its share of the machine's free memory
(hyper-rt's reading, §4), less a room kept free. The budget never rises above the shard's share
of the capacity, and never falls below what the shard cannot give back: its fixed write
reservations and the caches' indexes and ghosts.

- **When.** At each rotation, at the end of each get cycle (research/36's cycle, a memtable's
  worth of operations), and in a range task's turns and idle slices once a fill's time
  (`fill_ns`, the cycle a rotation measures) has passed since the last look. The flush after a
  fill, which has no rotations, follows the machine at the pace a fill does.
- **The room kept free.** The most that the process's memory outside the shard's account has
  grown between two looks: its resident memory less the budget's regions, the kept memtables and
  the memtables in flight (workers' buffers, the allocator's free lists, other shards). That is
  what may grow before the next look with no gate to ask. The gates keep their own steps' room as
  they admit them, so the budget does not keep it again: with twice the cache's next chunk
  (0.5-0.9 GiB) added, the budget swung by that much from look to look and left 4.7 GiB unused,
  the kept memtables were all given to the page cache by the fill's end, and the first gets after
  it read the device 1.61M times (`dirty/memtrace-memlimf-24g`, 60K gets a second). Other
  processes' memory lowers the free memory a look reads, not the room kept: a margin learned from
  them would hold memory back forever after one burst.
- **Giving back.** A budget that falls has the kept memtables give memory back at once
  (`fit_kept`), and the page cache and record cache trimmed to their new shares at once, rather
  than a few pages an operation: the machine has no room for them. A trim retires whole chunks,
  so their memory goes back to the OS.

`tests/shard_db_follow.rs` drives the machine's reading through three phases: memory to spare,
then none, then memory again. Every read is checked against an oracle throughout. With nothing
free, the next rotation's look lowers the budget from its bound to what the shard holds at most;
it never rises while nothing is free, the regions and kept memtables hold no more than it, and the
kept memtables give memory back. When memory returns, so does the budget.

## 8. Spare loan memory crowds out the pages

With the budget following the machine, every run in the VM's 24 GiB container survived (eight
of eight). But the first gets after the fill ran at 24K-137K a second, where the reading-only
runs that survived ran at 316K-728K. The page cache's own account shows why (pages of 4 KiB):

| 100M, VM, 24 GiB | live pages | spare pages | device reads in the queries | gets/s |
|---|---|---|---|---|
| reading only (memlimd) | 1,995,461 | 2,393,022 | 0 | 728,226 |
| budget follows (memlimf) | 337,242 | 3,755,238 | 1,613,151 | 59,992 |
| budget follows (memlim3) | 0 | 4,489,097 | 1,944,078 | 24,087 |

A loan (`Cache::lend`) lends the page cache's memory to a worker for the pages it writes; when
the loan comes back, its chunk holds the pages kept (`Held::Kept`). Once every one of those pages
is forgotten, as a branch merged away forgets its pages together, the chunk is spare
(`Held::Spare`): mapped memory kept for the next loan, holding no page. Spare chunks count against
the cache's limit, and read slots never use them. Nothing bounded them. At 100M with no limit,
land9's cache held 7.6 GiB of live pages, 2.2 GiB of dead slots and 10 GiB of spare loan memory
in 21 GB (r6085, `postfill_waits`), while the loans, about 13K pages each with at most one a
worker, never had more than about 0.9 GiB out at once. Under a budget that falls, the spare took
all but 50K pages of the cache's limit, and the get working set no longer fit.

Spare memory is now bounded by the most the loans have had out at once, less what is out now
(`Cache::shed_spare`). Beyond that, no loan has needed it, and its memory goes back to the OS
whenever a chunk becomes spare. The next loan still takes spare memory, already mapped, before
fresh memory. A chunk given back keeps its slot entries (`Held::Hollow`), as a trim's does. Fresh
memory is taken only when loans pass their earlier peak, so these entries are bounded by the
workers times the largest loan. Tests: `store::cache::tests::spare_loan_memory_holds_no_more_than_the_loans_had_out_at_once`
(eight loans in turn, their pages forgotten: spare stays within one loan, seven loans' memory
goes back, and the next loan takes the spare), and the 40,000-step test, which now asserts the
bound after every step.

With spare loan memory bounded (memlim4, 3f85185b), every 24 GiB run survived, but in two of
three the page cache ended at 2.7-5.5 GB, and the first gets ran at 48K-268K a second. A trace of
each look (`dirty/memtrace-memlimf2-*`) shows why the cache stayed small. After the fill, the
memory outside the account rose to 3.7-4.1 GiB, and the most it grew between two looks
(`grew_most`) reached 1.2-2.0 GiB: freed memory that glibc's arenas kept counted as growth outside
the account, and the budget kept that much free. With glibc's free pages released at each look
(`malloc_trim(3)`, a diagnostic: memlimf3, c0897f27), the outside memory stayed under 0.85 GiB,
the cache held the whole get working set (1,995,943 live pages, no device read in the gets), and
gets ran at 598K and 648K a second. But a trim took 1.7-2.2 ms at the median, 13-22 ms at the p99
and up to 95 ms on the shard's thread, at every look.

## 9. The allocator's threshold, kept, and looks in idle time

Turning glibc's threshold adjustment off at the process's start (`port::mmap::return_freed_blocks`)
keeps freed large blocks out of the arenas with no trim. Alone, it left one run of three killed in
the 24 GiB container, 23 s in, just after the fill: in the flush, with no rotation, the shard looked
at the machine only once a fill's time had passed, and the consolidations on every worker grew past
the room kept between two looks. In idle time the shard now looks whenever a worker's message has
come back since the last look; idle time is the shard's own to spend, and the workers' memory moves
with their jobs. A trim's ghost entries are charged to the cache, so after a look's trims the budget
is never below what the shard then cannot give back.

| 100M, VM, 24 GiB | survived | fill/s | gets/s | seeks/s | live pages |
|---|---|---|---|---|---|
| memlim4, the arenas' default | 3 of 3 | | 48K-268K | 39K-234K | 0-1,876,136 |
| memlim4t, the threshold kept | 2 of 3 | | 643K-657K | 618K-624K | 1,995,580-1,995,652 |
| memlim4t2, and looks in idle time | 8 of 8 | 5.08M-5.32M | 619K-799K | 607K-673K | 1,995,582-1,995,784 |

With no limit, against land9 (memhalf2's binary), median ratios (rounds won):

| 10M, 12 rounds | fill | gets | seeks | settled gets | settled seeks |
|---|---|---|---|---|---|
| memlim4 | 1.01x (7) | 1.02x (8) | 1.01x (8) | 1.01x (7) | 1.01x (9) |
| memlim4t | 1.01x (8) | 0.99x (4) | 1.01x (7) | 1.01x (7) | 1.02x (8) |

| 100M, 6 rounds | fill | gets | seeks | settled gets | settled seeks | resident |
|---|---|---|---|---|---|---|
| land9 | | | | | | 36-37 GB |
| memlim4 | 0.99x (2) | 0.95x (0) | 0.98x (1) | 0.96x (1) | 1.00x (3) | 38-40 GB |
| memlim4t | 1.01x (4) | 1.01x (4) | 1.06x (6) | 1.00x (3) | 1.08x (6) | 36-37 GB |

The spare bound alone (memlim4) gave memory back that glibc kept: 2-3 GB more resident and 5 % of
the gets at 100M. With the threshold kept, what the engine gives back leaves the process.

The longest puts are not the looks. A trace of every rotation and packing (stallm, never merged:
three runs with no limit, three in 24 GiB) put each stall of 300 ms or more on a packing's wait for
its writes, 0.69-0.83 s once each for three packings at once (research/58 §8's journal waits); no
look took more than 24 ms.

## 10. The release gate (land10, 24c1084d)

land10 is land9 with §4-§9. The VM, no limit, all arms interleaved (run7's order), in ops a
second (n = rounds):

| 1M, 8 rounds (r6211-6218) | fill | gets | seeks |
|---|---|---|---|
| RocksDB | 979,456 | 698,843 | 222,914 |
| SplinterDB | 1,429,009 | 1,887,110 | 352,433 |
| main63 | 6,207,072 | 4,715,282 | 1,317,354 |
| land10 | 7,220,702 | 4,829,191 | 1,319,390 |

| 10M, 6 rounds (r6221-6226) | fill | gets | seeks |
|---|---|---|---|
| RocksDB | 943,824 | 277,046 | 124,872 |
| SplinterDB | 1,295,537 | 961,262 | 221,009 |
| main63 | 4,912,822 | 3,080,342 | 1,029,052 |
| land9 | 6,954,509 | 4,478,570 | 1,003,508 |
| land10 | 7,221,894 | 4,523,253 | 1,019,846 |

| 100M, 6 rounds (r6231-6236) | fill | gets | seeks |
|---|---|---|---|
| RocksDB | 851,716 | 250,761 | 77,212 |
| SplinterDB | 1,351,961 | 400,706 | 49,210 |
| main63 | 4,416,734 | 2,100,036 | 610,558 |
| land9 | 6,090,162 | 2,569,302 | 598,420 |
| land10 | 5,835,783 | 2,567,592 | 634,012 |

| land10 | fill | gets | seeks |
|---|---|---|---|
| 10M x SplinterDB | 5.57x | 4.71x | 4.61x |
| 10M x RocksDB | 7.65x | 16.3x | 8.17x |
| 100M x SplinterDB | 4.32x | 6.41x | 12.9x |
| 100M x RocksDB | 6.85x | 10.2x | 8.21x |

In a 24 GiB container, three runs of three survived (fill 4.80M-5.40M, gets 562K-755K, seeks
448K-640K). On the box, in the 64 GB scope where main63 and land8 were killed at 1B, land10
completed both runs, beside SplinterDB's three (the box under kiln's load, which moves SplinterDB's
fill 2.8x between its runs):

| 1B, box | fill | gets | seeks | worst put |
|---|---|---|---|---|
| land10 r751, r752 | 2,950,196, 3,618,068 | 26,343, 29,533 | 10,966, 12,584 | 16.1 s, 4.5 s |
| SplinterDB r711, r721, r751 | 228,879-650,377 | 2,575-18,942 | 988-3,160 | 45.2, 8.5, 5.0 s |

With no limit at 100M, land10 runs land9's gets at 0.93-0.95x and seeks at 1.05-1.07x over three
12-round runs (r6241-6252, r6261-6272 with an A/A of layout alone moving 3 % or less, r6301-6312).
Every found get in both is a kept-index probe (no cache hit, no device read), and land10's gets
take 420 ns of CPU against 400, with no fault or switch. The spare bound is not the cause: land10
without it (a18214c1, never merged) runs gets at 0.90x. Nor is the allocator: memlim4, without the
threshold, holds the same larger kept index. A line at each generation's close (land9k/land10k,
never merged) shows the cause. A generation closes when the open index reaches a third of what the
kept memtables may hold (research/63 §3). land9's fixed budget is the machine's capacity, 67 GB,
and its third was reached at the fill's last rotation: one table, 62.8M records, 13.7 GB. land10's
budget is what the VM has free, about 60 GB, so its third came at rotation 147-148: 54.3M records
in a table closed just after a growth step (259 bytes a record against 235), and the last 30
memtables' records in a second, open table, 17.2 GB in all. Every get then reads two populated
tables. It is the one-third rule's step, landing between rotation 147 and 177 at this size; at 10M
neither reaches it, and a store a little larger than 100M crosses it under either budget.
research/63 §3 priced a get over two generations at one overlapped read (145-146 ns against 142 for
one, `mlp4` mode 2, at 10M); here, at 100M, the second generation costs 20 ns a get, about 5 %.
That gap, between the step's designed and measured price, is the kept index's to close
(research/63), not the budget's: land9 meets the same step at a larger store.

## 11. A retirement the limit no longer asks for

A trace of each look at 1B in the box's 64 GB scope (c1b 2396a6de, never merged; r761) showed the
page cache holding 35-44 GB of live pages through the fill, then losing them: from rotation 1386
of 1773 no loan was made again (11,337 loans, then none), and through the flush that followed the
cache's live pages fell from 35 GB to 16 GB while its limit stayed near 57 GB. land10's own 1B runs
began their gets with no live page at all (`postfill_waits`, r751 and r752), and read the device for
nearly every found key. The same happens at 100M in an 8 GB scope, in minutes rather than at 1B's
hour (c1b2 126eb51a, r771):

| 100M, box, 8 GB scope (r771) | at 32 s | at the fill's end | the gets |
|---|---|---|---|
| a chunk retiring | yes | yes | yes |
| chunks' room, GB | 4.87 | 3.53 | 0.79 |
| the limit, GB | 5.26 | 6.67 | 5.71 |
| loans made | 99 | 99 | 99 |
| live pages, GB | 3.76 | 2.52 | 0 |

A chunk retires when the chunks' room passes the limit (`Cache::trim`): its pages are dropped a
step at a time and the chunk is given back. While one retires the cache makes no loan
(`Cache::lend`), gives up each loan that comes back with its pages (`Cache::adopt`), and makes no
slot for a read (`Cache::make_now`, `Cache::unreserved`). A retirement ended when its chunk was gone
or, once the limit no longer asked for it, at the next `trim` or idle `reserve`
(`settle_retirement`). Under a budget that follows the machine (§7), the limit falls and rises
with each look. A retirement begun under a lower limit then outlived its need: `trim` runs only
while the cache is over its limit, which it no longer was, and `reserve` runs in idle time only
when slots are owed, which `unreserved` reports as none while a chunk retires. Nothing ended it,
and the cache took no page again for the rest of the run.

A retirement now ends wherever the limit rises (`resize`) or the room falls (`trim` once its
spare memory is given back, `shed_spare`). With that (c1b3 = c1b2 + the change, r781, same scope
and size), retirements began and ended as the limit moved (each look's line: retiring at 54 s,
over by 61 s), loans went on through the flush (102 at 33 s, 313 at the end), and the cache began
the gets with 1.36 GB of live pages and 1.39 GB spare. Test:
`store::cache::tests::a_retirement_ends_once_a_raised_limit_holds_the_room` (a retirement begun
under a lowered limit, the limit raised: the retirement is over, a loan is made and its pages
read back); without `resize`'s settle it fails at its first assertion (box, retire 0fdf9c1f).

At 1B in the box's 64 GB scope (retire 0fdf9c1f, r791; the box at a load of 60 to 290 from other
work through the run, where land10's r752 ran at 10 to 60), the cache began the gets holding 30.5
GB of live pages where land10's held none, and the gets read the device about a third less:

| 1B, box, 64 GB scope | land10 r752 | retire r791 |
|---|---|---|
| live pages when the gets begin | 0 | 7,983,683 (30.5 GB) |
| device reads in 1M gets | 595,301 | 374,117 |
| gets made in memory (inline) | 496,744 | 629,844 |
| a get's median | 47.1 µs | 8.2 µs |
| seeks made in memory | 0 | 407,012 |
| seeks a second | 12,584 | 15,136 |
| gets a second | 29,533 | 14,606 |

The gets' rate fell with the load, through its tail (p99.9 3.5 ms, p99.99 55 ms, against 0.8 and 1.8
ms in r752), not through its device reads; a run on a quiet host decides it. The cache held 43.8 GB
in all: 30.5 GB of live pages, 5.6 GB of dead slots in loans' chunks some of whose pages were
forgotten, 4.7 GB of spare loan memory kept for loans after the fill had ended, and the rest its
index and ghost. The loans could not hold 22.1M pages of what their workers wrote (84 GB of 412 GB):
what the cache holds when the gets begin is the next question at this size, not its limit.

## 12. Where a full cache's memory goes at 1B

A trace of each look with the retirement fixed (c1b3 4a4f99ea, never merged; r801, the box quiet,
the 64 GB scope; sizes in GB):

| s | rotation | cache / limit | live | dead | spare | loans' pages kept / missed in 30 s |
|---|---|---|---|---|---|---|
| 92 | 542 | 42.1 / 57.7 | 35.7 | 6.2 | 0.1 | 47.5 / 0.5 |
| 152 | 830 | 57.4 / 57.9 | 37.7 | 19.7 | 0 | 23.6 / 4.5 |
| 305 (the fill's end) | 1773 | 52.9 / 53.6 | 35.4 | 17.5 | 0 | 4.8 / 2.9 |
| 368 (the flush) | 1773 | 53.7 / 53.8 | 36.2 | 17.3 | 0 | 2.3 / 12.7 |
| 552 | 1773 | 54.7 / 54.7 | 40.8 | 12.5 | 0 | 3.8 / 5.4 |
| 582 (the gets) | 1773 | 37.2 / 54.7 | 29.1 | 4.9 | 3.2 | |

From the moment the cache meets its limit, a third of it is dead: slots of loans' chunks whose
pages were forgotten while others of the same chunk were not. A loan's fresh memory came back as
one chunk, the whole of what the job wrote (49 MB a loan on average here), and a chunk is free
only when its last page is. A job's pages are freed an extent at a time, as later jobs consume
the slices of the branch they wrote, so a chunk spanning many extents sits mostly dead for most of
its life. With no spare chunk and no room under the limit, a loan gets less room than its job
writes and the rest is missed: through the flush, whose whole consolidation writes the tree that
the gets will read, the loans missed 5 to 13 GB every 30 s, and kept 2 to 4.

A loan's fresh memory now comes in buffers of at most a huge page of whole extents, each its own
chunk once back ([`Cache::lend`], `loan_chunk_pages`), so a chunk is freed with the extents it
holds. The chunks' room, spare and lent pages, which `room`, `shed_spare` and `out_on_loan` summed
over every chunk, are kept as fields, with tens of thousands of chunks a cache at 1B. The cache's
40,000-step test checks the fields against those sums, and the retirement invariant (§11), after
every step, with loans in buffers of two pages.

## 13. Hollow chunks' slots and a cache that stopped taking pages

The same trace past the fill (r801; sizes in GiB, as the diagnostic prints them): the cache's
live pages are 7,624,385 at the fill's end, after the gets, after the seeks and after the
tombstones, while the gets read 383,890 pages from the device, the seeks 803,861 more, the cache
stands 17.5 under its limit and the machine has 20 free.

| s | phase | cache / limit | live | dead | spare | machine free |
|---|---|---|---|---|---|---|
| 548 | the fill's flush | 54.8 / 54.8 | 40.9 | 12.7 | 0 | 2.4 |
| 609 | the gets | 37.2 / 54.7 | 29.1 | 4.9 | 3.2 | 20.2 |
| 851 | the seeks' end | 37.2 / 54.7 | 29.1 | 4.9 | 3.2 | 20.0 |

A read's insert takes a free slot or makes one (`make_now`), and idle time makes the slots the
reads asked for (`reserve`, up to `unreserved`). Both counted the slot table's length against the
limit. A chunk whose memory goes back, spare beyond the loans' need (§8) or to a lowered limit,
keeps its slots in the table, since slots are indexed from it by every later chunk. With live
pages under the limit and none inserted, the table held at least the limit's 14.34M slots where
the chunks held 9.76M pages: at least 4.6M slots, 17.5 GiB, of memory the fill's last forgetting
had given back, counted as if the cache still held it. From then on no read made a slot: every
page the gets, seeks and tombstones read was left out.

The same slots were never used again. A loan's fresh memory always came back as new chunks, each
adding its slots (56 bytes a page) to the table, so the table grew with every page of fresh loan
memory over the process's life, with no bound.

Both are fixed at their cause ([`Cache::lend`], `make_now`, `unreserved`):

- The limit holds the chunks' room. A read makes a slot while the last read-made chunk has room
  not yet made or a new chunk fits under the limit (`make_slot`), and idle time makes as many;
  past the limit, until `trim` brings the room down, neither does.
- A loan's fresh memory fills hollow chunks' slots first, a whole chunk each, before any new
  buffer, and a new buffer is lent only once no hollow chunk is left. A refilled chunk counts as
  fresh room until it comes back. When a loan adds a chunk, then, the loans' chunks' slots are
  the memory those chunks and the loans out hold, at most the limit; read-made slots are within
  the chunks' room: the table stays within twice the largest limit. The bench prints it
  (`cache_slots`). The price: room under the limit short of the next hollow chunk's slots, at
  most a loan chunk (2 MiB on Linux), lends no fresh memory. A chunk refilled in part would keep
  its unused slots for as long as it lives, and nothing would bound how many such chunks the
  table holds.

Two tests reproduce the trace's two defects on the old code, and the cache's 40,000-step test
checks the bound, and that every hollow chunk is listed for the next loan, after every step.

## 14. 1B in the VM with the cache working

One round each in the VM (62.7 GiB, 18 CPUs), the engines the same day: the candidate (hollowio
b6793592: §11's retirement fix, §12's loan chunks, §13's hollow chunks, and the bench's per-phase
storage bytes) beside fairq, whose cache §11's freeze left empty (`runs/n1000000000-r6491-x_hollow`,
`r6401-x_fairq`, `r6401-sdb32all`, `r6401-rocks32`).

| 1B | candidate | fairq | SplinterDB | RocksDB |
|---|---|---|---|---|
| fill | 1.87M/s (534 s) | 2.80M/s (357 s) | 1.21M/s | 636K/s |
| gets | 21.5K/s | 8.7K/s | 9.64K/s | 13.2K/s |
| seeks | 10.4K/s | 3.77K/s | 1.68K/s | 5.56K/s |
| fill CPU a put (system) | 2,540 ns (1,031) | 1,996 ns (689) | 1,414 ns | |
| fill page faults | 124.8M | 76.4M | | |
| fill stalls, worst put | 269, 10.7 s | 137, 4.2 s | 2.2 s | |
| run's reads, writes (rusage) | 522 GB, 615 GB | 430 GB, 635 GB | | |
| cache at the fill's end: live, dead, spare | 38.9, 0.1, 5.4 GiB | 0, 0, 2.8 GiB | | |
| loans' pages kept, missed | 423 GiB, 67 GiB | 121 GiB, 1.7 GiB | | |

The cache holds the tree now: 38.9 GiB live at the fill's end, dead memory 0.1 GiB where §12 had
a third of the cache, the slot table 12.7M slots against a 12.3M-page limit, and the reads grow it
(40.0 GiB live after the gets, 42.0 after the seeks, 43.7 after the tombstones, the limit 47 GiB).
A get read the device 0.30 times (298,685 reads for 1M gets) at 46.5 µs a get, and the gets and
seeks ran 2.5 and 2.8 times fairq's.

The fill lost a third. Its storage, measured for the phase alone now (`fillrandom storage`), read
313 GB and wrote 525 GB. Against fairq's fill it took 48.4M more page faults and 342 ns a put more
system time, and stalled twice as often. The loans kept 3.5 times fairq's pages, about half of
them in memory newly mapped and faulted a page at a time; and the page cache the OS kept beside an
empty engine cache, which served the compactions' reads, is the engine's own cache now, which the
workers do not read. A traced 1B fill attributes the time (queue 94).

## 15. Loan chunks off huge pages

The 10M and 100M gates (VM, `runs/n10000000-r6461..r6470`, `n100000000-r6471..r6480`, ten
rounds, run7's balanced order) ran the candidate's seeks below cand's in every round at 10M, 898K
against 1,056K a second (0.85×), and 568K against 619K at 100M (0.92×), with the same cache hits,
the same sources a seek and no device read; its fills and gets ran level. On the box, bare metal,
the two seeked level (`n10000000-r901..r906` on CPUs 8-15: 824-902K and 845-895K).

§12's loan chunks are heap blocks of at most a huge page each. glibc maps such a block alone,
from past its own header and a page longer than asked (mallopt(3), `M_MMAP_THRESHOLD`), so none
lies on a huge page's boundary and the cached leaves in it sit on base pages: at the 10M seek
phase the box's engine held 1.59 GiB on huge pages of 3.79 GiB resident, against cand's 2.01 GiB of
3.39 (`r906`, `/proc/PID/smaps_rollup`). A seek's leaf read then misses the TLB, and in the VM a
miss walks the guest's tables and the host's for each of the guest's (the cost bare metal does
not pay twice).

`port::mmap::PageBuf` is a chunk's memory now: room fixed when made, written and emptied in place;
on Linux mapped from a huge page's boundary and advised for huge pages, faulted in by its first
writes, a worker's keep or a slot's making, so a loan chunk of a huge page's bytes takes one huge
page and one fault rather than 512; elsewhere a heap buffer. In the VM (`n10000000-r6521..r6526`,
six rounds of three arms):

| 10M | fill | gets | seeks | seeks, each round |
|---|---|---|---|---|
| cand 43cd9c15 | 7.32M | 4.85M | 1,136K | 1,035-1,150K |
| hollowio b6793592 | 7.38M | 4.85M | 984K | 969-1,006K |
| pagebuf b80151ce | 7.20M | 4.81M | 1,105K | 1,068-1,114K |

The seeks are back to 0.97× cand's, every round above hollowio's.

## 16. The machine stalling on memory under a full budget

A traced 1B fill of pagebuf (spantot-p 45e429ab, never merged; `diag-runs/n1000000000-r5301-t_stp1b`,
426 s, 2.35M puts a second), with the VM's memory sampled every 5 s beside it
(`-mem.txt`, `-mem2.txt`: `/proc/pressure/memory`, `/proc/vmstat`, `/proc/meminfo`; a sample
across the sampler's restart is left out):

| seconds into the run | all tasks stalled on memory | swapped out | swapped in | major faults | swap free |
|---|---|---|---|---|---|
| 119-259 (the fill) | 86-171 ms a second | 19-143 MB/s | 2-91 MB/s | 336-7,479/s | 0-1.1 of 4.2 GB |
| 286-395 (the fill) | 74-97 ms a second | 4-8 MB/s | 4-7 MB/s | 421-693/s | none |
| 601-784 (the reads) | 2-4 ms a second | none | 0-4.5 MB/s | 2-816/s | 3.5 GB |

Through the fill every task in the VM waited on memory 7-17% of the time, the swap full; the
engine's anonymous memory stood at 56-62 GB of the VM's 65.7. The container's own cgroup counted
26.0 s of "some" memory stall in the run, 25.2 s of "full" (`memory.pressure`). §14's candidate
took 1.1M major faults in its run against fairq's 0.27M. The budget (§7) follows what the shard
holds and a share of the free memory the machine reports: when the free memory runs out it stops
growing, but nothing in it gives memory back when the kernel reclaims the engine's own pages and
swaps them.

The same trace's spans name the shard's part of a 1B put (ns a put, the fill's 426):

| shard work | ns a put | a span |
|---|---|---|
| paying maintenance shares (the trunk's steps 65.7, forgetting 11.2, packs 10.5) | 90.2 | 10.3 µs |
| applying jobs' results (dead branches dropped 16.2, loans adopted 12.0) | 31.5 | |
| a loan's pages into the cache | 14.7 | 795 µs |
| lending a job memory | 14.6 | 722 µs |
| granting a job extents | 6.9 | 342 µs |
| puts waiting on a full memtable (115) | 36.4 | 316 ms |
| the client yielding to the shard's maintenance | 112.2 | 12.8 µs |

**Pressure in the budget.** Linux measures what a cgroup's tasks lose to memory shortage directly:
pressure stall information, a cumulative count of the microseconds at least one of the group's
tasks waited for memory ("some") in `memory.pressure` (kernel `Documentation/accounting/psi.rst`;
research/25 [PSI]). TMO (Weiner et al., "TMO: Transparent Memory Offloading in Datacenters",
ASPLOS 2022) sizes containers' memory from it: its controller, Senpai, holds a container where its
"some" stall stays at 10 ms in every 6 s (facebookincubator/senpai, `senpai.py`: `--pressure`
10*1000 microseconds, `--interval` 6 seconds; it backs off on any window past that). The budget
now reads the process's cgroup's `memory.pressure` (the system's `/proc/pressure/memory` where the
process has no cgroup v2 path with one) at each look. A window in which the stall passed 10 ms
lowers a ceiling on the budget to one tuning step (5%, §7's step, Luo & Carey) under what the
shard holds, and the caches and kept memtables give that memory back at once (§7's fall); each
6 s window with less raises the ceiling a step, until it no longer binds. Elsewhere than Linux
there is no reading, and the budget is §7's.
