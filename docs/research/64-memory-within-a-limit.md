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
