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
