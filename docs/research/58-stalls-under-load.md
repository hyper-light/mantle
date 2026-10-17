# 58. Stalls under load

The owner's rule: a busy machine is the norm, and Mantle degrading more than RocksDB under load is
the defect. This note measures the degradation under a controlled load, traces the stalls, and
names the stages they wait on. Runs are in `benchmark-results/mantle-splinterdb-20261013`
(`runs/` for release binaries, `diag-runs/` and `prof/` for diagnostic ones). Two builds:
- the candidate, x_wa5: fc80a9c, md5 69b435e0;
- the cache-work cut, x_cw1: branch cache-wr, 65e3709, md5 56670b48; it is described in that
  branch's research/57 §8.

## 1. Degradation under a controlled load

**Setup.**
- Each case runs beside the same 12 busy loops on the VM's 18 cores (inside7.sh's HOGS),
  started before the run and stopped after it.
- Compared with the same cases with none: 10M, 4 rounds each, every engine, run7.sh's balanced
  order.
  - None: `runs/n10000000-r2101..r2104-*`.
  - 12 loops: `r2111..r2114`.
- Another project's processes loaded the host throughout: VM load 6.7-8.8 at the no-loop cases'
  starts, 8-24 at the loaded ones'.

| engine | fills: none → 12 loops (kept) | gets | seeks |
|---|---|---|---|
| Mantle, the cut | 4.18M → 1.87M (0.45×) | 2.64M → 1.95M (0.74×) | 577K → 419K (0.73×) |
| Mantle, the candidate | 3.75M → 2.57M (0.69×) | 2.61M → 2.03M (0.78×) | 576K → 430K (0.75×) |
| RocksDB 32 GiB | 639K → 536K (0.84×) | 211K → 149K (0.70×) | 103K → 74K (0.72×) |
| SplinterDB 32 GiB | 916K → 649K (0.71×) | 804K → 480K (0.60×) | 196K → 142K (0.73×) |

| Mantle, medians | fill p99.99 | fill max | get p99.99 | get max | stalls |
|---|---|---|---|---|---|
| the cut, none | 22.3 µs | 7.6 ms | 36.4 µs | 0.1 ms | 0 |
| the cut, 12 loops | 49.7 µs | 1,828 ms | 75.8 µs | 1.4 ms | 2 |
| the candidate, none | 23.3 µs | 81.9 ms | 45.1 µs | 0.3 ms | 1 |
| the candidate, 12 loops | 60.4 µs | 773 ms | 53.2 µs | 2.2 ms | 2 |

**Reading.**
- Gets and seeks keep as much as RocksDB and SplinterDB do.
- Fills keep less: 0.45-0.69× against RocksDB's 0.84×.
- The worst puts reach seconds: the cut's 0.97-3.7 s and the candidate's 0.22-2.4 s, every
  loaded run stalling 2-3 times.

## 2. What the stalls wait on

The scalar event trace on the cut (trace-cw, `diag-runs/n100000000-r1931..r1933`, host load only)
caught four stalls. Each was a stage the writer depends on that ran slower than the writer fed it:

| stall | its length | what the rotation waited on | why that stage was slow |
|---|---|---|---|
| r1932, 3.6 s into the fill | 197 ms | the root's room | the root compaction that freed it computed 1.19 s |
| r1932, 4.3 s | 468 ms | the root's room, 6.7M entries owed | that compaction computed 992 ms, 85% on CPU, 5,741 page faults |
| r1933, 25.2 s | 227 ms | the frozen memtables' bound (two, one allowed) | the oldest's packing ran 31% of its 554 ms, 17,803 page faults |

Side by side with the candidate (`r1931..r1933`), the candidate stalled once in each run. These
are the stages a loaded host slows, and they are not the cut's.

## 3. The engine saturates its own disk

During the 25.2 s stall the VM's disk was saturated (I/O full 68%), and the writes were the
engine's own:
- Its container wrote 1,292 MB/s across the stall's window, against 632 MB/s over its whole run;
  no other container wrote (`iostat.txt`, cgroup io.stat a container, every 0.25 s).
- A growth was writing its segment's zeros then, 97% of its job off CPU, beside two compactions.

A 100M fill grows its file by about 36 segments (`runs/n100000000-r2011..r2014-x_cw1`):
- 13-15 of 64 MiB, then 21-22 of 626 MiB, the largest grant asked;
- 14.7-15.4 GB of zeros in all;
- each late segment one 626 MiB burst, about every 1.6 s of fill.

Whether the zeros were needed was taken from research/48 §4, a file's own first writes
stalling its other writers on ext4 as on APFS. That measurement wrote past the file's end; §8
measures a segment allocated whole, written without them.

## 4. The stalls of seconds: the root leaf's first settle

The candidate with the trace, at 10M beside 12 and 18 busy loops (`diag-runs/n10000000-r1941..r1944`
and `r1951..r1954`), stalled on two things:

| stall | its length | what the rotation waited on | the stage it waited for |
|---|---|---|---|
| r1954, 18 loops, 7.5 s | 1,257 ms | the root's room: 5 branches pending, 7.6M entries owed | the root leaf's first settle: 3,320 ms, 32% on CPU, 3,080 preemptions |
| r1951, 18 loops, 5.9 s | 134 ms | the root's room: 7 pending | that settle: 1,983 ms |
| r1943, 12 loops, 3.1 s | 147 ms | the root's room: 6 pending | that settle: 960 ms, 4,105,704 entries, 69% on CPU |
| rotation 2, each loaded run | 1-130 ms | the first packing: no overlap measured, none allowed frozen | the first packing, 165-380 ms |

**Why the root leaf.**
- The root starts as a leaf.
- A leaf settles past `leaf_limit()` = fanout × `leaf_entries`. The bench's `leaf_entries` is one
  memtable's entries, so the root leaf takes eight packings, about 4.1M entries at 10M.
- It then merges them all in one settle, which splits it into an index node.
- Until that settle applies, the packings that follow wait as pending branches, and the root's room
  (pending and live in-flight, below fanout) runs out.
- Afterwards the root split (research/57 §7) places each packing's parts in the root's pivots, and
  this wait does not come again.

**Step one: is the work needed?** Yes. The first settle is what builds the trunk's first level.

**Step two: is this much needed at once?** Open. The root leaf is the one node whose settle every
packing waits on. Settled at one memtable's entries instead of fanout of them, the root would be an
index node within the first few packings, and every packing after would take the root split.

That is to be measured against the candidate:
- at 0, 12 and 18 busy loops;
- at 10M and 100M;
- with its writes (entries written), seeks and gets beside its stalls.

## 5. Open

- **Step one for the slow stages.**
  - Does the root's first compaction, which computed 1-1.2 s, need to merge that much while
    the root fills?
  - Does a packing need to fault 17,803 fresh pages?
- **Step two for the zeros:** §8.
- **The cut under load.** 0.45× against the candidate's 0.69× at 12 loops, worse in 3 of 4
  rounds. That regression is unexplained, so the cut is not adopted (cache-wr's research/57 §8).

## 6. A compaction no seat could take: the shard spun

**What failed.** `ranges_room`'s one-worker test failed now and then in CI: on 2 CPUs beside 2
busy loops (cna-wr, eff4-wr), on windows-x86_64 and on linux-aarch64 (split-wr). Each failure
whose message was read waited for the same fact, the shard parking after it answered a probe.
The two whose guard names the run's progress stopped at the same place, 79 of 4096 operations
done.

**Reproduced.** In the Linux VM, on 2 CPUs beside 2 busy loops on the same CPUs:
- the test binary failed in 1 of 40 runs;
- the one-worker test alone failed in 2 of 36 and 2 of 40 runs.

**Traced.** Stacks were sampled with eu-stack while a run hung:
- The worker was in the held growth's write.
- The shard thread was not parked. It cycled through the range's turn: the paused put's rotation,
  the idle slice, the turn's growth look, the cancellation check.

A scratch build counted the range's passes that neither served a request nor waited. At 100,000
passes it printed the shard's state:

| part | state |
|---|---|
| writer | one put pending, its rotation waiting for the root's room |
| root | no room, its one compaction ready |
| pool | one seat, held by a growth; no wait registered |
| range | work owed (`idle_owed`), nothing to wait for (`waiting_for_io` false) |

**Cause.** A ready compaction that found every seat held returned from the dispatch with no wait
registered. Only a worker's message frees a seat. The range had nothing to wait for, so it passed
again at once, for as long as the seat stayed held. A packing in the same position already
registers the wait (`ShardDb::sort_off`).

The spin has a cost beyond the test. On a loaded host, the spinning shard takes CPU from the
very worker whose message it waits for. This is the condition §1 measures and §2's stalls sit in:
the rotation waiting for the root's room.

**Change** (`seat-wr` 6ec8434). The dispatch registers the wait for a worker's message
(`Pool::want_message`) when no seat takes a ready compaction. The range then waits for that
message or a request.

The test `trunk/seat_wait.rs`:
- holds a pool's one seat with a job the trunk does not own;
- lets a branch of more rows than a leaf holds into the root, and takes one paced step;
- requires that the compaction is ready, none is out, and the trunk reports the wait.

It fails on the code before the change and passes on it. It then lets the holder go, sees the
compaction take the freed seat, drains, and reads every row back.

**After.**
- The one-worker test passed 300 of 300 runs under the same load.
- The instrumented build with the change passed the whole binary 200 of 200 times, with no
  passes counted without progress.

**Measured.** The change (x_seat) against its base (x_eff3) in the same round, 6 rounds each,
run7's order. Each cell is the median of the same-round ratios and the rounds the change won.
Worst put and CPU are lower-is-better; CPU is the run's user and system seconds.

| condition | fills | gets | seeks | worst put | CPU | runs |
|---|---|---|---|---|---|---|
| 10M, whole host | 1.07×, 4/6 | 1.15×, 5/6 | 1.03×, 4/6 | 1.01×, 3/6 | 0.92×, 5/6 | `r4001..r4006` |
| 10M, 12 busy loops | 1.12×, 3/6 | 1.01×, 3/6 | 1.08×, 6/6 | 0.63×, 4/6 | 0.98×, 4/6 | `r4031..r4036` |
| 10M, 2 CPUs, 2 loops | 1.04×, 4/6 | 1.00×, 3/6 | 0.99×, 3/6 | 1.05×, 2/6 | 0.99×, 5/6 | `r4011..r4016` |
| 100M, whole host | 1.03×, 4/6 | 1.04×, 5/6 | 1.05×, 4/6 | 0.48×, 4/6 | 0.97×, 4/6 | `r4021..r4026` |

- Under 12 loops the base's worst puts were 0.35-2.6 s and the change's 0.16-2.0 s.
- On two CPUs both arms' worst puts stayed at 1.5-1.9 s. That tail is the one worker seat two
  CPUs get, not the spin.
- On the whole host at 10M the change did the same work with 8% less CPU.
- At 100M the base's worst puts were 35-736 ms and the change's 9-594 ms.
## 7. 100M: the growth's zeros

**Traced** (`diag-runs/n100000000-r2901..r2903`, t_trmain, stalls.py):

| round | stalls | stall time | waiting on a growth |
|---|---|---|---|
| r2901 | 2 | 0.9 s | none: a packing at 12% CPU, 1.26 s |
| r2902 | 5 | 2.5 s | 4, all in the fill's first 3 s |
| r2903 | 10 | 6.0 s | 6 in the first 7 s, one of 4.5 s |

Each early growth wrote a 64 MiB segment's zeros in 0.24-1.39 s at 0% CPU while the fill wanted
about 500 MB/s; later, packings and compactions ran at 2-16% CPU beside a growth writing its
zeros (`r2941`, `r2951..r2952`). The zeros are a write of every new byte: 14.65 GB in a 100M fill
whose writes came to 44-46 GB.

**What they buy on Linux.** None of the transfers they were for. ext4's, XFS's and btrfs's
buffered writes take the inode's lock exclusively whether they overwrite or not; their direct
writes within the file take it shared whatever the extent state; a segment is allocated whole
(`fallocate(2)`), so no write into it extends the file (research/48 [ext4] [xfs] [btrfs]). What they save is the extents' conversion,
which the journal commits at the next flush, and the store flushes at its checkpoints.

**The change: on Linux a new segment is allocated whole and not written with zeros**
(`store::ZEROS_NEW_SEGMENTS`); on APFS and NTFS the zeros stay. Against main in the same round,
4 rounds each (`runs/n*-r3101..r3164`), beside it the zeros gone with the pool on every core
("both": cores-wr's `Pool::cores`, a variant not landed):

| condition | fills, zeros gone | worst fill, zeros gone | worst fill, both | both's worst / main's |
|---|---|---|---|---|
| 10M, whole host | 1.06×, 4/4 | 0.86×, 3/4 | 0.80×, 4/4 | 5.6 / 89 ms |
| 10M, 18 busy loops | 1.24×, 3/4 | 0.76×, 2/4 | 1.86×, 2/4 | 420 / 778 ms |
| two CPUs | 1.07×, 4/4 | 0.92×, 3/4 | 0.37×, 4/4 | 464 / 836 ms |
| two CPUs, 2 loops | 0.92×, 2/4 | 0.99×, 2/4 | 0.01×, 4/4 | 14 / 2,128 ms |
| one CPU | 1.11×, 3/4 | 0.37×, 4/4 | 0.76×, 4/4 | 49 / 111 ms |
| 100M | 1.00×, 2/4 | 1.26×, 1/4 | 0.72×, 2/4 | 78 / 587 ms |

[The confirmation on main f417e4c, traced and against main with RocksDB and SplinterDB beside, goes here.]
