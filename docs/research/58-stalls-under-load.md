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
- **Step two for the zeros:** §7.
- **What a 100M fill's tail waits on without them:** §8.
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

**On main f417e4c.** Traced, 8 rounds each at 100M on the whole host (`diag-runs/n100000000-
r5001..r5008`, t_trm8, main; `r5011..r5018`, t_trz0m8, the zeros gone; jobs.py, stalls.py), a
job's time from its send to its result taken:

| job | main p50 / p99 / max | zeros gone p50 / p99 / max |
|---|---|---|
| growth | 25 / 1,359 / 1,862 ms | 5 / 1,400 / 1,660 ms |
| packing | 109 / 144 / 494 ms | 106 / 134 / 277 ms |
| compaction | 16 / 513 / 840 ms | 16 / 585 / 926 ms |
| index | 35 / 479 / 1,054 ms | 34 / 483 / 925 ms |

Main stalled twice in its 8 (114 and 283 ms), each a frozen memtable past the overlap whose
packing ran 379 and 494 ms, beside a growth writing its zeros (369 ms) in one and two compactions
(about 435 ms) in the other; the zeros gone, none stalled. The worst put, a round's, was 10-283 ms
on main (median of rounds 20.7 ms) and 8.9-43 ms without the zeros (11.3 ms).

Untraced, against main in the same rounds with RocksDB beside (`runs/n*-r4821..r4836`, 6 rounds a
size, run7's balanced order over the three; abtab.py, cmp3tab.py):

| zeros gone over main | 10M CPU | 10M rate | 100M CPU | 100M rate |
|---|---|---|---|---|
| fill | 0.93×, 6/6 | 1.08×, 6/6 | 1.00×, 3/6 | 1.00×, 4/6 |
| gets | 0.97×, 5/6 | 1.02×, 5/6 | 1.01×, 1/6 | 1.00×, 4/6 |
| seeks | 0.99×, 3/6 | 1.02×, 3/6 | 1.01×, 2/6 | 0.99×, 3/6 |

| a run's | main | zeros gone |
|---|---|---|
| bytes written, 10M | 6.1 GB | 2.0 GB |
| bytes written, 100M | 45.0 GB | 29.6 GB |
| stalls, 100M, 6 rounds | 3 (rounds' worst fill 7.2-60 ms) | 2 (13.6-357 ms) |

Against RocksDB the zeros gone are 5.22× its fills, 10.39× its gets and 5.42× its seeks at 10M and
4.91×, 8.09× and 5.24× at 100M, every round. A round's worst fill at 10M, the median of rounds, was
5.4 ms without the zeros, 5.5 ms on main and 6.1 ms for RocksDB; at 100M RocksDB's histogram
maximum wrapped in all 6 rounds, no measure. Without the zeros a 100M fill still stalls now and then (two
rounds of six, as main three): the zeros were one cause of the tail, not the last.

## 8. 100M without the zeros: what the tail waits on

**Traced** on the zeros-gone code, 8 rounds each at 100M on the whole host, two diagnostic builds
of it (never merged):
- spans over the shard's turn (`diag-runs/n100000000-r5031..r5038`, t_trz0t, trace-z0m8 b61beda;
  a span is one event at its end, kept when it ran 50 µs or more; spans.py);
- each worker job's waits for the issuer's answers and the runs it handed it (`r5051..r5058`,
  t_trz0w, 2f29d8b; events 43 and 44; issuerwaits.py).

**The shard's own work.** The 8 fills had 33 puts of 8 ms or more outside a stall. 32 waited on
the shard thread inside its turn's taking of the workers' results, on one of two pieces of
page-cache work done whole when a result came or a job went out: adopting a compaction's pages
(17 puts) or a grant forgetting its extents' cached pages (15). The other, 11.8 ms, was the put's
own operation, which froze the memtable and handed it off.

| a 100M fill, mean of 8 | spans of 50 µs or more | total | longest |
|---|---|---|---|
| adopting a compaction's pages | 1,209 | 687 ms | 17.3 ms |
| a grant forgetting its extents' cached pages | 1,133 | 527 ms | 15.1 ms |
| adopting a packing's pages | 161 | 171 ms | 3.2 ms |

Adopting took 77 ns a page and a grant 2.2 µs an extent (spans of 1 ms or more). A grant probed
16.6M pages a fill and found 5.8M cached. Adopting probed 12.1M addresses for a cached page first
and found none, the grant having forgotten them.

**Measured and not kept: extent churn** (churn-wr f8c8cc8, 60d20b0; traced `r5041..r5048`,
t_trchurn, trace-churn 0df7d0c). Free extents reused oldest first, a bit an extent marking whether
any of its pages may be cached, and no probe before an adopted page's insert:

| 8 traced 100M fills | zeros gone | churn |
|---|---|---|
| a grant's probes, a fill | 16.6M | 4.5M |
| a grant's time, a fill | 527 ms | 309 ms |
| one probe | 32 ns | 69 ns |
| adopting, a page | 77 ns | 96 ns |
| stalls | 3 (10, 56, 299 ms) | none |
| a round's worst put | 10.0-299 ms, median 13.8 | 9.5-35 ms, median 27.6 |

Oldest first, grants took the extents the paced forgetting had not reached: 4.49M of churn's 4.50M
probes found a page and released it, so a probe cost twice as much, and one grant of 5,424
extents took 27.2 ms against 8.3 ms. Adopting slowed without the probe before its insert, which
had likely brought the index's line in for it. In the rounds without a stall, a round's worst put
went from a median of 12.9 ms to 27.6 ms.

**The stalls.** A 100M fill still stalled in some rounds (§7), each a frozen memtable past the
overlap whose packing ran long. The long packings ran their usual CPU and waited the rest in one
wait for the issuer:

| packing | wall | CPU | waiting for the issuer |
|---|---|---|---|
| r5051 #461 | 440 ms | 113 ms | 326 ms, one wait |
| r5051 #547 | 402 ms | 114 ms | 287 ms, one wait |
| r5051 #462 | 325 ms | 112 ms | 211 ms, one wait |
| median of the 1,424 | 105 ms | 104 ms | none (80 waited at all) |

Beside each, two compactions had handed the issuer 3,984-4,223 runs each and ran 0.88-0.98 s;
three of those four waited on nothing. The VM's I/O pressure (some) was 53-65% across each long
packing, against 12% over the run (psi.txt). The issuer keeps one queue of transfers in the order
they came (hyper-block `issuer.rs`, `pending`). The packing's runs waited behind every compaction
run handed in before them. The trunk's tasks already follow SILK's first rule: the work clients
wait on is never queued behind compactions (Balmau et al., USENIX ATC 2019; research/34 §5;
engine-structure.md §6). The issuer's queue does not.

**Measured and not kept: the urgent class** (urgent-wr a62885b; traced twin trace-urgent e091a70).
A store's batches are marked urgent while its worker packs a memtable (`Attached::set_urgent`), and
the issuer dispatches an urgent batch's transfers before every transfer of a batch that is not. The
change is to hyper-block's issuer, so it would land through hyper-raft and a new snapshot. Traced
(`r5061..r5068`, t_trurg, against `r5051..r5058`):

| 8 traced 100M fills | zeros gone | urgent |
|---|---|---|
| a packing's issuer wait, p99 | 32.1 ms | 4.3 ms |
| a packing's issuer wait, longest | 326 ms | 200 ms |
| packings' issuer waits, total | 2,077 ms | 919 ms |
| stalls | 3 (211, 167, 7 ms) | 3 (75, 22, 5 ms) |
| a round's worst put | 9.3-211 ms | 8.9-75 ms |

Untraced, with RocksDB beside in run7's balanced order (`runs/n*-r4841..r4856`, x_z0m8 and x_urg,
6 rounds a size):

| | zeros gone | urgent |
|---|---|---|
| 10M, a round's worst fill | 5.1-6.1 ms | 5.3-6.4 ms |
| 100M, a round's worst fill | 15-73 ms | 7.7-802 ms |
| 100M, worst fill worse | | 4 of 6 rounds |
| 100M stalls, 6 rounds | 3 | 6 |

Rates and CPU a put were level: fills 0.99-1.00×, gets 1.00-1.02×, seeks 0.99-1.02× at both
sizes. In the traces the class did what it was for. Untraced, the 100M tail was worse in 4 of 6
rounds and the stalls doubled. A traced build's timing is not the release build's, and acceptance
comes only from binaries without diagnostic code, so the class is not kept.

**What remains.** With packings dispatched first, their longest waits were still 157-200 ms in one
or two waits each (r5061, r5063, r5065). Dispatched first, a packing's runs wait only for the
transfers already in the issuer's threads and for their own. Mantle's writes are buffered, and the
kernel pauses a writer over its dirty limits up to 200 ms at a time (`balance_dirty_pages`,
`MAX_PAUSE` = HZ/5; Linux v6.12 `mm/page-writeback.c`). These traces did not sample Dirty and
Writeback, so they cannot tell that throttle from the device's own queue. Two runs decide it: a
traced run that samples Dirty and Writeback beside the waits (inside-diag3.sh does), and the
direct-I/O A/B, whose writes dirty no page cache for the throttle to count.

**What the writes wait on.** Both runs are in, and neither is the throttle. Direct I/O
(`d_fill6`, fill6's files opened past the page cache, against the same build buffered,
`runs/n*-r5541..r5556`) slowed the fills and lengthened the tail:

| direct over buffered | fill | gets | a round's worst fill, direct | buffered |
|---|---|---|---|---|
| 10M | 0.79×, 0/6 | 0.98×, 2/6 | 6-1,228 ms | 5-13 ms |
| 100M | 0.83×, 2/6 | 0.94×, 3/6 | 299-1,520 ms | 19-155 ms |

Traced beside a 100M fill in a privileged container (landf 2d6504c with diagnostics, stallf
8c69059: each packing's sort, walk, finish, its thread's CPU and its write waits; `writeback:
balance_dirty_pages` pauses of 10 ms or more; bpftrace off-CPU sleeps of 10 ms or more of the
bench's threads with their kernel stacks, and `jbd2:jbd2_run_stats`; `native-ab/dirty/`,
`runs/n100000000-r5711..r5731`), no writer was throttled during any fill: Dirty peaked at 966K
pages (3.7 GB) against a throttle point near 1.7M, and the 21 pauses, 27-32 ms each, came in the
flush after the fill. The packings that ran long ran their usual CPU and waited once on their
writes, as before:

| packing | wall | CPU | write waits |
|---|---|---|---|
| r5712, the longest | 541 ms | 134 ms | 1, 407 ms |
| r5712 | 539 ms | 131 ms | 1, 408 ms |
| r5711, the longest | 444 ms | 136 ms | 1, 308 ms |
| median of 356 | 123 ms | 122 ms | none |

The device's threads (`hyper-io`) slept in ext4. A buffered write takes its file's inode lock
exclusively and updates the file's modification time; the update takes the inode's block in the
journal, and while a commit is writing that block the writer waits, holding the lock, and every
other thread writing the same segment waits for the lock (Linux v6.12 `fs/ext4/file.c`
`ext4_buffered_write_iter`, `fs/jbd2/transaction.c` `do_get_write_access`). The commits were slow
because their journal writes queued behind the data's writeback in the device's queue: one in the
flush logged 15 blocks in 416 ms. The volume is ext4 with `data=ordered`, `delalloc` and
`commit=5` (the VM's `/dev/vda1`).

| a 100M fill's sleeps of 10 ms or more, `hyper-io` (r5731) | sleeps | total | longest |
|---|---|---|---|
| the inode's lock in a buffered write | 27 | 1,737 ms | 88 ms |
| a timestamp's journal access | 9 | 681 ms | 88 ms |
| the descriptor table growing on an open | 13 | 146 ms | 12 ms |

A file growth adds a second wait. A segment is made whole by `fallocate`, `fdatasync` and the
directory's `fsync` (hyper-block `segmented.rs` `made_whole`), and each forces a commit. In r5721
a rotation stalled 720 ms with 13 of 17 workers idle: a growth was out, the store had fewer free
extents than the next packing asks, and the packing before it had waited 785 ms of its walk for
extents. Every stall in r5711 and r5712 also began with a growth out and ended once it was in.

**Measured and not kept: writeback started at each write** (wbhint ed6a92f, landf with
`sync_file_range(SYNC_FILE_RANGE_WRITE)` after every buffered write, as RocksDB's `bytes_per_sync`
does it; `runs/n*-r5741..r5756`, 6 rounds a size against landf):

| | 10M | 100M |
|---|---|---|
| fill | 0.83×, 0/6 | 0.87×, 0/6 |
| stalls a run | 0 | 8-13, against 0-1 |
| a round's worst fill | 4-16 ms, against 5-7 | 196-661 ms, against 19-413 |

The device's threads waited in the call instead, for room in the device's queue: at 100M this
disk does not take the fill's writes as they come, and the page cache holds them until after it.

**Measured and kept: each extent's cached pages counted** (`cache::Cache`, land9). A grant, the
paced forgetting of a freed extent and a flush skip an extent the count says holds no page, and an
adopted page's earlier slot is released by the index's own insert, one probe where a forget probed
first. Step timings of a 100M fill (land8 without them and with, `runs/n100000000-r6031..r6032`):

| a call | land8 | counted, one probe |
|---|---|---|
| a grant | 179-199 µs | 114-126 µs |
| an adoption | 331-365 µs | 362-369 µs |
| the puts' turn work, a put | 10.7-13.1 ns | 10.9-11.3 ns |

The counting itself cost adoption what its one probe saved; a grant's probes were the work kept off.
