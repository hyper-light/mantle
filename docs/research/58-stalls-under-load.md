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

The zeros are needed: a file's own first writes stall its other writers on ext4 as on APFS
(research/48 §4).

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

**Step two: is this much needed at once?** No. The root leaf is the one node whose settle every
packing waits on. Settled at one memtable's entries instead of fanout of them (x_rl1, rootleaf-wr
4a01df7, md5 af86913f), the root is an index node from the second packing, its first settle 1.07M
entries instead of 4.1M, and every packing after takes the root split.

Measured at 10M against the candidate, release binaries, run7.sh's balanced order, RocksDB and
SplinterDB beside them, 4 rounds a condition (`runs/n10000000-r2201..r2204`: 18 busy loops;
`r2211..r2214`: none; `r2221..r2224`: 12):

| arm, condition | fills | gets | seeks | fill max, median (worst) | stalls, median (worst) | entries written |
|---|---|---|---|---|---|---|
| candidate, none | 4.01M | 2.60M | 551K | 6.4 ms (30.8) | 1 (1) | 18.50M |
| root leaf, none | 4.23M | 2.46M | 561K | 2.7 ms (7.2) | 0 (0) | 16.72M |
| candidate, 12 loops | 2.12M | 1.90M | 385K | 1,475 ms (3,164) | 2 (3) | 17.51M |
| root leaf, 12 loops | 2.40M | 1.95M | 428K | 1,024 ms (1,204) | 2 (2) | 18.37M |
| candidate, 18 loops | 1.83M | 1.23M | 328K | 215 ms (541) | 1 (2) | 18.95M |
| root leaf, 18 loops | 1.67M | 924K | 330K | 97 ms (355) | 1 (1) | 17.46M |
| RocksDB 32 GiB, none / 12 / 18 | 726K / 459K / 279K | 213K / 143K / 87K | 102K / 67K / 48K | 3.4 / 27.1 / 32.4 ms | — | — |
| SplinterDB 32 GiB, none / 12 / 18 | 1.02M / 572K / 586K | 807K / 532K / 364K | 192K / 122K / 112K | 92.4 / 160.7 / 257.8 ms | — | — |

The gets differences above follow the order, not the build: the root leaf's two low-gets rounds
with no loops (r2212, r2214) each ran right after RocksDB's 32 GiB run, a carryover the Williams
order balances within a round but not across a round's boundary. With no foreign engine between
them (`r2261..r2266`, 6 rounds alternating, no loops), the root leaf's gets were 2.43M against
2.29M, seeks 562K against 519K, fills 3.87M against 3.90M, writes 16.72M against 18.23M, and
the most memory either held the same (4.21-4.30 GB against 4.26-4.32 GB).

**Reading.**
- With no loops, the root leaf removes the stall every candidate run had, and writes 8-10% less.
- Beside 18 loops, the worst fills halve.
- Beside 12 loops, both builds still stall for about a second (two stalls a run). The release
  binaries count stalls but do not time them. They are traced next, at 10M beside 12 loops and
  at 100M beside 18.

**What the root leaf leaves at 100M.** In the cut's trace at 100M (`diag-runs/n100000000-r1932`,
host load only), 1,312 trunk jobs had a median of 23 ms and a p90 of 127 ms. Ten were past 2M
entries:
- the root's compaction of the packings that had waited out the first settle, 5.1M, 2.3 s in;
- nine splits of first-level leaves, 3.9-4.5M entries each, 0.7-1.2 s, 14-19 s in, three of them
  at once.

A leaf's split holds its parent's pivot for as long as it runs: the pivot takes no parts and
plans no compaction, so the leaf's older data stays below everything newer in the parent. Each
packing meanwhile enters the root's in-flight list, and the root's room runs out once fanout of
them wait. The root leaf moves only the first of these settles.

## 5. What the root leaf leaves: a split holding a root pivot

Traced at 100M beside 18 busy loops (`diag-runs/n100000000-r1961..r1962`) and at 10M beside 12
(`r1971..r1973`), the candidate (t_trwr2), the root leaf (t_trrl1) and the root leaf with the
frozen allowance counting a packing's age (t_trrl2). These are diagnostic builds, and the
first 100M case, r1961 t_trrl2, ran beside a compile on the Mac:

| build | 100M, worst stall each round | 10M at 12 loops, worst each round | what the long ones waited on |
|---|---|---|---|
| candidate | 2,290 ms; 864 ms | 2,619; 6; 310 ms | the root leaf's first settle (jobs of 1.9-4.1 s at 24-45% CPU) |
| root leaf | 1,514 ms; 1,064 ms | 6; 1; 13 ms | a first-level leaf's split (jobs of 3.6-5.2 s) |
| root leaf, age allowance | 631 ms; 2,757 ms | 1,164 ms; none; none | first-level leaf splits (jobs of 1.3-5.9 s) |

Every stall of a second or more waited on one trunk job: a settle merging 3.5-7M entries on
one worker at 22-46% of a core. Each was a leaf's split under the root, which holds its pivot
there for the whole merge (§4), while every packing entered the root's in-flight list until
the root's room ran out. At 100M a packing came every 0.54 s, so a hold past fanout packings,
about 4.3 s, ran the room out.

**Step one: is the work needed?** Yes. The split's merge writes the leaves the data lives in.

**Step two: is this much needed at once?** Yes, as long as a split holds its parent's pivot.
The pivot is held for recency: a read checks a node's in-flight bundles, then its pivot
bundle, then the child, so the leaf's older data cannot sit above newer data in the parent's
bundle. Splitting by reference, as SplinterDB does (research/34 §1), would let the hold go,
but it shares branches between nodes, which a branch named by one node at a time does not.

**Step three: elsewhere, later?** A pivot that kept taking parts during the hold would grow
past fanout branches, which reads probe, or wait: the same stall.

**Step four: fewer resources?** No. Smaller leaves split sooner and smaller, but deepen the
trunk, a rewrite each level (research/57), which no change may add.

**Step five: spread.** The split's merge is cut into one job a part. The cuts are taken at the
separators of the largest input's leaf pages, where each `1/parts` of its entries begins:
its leaf index maps each separator to its page (`Trie::floor_by_value`), and its pages' entry
counts are in memory, so the cuts read nothing. The jobs run on the pool's workers at once, and
the settle applies when all are back, as the node's one change. The writes are the same
entries; the hold divides by the parts, about 8 for a leaf past `fanout` memtables.

**Measured at 100M beside 18 busy loops** (`runs/n100000000-r2331..r2334`, the same four builds,
release binaries, 4 rounds):

| arm | fills | gets | seeks | fill max, median (worst) | stalls, median (worst) | entries written |
|---|---|---|---|---|---|---|
| candidate | 1.29M | 620K | 160K | 1,073 ms (1,887) | 2 (4) | 298.6M |
| root leaf | 1.14M | 494K | 197K | 755 ms (3,156) | 2 (4) | 280.0M |
| split jobs | 1.19M | 635K | 158K | 280 ms (637) | 2 (2) | 286.5M |
| split jobs, one in flight | 1.21M | 569K | 181K | 89 ms (144) | 1 (3) | 289.7M |

**The split jobs made too many leaves.** Their splits wrote 2-3.5% more than the root leaf's:
- splits: 90-181 a run against 69-80;
- writes of size-caused settles: up to 66.2M against 37.3-41.1M.

A settle plans its parts on its count: the entries over every input, an upper bound that counts
the overwrites and tombstones its merge drops (at 100M a third of the puts overwrite). The one
job cuts its output by entries as it merges, so its leaves come out whole and fewer. The split
jobs made one leaf a planned part, smaller ones, which settled and split again. The jobs are
now as many as the leaves the output is expected to make. The trunk learns what share of their
count its settles' outputs keep (`Trunk::settle_output`): the entries every applied settle wrote,
over the entries it was planned on.

## 6. Open

- **Step one for the slow stages.**
  - Does the root's first compaction, which computed 1-1.2 s, need to merge that much while
    the root fills?
  - Does a packing need to fault 17,803 fresh pages?
- **Step two for the zeros.** Must they come as one 626 MiB burst beside the packing the writer
  waits on? A first writer in another file stalls no one (research/48 §4).
- **The cut under load.** 0.45× against the candidate's 0.69× at 12 loops, worse in 3 of 4
  rounds. That regression is unexplained, so the cut is not adopted (cache-wr's research/57 §8).
