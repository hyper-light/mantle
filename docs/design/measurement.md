# Measuring a point

Status: design, 2026-09-29; rounds, precision and counted rounds revised 2026-09-30; depth
without a thread per transfer, the device probes and load generation 2026-09-30. Sources:
docs/research/21 (benchmark states, cited as "21 §x"), docs/research/11 (operating parameters,
"11 §x"), docs/research/26 (the concurrency model, "26 §x"), 28 (storage classes and power), 29
(device classes); the measurements in docs/measurements/2026-09-29-chunk-store-states.md.

A benchmark point, such as puts of one chunk size at one number of requests in flight, is
measured in rounds, each giving one throughput. This record states how many rounds a point
runs, how its rounds are judged, and what its report states. `mantle_disk::rounds` implements
it, and `mantle bench chunk` reports through it.

## 1. Why a mean and its interval are not enough

On this machine a point's rounds are often not one distribution around one value. Reads at
one request in flight run at about 15,000 a second in some rounds and a tenth of that in
others, with almost nothing between: the drive answers a stream of full flushes by stalling
every read for about a second at a time, and a one-second round catches a stall or misses it
(measurements, findings 1 and 2). A mean of such rounds lies between the two states and
describes neither, and its interval can be narrow while the mean represents nothing (21 §6.1,
Hoefler and Belli's Fig. 3). The rounds also depend on each other: each round's puts change the
state the next round's reads start in, and the drive stays slow for tens of seconds after a
large write (measurements, finding 3). "If measurements are not i.i.d., the variance and
confidence interval estimates will be biased" (Kalibera and Jones, 21 §2.1).

## 2. Order in time

**Decision: before any interval is stated, a point's rounds are checked for order in time, and
rounds found ordered carry no interval.**

- **Two checks** (21 §4). The lag-1 autocorrelation against `±1.96/√n`, the bound for
  independent data in Le Boudec's §2.3.2 and the NIST/SEMATECH e-Handbook §1.3.3.1; and the
  number of runs above and below the median against its exact distribution, rejecting in either
  2.5% tail (e-Handbook §1.3.5.13; the distribution in 21 §4). Rounds equal to the median are
  left out of the runs.
- **Persistent or alternating.** Too few runs, or a correlation above the bound, means rounds
  stay in a state, as the drive does after a large write. Too many, or a correlation below it,
  means they alternate, as reads do when a stall recurs every few rounds; Kalibera and Jones
  name both patterns among the dependencies they found (21 §2.3).
- **From ten rounds.** Below ten the runs test has no two-sided 5% rejection region (21 §4), so
  fewer rounds are reported as untested.
- Two checks at 5% each mark at most about one point in ten whose rounds are in fact
  independent; the cost of a false mark is an interval not stated.

## 3. One state or two

**Decision: whether a point's rounds fall in one state or two is Hartigan and Hartigan's dip
test at the 5% level; two states are split where the within-group sum of squares is least.**

- **Why the dip** (21 §5). It is distribution-free, and the only test found with published null
  percentage points from four observations up: Hartigan and Hartigan's Table 1, from 9,999
  uniform samples for n = 4–10, 15, 20, 30, 50, 100 and 200, interpolated linearly on `√n·dip`
  between them as its note (4) directs. The alternatives fail at tens of rounds: the mixture
  likelihood ratio "does not provide a reliable method for detecting bimodality" below 50
  observations, the bimodality coefficient has no null distribution, and a comparison of one-
  and two-component Gaussian fits called 81% of unimodal samples bimodal (21 §5.5–§5.7).
- **The computation** is their §4 algorithm, steps (i)–(vii): the greatest convex minorant and
  least concave majorant of the empirical distribution over an interval that narrows until the
  gap between them is no larger than the distance already found. Tied values keep their own
  corners of the distribution function, so the jump at the mode may divide them, as the
  definition allows. It agrees exactly with R's `diptest` and with a linear program that
  minimizes the distance over unimodal distribution functions directly, on 5,000 samples of 1
  to 120 values, tied and untied; seventeen of them are the reference values of its tests.
- **What it can see.** A minority state is visible only when it holds more than twice the
  critical dip of the rounds: about 28% of ten rounds, 21% of twenty, 18% of thirty (21 §5.3).
- **The split** takes the cut between consecutive sorted values with the least within-group sum
  of squares, only between unequal values. No separation index is computed from it: splitting
  one normal distribution at its mean gives halves 2.65 standard deviations apart (21 §5.8).
- A third state is not looked for: tens of rounds cannot show one (21 §9.3).

## 4. Intervals

**Decision: each state is summarized by its median, with the order-statistic interval of Le
Boudec's Theorem 2.1, and by its share of the rounds, with the interval of his Theorem 2.4.**

- **The median, not the mean.** Throughput rounds are rarely normal (710 of 713 configurations
  in Maricq et al.; 21 §6.3), and Hoefler and Belli's Rule 6 asks for no assumption of normality
  without a check (21 §6.1). The median's interval needs only independent rounds.
- **The ranks** are equal-tailed: `j` is the largest rank whose tail below, `B(j−1)` with `B` the
  binomial distribution function at `p = 1/2`, holds at most 2.5%, and `k = n + 1 − j`, so the
  coverage `B(k−1) − B(j−1)` is at least 95%. Ten rounds give `[x_(2), x_(9)]`, Le Boudec's own
  example. Fewer than six rounds give none (his Table A.1), and such a state is reported with
  its median and range.
- **Shares** of two states are Clopper and Pearson's interval, his Theorem 2.4; 32 of 145 give
  15.6–29.7%, his Example 2.4. The interval does not allow for the split having been chosen
  from the same rounds, so it is narrower than it should be (21 §9.8).

## 5. How many rounds

**Decision: a point runs at least ten rounds and at most 28, and stops before 28 once its
rounds are independent and every state's interval lies within ±5% of its median.**

- **Ten** is the fewest at which order in time can be tested (§2).
- **28** is the fewest at which any second state the dip test can flag holds the six rounds its
  own median interval needs: tight states of minority share `p` are flagged at 5% only when
  `p > 2·D(n)` (21 §5.3), and the least whole count above `2·D(n)·n` is 5 at 27 rounds and 6 at
  28 (11 §20.4; `Policy::fewest_for_two_states`). The 30 this record first chose assumed a
  minority of a fifth. `--rounds` sets another limit, from ten to 120: `u128` holds the exact
  binomial sums of §2 and §4 up to 120.
- **±5%** is half of SNIA's 10% steady-state excursion (SNIA PTS 2.0.2 §2.1.24): intervals
  that wide part once two points differ by more than 10% of their mean, which is as fine a
  difference as a steady device lets a benchmark attribute to the point (11 §20.2). Stopping
  when an interval is narrow enough is justified only as intervals narrow (Chow and Robbins;
  21 §6.5), which is one more reason for the minimum.

## 5a. What a round is

**Decision: a round is a count of transfers, not a length of time.** A round of a point with
`N` in flight whose report states the `q` quantile runs
`max(⌈fewest(q) / 10⌉, ⌈N / (2 × 5%)⌉)` transfers: enough that the quantile has its 95%
order-statistic interval (Le Boudec, Theorem 2.1: 6 samples for a median, 368 for a p99,
3,688 for a p99.9) however soon the point stops, and enough that the round's last transfers,
which run with fewer than `N` in flight, cost it at most the precision (11 §20.3, §20.5). A
quantile whose state has fewer transfers than it needs is shown as a dash. Calibration first
runs a dimensioning round, not kept, whose transfers' coefficient of variation `cv` raises the
count to `(1.96·cv / 5%)²` where that is more (Jain's sample size for a mean within ±r%; 11
§20.5). The one-second
steps this replaced gave an 8 MiB put point a few hundred transfers and a 4 KiB read point
tens of thousands, whatever their quantiles needed. Work on one core (`bench ec`, `gateway`,
`hash`, `meta`) is timed the same way, one call discarded first (Georges et al., §4.1), each
round as many calls as keep the clock's measured step within the precision of the round.

## 6. What a point reports

For each operation of each point: a row for each state with its median throughput, the wider
side of the median's interval as a share of the median, its rounds out of the point's, the
share's interval when there are two states, and latency quantiles over the state's own
transfers; and, on the first row, whether the rounds persist or alternate. A state without an
interval shows a dash. No mean or coefficient of variation is reported (21 §9.6).

## 7. What a transfer is timed against

A measurement job's time runs from when its first transfer is issued to when its last
completes; starting the job's workers, and handing them their work, is outside it, and the
budget counts from the same start (audit P09). How a job keeps its depth in flight is §8.
Every block a write job sends differs from every other, a fresh random word at the head of each
of the device's blocks, stamped before the write is timed, so a device that
compresses or deduplicates is measured writing what mantle's data would make it write, and
making the payload is not charged to the device. Before, each worker wrote one buffer again
and again.

## 8. How a depth is kept in flight

**Decision: a measurement's depth is the number of transfers the device has in flight, kept by
the platform's asynchronous interface where it has one, and by a bounded, reusable pool of
blocking workers where it does not; never by a thread started per transfer.** A job ran as many
threads as its depth, held them at a latch woken by `notify_all`, and a calibration ladder
doubled the depth until the operating system refused a thread. On macOS a Rust `Condvar` is a
psynch condition variable, whose broadcast walks every waiter inside one kernel spinlock held
with preemption disabled; thousands of waiters on one such object, re-broadcast for every answer
the log's writer gave, held that lock past the kernel's timeout, and the machine panicked five
times (26 §1.1–§1.4). The number of threads an OS will create (16,384 in one task here) is not
the limit that matters; any operation whose kernel cost grows with the waiters on one object is
(26 §1.4).

| Platform | Mechanism | Threads a job | Depth bound |
|---|---|---|---|
| Linux with io_uring usable | one ring per job, `O_DIRECT`, registered buffers and file; durable writes linked to `IORING_OP_FSYNC` with `IORING_FSYNC_DATASYNC` | 1, plus io-wq's own, which the kernel bounds at `min(ring entries, 4 × CPUs)` | min(the device's reported queue, the knee, `IORING_MAX_ENTRIES`) |
| Windows | overlapped `ReadFile`/`WriteFile` with `FILE_FLAG_NO_BUFFERING` on a file written to its full length first, so no write extends it; one completion port reaped with `GetQueuedCompletionStatusEx` | 1 | min(device queue, knee); a call that completes synchronously is counted as such |
| macOS; Linux where io_uring is disabled or absent | blocking `pread`/`pwrite` (and `F_FULLFSYNC`, `fdatasync`) on a pool of exactly `depth` workers, each with its own job slot and completion slot, started once per calibration and reused across points | = depth | min(device queue, knee, the process thread budget) |

(26 §2.2–§2.6, recommendation 3.) macOS has no other choice: its POSIX AIO is a kernel pool
capped at 16 requests a process that cannot carry `F_FULLFSYNC`, kqueue refuses user
registrations of `EVFILT_AIO`, and dispatch_io serializes a device (26 §2.4). A worker is handed
its job through its own slot and woken alone (node.md §1.3); no latch is needed, because a
worker that has not received a job does nothing, and a pool that cannot start every worker it
needs starts no job and returns the error. The pool's size is decided before any thread starts:
the smaller of the device's reported queue, the knee the ladder measured, and the process's
thread budget (node.md §1.2), and a request past the budget is the typed refusal
`DiskError::Threads`, naming the device and depth, before any thread exists (26 recommendation 4).
Thread start-up, about 90 µs a thread in Apple's figure, is paid once per calibration and never
inside a timed interval. The pool starts its workers as the ladder deepens, each drawn from the
budget before it starts, so it never holds more than the deepest step measured
(`mantle_disk::measure::Pool`).

**The depth achieved is measured, not assumed.** Each completion samples the number in flight,
and a point reports the achieved depth's distribution beside the depth asked for, as fio does
("Keep an eye on the I/O depth distribution ... to verify that the achieved depth is as
expected", 26 §2.1). On Windows an overlapped handle "can also behave synchronously"; the
shortfall is reported, not hidden (26 §2.3).

## 9. What calibration measures of a device

Each probe runs on calibration's scratch file, or a raw device's scratch region, through §8's
mechanisms and depth bounds, in rounds judged as §2–§5 judge them, and states what it costs the
device (research/29 §10). Together they give the device plan of chunk-store.md §2.1.

| # | Measurement | Method | Device cost | When |
|---|---|---|---|---|
| C1 | Reported units and features | `minimum_io_size`, `optimal_io_size`, `discard_granularity`, `discard_max_bytes`, `max_write_streams`, `write_stream_granularity`, `zoned`, `write_cache`, `fua`, `max_hw_sectors_kb` on Linux; the IOKit and storage-IOCTL equivalents research/02 names | none | every open |
| C2 | The flush with and without dirty data | a durable write of `B` then a flush, against a flush with nothing written since the last, in rounds until the intervals separate or overlap; equal costs mark a drive whose flush is free (chunk-store.md §4) | `B` a round | format and calibrate |
| C3 | The write unit where none is reported | durable aligned random writes of 4, 8, 16, 32 and 64 KiB at depth one within a few units' region; the unit is the smallest size above which throughput per byte stops rising | a few MiB | format: SATA, USB, SD, cloud, NVMe without NPWG, macOS |
| C4 | Reads beside writes | the read ladder's knee repeated while a sequential write stream runs at the writer's batch size | a batch's bytes a round | calibrate |
| C5 | The exit from idle | one read after idle gaps of 50 ms, 200 ms, 1 s and 3 s; a jump marks a power-state exit; on a disk at most a few gaps past the head-unload timer, each counted against the load/unload budget | reads only | calibrate, on mains |
| C6 | A disk's positioning | depth-one random reads at controlled distances, fit to settle plus √d for short seeks and a + b·d for long (research/29 §5.1), with research/10 R12's zone profile | reads only | calibrate, rotational devices |
| C7 | A disk's commit placement | commit latency with the frame in the index log against the index log on flash (chunk-store.md §2) | a few batches | calibrate, rotational devices |
| C8 | Discard | the time to discard a freed segment and the read latency after it; on a file volume, the first-write penalty already measured (chunk-store.md §2) | one segment | calibrate |
| C9 | Burst against sustained write rate | a change point in the writer's throughput per burst, tagged with the volume's fullness, since a pSLC cache shrinks as the drive fills (research/29 §3.2) | none | always, passively |
| C10 | Device write amplification | Δ media units over Δ data units from the Endurance Group log | none | periodically, where readable |
| C11 | Workload and wear counters | lifetime reads, writes and power-on hours on disks; load/unload count; Percentage Used on flash | none | periodically, where readable |
| C12 | Device power | NVMe Interval Power Measurement and Operational Lifetime Energy Consumed, where the drive implements them | none | periodically, where readable |
| C13 | Differential energy | the same work with and without one component (flush cadence, request size), its power the difference of two runs over the battery's discharge or RAPL plus platform power (Zedlewski et al.; research/28 §8) | the workload's bytes | on request only |
| C14 | Striping chunk and interleave | Chen et al.'s paired-write latency probe | small writes | on request only |

Probes that write until a cache is exhausted, the pSLC cliff and drive-managed SMR's media
cache, stay in the explicit calibrate mode research/02 §6.3 restricts them to. Calibration and
benchmarks are refused on battery unless the operator forces them, since what they would
measure is a power-managed device (research/28 §4.5); C13 is never run below a stated charge.
A device the OS cannot describe gets these measurements and the conservative defaults of
CLAUDE.md §5.

**Energy, where the device measures it, and otherwise only on request.** The only per-device
energy meter a process can read is NVMe's power measurement, where present (C12). Elsewhere
energy is attributed to mantle's I/O by C13's differential method, against a system meter, with
the same rounds and intervals as throughput; the battery gauge's update period and resolution
are measured first by holding a known CPU load (research/28 §8). C13's points are 4 KiB, 64 KiB,
1 MiB and 8 MiB client writes at one to sixteen writers, under the mains wait rule and the
battery one (chunk-store.md §4), reported as joules per acknowledged byte and per flush.

## 10. Load generation

**Decision: a benchmark's logical clients are records multiplexed on at most as many driver
threads as the benchmark is granted cores, closed loop only where the system is closed, and open
loop with latency from each request's intended start everywhere else** (26 §3.4,
recommendations 5–6).

- **A client is a record, not a thread.** A replica in `bench log` is its group, next index,
  intended start and the `Pending` it holds; a client of `bench` or `bench gateway` the same. A
  completion's `Waker` pushes the client's index onto its driver's ready queue, bounded by the
  client count, and unparks the driver once, so a completion costs one push and at most one
  wake, never a scan of every client (26 §3.4 item 1). Every tool that scales to many clients
  keeps them as data: wrk's connections per thread, memtier's clients per thread, db_bench's
  coroutine jobs (26 §3.3).
- **Closed where the system is closed.** A Raft replica waits for its update before its next
  `Ready`, so the replica ladder is closed loop and reports its multiprogramming level. Agents
  against a gateway are independent: open or partly open with few requests a session, where a
  closed benchmark understates response time by up to an order of magnitude ("Principle (i)",
  26 §3.1). The gateway's and chunk store's client benchmarks are open loop, with arrivals from a
  stated process, Poisson unless the run names another, and latency measured from when each
  request should have been sent, which removes coordinated omission (26 §3.2).
- **The ladder's end is stated.** The replica ladder doubles while appends a second grow, up to
  the replicas a node hosts, a bound derived from the idle engine instance's cost (node.md §11),
  never until the operating system refuses a thread (26 §3.4 item 4).
- **The generator reports itself.** Each driver reports its own CPU time and, in open loop, its
  lateness, intended against actual submit time, so a run in which the generator was the
  bottleneck shows it; at the region and fleet steps the generator runs as several processes,
  each reporting the same (26 §7).
- **The cost of a client is measured.** Benchmarks report resident memory over client count at
  two counts, which separates the fixed cost from the slope `S`, bytes per idle and per active
  client, task or connection; a node holds at most `M / S` clients for its memory budget `M`
  (26 §6, recommendation 9). A claim of how many agents a node serves names the run that measured
  `S`.

## 11. Open

- **Where states change in time.** Rounds found ordered are reported without an interval; the
  rounds at which a state begins and ends are not located. Changepoint segmentation, as Barrett
  et al. classify warmup, needs a penalty calibrated on the device (21 §3, §9.3 step 2).
- **Repetition above the round.** Kalibera and Jones set the repetitions at each level from a
  dimensioning experiment, and an interval covers only the variation of the levels repeated
  (21 §2.4–§2.5). One run of four fresh volumes of twenty rounds each, in one process, gives by
  their Eq. 3 an optimum of 2 to 11 rounds a volume for 13 of 18 point-operations, and no bound
  for the other 5, whose variance between volumes estimates at or below zero. But 30 of its 72
  series of twenty rounds are ordered in time, where independent rounds would give at most
  about seven,
  and the method requires independent rounds, so the counts are recorded rather than adopted
  (measurements, finding 4). A report from one volume says nothing of the variation
  between volumes, which for some points is as large as that between rounds. The full
  experiment, across processes, is about eight hours of writing per device (21 §9.2).
- **Calibration** still judges its points by the mean's t-interval, over two to six rounds of
  counted transfers: two, the fewest with a t-interval, and six, the fewest whose range is a
  95% interval of the median (11 §20.4); the judgment here needs ten rounds a point.
- **The workload measured** (audit P09). §8 decides the backends; C4 measures reads beside
  writes; a cleaner or scrubber beside the foreground, and a device near full, are not yet
  measured, and the background budgets that should follow from them are not derived yet.
- **IoRing on Windows 11** as an alternative to completion ports (26 §10), not evaluated.
- **What Apple's SSDs expose**: power states, NPWG-like units, and whether macOS's power
  management is observable from user space (research/29 §11 items 6, 12); C3 and C5 measure
  around the gap.
