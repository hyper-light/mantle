# 42 — The event loop under load: what a park, a kick and a clock read cost

**Status:** research input for `docs/design/event-loop.md` (mantle's decisions for its vendored
hyper-rt). This is not a decision record; the decisions it supports belong in the design.
**Compiled:** 2026-10-10.
**Scope:** what a cross-thread wake costs on the development machine at its ordinary load, in
latency and in CPU; what spin-then-block's threshold is in the literature and what hyper-rt sized it
by; how many kicks a parked shard took per park; what each macOS clock costs a read; how late a timed
wait runs; and what tokio and trantor do for each. Each measurement names the directory that holds its
raw output and the host load at its start and end.

---

## 0. How to read this note

**Citation tags.** `[KEY §section]` or `[KEY p.N]` for papers, `[KEY path:line]` for source files at
the revision in the Sources table. Measurement directories are under
`/Users/adalundhe/Projects/benchmark-results/` and are named in full once, then by their last part.

**Evidence labels.** **primary**: source, man pages, vendor documentation, read directly. *(no
label)*: a peer-reviewed paper, read in its scanned text. **MEASURED**: a value read on the development
machine on 2026-10-10. **DERIVED**: arithmetic on stated facts. **INFERENCE**: reasoning here that the
sources do not state.

**The machine.** Mac17,6, Apple M5 Max, 18 cores (6 + 12), macOS 26.4.1 (25E253). Load averages
during these measurements ran from 48 to 100; a virtual machine holds about 11 cores and other agents'
builds and tests run beside. This is its ordinary state and nothing here waited for a quieter one.

## Sources

| Key | Source |
|---|---|
| KARLIN91 | A. R. Karlin, K. Li, M. S. Manasse, S. Owicki, "Empirical Studies of Competitive Spinning for Shared-Memory Multiprocessors", Princeton CS-TR-319-91 (March 1991), published at SOSP 1991; read in the technical report's scan (pp. 1–6). |
| KMMO89 | A. R. Karlin, M. S. Manasse, L. A. McGeoch, S. Owicki, "Competitive randomized algorithms for non-uniform problems" (SODA 1990; Algorithmica 1994): the 2-competitive and e/(e−1) results KARLIN91 cites. Not read here; cited through KARLIN91. |
| TOKIO | tokio at `09a57c27` (1.53.2), `/Users/adalundhe/Projects/tokio`: `tokio/src/runtime/scheduler/multi_thread/park.rs`, `worker.rs`, `runtime/blocking/pool.rs`. **primary** |
| TRANTOR | drogon's event loop. The submodule `drogon/trantor` is not checked out on this machine (an empty directory), so no line of it is cited; §3 states what its design is said to be, marked UNVERIFIED. |
| MAN-CLOCK | macOS `clock_gettime(3)` (the clock ids and what each counts), `mach_time.h` (`mach_continuous_time`, `mach_absolute_time`). **primary** |
| MAN-KQUEUE | macOS `kqueue(2)`, `EVFILT_TIMER` and its `NOTE_CRITICAL`, `NOTE_LEEWAY`. **primary** |
| MS-QTCT | Microsoft Learn, "QueryThreadCycleTime" and "GetThreadTimes". **primary** (cited by hyper-rt's `thread_clock.rs`). |
| WAKELAT | `benchmark-results/wake-latency-c-20261010/` (a C dispatch-semaphore ping, no mantle code). |
| PARKCOST | `benchmark-results/hyper-rt-vs-tokio-20261010/os-parkcost/` (`parkcost.c`, `parkcv.c`, `clockcost.c`, `clockcost2.c`). |
| TIMERLAT | `benchmark-results/hyper-rt-vs-tokio-20261010/os-timer/` (`timerlat.c`). |
| PANELS | `benchmark-results/hyper-rt-vs-tokio-20261010/runs/` (the harness `harness/src/main.rs`; each run's raw output and host load). |
| GETPROF | `benchmark-results/mantle-resident-reads-smoke-20261010/reads3m-sample.txt` (a 5 s `sample` of mantle's resident get path). |

## 1. What a wake costs: latency and CPU are different quantities

**Latency.** A thread parked on a dispatch semaphore and signalled from another ran again after a
median of 7–58 µs, a p90 of 217–986 µs and a p99 of 3.0–4.4 ms (MEASURED, WAKELAT, load 76–82).
Declaring `QOS_CLASS_USER_INTERACTIVE` on both threads changed nothing consistently. The same holds
for hyper-rt's own mechanism, a kqueue `EVFILT_USER` trigger: five runs of 3,000 wakes measured a mean
of 360–448 µs, a median of 63–109 µs and a p99 of 3.4–4.3 ms (MEASURED, PARKCOST `parkcost`, load
62–72). The latency is the scheduler's queue: the woken thread waits for a core.

**CPU.** The same five runs read each thread's own CPU clock (`CLOCK_THREAD_CPUTIME_ID`) around its
half of the cycle: the waiter's park and return cost 3.03–3.13 µs, the waker's trigger 1.64–1.74 µs,
4.69–4.86 µs in all (MEASURED, PARKCOST `run1.txt`). Across those runs the mean wake moved by 24 % and
the cycle's CPU by 4 %. Per cycle the CPU's coefficient of variation is 0.34–0.38 (`parkcv.txt`).

**DERIVED.** At this load a wake's latency is about eighty times its CPU. Any rule that sizes a
CPU-spending wait by the latency spends eighty cycles' worth of CPU where blocking would have cost one.

## 2. Spin-then-block: the threshold is the cost of a context switch

KARLIN91 states the cost model directly. Blocking costs processor time, "consumed by software overhead,
which usually includes saving the thread's state, enqueuing the blocked thread, and scheduling a ready
thread on the current processor. In addition, the thread's state must be restored when it is awakened.
This time is the cost of a context-switch" [KARLIN91 pp. 1–2]. Spinning costs the processor time spent
probing. The regime the paper studies is the loaded one: "(a) there are other threads waiting for a
processor, and (b) the lock is held by a concurrently-running thread. Condition (a) tells us that
spinning would use valuable processor resources" [p. 2]. And: "if the lock is held by a thread which is
waiting for a processor, it makes little sense to spin" [p. 2].

The threshold: "Fixed-spin with SpinThreshold equal to C has a competitive ratio of 2", where C is "the
context switch time measured in spins"; if the wait t ≥ C, "the optimal off-line algorithm blocks
immediately, paying a cost of C, while fixed-spin spins for time C and then blocks, for a total cost of
2C" [p. 6]. With a known waiting-time distribution P, the best spin τ minimizes
`∫₀^τ t dP(t) + (τ + C) ∫_τ^∞ dP(t)` [p. 6]; adaptive strategies that learn from observed waits did
better than fixed ones in the paper's measurements [abstract].

**What hyper-rt did.** hyper-rt sized its idle spin by the measured mean wake, "the expected cost of
parking: the 2-competitive spin-then-park threshold [A: Karlin, Li, Manasse, Owicki, SOSP'91]"
(`machine/calibration.rs`), and refined it online from the kick-to-running latency of its parks
(`shard_loop.rs`, `note_wake`). That is the latency of §1, not the cost KARLIN91 names. **MEASURED**
(PANELS `base-f5d66a8-r1`, load 48–83, five interleaved rounds): consecutive processes calibrated spin
windows from 1.9 µs to 577 µs; a closed loop of one client thread against one shard task cost 31.3 µs
of CPU per request against tokio's 5.8 µs (current-thread flavor) at the same throughput (5,530 against
4,990 requests a second) and a similar p99 (3.9 ms against 3.4 ms); a client that paused 200 µs between
requests cost 41.4 µs of CPU per request against tokio's 17.9 µs, with every spin missing. The spin did
pay where the event's producer was itself running: two shards bouncing a value through two channels
completed 351,440 round trips a second against tokio's 9,852 (current-thread runtimes on two threads),
because each side's reply arrived within a microsecond while the other spun.

**INFERENCE.** Both observations are KARLIN91's two conditions: a spin pays when the producer is running
(the ping-pong) and cannot when the producer waits for a processor (a client thread woken by the OS).
Sizing the spin by C keeps the first and bounds the second's waste to C per idle period.

## 3. Kicks: one per park

hyper-rt's sender kicked a shard whenever it read the shard's park announcement (`parking.rs`,
`kick_if_parked` before 2026-10-10), so every sender that published while the shard slept made the
system call (`kevent` with `NOTE_TRIGGER` on macOS, an `eventfd` write on Linux). **MEASURED**
(PANELS `base-f5d66a8-r1`): eight client threads against one shard took 4.5 kicks per park (3,584
kicks for 789 parks), four blocking-pool callers 3.2.

tokio's worker parker claims the notification with one swap: `unpark` swaps the state to `NOTIFIED`
and makes the system call only when the swap returned `PARKED_DRIVER` or `PARKED_CONDVAR`; a second
unpark reads `NOTIFIED` and does nothing [TOKIO `scheduler/multi_thread/park.rs`, `Inner::unpark`].
tokio's current-thread scheduler, by contrast, calls `driver.unpark()` on every remote schedule.
trantor's `EventLoop::queueInLoop` is said to write its wakeup descriptor on every call made off the
loop thread or while the loop is not looping (UNVERIFIED: TRANTOR not available here).

## 4. Clocks: what a read costs on macOS

`CLOCK_MONOTONIC` on macOS "will continue to increment while the system is asleep" [MAN-CLOCK]; its
implementation is the wall clock less the boot time: GETPROF shows `clock_gettime` →
`_mach_boottime_usec` → `gettimeofday` → `__commpage_gettimeofday_internal`. **MEASURED** (PARKCOST
`clockcost2.txt`, best of five rounds of 200,000 reads, load 94):

| Clock | ns a read | Resolution (`clock_getres`) | Counts sleep |
|---|---|---|---|
| `clock_gettime(CLOCK_MONOTONIC)` | 16.1 | 1,000 ns | yes |
| `clock_gettime(CLOCK_MONOTONIC_RAW)` | 12.1 | 42 ns | yes [MAN-CLOCK] |
| `clock_gettime(CLOCK_UPTIME_RAW)` | 12.3 | 42 ns | no |
| `clock_gettime_nsec_np(CLOCK_MONOTONIC_RAW)` | 8.4 | 42 ns | yes |
| `mach_continuous_time` (ticks) | 4.8 | one tick, 41.67 ns | yes [mach_time.h] |
| `mach_absolute_time` (ticks) | 4.9 | one tick | no |
| `CLOCK_THREAD_CPUTIME_ID` | 106.8 (`clockcost.txt`) | — | — |

The timebase on Apple silicon is 125/3 nanoseconds a tick (`mach_timebase_info`). hyper-rt's shard read
`CLOCK_MONOTONIC` twice a poll; in GETPROF those two reads are 240 of the shard thread's 4,049 samples
(6 %). The per-poll cost showed in a yield-only workload: 47.5 ns a poll (PANELS `spinclock-4686b38-r1`,
`yield`, the base arm).

## 5. Timed waits run late by the scheduler's queue, whatever the mechanism

**MEASURED** (TIMERLAT `run1.txt`, 400 waits of 1 ms each, load 57–60): a kevent timeout ran a median of
587 µs late (p99 5.6 ms); an `EVFILT_TIMER` one-shot 863 µs; the same with `NOTE_CRITICAL`, which
"override[s] default power-saving techniques to more strictly respect the leeway value" [MAN-KQUEUE],
793 µs; with zero leeway through `kevent64` 1,238 µs; `nanosleep` 909 µs; and all of them again at
`QOS_CLASS_USER_INTERACTIVE`, 627–1,354 µs. No wait mechanism removes the lateness: it is the woken
thread's wait for a core. hyper-rt's own 1 ms sleeps ran a median of 941 µs late against tokio's
1,974–2,100 µs, whose timer wheel ticks in milliseconds (PANELS `base-f5d66a8-r1`, `timer`).

## 6. The blocking pool's extra hop

hyper-rt's pool hands a job from the submitter to a dispatcher thread and from it to an idle worker
(`blocking.rs`: `run` → the dispatcher's `jobs.recv` → a worker's one-job slot), two thread wakes before
the job runs. tokio's pool pushes the job under one mutex and signals one idle worker's condition
variable [TOKIO `runtime/blocking/pool.rs`, `spawn_task`]. **MEASURED** (PANELS `base-f5d66a8-r1`): the
median from submission to the job's start was 4.1 µs against tokio's 1.9 µs with one caller, and 13.9
µs against 5.9 µs with four.

## 7. What the changes measured

PANELS `spinclock-4686b38-r1` (load 57–89; five rounds, arms interleaved and rotated; the base arm is
`f5d66a8`, the changed arm `4686b38`, a commit object of the changed tree):

| Case | Base | Changed | tokio (current-thread) |
|---|---|---|---|
| yield, polls a second | 13.5 M | 55.8 M | 0.13 M |
| spawn and join, tasks a second | 12.9 M | 19.3 M | 0.59 M |
| two-shard ping-pong, round trips a second (p999) | 145 k (565 µs) | 678 k (78 µs) | 18 k (11 ms) |
| eight clients onto one shard, requests a second (one-way p99) | 26.2 k (3.7 ms) | 37.2 k (2.9 ms) | 23.6 k (2.9 ms) |
| a client pausing 200 µs, CPU a request | 65.5 µs | 30.3 µs | 22.2 µs |
| one client closed loop, requests a second (CPU a request) | 58.2 k (9.7 µs) | 63.7 k (10.9 µs) | 53.2 k (7.8 µs) |

The last row's base processes happened to calibrate short spins (5.3–14.2 µs) at that panel's lighter
load, so its CPU matched; at load 48–63 the same base spent 31.3 µs (§2). The changed arm learned
blocking's cost online at 2.0–13.2 µs across its processes, the shard's park and its sender's kick
together.

## What remains unknown

- **The cost of blocking on Windows.** Its per-thread times advance a scheduler tick at a time, and its
  per-thread cycle count is documented as not convertible to time [MS-QTCT]; a tracking shard there
  learns no cost and does not spin.
- **trantor's loop**, whose source is not on this machine (§3).
- **Johnson, Stoica, Ailamaki and Mowry**, "Decoupling contention management from scheduling" (ASPLOS
  2010), and **Lim and Agarwal** (ASPLOS 1994), which bear on spinning under load, were not read: the
  publisher refused the fetch. KARLIN91 states the regime and the threshold this note relies on.
- **Linux's numbers.** Every measurement here is macOS on Apple silicon; the decisions measure their
  quantities on the running machine, so they hold there by construction, but no Linux run is recorded.
