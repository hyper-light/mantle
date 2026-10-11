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
| VYUKOV | Dmitry Vyukov, "Bounded MPMC queue", 1024cores.net: a slot per position with a sequence stamp, claims by compare-and-swap on the head and the tail. **primary** |
| STDMPMC | Rust std as shipped with 1.94.1 and nightly 2026-09-04 (the same code; 1.98.0's toolchain here carries no source), `library/std/src/sync/mpmc/array.rs` (the array channel: lap-encoded positions, `SeqCst` fences in its full and empty checks, a spin on a slot another end has claimed) and `waker.rs` (`SyncWaker`: blocked senders and receivers register under a `Mutex`). **primary** |
| LOOM | loom 0.7.2, `src/rt/execution.rs` (`schedule`: the reduction's dependency check), `src/rt/atomic.rs` (`last_dependent_access`: an object's last access only), `src/rt/thread.rs` (`set_unparked`: an unpark makes any blocked thread runnable). **primary** |
| FILLPROF | `benchmark-results/mantle-boxed-request-ab-20261010/fill5m-sample.txt`: mantle's fill under `sample`, with `SyncWaker::notify` (`waker.rs:172–183`) and `semaphore_signal_trap` under it. |
| PARKCOST | `benchmark-results/hyper-rt-vs-tokio-20261010/os-parkcost/` (`parkcost.c`, `parkcv.c`, `clockcost.c`, `clockcost2.c`). |
| TIMERLAT | `benchmark-results/hyper-rt-vs-tokio-20261010/os-timer/` (`timerlat.c`). |
| PANELS | `benchmark-results/hyper-rt-vs-tokio-20261010/runs/` (the harness `harness/src/main.rs`; each run's raw output and host load). |
| GETPROF | `benchmark-results/mantle-resident-reads-smoke-20261010/reads3m-sample.txt` (a 5 s `sample` of mantle's resident get path). |
| PRCTL | Linux man-pages, `prctl(2)`, `PR_SET_TIMERSLACK` (the page as Debian bookworm ships it, manpages.debian.org/bookworm/manpages-dev/prctl.2.en.html, read 2026-10-10). **primary** |
| JACOBSON88 | V. Jacobson, M. J. Karels, "Congestion Avoidance and Control", revised November 1988 from the SIGCOMM '88 paper (ee.lbl.gov/papers/congavoid.pdf), §1 "Getting to Equilibrium: Slow-start", p. 4; §2 "Conservation at equilibrium: round-trip timing", p. 7 (a retransmit's backoff: "only one scheme has any hope of working—exponential backoff"). |
| TOKIO-YIELD | tokio at `09a57c27`, `tokio/src/task/yield_now.rs:48-55` (`yield_now` hands its waker to `context::defer`). **primary** |
| LIM-AGARWAL | B.-H. Lim, A. Agarwal, "Waiting Algorithms for Synchronization in Large-Scale Multiprocessors", MIT/LCS/TR-498, which states it appears in ACM Transactions on Computer Systems 11(3), August 1993; read in the report's text (§3 p. 7, §4 pp. 9–10). |
| BOGUSLAVSKY93 | L. Boguslavsky, K. Harzallah, A. Kreinen, K. Sevcik, A. Vainshtein, "Optimal Strategies for Spinning and Blocking", Technical Report CSRI-278, Computer Systems Research Institute, University of Toronto, January 1993; read in the report's text (§3, pp. 9–15). |
| EPOLL-WAIT | Linux man-pages 6.9.1, `epoll_wait(2)` (2024-05-02), as Debian ships it (`manpages-dev`), saved in `benchmark-results/hyper-rt-vs-tokio-20261010/man/epoll_wait.2.txt`. **primary** |
| TIMERFD | Linux man-pages 6.9.1, `timerfd_create(2)` (2024-06-15), saved beside it as `timerfd_create.2.txt`. **primary** |
| RUSTIX | rustix 1.1.5 as the crate builds it, `src/timespec.rs`, `src/backend/linux_raw/event/syscalls.rs`. **primary** |
| PANELS-LINUX | `benchmark-results/hyper-rt-vs-tokio-20261010/runs-linux/` (`linux.sh`, `spin-linux.sh`, `spin-linux2.sh`): the harness in a Linux container on this machine's Docker virtual machine (aarch64, kernel 6.12.76-linuxkit), pinned to the CPUs each panel names, with a memory limit and with or without two busy loops beside it; each run's raw output, its CPU mask and the host load. |

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

## 8. Channels without a lock

hyper-rt's channel and one-shot carried their values through std's `sync_channel`, and a plain thread
blocked in `blocking_recv` or `blocking_send` waited inside it. std's array channel registers a blocked
end in `SyncWaker`, a waiter list behind a `Mutex`, and every send to a channel with a blocked receiver
takes that mutex to notify it [STDMPMC `waker.rs`, `SyncWaker::notify`]; mantle's fill spent its samples
there, in `SyncWaker::notify` and the `semaphore_signal_trap` it ends in [FILLPROF]. The async senders'
queue of waiters for room was a second `sync_channel`, whose entries stayed until the receiver's grants
walked past them, so a sender that stopped waiting still held a place. **primary**

What replaced it, with no lock on any path:

- **The values** go through Vyukov's bounded queue [VYUKOV], with positions a lap above an index as std
  encodes them [STDMPMC `array.rs`], so the capacity is exact. Where std spins on a slot another end has
  claimed and not finished, without bound [STDMPMC `array.rs`, `start_send`/`start_recv`], this one reports
  the slot full or empty and lets the waiting protocol carry progress, so no end waits on a preempted peer.
- **A waiting receiver**, task or thread, registers in its cell's waiter word, fences, and looks again; a
  sender publishes, fences, and takes the word (`crate::handoff`, the parking protocol's store-buffering
  argument, §3 and `parking.rs`). A thread registers its handle's address and parks; the publisher that
  takes it marks the word claimed until it has read the handle, and the thread does not leave its wait
  while the word reads claimed.
- **Senders waiting for room** hold one of `waiters` places, freed in place when a sender stops waiting,
  so the bound is the senders waiting now. Each take grants one waiting place, searching from where the
  last grant stopped. The receiver fences after each take and reads one counter of waiting places.

**The models.** loom checks the ring, the thread handoff and the room under the workspace's bounds, each
with mutations that must fail it (`tests/loom-channel.txt`). Writing them found four things:

1. A first ring popped a value with no consumer thread of its own: loom's reduction remembers an object's
   last access only [LOOM `atomic.rs`], so a pop made on the main thread before the producers ran was never
   reordered after a push, and no interleaving popped a published value. Every model now gives each side
   its own thread.
2. loom's `unpark` makes any blocked thread runnable [LOOM `thread.rs`], a thread blocked in a join
   included, which loom's join then rejects; a spurious wake is modelled as a flag a third thread raises.
3. A ring push that read a slot's stamp two laps old after reading the latest tail re-read the tail
   forever: nothing bounds how long a plain load may return an old value. The ring reads the latest tail
   or head once and then reports full or empty; the waiting protocols fence before their second look.
4. A grant could reach the place of a sender that had just sent on room made earlier and not yet let its
   place go; that sender kept the grant, and the other waiting sender slept with room free (deadlock at
   interleaving 3,776 of the two-sender model). A sender now hands on any grant it did not see before it
   sent.

**MEASURED** (PANELS `chan-98efdf7-r1`, load 15–21, five rounds, arms interleaved; plain threads; `98efdf7`
is the changed tree, `bfdf9c2` the tree before it; per value or round trip):

| Case | hyper-rt before | hyper-rt now | std `sync_channel` | tokio `mpsc` |
|---|---|---|---|---|
| one producer, capacity 1,024: values a second (CPU) | 29.7 M (49.6 ns) | 144.4 M (13.4 ns) | 101.7 M (18.5 ns) | 8.0 M (199 ns) |
| four producers, capacity 64 | 4.05 M (536 ns) | 5.33 M (389 ns) | 4.21 M (444 ns) | 4.07 M (961 ns) |
| sixteen producers, capacity 64 (context switches a value) | 407 k (8.8 µs, 0.75) | 674 k (2.7 µs, 0.36) | 429 k (7.3 µs, 0.61) | 341 k (7.4 µs, 0.79) |
| four producers, capacity 1 | 170 k (5.3 µs) | 179 k (5.1 µs) | 189 k (2.8 µs) | 154 k (5.9 µs) |
| two threads ping-pong, round trips a second (p50) | 134 k (2.9 µs) | 135 k (3.0 µs) | 130 k (3.5 µs) | 109 k (4.7 µs) |

Where a value waits for no one, the lock-free ring is the difference (the one-producer row); where sixteen
producers contend, the mutex's waits are (a third of the CPU, half the context switches). Where every value
blocks a thread (the last two rows), each value costs a park and an unpark, the same system calls whatever
the queue. std's blocking `send` and `recv` park at once, with no spin before [STDMPMC `array.rs`, `send`
and `recv` into `Context::wait_until`], but a send or receive that meets a slot the other end has claimed
and not finished spins, then yields, until it is finished, with no bound [STDMPMC `array.rs`,
`start_send`/`start_recv`: `spin_light`, then `spin_heavy`'s `yield_now`]. In the capacity-1 fan-in a
sender meeting the receiver mid-take waits that out instead of parking, which is why std spends half the
CPU there.

**MEASURED** again at a far heavier load (PANELS `fence-cost-r1`, load 96–110 from other sessions' builds,
seven rounds): one producer 46.8 M values a second against std's 20.7 M, sixteen producers 1.25 M against
677 k, four producers 1.16 M against 1.23 M at 106 against 122 ns of CPU a value. The same panel ran two
variants of the channel to price its fences: with both removed (`nofence`, no longer correct) and with
sequentially consistent operations in their place (`seqcst`): neither moved the CPU a value outside the
rounds' spread (106–111 ns, 12–14 ns), so the fences, which loom checks, stay.

**The receiver's close.** mantle's suite caught a send that succeeded into a channel whose receiver had gone
(`tests/sync.rs`, round 2,380 of 3,000, `tests/suite-spin-policy.txt`). The receiver's drop set a flag,
drained the ring and woke the waiting senders; a sender that had read the flag clear and found the ring full
could push into the room the drain made. std puts its disconnect in the tail: `disconnect_receivers` sets a
mark bit with one `fetch_or`, `start_send` refuses a marked tail before it claims, and a claim's
compare-and-swap fails on a tail the mark has changed, so a send and the disconnect are ordered by one word
[STDMPMC `array.rs:129–134`, `470–481`]; `discard_all_messages` then waits, spinning and then yielding, for
each position claimed before the mark to be published [STDMPMC `array.rs:495–539`]. **primary** hyper-rt's
ring takes the mark and not the wait: a sender whose push succeeded reads the mark after the `SeqCst` fence
its wake already makes (`handoff::take`) and drops what the ring holds if it is set, and the receiver fences
between its close and its drain, so whichever fence comes first, the later side sees the other's write (§3's
argument): the drain sees the value published, or the sender sees the mark. loom checks the protocol
(`tests/loom-ring-close.txt`), and the three mutations that must fail it do
(`tests/loom-ring-close-mutations.txt`).

**MEASURED** (PANELS `chan-ringclose-spsc-r3`, load 10, nine rounds, one producer): the send without its
read of the receiver's flag before the push cost 13.2 ns of CPU a value against 11.1 before the close, and
with the read kept 11.3: the read brings in the cell that the wake reads after its fence, which otherwise
waits for those loads. With the post-push check of the mark removed and the read not kept, 15.7 against
13.3 (`chan-ringclose-spsc-r2`, load 14): the check was not the cost.

**MEASURED** (PANELS `chan-ringclose-r4`, load 10–12, seven rounds, arms interleaved; median CPU a value or
round trip, before the close and with it): one producer 13.4 and 11.5 ns, four producers on capacity 64
514 and 528 ns, four on capacity 1 4,897 and 4,912 ns, the two-thread ping-pong 4,749 and 4,803 ns, each pair
inside the other's rounds. Sixteen producers on capacity 64 split between streaming and parking from round
to round in both (53 ns to 7.5 µs a value before, 99 ns to 6.9 µs with the close), so its medians say
nothing either way.

## 9. A clock read a poll

**MEASURED** (PANELS `spawnd-profile`, `sample` at 1 ms over a shard running a million detached spawns
at hyper-rt `bd7c0fd` with its fixed configuration): of the 2,915 samples in the shard's step, 1,147 were
in `poll_slot`, and 1,134 of those were the clock read at the poll's end (`KqueueDriver::now_ns` →
`shard_clock_ns` → `mach_continuous_time`, 1,091 in the call itself). A poll that spawns nothing and ends
costs a few tens of nanoseconds, so a read that costs 4.8 ns in a tight loop (§4) is a third of it where
the poll does little; **INFERENCE**: in the step's own instruction stream the read also waits on the
`isb` that `mach_continuous_time` issues before it reads the counter, which a loop of reads alone hides.
The read served the watchdog (each poll's length against the quantum, and its attribution), the next
poll's start, and an activity window's origin. A step's own two readings serve each of them for the step:
the watchdog asks whether the shard's other work was held back, which is a step's length, not a poll's.

**MEASURED** with the read removed (PANELS `mac-b-step-r1`, load 12–15, five rounds, arms rotated; base
`bd7c0fd`, changed the step-timing tree), per second:

| Case | Base | Changed | trantor |
|---|---|---|---|
| detached spawns | 22.8 M | 25.8 M | 37.5 M |
| yields | 46.9 M | 66.8 M | 36.2 M |
| two-shard ping-pong round trips | 1.07 M | 1.30 M | 0.15 M |
| one client closed loop, requests | 223 k | 215 k | 168 k |
| eight clients onto one shard, requests | 388 k | 383 k | 597 k |

**MEASURED** on mantle's fill (`benchmark-results/rtloop-async-fill-bisect-20261010/b-step-r1`, eight
interleaved rounds each, load 11.2–11.7): 1M random puts through the same-shard client at a median of
2.96 M a second against 2.55 M (`bd7c0fd`'s runtime), gets 2.15 M against 2.12 M, seeks 536 k against 534 k.

## 10. The timer slack: what the kernel grants a timed wait

**primary** [PRCTL] `PR_SET_TIMERSLACK`: every thread has a current timer slack, and the kernel groups
nearby timer expirations by it, so a timer "may be up to the specified number of nanoseconds late (but will
never expire early)". "The timer slack values of init (PID 1), the ancestor of all processes, are 50,000
nanoseconds (50 microseconds)", a new thread starts with its creator's value, real-time threads get none, and
"the timer expirations affected by timer slack are those set by select(2), pselect(2), poll(2), ppoll(2),
epoll_wait(2), epoll_pwait(2), clock_nanosleep(2), nanosleep(2), and futex(2)" and the library calls built on
futexes.

So on Linux a shard's timed park (`epoll_wait` with a timeout) may end up to 50 µs past its deadline by the
kernel's choice alone, and so may every timed wait of every ordinary thread on the host. **INFERENCE**: a
timing wheel whose tick is finer than that resolves deadlines the park does not honor more finely, and a busy
shard that looks at its timers, inboxes and driver once in that span adds no more lateness than the kernel's
own coalescing already allows. macOS and Windows coalesce timed waits under their own policies (MAN-KQUEUE's
`NOTE_LEEWAY` is the caller's; the default leeway is not documented there), so the value is Linux's, the one
a primary source states.

The wake probe set the same constants before (tick, step budget, the poll batch's budget, the control
channel's depth) and is the noisiest of the measurements here (§1): in a one-CPU Linux container its mean was
565 ns, making a 565 ns tick and quantum, a poll batch of 4–8 and a control depth of 4
(`benchmark-results/hyper-rt-vs-tokio-20261010/runs-linux/c1-quiet-bd7c0fd`).

## 11. A step's allowance of polls: what fills the budget, opened by slow start

A step polled a fixed count (`batch`): the latency budget over the cost of one trivial poll, measured once at
start on a simulated shard. Real polls are not trivial. **MEASURED** (PANELS `mac-a1-r1`, case `rttbusy`,
load 38–60, five rounds): a task computing in 50 µs slices and yielding between them beside a server, and a
client's request through a channel to the server, closed loop. At hyper-rt `bd7c0fd` (a calibrated count of
28) the request waited 803 µs at the median, 570 requests a second: the shard polled the slicing task up to
28 times before it next drained its foreign wakes. With the allowance below, 125 µs and 5,827 a second;
trantor answers in one slice, 52 µs and 11,416 a second (and tokio, PANELS `mac-ad-r1`, 55 µs).

**primary** [JACOBSON88 §1, p. 4] slow-start: "When starting or restarting after a loss, set cwnd to one
packet. On each ack for new data, increase cwnd by one packet", which "opens the window exponentially in
time"; it "takes time R log₂W", and "guarantees that a connection will source data at a rate at most twice the
maximum possible on the path". A step's allowance has the same unknown to find: how many polls fit the step
budget, at a cost per poll that changes with the work. **INFERENCE**: start at one poll; after each step that
polled, allow as many as would fill the budget at the cost the step measured a poll (its length over its
polls, both from the step's own two clock readings, D3), but at most twice the last allowance, since a step
too short for its clock to time (a tick or none on Apple silicon's 41.67 ns timebase) reads as costing
nothing. A step that ran long (a long poll, or the host preempting the thread) shrinks the next allowance to
what fits at once, as a loss restarts slow start.

## 12. A timed park on macOS runs later the longer it is

**MEASURED** (`benchmark-results/hyper-rt-vs-tokio-20261010/os-timer/leeway-r1`, TIMERLAT, 300 waits each,
three rounds interleaved, load 58–61): kevent's timeout ran a median of 259–348 µs late for 1 ms, 135–250 µs
for 500 µs, 68–166 µs for 250 µs, 58–85 µs for 125 µs and 18–30 µs for 62 µs; an `EVFILT_TIMER` one-shot
with `NOTE_CRITICAL` for 1 ms, 35–64 µs; with `NOTE_LEEWAY` and a leeway of zero, 517–522 µs. **INFERENCE**:
the lateness grows with the length of the wait, as a leeway proportional to the timeout would, and the
critical timer opts out of it; §5's run at a similar load found every mechanism a median of 587–1,354 µs late,
so the comparison wants more rounds before a design rests on it. A shard that sleeps a millisecond in one
kevent therefore wakes later than one that woke at intermediate boundaries and parked the rest short, which
is what the wake-sized tick's multi-level wheel did by accident (§11's runs: 129 µs at the median against
295 µs with the 50 µs tick, trantor 258 µs).

## 13. A yield and the wakes that came in meanwhile

With the allowance alone, the request above still waited about three slices (125 µs): the slicing task
re-queued itself during its slice, before the next step drained the request's wake, so the FIFO ran the
slicer's next slice first. tokio's `yield_now` does not wake the task at once: "Don't wake the task
immediately, as that would push it right back onto the run queue and it could be polled again before other
tasks or the IO/timer driver get a chance to run. Instead, hand the waker to the scheduler, which wakes
deferred tasks only after it has run out of ready tasks and polled the driver" [TOKIO-YIELD]. **MEASURED**
with a self-wake held apart until the ready tasks have run and no wake from another thread waits (PANELS
`mac-a1-r1`): the one-client request at 52 µs and 8,261 a second; eight clients 92,797 a second at 72 µs
against trantor's 95,659 at 72 µs. The price is a second ring a yield passes through: 16 tasks yielding in
turn run 53.9 million polls a second against 77.2 million without it (trantor runs its queued closures at
28.1 million).

## 14. A spin where its waker may not run, judged by what the spins saved

**primary** [LIM-AGARWAL §3, p. 7]: "A choice of waiting mechanisms has to be made only if there are runnable
threads to replace a blocked thread. To facilitate discussion, let us say that a program is *matched* if the
number of concurrently runnable threads assigned to any processor never exceeds the number of hardware
contexts on that processor; otherwise the program is *unmatched*. Thus, an always-poll algorithm should be
used for matched programs since there are no other runnable threads to replace a blocked thread." And, of
waiting by one mechanism alone: "If the program is unmatched, polling admits the possibility of deadlock if
non-preemptive scheduling is used. Although timeouts and preemptive scheduling can be used to avoid deadlock,
polling could still suffer from poor performance." Their analysis of two-phase waiting "assumes that we can
always find a runnable thread to replace a blocked thread" [§4, p. 9], and prices a wait in processor cycles:
polling up to `αB` and then blocking at cost `B` costs `∫₀^{αB} t f(t) dt + ∫_{αB}^∞ (1 + α)B f(t) dt` for a
wait-time density `f` [§4.2 eq. (1), p. 10, spinning's `β = 1`]. [BOGUSLAVSKY93 §3.3, pp. 14-15] models three
threads sharing a lock on two processors; its Fig. 7 maps where immediate blocking, pure spinning, or spinning
then blocking gives the most throughput, and "If T_c → 0, then immediate blocking is the best choice for all
other parameters" (`T_c` the time a context switch takes).

**INFERENCE**: a shard waiting for work and the thread that will send it are two runnable threads. Where the
process can run one thread at a time (one CPU in its affinity mask, or a cgroup quota of one CPU) they are
unmatched by construction: the sender runs only once the spinning shard is descheduled, so no spin can see its
work and its whole length is waste. Where the process can run more, whether the sender runs beside the spin
depends on everything else the host runs; only the spins' outcomes tell. Lim and Agarwal's always-poll for
matched programs rests on the processor having nothing else to do. On a laptop an idle core saves power, and
on a busy machine, this note's premise, the core has other work; so the price stays the one their equation (1)
sets, processor time, evaluated on the waits the shard met: a spin that saw its work after `t` saved `B − t`,
and one that ran out its length wasted it.

**MEASURED** (PANELS-LINUX `c1-quiet-bd7c0fd`, a container on one CPU, three rounds, load 10): two shards
bouncing a value with the calibrated spin ran 138,940 round trips a second at 5.27 µs of CPU each, and every
one of its 3,832 spins missed; the hand-configured loop with no spin (`HYPER_SIMPLE`) ran 366,896 at 1.51 µs;
trantor 420,243; tokio's current-thread runtime 478,135.

**MEASURED**: no fixed choice is right everywhere. "Always" spins for the park cost on every wait (D2), "never"
does not spin; ops/s medians, the spins that saw their work under "always".

| Panel (load) | Case | Always | Never | Spins that saw their work |
|---|---|---|---|---|
| PANELS `mac-spin-r2` (54–72) | ping-pong | 957,323 | 5,802 | 21,916 of 21,993 |
| PANELS-LINUX `c2-hogs2-trial` (27) | ping-pong | 71,596 | 151,533 | 2 of 15,482 |
| PANELS-LINUX `c2-quiet-spin` (40–41) | rtt | 154,111 | 293,956 | 15 of 3,916 |
| PANELS-LINUX `c2-quiet-trial` (15–17) | rtt | 49,987 | 31,212 | 16,329 of 21,828 |

The last two rows are the same two-CPU container with nothing of this work beside it: what else the virtual
machine ran made the program matched or not.

**MEASURED** (PANELS `mac-spin-r1`): judging each spin as it came, by a running mean of each spin's saving
and waste (D2's estimator), spinning on every wait while the mean saving exceeded the mean waste and otherwise
on one wait in a number that doubled up to 59, the ping-pong fell to 6,443 round trips a second (always
270,847, never 9,649). **INFERENCE**: two shards bouncing a message see each other's work early only while both
spin. The first misses, met while the peer still parked, turned both means; after that each shard's lone
probes met a parked peer, missed, and kept the verdict.

**MEASURED**: judging windows of 59 spins (D9), five rounds a panel; ops/s medians, CPU an operation in
parentheses (ns):

| Panel (load) | Case | Windows | Always | Never | trantor | tokio current-thread |
|---|---|---|---|---|---|---|
| PANELS `mac-spin-r2` (54–98) | ping-pong | 1,859,593 (1,078) | 957,323 (1,184) | 5,802 (9,479) | 9,045 | 10,928 |
| | fan-in | 37,844 (5,833) | 48,415 (6,263) | 29,869 (5,885) | 49,814 | 30,179 |
| | rtt | 8,782 (8,315) | 10,043 (11,745) | 8,791 (8,892) | 5,023 | 12,424 |
| PANELS-LINUX `c2-quiet-trial` (14–18) | ping-pong | 1,000,492 (1,979) | 1,043,059 (1,946) | 30,405 (18,738) | 32,319 | 29,670 |
| | fan-in | 307,304 (3,349) | 426,417 (4,108) | 221,416 (4,036) | 397,337 | 587,434 |
| | rtt | 31,447 (16,134) | 49,987 (17,256) | 31,212 (16,966) | 27,843 | 32,550 |
| PANELS-LINUX `c2-hogs2-trial` (27–31) | ping-pong | 136,550 (3,446) | 71,596 (7,659) | 151,533 (3,675) | 176,242 | 263,498 |
| | fan-in | 167,040 (4,100) | 127,240 (6,208) | 140,544 (5,390) | 233,730 | 143,558 |
| | rtt | 198,057 (2,715) | 130,997 (3,894) | 227,066 (2,299) | 197,346 | 286,841 |

**INFERENCE**: peers that wait in step, a ping-pong's two shards taking turns, rest the same number of waits
and start their next windows on the same round trip, so their spins meet again. Where every spin missed
(c2-hogs2 rtt, and the 200 µs-gap rtt on all three hosts), the windows tried before the rests reached their
cap were all the spinning there was, and cost from 0.7% to 18% more CPU an operation than never spinning in
these runs of a few thousand waits; at the cap the share is one wait in 60. Fan-in stays below trantor on all
three hosts, and ping-pong beside the busy loops below trantor and tokio, with no spin at all as well: those
gaps are in the park and wake path, not in the decision to spin.

## 15. A timed wait's own timer: what kqueue, epoll and timerfd promise

**primary** [MAN-KQUEUE]: `EVFILT_TIMER`'s `NOTE_NSECONDS` means "data is in nanoseconds", and `NOTE_CRITICAL`
means "override default power-saving techniques to more strictly respect the leeway value". [EPOLL-WAIT]:
"The timeout argument specifies the number of milliseconds that epoll_wait() will block", and "the timeout
interval will be rounded up to the system clock granularity"; `epoll_pwait2` "takes an argument of type
timespec to be able to specify nanosecond resolution timeout", from Linux 5.11. [RUSTIX] converts a finer
timeout to milliseconds "rounding up" (`timespec.rs:193-205`); without its `linux_5_11` feature it calls
`epoll_pwait` with that count whenever it fits a `c_int`, and with the feature it calls `epoll_pwait2` on every
kernel (`backend/linux_raw/event/syscalls.rs:285-334`). [TIMERFD]: with `TFD_TIMER_ABSTIME` "the timer will expire when the value of the timer's clock
reaches the value specified in new_value.it_value"; `CLOCK_BOOTTIME` (Linux 3.15) is monotonic and counts the
time the system is suspended; and a read returns the expirations "since its settings were last modified using
timerfd_settime(), or since the last successful read(2)", so setting the timer again empties it.

**MEASURED** (PANELS `mac-timers-r1`, load 8–10; PANELS-LINUX `c1-quiet-timers`, `c2-hogs2-timers`, load
8–11; five rounds; median lateness, p99 in parentheses, µs):

| Host | Timer | Before | Own timer | tokio current-thread | trantor |
|---|---|---|---|---|---|
| Mac | 1 ms | 147.5 (213.0) | 50.2 (104.4) | 1,245.2 (1,310.7) | 131.1 (192.5) |
| Mac | 250 µs | 49.2 (96.3) | 49.2 (92.2) | 884.7 (2,031.6) | 868.4 (917.5) |
| Mac | 5 ms | 233.5 (622.6) | 50.2 (108.5) | 1,736.7 (1,802.2) | 639.0 (704.5) |
| Linux, 1 CPU | 1 ms | 1,966.1 (2,621.4) | 983.0 (1,441.8) | 1,966.1 (2,687.0) | 983.0 (1,441.8) |
| Linux, 2 CPUs + 2 busy loops | 1 ms | 1,998.8 (1,998.8) | 999.4 (1,015.8) | 1,998.8 (1,998.8) | 983.0 (1,015.8) |
| Linux, 2 CPUs + 2 busy loops | 250 µs | 1,736.7 (1,802.2) | 737.3 (802.8) | 1,736.7 (1,769.5) | 737.3 (786.4) |

The shard's 50 µs median on the Mac is its timer tick (D6), the wheel's resolution; the OS timer adds little
to it. The Mac's CPU a timer rose by 2.2 µs at 1 ms (6,498 to 8,744 ns) and 6.0 µs at 5 ms: a timer knote
added and fired per wait, against the wait's own timeout. **MEASURED** (`os-timer3/run1.txt`): the container's
kernel (6.12.76-linuxkit) reports a 1 ms resolution for `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME`, and a 250 µs
`nanosleep` and `timerfd` run 752 and 750 µs late at the median: it runs without high-resolution timers, so
trantor's and the shard's timers there both meet the kernel's tick.

## 16. What trantor's clients waited with

The harness's plain-thread clients (`rtt`, fan-in, the gap and busy variants) wait for each answer in a
one-value mailbox. The Rust clients park with std's `park` and are unparked, a futex or `__ulock` wait that
blocks at once; trbench's C++ clients waited with C++20 `atomic::wait`, which in libc++ polls and then yields
for microseconds before it blocks. **MEASURED** (one run each, this Mac, `rtt`): trantor with that client ran
297,392 round trips a second at 0.84 context switches each; with a mailbox that blocks at once in the OS's
address wait (`futex`, `os_sync_wait_on_address`), as the Rust clients do, 130,358 at 2.04, beside hyper-rt's
175,481 and, never spinning, 132,934. Every client-thread comparison with trantor before `trbench-mb` (this
note's §7 and §13, D7, and the panels named there) gave trantor's clients a spin the other arms' did not have;
the panels from `mac-timers-r1` on use `trbench-mb`. **MEASURED** beside two busy loops (PANELS-LINUX
`c2-hogs2-trantor-client`, three rounds): trantor's fan-in ran 3,133 a second with the polling client and
232,500 with the blocking one, so its collapse in `c2-hogs2-c` (3,763) was the client's; its closed-loop round
trips ran 226,020 and 354,178 (hyper-rt 299,587). Its busy rows stay far below with either client (255 and
9,310 a second against hyper-rt's 381,193): its loop runs the busy job's slices ahead of the requests.

## What remains unknown

- **The cost of blocking on Windows.** Its per-thread times advance a scheduler tick at a time, and its
  per-thread cycle count is documented as not convertible to time [MS-QTCT]; a tracking shard there
  learns no cost and does not spin.
- **trantor's loop**, whose source is not on this machine (§3).
- **Johnson, Stoica, Ailamaki and Mowry**, "Decoupling contention management from scheduling" (ASPLOS
  2010), and **Lim and Agarwal**'s reactive synchronization (ASPLOS 1994), which bear on spinning under
  load, were not read: the publisher refused the fetch. KARLIN91 states the regime and the threshold this
  note relies on, and LIM-AGARWAL's earlier report (§14) the matched and unmatched cases.
- **Linux's numbers on a Linux host.** §1–§13 measure macOS on Apple silicon; §14's Linux panels run in
  containers on this machine's virtual machine, beside other containers, not on a Linux host of their own.
  The decisions measure their quantities on the running machine, so they hold there by construction.
