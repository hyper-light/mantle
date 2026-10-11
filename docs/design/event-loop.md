# The event loop under load: decisions for the vendored hyper-rt

Mantle's ranges run on hyper-rt's shards (`engine-structure.md` §2). hyper-rt is vendored from
hyper-raft (`vendor/UPSTREAM.md`); the owner directed the changes below to be made in mantle's copy,
each a self-contained commit touching `vendor/hyper-rt` and these notes only, so hyper-raft can take
them as they are. The bar: a busy machine is the normal case, and the loop's costs must not grow with
the load. The evidence is `docs/research/42-event-loop-under-load.md` (cited as "note 42 §x").

## D1. One kick a park

**Decision.** A shard's park announcement is a state word (running, parked, parked with its kick
claimed). A sender that reads it parked claims the kick with one compare-and-swap and only the winner
makes the system call; the others skip it, their wakes already published where the woken shard drains
them. The word is read before the compare-and-swap, so a wake to a running shard writes nothing to the
shard's parking line.

**Why.** Every sender that read the announcement used to kick: 3.2 to 4.5 system calls a park in
fan-in (note 42 §3). tokio's parker makes the same claim with its `NOTIFIED` state (note 42 §3).

**Proof.** `tests/one_kick_per_park.rs` holds a shard inside its driver's wait while four threads wake
four tasks: one kick (it took four before). The loom model `parking::two_senders_never_lose_a_word_and_a_park_takes_one_kick`
explores 3,520 interleavings of two senders against a parking shard: every word arrives and no park
takes a second kick; with the claim removed it fails at interleaving 63, and with a claimer that never
kicks it deadlocks at the first (`benchmark-results/hyper-rt-vs-tokio-20261010/tests/`).

**Constants.** None.

## D2. A tracking shard spins as long as blocking costs it in CPU

**Decision.** While a shard tracks its wake (`RuntimeConfig::wake_tracking`, set by
`RuntimeConfig::from_calibration`), its idle spin lasts the CPU one park/wake cycle costs: its own
thread's CPU across each park it waits in, and its senders' CPU across the kick they claimed, each read
on that thread's CPU clock. It spins not at all until its first park has been measured, and not at all
where the OS keeps no fine per-thread clock (Windows). A request served in a step opens the idle window
from where that step ended (D3). A shard with no wake tracking keeps its configured `spin_ns`. The step
quantum and the timer tick no longer follow the wake estimate (D6).

**Why.** Spin-then-block is 2-competitive in processor time when the spin lasts as long as a context
switch costs [KARLIN91, note 42 §2]. hyper-rt spun for the measured wake latency instead, which on this
machine is about eighty times the cycle's CPU and moves with the load (note 42 §1): calibrations a
minute apart spun from 1.9 µs to 577 µs, and a closed-loop client's request cost 31.3 µs of CPU against
tokio's 5.8 µs with no gain in throughput or tail (note 42 §2). Spinning by the cost keeps the case where
spinning pays, a producer that is running, as in two shards bouncing a message (four times the
throughput, note 42 §7), and bounds a miss to one cycle's CPU.

**Proof.** `tests/spin_by_park_cost.rs`: a shard tracking a one-second wake prior notes a client's
activity and sleeps on 20 ms timers five times. It parks for every timer and measures every park; with
the wake-sized spin it spun to nine deadlines and parked once. `park_cost.rs`'s tests hold the estimator:
an exact mean over the warm-up, then a weighted mean whose window the warm-up's spread sets.

**Constants.**
- The estimator's warm-up is the crate's stopping-rule floor, `machine::bench::MIN_SAMPLES` (59, Wilks
  1941, cited there), and its window is that rule's `N = (z · sd / (h · mean))²` on the warm-up's own
  spread (`machine::wake::sample_window`): DERIVED. At the measured per-cycle coefficient of variation of
  0.34–0.38 it is 256 samples.
- The idle window's multiple is the configuration's `WakeTracking::idle_ratio`, 1 from
  `from_calibration` (unchanged).

## D3. The shard's clock is the cheap one, read twice a step

**Decision.** The shard's clock (`driver::nanos_since`, every OS driver's epoch) reads
`machine::clock::shard_clock_ns`: on macOS `mach_continuous_time` scaled by the timebase as a 32-bit
fixed-point ratio (one multiply and a shift); elsewhere `monotonic_ns`, whose clocks there are already
the cheap ones (Linux's vDSO `CLOCK_BOOTTIME`, Windows' interrupt time). `monotonic_ns`, the host-wide
reading other processes compare, is unchanged. A step reads it twice, however many tasks it polls: at its
start, and once after its polls. The watchdog judges the step, not each poll: a step past the step
quantum by the wall clock, from its start to its polls' end, is attributed to its tasks or to the host
(`attribution.rs`), and the longest-step counter is the longest such step.

| Reader | Reads before | Reads now |
|---|---|---|
| a step: the overrun of a wait just ended, the attribution window, timer expiry, the harvest's quantum | 2 (the step's publication, then the timers' own) | 1 (the publication, which the others take) |
| a step's polls: their length (the watchdog and its attribution, the longest-step counter), an activity window's origin | one a poll, at its end (each poll's end was the next one's start) | 1, after the last poll |
| the idle spin, a turn | 2 | 1, and 1 more after a turn that asked the driver |
| a timed park and its kick (wake learning) | 4 of `CLOCK_MONOTONIC` | 4 of the shard's clock |

A step of `p` polls on macOS read `CLOCK_MONOTONIC` `2 + 2p` times at 16–25 ns; it now reads the shard's
clock twice at 5–7 ns. A request/reply step with one poll went from four reads to two.

**Why.** macOS `CLOCK_MONOTONIC` is the wall clock less the boot time: 16.1 ns a read at 1 µs resolution,
where `mach_continuous_time`, which also counts through sleep, costs 4.8 ns at 41.67 ns (note 42 §4). The
two reads a poll were 6 % of the shard's samples on mantle's resident get path, and a poll that yields
cost 47.5 ns; it cost 15.9 ns with one read a poll (note 42 §7). That one read was still the largest cost
of a poll that does little: 1,134 of the 2,915 samples in a detached-spawn shard's step were
`mach_continuous_time` after a poll (note 42 §9). Without it, on this Mac at load 12–15, detached spawns
run 25.8 million a second against 22.8 million, yields 66.8 million against 46.9 million, and a two-shard
ping-pong 1.30 million round trips against 1.07 million; a single client's round trip and an eight-client
fan-in are unchanged (`benchmark-results/hyper-rt-vs-tokio-20261010/runs/mac-b-step-r1`). Mantle's fill
through its same-shard client puts 2.96 million keys a second at the median against 2.55 million, with gets
and seeks unchanged (`benchmark-results/rtloop-async-fill-bisect-20261010/b-step-r1`). Per-poll timing
bought a per-task long-poll count no consumer read (`TaskSlot::long_steps`, removed); the step's own
timing is what the watchdog needs, since a step is what holds the shard's other work back.

**Proof.** `tests/step_clock_reads.rs`: one step polls 256 tasks and reads its driver's clock twice (257
times before). `machine::clock`'s tests: Apple silicon's 125/3 timebase converts 3 ticks to 125 ns and a
second's 24,000,000 ticks to exactly 10⁹ ns, a one-to-one timebase returns its ticks, and the shard's
clock never runs backwards. The attribution tests (`tests/wake_estimate.rs`) hold the long-step rules,
each held poll a step of its own (a batch of one); `attribution.rs`'s unit tests hold the tracker.

**Constants.**
- `SCALE_BITS` (32): format, the fixed-point scale's fraction, chosen so the 64×64-bit product cannot
  overflow 128 bits and the rounding error stays under 10⁻¹¹ of a tick.

## D4. Channels hand values over with no lock

**Decision.** hyper-rt's bounded channel and one-shot carry their values through a lock-free bounded
ring (`sync/ring.rs`), wake a waiting receiver through its cell's waiter word (`handoff.rs`), whether the
receiver is a task or a plain thread parked with its handle registered there, and hold senders waiting for
room in places of their own (`sync/room.rs`). No path takes a lock. Each take grants its room to one
waiting sender; a sender that stops waiting frees its place at once, and one that leaves with a grant it
did not see hands it on. The API is unchanged: `channel`, `channel_with`, `oneshot`, `Sender::{try_send,
send, blocking_send}`, `ChannelReceiver::{try_recv, recv, blocking_recv}`, the one-shot's ends. One
meaning is new: `channel_with`'s `waiters` bounds the senders, tasks and threads alike, waiting for room at
once, and `blocking_send` past it is told `Full` (it used to wait inside std's channel, unbounded).

**Why.** The values used to go through std's `sync_channel`, whose blocked ends register behind a mutex
that every send to a blocked receiver takes; mantle's fill spent samples in it and in the semaphore signal
under it, and the owner asked for the lockless design (note 42 §8). The queue of waiting senders kept a
place for each sender that had stopped waiting until the receiver's grants walked past it, so live senders
were refused. Measured with plain threads (note 42 §8): one producer moves 144 million values a second
against 30 million before, and sixteen contending producers 674 thousand against 407 thousand, at a third
of the CPU and half the context switches; where every value blocks a thread, the cost is the park and
unpark either way.

**Proof.** loom, under the workspace's bounds (`benchmark-results/hyper-rt-vs-tokio-20261010/tests/loom-channel.txt`):
`sync::ring` two producers and a consumer through one slot (548 interleavings), `handoff` a thread waiter
woken once and its handle never read after it leaves (6,289), `sync::room` one sender (50) and two senders
(71,847) waiting for room, the two driving the same `Room::send_waiting` that `blocking_send` runs. Each
model fails under the mutations recorded there (a value read before its stamp, either fence of a pair
removed, a grant kept that the sender did not see, a granted sender that does not wait again).
`tests/channel_threads.rs` runs real threads through the blocking calls.

**The receiver's close.** The receiver's drop set a flag and then drained the ring, and a sender that had
read the flag clear could push into the room the drain made: its send succeeded into a channel no one would
read, where it had to be `Closed` (`tests/sync.rs` caught it at round 2,380 of 3,000,
`benchmark-results/hyper-rt-vs-tokio-20261010/tests/suite-spin-policy.txt`). The close now marks the
ring's tail, between the index and the lap, as std's array channel marks a disconnect: the compare-and-swap
that claims a position fails on the mark, so a push and the close are ordered by that one word (note 42
§8). A push that claimed its position before the mark may publish after the drain has looked; std's drain
spins until it does, and here the sender drops what the drain left, reading the mark after its wake's
`SeqCst` fence while the receiver fences between its close and its drain. Proof: loom
(`tests/loom-ring-close.txt`): a push against a close and drain never gets in with the one slot full (50
interleavings), and with it empty its value is dropped exactly once (50); with the close a flag the push does
not read, the push gets in (interleaving 8), and without the sender's drain, or the receiver's fence, the
value is stranded (interleavings 9 and 1; `tests/loom-ring-close-mutations.txt`). `sync::ring`'s unit test:
a closed ring refuses every push at capacities 1 to 7 across laps, and its drains drop what it held. The send
keeps its read of the receiver's flag before the push, now only to refuse a gone receiver early: that read
brings in the cell the wake reads after its fence, and without it one producer moved 13.2 ns of CPU a value
against 11.1 (PANELS `chan-ringclose-spsc-r3`).

**Constants.**
- `Padded`'s alignment (128 bytes): shape, the largest cache line targeted (Apple silicon), keeping the
  ring's head and tail on separate lines.
- The slot and word states (`FREE`, `WAITING`, `GRANTED`; `NO_WAITER`, `CLAIMED`, `THREAD`): format.
- The ring's mark: format, the capacity plus one rounded up to a power of two (std's `mark_bit`), with a
  lap twice that.


## D5. A shard's control channel holds its admission limit

**Decision.** The channel that carries spawns and cancels from other threads to a shard is bounded at the
shard's admission limit, `tasks_per_shard`, as `control.rs` states, not at `ring_entries`. A send past it
is refused `ControlFull`.

**Why.** A request past the admission limit could not be admitted at the shard's next drain, so the limit
refuses nothing the arena would have admitted, however long the shard takes to drain. `ring_entries` was
derived from the wake probe (`next_pow2(wake p99 / syscall median)`): four in a one-CPU Linux container,
where the shard's thread did not run while a benchmark's main thread sent nine spawns, and the fifth was
refused (`benchmark-results/hyper-rt-vs-tokio-20261010/runs-linux/c1-quiet-bd7c0fd/fanin/r3-hyper.txt`).
hyper-rt returned the typed refusal; the panic in that run was the benchmark's own `expect`.

**Proof.** `tests/control_depth.rs` holds a shard inside one poll, sends it a burst of 64 spawns through a
configuration whose `ring_entries` is 4, and is refused the 65th; every spawn of the burst then runs. With
the channel bounded at `ring_entries` the fifth spawn was refused
(`benchmark-results/hyper-rt-vs-tokio-20261010/tests/control-depth-red-bd7c0fd.txt`).

**Constants.** None.

## D6. A step's budget is the timer slack, and a step takes the polls that fill it

**Decision.**
- `Calibration::constants` sets the timer tick and the step budget to the timer slack Linux gives every
  ordinary thread, 50 µs (`TIMER_SLACK_NS`; a consumer's tighter latency objective tightens the step).
  The quantum a cooperative task slices its work by (`ShardContext::quantum_ns`, `futures::step_budget_ns`)
  is the step budget whatever the wake. `from_calibration` sizes `ring_entries`, now only the completion
  buffer, from the readiness waits.
- A shard on a real clock gives each step an allowance of polls: one at first; after each step that polled,
  as many as fill the step budget at the cost that step measured a poll (its length over its polls, from
  its two clock readings, D3), at most twice its own allowance, at least one, and never more than the
  configured `batch`, now only the cap (`from_calibration` sets it to the budget in nanoseconds: no poll costs
  less than a nanosecond). A shard with no clock (the simulation) takes the configured batch every step.
- The watchdog judges a step against its budget and one quantum: a step's polls fill its budget, and the
  poll that crosses it may run a quantum past.

**Why.** The wake probe set these before: the tick and the quantum were its mean, the poll batch its mean
over the measured cost of a trivial poll, and the control depth its p99 over a system call (D5). It is the
noisiest measurement the runtime takes (note 42 §1): in a one-CPU Linux container it made a 565 ns tick
and quantum and a batch of 4–8, and its attribution read the thread's CPU account 122,000 times in a
million polls (`benchmark-results/hyper-rt-vs-tokio-20261010/runs-linux/c1-quiet-bd7c0fd`). The slack is the
precision the kernel itself grants the timed wait a shard parks in (prctl(2), note 42 §10): a tick finer
than it resolves no deadline sooner, and a busy shard that looks at its timers, inboxes and driver once in
it adds no more lateness than the kernel's coalescing already allows.

A count of polls bounds a step's time only for polls as cheap as the one it was measured on. A task that
slices long work by the quantum and yields between slices held its shard for the whole count: with a 50 µs
slice beside a server, a request from another thread waited 0.8–2.4 ms at the median (`bd7c0fd`, a
calibrated count of 28; `runs/mac-a1-r1`, `runs/mac-ad-r1`) where trantor and tokio answer in one slice. The allowance follows the polls the step actually
runs (note 42 §11). It opens as slow start opens a window [JACOBSON88]: from one, doubling, so a step the
clock cannot time (a tick or none) is never trusted past twice the last, and a step that ran long restarts
it at what fits.

Measured on this Mac at load 25–60 (`benchmark-results/hyper-rt-vs-tokio-20261010/runs/mac-a1-r1`, five
rounds, arms rotated; base `bd7c0fd`): with a task slicing in 50 µs beside one client's server, 5,827
requests a second at a median of 125 µs, against 570 at 803 µs; with eight clients, 34,362 against 4,237;
detached spawns 22.0 million a second against 17.1; yields 77.2 million against 38.5; eight clients without
the slicing task 336 thousand against 258. A 1 ms sleep runs later: 295 µs at the median against 129 µs
(trantor 258 µs). A wait for the next 50 µs tick parks once for the whole millisecond, where the wake-sized
tick's wheel woke at its level boundaries and parked the last stretch short, and macOS stretches a longer
kevent timeout further: at load 58, 1 ms waits ran a median of 259–348 µs late against 58–85 µs for 125 µs
waits (`benchmark-results/hyper-rt-vs-tokio-20261010/os-timer/leeway-r1`). The timed park is the next
item of work (note 42 §12).

Mantle's fill through its same-shard client is unchanged: 1.96 million puts a second at the median of the
five rounds at load 24–27 against 2.02 million at `bd7c0fd`, gets and seeks no lower
(`benchmark-results/rtloop-async-fill-bisect-20261010/a-r1`; its last three rounds ran at load 35–50, where
every arm fell).

**Proof.**
- `machine::calibration`'s test: the tick and the step are `TIMER_SLACK_NS` whatever the wake measured,
  and a tolerance below `tick + wake p99` is refused. RED before: the tick was the wake mean, 5,527 ns
  (`benchmark-results/hyper-rt-vs-tokio-20261010/tests/cited-constants-RED.txt`).
- `shard_loop`'s `next_batch` tests: a long step shrinks the allowance to what fits, a step read as no time
  or a tick doubles it at most, and a step that ran out of work measures the polls it ran.
- `tests/step_budget.rs`, on a virtual clock its tasks move: a task yielding after every 500 ns poll gets
  1, 2, 4, …, 64 polls a step, then the 100 that fill a 50 µs budget; after a poll ten budgets long the
  allowance falls to what fits and doubles back to the cap. RED before: 2,000 polls every step
  (`tests/step-budget-RED.txt`).

**Constants.**
- `TIMER_SLACK_NS` (50,000 ns): cited, prctl(2) (note 42 §10).
- The allowance's growth bound (twice the last): cited, slow start [JACOBSON88 §1].
- `from_calibration`'s `batch` (the step budget in nanoseconds): bound, a poll costs at least a nanosecond.

## D7. A yield goes behind what came in from other threads

**Decision.** A task that wakes itself while it is polled — a yield — waits apart from the ready queue
(`LocalQueue::push_deferred`) and rejoins it behind everything ready: when what was ready has run, unless
another thread has left a wake or a control message meanwhile (two loads), in which case the step ends and
the next step's drains queue that work ahead of the yielders; and at every step's start, after its drains.
Wakes of one task by another keep their place in the step's FIFO, so a local request and its reply still
run in one step.

**Why.** A task that slices long work and yields between slices re-queued itself during its slice, before the
step after it drained the wake that a request from another thread had left meanwhile, so the request waited
for the yielder's next slice too: with 50 µs slices, about three slices at the median even with D6's allowance
(125 µs, `benchmark-results/hyper-rt-vs-tokio-20261010/runs/mac-a1-r1`). tokio's `yield_now` hands its waker
to the scheduler, "which wakes deferred tasks only after it has run out of ready tasks and polled the driver"
[TOKIO-YIELD, note 42 §13]; trantor runs its one queue in order. Both answer in one slice; so does a yield
here now: 52 µs at the median, 8,261 requests a second against 5,827; eight clients 92,797 a second at 72 µs
against 34,362 at 135 µs, where trantor ran 95,659 at 72 µs (`runs/mac-a1-r1`, load 25–60). The price is a
second ring a yield passes through: sixteen tasks yielding in turn run 53.9 million polls a second against
77.2 million (trantor runs its queued closures at 28.1 million). The check of the inboxes replaces tokio's
poll of the driver when the ready tasks run out: a driver poll is a system call, and I/O readiness already
reaches a busy shard once a quantum (D6).

**Proof.** `tests/yield_order.rs`: a task woken from another thread between two steps runs before a yielding
task's next poll; and a wake that lands while a task yields ends the step after that poll. RED before: the
yielder ran first, and the step polled it four times
(`benchmark-results/hyper-rt-vs-tokio-20261010/tests/yield-order-RED.txt`). `queue.rs`'s unit test holds the
deferred ring.

**Constants.** None.

## D8. Attribution stops reading at the first step within its bound

**Decision.** The long-step tracker (`attribution::Tracker`) disarms at the first step that polled and ran
within its bound, as well as after a busy period between two waits that ran no long step.

**Why.** An armed shard reads its thread's CPU account at every step's start — a system call or a Mach trap,
107–161 ns (`attribution.rs`) — until its next wait. A shard that never waits, as one serving requests beside a
task that slices long work, read it at every step for as long as it stayed busy: 12,356 to 23,937 reads in
11,995 to 20,993 steps per run (`benchmark-results/hyper-rt-vs-tokio-20261010/runs/mac-a1-r1/rttbusy`,
`r*-ad.txt`). A step within its bound shows the window its start opened was not needed.

**Proof.** `tests/thread_account_reads.rs`: on a virtual clock, one long step and then a thousand short ones,
the shard never waiting, read the account twice where the OS keeps a per-thread clock (the long step's own
reading and the next step's start) and once elsewhere. RED before: 1,002 reads
(`benchmark-results/hyper-rt-vs-tokio-20261010/tests/never-waiting-reads-RED.txt`). `attribution.rs`'s unit test
holds the tracker's rule.

**Constants.** None.
