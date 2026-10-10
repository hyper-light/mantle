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
where the OS keeps no fine per-thread clock (Windows). A request served in a poll opens the idle window
from where that poll ended. A shard with no wake tracking keeps its configured `spin_ns`. The step
quantum and the timer tick still follow the wake estimate.

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

## D3. The shard's clock is the cheap one, read once a poll

**Decision.** The shard's clock (`driver::nanos_since`, every OS driver's epoch) reads
`machine::clock::shard_clock_ns`: on macOS `mach_continuous_time` scaled by the timebase as a 32-bit
fixed-point ratio (one multiply and a shift); elsewhere `monotonic_ns`, whose clocks there are already
the cheap ones (Linux's vDSO `CLOCK_BOOTTIME`, Windows' interrupt time). `monotonic_ns`, the host-wide
reading other processes compare, is unchanged. Each reading serves every decision that can share it:

| Reader | Reads before | Reads now |
|---|---|---|
| a step: the overrun of a wait just ended, the attribution window, timer expiry, the harvest's quantum | 2 (the step's publication, then the timers' own) | 1 (the publication, which the others take) |
| a poll: its length (attribution, the longest-poll counters), the next poll's start, an activity window's origin | 2 (start and end) | 1 (its end; it starts where the previous poll ended) |
| a step's first poll | its own start | the step's reading when nothing ran since it (no drain, timer, poller or driver call), else one fresh read |
| the idle spin, a turn | 2 | 1, and 1 more after a turn that asked the driver |
| a timed park and its kick (wake learning) | 4 of `CLOCK_MONOTONIC` | 4 of the shard's clock |

A step of `p` polls on macOS read `CLOCK_MONOTONIC` `2 + 2p` times at 16–25 ns; it now reads the shard's
clock `1 + p` times (`2 + p` when its drains or driver did work) at 5–7 ns. A request/reply step with one
poll went from four reads to two or three.

**Why.** macOS `CLOCK_MONOTONIC` is the wall clock less the boot time: 16.1 ns a read at 1 µs resolution,
where `mach_continuous_time`, which also counts through sleep, costs 4.8 ns at 41.67 ns (note 42 §4). The
two reads a poll were 6 % of the shard's samples on mantle's resident get path, and a poll that yields
cost 47.5 ns; it costs 15.9 ns now (note 42 §7).

**Proof.** `machine::clock`'s tests: Apple silicon's 125/3 timebase converts 3 ticks to 125 ns and a
second's 24,000,000 ticks to exactly 10⁹ ns, a one-to-one timebase returns its ticks, and the shard's
clock never runs backwards. The suite's attribution tests (`tests/wake_estimate.rs`) hold the long-poll
rules on the new reading points.

**Constants.**
- `SCALE_BITS` (32): format, the fixed-point scale's fraction, chosen so the 64×64-bit product cannot
  overflow 128 bits and the rounding error stays under 10⁻¹¹ of a tick.
