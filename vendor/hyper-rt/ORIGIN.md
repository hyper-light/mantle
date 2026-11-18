# hyper-rt: where it comes from

slates' runtime, taken at slates `6b9ce5c` (2026-10-05) and designed onward for all three consumers
(docs/runtime.md). slates was not changed; it moves onto this crate on its own schedule.

## What was taken

| Here | From slates | What changed in the port |
|---|---|---|
| `src/*.rs` (runtime, shard, registry, parking, task, queue, timer, driver, kqueue, epoll, iocp, afd, netsys, udp, tcp, readiness, futures, sim, attribution, thread_clock, waker, control, error) | `crates/rt/src` | Paths renamed (`slates_mem` → `crate::mem`, `slates_machine` → `crate::machine`). `uring.rs` not taken: its io_uring driver runs in readiness mode only, a second path to epoll (docs/runtime.md §3.7). `RuntimeConfig::from_profile` became `from_calibration` (below). `UdpSocket::dont_fragment` added, so the don't-fragment tests read the option through the crate instead of an `unsafe` call of their own. |
| `src/mem/{error,handle,loom_bounds,mpsc,ring,segmented,slab}.rs` | `crates/mem/src` | Only what the runtime uses. The arenas, the buddy allocator, budgets, locked and shared regions and pre-faulting are slates' storage, not a runtime, and are not taken: the runtime locks and pre-faults nothing. |
| `src/machine/{bench,clock,derived,error,facts,placement,stats,wake}.rs` | `crates/machine/src` | `serde` derives and the JSON profile removed; the BLAKE3 identity hash removed (a consumer keys its stored calibration as it chooses, docs/runtime.md §10.2). `Placement::of` takes the consumer's reserved cores in place of slates' fixed one for control and the OS (§10.3); the wake probe's waker runs on the first reserved core, or a sibling shard's when none is reserved. |
| `src/machine/probes.rs` | `crates/machine/src/probes.rs` | Only the system-call probe and thread pinning. The fault, memcpy, hash, codec, lock-capacity and core-matrix probes size slates' storage and are not taken. |
| `src/machine/calibration.rs` | new, in place of slates' `profile.rs` | The three measurements the runtime needs and the constants derived from them under the consumer's policy (docs/runtime.md §10). |
| `tests/*.rs` | `crates/rt/tests` | Paths renamed. `timer_allocations.rs` counts through hyper-measure's counting allocator; slates' own check that its allocator does not attribute another thread's allocations is hyper-measure's to test, and is not carried. The `rt_bench` example and the callgrind bench are not taken; docs/runtime.md §12's benchmarks replace them. |

## The suite at the port

`cargo test -p hyper-rt` on macOS arm64, Rust 1.98.0, before any of docs/runtime.md's departures:
91 unit tests, 70 integration tests in 22 files, 6 doctests, all passing. The departures are made
against this suite.

| `src/udp/linux.rs`, `src/udp/macos.rs`, `src/udp/batched.rs` | hyper-tokio `src/sys/{linux,macos}.rs`, `src/socket.rs`, `src/clock.rs` | The batched calls and kernel stamps, moved so hyper-tokio and hyper-rt share one layer (docs/runtime.md §5.1, §14). `Batched` awaits hyper-rt's readiness instead of tokio's reactor, so a receive always asks the kernel (no `Through::Reactor`). Stamps land on the shard's clock by their age; macOS reads the age on `mach_absolute_time` (`Clock::now_ticks`), the stamp, in the kernel's ticks, carried into the process's at the kernel's rate first (`machine::clock::MachTicks`: a process Rosetta 2 translates is shown a timebase of 1/1 against the stamp's 24 MHz, docs/runtime.md §5.1). Astray reports are a seam outcome; EIO from a segmented send resends unsegmented instead of dropping. |

## Mantle's runtime work, taken here (2026-10-12)

Made in mantle's vendored copy (by the owner's direction and by Codex) and taken here from mantle's
commits as they were (`cf6e37c`, `6f05bd8`, `3743b25`, `f797bd9`, `bfdf9c2`, `bb0779e`, `bd7c0fd`,
replayed onto `8cd3604`, the revision mantle took, and merged as mantle merged them at `be8d04d`), then
merged with `main`:
- cooperative service admission and cancellation ownership (`spawn_service`; a service's cancellation
  is a level it reads, its future kept until its cleanup returns), nonblocking manual-step and readiness
  progression, typed driver-loss cleanup, context checks before each synchronization receive poll,
  checked channel slot and ticket layouts before construction, and cold, runtime-owned, bounded native
  retirement groups (`prepare_retirement`, `prepare_retirement_with_original`); TCP serving metadata
  owned from the first shard claim, accept futures keeping their guards across refused admission and
  cancellation (`docs/runtime.md` §4, §8);
- the loop under load: one kick a park, a tracking shard spinning as long as blocking costs it in CPU,
  the cheap clock read once a poll, and channels that hand values over with no lock (`docs/runtime.md`
  §3.9, mantle's `docs/design/event-loop.md` D1–D4);
- a step's poll budget covering the wakes its own polls make (hyper-raft PR #16, `ca14726`) and a
  thread's CPU account read only while attribution is armed (#15).

In the merge with `main`: the one-shot keeps mantle's lock-free state, with `main`'s
`OneshotSender::is_closed` read over it; a poll that unwound still returns its end, as mantle's step reads
it, and tells a waiting `block_on` its root unwound (`RtError::RootUnwound`, `main`'s); the service
unwind test now holds that a root's unwind ends its call refused, its peer service cancelled, released
and joined inside the call, where it held the panic passing out of `block_on`.

Made here with it, for `tests/service_resources.rs`, red in mantle since `6f05bd8`: the shard's control
queue is a ring held in its registry entry (`control.rs`, `docs/runtime.md` §8), not std's
`sync_channel`, whose lock on macOS is a pthread mutex boxed at its first lock, so a shard losing its
driver allocated as it closed the queue, inside the measured driver-loss round (backtraces:
`OnceBox::initialize` under `close_control` from `lose_driver`). The queue has no cell: hyper-rt's
channel there, the first try, put a call cycle through the registry (the entry's free dropping its
sender, whose drop wakes through the registry) that `scripts/check-recursion.py` refused on every
target (CI run 38282201776). The test's harness moved to hyper-rt's channels (std's also grew a blocked
receiver's waiter list, `mpmc::waker::Entry`); with both, no hot round allocates. One observer thread
serves every driver-loss round, as one serves every reuse round: a thread started per round began inside
the hot window it was started for, and its first stack pages counted (4 of 10 runs red, 1–2 faults; a
64 KiB stack touch at its start made it 20 of 20, 3–4 faults a round; with one observer 30 of 30 green,
and 20 of 20 with that touch).

Also made here with it, each found on CI or by widening a window the scheduler opens on a busy host:
- A receiver's drop closes its ring, a mark in the tail, so a send claims its place before the close or
  is refused `Closed` (`docs/runtime.md` §8). mantle's channel read a state bit before the push, and a
  receiver's drop could free a full ring's room between the read and the push (`tests/sync.rs`).
- `readiness_waits`' wait dropped on another shard takes the slot back once a later step has polled the
  task, where it took one yield for a control drain: a step polls a task that yields again within its
  batch (PR #16).
- `service_driverless_receipts` counted hyper-measure's Linux probe as the runtime's: the probe read
  `/proc` through `format!` and `read_to_string`; it reads into a buffer on the stack now.
- `service_resources`' observer yields once before any window: its loop yields only when the shard has
  not parked yet, and its first yield, in a random measured round, ran kernel32!SwitchToThread for the
  first time in the process, which Windows counts as a page fault (8 runs in 40 on windows-2025, the
  page named by Windows' working-set watch and cdb; 0 in 80 with the yield).

## Made here: long polls judged by step (2026-10-12)

- mantle's ranges bench found the shard reading its thread's CPU clock once at the end of every poll past
  the quantum and once at every step's start: a seek's poll ran a little past the 2.1–4.0 µs quantum, so
  727,739 reads in a million seeks, 2.9% of the shard's time in `__thread_selfusage`. From a long poll
  judged a run on the CPU, a step's long polls are judged together at one reading at its end
  (`attribution::Tracker::by_step`, `judge_in_window`): an overrun past the window's off-CPU time was
  spent on the CPU, the per-poll rule's verdict, and a window that leaves one undecided judges one at a
  time again. Reads fell to about one a step (8–9 times fewer), 0.5% of the shard's time
  (`docs/benchmarks.md`, "hyper-rt: long polls judged by step"). Tests: `tests/attribution_by_step.rs`
  (the reads beyond one a step paid for by undecided verdicts; it fails on the per-poll reading, 41
  reads for 32 long polls), two unit tests of the window and the modes; `tests/wake_estimate.rs`'s
  attribution counts unchanged.


## Made here: the pin protocol public (2026-11-14)

- `retire::Pinned`, the registry slot's counted-reader protocol, is public, its publish, read and
  retire asking `T: Send + Sync` (an entry read from any thread and freed on whichever finishes its
  retirement), so hyper-block's segmented files share their open files by it: the owner retires
  them, a transfer through any other handle pins them for its call, and the last such closes them
  (hyper-block `ORIGIN.md` change 17). The protocol and its loom model are unchanged.

## Made here: what held a shard, by spawn site and label (2026-11-18)

- Each long poll (docs/runtime.md §3.11) is counted against its task's spawn site and label on a fixed
  table on the shard's registry entry, with its hold, its longest and the attribution's verdict
  (`src/slow.rs`, docs/runtime.md §3.12): every spawn records its caller (`#[track_caller]`;
  `Incoming::site`, `TaskCell::site`, `SpawnRequest::site`), a task labels itself
  (`futures::label`, `TaskCell::label`), and any thread reads a shard's table
  (`slow::slow_polls`). Taken from Kiln's runtime copy (`kiln-rt`'s `slow.rs`, plan `01a1a87b2cec`
  step 2; the file as Kiln's `84fa1838b` left it, read at Kiln's `28b6c8ca8c4`): the table on the registry entry, its key's
  comparison and its probe's hash, its unkeyed entry and its reader with no lock, as Kiln wrote
  them. Changed in the port: Kiln counts polls past a fixed 100 µs, here the threshold is §3.11's,
  twice the quantum, so every counted poll has a verdict; a poll judged at its step's end has its
  verdict counted on its entry then, where Kiln's counted it as no host's; a task's label stays on
  its slot until it sets another, where Kiln clears it as each poll begins; Kiln's table of 128
  entries is here the entries one 4 KiB page holds; Kiln's readings of the CPU clock (its step 3)
  are not taken. Tests: `tests/slow_polls.rs`, three unit tests in `slow.rs`.

## Made here: the watchdog, a shard held in one poll seen while it runs (2026-11-18)

- One thread per OS runtime reports a shard held in one poll past `batch` × twice its quantum, once, with
  the task, its spawn site and label, the hold and whose time it was, into a board on the shard's registry
  entry that any thread reads with no lock (`src/watch.rs`, docs/runtime.md §3.13; `Entry::watch`,
  `board`, `os_thread`; `registry::StaticCell`, `shard_runnable`; `thread_clock::runnable`, `os_thread`;
  `Shard::watch_begin` and `watch_end` around each poll; `ShardContext::wanted`'s go-on and
  `futures::label` written to the watch; `Runtime`'s `watchdog`, started after every shard is built and
  joined after every worker). Taken from Kiln's runtime copy (`kiln-rt`'s `watch.rs`, its goal 6 step 2,
  plan `01a1a59c2abe`; the file as Kiln's `a24bb683f` left it, read at Kiln's `28b6c8ca8c4`): the watch's
  odd count read whole, the board's report under a sequence count, the verdict from the thread's CPU clock
  and state, and the thread that allocates nothing once started, as Kiln wrote them. Changed in the port: Kiln's limit is `batch` × the quantum, a task charged by
  its leaves holding a wanted shard about a quantum (Kiln's own coop budget, not taken); here a task keeps
  §3.11's contract up to twice the quantum, so the limit is `batch` × that threshold, and a go-on at `wanted`
  starts a new hold as §3.11's does; the watch carries the spawn site and the label (Kiln's report names
  the site from a per-slot table and no label); Linux's run state is read from `/proc` into the frame's own
  buffers; a report read whole retries once, where Kiln's retries without a count; Kiln's cap of five
  reports a minute a shard (Seastar's default, another system's tuning) is not taken: the store keeps one
  report, so a reader bounds its own log by how often it reads; Kiln's movable spawns, handed off a held
  shard, are not taken (mantle's placement, its S3b, asks `watch::stalled` instead). Tests:
  `tests/watchdog.rs`, `tests/watchdog_ends.rs`, four unit tests in `watch.rs`.

## Made here: turns by time, sockets and timers (2026-11-17)

- A task's operations charge its turn, and past its share (the step quantum) on a shard something else
  wants, the next charge's await yields, with no yield written in (`src/coop.rs`, docs/runtime.md §3.14;
  `ShardContext::charge`, `wanted_untimed`, `coop_spent`, `coop_charged`, `interest_armed`; `Shard`
  clears `coop_spent` and `coop_charged` as each poll begins; `Counters::coop_charges`, `coop_yields`).
  Charged: `TcpStream`'s `read`, `read_vectored`, `write`, `write_vectored` (so `write_all` and
  `LocalStream`'s), `TcpListener::accept` and `LocalListener::accept`, as each tries; `readiness::Ready`
  as its wait goes on, fired; `Sleep` as it ends. Taken from Kiln's runtime copy (`kiln-rt`'s `coop.rs`
  and its charge in `shard.rs`, its goal 6 step 1, plan `01a1a59c2abe`; read at Kiln's `7205b10c5b4`):
  `poll_proceed`, `consume_budget`, `spent` and the charge as Kiln wrote them, with its two fixes of the
  livelocks its tests found: each poll's first charge under the task's own waker goes on (a poll whose own
  work before its first leaf outlasted the share was refused there at every poll, redoing that work), and a
  leaf is charged only as it goes on, never as it registers or waits (a wait charged as it looked spent the
  poll's first charge on no operation, so the leaf after it was refused at every poll). Changed in the
  port: a charge that lets a task go on past its share with nothing else wanting the shard starts a new
  hold for the watchdog too (§3.13's go-on, which Kiln's `wanted` does not write); Kiln's task classes
  (`Class`, `set_class`, `give_way`) are its goal 6 step 3, not taken; channels, joins, datagrams and
  signals do not charge yet. Tests: `tests/coop_budget.rs` and `tests/coop_waits.rs`, Kiln's cases for
  these leaves (its `readiness_waits.rs` charge case among them) with socket reads and writes added beside
  its timer.

## Made here: turns by time, the sync primitives, joins, datagrams and signals (2026-11-17)

- The rest of Kiln's charges (its goal 6 step 1, plan `01a1a59c2abe`; read at Kiln's `7205b10c5b4`), each
  as Kiln wrote it: a leaf looks first, taking nothing, registers and waits uncharged, and where it goes on
  is charged before it takes. `Notified`, `OneshotReceiver`, `Recv` and `Changed` look and loop as Kiln's
  do (`Ring::has_owned`, `WatchReceiver::has_news`); `Send` looks with `Sender::goes_on`
  (`Ring::has_room`) and charges in its `go_on`, the value kept for the next poll on a refusal; `Acquire`
  charges a free permit (`Semaphore::looks_free`) and a grant, and its `queue_up` hands a permit freed
  while it queued to the oldest waiter as a release does (it took the permit itself before), so a permit
  is taken only through a charge; `Join` charges where `ShardContext::join_ends`; `signal::Recv` looks,
  registers with a read-modify-write look, and charges before it takes. `UdpSocket::recv_from` and
  `send_to_writable` charge as they try, as Kiln's do. Changed in the port: none of the code; Kiln's
  one-shot ring without padding (its goal 9) is not taken. Tests: Kiln's `coop_waits.rs` cases for these
  (notification, one-shot, channel receive and send, watch, permit, join, signal), with a granted permit,
  a delivered signal and datagrams added so every new charge has a case that fails without it.

## Made here: task classes (2026-11-17)

- Tasks carry a class, Critical, Normal (the default) or Background, and each class with work has an equal
  share of its shard's time, the most urgent first while it has run no more than the shard's quantum past the
  least-run class ready (`src/coop.rs`'s `Class`, `set_class`, `class`, `outranked`, `give_way`;
  `src/queue.rs`'s classes' lists, `pop_within`, `charge`, `waiting_above`, `has_classes`; `src/shard.rs`'s
  `Incoming::class`, set as each slot is claimed; `src/shard_loop.rs`, each poll charged to its class at most
  the watchdog's limit, `watch::stall_limit_ns`, and the inputs read once a budget while classes are kept;
  `src/futures.rs`'s `spawn_detached_as`; docs/runtime.md §3.15). Taken from Kiln's runtime copy (`kiln-rt`'s
  `queue.rs`, `coop.rs` and `shard_loop.rs`, its goal 6 step 3, plan `01a1a59c2abe`; read at Kiln's
  `b1bcb2fa589`): the lists, the shares by time run, the virtual time a class starting to list is raised to,
  the quantum as the lead, the stall cap and `give_way` as Kiln wrote them. Changed in the port: the classes
  sit on this queue's rings and pending flags (Kiln's slab of places, its stamps of each listing and its bits
  of the classes with work, its goals 9 and 11, are not taken), and the slots' classes and each other class's
  lists are made as a task first takes that class; a spawn takes the class it is given, Normal unless
  `spawn_detached_as` names another, where Kiln's take their spawner's (a node's Critical task accepts
  connections that stay Normal); Kiln's per-class queueing counters and its blocking pool's Critical line are
  not taken. Tests: Kiln's `queue.rs` cases (the order among classes run alike, no credit for idling, no debt
  for running alone, a woken Critical slot within the lead, equal shares over time), its `coop.rs` cases
  (`give_way`, a woken Critical task on a shard) and its `tests/classes.rs`, with a spawn's class and a busy
  Critical class beside busy Normal and Background ones added.

## Made here: the compute pool (2026-11-18)

- CPU-heavy jobs off the shards, below them, in pieces (`src/compute.rs`: `start`, `run`, `run_pieces`,
  `run_here`, `running`, `stats`, `stop`, `Config::for_runtime`, `workers_for`, `Turn`, `Step`, `Refused`,
  `Priority`, `thread_priority`; `src/prioritysys.rs`, the OS-interface file that lowers a pool thread and
  reads a thread's priority back; `crossbeam-deque` 0.8, by the owner's approval of 2026-11-18;
  docs/runtime.md §3.16). Taken from Kiln's runtime copy (`kiln-rt`'s `compute.rs` and `prioritysys.rs`, its
  goal 3, plan `01a1a59c2a8e`; read at Kiln's `8d36e0d3597`, the files last changed at `27da36b65`): the
  injector and per-worker deques, the pieces and their `Turn`, the turns alternating the injector and the
  worker's own deque, admission refusing `Capacity` before anything is queued and handing the job back,
  results by one-shot with the place given back first, an unwind boundary around each piece and each drop of
  a job's values, the idle slot word and its claim by compare-and-swap (one wake a submit), one pool per
  process, the stop, and the rank below the shards (Linux's batch policy at nice +10 by `sched_setattr`, the
  nice value alone where refused; macOS's utility QoS class; Windows' below-normal level), with their notes
  and Kiln's measurements as Kiln wrote them.
- Changed in the port: the size is `Facts::cpus_at_once` less the shards (Kiln's `Grant` and `cpu_grant` read
  the same answer); `Config::for_runtime` derives the room and the piece from the runtime's own numbers, the
  shards' admission limit and step quantum, as Kiln's server derives them; the pool's rank is the one below
  (Kiln's `Rank::Idle`, measured and not taken there, is not taken). Not taken: a shard's EEVDF slice and its
  real-time class and nudge (`prefer_current_thread`, `realtime_current_thread`, `nudge_current_thread`, the
  owner's decisions after an A/B), the observer of each job's wait and piece, the pool's occupancy, idle
  workers' spare work and cold parks (Kiln's `idle.rs`), and kicks counted by sender. A real-time class is
  still read back, as the OS may report one.
- Tests: Kiln's `compute.rs` unit cases (the size, the priorities' order, a park and the one unpark that ends
  it, a stopping pool's worker), its `tests/compute.rs` (with a thread's wait, the rank stated per OS and
  statics in place of `Arc`), a pool beside a runtime taking its numbers, `run_here`, and
  `tests/compute_beside.rs`, new: a saturated pool sized from the machine's grant beside a shard whose loop
  keeps turning, each job ending on a thread below it.

## Made here: start-up hand-offs that allocate nothing (2026-11-18)

- A started thread (a shard's worker, the watchdog) says it runs through a cell of the process-wide table
  (`src/startup.rs`: `Answers`, `Answer`, `Went`; docs/runtime.md §3.17), in place of a
  `std::sync::mpsc::sync_channel` that whichever end dropped last freed, which a starter woken first could
  return before. A refused build is the worker's own return, which the rollback's join reads.
- Tests: `startup.rs`'s unit cases (every thread ran; an answer dropped unsaid; the cell given back by the
  last end) and `tests/start_memory.rs`, new: 2,000 starts on one pinned thread each leave the same bytes
  live.
