//! The watchdog: one thread per OS runtime, outside its shards, that sees a shard held in one poll past its limit
//! while the poll still runs, and reports it once, naming the task, where it was spawned, its label, how long it
//! had held the shard and whose the time was. The long-poll table (`crate::slow`) learns of a poll only as it
//! ends, so a shard wedged in one poll says nothing there until it returns. Taken from Kiln's copy of this crate
//! (ORIGIN.md says what was taken and what changed), after Seastar's stall detector and Go's sysmon.
//!
//! - **The limit** is a step's worth of polls that each keep the bounded-work contract: `batch` polls, each held
//!   at most the long-poll threshold of twice the quantum (docs/runtime.md §3.3, §3.11). A task that asks
//!   `wanted` once a quantum and yields when told to never holds its shard past that threshold, and a step polls
//!   at most `batch` tasks, so no step of such tasks holds the shard longer than `batch` × 2 × the quantum; one
//!   poll held longer delays the shard's work more than any of them could. Seastar keeps the same two marks: a
//!   task past twice its quota is counted after the fact (the long poll here), and a reactor that makes no
//!   progress past its stall threshold is reported as it happens (`blocked_reactor_notify_ms`). As a long poll's
//!   hold does, a poll's hold starts again when its task asks `wanted` and is let go on.
//! - **Looks** come every half limit, never under the timer wheel's tick (the finest wait the runtime keeps).
//!   Each look reads what the shard writes as each poll begins and ends ([`Watch`]): which poll runs, its task,
//!   the task's spawn site and its label. A poll seen at two looks is one hold, at least the time between them,
//!   so a poll first seen within a look of beginning is reported within a look of passing the limit: by twice the
//!   limit after it began.
//! - **Whose the time was**: the shard thread's CPU clock (`crate::thread_clock`, which any thread may read), read
//!   at the look that first saw the poll and at the look that finds it past the limit. More CPU than a quantum
//!   between them: the task ran past its share on the CPU, its own (the attribution's `Long`,
//!   `crate::attribution`). Less: the thread was off the CPU, and its state says whether it waits inside a call
//!   (the task's, blocked) or is runnable and not running (the host's): Linux's `/proc/self/task/<tid>/stat`
//!   state (proc(5)), macOS's Mach run state (`thread_basic_info.run_state`); elsewhere the report says off the
//!   CPU. Windows keeps a thread's CPU time at the scheduler's tick, so its verdicts there are as coarse.
//! - **What it keeps**, on each shard's registry entry, written by the watchdog alone and read by any thread
//!   without a lock ([`stalled`], [`stalls`], [`report`], as [`crate::slow::slow_polls`] reads the long-poll
//!   table): whether the shard is held now, the episodes and the time those that ended held it, and the latest
//!   report, read whole under a sequence count. Nothing grows per stall: a report replaces the one before it and
//!   its number says how many came before, so a reader that logs reports (Seastar caps its stall detector's
//!   logged reports a minute) bounds its own log by how often it reads, and a report costs the watchdog a few
//!   stores, so none is refused.
//! - **No backtrace**: Rust has no unwinder that is safe to run in a signal handler, and Windows has no signals,
//!   so a report names the task's spawn site (`#[track_caller]` at every spawn) and its label instead.

use std::panic::Location;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering, fence};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::error::RtError;
use crate::mem::Encoded;
use crate::registry::{self, SlotHolder, StaticCell};
use crate::shard::TaskId;
use crate::slow::Label;
use crate::thread_clock::CpuReading;

/// Watchdog threads running in the process: one more as each starts, one fewer as each ends.
static RUNNING: AtomicU64 = AtomicU64::new(0);

/// Watchdog threads running in this process now: one for each OS runtime started and not yet shut down or
/// dropped, whose shutdown joins it (what tells a shutdown left nothing running).
pub fn running() -> u64 {
    RUNNING.load(Ordering::Acquire)
}

/// Counts its thread out as it ends, however it ends.
struct Counted;

impl Counted {
    fn new() -> Counted {
        RUNNING.fetch_add(1, Ordering::AcqRel);
        Counted
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        let _ = RUNNING.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
    }
}

/// Derived: the tries a read of the latest report makes: one, and one more after a write it overlapped. The
/// watchdog writes a shard's report at most once a look, a timer tick or more apart, so a second try begun at once
/// overlaps another write only if its reader is held off the CPU that long; it then reads `None`, and its next
/// read sees the report whole.
const READ_TRIES: u64 = 2;

/// What a shard writes as each poll begins and ends, for the watchdog to read: on a line of its own (the largest
/// we target, Apple silicon's 128 bytes), which only the shard writes.
#[repr(align(128))]
#[derive(Debug, Default)]
pub(crate) struct Watch {
    /// Odd while a task's poll runs: one more as each poll begins and as it ends, two more as its task is let go
    /// on at `wanted` (a new hold of the same poll).
    polls: AtomicU64,
    /// The word of the task being polled, written before `polls` turns odd.
    task: AtomicU64,
    /// Where that task was spawned.
    site: StaticCell<Location<'static>>,
    /// The task's label, as the poll began or as it set it since.
    label: StaticCell<Label>,
}

/// A poll as one look read it whole.
#[derive(Clone, Copy, Debug)]
struct Running {
    polls: u64,
    task: u64,
    site: Option<&'static Location<'static>>,
    label: Option<&'static Label>,
}

impl Watch {
    /// The shard begins polling the task `word`, spawned at `site` and labelled `label`. Release on every store: a
    /// reader that sees the count odd sees the fields, and one that sees a later poll's field sees the count past
    /// this poll's.
    #[inline]
    pub(crate) fn begin(
        &self,
        word: u64,
        site: Option<&'static Location<'static>>,
        label: Option<&'static Label>,
    ) {
        self.task.store(word, Ordering::Release);
        self.site.store(site, Ordering::Release);
        self.label.store(label, Ordering::Release);
        let polls = self.polls.load(Ordering::Relaxed);
        self.polls.store(polls.wrapping_add(1), Ordering::Release);
    }

    /// The shard's poll ended (returned or unwound).
    #[inline]
    pub(crate) fn end(&self) {
        let polls = self.polls.load(Ordering::Relaxed);
        if polls & 1 == 1 {
            self.polls.store(polls.wrapping_add(1), Ordering::Release);
        }
    }

    /// The polling task asked `wanted` and was let go on: its hold starts again, as a new count still odd.
    #[inline]
    pub(crate) fn went_on(&self) {
        let polls = self.polls.load(Ordering::Relaxed);
        if polls & 1 == 1 {
            self.polls.store(polls.wrapping_add(2), Ordering::Release);
        }
    }

    /// The polling task set its label (`crate::futures::label`).
    #[inline]
    pub(crate) fn labelled(&self, label: Option<&'static Label>) {
        self.label.store(label, Ordering::Release);
    }

    /// The poll running now, read whole; `None` between polls. The count read again after the fields: the same
    /// odd count both times means no poll began between, so the fields are this poll's (`begin`'s order).
    fn running(&self) -> Option<Running> {
        let polls = self.polls.load(Ordering::Acquire);
        if polls & 1 == 0 {
            return None;
        }
        let task = self.task.load(Ordering::Acquire);
        let site = self.site.load(Ordering::Acquire);
        let label = self.label.load(Ordering::Acquire);
        (self.polls.load(Ordering::Acquire) == polls).then_some(Running {
            polls,
            task,
            site,
            label,
        })
    }
}

/// Whose the time of a stall was, as the readings tell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Whose {
    /// The task's: it ran on the CPU for more than a quantum of the time.
    Task = 1,
    /// The task's: the thread was off the CPU, waiting inside a call (blocked in the kernel).
    Blocked = 2,
    /// The host's: the thread was off the CPU while runnable (preempted, or its virtual CPU stolen).
    Host = 3,
    /// Off the CPU, and this platform cannot tell a call's wait from a preemption.
    OffCpu = 4,
    /// No reading of the thread's CPU clock.
    Unknown = 5,
}

impl Whose {
    fn from_u8(value: u8) -> Whose {
        match value {
            1 => Whose::Task,
            2 => Whose::Blocked,
            3 => Whose::Host,
            4 => Whose::OffCpu,
            _ => Whose::Unknown,
        }
    }

    /// The task's own (a bug of its code, as the bounded-work rule judges a long poll), rather than the host's or
    /// unknown.
    pub fn is_tasks(self) -> bool {
        matches!(self, Whose::Task | Whose::Blocked)
    }

    /// A word for it, as a metric's label or a log's field says it.
    pub fn as_str(self) -> &'static str {
        match self {
            Whose::Task => "task",
            Whose::Blocked => "blocking",
            Whose::Host => "host",
            Whose::OffCpu => "off_cpu",
            Whose::Unknown => "unknown",
        }
    }
}

/// What the watchdog found of one shard, written by it alone and read by anyone: on a line of its own.
#[repr(align(128))]
#[derive(Debug, Default)]
pub(crate) struct Board {
    /// Whether the shard is held in one poll past its limit now.
    stalled: AtomicBool,
    /// Polls seen held past the limit.
    episodes: AtomicU64,
    /// The time those polls held the shard, as the watchdog saw them end, nanoseconds.
    held_ns: AtomicU64,
    /// Episodes reported: the latest report's number.
    reports: AtomicU64,
    /// The latest report, read whole under `seq` (odd while the watchdog writes it).
    seq: AtomicU64,
    task: AtomicU64,
    site: StaticCell<Location<'static>>,
    label: StaticCell<Label>,
    report_held_ns: AtomicU64,
    /// The CPU the thread ran in the report's window, nanoseconds (`u64::MAX`: no reading).
    cpu_ns: AtomicU64,
    whose: AtomicU8,
}

/// A shard's stalls as the watchdog counted them ([`stalls`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stalls {
    /// Whether the shard is held in one poll past its limit now.
    pub stalled: bool,
    /// Polls seen held past the limit.
    pub episodes: u64,
    /// The time those that ended held the shard, nanoseconds (seen to within a look).
    pub held_ns: u64,
    /// Episodes reported ([`report`]): one each, as each passed the limit.
    pub reports: u64,
}

/// The latest stall reported on a shard ([`report`]).
#[derive(Clone, Copy, Debug)]
pub struct StallReport {
    /// Which report of the shard's this is, counting from one: a reader tells a new one by it.
    pub number: u64,
    /// The task whose poll held the shard.
    pub task: TaskId,
    /// Where that task was spawned (`#[track_caller]` at its spawn), when its slot recorded it.
    pub site: Option<&'static Location<'static>>,
    /// The task's label as the watchdog saw the poll (`crate::futures::label`), if it had one.
    pub label: Option<&'static Label>,
    /// How long the poll had held the shard when it was reported, nanoseconds (at least this long).
    pub held_ns: u64,
    /// The CPU the shard's thread ran while the watchdog watched the poll, nanoseconds; `None` without a reading.
    pub cpu_ns: Option<u64>,
    /// Whose the time was.
    pub whose: Whose,
}

impl Board {
    fn stalls(&self) -> Stalls {
        Stalls {
            stalled: self.stalled.load(Ordering::Acquire),
            episodes: self.episodes.load(Ordering::Relaxed),
            held_ns: self.held_ns.load(Ordering::Relaxed),
            reports: self.reports.load(Ordering::Relaxed),
        }
    }

    /// Writes a report (the watchdog's alone): `seq` odd around the fields, so a reader that sees the same even
    /// count before and after read them whole.
    fn write(&self, report: &StallReport) {
        let seq = self.seq.load(Ordering::Relaxed);
        self.seq.store(seq.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        self.task.store(report.task.0.word(), Ordering::Relaxed);
        self.site.store(report.site, Ordering::Relaxed);
        self.label.store(report.label, Ordering::Relaxed);
        self.report_held_ns.store(report.held_ns, Ordering::Relaxed);
        self.cpu_ns
            .store(report.cpu_ns.unwrap_or(u64::MAX), Ordering::Relaxed);
        self.whose.store(report.whose as u8, Ordering::Relaxed);
        self.reports.store(report.number, Ordering::Relaxed);
        self.seq.store(seq.wrapping_add(2), Ordering::Release);
    }

    /// The latest report, read whole; `None` before the first, and while a write is under way through
    /// [`READ_TRIES`] tries.
    fn read(&self) -> Option<StallReport> {
        for _ in 0..READ_TRIES {
            let before = self.seq.load(Ordering::Acquire);
            if before == 0 {
                return None;
            }
            if before & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let report = StallReport {
                number: self.reports.load(Ordering::Relaxed),
                task: TaskId(Encoded::from_word(self.task.load(Ordering::Relaxed))),
                site: self.site.load(Ordering::Relaxed),
                label: self.label.load(Ordering::Relaxed),
                held_ns: self.report_held_ns.load(Ordering::Relaxed),
                cpu_ns: Some(self.cpu_ns.load(Ordering::Relaxed)).filter(|&ns| ns != u64::MAX),
                whose: Whose::from_u8(self.whose.load(Ordering::Relaxed)),
            };
            fence(Ordering::Acquire);
            if self.seq.load(Ordering::Relaxed) == before {
                return Some(report);
            }
        }
        None
    }
}

/// Whether the watchdog sees the shard `holder` names held in one poll past its limit now: what placement asks
/// before it sends the shard new work. `false` for a shard gone, and for one no watchdog watches (a local or
/// simulated runtime's).
pub fn stalled(holder: SlotHolder) -> bool {
    registry::with_holder(holder, |entry| entry.board.stalled.load(Ordering::Acquire))
        .unwrap_or(false)
}

/// The shard's stalls as the watchdog counted them; `None` for a shard gone.
pub fn stalls(holder: SlotHolder) -> Option<Stalls> {
    registry::with_holder(holder, |entry| entry.board.stalls())
}

/// The latest stall reported on the shard; `None` before its first, and for a shard gone.
pub fn report(holder: SlotHolder) -> Option<StallReport> {
    registry::with_holder(holder, |entry| entry.board.read()).flatten()
}

/// How the watchdog judges a runtime's shards: from its configuration.
#[derive(Clone, Copy, Debug)]
struct Limits {
    batch: u64,
    /// The configured step budget: the quantum of a shard that tracks no wake.
    fixed_quantum_ns: u64,
    /// The prior of a shard that tracks its wake, its quantum until it has measured one.
    tracked_prior_ns: Option<u64>,
    /// The timer wheel's tick: the finest wait the runtime keeps, the watchdog's shortest look.
    tick_ns: u64,
}

impl Limits {
    fn of(config: &crate::runtime::RuntimeConfig) -> Limits {
        Limits {
            batch: u64::try_from(config.batch.max(1)).unwrap_or(u64::MAX),
            fixed_quantum_ns: config.step_budget_ns.max(1),
            tracked_prior_ns: config
                .wake_tracking
                .map(|tracking| tracking.prior_ns.max(1)),
            tick_ns: config.timer_tick_ns.max(1),
        }
    }

    /// The shard's quantum now: its online wake estimate while it tracks one (mirrored on its pulse at each fold;
    /// the prior before its first), else the configured step budget, as `Shard::quantum_ns` has it.
    fn quantum_ns(&self, entry: &registry::Entry) -> u64 {
        match self.tracked_prior_ns {
            Some(prior) => match entry.pulse.wake_cost_ns() {
                0 => prior,
                measured => measured,
            },
            None => self.fixed_quantum_ns,
        }
    }

    /// The limit: a step's worth of polls each held at most the long-poll threshold, twice the quantum (the
    /// module's notes).
    fn limit_ns(&self, quantum_ns: u64) -> u64 {
        quantum_ns.saturating_mul(2).saturating_mul(self.batch)
    }

    /// How long to wait between looks given the shortest limit now: half of it, never under a tick.
    fn look_every_ns(&self, shortest_limit_ns: u64) -> u64 {
        (shortest_limit_ns / 2).max(self.tick_ns)
    }
}

/// A hold the watchdog has seen running.
#[derive(Clone, Copy, Debug)]
struct Seen {
    running: Running,
    /// When the watchdog first saw it, on the monotonic clock.
    since_ns: u64,
    /// The shard thread's CPU clock then.
    cpu: Option<CpuReading>,
    /// Whether it has been judged held past the limit.
    stalled: bool,
}

/// One shard as the watchdog keeps it.
#[derive(Debug)]
struct Watched {
    holder: SlotHolder,
    seen: Option<Seen>,
}

impl Watched {
    fn new(holder: SlotHolder) -> Watched {
        Watched { holder, seen: None }
    }

    /// One look at `now_ns`: a hold seen at two looks is held at least the time between; past the limit it is a
    /// stall, marked and reported; a hold that ended ends its episode. The shard's limit, `None`
    /// for a shard gone.
    fn look(&mut self, limits: &Limits, now_ns: u64) -> Option<u64> {
        let looked = registry::with_holder(self.holder, |entry| {
            (entry.watch.running(), limits.quantum_ns(entry))
        });
        let Some((running, quantum_ns)) = looked else {
            self.end_episode(now_ns);
            self.seen = None;
            return None;
        };
        let limit_ns = limits.limit_ns(quantum_ns);
        match (running, self.seen) {
            (Some(now), Some(seen)) if seen.running.polls == now.polls => {
                if !seen.stalled && now_ns.saturating_sub(seen.since_ns) >= limit_ns {
                    // The label as it stands: the task may have set it since the look that first saw the poll.
                    self.stall(
                        Seen {
                            running: now,
                            ..seen
                        },
                        now_ns,
                        quantum_ns,
                    );
                }
            }
            _ => {
                self.end_episode(now_ns);
                self.seen = running.map(|running| Seen {
                    running,
                    since_ns: now_ns,
                    cpu: registry::shard_cpu(self.holder),
                    stalled: false,
                });
            }
        }
        Some(limit_ns)
    }

    /// The hold `seen` has held the shard past the limit: marked, counted and reported.
    fn stall(&mut self, seen: Seen, now_ns: u64, quantum_ns: u64) {
        self.seen = Some(Seen {
            stalled: true,
            ..seen
        });
        let holder = self.holder;
        let cpu_ns = seen
            .cpu
            .zip(registry::shard_cpu(holder))
            .map(|(before, after)| after.spent_ns.saturating_sub(before.spent_ns));
        let whose = match cpu_ns {
            None => Whose::Unknown,
            Some(ran) if ran > quantum_ns => Whose::Task,
            Some(_) => off_cpu(holder),
        };
        let report = StallReport {
            number: 0,
            task: TaskId(Encoded::from_word(seen.running.task)),
            site: seen.running.site,
            label: seen.running.label,
            held_ns: now_ns.saturating_sub(seen.since_ns),
            cpu_ns,
            whose,
        };
        let _ = registry::with_holder(holder, |entry| mark(&entry.board, &report));
    }

    /// The hold it saw ended (or the shard went): an episode under way ends, its time counted, its mark lifted.
    fn end_episode(&mut self, now_ns: u64) {
        let Some(seen) = self.seen.filter(|seen| seen.stalled) else {
            return;
        };
        let held_ns = now_ns.saturating_sub(seen.since_ns);
        let _ = registry::with_holder(self.holder, |entry| {
            entry.board.held_ns.fetch_add(held_ns, Ordering::Relaxed);
            entry.board.stalled.store(false, Ordering::Release);
        });
    }
}

/// Counts the episode, writes its report, numbered next, and marks the shard whose board is `board` held: the
/// mark last, with release ordering, so a reader that sees it sees the episode's count and its report.
fn mark(board: &Board, report: &StallReport) {
    board.episodes.fetch_add(1, Ordering::Relaxed);
    board.write(&StallReport {
        number: board.reports.load(Ordering::Relaxed).saturating_add(1),
        ..*report
    });
    board.stalled.store(true, Ordering::Release);
}

/// Whose the time off the CPU was: the thread's state now, where the platform tells it.
fn off_cpu(holder: SlotHolder) -> Whose {
    match registry::shard_runnable(holder) {
        Some(true) => Whose::Host,
        Some(false) => Whose::Blocked,
        None => Whose::OffCpu,
    }
}

/// A runtime's watchdog: its thread, stopped and joined when this drops.
///
/// It allocates nothing once started: what it keeps is made on the starting thread and moved to it, it waits by
/// parking and reads the stop by a receive that never blocks, and `start` returns only once the thread runs, so
/// the thread's own start is counted against the runtime's start, never against work measured after it.
#[derive(Debug)]
pub(crate) struct Watchdog {
    /// Dropped to stop the thread, which is unparked to see it.
    stop: Option<SyncSender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    /// Starts watching `holders`, the shards of a runtime built from `config`; returns once its thread runs.
    pub(crate) fn start(
        holders: &[SlotHolder],
        config: &crate::runtime::RuntimeConfig,
    ) -> Result<Watchdog, RtError> {
        let limits = Limits::of(config);
        let shards: Vec<Watched> = holders.iter().map(|&holder| Watched::new(holder)).collect();
        let (stop, stopped) = sync_channel(1);
        let (running, started) = sync_channel(1);
        #[allow(
            clippy::disallowed_methods,
            reason = "a runtime's watchdog is one thread outside its shards that the runtime starts and joins (crate::watch)"
        )]
        let thread = std::thread::Builder::new()
            .name("hyper-rt-watchdog".into())
            .spawn(move || {
                let counted = Counted::new();
                let _ = running.send(());
                // Let go now, while the starting thread still holds its end: the channel is freed there.
                drop(running);
                watch(shards, limits, &stopped);
                drop(counted);
            })
            .map_err(|error| RtError::DriverRefused {
                call: "thread spawn",
                code: error.raw_os_error(),
            })?;
        let mut watchdog = Watchdog {
            stop: Some(stop),
            thread: Some(thread),
        };
        if started.recv().is_err() {
            watchdog.stop();
            return Err(RtError::DriverRefused {
                call: "thread spawn",
                code: None,
            });
        }
        Ok(watchdog)
    }

    /// Stops the thread and joins it.
    pub(crate) fn stop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            if thread.join().is_err() {
                registry::note_unreported_failure();
            }
        }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The watchdog's thread: looks at every shard each half limit until its runtime drops its end of `stopped`.
fn watch(mut shards: Vec<Watched>, limits: Limits, stopped: &Receiver<()>) {
    let mut every_ns = limits.look_every_ns(limits.limit_ns(limits.fixed_quantum_ns));
    loop {
        // A wait that ends early (a spurious unpark) only looks sooner.
        std::thread::park_timeout(Duration::from_nanos(every_ns));
        // Until the runtime drops its end (or sends on it).
        if !matches!(stopped.try_recv(), Err(TryRecvError::Empty)) {
            break;
        }
        let now = crate::machine::clock::monotonic_ns();
        let shortest = shards
            .iter_mut()
            .filter_map(|shard| shard.look(&limits, now))
            .min();
        if let Some(shortest) = shortest {
            every_ns = limits.look_every_ns(shortest);
        }
    }
    let now = crate::machine::clock::monotonic_ns();
    for shard in &mut shards {
        shard.end_episode(now);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn report(n: u64) -> StallReport {
        StallReport {
            number: n,
            task: TaskId(Encoded::from_word(n)),
            site: Some(Location::caller()),
            label: None,
            held_ns: n,
            cpu_ns: Some(n),
            whose: Whose::Task,
        }
    }

    /// A board keeps one report however many stalls it is told of: each replaces the one before, numbered on,
    /// and every episode is counted.
    #[test]
    fn a_board_keeps_the_latest_report_and_counts_every_episode() {
        let board = Board::default();
        assert!(board.read().is_none());
        for n in 0..1000 {
            mark(&board, &report(n));
        }
        let read = board.read().unwrap();
        assert_eq!(read.number, 1000);
        assert_eq!(read.cpu_ns, Some(999), "the latest report's fields");
        let stalls = board.stalls();
        assert_eq!((stalls.episodes, stalls.reports), (1000, 1000));
        assert!(stalls.stalled);
    }

    /// The limit is a step's worth of long-poll thresholds, and a look comes every half limit, never under a tick.
    #[test]
    fn the_limit_is_batch_times_the_long_poll_threshold() {
        let limits = Limits {
            batch: 16,
            fixed_quantum_ns: 50_000,
            tracked_prior_ns: None,
            tick_ns: 1_000_000,
        };
        assert_eq!(limits.limit_ns(50_000), 1_600_000);
        assert_eq!(limits.look_every_ns(limits.limit_ns(50_000)), 1_000_000);
        assert_eq!(limits.look_every_ns(limits.limit_ns(500_000)), 8_000_000);
    }

    /// A read of the running poll pairs its count with its own fields; a go-on is a new hold of the same poll.
    #[test]
    fn a_go_on_starts_a_new_hold() {
        let watch = Watch::default();
        assert!(watch.running().is_none());
        watch.begin(7, Some(Location::caller()), None);
        let first = watch.running().unwrap();
        assert_eq!(first.task, 7);
        watch.went_on();
        let second = watch.running().unwrap();
        assert_ne!(first.polls, second.polls);
        watch.end();
        assert!(watch.running().is_none());
        watch.went_on();
        assert!(watch.running().is_none(), "no poll runs to go on");
    }
}
