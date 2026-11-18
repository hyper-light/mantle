//! The watchdog (`hyper_rt::watch`): a shard held in one poll past its limit (`batch` polls each held at most the
//! long-poll threshold, twice the quantum) is reported while the poll still runs, once, with its task, the task's
//! spawn site and label, how long it had held the shard and whose the time was; a shard whose polls keep to their
//! quantum, by yielding or by asking `wanted` and being let go on, is never reported; and a shard's store keeps one
//! report and its counts however many stalls it has, each stall reported once.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_possible_truncation
)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel};
use std::time::{Duration, Instant};

use hyper_rt::futures;
use hyper_rt::machine::clock::monotonic_ns;
use hyper_rt::runtime::{Runtime, RuntimeConfig};
use hyper_rt::slow::Label;
use hyper_rt::watch::{self, Whose};

/// Shape: a fixed quantum of a millisecond (no wake tracking), far past any wake.
const QUANTUM_NS: u64 = 1_000_000;
/// Shape: a batch of four: a limit of four polls of twice the quantum, eight milliseconds.
const BATCH: usize = 4;
const LIMIT_NS: u64 = BATCH as u64 * 2 * QUANTUM_NS;

/// Shape: how long a test waits for what the watchdog does before it fails: far past a look (half the limit) on a
/// loaded host.
const WAIT: Duration = Duration::from_secs(10);

static WEDGE: Label = Label::new("Wedge");

fn config(shards: u16) -> RuntimeConfig {
    RuntimeConfig {
        shards,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 128,
        ring_entries: 64,
        step_budget_ns: QUANTUM_NS,
        timer_tick_ns: QUANTUM_NS / 10,
        batch: BATCH,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// A release the test sets and a wedged task reads from another thread.
fn release() -> &'static AtomicBool {
    Box::leak(Box::new(AtomicBool::new(false)))
}

/// Spins on the CPU until `release` is set: a poll with no await, holding its shard.
fn hold_until(release: &AtomicBool) {
    while !release.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
}

/// Waits until `test` holds, failing past [`WAIT`].
fn wait_for(what: &str, mut test: impl FnMut() -> bool) {
    let until = Instant::now() + WAIT;
    while !test() {
        assert!(Instant::now() < until, "waited {WAIT:?} for {what}");
        std::thread::yield_now();
    }
}

/// The report of the wedge spawned at `line` of this file names its task `wedge`, its site, its label and a hold
/// past the limit, and, where the platform tells, time run on the CPU.
fn assert_names_the_wedge(report: &watch::StallReport, wedge: hyper_rt::TaskId, line: u32) {
    assert_eq!(report.number, 1);
    assert_eq!(report.task, wedge, "the report names the wedging task");
    let site = report.site.expect("the task's spawn site");
    assert!(site.file().ends_with("watchdog.rs"), "{site}");
    assert_eq!(site.line(), line, "the spawn's own line: {site}");
    assert!(
        report
            .label
            .is_some_and(|label| std::ptr::eq(label, &WEDGE)),
        "{report:?}"
    );
    assert!(report.held_ns >= LIMIT_NS, "{report:?}");
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        // A spin runs on the CPU: the task's, unless the host took the CPU away for most of the window.
        assert!(
            matches!(report.whose, Whose::Task | Whose::Host),
            "{report:?}"
        );
    }
}

/// A shard wedged by a task that spins without awaiting is reported while it is held: the report names the
/// task, its spawn site (this file, the spawn's line) and the label it set, a hold of at least the limit and,
/// where the platform tells, that the time ran on the CPU; the other shard is not marked; once the poll ends the
/// mark lifts and the episode is counted with its time, reported once.
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue, an eventfd or a port, which Miri does not model
fn a_wedged_shard_is_reported_with_its_task_spawn_site_and_label() {
    let rt = Runtime::start(&config(2)).unwrap();
    let (a, b) = (rt.shard_ids()[0], rt.shard_ids()[1]);
    let (holder_a, holder_b) = (rt.holder_of(a).unwrap(), rt.holder_of(b).unwrap());
    let release = release();
    let (started, wedge_task) = channel();
    let wedging = async move {
        futures::label(Some(&WEDGE)).unwrap();
        started.send(futures::current_task().unwrap()).unwrap();
        hold_until(release);
    };
    #[rustfmt::skip]
    let (line, spawned) = (line!(), rt.spawn_on(a, wedging));
    spawned.unwrap();
    let wedge = wedge_task.recv_timeout(WAIT).unwrap();
    let wedged_at = monotonic_ns();
    wait_for("the watchdog to mark A held", || watch::stalled(holder_a));
    assert!(
        monotonic_ns() - wedged_at >= LIMIT_NS / 2,
        "not marked before the limit could pass"
    );
    assert!(!watch::stalled(holder_b), "B is not held");

    let report = watch::report(holder_a).expect("a report of A's stall");
    assert_names_the_wedge(&report, wedge, line);

    release.store(true, Ordering::Release);
    wait_for("A's mark to lift", || !watch::stalled(holder_a));
    let stalls = watch::stalls(holder_a).unwrap();
    assert_eq!((stalls.episodes, stalls.reports), (1, 1), "{stalls:?}");
    assert!(stalls.held_ns >= LIMIT_NS, "{stalls:?}");
    assert_eq!(watch::stalls(holder_b).unwrap().episodes, 0);
    rt.shutdown().unwrap();
}

/// A poll that waits inside a call (a blocking receive, off the CPU and not runnable) is the task's, blocked,
/// where the platform tells a call's wait from a preemption (Linux's `/proc`, macOS's run state).
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue, an eventfd or a port, which Miri does not model
fn a_poll_blocked_in_a_call_is_reported_as_the_tasks() {
    let rt = Runtime::start(&config(1)).unwrap();
    let holder = rt.holder_of(rt.shard_ids()[0]).unwrap();
    let (unblock, blocked) = sync_channel::<()>(1);
    rt.spawn_on(rt.shard_ids()[0], async move {
        futures::label(Some(&WEDGE)).unwrap();
        let _ = blocked.recv();
    })
    .unwrap();
    wait_for("the watchdog to mark the shard held", || {
        watch::stalled(holder)
    });
    let report = watch::report(holder).unwrap();
    assert!(report.label.is_some_and(|label| label.name == WEDGE.name));
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        assert_eq!(report.whose, Whose::Blocked, "{report:?}");
        assert!(report.whose.is_tasks());
    }
    unblock.send(()).unwrap();
    wait_for("the mark to lift", || !watch::stalled(holder));
    rt.shutdown().unwrap();
}

/// A shard whose polls keep to their quantum is never marked: a task that spins half a quantum and yields, and
/// one that runs one poll for as long, asking `wanted` each half quantum and let go on (a new hold each time),
/// both for many limits. A loaded host may hold the shard's thread off the CPU past a limit; such an episode is
/// the host's.
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue, an eventfd or a port, which Miri does not model
fn a_shard_whose_polls_keep_to_their_quantum_is_never_reported() {
    let rt = Runtime::start(&config(2)).unwrap();
    let (a, b) = (rt.shard_ids()[0], rt.shard_ids()[1]);
    let (done, finished) = channel();
    let spin_half_a_quantum = || {
        let piece = monotonic_ns() + QUANTUM_NS / 2;
        while monotonic_ns() < piece {
            std::hint::spin_loop();
        }
    };
    let yielding_done = done.clone();
    rt.spawn_on(a, async move {
        let end = monotonic_ns() + 50 * LIMIT_NS;
        while monotonic_ns() < end {
            spin_half_a_quantum();
            futures::yield_now().await;
        }
        yielding_done.send(()).unwrap();
    })
    .unwrap();
    rt.spawn_on(b, async move {
        let end = monotonic_ns() + 50 * LIMIT_NS;
        while monotonic_ns() < end {
            spin_half_a_quantum();
            futures::maybe_yield().await;
        }
        done.send(()).unwrap();
    })
    .unwrap();
    finished.recv_timeout(WAIT).unwrap();
    finished.recv_timeout(WAIT).unwrap();
    for shard in [a, b] {
        let holder = rt.holder_of(shard).unwrap();
        let stalls = watch::stalls(holder).unwrap();
        assert!(
            stalls.episodes == 0
                || watch::report(holder).is_some_and(|report| report.whose == Whose::Host),
            "{shard:?}: {stalls:?}"
        );
    }
    rt.shutdown().unwrap();
}

/// A shard's store stays one report and its counts however many stalls it has: wedged many times, each past the
/// limit with the mark lifted between, it counts every episode, reports each once, and keeps only the latest
/// report, which names the latest wedge's task.
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue, an eventfd or a port, which Miri does not model
fn the_store_stays_bounded_under_many_stalls() {
    /// Shape: stalls enough that a store growing per stall would show it.
    const EPISODES: u64 = 16;
    let rt = Runtime::start(&config(1)).unwrap();
    let a = rt.shard_ids()[0];
    let holder = rt.holder_of(a).unwrap();
    for episode in 1..=EPISODES {
        let release = release();
        let (started, wedge_task) = channel();
        rt.spawn_on(a, async move {
            started.send(futures::current_task().unwrap()).unwrap();
            hold_until(release);
        })
        .unwrap();
        let wedge = wedge_task.recv_timeout(WAIT).unwrap();
        wait_for("the wedge's mark", || watch::stalled(holder));
        let report = watch::report(holder).unwrap();
        assert_eq!((report.number, report.task), (episode, wedge));
        release.store(true, Ordering::Release);
        wait_for("the mark to lift", || !watch::stalled(holder));
        let stalls = watch::stalls(holder).unwrap();
        assert_eq!((stalls.episodes, stalls.reports), (episode, episode));
    }
    rt.shutdown().unwrap();
}
