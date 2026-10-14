//! `maybe_yield` yields only where the shard is wanted (docs/runtime.md §3.10): alone on its shard a task
//! that runs operations back to back goes on without the loop's round trip, and every kind of want lets
//! the others in at the task's next `maybe_yield`: a task it listed, a wake from another thread, a timer
//! fallen due, a cancellation from another thread. The simulation keeps a step a yield.
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::futures::{maybe_yield, yield_now};
use hyper_rt::runtime::{LocalRuntime, Runtime, RuntimeConfig};

/// Shape: one shard with room for a few tasks and timers; the step budget and tick are the runtime tests'
/// usual values (`tests/reclaim.rs`).
fn config(shards: u16) -> RuntimeConfig {
    RuntimeConfig {
        shards,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 64,
        ring_entries: 16,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Shape: how long the cancellation test waits for the task's report — far past a control message's
/// microseconds, short enough that a cancellation never taken fails the test rather than hanging it.
const REPORT_WAIT: Duration = Duration::from_secs(5);

/// Shape: how many conditional yields a task makes in a row; any count above one tells one poll from many.
const YIELDS: u32 = 1000;

#[test]
fn alone_on_its_shard_a_task_is_polled_once() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    let before = rt.counters().polls;
    rt.block_on(async {
        for _ in 0..YIELDS {
            maybe_yield().await;
        }
    })
    .unwrap();
    assert_eq!(rt.counters().polls - before, 1, "no want: no round trip");
    // The unconditional yield takes the round trip every time: the counter tells the two apart.
    let before = rt.counters().polls;
    rt.block_on(async {
        for _ in 0..YIELDS {
            yield_now().await;
        }
    })
    .unwrap();
    assert_eq!(rt.counters().polls - before, u64::from(YIELDS) + 1);
}

#[test]
fn a_task_listed_while_it_runs_goes_first() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    let ran_first = rt
        .block_on(async {
            let (ran, report) = channel();
            hyper_rt::futures::spawn(async move { ran.send(()).unwrap() }).unwrap();
            maybe_yield().await;
            report.try_recv().is_ok()
        })
        .unwrap();
    assert!(ran_first, "the listed task runs at the next maybe_yield");
}

/// A task that hands its waker out on its first poll and stays pending, then reports on the poll a wake
/// brings.
struct Parked {
    wakers: Sender<Waker>,
    woken: Sender<()>,
    handed_out: bool,
}

impl Future for Parked {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.handed_out {
            self.woken.send(()).unwrap();
            return Poll::Ready(());
        }
        self.handed_out = true;
        self.wakers.send(cx.waker().clone()).unwrap();
        Poll::Pending
    }
}

#[test]
fn a_wake_from_another_thread_is_let_in() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    let woken_first = rt
        .block_on(async {
            let (wakers, waker) = channel();
            let (woken, report) = channel();
            hyper_rt::futures::spawn(Parked {
                wakers,
                woken,
                handed_out: false,
            })
            .unwrap();
            // The parked task's first poll, which hands its waker out.
            yield_now().await;
            let waker = waker.try_recv().unwrap();
            // The wake lands in the shard's bitmap from another thread, before this task's next yield.
            std::thread::spawn(move || waker.wake()).join().unwrap();
            maybe_yield().await;
            report.try_recv().is_ok()
        })
        .unwrap();
    assert!(woken_first, "a foreign wake runs at the next maybe_yield");
}

#[test]
fn a_timer_fallen_due_is_let_in() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    let fired_first = rt
        .block_on(async {
            let (fired, report) = channel();
            let tick = hyper_rt::futures::timer_tick_ns();
            hyper_rt::futures::spawn(async move {
                hyper_rt::futures::sleep(tick).await.unwrap();
                fired.send(()).unwrap();
            })
            .unwrap();
            // The sleeper's first poll, which arms its timer a tick past the clock the sleep reads then: no later
            // than a tick past this reading, taken after that poll. A deadline the sleeper read for itself
            // before its sleep did is not the sleep's: a tick boundary between the two readings put the timer
            // a tick past it (12 failures in 12 runs with the readings 50 µs apart across a boundary).
            yield_now().await;
            // The wheel fires a deadline at the first tick boundary at or after it, within a tick of it: this
            // task runs until that tick has begun.
            let due = hyper_rt::futures::now_ns() + 2 * tick;
            while hyper_rt::futures::now_ns() < due {
                std::hint::spin_loop();
            }
            maybe_yield().await;
            report.try_recv().is_ok()
        })
        .unwrap();
    assert!(fired_first, "a due timer fires at the next maybe_yield");
}

/// Set once the test thread's cancellation is sent: the task's next `maybe_yield` must find it.
static CANCEL_SENT: AtomicBool = AtomicBool::new(false);

/// Reports `Dropped` when the task's future is dropped, as a cancellation does at a poll's boundary.
struct DropReport(Sender<Report>);

impl Drop for DropReport {
    fn drop(&mut self) {
        let _ = self.0.send(Report::Dropped);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Report {
    Started(hyper_rt::TaskId),
    Survived,
    Dropped,
}

#[test]
fn a_cancellation_from_another_thread_is_let_in() {
    let rt = Runtime::start(&config(1)).unwrap();
    let shard = rt.shard_ids()[0];
    let (reports, report) = channel();
    rt.spawn_on(shard, async move {
        let guard = DropReport(reports.clone());
        reports
            .send(Report::Started(hyper_rt::futures::current_task().unwrap()))
            .unwrap();
        // The task holds its shard until the cancellation is in the shard's control queue.
        while !CANCEL_SENT.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        maybe_yield().await;
        reports.send(Report::Survived).unwrap();
        drop(guard);
    })
    .unwrap();
    let Report::Started(task) = report.recv_timeout(REPORT_WAIT).unwrap() else {
        panic!("the task reports its start first");
    };
    rt.cancel(task).unwrap();
    CANCEL_SENT.store(true, Ordering::Release);
    assert_eq!(report.recv_timeout(REPORT_WAIT).unwrap(), Report::Dropped);
    rt.shutdown().unwrap();
}

// The simulation's order of polls is the fact under test; no I/O or advancing clock takes part in it.
struct IdleDriver;

impl Driver for IdleDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        0
    }
    fn wait(&mut self, _timeout: Option<u64>, _out: &mut Vec<Completion>) -> Result<(), RtError> {
        Ok(())
    }
    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }
    fn arm(
        &mut self,
        _raw: i32,
        _want: hyper_rt::interests::Readiness,
        _tag: u64,
    ) -> Result<(), RtError> {
        Err(RtError::BadConfig {
            what: "the maybe_yield fixture registers no native I/O",
        })
    }
    fn has_pending(&self) -> bool {
        false
    }
}

#[test]
fn the_simulation_keeps_a_step_a_yield() {
    let seed: DriverSeed = Box::new(|_| Ok(Box::new(IdleDriver) as Box<dyn Driver>));
    let mut rt = LocalRuntime::with_driver(&config(1), seed, Kick::None).unwrap();
    let before = rt.counters().polls;
    rt.block_on(async {
        for _ in 0..YIELDS {
            maybe_yield().await;
        }
    })
    .unwrap();
    assert_eq!(rt.counters().polls - before, u64::from(YIELDS) + 1);
}
