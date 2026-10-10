//! A task that wakes itself while it is polled — a yield — goes again after the tasks ready before it and
//! after wakes that came from other threads meanwhile (mantle `docs/design/event-loop.md` D7). A task that
//! slices long work and yields between slices then holds a request that arrives from another thread for at
//! most the slice in hand, as tokio's yield (its `defer`) and trantor's single queue do, not for its own
//! next slice as well.

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

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::task::{Context, Poll, Waker};

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: the polls a step may take, enough for several rounds of the yielding task.
const BATCH: usize = 4;

thread_local! {
    /// The yielding task's polls so far.
    static YIELDS: Cell<u64> = const { Cell::new(0) };
    /// The yielding task's polls when the woken task ran; `u64::MAX` until it has.
    static SEEN: Cell<u64> = const { Cell::new(u64::MAX) };
}

/// A driver with no clock and nothing to complete: the steps run exactly what is queued.
struct Quiet;

impl Driver for Quiet {
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
        Ok(())
    }
    fn has_pending(&self) -> bool {
        false
    }
}

/// A task that wakes itself at every poll and is never done. With `wakes`, its first poll also has another
/// thread wake the waker the channel hands it, and waits until that wake has landed.
struct Yielder {
    wakes: Option<Receiver<Waker>>,
}

impl Future for Yielder {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        YIELDS.with(|count| count.set(count.get() + 1));
        if let Some(wakes) = self.wakes.take() {
            let waker = wakes.recv().unwrap();
            std::thread::scope(|scope| {
                scope.spawn(move || waker.wake());
            });
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A task that hands its waker over at its first poll, and at its second notes how many polls the yielder
/// had run.
struct Woken {
    wakers: Sender<Waker>,
    polled: bool,
}

impl Future for Woken {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.polled {
            SEEN.with(|seen| seen.set(YIELDS.with(Cell::get)));
            return Poll::Ready(());
        }
        self.polled = true;
        self.wakers.send(cx.waker().clone()).unwrap();
        Poll::Pending
    }
}

fn runtime() -> LocalRuntime {
    YIELDS.with(|count| count.set(0));
    SEEN.with(|seen| seen.set(u64::MAX));
    LocalRuntime::with_driver(
        &RuntimeConfig {
            shards: 1,
            tasks_per_shard: 4,
            timers_per_shard: 1,
            interests_per_shard: 1,
            ring_entries: 1,
            step_budget_ns: 1_000_000,
            timer_tick_ns: 100_000,
            batch: BATCH,
            pin: false,
            cores: Vec::new(),
            page_bytes: 4096,
            spin_ns: 0,
            wake_tracking: None,
        },
        Box::new(|_kick| Ok(Box::new(Quiet) as Box<dyn Driver>)),
        Kick::None,
    )
    .unwrap()
}

/// Do: run a yielding task beside a task waiting for a wake, then wake the waiting task from another thread
/// between two steps. Expect: the next step polls the woken task before the yielder's next poll.
#[test]
fn a_wake_from_another_thread_runs_before_a_yielder_goes_again() {
    let mut rt = runtime();
    rt.spawn(Yielder { wakes: None }).unwrap();
    let (wakers, handed) = channel();
    rt.spawn(Woken {
        wakers,
        polled: false,
    })
    .unwrap();
    assert!(rt.step().did_work);
    let waker = handed
        .try_recv()
        .expect("the waiting task handed over its waker");
    std::thread::scope(|scope| {
        scope.spawn(move || waker.wake());
    });
    let before = YIELDS.with(Cell::get);
    assert!(rt.step().did_work);
    assert_eq!(SEEN.with(Cell::get), before, "the woken task ran first");
}

/// Do: run a yielding task whose first poll has another thread wake a waiting task. Expect: that step ends
/// after the yield, with the yielder polled once, and the next step polls the woken task before the
/// yielder goes again: a yield gives way to work that came in from outside meanwhile.
#[test]
fn a_wake_that_lands_while_a_task_yields_ends_the_step_before_it_goes_again() {
    let mut rt = runtime();
    let (wakers, handed) = channel();
    rt.spawn(Woken {
        wakers,
        polled: false,
    })
    .unwrap();
    assert!(rt.step().did_work);
    let (to_yielder, wakes) = channel();
    to_yielder
        .send(
            handed
                .try_recv()
                .expect("the waiting task handed over its waker"),
        )
        .unwrap();
    rt.spawn(Yielder { wakes: Some(wakes) }).unwrap();
    let polls = rt.counters().polls;
    assert!(rt.step().did_work);
    assert_eq!(rt.counters().polls - polls, 1, "the yield ended the step");
    assert!(rt.step().did_work);
    assert_eq!(
        SEEN.with(Cell::get),
        1,
        "the woken task ran before the yielder's second poll"
    );
}
