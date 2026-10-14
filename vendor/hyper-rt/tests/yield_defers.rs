//! A task that wakes itself in its own poll, by `yield_now` or any future with more to do, resumes only
//! after the loop has read its inputs again: a wake from another thread that lands while it runs is
//! polled before it runs again (`hyper_rt::queue`). Before, the task was re-listed in the step's FIFO at
//! once: alone there, it was polled again and again until the step's budget ran out, while the wake
//! waited in the shard's bitmap for the step to end.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::mpsc::channel;
use std::task::{Context, Poll, Waker};

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

// The order of polls is the fact under test; no I/O or advancing clock takes part in it.
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
            what: "the yield fixture registers no native I/O",
        })
    }
    fn has_pending(&self) -> bool {
        false
    }
}

/// One shard for `tasks` tasks whose step may poll `batch` times.
fn owner(tasks: usize, batch: usize) -> LocalRuntime {
    let config = RuntimeConfig {
        shards: 1,
        tasks_per_shard: tasks,
        timers_per_shard: tasks,
        interests_per_shard: tasks,
        ring_entries: tasks,
        step_budget_ns: 1,
        timer_tick_ns: 1,
        batch,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    };
    let seed: DriverSeed = Box::new(|_| Ok(Box::new(IdleDriver) as Box<dyn Driver>));
    LocalRuntime::with_driver(&config, seed, Kick::None).unwrap()
}

#[derive(Debug, PartialEq, Eq)]
enum Fact {
    Yielder,
    Waiter,
}

/// Pending once with its own waker woken, as a future with more to do returns.
struct SelfWake(bool);

impl Future for SelfWake {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A task that lets others go first between its polls, by `yield_now` or by `SelfWake`, and a waiter that
/// it wakes from another thread once the waiter has parked: the first five polls, in their order.
fn first_polls(by_yield_now: bool) -> Vec<Fact> {
    // Room for eight polls a step: the yielder alone could fill them.
    let mut rt = owner(2, 8);
    let (facts, observed) = channel();
    let (wakers, parked) = channel::<Waker>();
    let yielder_facts = facts.clone();
    let yielder = rt
        .spawn(async move {
            loop {
                yielder_facts.send(Fact::Yielder).unwrap();
                // A wake from another thread lands while this task runs: the waiter's, once it has parked.
                if let Ok(waker) = parked.try_recv() {
                    std::thread::spawn(move || waker.wake()).join().unwrap();
                }
                if by_yield_now {
                    hyper_rt::futures::yield_now().await;
                } else {
                    SelfWake(false).await;
                }
            }
        })
        .unwrap();
    let waiter = rt
        .spawn(async move {
            loop {
                facts.send(Fact::Waiter).unwrap();
                // Pending until its waker, handed to the yielder, is woken from another thread.
                let mut handed = false;
                poll_fn(|cx| {
                    if handed {
                        return Poll::Ready(());
                    }
                    handed = true;
                    wakers.send(cx.waker().clone()).unwrap();
                    Poll::Pending
                })
                .await;
            }
        })
        .unwrap();
    let mut seen = Vec::new();
    for _ in 0..4 {
        rt.step();
        seen.extend(observed.try_iter());
        if seen.len() >= 5 {
            break;
        }
    }
    rt.context().cancel(yielder).unwrap();
    rt.context().cancel(waiter).unwrap();
    rt.run_until_idle();
    assert_eq!(rt.counters().cancelled, 2);
    seen.truncate(5);
    seen
}

/// The order the wake from another thread must keep: it runs before the yielder runs again.
const WAKE_FIRST: [Fact; 5] = [
    Fact::Yielder,
    Fact::Waiter,
    Fact::Yielder,
    Fact::Waiter,
    Fact::Yielder,
];

#[test]
fn a_wake_from_another_thread_goes_ahead_of_a_task_that_yielded() {
    assert_eq!(first_polls(true), WAKE_FIRST);
}

#[test]
fn a_wake_from_another_thread_goes_ahead_of_a_task_that_woke_itself() {
    assert_eq!(first_polls(false), WAKE_FIRST);
}

#[test]
fn a_task_that_only_yields_keeps_the_loop_stepping() {
    // Nothing else is ready and nothing will wake the shard: the yields alone must carry it to the end,
    // through the owning loop's park decisions (`run_until_idle` returns only when idle).
    let mut rt = owner(1, 8);
    let (facts, observed) = channel();
    rt.spawn(async move {
        for _ in 0..5 {
            facts.send(()).unwrap();
            hyper_rt::futures::yield_now().await;
        }
    })
    .unwrap();
    rt.run_until_idle();
    assert_eq!(observed.try_iter().count(), 5);
    assert_eq!(rt.counters().completed, 1);
}
