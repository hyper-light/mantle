//! A busy shard's step takes as many polls as fill its step budget at the cost its last step measured a poll,
//! so what it looks at between steps — its inboxes, timers and driver — waits about one budget, not a
//! configured count of polls however long they run (mantle `docs/design/event-loop.md` D6). The allowance
//! starts at one poll and opens by doubling until a step's polls fill the budget; the configured batch is
//! only the cap. With a fixed count, a task that yields after each slice of work held its shard for the count
//! times the slice: a cooperative client slicing by a 50 µs quantum would hold it for tens of milliseconds
//! behind a count sized for polls of a few hundred nanoseconds.
//!
//! The driver here is a clock the tasks move: each poll costs what its task adds, so every step's length is
//! exact and the allowance it leaves is too.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::disallowed_macros,
    clippy::missing_panics_doc
)]

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: the step budget, fifty microseconds.
const BUDGET_NS: u64 = 50_000;
/// Shape: the configured batch, the cap: twenty times what fits the budget at [`COST_NS`] a poll.
const CAP: usize = 2_000;
/// Shape: what one poll costs on the virtual clock, so a hundred fill the budget.
const COST_NS: u64 = 500;

thread_local! {
    /// The virtual clock of this test thread's shard.
    static NOW: Cell<u64> = const { Cell::new(0) };
}

/// A driver whose clock is [`NOW`], which only the tasks move. It is not the simulation's driver, so the
/// shard times its steps by this clock as it would by a real one.
struct Virtual;

impl Driver for Virtual {
    fn kind(&self) -> DriverKind {
        DriverKind::Epoll
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        NOW.with(Cell::get)
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

/// A task that yields after every poll, each poll costing the next of `costs` on the virtual clock (the last
/// one repeating).
struct Slices {
    costs: Vec<u64>,
    next: usize,
}

impl Future for Slices {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let at = self.next.min(self.costs.len() - 1);
        let cost = self.costs[at];
        NOW.with(|now| now.set(now.get() + cost));
        self.next += 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn runtime() -> LocalRuntime {
    NOW.with(|now| now.set(0));
    LocalRuntime::with_driver(
        &RuntimeConfig {
            shards: 1,
            tasks_per_shard: 4,
            timers_per_shard: 1,
            interests_per_shard: 1,
            ring_entries: 1,
            step_budget_ns: BUDGET_NS,
            timer_tick_ns: BUDGET_NS,
            batch: CAP,
            pin: false,
            cores: Vec::new(),
            page_bytes: 4096,
            spin_ns: 0,
            wake_tracking: None,
        },
        Box::new(|_kick| Ok(Box::new(Virtual) as Box<dyn Driver>)),
        Kick::None,
    )
    .unwrap()
}

/// The polls each of `steps` steps ran.
fn polls_per_step(rt: &mut LocalRuntime, steps: usize) -> Vec<u64> {
    (0..steps)
        .map(|_| {
            let before = rt.counters().polls;
            assert!(rt.step().did_work);
            rt.counters().polls - before
        })
        .collect()
}

/// Do: run a task that yields after every poll of 500 ns. Expect: the steps open from one poll, doubling
/// (nothing is measured yet, and a step is not trusted past twice the last), until a step's polls fill the
/// 50 µs budget: a hundred a step from then on.
#[test]
fn a_step_takes_the_polls_that_fill_its_budget() {
    let mut rt = runtime();
    rt.spawn(Slices {
        costs: vec![COST_NS],
        next: 0,
    })
    .unwrap();
    assert_eq!(
        polls_per_step(&mut rt, 10),
        vec![1, 2, 4, 8, 16, 32, 64, 100, 100, 100]
    );
}

/// Do: run a task whose first poll costs ten budgets and whose later polls cost nothing on the clock.
/// Expect: the step after the long poll takes one poll (no more fit), and each step after that twice its
/// predecessor's (a step the clock reads as no time is not trusted past twofold), as far as the cap.
#[test]
fn after_a_long_step_the_allowance_shrinks_to_the_fit_then_doubles_back_to_the_cap() {
    let mut rt = runtime();
    rt.spawn(Slices {
        costs: vec![10 * BUDGET_NS, 0],
        next: 0,
    })
    .unwrap();
    assert_eq!(
        polls_per_step(&mut rt, 14),
        vec![
            1, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 2_000, 2_000
        ]
    );
}
