//! The thread's CPU account (a system call) is read only while step attribution is armed: a shard whose
//! steps never pass their bound never arms, and reads it not once however many steps it runs; and a shard
//! that armed stops reading at the first step within its bound, waits or not (`attribution::Tracker`,
//! `Counters::thread_accounts`).

#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::arithmetic_side_effects
)]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick};
use hyper_rt::futures::yield_now;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 16,
        ring_entries: 16,
        // A quantum no yield-and-return poll can pass: no poll is long, so nothing arms.
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        // One poll a step, so each yield below is a step of its own: a step's poll budget also covers the
        // wakes its own polls make (`queue.rs`), and at a larger batch the thousand yields ran in a few dozen
        // steps, leaving this test's count of steps short of the thousand it names.
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

#[test]
fn an_unarmed_shard_reads_no_thread_account() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        for _ in 0..1_000 {
            yield_now().await;
        }
    })
    .unwrap();
    let counters = rt.counters();
    assert!(counters.steps >= 1_000, "{counters:?}");
    assert_eq!(counters.long_steps, 0, "{counters:?}");
    assert_eq!(counters.thread_accounts, 0, "{counters:?}");
}

/// Shape: the step budget of the never-waiting shard, a millisecond: its bound is two.
const BUDGET_NS: u64 = 1_000_000;
/// Shape: the polls after the long one, each a step of its own.
const SHORT_STEPS: u64 = 1_000;

thread_local! {
    /// The virtual clock of this test thread's never-waiting shard: only its task moves it, so no host
    /// preemption can make a step long.
    static NOW: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// A driver whose clock is [`NOW`]. It is not the simulation's driver, so the shard times and attributes its
/// steps by this clock as it would by a real one.
struct Virtual;

impl Driver for Virtual {
    fn kind(&self) -> DriverKind {
        DriverKind::Epoll
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        NOW.with(std::cell::Cell::get)
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

/// A task whose first poll takes three budgets on the virtual clock (past a step's bound of two) and whose
/// next [`SHORT_STEPS`] polls take none, each yielding.
struct LongThenShort {
    polls: u64,
}

impl Future for LongThenShort {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.polls == 0 {
            NOW.with(|now| now.set(now.get() + 3 * BUDGET_NS));
        }
        if self.polls == SHORT_STEPS {
            return Poll::Ready(());
        }
        self.polls += 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Do: run a long step and then a thousand short ones, one poll each, the shard never waiting between them.
/// Expect: two reads of the account where the OS keeps one per thread — the long step's own, and the start
/// of the step after it, which ran within its bound and so disarmed — and one elsewhere (no thread clock,
/// so nothing arms). Before, an armed shard read at every step's start until it next waited, and one that
/// never waits read at every step.
#[test]
fn a_shard_that_never_waits_stops_reading_at_its_first_step_within_bound() {
    NOW.with(|now| now.set(0));
    let mut rt = LocalRuntime::with_driver(
        &RuntimeConfig {
            step_budget_ns: BUDGET_NS,
            ..config()
        },
        Box::new(|_kick| Ok(Box::new(Virtual) as Box<dyn Driver>)),
        Kick::None,
    )
    .unwrap();
    rt.spawn(LongThenShort { polls: 0 }).unwrap();
    while rt.step().did_work {}
    let counters = rt.counters();
    assert!(counters.steps > SHORT_STEPS, "{counters:?}");
    let expected = if cfg!(any(target_os = "linux", target_os = "macos")) {
        2
    } else {
        1
    };
    assert_eq!(counters.thread_accounts, expected, "{counters:?}");
}
