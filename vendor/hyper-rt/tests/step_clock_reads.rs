//! A step reads its shard's clock twice, at its start and after its polls, however many tasks it polls
//! (mantle `docs/design/event-loop.md` D3). A reading after every poll was a third of a shard's samples where
//! each poll does little (`benchmark-results/hyper-rt-vs-tokio-20261010/runs/spawnd-profile/sample.txt`:
//! 1,134 of 2,915 samples of the step in `mach_continuous_time`), and the reading bought nothing a step's
//! two do not: the watchdog and its attribution judge the step, and a client's idle window opens where
//! the step ended.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::missing_panics_doc
)]

use std::cell::Cell;

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: the tasks one step polls: enough that a reading a poll is unmistakable.
const TASKS: usize = 256;

thread_local! {
    /// The driver clock's readings on this thread (each test's shard runs on its own test thread).
    static READS: Cell<u64> = const { Cell::new(0) };
}

fn reads() -> u64 {
    READS.with(Cell::get)
}

/// A driver whose clock counts its readings and never moves.
struct Counting;

impl Driver for Counting {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        READS.with(|reads| reads.set(reads.get() + 1));
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

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: TASKS,
        timers_per_shard: 1,
        interests_per_shard: 1,
        ring_entries: 1,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: TASKS,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

/// Do: spawn [`TASKS`] tasks that each end at their first poll, and run one step. Expect: the step polls
/// every one of them and reads the clock twice.
#[test]
fn a_step_reads_the_clock_twice_however_many_tasks_it_polls() {
    let mut rt = LocalRuntime::with_driver(
        &config(),
        Box::new(|_kick| Ok(Box::new(Counting) as Box<dyn Driver>)),
        Kick::None,
    )
    .unwrap();
    for _ in 0..TASKS {
        rt.spawn(async {}).unwrap();
    }
    let before = reads();
    assert!(rt.step().did_work);
    let read = reads() - before;
    let counters = rt.counters();
    assert_eq!(
        counters.polls,
        u64::try_from(TASKS).unwrap(),
        "{counters:?}"
    );
    assert_eq!(read, 2, "clock readings in a step of {TASKS} polls");
}
