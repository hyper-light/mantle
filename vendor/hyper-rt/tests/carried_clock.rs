//! The shard's clock carried by the counter between the loop's readings of it (docs/runtime.md §3.9, "a
//! poll's end from the counter"): a task's live readings never run backwards, within a step or across the
//! step's new reading of the driver's clock, and a poll held past the quantum still reads as long.

#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::time::{Duration, Instant};

use hyper_rt::futures::yield_now;
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: the quantum of the long-poll case, a millisecond, far above a yielding poll's own cost.
const QUANTUM_NS: u64 = 1_000_000;

fn config(batch: usize, step_budget_ns: u64) -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 4,
        timers_per_shard: 4,
        interests_per_shard: 4,
        ring_entries: 16,
        step_budget_ns,
        timer_tick_ns: 100_000,
        batch,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// The shard's clock as a task reads it.
fn now_ns() -> u64 {
    registry::with_current(|ctx| ctx.now_ns()).unwrap()
}

/// Do: a task reads the shard's clock twice a poll and yields, through steps of one poll and of several, so
/// readings fall within a step and on both sides of each step's new reading of the driver's clock. Expect:
/// every reading at or after the one before it, and the readings advancing over the run.
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_tasks_readings_never_run_backwards_across_steps() {
    for batch in [1, 8] {
        let mut rt = LocalRuntime::new(&config(batch, QUANTUM_NS)).unwrap();
        let (first, last) = rt
            .block_on(async move {
                let first = now_ns();
                let mut last = first;
                // Each batch's polls end a step: a reading on either side of every step boundary of the run.
                for _ in 0..batch.saturating_mul(64) {
                    for _ in 0..2 {
                        let now = now_ns();
                        assert!(now >= last, "{now} after {last} (batch {batch})");
                        last = now;
                    }
                    yield_now().await;
                }
                (first, last)
            })
            .unwrap();
        assert!(last > first, "the readings advanced: {first} to {last}");
        assert!(rt.counters().steps > 1, "{:?}", rt.counters());
    }
}

/// Do: a task's poll holds its thread three quanta by the wall clock, busy. Expect: the poll reads as
/// long by the step's reading carried by the counter, as by the driver's clock: the loop judges it (the
/// first long poll has no window yet, so it is counted unattributed), and its longest step spans the hold
/// to within a quantum.
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_poll_held_past_the_quantum_reads_as_long() {
    let hold = Duration::from_nanos(QUANTUM_NS.saturating_mul(3));
    let mut rt = LocalRuntime::new(&config(1, QUANTUM_NS)).unwrap();
    rt.block_on(async move {
        let began = Instant::now();
        while began.elapsed() < hold {
            std::hint::spin_loop();
        }
    })
    .unwrap();
    let counters = rt.counters();
    assert!(
        counters.long_steps + counters.preempted_steps + counters.unattributed_steps >= 1,
        "{counters:?}"
    );
    // The poll's length by the carried clock: past the hold less a quantum, which covers the reads' own
    // spread (the counter read ahead of the poll's last instructions, the hold timed on another clock).
    assert!(
        counters.longest_step_ns >= u64::try_from(hold.as_nanos()).unwrap() - QUANTUM_NS,
        "{counters:?}"
    );
}
