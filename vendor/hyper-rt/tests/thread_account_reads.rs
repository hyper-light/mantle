//! The thread's CPU account (a system call) is read only while poll attribution is armed: a shard whose
//! polls never pass the step quantum never arms, and reads it not once however many steps it runs; and a
//! shard that never waits, which keeps the arming one long poll gave it, reads it at most twice a long poll
//! (`attribution::Tracker`, `Counters::thread_accounts`).

// A real shard's clock is the fact under test: one poll is held past its quantum on the host's time.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::arithmetic_side_effects
)]

use std::time::{Duration, Instant};

use hyper_rt::futures::yield_now;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: a quantum no yield-and-return poll passes unless the host holds its thread that long.
const QUANTUM_NS: u64 = 200_000;

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
        latency_objective_ns: None,
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

#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_shard_that_never_waits_reads_at_most_twice_a_long_poll() {
    // A poll held four quanta arms the attribution, then two tasks only yield; the steps run back to back,
    // so no wait ever disarms it. Every step's start read the account before, long after the last long
    // poll: 3.3% of a yielding shard's samples (`docs/benchmarks.md`, "hyper-rt: the loop under load").
    let mut rt = LocalRuntime::new(&RuntimeConfig {
        step_budget_ns: QUANTUM_NS,
        batch: 16,
        ..config()
    })
    .unwrap();
    rt.spawn(async {
        let began = Instant::now();
        while began.elapsed() < Duration::from_nanos(4 * QUANTUM_NS) {
            std::hint::spin_loop();
        }
        loop {
            yield_now().await;
        }
    })
    .unwrap();
    rt.spawn(async {
        loop {
            yield_now().await;
        }
    })
    .unwrap();
    for _ in 0..2_000 {
        assert!(rt.step().did_work);
    }
    let c = rt.counters();
    let long_polls = c.long_steps + c.preempted_steps + c.unattributed_steps;
    assert!(long_polls >= 1, "the held poll is long: {c:?}");
    assert_eq!(c.waits, 0, "steps back to back: {c:?}");
    // At a long poll's end (or its step's), and at the next step's start; a host that held a yield past the
    // quantum made a long poll of it, which the bound counts too.
    assert!(
        c.thread_accounts <= 2 * long_polls,
        "at most two readings a long poll: {c:?}"
    );
}
