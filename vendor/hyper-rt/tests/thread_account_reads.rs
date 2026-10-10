//! The thread's CPU account (a system call) is read only while poll attribution is armed: a shard whose
//! polls never pass the step quantum never arms, and reads it not once however many steps it runs
//! (`attribution::Tracker`, `Counters::thread_accounts`).

#![allow(clippy::unwrap_used, clippy::disallowed_macros)]

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
