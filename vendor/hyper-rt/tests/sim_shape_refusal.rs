//! Configuration representation refusals hold even when an empty simulation builds no shard.
#![allow(clippy::unwrap_used, clippy::panic, clippy::disallowed_macros)]

use hyper_rt::RtError;
use hyper_rt::runtime::RuntimeConfig;
use hyper_rt::sim::SimRuntime;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 0,
        tasks_per_shard: 0,
        timers_per_shard: 0,
        interests_per_shard: 0,
        ring_entries: 1,
        step_budget_ns: 1,
        timer_tick_ns: 1,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

#[test]
fn an_empty_simulation_refuses_an_unrepresentable_ring_and_keeps_valid_zero_arenas() {
    let mut impossible = config();
    impossible.ring_entries = usize::MAX;
    let refused = SimRuntime::new(&impossible, 302);
    let mut empty = SimRuntime::new(&config(), 302).unwrap();
    empty.run_until_idle();
    assert!(
        matches!(refused, Err(RtError::BadConfig { .. })),
        "a simulation must check its public configuration before acquiring resources"
    );
}
