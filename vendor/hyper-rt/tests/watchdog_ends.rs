//! A runtime's watchdog thread (`hyper_rt::watch`) ends with its runtime: a shutdown and a drop each join it, so
//! nothing of the runtime is left running, and a local runtime starts none. Its own binary, so no other test's
//! runtime is counted in the process's watchdogs.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::sync::mpsc::channel;
use std::time::Duration;

use hyper_rt::runtime::{LocalRuntime, Runtime, RuntimeConfig};
use hyper_rt::watch;

/// Shape: a fixed millisecond quantum and a batch of four (a limit of eight milliseconds).
fn config(shards: u16) -> RuntimeConfig {
    RuntimeConfig {
        shards,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 128,
        ring_entries: 64,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 4,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue, an eventfd or a port, which Miri does not model
fn the_watchdog_ends_when_its_runtime_shuts_down_or_drops() {
    assert_eq!(watch::running(), 0);
    let local = LocalRuntime::new(&config(1)).unwrap();
    assert_eq!(watch::running(), 0, "a local runtime starts no watchdog");
    drop(local);

    let first = Runtime::start(&config(2)).unwrap();
    assert_eq!(watch::running(), 1, "start returns once its watchdog runs");
    let second = Runtime::start(&config(1)).unwrap();
    assert_eq!(watch::running(), 2);

    // Work ran and the shards were watched before each runtime ends.
    let (ran, done) = channel();
    first
        .spawn_on(first.shard_ids()[1], async move {
            ran.send(()).unwrap();
        })
        .unwrap();
    done.recv_timeout(Duration::from_secs(10)).unwrap();

    let counters = first.shutdown().unwrap();
    assert_eq!(counters.len(), 2);
    assert_eq!(watch::running(), 1, "the shutdown joined its watchdog");
    drop(second);
    assert_eq!(watch::running(), 0, "the drop joined its watchdog");
}
