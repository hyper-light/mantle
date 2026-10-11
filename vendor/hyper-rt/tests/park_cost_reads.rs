//! The thread's CPU clock (a system call) is read to learn what a park costs only by a shard that learns it,
//! and only around a wait the shard makes: two readings a measured park, none for a shard that does not
//! learn (`Counters::park_cost_reads`; mantle `docs/design/event-loop.md` D2). A reading after every park,
//! learning or not, doubled the cost of the loop item the runtime calibrates its poll batch by, and halved
//! every shard's batch (`benchmark-results/rtloop-async-fill-bisect-20261010`).

#![allow(clippy::unwrap_used, clippy::disallowed_macros)]

use hyper_rt::futures::sleep;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, WakeTracking};

/// Shape: the sleeps each case parks for: each ends in a park, whatever the host's speed.
const SLEEPS: u64 = 20;
/// Shape: each sleep's length, long against a step so the shard parks rather than finds its timer due.
const SLEEP_NS: u64 = 1_000_000;

fn config(learns: bool) -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 16,
        ring_entries: 16,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: learns.then_some(WakeTracking {
            idle_ratio: 1,
            cpus_at_once: 2,
        }),
    }
}

/// Runs the sleeps on a shard configured by `learns` and hands back its counters.
fn parks(learns: bool) -> hyper_rt::shard_loop::Counters {
    let mut rt = LocalRuntime::new(&config(learns)).unwrap();
    rt.block_on(async {
        for _ in 0..SLEEPS {
            sleep(SLEEP_NS).await.unwrap();
        }
    })
    .unwrap();
    rt.counters()
}

#[test]
fn a_shard_that_does_not_learn_reads_no_cpu_clock_when_it_parks() {
    let counters = parks(false);
    assert!(counters.waits >= SLEEPS, "{counters:?}");
    assert_eq!(counters.park_cost_reads, 0, "{counters:?}");
}

#[test]
fn a_learning_shard_reads_its_cpu_clock_twice_a_measured_park() {
    let counters = parks(true);
    assert!(counters.waits >= SLEEPS, "{counters:?}");
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        assert!(counters.park_samples > 0, "{counters:?}");
        assert_eq!(
            counters.park_cost_reads,
            2 * counters.park_samples,
            "{counters:?}"
        );
    } else {
        // No fine per-thread clock (Windows): one reading a wait, which answers nothing, and no sample.
        assert_eq!(counters.park_samples, 0, "{counters:?}");
    }
}
