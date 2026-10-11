//! With one of its process's threads able to run at a time, a shard never spins: the thread that would end
//! the spin cannot run beside it, so the spin cannot see its event and only holds the CPU that thread
//! needs (mantle `docs/design/event-loop.md` D9). Nor does such a shard learn what blocking costs, which only
//! a spin's length needs: no CPU clock is read around its parks.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::missing_panics_doc
)]

use std::sync::mpsc::channel;
use std::time::Duration;

use hyper_rt::futures::sleep;
use hyper_rt::registry;
use hyper_rt::runtime::{Runtime, RuntimeConfig, WakeTracking};

/// Shape: the timer a served client's task sleeps on after each request: milliseconds, so the shard goes
/// idle inside the window the request opened and would spin there.
const TIMER_NS: u64 = 2_000_000;
/// Shape: rounds of activity and timer.
const ROUNDS: u64 = 8;
/// Shape: how long the test waits for the task's answer before it fails.
const ANSWER: Duration = Duration::from_secs(30);

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 4,
        timers_per_shard: 4,
        interests_per_shard: 4,
        ring_entries: 4,
        step_budget_ns: 50_000,
        timer_tick_ns: 50_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: Some(WakeTracking {
            idle_ratio: 1,
            cpus_at_once: 1,
        }),
    }
}

/// Do: on a shard whose process runs one thread at a time, a task notes a client's activity and sleeps a
/// timer, eight times. Expect: no spin of any kind, no park measured, no CPU clock read.
#[test]
fn a_shard_that_runs_alone_never_spins_and_never_learns_what_blocking_costs() {
    let rt = Runtime::start(&config()).unwrap();
    let shard = rt.shard_ids()[0];
    let (tx, rx) = channel();
    rt.spawn_on(shard, async move {
        for _ in 0..ROUNDS {
            registry::with_current(|ctx| ctx.note_activity());
            sleep(TIMER_NS).await.unwrap();
        }
        tx.send(registry::with_current(|ctx| ctx.counters()).unwrap())
            .unwrap();
    })
    .unwrap();
    let counters = rx.recv_timeout(ANSWER).unwrap();
    rt.shutdown().unwrap();
    assert!(counters.waits >= ROUNDS, "{counters:?}");
    assert_eq!(
        (
            counters.spin_hits,
            counters.spin_misses,
            counters.spin_deadlines
        ),
        (0, 0, 0),
        "{counters:?}"
    );
    assert_eq!(
        (counters.park_samples, counters.park_cost_reads),
        (0, 0),
        "{counters:?}"
    );
}
