//! A tracking shard's idle spin lasts as long as blocking costs it in CPU, not as long as its wake takes
//! (docs/runtime.md §3.4; `park_cost`): spin-then-block is 2-competitive in CPU at that threshold [A: Karlin,
//! Li, Manasse, Owicki, SOSP'91], and a wake's latency is the host scheduler's queue, which a busy host
//! stretches to hundreds of microseconds while a park/wake cycle's CPU holds at a few. Until 2026-10-10 the
//! spin followed the wake estimate, so a client's shard on a loaded host spun through windows of tens to
//! hundreds of microseconds after each request: 31.3 µs of CPU a request where tokio spent 5.8 µs
//! (`benchmark-results/hyper-rt-vs-tokio-20261010/runs/base-f5d66a8-r1`, `rtt`).
//!
//! The wake prior here is a second, so a spin sized by it outlasts every timer below: such a shard spins to
//! each timer's deadline and never parks. A spin sized by what a park costs ends long before the timer
//! (a park's CPU is microseconds; the timer is milliseconds away) and the shard parks for each.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::sync::mpsc::channel;
use std::time::Duration;

use hyper_rt::futures::sleep;
use hyper_rt::registry;
use hyper_rt::runtime::{Runtime, RuntimeConfig, WakeTracking};

/// Shape: the wake prior, a second: far longer than any timer below.
const PRIOR_NS: u64 = 1_000_000_000;
/// Shape: the timer a served client's task sleeps on, twenty milliseconds: a thousand times a park's CPU,
/// a fiftieth of the prior.
const TIMER_NS: u64 = 20_000_000;
/// Shape: rounds of activity and timer.
const ROUNDS: u64 = 5;
/// Shape: how long the test waits for the task's answer before it fails (the rounds take ROUNDS timers).
const ANSWER: Duration = Duration::from_secs(30);

fn config() -> RuntimeConfig {
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
        spin_ns: PRIOR_NS,
        wake_tracking: Some(WakeTracking {
            prior_ns: PRIOR_NS,
            shift: 4,
            idle_ratio: 1,
        }),
    }
}

/// Do: on a tracking shard whose wake prior is a second, a task notes a client's activity and sleeps a
/// timer, five times. Expect: the shard parked for every timer (its driver waits count them) and no spin ran
/// to a timer's deadline.
#[test]
fn a_tracking_shard_parks_for_a_timer_its_wake_estimate_would_have_spun_to() {
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
    assert_eq!(
        counters.spin_deadlines, 0,
        "a spin ran to a timer milliseconds away: {counters:?}"
    );
    assert!(
        counters.waits >= ROUNDS,
        "the shard parked for fewer than its {ROUNDS} timers: {counters:?}"
    );
    // Where the OS keeps a fine per-thread CPU clock, every park was charged to what blocking costs; where it
    // keeps none (Windows) nothing is learned and the shard parks at once.
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        assert!(
            counters.park_samples >= ROUNDS && counters.park_cost_ns > 0,
            "the parks were not measured: {counters:?}"
        );
    } else {
        assert_eq!(counters.park_samples, 0, "{counters:?}");
    }
}
