//! A tracking shard's idle spin lasts as long as blocking costs it in CPU (docs/runtime.md §3.4; `park_cost`),
//! not as long as a wake takes: spin-then-block is 2-competitive in CPU at that threshold [A: Karlin, Li,
//! Manasse, Owicki, SOSP'91], and a wake's latency is the host scheduler's queue, which a busy host stretches
//! to hundreds of microseconds while a park/wake cycle's CPU holds at a few. Until 2026-10-10 the spin
//! followed a wake estimate, so a client's shard on a loaded host spun through windows of tens to hundreds of
//! microseconds after each request: 31.3 µs of CPU a request where tokio spent 5.8 µs
//! (`benchmark-results/hyper-rt-vs-tokio-20261010/runs/base-f5d66a8-r1`, `rtt`).
//!
//! A spin sized by what a park costs ends long before a timer milliseconds away (a park's CPU is
//! microseconds), so the shard parks for each; and the spins that miss stop it spinning (`spin_policy`).

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

/// Shape: the timer a served client's task sleeps on, twenty milliseconds: a thousand times a park's CPU.
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
        spin_ns: 0,
        wake_tracking: Some(WakeTracking {
            idle_ratio: 1,
            cpus_at_once: 2,
        }),
    }
}

/// Do: on a tracking shard that can spin, a task notes a client's activity and sleeps a timer, five times.
/// Expect: the shard parked for every timer (its driver waits count them) and no spin ran to a timer's
/// deadline.
#[test]
fn a_tracking_shard_parks_for_each_timer_and_never_spins_to_one() {
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
