//! `Runtime::start` returns with nothing of its start-up hand-offs left for another thread to free
//! (docs/runtime.md §3.17, `crate::startup`): the bytes live across the process as `start` returns are the
//! same at every start of one shape, as a caller counting allocations around it (mantle's
//! `crates/node/tests/shard_memory.rs`) needs them to be.
//!
//! The hand-offs were `std::sync::mpsc` channels, freed by whichever end dropped last, and a started thread
//! dropped its end only after its send had woken the starter, so a starter that ran first returned with the
//! channel still allocated. That is a race, so no single start shows it: the test pins its thread to one
//! core where the OS pins (the runtime's threads, unpinned, inherit the mask on Linux, which is CI's one-CPU
//! leg's shape) and starts many runtimes. With the channels, 15 of 20 runs failed on an Apple M-series Mac,
//! where pinning is a hint (2026-11-18): 1 to 8 of the 2,000 starts each left 640 to 960 bytes more. With the
//! cells, none of 20 did. One test per binary, so no other test's allocations move the count.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_measure::alloc;
use hyper_rt::runtime::{Runtime, RuntimeConfig};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Shape: two shards, so the shards' hand-off has several threads answering, beside the watchdog's.
fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 2,
        tasks_per_shard: 4,
        timers_per_shard: 4,
        interests_per_shard: 8,
        ring_entries: 4,
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

/// Shape: starts compared after the first two. The channels lost the race at about one start in a thousand
/// on macOS (41 of 40,000, above), so this many lose it at least once in most runs of them.
const STARTS: usize = 2000;

/// The bytes live across the process as a runtime of `config` returns from `start`, past those before.
fn started(config: &RuntimeConfig) -> i64 {
    alloc::begin_process();
    let rt = Runtime::start(config).unwrap();
    let live = alloc::end_process().live;
    rt.shutdown().unwrap();
    live
}

/// Every start of one shape leaves the same bytes live as it returns: none of a hand-off a started thread
/// has yet to free.
#[test]
fn every_start_leaves_the_same_bytes_live() {
    let _ = hyper_rt::machine::probes::pin_current_thread(0);
    let config = config();
    // Once, for what a process makes at its first runtime and keeps.
    started(&config);
    let held = started(&config);
    let over: Vec<(usize, i64)> = (0..STARTS)
        .map(|start| (start, started(&config)))
        .filter(|&(_, live)| live != held)
        .collect();
    assert!(over.is_empty(), "held {held}; differing starts {over:?}");
}
