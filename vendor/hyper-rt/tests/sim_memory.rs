//! A simulation gives back what it allocated (§4.3; banned item 8: reclamation is part of every bound):
//! its shard contexts through the registry, its clock with the runtime, its per-shard driver flags
//! through the registry slot. Before 2026-09-14 every `SimRuntime::new` leaked its contexts, its clock
//! and one flag block per shard for the process lifetime — the simulation-driven suites (transport,
//! cluster, db, vfs) build thousands of runtimes per process. One test per binary, so no other test's
//! allocations move the counts. Judged by the heap's live bytes, exactly: the process's resident size
//! moves with the allocator's retention of freed regions (0 to 1.6 MiB over 32 dropped simulations in
//! five identical runs, 2026-10-06), which says nothing about a leak.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::string_slice,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::missing_panics_doc
)]
// Test harness code: an unwrap here is a failed test, which is what it should be.

use hyper_measure::alloc::{self, Counting};
use hyper_rt::registry::contexts_reclaimed;
use hyper_rt::runtime::RuntimeConfig;
use hyper_rt::sim::SimRuntime;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Shape: a daemon-sized task budget per shard, so one simulation is megabytes — far above the page
/// granularity of the resident-size accounting.
fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 2,
        tasks_per_shard: 4096,
        timers_per_shard: 4096,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

/// Shape: how many simulations are built in turn after the warm-up — enough that a per-run leak of one
/// footprint is many times the footprint, so the bound below cannot be met by noise.
const CYCLES: u64 = 32;

/// Runs one simulation to idle with a task on each shard, and drops it.
fn one_simulation() {
    let mut sim = SimRuntime::new(&config(), 7).unwrap();
    for id in sim.shard_ids() {
        sim.spawn_on(id, async {}).unwrap();
    }
    sim.run_until_idle();
}

/// Do: measure the live heap of one two-shard simulation, then build and drop `CYCLES` simulations in turn
/// (a warm-up that pays the process's one-time allocations), then `CYCLES` more under a process-wide count.
/// Expect: the second batch leaves the live heap exactly where it found it, and the reclamation counter moved
/// by every context built (non-vacuity: a live simulation holds heap). Before the fix, every simulation
/// leaked its contexts, clock and flags.
#[test]
fn a_dropped_simulation_gives_back_its_contexts_clock_and_flags() {
    let reclaimed_before = contexts_reclaimed();
    alloc::begin_process();
    let warm = SimRuntime::new(&config(), 7).unwrap();
    let footprint = alloc::read_process().live;
    drop(warm);
    for _ in 0..CYCLES {
        one_simulation();
    }
    alloc::begin_process();
    for _ in 0..CYCLES {
        one_simulation();
    }
    let growth = alloc::read_process().live;
    let reclaimed = contexts_reclaimed() - reclaimed_before;
    eprintln!(
        "live heap: one simulation {footprint} B; after {CYCLES} more dropped, {growth} B; contexts reclaimed \
     {reclaimed}"
    );
    assert!(footprint > 0, "a live simulation holds heap (non-vacuity)");
    assert_eq!(
        reclaimed,
        (2 * CYCLES + 1) * u64::from(config().shards),
        "every context built was reclaimed"
    );
    assert_eq!(
        growth, 0,
        "{CYCLES} dropped simulations gave back every byte they allocated"
    );
}
