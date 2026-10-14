//! A task's code unwinding: its poll and its future's drop run inside an unwind boundary of the
//! shard's own (CLAUDE.md §1), so the task ends typed, `Outcome::Unwound`, and the shard and its
//! other tasks go on.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::sync::mpsc::channel;

use hyper_rt::futures::{cancel, join, sleep, spawn, spawn_child, yield_now};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::task::Outcome;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// A task that panics in its poll ends `Unwound`, its joiner told so, and the shard goes on: a
/// task spawned after it runs to completion.
#[test]
fn a_task_that_unwinds_ends_typed_and_the_shard_goes_on() {
    let (tell, heard) = channel();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let root = rt
        .spawn(async move {
            let unwinding = spawn_child(async {
                yield_now().await;
                panic!("a task that unwinds");
            })
            .unwrap();
            let _ = tell.send(join(unwinding).await);
            let after = spawn_child(async { yield_now().await }).unwrap();
            let _ = tell.send(join(after).await);
        })
        .unwrap();
    rt.run_until_idle();
    let outcomes: Vec<_> = heard.try_iter().collect();
    assert_eq!(outcomes, [Ok(Outcome::Unwound), Ok(Outcome::Completed)]);
    let counters = rt.counters();
    assert_eq!(counters.unwound, 1, "{counters:?}");
    assert_eq!(
        counters.completed, 2,
        "the root and the task after; {counters:?}"
    );
    rt.context().detach(root).unwrap();
}

/// A panic payload whose drop unwinds too.
struct Bomb;

impl Drop for Bomb {
    fn drop(&mut self) {
        panic!("a payload that unwinds as it drops");
    }
}

/// A task whose unwind carries a payload that unwinds again as it drops ends `Unwound` all the
/// same: the payload is dropped inside a boundary of its own, and the shard goes on.
#[test]
fn a_task_whose_payload_unwinds_as_it_drops_ends_typed_and_the_shard_goes_on() {
    let (tell, heard) = channel();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let root = rt
        .spawn(async move {
            let unwinding = spawn_child(async {
                yield_now().await;
                std::panic::panic_any(Bomb);
            })
            .unwrap();
            let _ = tell.send(join(unwinding).await);
            let after = spawn_child(async { yield_now().await }).unwrap();
            let _ = tell.send(join(after).await);
        })
        .unwrap();
    rt.run_until_idle();
    let outcomes: Vec<_> = heard.try_iter().collect();
    assert_eq!(outcomes, [Ok(Outcome::Unwound), Ok(Outcome::Completed)]);
    assert_eq!(rt.counters().unwound, 1);
    rt.context().detach(root).unwrap();
}

/// A task's future that panics as it is dropped, here at its cancellation, is counted and costs
/// the shard nothing: the task ends `Cancelled`, and the shard runs on.
#[test]
fn a_future_whose_drop_unwinds_is_counted_and_the_shard_goes_on() {
    struct UnwindsOnDrop;
    impl Drop for UnwindsOnDrop {
        fn drop(&mut self) {
            panic!("a drop that unwinds");
        }
    }
    let (tell, heard) = channel();
    let (started, start) = channel();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let root = rt
        .spawn(async move {
            let held = spawn(async move {
                let _held = UnwindsOnDrop;
                let _ = started.send(());
                sleep(1_000_000_000).await.unwrap();
            })
            .unwrap();
            // The task holds what panics as it drops once it has run to its sleep.
            while start.try_recv().is_err() {
                yield_now().await;
            }
            cancel(held).unwrap();
            let _ = tell.send(join(held).await);
            let after = spawn_child(async { yield_now().await }).unwrap();
            let _ = tell.send(join(after).await);
        })
        .unwrap();
    rt.run_until_idle();
    let outcomes: Vec<_> = heard.try_iter().collect();
    assert_eq!(outcomes, [Ok(Outcome::Cancelled), Ok(Outcome::Completed)]);
    let counters = rt.counters();
    assert_eq!(counters.unwound, 1, "{counters:?}");
    rt.context().detach(root).unwrap();
}
