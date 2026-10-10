//! Completed block_on calls release their roots and the children they leave pending.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::sync::atomic::{AtomicUsize, Ordering};

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        // Test input: room for one root and one child, reused across calls.
        tasks_per_shard: 2,
        timers_per_shard: 2,
        interests_per_shard: 2,
        ring_entries: 2,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 2,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

#[test]
#[cfg_attr(miri, ignore)] // LocalRuntime opens the native platform driver.
fn completed_roots_allow_more_calls_than_the_configured_task_capacity() {
    let config = config();
    let mut runtime = LocalRuntime::new(&config).unwrap();
    for value in 0..config.tasks_per_shard.saturating_add(1) {
        assert_eq!(runtime.block_on(async move { value }), Ok(value));
    }
}

static CHILDREN_DROPPED: AtomicUsize = AtomicUsize::new(0);

struct Child;

impl Drop for Child {
    fn drop(&mut self) {
        CHILDREN_DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
#[cfg_attr(miri, ignore)] // LocalRuntime opens the native platform driver.
fn each_call_cleans_its_pending_child_and_leaves_room_for_the_next_root() {
    CHILDREN_DROPPED.store(0, Ordering::SeqCst);
    let config = config();
    let mut runtime = LocalRuntime::new(&config).unwrap();
    for value in 0..config.tasks_per_shard.saturating_add(1) {
        let returned = runtime
            .block_on(async move {
                let child = Child;
                hyper_rt::futures::spawn_detached(async move {
                    let _child = child;
                    std::future::pending::<()>().await;
                })?;
                Ok::<_, hyper_rt::RtError>(value)
            })
            .unwrap()
            .unwrap();
        assert_eq!(returned, value);
        assert_eq!(
            CHILDREN_DROPPED.load(Ordering::SeqCst),
            value.saturating_add(1),
            "the call returns only after its captured child is dropped"
        );
    }
    assert_eq!(
        runtime.block_on(async { "still serving" }),
        Ok("still serving")
    );
}
