//! A watched word's receivers stay within their bound (mantle's review of hyper-rt, finding 4): a clone
//! past the bound is refused even after the sender has taken earlier clones in (it used to succeed and then
//! never wake), and a receiver that drops gives its place back to a new one, which wakes on the next change.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use hyper_rt::RtError;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::watch;

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
    }
}

#[test]
fn a_clone_past_the_bound_is_refused_and_a_freed_place_is_reused() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let (mut sender, first) = watch(0, 2).unwrap();
        let second = first.try_clone().unwrap();
        // The send takes the second receiver in, emptying the joining queue.
        sender.send(1);
        assert!(
            matches!(first.try_clone(), Err(RtError::Capacity { .. })),
            "a third receiver past a bound of two"
        );
        drop(second);
        let mut third = first
            .try_clone()
            .expect("the dropped receiver's place is free");
        third.borrow_and_update();
        sender.send(2);
        let woke = hyper_rt::futures::within(5_000_000_000, third.changed())
            .await
            .unwrap();
        woke.expect("the new receiver wakes").unwrap();
        assert_eq!(third.borrow_and_update(), 2);
    })
    .unwrap();
}
