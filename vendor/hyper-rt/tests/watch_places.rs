//! A watched word's receiver places (mantle's final review of hyper-rt, finding 6): a receiver dropped
//! before any send gives its place back at once. Clones used to reach the sender through a channel only the
//! sender drained at a send, so a clone dropped before the next send still held a place: a `subscribe` at
//! the bound was refused although a place was free, and clone-and-drop cycles filled the channel until a
//! `try_clone` was refused. This fixture owns its process: it counts the process's live cells.

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
use hyper_rt::sync::{cell, watch};

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 16,
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

/// Shape: the receivers the word allows.
const BOUND: usize = 2;

/// Do: with one receiver live, clone a second and drop it before any send, then subscribe; then clone and
/// drop many times more than the bound with no send between; then check the places by filling them; then
/// drop everything. Expect: the subscribe and every clone succeed while a place is free, a clone past the
/// bound is refused, the receivers that hold places wake on a send, and the cells come back to where they
/// were.
#[test]
fn a_dropped_receiver_gives_its_place_back_before_any_send() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let before = cell::live();
        let (mut sender, first) = watch(0, BOUND).unwrap();

        drop(first.try_clone().unwrap());
        let second = sender
            .subscribe()
            .expect("the dropped clone's place is free again");
        drop(second);

        for round in 0..BOUND * 16 {
            let clone = first.try_clone().unwrap_or_else(|e| {
                panic!("round {round}: a clone with a place free was refused: {e:?}")
            });
            drop(clone);
        }

        let mut held = first.try_clone().unwrap();
        assert!(
            matches!(first.try_clone(), Err(RtError::Capacity { .. })),
            "past the bound, refused"
        );
        let mut first = first;
        sender.send(7);
        first.changed().await.unwrap();
        held.changed().await.unwrap();
        assert_eq!((first.borrow(), held.borrow()), (7, 7));

        drop(held);
        drop(first);
        drop(sender);
        assert_eq!(cell::live(), before, "the word's cells all came back");
    })
    .unwrap();
}
