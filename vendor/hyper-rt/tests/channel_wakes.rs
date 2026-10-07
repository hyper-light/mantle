//! A bounded channel's waiting senders (mantle's review of hyper-rt, finding 3): a send cancelled while it
//! waited for room does not spend the wake meant for the sender behind it; one granted room and cancelled
//! before using it hands the room on; and senders racing for one slot all get their values through.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation
)]

use std::sync::atomic::{AtomicU32, Ordering};

use hyper_rt::combine::race2;
use hyper_rt::futures::{cancel, sleep, spawn_detached, yield_now};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::{channel, channel_with};

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

/// Shape: how long a receive may wait before the test fails rather than hangs.
const PATIENCE_NS: u64 = 5_000_000_000;

#[test]
fn a_cancelled_send_does_not_spend_the_next_senders_wake() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        // Two waiting places, so B can wait behind A's abandoned one.
        let (tx, mut rx) = channel_with::<u32>(1, 2).unwrap();
        tx.try_send(0).unwrap();
        // A waits for room, then gives up: its place is abandoned.
        let gave_up = race2(tx.send(1), sleep(1_000_000)).await;
        assert!(matches!(gave_up, hyper_rt::combine::Either::Second(_)));
        // B waits for room behind A's abandoned place (a spawned task runs once the root yields twice: the
        // first yield re-queues the root ahead of the task's installation).
        static B_STARTED: AtomicU32 = AtomicU32::new(0);
        let b = tx.clone();
        spawn_detached(async move {
            B_STARTED.store(1, Ordering::Release);
            b.send(2).await.unwrap();
        })
        .unwrap();
        yield_now().await;
        yield_now().await;
        assert_eq!(
            B_STARTED.load(Ordering::Acquire),
            1,
            "B is waiting before the receive"
        );
        assert_eq!(rx.recv().await.unwrap(), 0);
        let next = hyper_rt::futures::within(PATIENCE_NS, rx.recv())
            .await
            .unwrap();
        assert_eq!(next.expect("B was woken, not A's dead place").unwrap(), 2);
    })
    .unwrap();
}

#[test]
fn a_granted_send_cancelled_unused_hands_its_room_on() {
    static SENT: AtomicU32 = AtomicU32::new(0);
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let (tx, mut rx) = channel_with::<u32>(1, 2).unwrap();
        tx.try_send(0).unwrap();
        let (a, b) = (tx.clone(), tx.clone());
        let first = spawn_detached(async move {
            a.send(1).await.unwrap();
            SENT.fetch_add(1, Ordering::AcqRel);
        })
        .unwrap();
        spawn_detached(async move {
            b.send(2).await.unwrap();
            SENT.fetch_add(10, Ordering::AcqRel);
        })
        .unwrap();
        yield_now().await;
        yield_now().await;
        // Both wait now: A first. Taking a value grants the room to A and wakes it; A is cancelled before it
        // runs.
        assert_eq!(rx.try_recv().unwrap(), Some(0));
        cancel(first).unwrap();
        let next = hyper_rt::futures::within(PATIENCE_NS, rx.recv())
            .await
            .unwrap();
        assert_eq!(next.expect("the room passed to B").unwrap(), 2);
        assert_eq!(SENT.load(Ordering::Acquire), 10, "B sent; A never did");
    })
    .unwrap();
}

/// A sender woken for room that a racing sender took first waits again, and the next receive wakes it.
#[test]
fn a_woken_sender_whose_room_was_taken_waits_again() {
    static A_STARTED: AtomicU32 = AtomicU32::new(0);
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let (tx, mut rx) = channel::<u32>(1).unwrap();
        tx.try_send(0).unwrap();
        let a = tx.clone();
        spawn_detached(async move {
            A_STARTED.store(1, Ordering::Release);
            a.send(1).await.unwrap();
        })
        .unwrap();
        yield_now().await;
        yield_now().await;
        assert_eq!(A_STARTED.load(Ordering::Acquire), 1, "A is waiting");
        // The receive grants the room to A and wakes it; before A runs, a racing send takes the room.
        assert_eq!(rx.try_recv().unwrap(), Some(0));
        tx.try_send(9).unwrap();
        // A runs now, finds the channel full again, and must wait anew.
        yield_now().await;
        yield_now().await;
        assert_eq!(rx.recv().await.unwrap(), 9);
        let next = hyper_rt::futures::within(PATIENCE_NS, rx.recv())
            .await
            .unwrap();
        assert_eq!(next.expect("A waited again and was woken").unwrap(), 1);
    })
    .unwrap();
}

#[test]
fn senders_racing_for_one_slot_all_get_through() {
    const SENDERS: u32 = 16;
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let (tx, mut rx) = channel_with::<u32>(1, SENDERS as usize).unwrap();
        for value in 0..SENDERS {
            let tx = tx.clone();
            spawn_detached(async move {
                tx.send(value).await.unwrap();
            })
            .unwrap();
        }
        drop(tx);
        let mut seen = 0u32;
        let mut sum = 0u32;
        while let Some(value) = hyper_rt::futures::within(PATIENCE_NS, rx.recv())
            .await
            .unwrap()
        {
            match value {
                Ok(value) => {
                    seen += 1;
                    sum += value;
                }
                Err(_) => break,
            }
        }
        assert_eq!(seen, SENDERS);
        assert_eq!(sum, (0..SENDERS).sum::<u32>());
    })
    .unwrap();
}
