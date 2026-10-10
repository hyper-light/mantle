//! Plain threads blocked on hyper-rt's channels (docs/runtime.md §8; mantle `docs/design/event-loop.md` D4):
//! a thread parked in `blocking_recv` or `blocking_send` is registered in the channel's waiter word and woken
//! by the one publisher that takes it, with no lock between them (`crate::handoff`). Each case waits on the
//! fact it needs (a value, a refusal, a parked state the test can read); no clock decides an outcome.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation
)]

use hyper_rt::runtime::{Runtime, RuntimeConfig};
use hyper_rt::sync::{SyncError, channel, channel_with, oneshot};

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
        wake_tracking: None,
    }
}

/// Shape: values each producer sends in the fan-in, and the producers: enough that the receiver parks and is
/// woken many times while several producers race to take its registration.
const EACH: u64 = 5_000;
/// Shape: the fan-in's producers.
const PRODUCERS: u64 = 6;

/// A task sends to a thread in `blocking_recv` (parked or about to park: either way the task's publication
/// either precedes the thread's check or finds its registration): the value arrives, then the task's sender
/// drops and the thread sees `Closed`.
#[test]
fn a_task_wakes_a_thread_in_blocking_recv() {
    let rt = Runtime::start(&config()).unwrap();
    let (tx, mut rx) = channel::<u64>(1).unwrap();
    rt.spawn_on(rt.shard_ids()[0], async move {
        tx.try_send(7).unwrap();
    })
    .unwrap();
    assert_eq!(rx.blocking_recv(), Ok(7));
    assert_eq!(rx.blocking_recv(), Err(SyncError::Closed(())));
    rt.shutdown().unwrap();
}

/// Many threads send to one thread parked in `blocking_recv`, the channel small enough that they also park in
/// `blocking_send`: every value arrives, once, in each sender's order, and the last sender's drop closes the
/// channel.
#[test]
fn many_threads_feed_one_parked_receiver_and_every_value_arrives_once() {
    let (tx, mut rx) = channel_with::<(u64, u64)>(2, PRODUCERS as usize).unwrap();
    std::thread::scope(|scope| {
        for producer in 0..PRODUCERS {
            let tx = tx.clone();
            scope.spawn(move || {
                for sequence in 0..EACH {
                    tx.blocking_send((producer, sequence)).unwrap();
                }
            });
        }
        drop(tx);
        let mut next = [0u64; PRODUCERS as usize];
        let mut received = 0u64;
        loop {
            match rx.blocking_recv() {
                Ok((producer, sequence)) => {
                    assert_eq!(next[producer as usize], sequence, "out of order or twice");
                    next[producer as usize] += 1;
                    received += 1;
                }
                Err(SyncError::Closed(())) => break,
                Err(other) => panic!("{other:?}"),
            }
        }
        assert_eq!(received, PRODUCERS * EACH);
    });
}

/// Two threads bounce a value through two channels of one slot, each parking in `blocking_recv` between turns:
/// every round trip completes.
#[test]
fn two_threads_ping_pong_through_parked_receives() {
    const ROUNDS: u64 = 10_000;
    let (to_b, mut b_rx) = channel::<u64>(1).unwrap();
    let (to_a, mut a_rx) = channel::<u64>(1).unwrap();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            while let Ok(value) = b_rx.blocking_recv() {
                to_a.blocking_send(value + 1).unwrap();
            }
        });
        for round in 0..ROUNDS {
            to_b.blocking_send(round * 2).unwrap();
            assert_eq!(a_rx.blocking_recv(), Ok(round * 2 + 1));
        }
        drop(to_b);
    });
}

/// A thread parked in `blocking_send` on a full channel is granted the room a receiver's take makes, and one
/// parked when the receiver goes is woken to see `Closed`.
#[test]
fn a_parked_sender_is_granted_room_and_woken_by_the_receivers_end() {
    let (tx, mut rx) = channel_with::<u64>(1, 2).unwrap();
    tx.try_send(1).unwrap();
    std::thread::scope(|scope| {
        let sender = scope.spawn(|| tx.blocking_send(2));
        // The sender finds the channel full and waits for the room this take grants (or the take came first).
        assert_eq!(rx.blocking_recv(), Ok(1));
        assert_eq!(sender.join().unwrap(), Ok(()));
        assert_eq!(rx.blocking_recv(), Ok(2));
        tx.try_send(3).unwrap();
        // Queued for room or not yet, the second sender ends `Closed` once the receiver is gone.
        let blocked = scope.spawn(|| tx.blocking_send(4));
        drop(rx);
        assert!(matches!(blocked.join().unwrap(), Err(SyncError::Closed(4))));
    });
}

/// A one-shot to a thread parked in `blocking_recv`: the value arrives; and a sender dropped unsent wakes the
/// thread to see `Closed`.
#[test]
fn a_one_shot_wakes_its_parked_receiver_with_the_value_or_the_senders_end() {
    let (tx, mut rx) = oneshot::<u64>().unwrap();
    std::thread::scope(|scope| {
        scope.spawn(move || tx.send(9).unwrap());
        assert_eq!(rx.blocking_recv(), Ok(9));
    });
    let (tx, mut rx) = oneshot::<u64>().unwrap();
    std::thread::scope(|scope| {
        scope.spawn(move || drop(tx));
        assert_eq!(rx.blocking_recv(), Err(SyncError::Closed(())));
    });
}
