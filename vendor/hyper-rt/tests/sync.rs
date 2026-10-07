//! docs/runtime.md §8: the synchronization primitives between tasks of one shard, tasks of two shards, and
//! a task and a plain thread; each primitive's bound and its closing; and every cell given back.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity
)]

use std::sync::mpsc::channel as std_channel;
use std::time::Duration;

use hyper_rt::runtime::{LocalRuntime, Runtime, RuntimeConfig};
use hyper_rt::sync::{self, SyncError};

fn config(shards: u16) -> RuntimeConfig {
    RuntimeConfig {
        shards,
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

/// Shape: how long a test waits for a cross-thread answer before calling it lost (the work is microseconds).
const WAIT: Duration = Duration::from_secs(10);

#[test]
fn a_channel_between_two_tasks_of_one_shard_delivers_in_order_and_waits_for_room() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    let got = rt
        .block_on(async {
            let (tx, mut rx) = sync::channel::<u32>(4).unwrap();
            hyper_rt::futures::spawn(async move {
                for i in 0..1_000 {
                    tx.send(i).await.unwrap();
                }
            })
            .unwrap();
            let mut got = Vec::new();
            while let Ok(value) = rx.recv().await {
                got.push(value);
            }
            got
        })
        .unwrap();
    assert_eq!(got, (0..1_000).collect::<Vec<_>>());
}

#[test]
fn a_full_channel_refuses_try_send_and_a_closed_one_says_so() {
    let (tx, mut rx) = sync::channel::<u8>(1).unwrap();
    tx.try_send(1).unwrap();
    assert_eq!(tx.try_send(2), Err(SyncError::Full(2)));
    assert_eq!(rx.try_recv(), Ok(Some(1)));
    drop(tx);
    assert_eq!(rx.try_recv(), Err(SyncError::Closed(())));
    assert!(sync::channel::<u8>(0).is_err(), "no capacity is refused");
}

#[test]
fn a_thread_and_a_task_exchange_values_both_ways() {
    let rt = Runtime::start(&config(1)).unwrap();
    let shard = rt.shard_ids()[0];
    let (to_task, mut task_rx) = sync::channel::<u64>(8).unwrap();
    let (from_task, mut thread_rx) = sync::channel::<u64>(8).unwrap();
    rt.spawn_on(shard, async move {
        while let Ok(value) = task_rx.recv().await {
            from_task.send(value * 2).await.unwrap();
        }
    })
    .unwrap();
    for i in 0..100 {
        to_task.blocking_send(i).unwrap();
        assert_eq!(thread_rx.blocking_recv().unwrap(), i * 2);
    }
    drop(to_task);
    assert_eq!(thread_rx.blocking_recv(), Err(SyncError::Closed(())));
    rt.shutdown().unwrap();
}

/// Shape: rounds of a drop race. The race these tests meet was lost about once in 300 runs of the exchange
/// above (CI run 37543499272, and locally on macOS); 3,000 rounds miss it with probability about e^-10.
const DROP_RACE_ROUNDS: u32 = 3_000;

/// The last sender dropped on a thread just after a task answered it and went back to wait: the task must see
/// the channel close. The drop used to wake the task before the channel disconnected, and a task retrying in
/// between found it empty and still open, and waited for good.
#[test]
fn a_task_waiting_to_receive_sees_the_last_senders_drop() {
    let rt = Runtime::start(&config(1)).unwrap();
    let shard = rt.shard_ids()[0];
    let (ended, closed) = std_channel();
    for round in 0..DROP_RACE_ROUNDS {
        let (to_task, mut task_rx) = sync::channel::<u32>(1).unwrap();
        let (from_task, mut thread_rx) = sync::channel::<u32>(1).unwrap();
        let ended = ended.clone();
        rt.spawn_on(shard, async move {
            while let Ok(value) = task_rx.recv().await {
                if from_task.send(value).await.is_err() {
                    return;
                }
            }
            let _ = ended.send(());
        })
        .unwrap();
        to_task.blocking_send(round).unwrap();
        assert_eq!(thread_rx.blocking_recv(), Ok(round));
        // The task is going back to wait: drop the only sender as it does.
        drop(to_task);
        assert_eq!(closed.recv_timeout(WAIT), Ok(()), "round {round}");
    }
    rt.shutdown().unwrap();
}

/// The receiver dropped on a thread while a task waits for room to send: the task must see the channel close.
/// The drop used to wake queued senders before the channel disconnected (one retrying in between found it
/// full and queued again behind the drop), and a sender queued after the drop's drain was never woken; now
/// the queue's own drop, after the disconnect, wakes each sender still in it.
#[test]
fn a_task_waiting_for_room_sees_the_receivers_drop() {
    let rt = Runtime::start(&config(1)).unwrap();
    let shard = rt.shard_ids()[0];
    let (done, finished) = std_channel();
    for round in 0..DROP_RACE_ROUNDS {
        let (tx, rx) = sync::channel::<u32>(1).unwrap();
        tx.try_send(0).unwrap();
        let done = done.clone();
        rt.spawn_on(shard, async move {
            let refused = tx.send(1).await;
            let _ = done.send(matches!(refused, Err(SyncError::Closed(1))));
        })
        .unwrap();
        drop(rx);
        assert_eq!(finished.recv_timeout(WAIT), Ok(true), "round {round}");
    }
    rt.shutdown().unwrap();
}

#[test]
fn tasks_on_two_shards_exchange_values() {
    let rt = Runtime::start(&config(2)).unwrap();
    let shards = rt.shard_ids().to_vec();
    let (tx, mut rx) = sync::channel::<u32>(2).unwrap();
    let (done, finished) = std_channel();
    rt.spawn_on(shards[0], async move {
        for i in 0..500 {
            tx.send(i).await.unwrap();
        }
    })
    .unwrap();
    rt.spawn_on(shards[1], async move {
        let mut sum = 0u64;
        while let Ok(value) = rx.recv().await {
            sum += u64::from(value);
        }
        let _ = done.send(sum);
    })
    .unwrap();
    assert_eq!(finished.recv_timeout(WAIT).unwrap(), (0..500u64).sum());
    rt.shutdown().unwrap();
}

#[test]
fn a_oneshot_carries_its_value_or_closes_when_its_sender_goes() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    let (sent, dropped) = rt
        .block_on(async {
            let (tx, rx) = sync::oneshot::<&'static str>().unwrap();
            hyper_rt::futures::spawn(async move {
                hyper_rt::futures::sleep(1_000_000).await.unwrap();
                tx.send("value").unwrap();
            })
            .unwrap();
            let sent = rx.await;
            let (tx, rx) = sync::oneshot::<&'static str>().unwrap();
            drop(tx);
            (sent, rx.await)
        })
        .unwrap();
    assert_eq!(sent, Ok("value"));
    assert_eq!(dropped, Err(SyncError::Closed(())));
}

#[test]
fn every_watch_receiver_sees_the_latest_version_and_the_senders_end() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    let seen = rt
        .block_on(async {
            let (mut tx, mut first) = sync::watch(0, 4).unwrap();
            let mut second = tx.subscribe().unwrap();
            let (seen_tx, seen_rx) = std_channel();
            for receiver in [&mut first, &mut second] {
                let mut receiver = receiver.try_clone().unwrap();
                let seen_tx = seen_tx.clone();
                hyper_rt::futures::spawn(async move {
                    let mut values = Vec::new();
                    while receiver.changed().await.is_ok() {
                        values.push(receiver.borrow_and_update());
                    }
                    let _ = seen_tx.send(values);
                })
                .unwrap();
            }
            for value in 1..=3 {
                hyper_rt::futures::yield_now().await;
                tx.send(value);
                hyper_rt::futures::sleep(1_000_000).await.unwrap();
            }
            drop(tx);
            hyper_rt::futures::sleep(1_000_000).await.unwrap();
            (seen_rx.try_recv().unwrap(), seen_rx.try_recv().unwrap())
        })
        .unwrap();
    assert_eq!(seen.0, vec![1, 2, 3]);
    assert_eq!(seen.1, vec![1, 2, 3]);
}

#[test]
fn a_semaphore_admits_its_permits_at_once_and_hands_releases_out_in_arrival_order() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    let order = rt
        .block_on(async {
            let semaphore: &'static sync::Semaphore =
                Box::leak(Box::new(sync::Semaphore::new(2, 16).unwrap()));
            let (order_tx, order_rx) = std_channel();
            for task in 0..8u32 {
                let order_tx = order_tx.clone();
                hyper_rt::futures::spawn(async move {
                    let permit = semaphore.acquire().await.unwrap();
                    let _ = order_tx.send(task);
                    hyper_rt::futures::sleep(1_000_000).await.unwrap();
                    drop(permit);
                })
                .unwrap();
            }
            hyper_rt::futures::sleep(50_000_000).await.unwrap();
            assert_eq!(semaphore.available(), 2, "every permit given back");
            order_rx.try_iter().collect::<Vec<_>>()
        })
        .unwrap();
    assert_eq!(
        order,
        (0..8).collect::<Vec<_>>(),
        "first come, first served"
    );
}

#[test]
fn a_notification_wakes_its_waiter_or_waits_for_it() {
    let mut rt = LocalRuntime::new(&config(1)).unwrap();
    rt.block_on(async {
        let notify: &'static sync::Notify = Box::leak(Box::new(sync::Notify::new().unwrap()));
        notify.notify_one();
        notify.notified().await.unwrap();
        hyper_rt::futures::spawn(async move {
            hyper_rt::futures::sleep(1_000_000).await.unwrap();
            notify.notify_one();
        })
        .unwrap();
        notify.notified().await.unwrap();
    })
    .unwrap();
}
