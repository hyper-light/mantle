//! Public contract REDs: foreign polls leave ready values, versions and permits untouched.
//! Artifact only; no timings, scheduler sleeps or private-state assertions.
#![allow(clippy::unwrap_used, clippy::panic, clippy::disallowed_macros)]

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};

use hyper_rt::RtError;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::sync::{self, SyncError};

fn runtime() -> LocalRuntime {
    // One root task and its own pending wait. Timing/page shape from sync.rs;
    // capacities are this test's actual task role, with derived interest capacity.
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 1,
        interests_per_shard: interests_for(1),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}

#[test]
fn a_ready_channel_receive_refuses_before_consuming() {
    let (tx, mut rx) = sync::channel(1).unwrap();
    tx.try_send(7_u32).unwrap();
    let foreign = pin!(rx.recv())
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    let refused = matches!(foreign, Poll::Ready(Err(SyncError::NotOnShardThread(()))));
    let value = match foreign {
        Poll::Ready(Ok(value)) => value,
        Poll::Ready(Err(_)) => rx.try_recv().unwrap().unwrap(),
        Poll::Pending => panic!("a populated receiver is ready"),
    };
    assert_eq!(value, 7);
    drop(tx);
    assert_eq!(rx.try_recv(), Err(SyncError::Closed(())));
    assert!(refused, "a foreign poll consumed the queued value");
}

#[test]
fn a_ready_channel_send_refuses_with_its_unsent_value() {
    let (tx, mut rx) = sync::channel(1).unwrap();
    let foreign = pin!(tx.send(11_u32))
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    let refused = matches!(foreign, Poll::Ready(Err(SyncError::NotOnShardThread(11))));
    let before = rx.try_recv().unwrap();
    let value = match foreign {
        Poll::Ready(Err(SyncError::NotOnShardThread(value))) => {
            assert!(before.is_none());
            tx.try_send(value).unwrap();
            rx.try_recv().unwrap().unwrap()
        }
        Poll::Ready(Ok(())) => before.unwrap(),
        _ => panic!("a free channel has room for its original value"),
    };
    assert_eq!(value, 11);
    drop(tx);
    assert!(refused, "a foreign poll published its value");
}

#[test]
fn a_ready_oneshot_refuses_before_consuming() {
    let (tx, mut rx) = sync::oneshot().unwrap();
    tx.send(13_u32).unwrap();
    let foreign = Pin::new(&mut rx).poll(&mut Context::from_waker(Waker::noop()));
    let refused = matches!(foreign, Poll::Ready(Err(SyncError::NotOnShardThread(()))));
    let value = match foreign {
        Poll::Ready(Ok(value)) => value,
        Poll::Ready(Err(_)) => rx.try_recv().unwrap().unwrap(),
        Poll::Pending => panic!("a populated one-shot is ready"),
    };
    assert_eq!(value, 13);
    assert!(refused, "a foreign poll consumed the one-shot value");
}

#[test]
fn a_ready_notification_refuses_without_spending_its_permit() {
    let notify = sync::Notify::new().unwrap();
    notify.notify_one();
    let foreign = pin!(notify.notified())
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    let refused = matches!(foreign, Poll::Ready(Err(RtError::NotOnShardThread)));
    if matches!(foreign, Poll::Ready(Ok(()))) {
        // Only baseline cleanup: its foreign poll spent the permit.
        notify.notify_one();
    }
    runtime()
        .block_on(async move {
            notify.notified().await.unwrap();
        })
        .unwrap();
    assert!(refused, "a foreign poll spent the pending notification");
}

#[test]
fn a_ready_watch_refuses_without_advancing_the_seen_version() {
    let (mut tx, mut rx) = sync::watch(0, 1).unwrap();
    tx.send(17);
    let foreign = pin!(rx.changed())
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    let refused = matches!(foreign, Poll::Ready(Err(SyncError::NotOnShardThread(()))));
    if matches!(foreign, Poll::Ready(Ok(()))) {
        // Only baseline cleanup: its foreign poll already marked version 17 seen.
        tx.send(19);
    }
    let observed = runtime()
        .block_on(async move {
            rx.changed().await.unwrap();
            rx.borrow()
        })
        .unwrap();
    assert_eq!(observed, if refused { 17 } else { 19 });
    assert!(refused, "a foreign poll advanced the watch version");
}

#[test]
fn a_free_semaphore_refuses_without_taking_a_permit() {
    let semaphore = sync::Semaphore::new(1, 1).unwrap();
    let foreign = pin!(semaphore.acquire())
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    let refused = matches!(foreign, Poll::Ready(Err(SyncError::NotOnShardThread(()))));
    let retained = semaphore.available() == 1;
    drop(foreign);
    assert_eq!(semaphore.available(), 1);
    assert!(refused && retained, "a foreign poll took the free permit");
}

#[test]
fn a_pending_channel_rechecks_context_before_its_ready_repoll() {
    let (tx, mut rx) = sync::channel(1).unwrap();
    let refused = runtime()
        .block_on(async move {
            let foreign = {
                let mut receive = pin!(rx.recv());
                poll_fn(|cx| {
                    assert!(receive.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                tx.try_send(23_u32).unwrap();
                receive
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            let refused = matches!(foreign, Poll::Ready(Err(SyncError::NotOnShardThread(()))));
            let value = match foreign {
                Poll::Ready(Ok(value)) => value,
                Poll::Ready(Err(_)) => rx.try_recv().unwrap().unwrap(),
                Poll::Pending => panic!("the published value is ready after genuine Pending"),
            };
            assert_eq!(value, 23);
            refused
        })
        .unwrap();
    assert!(refused, "the ready repoll consumed a value off its task");
}

#[test]
fn a_granted_semaphore_rechecks_context_before_taking_its_grant() {
    let semaphore = sync::Semaphore::new(1, 1).unwrap();
    let refused = runtime()
        .block_on(async move {
            let permit = semaphore.try_acquire().unwrap();
            let refused = {
                let mut acquire = pin!(semaphore.acquire());
                poll_fn(|cx| {
                    assert!(acquire.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(permit); // Real oldest-waiter grant, while its future remains owned.
                let foreign = acquire
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                let refused = matches!(foreign, Poll::Ready(Err(SyncError::NotOnShardThread(()))));
                drop(foreign); // Baseline Permit returned; fixed Acquire Drop returns its unused grant.
                refused
            };
            assert_eq!(semaphore.available(), 1);
            refused
        })
        .unwrap();
    assert!(refused, "the foreign repoll took a queued grant");
}
