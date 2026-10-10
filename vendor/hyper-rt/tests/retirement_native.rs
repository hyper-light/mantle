//! Public native retirement ownership/refusal/cancellation. Source-only; run with an external guard.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};

use hyper_rt::combine::{Either, race2};
use hyper_rt::runtime::{RetirementLease, interests_for};
use hyper_rt::{RtError, Runtime, RuntimeConfig};

fn runtime(roles: usize) -> Runtime {
    // The cooperative-service native fixture's timing/page shape. Task/queue capacities
    // are exactly this test's separately admitted actor and progress roles.
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: roles,
        timers_per_shard: roles,
        interests_per_shard: interests_for(roles),
        ring_entries: roles,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: roles,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}

fn lease(runtime: &mut Runtime, capacity: usize) -> RetirementLease {
    let mut leases = runtime.prepare_retirement(&[capacity]).unwrap();
    leases.pop().unwrap()
}

struct Returned(Arc<AtomicUsize>);
impl Drop for Returned {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn an_unused_live_lease_does_not_hold_runtime_shutdown() {
    let mut rt = runtime(1);
    let mut lease = lease(&mut rt, 1);
    // The endpoint intentionally remains alive; shutdown must retire the empty reservation.
    rt.shutdown().unwrap();
    let mut threads = vec![std::thread::spawn(|| {})];
    let id = threads[0].thread().id();
    let pointer = threads.as_ptr();
    assert!(matches!(
        lease.adopt(&mut threads),
        Err(RtError::BadConfig { .. })
    ));
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].thread().id(), id);
    assert_eq!(threads.as_ptr(), pointer);
    threads.pop().unwrap().join().unwrap();
    drop(lease);
}

#[test]
fn waits_without_adoption_refuse_before_pending_and_keep_the_lease_usable() {
    let mut rt = runtime(1);
    let mut lease = lease(&mut rt, 1);
    assert!(matches!(lease.try_result(), Err(RtError::BadConfig { .. })));
    assert!(matches!(
        lease.wait_blocking(),
        Err(RtError::BadConfig { .. })
    ));
    let shard = rt.shard_ids()[0];
    let (back, returned) = mpsc::sync_channel(1);
    rt.spawn_on(shard, async move {
        let refused = {
            let mut wait = pin!(lease.wait());
            poll_fn(|cx| {
                Poll::Ready(matches!(
                    wait.as_mut().poll(cx),
                    Poll::Ready(Err(RtError::BadConfig { .. }))
                ))
            })
            .await
        };
        back.try_send((lease, refused)).unwrap();
    })
    .unwrap();
    let (mut lease, refused) = returned.recv().unwrap();
    let mut threads = vec![std::thread::spawn(|| {})];
    lease.adopt(&mut threads).unwrap();
    assert!(threads.is_empty());
    lease.wait_blocking().unwrap();
    drop(lease);
    rt.shutdown().unwrap();
    assert!(
        refused,
        "an unadopted wait published or suspended instead of refusing"
    );
}

#[test]
fn capacity_and_duplicate_adoption_return_every_original_handle() {
    let mut rt = runtime(1);
    let mut lease = lease(&mut rt, 1);
    let mut threads = vec![std::thread::spawn(|| {}), std::thread::spawn(|| {})];
    let ids: Vec<_> = threads.iter().map(|thread| thread.thread().id()).collect();
    let pointer = threads.as_ptr();
    assert!(matches!(
        lease.adopt(&mut threads),
        Err(RtError::Capacity { bound: 1, .. })
    ));
    assert_eq!(
        threads
            .iter()
            .map(|thread| thread.thread().id())
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(threads.as_ptr(), pointer);
    threads.pop().unwrap().join().unwrap();
    lease.adopt(&mut threads).unwrap();
    assert!(threads.is_empty());
    let mut extra = vec![std::thread::spawn(|| {})];
    let id = extra[0].thread().id();
    assert!(matches!(
        lease.adopt(&mut extra),
        Err(RtError::BadConfig { .. })
    ));
    assert_eq!(extra.len(), 1);
    assert_eq!(extra[0].thread().id(), id);
    extra.pop().unwrap().join().unwrap();
    lease.wait_blocking().unwrap();
    lease.wait_blocking().unwrap();
    drop(lease);
    rt.shutdown().unwrap();
}

#[test]
fn cancelled_and_foreign_repolled_waits_keep_the_same_native_owner() {
    // One retirement-waiting task and an independently runnable same-shard release task.
    let mut rt = runtime(2);
    let mut lease = lease(&mut rt, 1);
    let returned = Arc::new(AtomicUsize::new(0));
    let capture = Returned(Arc::clone(&returned));
    let (release, held) = mpsc::sync_channel(1);
    let (started, entered) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let _capture = capture;
        started.try_send(()).unwrap();
        // Closure on a failed test is cleanup, never an exact success verdict.
        let _ = held.recv();
    });
    let mut threads = vec![thread];
    lease.adopt(&mut threads).unwrap();
    entered.recv().unwrap();
    let shard = rt.shard_ids()[0];
    let (pending, mut was_pending) = hyper_rt::sync::channel(1).unwrap();
    let (back, completed) = mpsc::sync_channel(1);
    let observation = Arc::clone(&returned);
    rt.spawn_on(shard, async move {
        let mut first_pending = false;
        let cancelled = {
            let mut wait = pin!(lease.wait());
            race2(
                poll_fn(|cx| {
                    let first = wait.as_mut().poll(cx);
                    first_pending |= first.is_pending();
                    first
                }),
                async {},
            )
            .await
        };
        assert!(first_pending && matches!(cancelled, Either::Second(())));
        // Re-poll the SAME retained wait under a foreign context after genuine Pending.
        {
            let mut wait = pin!(lease.wait());
            poll_fn(|cx| {
                assert!(wait.as_mut().poll(cx).is_pending());
                let mut foreign = Context::from_waker(Waker::noop());
                assert!(matches!(
                    wait.as_mut().poll(&mut foreign),
                    Poll::Ready(Err(RtError::NotOnShardThread))
                ));
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(observation.load(Ordering::SeqCst), 0);
        pending.try_send(()).unwrap();
        lease.wait().await.unwrap();
        assert_eq!(observation.load(Ordering::SeqCst), 1);
        lease.wait().await.unwrap();
        back.try_send(()).unwrap();
    })
    .unwrap();
    rt.spawn_on(shard, async move {
        was_pending.recv().await.unwrap();
        assert_eq!(returned.load(Ordering::SeqCst), 0);
        release.try_send(()).unwrap();
    })
    .unwrap();
    completed.recv().unwrap();
    rt.shutdown().unwrap();
}

#[test]
fn native_join_failure_is_reported_after_all_owners_retire() {
    let unreported = hyper_rt::registry::unreported_failures();
    let mut rt = runtime(1);
    let mut lease = lease(&mut rt, 2);
    let returned = Arc::new(AtomicUsize::new(0));
    let first = Returned(Arc::clone(&returned));
    let second = Returned(Arc::clone(&returned));
    let mut threads = vec![
        std::thread::spawn(move || {
            let _capture = first;
            panic!("ordinary test thread failure");
        }),
        std::thread::spawn(move || {
            let _capture = second;
        }),
    ];
    lease.adopt(&mut threads).unwrap();
    assert!(matches!(
        lease.wait_blocking(),
        Err(RtError::BadConfig { .. })
    ));
    assert_eq!(returned.load(Ordering::SeqCst), 2);
    drop(lease);
    assert!(matches!(rt.shutdown(), Err(RtError::BadConfig { .. })));
    assert_eq!(
        hyper_rt::registry::unreported_failures(),
        unreported,
        "explicit shutdown error was reported again as an unreported Drop failure"
    );
}
