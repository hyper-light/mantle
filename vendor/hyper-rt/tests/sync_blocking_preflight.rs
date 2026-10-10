//! Borrowed cold API preflight before publication/consumption, with owned payload reuse.
#![allow(clippy::unwrap_used, clippy::panic, clippy::disallowed_macros)]

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::sync::{self, SyncError};

fn runtime() -> LocalRuntime {
    // Same native sync fixture timing shape; one root task is the complete role count.
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
fn a_runtime_blocking_receive_refuses_without_taking_its_ready_value() {
    let (tx, mut rx) = sync::channel(1).unwrap();
    tx.try_send(vec![1_u8, 2, 3]).unwrap();
    let (mut rx, result) = runtime()
        .block_on(async move {
            let result = rx.blocking_recv();
            (rx, result)
        })
        .unwrap();
    let refused = matches!(result, Err(SyncError::NotOnShardThread(())));
    let value = match result {
        Ok(value) => value,
        Err(_) => rx.try_recv().unwrap().unwrap(),
    };
    assert_eq!(value, vec![1, 2, 3]);
    drop(tx);
    assert_eq!(rx.try_recv(), Err(SyncError::Closed(())));
    assert!(
        refused,
        "the blocking call consumed ready data on its shard"
    );
}

#[test]
fn a_runtime_blocking_send_returns_its_owned_payload_without_publication() {
    let (tx, mut rx) = sync::channel(1).unwrap();
    tx.try_send(vec![7_u8]).unwrap(); // Actual full data capacity before the call.
    let payload = vec![1_u8, 2, 3, 4];
    let pointer = payload.as_ptr() as usize;
    let capacity = payload.capacity();
    let (attempted, entered) = std::sync::mpsc::sync_channel(1);
    // A cold controller drains the original value after the call-attempt fact. This also
    // releases the baseline blocking operation, allowing a semantic RED after cleanup.
    // It does not claim an observed OS-blocked interval or sibling progress while full.
    let (drained, mut released) = sync::channel(1).unwrap();
    let (mut rx, tx, result) = std::thread::scope(|scope| {
        let controller = scope.spawn(move || {
            entered.recv().unwrap();
            assert_eq!(rx.try_recv().unwrap().unwrap(), vec![7]);
            drained.try_send(()).unwrap();
            rx
        });
        let (tx, result) = runtime()
            .block_on(async move {
                attempted.try_send(()).unwrap();
                let result = tx.blocking_send(payload);
                released.recv().await.unwrap();
                (tx, result)
            })
            .unwrap();
        (controller.join().unwrap(), tx, result)
    });
    let refused = matches!(result, Err(SyncError::NotOnShardThread(_)));
    let payload = match result {
        Err(SyncError::NotOnShardThread(payload)) => {
            assert!(rx.try_recv().unwrap().is_none());
            payload
        }
        Ok(()) => rx.try_recv().unwrap().unwrap(),
        Err(_) => panic!("the data receiver remains alive"),
    };
    assert_eq!(payload, vec![1, 2, 3, 4]);
    assert_eq!(payload.as_ptr() as usize, pointer);
    assert_eq!(payload.capacity(), capacity);
    tx.try_send(payload).unwrap();
    assert_eq!(rx.try_recv().unwrap().unwrap(), vec![1, 2, 3, 4]);
    drop(tx);
    assert_eq!(rx.try_recv(), Err(SyncError::Closed(())));
    assert!(
        refused,
        "the synchronous call published its payload on the shard"
    );
}

#[test]
fn a_runtime_blocking_oneshot_refuses_before_consuming_its_ready_value() {
    #[derive(Debug)]
    struct Payload {
        bytes: Vec<u8>,
        dropped: &'static std::sync::atomic::AtomicUsize,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let dropped: &'static std::sync::atomic::AtomicUsize = Box::leak(Box::default());
    let (tx, rx) = sync::oneshot().unwrap();
    tx.send(Payload {
        bytes: vec![1, 2, 3],
        dropped,
    })
    .unwrap();
    let (result, dropped_at_return) = runtime()
        .block_on(async move {
            // Mutable for the corrected borrowed API; the same source must also compile
            // against the historical consuming signature for a real semantic RED.
            #[allow(unused_mut)]
            let mut rx = rx;
            let result = rx.blocking_recv();
            let before_scope_drop = dropped.load(std::sync::atomic::Ordering::SeqCst);
            (result, before_scope_drop)
        })
        .unwrap();
    let refused = matches!(result, Err(SyncError::NotOnShardThread(())));
    if let Ok(payload) = &result {
        assert_eq!(payload.bytes, vec![1, 2, 3]); // Baseline's consumed value is still exact.
    }
    assert_eq!(
        dropped_at_return, 0,
        "refusal dropped the ready payload during the call"
    );
    drop(result); // Baseline payload and corrected receiver both clean up before verdict.
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        refused,
        "a runtime consuming call took its ready one-shot value"
    );
}
