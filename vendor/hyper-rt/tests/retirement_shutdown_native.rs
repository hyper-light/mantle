//! Runtime owner joins shard services before its cold native retirement role.
//! Success requires actual service cancellation and a Pending native join receipt.
#![allow(
    clippy::cognitive_complexity,
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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::Poll;

use hyper_rt::driver::{Completion, Driver, DriverKind, Kick, Prepared};
use hyper_rt::runtime::interests_for;
use hyper_rt::task::Admission;
use hyper_rt::{RtError, Runtime, RuntimeConfig, futures};

fn config() -> RuntimeConfig {
    // One service only; the gate controller is a plain independently joined test thread.
    let roles = 1;
    RuntimeConfig {
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
    }
}

struct Returned(Arc<AtomicUsize>);
impl Drop for Returned {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

// A failed setup closes the gate with the wrong value, releasing owned native resources
// without satisfying the successful completion oracle.
struct OpenOnDrop(mpsc::SyncSender<u64>);
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        let _ = self.0.try_send(0);
    }
}

struct LossOnArm {
    lost: bool,
    arm_lost: Arc<AtomicBool>,
    dead_calls: Arc<AtomicUsize>,
}
impl LossOnArm {
    fn called(&self) {
        if self.lost {
            self.dead_calls.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Driver for LossOnArm {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        self.called();
        0
    }
    fn wait(&mut self, _: Option<u64>, _: &mut Vec<Completion>) -> Result<(), RtError> {
        self.called();
        Ok(())
    }
    fn submit_nop(&mut self, _: u64) -> Result<(), RtError> {
        self.called();
        Ok(())
    }
    fn arm(&mut self, _: i32, _: hyper_rt::interests::Readiness, _: u64) -> Result<(), RtError> {
        self.called();
        self.lost = true;
        self.arm_lost.store(true, Ordering::SeqCst);
        Err(RtError::DriverLost)
    }
    fn disarm(&mut self, _: i32) {
        self.called();
    }
    fn has_pending(&self) -> bool {
        self.called();
        false
    }
}

fn shutdown_with_held_adopted_worker(loss: bool) {
    let arm_lost = Arc::new(AtomicBool::new(false));
    let dead_calls = Arc::new(AtomicUsize::new(0));
    let mut rt = if loss {
        let arm_lost = Arc::clone(&arm_lost);
        let dead_calls = Arc::clone(&dead_calls);
        Runtime::start_with(&config(), &mut || {
            let arm_lost = Arc::clone(&arm_lost);
            let dead_calls = Arc::clone(&dead_calls);
            Ok(Prepared {
                seed: Box::new(move |_| {
                    Ok(Box::new(LossOnArm {
                        lost: false,
                        arm_lost,
                        dead_calls,
                    }))
                }),
                kick_fd: None,
                notes: Vec::new(),
            })
        })
        .unwrap()
    } else {
        Runtime::start(&config()).unwrap()
    };
    let mut leases = rt.prepare_retirement(&[1]).unwrap();
    let mut lease = leases.pop().unwrap();
    let worker_returned = Arc::new(AtomicUsize::new(0));
    let service_returned = Arc::new(AtomicUsize::new(0));
    let actual_release = Arc::new(AtomicBool::new(false));
    let (release, held) = mpsc::sync_channel(1);
    let _open = OpenOnDrop(release.clone());
    let (native_started, native_entered) = mpsc::sync_channel(1);
    let native_capture = Returned(Arc::clone(&worker_returned));
    let release_fact = Arc::clone(&actual_release);
    let mut threads = vec![std::thread::spawn(move || {
        let _capture = native_capture;
        native_started.try_send(()).unwrap();
        release_fact.store(held.recv() == Ok(42), Ordering::SeqCst);
    })];
    lease.adopt(&mut threads).unwrap();
    assert!(threads.is_empty());
    native_entered.recv().unwrap();

    let (waiting, pending) = mpsc::sync_channel(1);
    let (began, started) = mpsc::sync_channel(1);
    let (finished, result) = mpsc::sync_channel(1);
    let capture = Returned(Arc::clone(&service_returned));
    let shard = rt.shard_ids()[0];
    let receipt = rt
        .spawn_service_on_with_receipt(shard, async move {
            let _capture = capture;
            let read_result = if loss {
                let result = hyper_rt::readiness::readable(0).await;
                began.try_send(()).unwrap(); // Actual arm loss and its typed outcome precede shutdown.
                result == Err(RtError::DriverLost)
            } else {
                let mut cancellation = pin!(futures::cancelled());
                let mut announced = false;
                let result = poll_fn(|cx| match cancellation.as_mut().poll(cx) {
                    Poll::Pending => {
                        if !announced {
                            began.try_send(()).unwrap();
                            announced = true;
                        }
                        Poll::Pending
                    }
                    Poll::Ready(result) => Poll::Ready(result),
                })
                .await;
                result.is_ok()
            };
            let canceled = futures::cancelled().await.is_ok();
            let mut wait = pin!(lease.wait());
            let mut announced = false;
            let retired = poll_fn(|cx| match wait.as_mut().poll(cx) {
                Poll::Pending => {
                    if !announced {
                        waiting.try_send(()).unwrap();
                        announced = true;
                    }
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            })
            .await;
            finished
                .try_send((read_result, canceled, retired.is_ok(), announced))
                .unwrap();
        })
        .unwrap();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    started.recv().unwrap();

    let worker_observed = Arc::clone(&worker_returned);
    let service_observed = Arc::clone(&service_returned);
    let shutdown = std::thread::scope(|scope| {
        let controller = scope.spawn(move || {
            let _cleanup = OpenOnDrop(release.clone());
            let fact = pending.recv().is_ok();
            let retained = worker_observed.load(Ordering::SeqCst) == 0
                && service_observed.load(Ordering::SeqCst) == 0;
            let sent = fact && retained && release.try_send(42).is_ok();
            (fact, retained, sent)
        });
        let shutdown = rt.shutdown();
        let controller = controller.join().unwrap();
        assert_eq!(controller, (true, true, true));
        shutdown
    });
    if loss {
        assert!(matches!(shutdown, Err(RtError::DriverLost)), "{shutdown:?}");
    } else {
        assert!(shutdown.is_ok(), "{shutdown:?}");
    }
    assert_eq!(result.try_recv(), Ok((true, true, true, true)));
    assert_eq!(worker_returned.load(Ordering::SeqCst), 1);
    assert_eq!(service_returned.load(Ordering::SeqCst), 1);
    assert!(actual_release.load(Ordering::SeqCst));
    assert_eq!(arm_lost.load(Ordering::SeqCst), loss);
    assert_eq!(dead_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn shutdown_keeps_retirement_alive_through_cancelled_service_cleanup() {
    shutdown_with_held_adopted_worker(false);
}

#[test]
fn shutdown_keeps_retirement_alive_through_driver_loss_service_cleanup() {
    shutdown_with_held_adopted_worker(true);
}
