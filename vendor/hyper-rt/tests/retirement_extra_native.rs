//! Additional public reaper lifecycle oracles. Artifact only: not compiled or run.
//! The external supervisor's deadline is failure only; every successful release follows actual facts.
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

use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::task::Poll;
use std::thread;

use hyper_rt::driver::{Clock, Completion, Driver, DriverKind, Kick, Prepared, os_driver};
use hyper_rt::interests::Readiness;
use hyper_rt::registry;
use hyper_rt::runtime::{RetirementLease, interests_for};
use hyper_rt::task::Admission;
use hyper_rt::{RtError, Runtime, RuntimeConfig, futures};

fn config(roles: usize) -> RuntimeConfig {
    // Reuse the native cooperative-service fixture's clock/page shape. Capacity is
    // the exact number of public task/group reservations exercised by the test.
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

fn lease(runtime: &mut Runtime, capacity: usize) -> RetirementLease {
    runtime
        .prepare_retirement(&[capacity])
        .unwrap()
        .pop()
        .unwrap()
}

struct Returned(Arc<AtomicUsize>);
impl Drop for Returned {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn completed_group_receipts_allow_repeated_full_capacity_cold_setup() {
    // Two simultaneously reserved groups fill the configured public reservation bound.
    // More setup generations than that bound require actual completed-owner pruning.
    let cfg = config(2);
    let mut runtime = Runtime::start(&cfg).unwrap();
    let returned = Arc::new(AtomicUsize::new(0));
    for generation in 0..cfg.tasks_per_shard.saturating_add(1) {
        let capacities = vec![1; cfg.tasks_per_shard];
        let mut leases = runtime.prepare_retirement(&capacities).unwrap();
        for lease in &mut leases {
            let capture = Returned(Arc::clone(&returned));
            let mut threads = vec![thread::spawn(move || {
                let _capture = capture;
            })];
            lease.adopt(&mut threads).unwrap();
            assert!(threads.is_empty());
        }
        for lease in &mut leases {
            lease.request().unwrap();
        }
        for lease in &mut leases {
            lease.wait_blocking().unwrap();
        }
        assert_eq!(
            returned.load(Ordering::SeqCst),
            (generation + 1) * cfg.tasks_per_shard
        );
        // The next iteration does not poll an internal done flag or retry Capacity.
        // All completed public receipts must be sufficient for another cold setup.
        drop(leases);
    }
    runtime.shutdown().unwrap();
}

struct HeldTls {
    entered: SyncSender<()>,
    release: Receiver<()>,
    returned: Arc<AtomicUsize>,
}

impl Drop for HeldTls {
    fn drop(&mut self) {
        let _ = self.entered.try_send(());
        // A guard release or channel closure is cleanup. Success additionally requires
        // the explicit positive Pending/park/retained-owner facts before that release.
        let _ = self.release.recv();
        self.returned.fetch_add(1, Ordering::SeqCst);
    }
}

thread_local! {
    static TLS_HELD: Cell<Option<HeldTls>> = const { Cell::new(None) };
}

struct OpenOnDrop(Option<SyncSender<()>>);
impl OpenOnDrop {
    fn open(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.try_send(());
        }
    }
}
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.open();
    }
}

async fn first_pending<F: Future>(mut future: Pin<&mut F>, fact: &SyncSender<()>) -> F::Output {
    let mut sent = false;
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => {
            if !sent {
                fact.try_send(()).unwrap();
                sent = true;
            }
            Poll::Pending
        }
        Poll::Ready(result) => Poll::Ready(result),
    })
    .await
}

struct LostOnArm {
    inner: Box<dyn Driver>,
    lost: SyncSender<()>,
    failed: bool,
}

impl Driver for LostOnArm {
    fn kind(&self) -> DriverKind {
        self.inner.kind()
    }
    fn kick_handle(&self) -> Kick {
        self.inner.kick_handle()
    }
    fn now_ns(&self) -> u64 {
        self.inner.now_ns()
    }
    fn wait(&mut self, timeout: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        self.inner.wait(timeout, out)
    }
    fn submit_nop(&mut self, tag: u64) -> Result<(), RtError> {
        self.inner.submit_nop(tag)
    }
    fn arm(&mut self, _: i32, _: Readiness, _: u64) -> Result<(), RtError> {
        if !self.failed {
            self.lost.try_send(()).map_err(|_| RtError::BadConfig {
                what: "test driver-loss observer unavailable",
            })?;
            self.failed = true;
        }
        Err(RtError::DriverLost)
    }
    fn disarm(&mut self, raw: i32) {
        if !self.failed {
            self.inner.disarm(raw);
        }
    }
    fn reserve_handles(&mut self, handles: usize) -> Result<(), RtError> {
        self.inner.reserve_handles(handles)
    }
    fn has_pending(&self) -> bool {
        self.inner.has_pending()
    }
    fn clock(&self) -> Clock {
        self.inner.clock()
    }
    fn is_sim(&self) -> bool {
        self.inner.is_sim()
    }
}

fn drop_with_held_native_owner(driver_loss: bool) {
    // One admitted service. Its complete native group is adopted before task admission.
    let cfg = config(1);
    let (lost, loss) = sync_channel(1);
    let mut runtime = if driver_loss {
        Runtime::start_with(&cfg, &mut || {
            let prepared = os_driver(u32::try_from(cfg.ring_entries).unwrap())?;
            let seed = prepared.seed;
            let lost = lost.clone();
            Ok(Prepared {
                kick_fd: prepared.kick_fd,
                notes: prepared.notes,
                seed: Box::new(move |kick| {
                    Ok(Box::new(LostOnArm {
                        inner: seed(kick)?,
                        lost,
                        failed: false,
                    }))
                }),
            })
        })
        .unwrap()
    } else {
        Runtime::start(&cfg).unwrap()
    };
    let mut lease = lease(&mut runtime, 1);
    let shard = runtime.shard_ids()[0];
    let native_returned = Arc::new(AtomicUsize::new(0));
    let service_returned = Arc::new(AtomicUsize::new(0));
    let (release, held) = sync_channel(1);
    // Declared after Runtime: setup unwinding releases native TLS before Runtime joins.
    let guard = OpenOnDrop(Some(release));
    let (entered, actually_held) = sync_channel(1);
    let returned = Arc::clone(&native_returned);
    let mut threads = vec![thread::spawn(move || {
        TLS_HELD.with(|slot| {
            assert!(
                slot.replace(Some(HeldTls {
                    entered,
                    release: held,
                    returned
                }))
                .is_none()
            );
        });
    })];
    actually_held.recv().unwrap();
    lease.adopt(&mut threads).unwrap();
    assert!(threads.is_empty());
    let (started, first_pending_fact) = sync_channel(1);
    let (closing, cleanup_pending) = sync_channel(1);
    let (finished, service_finished) = sync_channel(1);
    let capture = Returned(Arc::clone(&service_returned));
    let observed_native = Arc::clone(&native_returned);
    let receipt = runtime
        .spawn_service_on_with_receipt(shard, async move {
            let _capture = capture;
            if driver_loss {
                // Opaque handle zero reaches only the fault adapter; it never arms the OS.
                let mut ready = pin!(hyper_rt::readiness::readable(0));
                assert_eq!(
                    first_pending(ready.as_mut(), &started).await,
                    Err(RtError::DriverLost)
                );
            } else {
                let mut cancel = pin!(futures::cancelled());
                first_pending(cancel.as_mut(), &started).await.unwrap();
            }
            let mut wait = pin!(lease.wait());
            first_pending(wait.as_mut(), &closing).await.unwrap();
            assert_eq!(observed_native.load(Ordering::SeqCst), 1);
            finished.try_send(()).unwrap();
        })
        .unwrap();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    first_pending_fact.recv().unwrap();
    if driver_loss {
        loss.recv().unwrap();
    }
    thread::scope(|scope| {
        // Inside the scope, so a failed assertion opens TLS before implicit thread joins.
        let mut guard = guard;
        let (dropping, drop_started) = sync_channel(1);
        let (done, runtime_dropped) = sync_channel(1);
        let owner = scope.spawn(move || {
            dropping.try_send(()).unwrap();
            drop(runtime);
            done.try_send(()).unwrap();
        });
        drop_started.recv().unwrap();
        cleanup_pending.recv().unwrap();
        loop {
            assert!(
                runtime_dropped.try_recv().is_err(),
                "Runtime Drop returned while native TLS was held"
            );
            let parked = registry::with_entry(shard.0, |entry| entry.parking.parked())
                .expect("the service registration must remain owned before retirement");
            if parked {
                break;
            }
            thread::yield_now();
        }
        assert_eq!(native_returned.load(Ordering::SeqCst), 0);
        assert_eq!(service_returned.load(Ordering::SeqCst), 0);
        assert!(service_finished.try_recv().is_err());
        guard.open();
        runtime_dropped.recv().unwrap();
        owner.join().unwrap();
        service_finished.recv().unwrap();
        assert_eq!(native_returned.load(Ordering::SeqCst), 1);
        assert_eq!(service_returned.load(Ordering::SeqCst), 1);
        assert!(registry::with_entry(shard.0, |_| ()).is_none());
    });
}

#[test]
fn runtime_drop_keeps_a_cancelled_service_and_its_held_native_tls_owned() {
    drop_with_held_native_owner(false);
}

#[test]
fn runtime_drop_keeps_a_driver_lost_service_and_its_held_native_tls_owned() {
    drop_with_held_native_owner(true);
}
