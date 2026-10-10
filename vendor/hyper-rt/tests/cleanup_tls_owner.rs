//! macOS Rust TLS destruction occurs before std thread cleanup. The owner stays
//! held through actual driverless cleanup and an independent channel release.
//! Foreign/native TLS destructors after std cleanup are outside this proof.
#![cfg(target_os = "macos")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::futures;
use hyper_rt::interests::Readiness;
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::sync;
use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::task::Poll;
use std::thread::{self, JoinHandle};

/// One retained service and one separate actual driver-loss trigger.
const ROLES: usize = 2;
/// Adapter-only identity, never passed to an OS backend.
const HANDLE: i32 = 0;
static AFTER_LOSS: AtomicUsize = AtomicUsize::new(0);

struct TlsOwner {
    runtime: Option<LocalRuntime>,
    returned: SyncSender<()>,
}
impl Drop for TlsOwner {
    fn drop(&mut self) {
        drop(self.runtime.take());
        let _ = self.returned.try_send(());
    }
}
thread_local! {
    static OWNER: Cell<Option<TlsOwner>> = const { Cell::new(None) };
}

// Failure closes the sole completion sender before joining the owned child. A
// Closed reply releases resources but cannot satisfy the explicit Ok(42) oracle.
struct Child {
    release: Option<sync::Sender<u64>>,
    thread: Option<JoinHandle<()>>,
}
impl Drop for Child {
    fn drop(&mut self) {
        drop(self.release.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct Returned(SyncSender<()>);
impl Drop for Returned {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

struct Lost(bool);
impl Lost {
    fn record(&self) {
        if self.0 {
            AFTER_LOSS.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Driver for Lost {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        self.record();
        0
    }
    fn has_pending(&self) -> bool {
        self.record();
        false
    }
    fn wait(&mut self, _: Option<u64>, _: &mut Vec<Completion>) -> Result<(), RtError> {
        self.record();
        Ok(())
    }
    fn arm(&mut self, _: i32, _: Readiness, _: u64) -> Result<(), RtError> {
        self.record();
        self.0 = true;
        Err(RtError::DriverLost)
    }
    fn submit_nop(&mut self, _: u64) -> Result<(), RtError> {
        self.record();
        Ok(())
    }
    fn disarm(&mut self, _: i32) {
        self.record();
    }
}

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: ROLES,
        timers_per_shard: ROLES,
        interests_per_shard: interests_for(ROLES),
        ring_entries: ROLES,
        batch: ROLES,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

#[test]
fn a_rust_tls_local_owner_retains_driverless_cleanup_until_an_independent_completion() {
    AFTER_LOSS.store(0, Ordering::SeqCst);
    let (first, pending) = sync_channel(1);
    let (waiting, cleanup) = sync_channel(1);
    let (stored, installed) = sync_channel(1);
    let (capture, captured) = sync_channel(1);
    let (owner, owner_returned) = sync_channel(1);
    let (answered, result) = sync_channel(1);
    let (release, mut receipt) = sync::channel::<u64>(1).unwrap();
    let child = thread::spawn(move || {
        let seed: DriverSeed = Box::new(|_| Ok(Box::new(Lost(false))));
        let mut rt = LocalRuntime::with_driver(&config(), seed, Kick::None).unwrap();
        let shard = rt.shard_id();
        let service = rt
            .spawn_service(async move {
                let _capture = Returned(capture);
                let mut cancel = pin!(futures::cancelled());
                let mut noted = false;
                let cancelled = poll_fn(|cx| match cancel.as_mut().poll(cx) {
                    Poll::Ready(value) => Poll::Ready(value),
                    Poll::Pending => {
                        if !noted {
                            first.try_send(()).unwrap();
                            noted = true;
                        }
                        Poll::Pending
                    }
                })
                .await;
                let mut receive = pin!(receipt.recv());
                let mut noted = false;
                let value = poll_fn(|cx| match receive.as_mut().poll(cx) {
                    Poll::Ready(value) => Poll::Ready(value),
                    Poll::Pending => {
                        if !noted {
                            assert_eq!(
                                registry::with_entry(shard.0, |entry| entry.parking.parked()),
                                Some(false)
                            );
                            waiting.try_send(shard).unwrap();
                            noted = true;
                        }
                        Poll::Pending
                    }
                })
                .await;
                // Check outside the owner after joining, so failure cleanup can
                // close the channel without panicking inside a TLS destructor.
                let _ = answered.try_send((cancelled, value));
            })
            .unwrap();
        rt.context().detach(service).unwrap();
        rt.run_until_idle();
        // The initial cancellation wait must really have been polled before loss.
        // The parent consumes its fact; no guessed number of turns is required.
        let trigger = rt
            .spawn(async {
                let _ = hyper_rt::readiness::readable(HANDLE).await;
            })
            .unwrap();
        rt.context().detach(trigger).unwrap();
        rt.run_until_idle();
        assert!(
            !rt.exited(),
            "pending service remains owned before TLS teardown"
        );
        OWNER.with(|slot| {
            assert!(
                slot.replace(Some(TlsOwner {
                    runtime: Some(rt),
                    returned: owner,
                }))
                .is_none()
            );
        });
        stored.try_send(shard).unwrap();
        // Returning starts the Rust TLS destructor, which owns the only runtime.
    });
    let mut child = Child {
        release: Some(release),
        thread: Some(child),
    };
    pending.recv().unwrap();
    let shard = cleanup.recv().unwrap();
    assert_eq!(installed.recv().unwrap(), shard);
    // run_until_idle has already returned and the owner is now in Rust TLS.
    // The next fresh park therefore belongs to owner destruction, not setup.
    while !registry::with_entry(shard.0, |entry| entry.parking.parked()).unwrap_or(false) {
        thread::yield_now();
    }
    let capture_was_held = captured.try_recv().is_err();
    let owner_was_held = owner_returned.try_recv().is_err();
    child.release.as_ref().unwrap().try_send(42).unwrap();
    drop(child.release.take());
    let answer = result.recv().unwrap();
    captured.recv().unwrap();
    owner_returned.recv().unwrap();
    child.thread.take().unwrap().join().unwrap();
    assert!(capture_was_held && owner_was_held);
    assert_eq!(answer, (Ok(()), Ok(42)));
    assert!(registry::holder_of(shard.0).is_none());
    assert_eq!(AFTER_LOSS.load(Ordering::SeqCst), 0);
    assert!(
        captured.try_recv().is_err(),
        "capture returned exactly once"
    );
    assert!(
        owner_returned.try_recv().is_err(),
        "TLS owner returned exactly once"
    );
}
