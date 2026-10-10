//! Actual worker-thread driverless parks and foreign channel completions. A
//! supervisor deadline may fail the test, never release or satisfy an ordinal.
#![allow(
    clippy::cognitive_complexity,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::arithmetic_side_effects
)]

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick, Prepared};
use hyper_rt::futures;
use hyper_rt::interests::Readiness;
use hyper_rt::registry;
use hyper_rt::runtime::{Runtime, RuntimeConfig, interests_for};
use hyper_rt::sync;
use hyper_rt::task::Admission;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::task::Poll;
use std::thread::{self, Thread};

/// The existing sync.rs DROP_RACE_ROUNDS stress budget, without a new cadence.
const ROUNDS: u64 = 3_000;
/// One retained service and the separate task that actually loses the driver.
const ROLES: usize = 2;
/// Adapter-only readiness identity, never passed to an OS backend.
const HANDLE: i32 = 0;
static AFTER_LOSS: AtomicUsize = AtomicUsize::new(0);

fn config() -> RuntimeConfig {
    // Existing cooperative_service shape/timing, with exactly the two roles.
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

struct Lost {
    lost: bool,
}
impl Lost {
    fn record(&self) {
        if self.lost {
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
    fn submit_nop(&mut self, _: u64) -> Result<(), RtError> {
        self.record();
        Ok(())
    }
    fn arm(&mut self, _: i32, _: Readiness, _: u64) -> Result<(), RtError> {
        self.record();
        self.lost = true;
        Err(RtError::DriverLost)
    }
    fn disarm(&mut self, _: i32) {
        self.record();
    }
}

struct Returned(SyncSender<u64>);
impl Drop for Returned {
    fn drop(&mut self) {
        let _ = self.0.try_send(ROUNDS);
    }
}

#[test]
fn a_lost_worker_parks_on_its_actual_thread_and_repeated_foreign_wakes_preserve_exact_values() {
    AFTER_LOSS.store(0, Ordering::SeqCst);
    let mut prepare = || {
        Ok(Prepared {
            seed: Box::new(|_| Ok(Box::new(Lost { lost: false }))),
            kick_fd: None,
            notes: Vec::new(),
        })
    };
    let rt = Runtime::start_with(&config(), &mut prepare).unwrap();
    let shard = *rt.shard_ids().first().unwrap();
    let (first, pending) = sync_channel::<Thread>(1);
    let (facts, observed) = sync_channel::<u64>(1);
    let (release, mut values) = sync::channel::<u64>(1).unwrap();
    let (done, returned) = sync_channel(1);
    let receipt = rt
        .spawn_service_on_with_receipt(shard, async move {
            let _capture = Returned(done);
            let mut cancel = pin!(futures::cancelled());
            let mut noted = false;
            poll_fn(|cx| match cancel.as_mut().poll(cx) {
                Poll::Ready(value) => Poll::Ready(value),
                Poll::Pending => {
                    if !noted {
                        first.try_send(thread::current()).unwrap();
                        noted = true;
                    }
                    Poll::Pending
                }
            })
            .await
            .unwrap();
            for ordinal in 0..ROUNDS {
                let mut receive = pin!(values.recv());
                let mut noted = false;
                let value = poll_fn(|cx| match receive.as_mut().poll(cx) {
                    Poll::Ready(value) => Poll::Ready(value),
                    Poll::Pending => {
                        if !noted {
                            assert_eq!(
                                registry::with_entry(shard.0, |entry| entry.parking.parked()),
                                Some(false)
                            );
                            facts.try_send(ordinal).unwrap();
                            noted = true;
                        }
                        Poll::Pending
                    }
                })
                .await;
                assert_eq!(value, Ok(ordinal));
            }
            assert!(matches!(values.try_recv(), Ok(None)));
        })
        .unwrap();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    let owner = pending.recv().unwrap();
    assert_ne!(
        owner.id(),
        thread::current().id(),
        "seed caller is not the owner"
    );
    let trigger = rt
        .spawn_on_with_receipt(shard, async {
            let _ = hyper_rt::readiness::readable(HANDLE).await;
        })
        .unwrap();
    assert!(matches!(
        trigger.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    for ordinal in 0..ROUNDS {
        assert_eq!(observed.recv().unwrap(), ordinal);
        // Coalesced/stale permits carry no message and cannot satisfy the value.
        // They can precede or interrupt a park; this tests both legal schedules.
        owner.unpark();
        owner.unpark();
        while !registry::with_entry(shard.0, |entry| entry.parking.parked()).unwrap_or(false) {
            thread::yield_now();
        }
        assert!(
            returned.try_recv().is_err(),
            "service still owns its capture"
        );
        release.blocking_send(ordinal).unwrap();
    }
    assert_eq!(returned.recv().unwrap(), ROUNDS);
    drop(release);
    assert_eq!(rt.shutdown(), Err(RtError::DriverLost));
    assert_eq!(AFTER_LOSS.load(Ordering::SeqCst), 0);
    assert!(
        returned.try_recv().is_err(),
        "capture returned exactly once"
    );
}
