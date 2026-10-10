//! Simulation steps deliver readiness without relying on advancement of a virtual wall clock.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::task::Poll;

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::interests::Readiness;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

// These are the two actual roles below, not a cadence or timeout chosen for the test.
const ROLES: usize = 2;
// A public adapter identifier, never an operating-system descriptor.
const HANDLE: i32 = 0;
static BUSY_STARTED: AtomicBool = AtomicBool::new(false);
static STOP_BUSY: AtomicBool = AtomicBool::new(false);

struct VirtualReady {
    armed: Option<u64>,
    delivered: bool,
}

impl Driver for VirtualReady {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }

    fn kick_handle(&self) -> Kick {
        Kick::None
    }

    fn now_ns(&self) -> u64 {
        0
    }

    fn wait(&mut self, timeout: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        if timeout != Some(0) {
            return Err(RtError::BadConfig {
                what: "a manual virtual step must harvest without blocking",
            });
        }
        if !self.delivered
            && let Some(tag) = self.armed.take()
        {
            out.push(Completion {
                user_data: tag,
                result: Readiness::READ.bits(),
            });
            self.delivered = true;
        }
        Ok(())
    }

    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }

    fn arm(&mut self, raw: i32, want: Readiness, tag: u64) -> Result<(), RtError> {
        if raw != HANDLE || want != Readiness::READ {
            return Err(RtError::BadConfig {
                what: "the virtual driver accepts only its one read target",
            });
        }
        self.armed = Some(tag);
        Ok(())
    }

    fn has_pending(&self) -> bool {
        // Like native readiness, this adapter discovers completion only when wait is called.
        false
    }
}

fn seed() -> DriverSeed {
    Box::new(|_kick| {
        Ok(Box::new(VirtualReady {
            armed: None,
            delivered: false,
        }) as Box<dyn Driver>)
    })
}

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: ROLES,
        timers_per_shard: ROLES,
        interests_per_shard: ROLES,
        ring_entries: ROLES,
        // Existing lifecycle fixture values; no wall time advances in this test.
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: ROLES,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

#[derive(Debug)]
enum Fact {
    FirstPending(u64),
    Completed(Result<(), RtError>, u64, bool),
    Dropped(&'static str),
}

struct Capture(&'static str, Sender<Fact>);

impl Drop for Capture {
    fn drop(&mut self) {
        let _ = self.1.send(Fact::Dropped(self.0));
    }
}

#[test]
fn manual_simulation_steps_deliver_ready_io_while_time_stays_zero_and_a_sibling_yields() {
    BUSY_STARTED.store(false, Ordering::Release);
    STOP_BUSY.store(false, Ordering::Release);
    let config = config();
    // Both actors fit in one configured batch. One more owner turn allows event retrieval
    // and its waiter repoll; the bound is derived from the admitted task capacity.
    let turns = config.tasks_per_shard.checked_add(1).unwrap();
    let mut rt = LocalRuntime::with_driver(&config, seed(), Kick::None).unwrap();
    let (events, observed) = channel();
    let root_capture = Capture("read", events.clone());
    let result_events = events.clone();
    rt.spawn(async move {
        let _capture = root_capture;
        let mut read = pin!(hyper_rt::readiness::readable(HANDLE));
        let first = poll_fn(|cx| {
            Poll::Ready(match read.as_mut().poll(cx) {
                Poll::Pending => {
                    let _ = result_events.send(Fact::FirstPending(hyper_rt::futures::now_ns()));
                    Ok(())
                }
                Poll::Ready(_) => Err(RtError::BadConfig {
                    what: "readiness must first be pending before driver delivery",
                }),
            })
        })
        .await;
        let result = match first {
            Ok(()) => read.await,
            Err(error) => Err(error),
        };
        let _ = result_events.send(Fact::Completed(
            result,
            hyper_rt::futures::now_ns(),
            BUSY_STARTED.load(Ordering::Acquire) && !STOP_BUSY.load(Ordering::Acquire),
        ));
    })
    .unwrap();
    let busy_capture = Capture("busy", events);
    rt.spawn(async move {
        let _capture = busy_capture;
        BUSY_STARTED.store(true, Ordering::Release);
        while !STOP_BUSY.load(Ordering::Acquire) {
            hyper_rt::futures::yield_now().await;
        }
    })
    .unwrap();
    let mut before_drop = Vec::new();
    for _ in 0..turns {
        // No park, block_on, timer, fake clock advance or separate driver pump.
        let _ = rt.step();
        before_drop.extend(observed.try_iter());
        if before_drop
            .iter()
            .any(|fact| matches!(fact, Fact::Completed(..)))
        {
            break;
        }
    }
    drop(rt);
    let mut after_drop: Vec<_> = observed.try_iter().collect();
    let pending = before_drop
        .iter()
        .any(|fact| matches!(fact, Fact::FirstPending(0)));
    let completed = before_drop
        .iter()
        .any(|fact| matches!(fact, Fact::Completed(Ok(()), 0, true)));
    after_drop.extend(before_drop.iter().filter_map(|fact| match fact {
        Fact::Dropped(role) => Some(Fact::Dropped(role)),
        _ => None,
    }));
    let mut dropped: Vec<_> = after_drop
        .iter()
        .filter_map(|fact| match fact {
            Fact::Dropped(role) => Some(*role),
            _ => None,
        })
        .collect();
    dropped.sort_unstable();
    assert!(
        pending,
        "the read actually waited at virtual time zero: {before_drop:?}"
    );
    assert!(
        completed,
        "bounded manual steps must deliver while the sibling remains busy: {before_drop:?}"
    );
    assert_eq!(
        dropped,
        ["busy", "read"],
        "owner retirement closes both captured tasks"
    );
}
