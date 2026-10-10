//! A failed owner refuses admission while retained; cancellation cannot leave destructor-spawned work.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, pending};
use std::sync::mpsc::{Sender, channel};
use std::time::Duration;

use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::interests::Readiness;
use hyper_rt::registry::{self, SlotHolder};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, submit_to_holder};
use hyper_rt::task::{Admission, AdmissionReceipt};
use hyper_rt::{RtError, TaskId};

/// The driver's one opaque test handle; no native descriptor is accessed.
const HANDLE: i32 = 0;

struct TestDriver {
    lost: bool,
}

impl Driver for TestDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }

    fn kick_handle(&self) -> Kick {
        Kick::None
    }

    fn now_ns(&self) -> u64 {
        // Virtual time need not advance: the first driver wait is already a terminal failure.
        0
    }

    fn wait(&mut self, _timeout: Option<u64>, _out: &mut Vec<Completion>) -> Result<(), RtError> {
        if self.lost {
            Err(RtError::DriverLost)
        } else {
            Ok(())
        }
    }

    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }

    fn arm(&mut self, _raw: i32, _want: Readiness, _tag: u64) -> Result<(), RtError> {
        Ok(())
    }

    fn has_pending(&self) -> bool {
        self.lost
    }
}

fn seed(lost: bool) -> DriverSeed {
    Box::new(move |_kick| Ok(Box::new(TestDriver { lost }) as Box<dyn Driver>))
}

fn config() -> RuntimeConfig {
    // Existing lifecycle fixture settings; no time budget drives the injected failure.
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 64,
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

fn owner() -> LocalRuntime {
    LocalRuntime::with_driver(&config(), seed(true), Kick::None).unwrap()
}

fn healthy_owner() -> LocalRuntime {
    LocalRuntime::with_driver(&config(), seed(false), Kick::None).unwrap()
}

struct DropNotice(&'static str, Sender<&'static str>);

impl Drop for DropNotice {
    fn drop(&mut self) {
        let _ = self.1.send(self.0);
    }
}

fn probe(role: &'static str, notices: Sender<&'static str>) -> impl Future<Output = ()> + Send {
    // Captured before polling: even an unadmitted future must give this object back.
    let notice = DropNotice(role, notices);
    async move {
        let _notice = notice;
        pending::<()>().await;
    }
}

fn failed_owner() -> (LocalRuntime, SlotHolder) {
    let mut rt = owner();
    let holder = registry::holder_of(rt.shard_id().0).expect("the live owner has a holder");
    let result = rt.block_on(hyper_rt::readiness::readable(HANDLE));
    assert!(
        matches!(result, Err(RtError::ShardGone { .. })),
        "setup must lose its driver: {result:?}"
    );
    (rt, holder)
}

fn healthy_after_retirement() {
    let mut fresh = healthy_owner();
    // This owner advertises no pending work and has no injected driver failure.
    assert_eq!(fresh.block_on(async { "fresh owner" }), Ok("fresh owner"));
}

#[test]
fn a_retained_failed_owner_refuses_local_work_and_returns_its_capture_immediately() {
    let (notices, dropped) = channel();
    let (mut rt, _) = failed_owner();
    let result = rt.spawn(probe("local", notices));
    let returned_before_owner_drop = dropped.try_recv().ok();
    drop(rt);
    assert!(
        matches!(result, Err(RtError::ShardGone { .. })),
        "post-exit local admission: {result:?}"
    );
    assert_eq!(returned_before_owner_drop, Some("local"));
    healthy_after_retirement();
}

#[test]
fn a_retained_failed_holder_refuses_foreign_work_and_returns_its_capture_immediately() {
    let (notices, dropped) = channel();
    let (rt, holder) = failed_owner();
    let result = submit_to_holder(holder, probe("foreign", notices));
    let returned_before_owner_drop = dropped.try_recv().ok();
    drop(rt);
    assert!(
        matches!(result, Err(RtError::ShardGone { .. })),
        "post-exit foreign admission: {result:?}"
    );
    assert_eq!(returned_before_owner_drop, Some("foreign"));
    healthy_after_retirement();
}

#[derive(Debug)]
enum Attempt {
    Local(Result<TaskId, RtError>),
    Foreign(Result<AdmissionReceipt, RtError>),
}

struct SpawnOnDrop {
    foreign: Option<SlotHolder>,
    notices: Sender<&'static str>,
    attempts: Sender<Attempt>,
}

impl Drop for SpawnOnDrop {
    fn drop(&mut self) {
        let _ = self.notices.send("original");
        let local = hyper_rt::futures::spawn_detached(probe("spawned", self.notices.clone()));
        let _ = self.attempts.send(Attempt::Local(local));
        if let Some(holder) = self.foreign {
            let foreign = submit_to_holder(holder, probe("foreign", self.notices.clone()));
            let _ = self.attempts.send(Attempt::Foreign(foreign));
        }
    }
}

#[test]
fn fatal_cancellation_seals_destructor_admission_and_ends_any_accepted_receipt() {
    let (notices, dropped) = channel();
    let (attempts, reported) = channel();
    let mut rt = owner();
    let holder = registry::holder_of(rt.shard_id().0).unwrap();
    let on_drop = SpawnOnDrop {
        foreign: Some(holder),
        notices,
        attempts,
    };
    let result = rt.block_on(async move {
        let _on_drop = on_drop;
        hyper_rt::readiness::readable(HANDLE).await
    });
    let attempts: Vec<_> = reported.try_iter().collect();
    let mut returned: Vec<_> = dropped.try_iter().collect();
    returned.sort_unstable();
    // Accepted just before closure is also lawful, but its receipt must be terminal by return.
    let local_refused = attempts
        .iter()
        .any(|attempt| matches!(attempt, Attempt::Local(Err(RtError::ShardGone { .. }))));
    let foreign_terminal = attempts.iter().any(|attempt| match attempt {
        Attempt::Foreign(Err(RtError::ShardGone { .. })) => true,
        Attempt::Foreign(Ok(receipt)) => {
            receipt.wait(Duration::ZERO) == Some(Admission::Terminated)
        }
        _ => false,
    });
    drop(rt);
    assert!(
        matches!(result, Err(RtError::ShardGone { .. })),
        "fatal result: {result:?}"
    );
    assert!(
        local_refused,
        "destructor cannot admit uncancelled local work: {attempts:?}"
    );
    assert!(
        foreign_terminal,
        "foreign refusal or terminal queued receipt: {attempts:?}"
    );
    assert_eq!(returned, ["foreign", "original", "spawned"]);
    healthy_after_retirement();
}

#[test]
fn normal_root_cleanup_drops_destructor_work_before_return_and_preserves_owner_reuse() {
    let (notices, dropped) = channel();
    let (attempts, reported) = channel();
    let on_drop = SpawnOnDrop {
        foreign: None,
        notices,
        attempts,
    };
    let mut rt = healthy_owner();
    let result = rt.block_on(async move {
        hyper_rt::futures::spawn_detached(async move {
            let _on_drop = on_drop;
            pending::<()>().await;
        })?;
        Ok::<_, RtError>(42)
    });
    let mut returned: Vec<_> = dropped.try_iter().collect();
    returned.sort_unstable();
    let attempted = reported
        .try_iter()
        .any(|attempt| matches!(attempt, Attempt::Local(_)));
    let reused = rt.block_on(async { 43 });
    drop(rt);
    assert_eq!(result, Ok(Ok(42)));
    assert!(
        attempted,
        "the cancelled capture actually attempted another admission"
    );
    assert_eq!(
        returned,
        ["original", "spawned"],
        "nothing spawned by cleanup outlives the call"
    );
    assert_eq!(reused, Ok(43));
}
