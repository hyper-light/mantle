//! Driver loss terminates a threaded owner; all owners join, and repeated loss leaves healthy reuse.
//! Each wait is on the fact it needs: a lost owner that never drops its tasks never sends their
//! drops, and the job's own limit is the failure. No clock decides an outcome.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cognitive_complexity
)]

use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::time::Instant;

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick, Prepared};
use hyper_rt::interests::Readiness;
use hyper_rt::runtime::{LocalRuntime, Runtime, RuntimeConfig};

static BUSY_STARTED: AtomicBool = AtomicBool::new(false);

struct LossOnRead {
    epoch: Instant,
    armed: bool,
    losses: Sender<bool>,
}

impl Driver for LossOnRead {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
    fn wait(&mut self, timeout: Option<u64>, _out: &mut Vec<Completion>) -> Result<(), RtError> {
        if self.armed {
            let _ = self.losses.send(timeout == Some(0));
            Err(RtError::DriverLost)
        } else {
            // A harmless spurious wake keeps this adapter independent of a native kick handle.
            std::thread::yield_now();
            Ok(())
        }
    }
    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }
    fn arm(&mut self, _raw: i32, _want: Readiness, _tag: u64) -> Result<(), RtError> {
        self.armed = true;
        Ok(())
    }
    fn has_pending(&self) -> bool {
        self.armed
    }
}

fn seed(losses: Sender<bool>) -> DriverSeed {
    Box::new(move |_kick| {
        Ok(Box::new(LossOnRead {
            epoch: Instant::now(),
            armed: false,
            losses,
        }) as Box<dyn Driver>)
    })
}

fn prepared(losses: Sender<bool>) -> Prepared {
    Prepared {
        seed: seed(losses),
        kick_fd: None,
        notes: Vec::new(),
    }
}

fn config() -> RuntimeConfig {
    RuntimeConfig {
        // Two actual owner threads: a failed owner and an unaffected sibling.
        shards: 2,
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

struct Capture(&'static str, Sender<&'static str>);

impl Drop for Capture {
    fn drop(&mut self) {
        let _ = self.1.send(self.0);
    }
}

#[test]
fn busy_threaded_driver_loss_drops_its_tasks_and_shutdown_joins_the_healthy_sibling() {
    BUSY_STARTED.store(false, Ordering::Release);
    let (losses, lost) = channel();
    let runtime = Runtime::start_with(&config(), &mut || Ok(prepared(losses.clone()))).unwrap();
    let first = runtime.shard_ids()[0];
    let second = runtime.shard_ids()[1];
    let (drops, dropped) = channel();
    let (started, live) = channel();
    let sibling = Capture("sibling", drops.clone());
    runtime
        .spawn_on(second, async move {
            let _capture = sibling;
            let _ = started.send(());
            pending::<()>().await;
        })
        .unwrap();
    let sibling_live = live.recv().is_ok();
    let root = Capture("root", drops.clone());
    let busy = Capture("busy", drops.clone());
    let (completed, completion) = channel();
    let admitted = runtime.spawn_on(first, async move {
        let _root = root;
        if hyper_rt::futures::spawn_detached(async move {
            let _busy = busy;
            BUSY_STARTED.store(true, Ordering::Release);
            loop {
                hyper_rt::futures::yield_now().await;
            }
        })
        .is_err()
        {
            let _ = completed.send("busy admission refused");
            return;
        }
        while !BUSY_STARTED.load(Ordering::Acquire) {
            hyper_rt::futures::yield_now().await;
        }
        let _ = hyper_rt::readiness::readable(0).await;
        let _ = completed.send("the root returned after driver loss");
    });
    // The two captures the failed shard owns must be dropped before an explicit shutdown: the test
    // waits for two drops, and any capture's counts, so the sibling's would show here too.
    let mut before_shutdown: Vec<_> = (0..2).map_while(|_| dropped.recv().ok()).collect();
    let actual_busy_loss = lost.try_iter().any(|nonblocking| nonblocking);
    let no_normal_return = completion.try_iter().next().is_none();
    let shutdown = runtime.shutdown();
    let after_shutdown: Vec<_> = dropped.try_iter().collect();
    before_shutdown.sort_unstable();
    assert!(
        sibling_live && admitted.is_ok(),
        "both public owners admitted their work"
    );
    assert!(
        actual_busy_loss,
        "the adapter returned DriverLost on a busy zero-timeout wait"
    );
    assert!(
        no_normal_return,
        "driver loss cancels the root instead of completing its await"
    );
    assert_eq!(
        before_shutdown,
        ["busy", "root"],
        "shutdown cannot hide failure to terminate the lost owner"
    );
    assert_eq!(after_shutdown, ["sibling"]);
    assert!(
        matches!(shutdown, Err(RtError::DriverLost)),
        "threaded termination reports its driver failure: {shutdown:?}"
    );
    let (losses, _) = channel();
    let healthy = Runtime::start_with(&config(), &mut || Ok(prepared(losses.clone()))).unwrap();
    let (answer, answered) = channel();
    healthy
        .spawn_on(healthy.shard_ids()[0], async move {
            let _ = answer.send(42);
        })
        .unwrap();
    let value = answered.recv();
    let retired = healthy.shutdown();
    assert_eq!(value, Ok(42));
    assert!(
        retired.is_ok(),
        "fresh healthy threaded owners stop cleanly: {retired:?}"
    );
}

#[test]
fn repeated_lost_and_healthy_local_owners_release_their_captured_work() {
    let config = config();
    // More owner histories than available registry slots detects a forgotten retirement;
    // each healthy owner also completes more roots than its task capacity detects a root leak.
    for _ in 0..hyper_rt::registry::MAX_SHARDS.saturating_add(1) {
        let (losses, lost) = channel();
        let (drops, dropped) = channel();
        let capture = Capture("failed root", drops);
        let mut failed = LocalRuntime::with_driver(&config, seed(losses), Kick::None).unwrap();
        let result = failed.block_on(async move {
            let _capture = capture;
            hyper_rt::readiness::readable(0).await
        });
        let returned_before_drop: Vec<_> = dropped.try_iter().collect();
        let actual_loss = lost.try_iter().next().is_some();
        drop(failed);
        assert!(
            actual_loss && matches!(result, Err(RtError::ShardGone { .. })),
            "actual driver loss: {result:?}"
        );
        assert_eq!(returned_before_drop, ["failed root"]);
        let (losses, _) = channel();
        let mut healthy = LocalRuntime::with_driver(&config, seed(losses), Kick::None).unwrap();
        for value in 0..config.tasks_per_shard.saturating_add(1) {
            assert_eq!(healthy.block_on(async move { value }), Ok(value));
        }
    }
}
