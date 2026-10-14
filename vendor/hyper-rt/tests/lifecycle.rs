//! AUD-29-12 (§4.3; banned item 9): a runtime owns its workers from start to a terminal state. A start
//! that fails part-way leaves no worker running and no registry slot claimed; a runtime dropped without
//! `shutdown` stops, cancels and joins its workers; a worker that fails is reported typed, never as
//! default counters; and a shard that failed is never advertised as started.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::string_slice,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::missing_panics_doc
)]
// Test harness code: an unwrap here is a failed test, which is what it should be.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_rt::RtError;
use hyper_rt::driver::{Clock, Completion, Driver, DriverKind, Kick};
use hyper_rt::interests::Readiness;
use hyper_rt::runtime::{Runtime, RuntimeConfig};

/// The tests of this binary read process-global state (which registry slots are free), so they run one at
/// a time (a test harness lock, D-8's stated exception).
#[allow(clippy::disallowed_types)] // a test harness lock (D-8's stated exception), poison recovered
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Shape: how long a shard may take to stop and drop its tasks.
const STOP_WAIT: Duration = Duration::from_secs(10);
/// Shape: shards per runtime: more than one, so a part-way failure has work to undo.
const SHARDS: u16 = 3;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: SHARDS,
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
        latency_objective_ns: None,
    }
}

/// A task that never completes and reports when it is dropped (its shard stopped and cancelled it).
struct Parked(Sender<()>);
impl Future for Parked {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}
impl Drop for Parked {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// AUD-29-12. Do: start a runtime, park a task on every shard, drop the runtime without `shutdown`.
/// Expect: every shard stops and drops its task (its worker was stopped and joined, not detached), and a
/// new runtime then gets the same registry slots (they were given back).
#[test]
fn a_runtime_dropped_without_shutdown_stops_and_joins_its_workers() {
    let _serial = serial();
    let runtime = Runtime::start(&config()).unwrap();
    let ids = runtime.shard_ids().to_vec();
    let (dropped, told) = channel();
    for id in &ids {
        runtime.spawn_on(*id, Parked(dropped.clone())).unwrap();
    }
    drop(dropped);
    drop(runtime);
    for _ in &ids {
        told.recv_timeout(STOP_WAIT)
            .expect("a dropped runtime stopped its shard and dropped the parked task");
    }
    let again = Runtime::start(&config()).unwrap();
    assert_eq!(again.shard_ids(), &ids[..], "the slots were given back");
    again.shutdown().unwrap();
}

/// Found by `tcp::serve`: a task that spawned a joinable task and ended left that task finished and never
/// joined, holding its slot, and shutdown waited for every slot to be free, so it never returned. Do: on
/// every shard, a task spawns a joinable task that ends at once, and neither is joined; then shut down on
/// another thread. Expect: the shutdown returns within the bound.
#[test]
fn a_finished_task_nobody_joins_does_not_hold_shutdown() {
    let _serial = serial();
    let runtime = Runtime::start(&config()).unwrap();
    let (spawned, told) = channel();
    for id in runtime.shard_ids() {
        let spawned = spawned.clone();
        runtime
            .spawn_on(*id, async move {
                let _ = spawned.send(hyper_rt::futures::spawn(async {}).is_ok());
            })
            .unwrap();
    }
    for _ in runtime.shard_ids() {
        assert!(
            told.recv_timeout(STOP_WAIT).unwrap(),
            "the joinable task spawned"
        );
    }
    let (stopped, stop) = channel();
    let stopper = std::thread::spawn(move || {
        let _ = stopped.send(runtime.shutdown().is_ok());
    });
    assert!(
        stop.recv_timeout(STOP_WAIT).expect("shutdown returned"),
        "shutdown succeeded"
    );
    stopper.join().unwrap();
}

/// The slots a runtime of this configuration gets when nothing else holds any: the baseline a failed start
/// must leave intact.
fn baseline_slots() -> Vec<hyper_rt::shard::ShardId> {
    let runtime = Runtime::start(&config()).unwrap();
    let ids = runtime.shard_ids().to_vec();
    runtime.shutdown().unwrap();
    ids
}

/// A start over `prepare` must be refused with `expected`, and a runtime started after it must get exactly
/// the baseline's slots: nothing the refused start claimed was kept.
fn refused_without_residue(
    prepare: &mut dyn FnMut() -> Result<hyper_rt::driver::Prepared, hyper_rt::error::RtError>,
    expected: &hyper_rt::error::RtError,
) {
    let baseline = baseline_slots();
    let refused = Runtime::start_with(&config(), prepare);
    assert_eq!(
        refused.as_ref().err(),
        Some(expected),
        "the start was refused with its cause"
    );
    let after = Runtime::start(&config()).unwrap();
    assert_eq!(
        after.shard_ids(),
        &baseline[..],
        "the refused start kept no slot"
    );
    after.shutdown().unwrap();
}

/// Format: the refusal the test's driver double answers with.
fn injected() -> hyper_rt::error::RtError {
    hyper_rt::error::RtError::DriverRefused {
        call: "the test's injected refusal",
        code: None,
    }
}

/// AUD-29-12. Do: start a runtime whose third shard's driver cannot be prepared. Expect: the start is
/// refused with that refusal and the two shards registered before it are given back.
#[test]
fn a_start_refused_while_preparing_a_driver_gives_every_slot_back() {
    let _serial = serial();
    let entries = u32::try_from(config().ring_entries).unwrap();
    let mut prepared = 0;
    let mut prepare = || {
        prepared += 1;
        if prepared == 3 {
            return Err(injected());
        }
        hyper_rt::driver::os_driver(entries)
    };
    refused_without_residue(&mut prepare, &injected());
}

/// AUD-29-12. Do: start a runtime whose last shard's context fails to build on its own thread (its driver
/// seed refuses there) while the first shards build and run. Expect: the start is refused with that
/// refusal — the failed shard is never advertised — and every worker is stopped and joined and every slot
/// given back.
#[test]
fn a_shard_whose_context_fails_to_build_is_never_advertised_and_its_siblings_stop() {
    let _serial = serial();
    let entries = u32::try_from(config().ring_entries).unwrap();
    let mut prepared = 0;
    let mut prepare = || {
        prepared += 1;
        let mut driver = hyper_rt::driver::os_driver(entries)?;
        if prepared == usize::from(SHARDS) {
            driver.seed = Box::new(|_kick| Err(injected()));
        }
        Ok(driver)
    };
    refused_without_residue(&mut prepare, &injected());
}

/// A task that reports it was polled, then panics.
struct Panics(Sender<()>);
impl Future for Panics {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let _ = self.0.send(());
        panic!("the test's injected task failure");
    }
}

/// Do: make a task on one shard panic in its poll, then run another task there and shut the runtime
/// down. Expect: the task unwound inside its shard's boundary, so the shard ran the next task, and
/// the shutdown reports every shard's counters, the unwind counted on that shard's alone, and gives
/// every slot back.
#[test]
fn a_task_that_panics_ends_unwound_and_its_shard_goes_on() {
    let _serial = serial();
    let baseline = baseline_slots();
    let runtime = Runtime::start(&config()).unwrap();
    let shard = runtime.shard_ids()[1];
    let (polled, told) = channel();
    runtime.spawn_on(shard, Panics(polled)).unwrap();
    told.recv_timeout(STOP_WAIT)
        .expect("the panicking task was polled");
    let (ran, heard) = channel();
    runtime
        .spawn_on(shard, async move {
            let _ = ran.send(());
        })
        .unwrap();
    heard
        .recv_timeout(STOP_WAIT)
        .expect("the shard ran a task after the one that panicked");
    let counters = runtime.shutdown().unwrap();
    let unwound: Vec<u64> = counters.iter().map(|shard| shard.unwound).collect();
    assert_eq!(unwound, [0, 1, 0], "{counters:?}");
    let after = Runtime::start(&config()).unwrap();
    assert_eq!(
        after.shard_ids(),
        &baseline[..],
        "every slot was given back"
    );
    after.shutdown().unwrap();
}

/// Whether [`PanicsOnWait`] panics at its next wait.
static ARMED: AtomicBool = AtomicBool::new(false);

/// A shard's OS driver whose wait panics once [`ARMED`] is set, telling the test first: a worker
/// that fails outside its tasks, in a caller's driver.
struct PanicsOnWait {
    driver: Box<dyn Driver>,
    failing: Sender<()>,
}

impl Driver for PanicsOnWait {
    fn kind(&self) -> DriverKind {
        self.driver.kind()
    }
    fn kick_handle(&self) -> Kick {
        self.driver.kick_handle()
    }
    fn now_ns(&self) -> u64 {
        self.driver.now_ns()
    }
    fn wait(&mut self, timeout_ns: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        if ARMED.load(Ordering::SeqCst) {
            let _ = self.failing.send(());
            panic!("the test's injected worker failure");
        }
        self.driver.wait(timeout_ns, out)
    }
    fn submit_nop(&mut self, user_data: u64) -> Result<(), RtError> {
        self.driver.submit_nop(user_data)
    }
    fn arm(&mut self, raw: i32, want: Readiness, tag: u64) -> Result<(), RtError> {
        self.driver.arm(raw, want, tag)
    }
    fn disarm(&mut self, raw: i32) {
        self.driver.disarm(raw);
    }
    fn reserve_handles(&mut self, handles: usize) -> Result<(), RtError> {
        self.driver.reserve_handles(handles)
    }
    fn has_pending(&self) -> bool {
        self.driver.has_pending()
    }
    fn clock(&self) -> Clock {
        self.driver.clock()
    }
    fn is_sim(&self) -> bool {
        self.driver.is_sim()
    }
}

/// AUD-29-12. Do: make one shard's worker panic outside its tasks (its driver's wait panics), then
/// shut the runtime down. Expect: `WorkerFailed` naming that shard — never default counters — and
/// every slot given back all the same.
#[test]
fn a_worker_that_panics_is_reported_typed_and_its_slot_is_given_back() {
    let _serial = serial();
    let baseline = baseline_slots();
    let entries = u32::try_from(config().ring_entries).unwrap();
    let (failing_told, failed) = channel();
    let mut prepared = 0;
    let mut prepare = || {
        prepared += 1;
        let mut driver = hyper_rt::driver::os_driver(entries)?;
        if prepared == 2 {
            let seed = driver.seed;
            let failing = failing_told.clone();
            driver.seed = Box::new(move |kick| {
                seed(kick)
                    .map(|driver| Box::new(PanicsOnWait { driver, failing }) as Box<dyn Driver>)
            });
        }
        Ok(driver)
    };
    let runtime = Runtime::start_with(&config(), &mut prepare).unwrap();
    let failing = runtime.shard_ids()[1];
    ARMED.store(true, Ordering::SeqCst);
    // A task wakes the shard from its wait, and its next wait panics.
    runtime.spawn_on(failing, async {}).unwrap();
    failed
        .recv_timeout(STOP_WAIT)
        .expect("the shard's driver panicked");
    ARMED.store(false, Ordering::SeqCst);
    assert_eq!(
        runtime.shutdown(),
        Err(RtError::WorkerFailed { shard: failing.0 })
    );
    let after = Runtime::start(&config()).unwrap();
    assert_eq!(
        after.shard_ids(),
        &baseline[..],
        "every slot was given back"
    );
    after.shutdown().unwrap();
}
