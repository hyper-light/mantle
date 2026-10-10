//! Warm service cancellation and completion resource oracle, not a throughput benchmark.
//! Run this integration-test binary alone, with one test thread and an external deadline.
//! The observer's registry reads and yield calls are included in whole-process costs.
#![allow(
    clippy::cognitive_complexity,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, pending, poll_fn};
use std::hint::black_box;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::task::Poll;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use hyper_measure::alloc::{self, Counting};
use hyper_measure::{faults, usage};
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::futures;
use hyper_rt::interests::Readiness;
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::sync::{self, ChannelReceiver, SyncError};
use hyper_rt::task::Outcome;
use hyper_rt::{RtError, TaskId};

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Existing public sync drop-race stress budget, tests/sync.rs DROP_RACE_ROUNDS.
const REUSE_ROUNDS: usize = 3_000;
/// Existing context/reclamation stress budget, tests/sim_memory.rs CYCLES.
const LOSS_ROUNDS: usize = 32;
/// The first lifecycle warms admission/cleanup; the second warms stale-tag/slot reuse.
const REUSE_WARMUP: usize = 2;
/// A first complete loss lifecycle warms the distinct fatal/driverless completion code.
const LOSS_WARMUP: usize = 1;
/// One controller and exactly one reusable service slot.
const REUSE_ROLES: usize = 2;
/// One controller, one service, and one fault-trigger task.
const LOSS_ROLES: usize = 3;
/// Adapter-only identity; never passed to an OS driver.
const HANDLE: i32 = 0;
/// A controller-abort wake is distinguishable from every tested ordinal.
const ABORT: u64 = u64::MAX;

static STOP_OBSERVER: AtomicBool = AtomicBool::new(false);
static RETURNED: AtomicU64 = AtomicU64::new(0);
static AFTER_LOSS_CALLS: AtomicU64 = AtomicU64::new(0);

fn config(roles: usize) -> RuntimeConfig {
    // These fixed timing/page values are the existing cooperative_service fixture's
    // complete configuration. They decide no result, watchdog or retry count.
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

#[derive(Clone, Copy, Debug, Default)]
struct Costs {
    counts: alloc::Counts,
    faults: faults::Faults,
    cpu: Option<usage::Usage>,
    wall_ns: u128,
}

struct Mark {
    counts: alloc::Counts,
    faults: faults::Faults,
    cpu: Option<usage::Usage>,
    start: Instant,
}

fn mark() -> Mark {
    // OS probes precede this window's allocator snapshot. Counting continues across
    // every phase, so their allocations still appear in the full-process total.
    let faults = faults::read().unwrap();
    let cpu = usage::this().ok();
    let counts = alloc::read_process();
    Mark {
        counts,
        faults,
        cpu,
        start: Instant::now(),
    }
}

fn close(mark: Mark) -> Costs {
    let wall_ns = mark.start.elapsed().as_nanos();
    let counts = alloc::read_process().less(&mark.counts);
    let faults = faults::read().unwrap().since(&mark.faults);
    let cpu = usage::this()
        .ok()
        .zip(mark.cpu)
        .map(|(now, then)| now.since(&then));
    Costs {
        counts,
        faults,
        cpu,
        wall_ns,
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Row {
    setup: Costs,
    hot: Costs,
    retirement: Costs,
}

struct Returned;
impl Drop for Returned {
    fn drop(&mut self) {
        RETURNED.fetch_add(1, Ordering::SeqCst);
    }
}

async fn first_pending<F: Future>(mut future: Pin<&mut F>, fact: &sync::Sender<()>) -> F::Output {
    let mut noted = false;
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Ready(result) => Poll::Ready(result),
        Poll::Pending => {
            if !noted {
                fact.try_send(()).unwrap();
                noted = true;
            }
            Poll::Pending
        }
    })
    .await
}

#[derive(Clone, Copy)]
enum Wake {
    Parked {
        shard: u16,
        waits_before: u64,
        value: u64,
    },
    CleanupParked {
        shard: u16,
        value: u64,
    },
    Stop,
}

fn waits(shard: u16) -> u64 {
    registry::with_entry(shard, |entry| entry.pulse.waits()).unwrap()
}

/// This plain thread only observes actual public parked state. It publishes an
/// exact channel completion once that state is reached; no sleep or timer can pass.
fn observer(receiver: std::sync::mpsc::Receiver<Wake>, release: sync::Sender<u64>) {
    while let Ok(command) = receiver.recv() {
        let (shard, waits_before, value) = match command {
            Wake::Stop => break,
            Wake::Parked {
                shard,
                waits_before,
                value,
            } => (shard, Some(waits_before), value),
            Wake::CleanupParked { shard, value } => (shard, None, value),
        };
        while !STOP_OBSERVER.load(Ordering::Acquire) {
            let parked = registry::with_entry(shard, |entry| {
                waits_before.is_none_or(|before| entry.pulse.waits() > before)
                    && entry.parking.parked()
            })
            .unwrap_or(false);
            if parked {
                release.try_send(value).unwrap();
                break;
            }
            thread::yield_now();
        }
        if STOP_OBSERVER.load(Ordering::Acquire) {
            break;
        }
    }
    // Failure cleanup can wake a retained receiver, but ABORT cannot satisfy an
    // exact ordinal oracle. try_send avoids a second blocked cleanup owner.
    let _ = release.try_send(ABORT);
}

struct Observer {
    commands: SyncSender<Wake>,
    thread: Option<JoinHandle<()>>,
}
impl Drop for Observer {
    fn drop(&mut self) {
        STOP_OBSERVER.store(true, Ordering::Release);
        let _ = self.commands.try_send(Wake::Stop);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn start_observer(release: sync::Sender<u64>) -> (Observer, SyncSender<Wake>) {
    STOP_OBSERVER.store(false, Ordering::Release);
    let (commands, receiver) = sync_channel(1);
    let thread = thread::spawn(move || observer(receiver, release));
    (
        Observer {
            commands: commands.clone(),
            thread: Some(thread),
        },
        commands,
    )
}

struct Bundle {
    receiver: ChannelReceiver<u64>,
    observed: Result<u64, SyncError<()>>,
    cancelled: Result<(), RtError>,
}

fn stale(id: TaskId) -> RtError {
    RtError::StaleTask {
        slot: id.0.slot(),
        generation: id.0.generation(),
    }
}

fn reusable_native_services() -> (Costs, Vec<Row>, Costs) {
    let boot = mark();
    let cfg = config(REUSE_ROLES);
    let mut rt = LocalRuntime::new(&cfg).unwrap();
    let shard = rt.shard_id().0;
    let (release, receiver) = sync::channel(1).unwrap();
    let (first_fact, mut first) = sync::channel(1).unwrap();
    let (closing_fact, mut closing) = sync::channel(1).unwrap();
    let (back, mut returned) = sync::channel(1).unwrap();
    let mut rows = vec![Row::default(); REUSE_ROUNDS];
    black_box(&mut rows); // Writes above are observable before hot windows.
    // Declared after runtime: its failure Drop wakes receivers before runtime Drop.
    let (guard, commands) = start_observer(release);
    let boot = close(boot);
    let rows = rt
        .block_on(async move {
            // This root and its ThreadCtx are live before every measured lifecycle.
            let mut receiver = Some(receiver);
            let mut previous: Option<TaskId> = None;
            for ordinal in 0..(REUSE_WARMUP + REUSE_ROUNDS) {
                let setup = mark();
                let pending_fact = first_fact.clone();
                let closing_fact = closing_fact.clone();
                let back = back.clone();
                let mut retirement = receiver.take().unwrap();
                let before_returned = RETURNED.load(Ordering::SeqCst);
                let capture = Returned;
                let id = futures::spawn_service(async move {
                    let _capture = capture;
                    let mut cancel = pin!(futures::cancelled());
                    let cancelled = first_pending(cancel.as_mut(), &pending_fact).await;
                    let observed = {
                        let mut receive = pin!(retirement.recv());
                        first_pending(receive.as_mut(), &closing_fact).await
                    };
                    // This move returns the same receiver to its sole controller for reuse.
                    // Failure of the controller must still drop it and return the capture.
                    let _ = back.try_send(Bundle {
                        receiver: retirement,
                        observed,
                        cancelled,
                    });
                })
                .unwrap();
                first.recv().await.unwrap(); // Genuine cancel Pending, future/arena already touched.
                let setup = close(setup);
                let hot = mark();
                if let Some(old) = previous {
                    assert_eq!(id.0.slot(), old.0.slot(), "only one service slot exists");
                    assert_ne!(id.0.generation(), old.0.generation());
                    assert_eq!(futures::cancel(old), Err(stale(old)));
                }
                futures::cancel(id).unwrap();
                futures::cancel(id).unwrap(); // Idempotent repeated level before service repoll.
                closing.recv().await.unwrap(); // Genuine retained completion Pending.
                assert_eq!(RETURNED.load(Ordering::SeqCst), before_returned);
                futures::cancel(id).unwrap(); // Repeated cancel during cleanup cannot force Drop.
                let value = u64::try_from(ordinal).unwrap();
                commands
                    .try_send(Wake::Parked {
                        shard,
                        waits_before: waits(shard),
                        value,
                    })
                    .unwrap();
                let bundle = returned.recv().await.unwrap();
                assert_eq!(bundle.cancelled, Ok(()));
                assert_eq!(bundle.observed, Ok(value));
                receiver = Some(bundle.receiver);
                assert_eq!(futures::join(id).await, Ok(Outcome::Cancelled));
                futures::yield_now().await; // Applies the accepted JOINED retirement before reuse.
                assert_eq!(RETURNED.load(Ordering::SeqCst), before_returned + 1);
                assert_eq!(futures::cancel(id), Err(stale(id)));
                let hot = close(hot);
                if ordinal >= REUSE_WARMUP {
                    rows[ordinal - REUSE_WARMUP] = Row {
                        setup,
                        hot,
                        retirement: Costs::default(),
                    };
                }
                previous = Some(id);
            }
            rows
        })
        .unwrap();
    let retirement = mark();
    drop(guard);
    drop(rt);
    (boot, rows, close(retirement))
}

struct Loss {
    lost: bool,
}

#[derive(Debug)]
struct LossObservation {
    cancelled: Result<(), RtError>,
    ready: Result<(), RtError>,
    timer: Result<(), RtError>,
    value: Result<u64, SyncError<()>>,
}
impl Loss {
    fn record(&self) {
        if self.lost {
            AFTER_LOSS_CALLS.fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl Driver for Loss {
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

fn one_loss(ordinal: u64) -> Row {
    let setup = mark();
    let cfg = config(LOSS_ROLES);
    let seed: DriverSeed = Box::new(|_| Ok(Box::new(Loss { lost: false })));
    let mut rt = LocalRuntime::with_driver(&cfg, seed, Kick::None).unwrap();
    let shard = rt.shard_id().0;
    let (release, mut retirement) = sync::channel(1).unwrap();
    let (pending_fact, mut first) = sync::channel(1).unwrap();
    let (go, mut start_loss) = sync::channel(1).unwrap();
    // One cold result slot; hot Mark/costs are moved values, not new allocations.
    let (started, timing) = sync_channel(1);
    let (observed, observation) = sync_channel(1);
    let before_returned = RETURNED.load(Ordering::SeqCst);
    let (guard, commands) = start_observer(release);
    let service = rt
        .spawn_service(async move {
            let _capture = Returned;
            let mut cancel = pin!(futures::cancelled());
            let cancelled = first_pending(cancel.as_mut(), &pending_fact).await;
            let ready = hyper_rt::readiness::readable(HANDLE).await;
            let timer = futures::sleep(config(LOSS_ROLES).timer_tick_ns).await;
            let mut receive = pin!(retirement.recv());
            let mut sent = false;
            let value = poll_fn(|cx| match receive.as_mut().poll(cx) {
                Poll::Ready(value) => Poll::Ready(value),
                Poll::Pending => {
                    if !sent {
                        // This same owning poll cannot still be announcing a park.
                        // std command publication follows this false fact, so a later
                        // true observation is fresh; Pulse counts only native waits.
                        assert_eq!(
                            registry::with_entry(shard, |entry| entry.parking.parked()),
                            Some(false)
                        );
                        commands
                            .try_send(Wake::CleanupParked {
                                shard,
                                value: ordinal,
                            })
                            .unwrap();
                        sent = true;
                    }
                    Poll::Pending
                }
            })
            .await;
            // ABORT wakes cannot pass an exact completion. The capture is returned even
            // during failure cleanup, before any eventual verdict is considered.
            let _ = observed.try_send(LossObservation {
                cancelled,
                ready,
                timer,
                value,
            });
        })
        .unwrap();
    rt.context().detach(service).unwrap();
    let trigger = rt
        .spawn(async move {
            start_loss.recv().await.unwrap();
            let _ = hyper_rt::readiness::readable(HANDLE).await;
        })
        .unwrap();
    rt.context().detach(trigger).unwrap();
    let outcome = rt.block_on(async move {
        first.recv().await.unwrap();
        let setup = close(setup); // First ThreadCtx/actual service poll are cold setup.
        let hot = mark();
        started.try_send((setup, hot)).unwrap();
        go.try_send(()).unwrap();
        pending::<()>().await;
    });
    let (setup, hot) = timing
        .try_recv()
        .expect("fault was triggered after actual Pending");
    let hot = close(hot);
    assert_eq!(outcome, Err(RtError::ShardGone { shard }));
    assert!(rt.exited());
    assert_eq!(rt.live_tasks(), 0);
    assert_eq!(RETURNED.load(Ordering::SeqCst), before_returned + 1);
    let retirement = mark();
    drop(guard);
    drop(rt);
    let retirement = close(retirement);
    let observed = observation.try_recv().unwrap();
    assert_eq!(observed.cancelled, Ok(()));
    assert_eq!(observed.ready, Err(RtError::DriverLost));
    assert_eq!(observed.timer, Err(RtError::DriverLost));
    assert_eq!(observed.value, Ok(ordinal));
    assert_eq!(AFTER_LOSS_CALLS.load(Ordering::Relaxed), 0);
    Row {
        setup,
        hot,
        retirement,
    }
}

fn summary(label: &str, phase: &str, rows: &[Row], select: impl Fn(&Row) -> Costs) {
    let mut allocations = 0;
    let mut reallocations = 0;
    let mut bytes = 0;
    let mut minor = 0;
    let mut major = Some(0u64);
    let mut wall_ns = 0;
    let mut cpu_ns = Some(0u64);
    let mut worst: Option<(usize, Costs)> = None;
    for (round, row) in rows.iter().enumerate() {
        let cost = select(row);
        allocations += cost.counts.allocations;
        reallocations += cost.counts.reallocations;
        bytes += cost.counts.bytes;
        minor += cost.faults.minor;
        major = major.zip(cost.faults.major).map(|(sum, value)| sum + value);
        wall_ns += cost.wall_ns;
        cpu_ns = cpu_ns
            .zip(cost.cpu)
            .map(|(sum, value)| sum + value.cpu_ns());
        if worst.is_none_or(|(_, old)| cost.wall_ns > old.wall_ns) {
            worst = Some((round, cost));
        }
        if phase == "hot"
            && (cost.counts.calls() != 0
                || cost.counts.bytes != 0
                || cost.faults.total() != 0
                || cost.faults.task.is_some_and(|task| {
                    task.faults != 0 || task.pageins != 0 || task.cow_faults != 0
                }))
        {
            eprintln!(
                "service_resource nonzero label={label} round={round} phase={phase} costs={cost:?}"
            );
        }
    }
    eprintln!(
        "service_resource label={label} phase={phase} rounds={} allocations={allocations} reallocations={reallocations} bytes={bytes} minor={minor} major={major:?} cpu_ns={cpu_ns:?} wall_ns={wall_ns} worst={worst:?}",
        rows.len()
    );
}

#[test]
fn warm_cancellation_channel_wake_reuse_and_driver_loss_have_measured_resources() {
    // Process-wide scope must run alone. Cold instrumentation first invokes every
    // OS probe and the counter TLS; none is a hidden first hot ThreadCtx setup.
    alloc::begin_process();
    let floor = mark();
    black_box(());
    let floor = close(floor);
    let whole = mark();
    let (boot, reuse, reuse_retirement) = reusable_native_services();
    let mut loss = vec![Row::default(); LOSS_ROUNDS];
    black_box(&mut loss);
    for ordinal in 0..(LOSS_WARMUP + LOSS_ROUNDS) {
        let row = one_loss(u64::try_from(ordinal).unwrap());
        if ordinal >= LOSS_WARMUP {
            loss[ordinal - LOSS_WARMUP] = row;
        }
    }
    let whole = close(whole);
    let total = alloc::end_process();
    eprintln!(
        "service_resource full_config_reuse={:?} full_config_loss={:?}",
        config(REUSE_ROLES),
        config(LOSS_ROLES)
    );
    eprintln!(
        "service_resource floor={floor:?} boot={boot:?} reuse_retirement={reuse_retirement:?} whole={whole:?} continuous_counts={total:?}"
    );
    for (label, rows) in [
        ("native-reuse", reuse.as_slice()),
        ("driver-loss", loss.as_slice()),
    ] {
        summary(label, "setup", rows, |row| row.setup);
        summary(label, "hot", rows, |row| row.hot);
    }
    // Native service retirement is included in its hot window. Only driver loss
    // has a separate per-owner teardown row; never print absent work as zero.
    summary("driver-loss", "owner-retirement", &loss, |row| {
        row.retirement
    });
    // No nonzero outcome is discarded or silently retried. Fault probes also have
    // their floor above; counts establish an observed bound, not universal OS immunity.
    for row in reuse.iter().chain(&loss) {
        assert_eq!(row.hot.counts.allocations, 0);
        assert_eq!(row.hot.counts.reallocations, 0);
        assert_eq!(row.hot.counts.bytes, 0);
        assert_eq!(row.hot.faults.minor, 0);
        if let Some(major) = row.hot.faults.major {
            assert_eq!(major, 0);
        } // Windows' unavailable split remains None in output, never fabricated zero.
        if let Some(task) = row.hot.faults.task {
            assert_eq!(task.faults, 0);
            assert_eq!(task.pageins, 0);
            assert_eq!(task.cow_faults, 0);
        }
    }
}
