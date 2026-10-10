//! Supplemental resource decomposition, independent of service_resources.rs's
//! unchanged strict lifecycle RED. Measures 3,000 repeated receipts in one lost
//! owner's retained service, then reports setup/transition/teardown and row reporting
//! through the final reporting stamp. The last summary/assertions/harness tail are
//! outside Counting and explicitly excluded, not declared allocation-free.
//! Requires an external failure-only native supervisor: a broken wake protocol may
//! stall before an assertion; no timeout or cleanup release can count as success.
//! Rust GlobalAlloc counters are not a census of libc/System/native heap calls.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::missing_panics_doc
)]

use hyper_measure::alloc::{self, Counting, Counts};
use hyper_measure::{faults, usage};
use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick, Prepared};
use hyper_rt::futures;
use hyper_rt::interests::Readiness;
use hyper_rt::registry;
use hyper_rt::runtime::{Runtime, RuntimeConfig, interests_for};
use hyper_rt::sync;
use hyper_rt::task::{Admission, Outcome};
use std::future::{Future, poll_fn};
use std::hint::black_box;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;
use std::task::Poll;
use std::thread::{self, Thread};
use std::time::Instant;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Existing sync.rs DROP_RACE_ROUNDS; no new tuning or polling cadence.
const ROUNDS: usize = 3_000;
/// One complete first driverless Pending/park/completion, shown as cold warmup.
/// This is the existing service_resources LOSS_WARMUP's one distinct path.
const WARMUP: usize = 1;
/// Actual messages: one warmup plus the measured receipt stream.
const TOTAL: usize = WARMUP + ROUNDS;
/// One retained service parent plus its joinable ordinary fault-trigger child.
const ROLES: usize = 2;
/// Adapter-only identity, never handed to a native socket driver.
const HANDLE: i32 = 0;
/// Initial empty mailbox; receipts are one-based, so it is not a message.
const EMPTY: u64 = 0;
/// One final command after every real receipt, derived from the input count.
const DONE: u64 = (TOTAL as u64) + 1;

// One publisher and one observer, with only one outstanding receipt. A command
// must be taken before its completion is published; the next receive cannot
// become Pending until the previous value was consumed. CAS detects a violation.
static COMMAND: AtomicU64 = AtomicU64::new(EMPTY);
static MAILBOX_REFUSED: AtomicBool = AtomicBool::new(false);
static AFTER_LOSS: AtomicUsize = AtomicUsize::new(0);
static CHILD_RETURNED: AtomicUsize = AtomicUsize::new(0);
static SERVICE_RETURNED: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug)]
struct Stamp {
    at: Instant,
    counts: Counts,
    faults: faults::Faults,
    cpu: Option<usage::Usage>,
}

fn stamp() -> Stamp {
    // Shared snapshots are reused at adjacent boundaries. Probe effects remain
    // in continuous counts and an explicit probe floor, never silently removed.
    let at = Instant::now();
    let faults = faults::read().unwrap();
    let cpu = usage::this().ok();
    let counts = alloc::read_process();
    Stamp {
        at,
        counts,
        faults,
        cpu,
    }
}

#[derive(Clone, Copy, Default)]
struct Window {
    start_ns: u128,
    end_ns: u128,
    counts: Counts,
    faults: faults::Faults,
    cpu: Option<usage::Usage>,
}

impl std::fmt::Debug for Window {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Window")
            .field("start_ns", &self.start_ns)
            .field("end_ns", &self.end_ns)
            .field("counts", &self.counts)
            .field("faults", &self.faults)
            .field("cpu", &self.cpu)
            .finish()
    }
}

fn window(epoch: Instant, from: Stamp, to: Stamp) -> Window {
    Window {
        start_ns: from.at.duration_since(epoch).as_nanos(),
        end_ns: to.at.duration_since(epoch).as_nanos(),
        counts: to.counts.less(&from.counts),
        faults: to.faults.since(&from.faults),
        cpu: to.cpu.zip(from.cpu).map(|(now, then)| now.since(&then)),
    }
}

// Counts::less deliberately leaves live/peak bytes out: those are only meaningful
// in the continuous account. These five event fields can be added exactly in a
// u128 even if every u64 field reaches its representational maximum in each of
// the input-derived windows. This is accounting, not an allocation allowance.
fn events(counts: Counts) -> [u128; 5] {
    [
        u128::from(counts.allocations),
        u128::from(counts.reallocations),
        u128::from(counts.moved),
        u128::from(counts.frees),
        u128::from(counts.bytes),
    ]
}

fn add_events(sum: &mut [u128; 5], counts: Counts) {
    for (sum, value) in sum.iter_mut().zip(events(counts)) {
        *sum += value;
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Row {
    gap: Window,
    receipt: Window,
}

#[derive(Clone, Copy, Debug, Default)]
struct ObserverRow {
    idle: Window,
    observe_publish: Window,
}

#[derive(Debug)]
struct Report {
    transition: Window,
    work_end: Stamp,
    terminal: Window,
    rows: Vec<Row>,
    completed: usize,
    cancelled: bool,
    child_joined: bool,
    child_returned_before_work: bool,
    exact_values: bool,
    final_gate: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Setup {
    ServicePending,
    ChildPending,
}

struct Returned(&'static AtomicUsize);
impl Drop for Returned {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
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
    fn submit_nop(&mut self, _: u64) -> Result<(), RtError> {
        self.record();
        Ok(())
    }
    fn arm(&mut self, _: i32, _: Readiness, _: u64) -> Result<(), RtError> {
        self.record();
        self.0 = true;
        Err(RtError::DriverLost)
    }
    fn disarm(&mut self, _: i32) {
        self.record();
    }
}

fn config() -> RuntimeConfig {
    // Complete existing cooperative_service configuration with exactly two roles.
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

fn publish(command: u64, observer: &Thread) {
    if COMMAND
        .compare_exchange(EMPTY, command, Ordering::Release, Ordering::Relaxed)
        .is_err()
    {
        MAILBOX_REFUSED.store(true, Ordering::Release);
    }
    // A retained permit closes take-empty -> park. Payload is always published first.
    observer.unpark();
}

fn command() -> u64 {
    loop {
        let command = COMMAND.swap(EMPTY, Ordering::AcqRel);
        if command != EMPTY {
            return command;
        }
        // Not a polling cadence: native park can return spuriously, and a permit
        // published between swap and park remains available to that same thread.
        thread::park();
    }
}

// Declared before any potential assertion can unwind with an admitted service.
// Closure wakes the retained waits with a distinguishable error, then Runtime
// owns and joins cancellation cleanup. No detached native owner is left behind.
struct Owned {
    runtime: Option<Runtime>,
    go: Option<sync::Sender<()>>,
    marks: Option<sync::Sender<Stamp>>,
    values: Option<sync::Sender<u64>>,
    finish: Option<sync::Sender<()>>,
}
impl Owned {
    fn close_inputs(&mut self) {
        drop(self.go.take());
        drop(self.marks.take());
        drop(self.values.take());
        drop(self.finish.take());
    }
}
impl Drop for Owned {
    fn drop(&mut self) {
        self.close_inputs();
        drop(self.runtime.take());
    }
}

#[test]
fn repeated_driverless_receipts_report_work_and_every_lifecycle_cost_separately() {
    COMMAND.store(EMPTY, Ordering::Relaxed);
    MAILBOX_REFUSED.store(false, Ordering::Relaxed);
    AFTER_LOSS.store(0, Ordering::Relaxed);
    CHILD_RETURNED.store(0, Ordering::Relaxed);
    SERVICE_RETURNED.store(0, Ordering::Relaxed);
    alloc::begin_process();
    let start = stamp();
    let epoch = start.at;
    let floor_end = stamp();
    let floor = window(epoch, start, floor_end);
    let observer = thread::current(); // System/Parker first use is cold, still paid.
    let mut rows = vec![Row::default(); TOTAL];
    let mut observer_rows = vec![ObserverRow::default(); TOTAL];
    black_box(&mut rows);
    black_box(&mut observer_rows);
    let (facts, mut setup) = sync::channel(ROLES).unwrap();
    let (go, mut trigger) = sync::channel(1).unwrap();
    let (marks, mut transition_mark) = sync::channel(1).unwrap();
    let (values, mut receiver) = sync::channel(1).unwrap();
    let (finish, mut terminal_gate) = sync::channel(1).unwrap();
    let (reported, reports) = sync_channel(1);
    let mut prepare = || {
        Ok(Prepared {
            seed: Box::new(|_| Ok(Box::new(Lost(false)))),
            kick_fd: None,
            notes: Vec::new(),
        })
    };
    let runtime = Runtime::start_with(&config(), &mut prepare).unwrap();
    let shard = *runtime.shard_ids().first().unwrap();
    let mut owned = Owned {
        runtime: Some(runtime),
        go: Some(go),
        marks: Some(marks),
        values: Some(values),
        finish: Some(finish),
    };
    let receipt = owned
        .runtime
        .as_ref()
        .unwrap()
        .spawn_service_on_with_receipt(shard, async move {
            let _capture = Returned(&SERVICE_RETURNED);
            let child_facts = facts.clone();
            let child = futures::spawn_child(async move {
                let _capture = Returned(&CHILD_RETURNED);
                let mut go = pin!(trigger.recv());
                let mut noted = false;
                let begin = poll_fn(|cx| match go.as_mut().poll(cx) {
                    Poll::Ready(value) => Poll::Ready(value),
                    Poll::Pending => {
                        if !noted {
                            child_facts.try_send(Setup::ChildPending).unwrap();
                            noted = true;
                        }
                        Poll::Pending
                    }
                })
                .await;
                if begin.is_ok() {
                    let _ = hyper_rt::readiness::readable(HANDLE).await;
                }
            })
            .unwrap();
            let mut cancellation = pin!(futures::cancelled());
            let mut noted = false;
            let cancelled = poll_fn(|cx| match cancellation.as_mut().poll(cx) {
                Poll::Ready(value) => Poll::Ready(value),
                Poll::Pending => {
                    if !noted {
                        facts.try_send(Setup::ServicePending).unwrap();
                        noted = true;
                    }
                    Poll::Pending
                }
            })
            .await
            .is_ok();
            let child_joined = futures::join(child).await == Ok(Outcome::Cancelled);
            futures::yield_now().await; // Accepted JOINED application belongs to transition.
            let child_returned_before_work = CHILD_RETURNED.load(Ordering::SeqCst) == 1;
            let Ok(transition_start) = transition_mark.recv().await else {
                // A setup/controller failure closes all inputs before joining
                // this owner. It cannot strand cleanup at an unowned mark gate
                // or satisfy the report/ordinal verdict below.
                publish(DONE, &observer);
                return;
            };
            let mut boundary = stamp();
            let transition = window(epoch, transition_start, boundary);
            let mut completed = 0;
            let mut exact_values = true;
            for (index, row) in rows.iter_mut().enumerate() {
                let hot_start = stamp();
                let gap = window(epoch, boundary, hot_start);
                let value = {
                    let mut receive = pin!(receiver.recv());
                    let mut noted = false;
                    poll_fn(|cx| match receive.as_mut().poll(cx) {
                        Poll::Ready(value) => Poll::Ready(value),
                        Poll::Pending => {
                            if !noted {
                                assert_eq!(
                                    registry::with_entry(shard.0, |entry| entry.parking.parked()),
                                    Some(false)
                                );
                                publish(u64::try_from(index + 1).unwrap(), &observer);
                                noted = true;
                            }
                            Poll::Pending
                        }
                    })
                    .await
                }; // Borrowed Recv registration retires inside the receipt work.
                let expected = u64::try_from(index + 1).unwrap();
                exact_values &= value == Ok(expected);
                let hot_end = stamp();
                *row = Row {
                    gap,
                    receipt: window(epoch, hot_start, hot_end),
                };
                completed += 1;
                boundary = hot_end;
                if value.is_err() {
                    break; // Failure closes ownership; incomplete stream cannot pass.
                }
            }
            let work_end = boundary;
            publish(DONE, &observer);
            let final_gate = terminal_gate.recv().await.is_ok();
            let terminal_end = stamp();
            let report = Report {
                transition,
                work_end,
                terminal: window(epoch, work_end, terminal_end),
                rows,
                completed,
                cancelled,
                child_joined,
                child_returned_before_work,
                exact_values,
                final_gate,
            };
            let _ = reported.try_send(report);
            // Endpoint/task destruction is after every measured receipt end.
        })
        .unwrap();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    let first = setup.blocking_recv().unwrap();
    let second = setup.blocking_recv().unwrap();
    assert_ne!(first, second, "both configured roles were actually Pending");
    let boot_end = stamp();
    let boot = window(epoch, floor_end, boot_end);
    owned.marks.as_ref().unwrap().try_send(boot_end).unwrap();
    owned.go.as_ref().unwrap().try_send(()).unwrap();
    let mut observer_boundary = boot_end;
    let mut observed = 0;
    loop {
        let next = command();
        let taken = stamp();
        if next == DONE {
            break;
        }
        assert_eq!(next, u64::try_from(observed + 1).unwrap());
        while !registry::with_entry(shard.0, |entry| entry.parking.parked()).unwrap_or(false) {
            thread::yield_now();
        }
        assert_eq!(SERVICE_RETURNED.load(Ordering::SeqCst), 0);
        owned.values.as_ref().unwrap().try_send(next).unwrap();
        let published = stamp();
        observer_rows[observed] = ObserverRow {
            idle: window(epoch, observer_boundary, taken),
            observe_publish: window(epoch, taken, published),
        };
        observed += 1;
        observer_boundary = published;
    }
    let terminal_start = stamp();
    let observer_final_gap = window(epoch, observer_boundary, terminal_start);
    owned.finish.as_ref().unwrap().try_send(()).unwrap();
    let report = reports.recv().unwrap();
    owned.close_inputs();
    let shutdown = owned.runtime.take().unwrap().shutdown();
    drop(owned);
    drop(setup);
    drop(receipt);
    drop(reports);
    let retired = stamp();
    let observer_terminal = window(epoch, terminal_start, retired);
    let owner_post_work = window(epoch, report.work_end, retired);
    let behavioral = report.cancelled
        && report.child_joined
        && report.child_returned_before_work
        && report.exact_values
        && report.final_gate
        && report.completed == TOTAL
        && observed == TOTAL
        && CHILD_RETURNED.load(Ordering::SeqCst) == 1
        && SERVICE_RETURNED.load(Ordering::SeqCst) == 1
        && !MAILBOX_REFUSED.load(Ordering::Acquire)
        && AFTER_LOSS.load(Ordering::SeqCst) == 0
        && matches!(shutdown, Err(RtError::DriverLost))
        && registry::holder_of(shard.0).is_none();
    let mut zero_receipt_allocations = true;
    let mut owner_partition = events(floor.counts);
    add_events(&mut owner_partition, boot.counts);
    add_events(&mut owner_partition, report.transition.counts);
    for row in report.rows.iter().take(report.completed) {
        add_events(&mut owner_partition, row.gap.counts);
        add_events(&mut owner_partition, row.receipt.counts);
    }
    add_events(&mut owner_partition, owner_post_work.counts);
    let mut observer_partition = events(observer_final_gap.counts);
    add_events(&mut observer_partition, observer_terminal.counts);
    for row in observer_rows.iter().take(observed) {
        add_events(&mut observer_partition, row.idle.counts);
        add_events(&mut observer_partition, row.observe_publish.counts);
    }
    let observer_account = window(epoch, boot_end, retired);
    let observer_reconciled = observer_partition == events(observer_account.counts);
    // Reporting and measurement-storage retirement stay inside continuous counts,
    // but outside runtime work. Overlapping observer/process windows are NOT summed.
    let reporting_start = retired;
    eprintln!(
        "driverless_resource config={:?} warmup={WARMUP} measured={ROUNDS} actual={observed}",
        config()
    );
    eprintln!(
        "driverless_resource floor={floor:?} boot={boot:?} transition={:?}",
        report.transition
    );
    for (index, row) in report.rows.iter().take(report.completed).enumerate() {
        let phase = if index < WARMUP { "warmup" } else { "receipt" };
        eprintln!(
            "driverless_resource phase={phase} ordinal={} owner_process={row:?} observer_process={:?}",
            index + 1,
            observer_rows[index]
        );
        if index >= WARMUP {
            zero_receipt_allocations &= row.receipt.counts.allocations == 0
                && row.receipt.counts.reallocations == 0
                && row.receipt.counts.bytes == 0;
        }
    }
    eprintln!(
        "driverless_resource owner_terminal_to_report={:?} owner_post_work={owner_post_work:?} observer_final_gap={observer_final_gap:?} observer_terminal={observer_terminal:?}",
        report.terminal
    );
    eprintln!(
        "driverless_resource observer_account={observer_account:?} observer_partition={observer_partition:?} observer_reconciled={observer_reconciled}"
    );
    eprintln!(
        "driverless_resource scope=concurrent-process-windows-overlap-do-not-sum RustGlobalAlloc-only native-System-unmeasured fault-values-observed-not-universal"
    );
    drop(report);
    drop(observer_rows);
    let reported = stamp();
    let reporting = window(epoch, reporting_start, reported);
    let whole = window(epoch, start, reported);
    add_events(&mut owner_partition, reporting.counts);
    let owner_reconciled = owner_partition == events(whole.counts);
    let total = alloc::end_process();
    let mut continuous_partition = events(start.counts);
    add_events(&mut continuous_partition, whole.counts);
    let continuous_reconciled = continuous_partition == events(total);
    eprintln!(
        "driverless_resource prefix_counts={:?} whole={whole:?} reporting={reporting:?} continuous_counts={total:?} owner_partition={owner_partition:?} owner_reconciled={owner_reconciled} continuous_reconciled={continuous_reconciled} behavioral={behavioral} zero_receipt_rust_alloc={zero_receipt_allocations}",
        start.counts
    );
    // Fault rows remain fully visible and have no universal-zero assertion.
    // The original independent lifecycle-zero test remains unchanged and RED.
    assert!(behavioral, "exact wake/retirement behavior failed");
    assert!(
        owner_reconciled && observer_reconciled && continuous_reconciled,
        "resource-window accounting did not close; all rows retained"
    );
    assert!(
        zero_receipt_allocations,
        "repeated receipt Rust allocations were nonzero; all rows retained"
    );
}
