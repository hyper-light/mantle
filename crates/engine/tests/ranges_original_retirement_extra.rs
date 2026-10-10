//! Original ownership survives real accepted I/O, cancellation and later destructor failure.
//! Source-only supplemental cases; unchanged R60 lives in its original file.
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
use std::path::Path;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::task::Poll;
use std::time::Duration;

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::shard_db::{ShardDb, issuer_batches};
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

// Identical existing ranges_tls_retirement.rs native geometry/failure watchdog.
const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const DEPTH: usize = 1;
// One Range service, one stopping client, one independent same-shard task.
const ROLES: usize = 3;
const WAIT: Duration = Duration::from_secs(5);
// The maintenance slice of the native geometry above: its keys a slice come from measured time.
const SLICE_NS: u64 = 50_000;
// No slice budget: `Slice::keys` floors at one key a slice whatever time measures. The one
// slice an actor runs between taking the checkpoint and finding its owner gone then pays
// only part of sorting the checkpoint's one-entry memtable, so its packing reaches no
// worker unless the owner stays until a worker write is held.
const ONE_KEY_SLICES: u64 = 0;

#[derive(Clone, Copy)]
enum Role {
    Original,
    OwnerIo,
    Worker,
    WorkerIo,
}
#[derive(Clone, Copy, Debug)]
enum Event {
    IoHeld,
    PhysicalFailed,
    Entered {
        duplicates: usize,
        tls: usize,
        writes: usize,
        shard: bool,
    },
    Returned,
}
#[derive(Clone, Copy)]
enum Case {
    Cancel,
    FirstError,
}
const PHYSICAL: &str = "actual worker write failed before original destructor";
// One I/O witness plus original Entered and Returned, in either case.
const EVENT_ROLES: usize = 3;
struct State {
    io: bool,
    close: bool,
}
struct Gate {
    armed: AtomicBool,
    hold: AtomicBool,
    failed: AtomicBool,
    panic_close: bool,
    selected: AtomicBool,
    state: Mutex<State>,
    changed: Condvar,
    events: mpsc::SyncSender<Event>,
    duplicates: AtomicUsize,
    tls: AtomicUsize,
    writes: AtomicUsize,
    failures: AtomicUsize,
    active: AtomicUsize,
    entered: AtomicUsize,
    returned: AtomicUsize,
    io_progress: AtomicBool,
    close_progress: AtomicBool,
    stop_replied: AtomicBool,
    expired: AtomicBool,
}
impl Gate {
    fn io(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).io = true;
        self.changed.notify_all();
    }
    fn close(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).close = true;
        self.changed.notify_all();
    }
    fn all(&self) {
        self.io();
        self.close();
    }
}
struct TlsReturned(Arc<Gate>);
impl Drop for TlsReturned {
    fn drop(&mut self) {
        self.0.tls.fetch_add(1, Ordering::SeqCst);
    }
}
thread_local! { static TLS: Cell<Option<TlsReturned>> = const { Cell::new(None) }; }
struct File {
    file: Option<DeviceFile>,
    gate: Arc<Gate>,
    role: Role,
}
impl Drop for File {
    fn drop(&mut self) {
        if matches!(self.role, Role::Original) {
            if !self.gate.armed.load(Ordering::SeqCst) {
                drop(self.file.take());
                return;
            }
            self.gate.entered.fetch_add(1, Ordering::SeqCst);
            self.gate
                .events
                .try_send(Event::Entered {
                    duplicates: self.gate.duplicates.load(Ordering::SeqCst),
                    tls: self.gate.tls.load(Ordering::SeqCst),
                    writes: self.gate.writes.load(Ordering::SeqCst),
                    shard: hyper_rt::registry::current_shard().is_some(),
                })
                .unwrap();
            let mut state = self.gate.state.lock().unwrap();
            while !state.close {
                state = self.gate.changed.wait(state).unwrap();
            }
            drop(state);
            drop(self.file.take()); // Real native close precedes the returned witness and panic.
            self.gate.returned.fetch_add(1, Ordering::SeqCst);
            self.gate.events.try_send(Event::Returned).unwrap();
            if self.gate.panic_close {
                panic!("later original file Drop failure");
            }
        } else {
            drop(self.file.take());
            self.gate.duplicates.fetch_sub(1, Ordering::SeqCst);
            if matches!(self.role, Role::Worker) {
                TLS.with(|slot| {
                    let held = slot.take();
                    slot.set(held.or_else(|| Some(TlsReturned(Arc::clone(&self.gate)))));
                });
            }
        }
    }
}
impl BlockFile for File {
    fn alignment(&self) -> Alignment {
        self.file.as_ref().unwrap().alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.as_ref().unwrap().len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], at: u64) -> Result<(), DiskError> {
        self.file.as_ref().unwrap().read_exact_at(bytes, at)
    }
    fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
        let worker = matches!(self.role, Role::Worker | Role::WorkerIo);
        if worker && self.gate.failed.load(Ordering::SeqCst) {
            self.gate.failures.fetch_add(1, Ordering::SeqCst);
            if !self.gate.selected.swap(true, Ordering::SeqCst) {
                self.gate.events.try_send(Event::PhysicalFailed).unwrap();
            }
            return Err(DiskError::Io {
                op: PHYSICAL,
                path: Path::new("original-retirement").to_owned(),
                source: std::io::Error::other(PHYSICAL),
            });
        }
        let held = worker
            && self.gate.hold.load(Ordering::SeqCst)
            && !self.gate.selected.swap(true, Ordering::SeqCst);
        if held {
            self.gate.active.fetch_add(1, Ordering::SeqCst);
            self.gate.events.try_send(Event::IoHeld).unwrap();
            let mut state = self.gate.state.lock().unwrap();
            while !state.io {
                state = self.gate.changed.wait(state).unwrap();
            }
        }
        let result = self.file.as_ref().unwrap().write_all_at(bytes, at);
        if held {
            self.gate.active.fetch_sub(1, Ordering::SeqCst);
        }
        if worker && result.is_ok() {
            self.gate.writes.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.as_ref().unwrap().sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        let file = self.file.as_ref().unwrap().try_clone()?;
        self.gate.duplicates.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            file: Some(file),
            gate: Arc::clone(&self.gate),
            role: match self.role {
                Role::Original | Role::OwnerIo => Role::OwnerIo,
                Role::Worker | Role::WorkerIo => Role::WorkerIo,
            },
        })
    }
}
fn open(path: &Path, create: bool, role: Role, gate: Arc<Gate>) -> Result<File, Error> {
    let file = DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .map_err(|error| Error::Io {
        op: "open original-retirement extra fixture",
        detail: error.to_string(),
    })?;
    if !matches!(role, Role::Original) {
        gate.duplicates.fetch_add(1, Ordering::SeqCst);
    }
    Ok(File {
        file: Some(file),
        gate,
        role,
    })
}
struct ReleaseOnDrop {
    gate: Arc<Gate>,
    stop: mpsc::SyncSender<()>,
    watch: Option<std::thread::JoinHandle<()>>,
}
impl ReleaseOnDrop {
    fn new(gate: Arc<Gate>) -> Self {
        let (stop, stopped) = mpsc::sync_channel(1);
        let held = Arc::clone(&gate);
        let watch = std::thread::spawn(move || {
            if stopped.recv_timeout(WAIT).is_err() {
                held.expired.store(true, Ordering::SeqCst);
                held.all();
            }
        });
        Self {
            gate,
            stop,
            watch: Some(watch),
        }
    }
}
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.gate.all();
        let _ = self.stop.try_send(());
        if let Some(watch) = self.watch.take() {
            watch.join().unwrap();
        }
    }
}
#[derive(Debug)]
enum Command {
    CancelIo(hyper_rt::TaskId),
    Close,
}
struct Reply {
    checkpoint: Option<Result<(), Error>>,
    stopped: Option<Result<(), Error>>,
}
async fn pending_once<F: Future>(future: std::pin::Pin<&mut F>) -> bool {
    let mut future = future;
    poll_fn(|cx| Poll::Ready(matches!(future.as_mut().poll(cx), Poll::Pending))).await
}
fn run(case: Case, slice_ns: u64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let (events, received) = mpsc::sync_channel(EVENT_ROLES);
    let gate = Arc::new(Gate {
        armed: AtomicBool::new(false),
        hold: AtomicBool::new(false),
        failed: AtomicBool::new(false),
        panic_close: matches!(case, Case::FirstError),
        selected: AtomicBool::new(false),
        state: Mutex::new(State {
            io: false,
            close: false,
        }),
        changed: Condvar::new(),
        events,
        duplicates: AtomicUsize::new(0),
        tls: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        failures: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
        returned: AtomicUsize::new(0),
        io_progress: AtomicBool::new(false),
        close_progress: AtomicBool::new(false),
        stop_replied: AtomicBool::new(false),
        expired: AtomicBool::new(false),
    });
    let mut db = ShardDb::create(
        open(&path, true, Role::Original, Arc::clone(&gate)).unwrap(),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    let run = STORE.page_size * usize::try_from(STORE.extent_pages).unwrap();
    db.set_memory(2 * (DEPTH + 2) * run).unwrap();
    let issuer = Issuer::start_for(&path, DEPTH, issuer_batches(DEPTH, DEPTH)).unwrap();
    db.attach(&issuer, DEPTH).unwrap();
    let worker_path = path.clone();
    let worker_gate = Arc::clone(&gate);
    db.set_workers(move || open(&worker_path, false, Role::Worker, Arc::clone(&worker_gate)))
        .unwrap();
    let mut runtime = Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: ROLES,
        timers_per_shard: ROLES,
        interests_per_shard: hyper_rt::runtime::interests_for(ROLES),
        ring_entries: ROLES,
        step_budget_ns: 50_000,
        timer_tick_ns: 100_000,
        batch: ROLES,
        pin: false,
        cores: Vec::new(),
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    // Failure cleanup is declared after Runtime, so it opens both genuine holds before joins.
    let guard = ReleaseOnDrop::new(Arc::clone(&gate));
    let shard = runtime.shard_ids()[0];
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 1,
            slice_ns,
            spin_ns: 0,
            inline: false,
        },
    )
    .unwrap();
    let actor = ranges.task_ids()[0];
    let (command, mut commands) = hyper_rt::sync::channel(1).unwrap();
    let (progress, progressed) = mpsc::sync_channel(2);
    let observed = Arc::clone(&gate);
    runtime
        .spawn_on(shard, async move {
            while let Ok(command) = commands.recv().await {
                match command {
                    Command::CancelIo(actor) => {
                        hyper_rt::futures::cancel(actor).unwrap();
                        observed.io_progress.store(
                            observed.active.load(Ordering::SeqCst) == 1
                                && observed.entered.load(Ordering::SeqCst) == 0
                                && observed.tls.load(Ordering::SeqCst) == 0
                                && !observed.expired.load(Ordering::SeqCst),
                            Ordering::SeqCst,
                        );
                        observed.io();
                        progress.try_send(1u8).unwrap();
                    }
                    Command::Close => {
                        observed.close_progress.store(
                            observed.entered.load(Ordering::SeqCst) == 1
                                && observed.returned.load(Ordering::SeqCst) == 0
                                && observed.active.load(Ordering::SeqCst) == 0
                                && observed.duplicates.load(Ordering::SeqCst) == 0
                                && observed.tls.load(Ordering::SeqCst) == 1
                                && !observed.stop_replied.load(Ordering::SeqCst)
                                && !observed.expired.load(Ordering::SeqCst),
                            Ordering::SeqCst,
                        );
                        observed.close();
                        progress.try_send(2u8).unwrap();
                        break;
                    }
                }
            }
        })
        .unwrap();
    let (published, publication) = mpsc::sync_channel(1);
    let (reply, replied) = mpsc::sync_channel(1);
    // A fact-driven probe. Cancel: the owner leaves on it once a worker write is held.
    // FirstError: the same pinned Stop is repolled on it during actual original hold.
    let (probe, mut probed) = hyper_rt::sync::channel(1).unwrap();
    let (checked, check) = mpsc::sync_channel(1);
    let driver_gate = Arc::clone(&gate);
    gate.armed.store(true, Ordering::SeqCst);
    runtime
        .spawn_on(shard, async move {
            let mut client = ranges.client().unwrap();
            client.put_async(b"durable", b"value").await.unwrap();
            client.checkpoint_async(7).await.unwrap();
            client.put_async(b"pending", b"next").await.unwrap();
            match case {
                Case::Cancel => driver_gate.hold.store(true, Ordering::SeqCst),
                Case::FirstError => driver_gate.failed.store(true, Ordering::SeqCst),
            }
            let (first_pending, checkpoint) = {
                let mut checkpoint = pin!(client.checkpoint_async(8));
                let first_pending = pending_once(checkpoint.as_mut()).await;
                match case {
                    // The actor runs one maintenance slice between taking the checkpoint and
                    // finding its owner gone, and the slice's measured key budget may not cover
                    // sorting the memtable: an owner dropped now may leave no write to hold.
                    Case::Cancel => {
                        probed.recv().await.unwrap();
                        (first_pending, None)
                    }
                    Case::FirstError => (first_pending, Some(checkpoint.await)),
                }
            };
            drop(client);
            match case {
                Case::Cancel => {
                    // The borrower and actual Ranges owner disappear while the accepted request
                    // lives, its worker write held.
                    drop(ranges);
                    published.try_send(first_pending).unwrap();
                    reply
                        .try_send(Reply {
                            checkpoint,
                            stopped: None,
                        })
                        .unwrap();
                }
                Case::FirstError => {
                    let mut stopping = pin!(ranges.stop_async());
                    let stop_pending = pending_once(stopping.as_mut()).await;
                    published.try_send(first_pending && stop_pending).unwrap();
                    probed.recv().await.unwrap();
                    let ready = poll_fn(|cx| match stopping.as_mut().poll(cx) {
                        Poll::Pending => Poll::Ready(None),
                        Poll::Ready(result) => Poll::Ready(Some(result)),
                    })
                    .await;
                    checked.try_send(ready.is_none()).unwrap();
                    let stopped = match ready {
                        Some(result) => result,
                        None => stopping.await,
                    };
                    driver_gate.stop_replied.store(true, Ordering::SeqCst);
                    reply
                        .try_send(Reply {
                            checkpoint,
                            stopped: Some(stopped),
                        })
                        .unwrap();
                }
            }
        })
        .unwrap();
    let first = received.recv_timeout(WAIT).unwrap();
    if matches!(case, Case::Cancel) {
        assert!(
            matches!(first, Event::IoHeld),
            "the owner may leave only during a held worker write"
        );
        probe.try_send(()).unwrap();
    }
    let published_pending = publication.recv_timeout(WAIT).unwrap();
    let progressed_io = if matches!(case, Case::Cancel) {
        command.try_send(Command::CancelIo(actor)).unwrap();
        Some(progressed.recv_timeout(WAIT).unwrap())
    } else {
        None
    };
    let entered = received.recv_timeout(WAIT).unwrap();
    let held_pending = if matches!(case, Case::FirstError) {
        probe.try_send(()).unwrap();
        Some(check.recv_timeout(WAIT).unwrap())
    } else {
        None
    };
    // Close is released only after the actual held-Stop repoll verdict was captured.
    command.try_send(Command::Close).unwrap();
    let reply = replied.recv_timeout(WAIT).unwrap();
    let returned = received.recv_timeout(WAIT).unwrap();
    let progressed_close = progressed.recv_timeout(WAIT).unwrap();
    drop(command);
    drop(guard);
    let shutdown = runtime.shutdown();
    drop(issuer);
    let file = DeviceFile::open(
        &path,
        false,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .unwrap();
    let (mut reopened, recovered) = ShardDb::open(file, STORE, STORE.page_size, TRUNK).unwrap();
    assert_eq!(recovered, 7);
    let mut value = Vec::new();
    assert!(reopened.get(b"durable", &mut value).unwrap());
    assert_eq!(value, b"value");
    value.clear();
    assert!(!reopened.get(b"pending", &mut value).unwrap());
    reopened.check_references().unwrap();
    assert!(published_pending);
    match (case, first) {
        (Case::Cancel, Event::IoHeld) => {
            assert_eq!(progressed_io, Some(1));
            assert!(gate.io_progress.load(Ordering::SeqCst));
            shutdown.unwrap();
        }
        (Case::FirstError, Event::PhysicalFailed) => {
            assert_eq!(
                held_pending,
                Some(true),
                "Stop completed while original native close was held"
            );
            assert!(gate.failures.load(Ordering::SeqCst) > 0);
            for result in [reply.checkpoint, reply.stopped] {
                assert!(
                    matches!(result, Some(Err(Error::Io { detail, .. })) if detail.contains(PHYSICAL))
                );
            }
            assert!(
                shutdown.is_err(),
                "explicit original Drop panic must also be reported cold"
            );
        }
        _ => panic!("missing genuine worker I/O witness"),
    }
    match entered {
        Event::Entered {
            duplicates,
            tls,
            writes,
            shard,
        } => {
            assert_eq!(duplicates, 0);
            assert_eq!(tls, 1);
            assert!(writes > 0);
            assert!(!shard);
        }
        _ => panic!("missing actual original Drop witness"),
    }
    assert!(matches!(returned, Event::Returned));
    assert_eq!(progressed_close, 2);
    assert_eq!(gate.entered.load(Ordering::SeqCst), 1);
    assert_eq!(gate.returned.load(Ordering::SeqCst), 1);
    assert!(gate.close_progress.load(Ordering::SeqCst));
    assert!(!gate.expired.load(Ordering::SeqCst));
}
#[test]
fn owning_drop_and_cancellation_keep_original_through_held_io_tls_and_off_shard_close() {
    run(Case::Cancel, SLICE_NS);
}
#[test]
fn owning_drop_at_one_key_a_slice_still_leaves_during_held_io() {
    run(Case::Cancel, ONE_KEY_SLICES);
}
#[test]
fn earlier_physical_failure_survives_later_original_drop_panic_after_native_close() {
    run(Case::FirstError, SLICE_NS);
}
