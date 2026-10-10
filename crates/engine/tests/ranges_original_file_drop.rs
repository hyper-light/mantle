//! Final original-file retirement must keep the actual Range's shard available.
//! Artifact-only public oracle; run alone under Root's external native deadline.
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

#[derive(Clone, Copy)]
enum Role {
    Original,
    OwnerIo,
    Worker,
    WorkerIo,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Entered {
        duplicates: usize,
        worker_tls: usize,
        worker_writes: usize,
    },
    Returned,
}

struct Gate {
    armed: AtomicBool,
    open: Mutex<bool>,
    changed: Condvar,
    events: mpsc::SyncSender<Event>,
    duplicates: AtomicUsize,
    worker_tls: AtomicUsize,
    worker_writes: AtomicUsize,
    entered: AtomicUsize,
    returned: AtomicUsize,
    stop_replied: AtomicBool,
    progressed: AtomicBool,
    expired: AtomicBool,
}
impl Gate {
    fn release(&self) {
        *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
}
struct TlsReturned(Arc<Gate>);
impl Drop for TlsReturned {
    fn drop(&mut self) {
        self.0.worker_tls.fetch_add(1, Ordering::SeqCst);
    }
}
thread_local! {
    static TLS: Cell<Option<TlsReturned>> = const { Cell::new(None) };
}

struct File {
    file: Option<DeviceFile>,
    gate: Arc<Gate>,
    role: Role,
}
impl Drop for File {
    fn drop(&mut self) {
        if matches!(self.role, Role::Original) {
            if !self.gate.armed.load(Ordering::SeqCst) {
                // Cold setup refusal must never enter this diagnostic hold.
                drop(self.file.take());
                return;
            }
            self.gate.entered.fetch_add(1, Ordering::SeqCst);
            let event = Event::Entered {
                duplicates: self.gate.duplicates.load(Ordering::SeqCst),
                worker_tls: self.gate.worker_tls.load(Ordering::SeqCst),
                worker_writes: self.gate.worker_writes.load(Ordering::SeqCst),
            };
            eprintln!(
                "actual original file Drop entered shard={} {event:?}",
                hyper_rt::registry::current_shard().is_some()
            );
            let _ = self.gate.events.try_send(event);
            let mut open = self.gate.open.lock().unwrap();
            while !*open {
                open = self.gate.changed.wait(open).unwrap();
            }
            drop(open);
            // The inner native file closes before this final returned witness.
            drop(self.file.take());
            self.gate.returned.fetch_add(1, Ordering::SeqCst);
            let _ = self.gate.events.try_send(Event::Returned);
        } else {
            drop(self.file.take());
            self.gate.duplicates.fetch_sub(1, Ordering::SeqCst);
            if matches!(self.role, Role::Worker) {
                // Installed only by the base worker file, not its issuer duplicates.
                // The independent reaper's native join must include this real TLS Drop.
                TLS.with(|slot| {
                    let current = slot.take();
                    slot.set(current.or_else(|| Some(TlsReturned(Arc::clone(&self.gate)))));
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
        let result = self.file.as_ref().unwrap().write_all_at(bytes, at);
        if result.is_ok() && matches!(self.role, Role::Worker | Role::WorkerIo) {
            self.gate.worker_writes.fetch_add(1, Ordering::SeqCst);
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
        op: "open an original file retirement fixture",
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
                held.release(); // Failure-only cleanup; expired forbids success.
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
        self.gate.release();
        let _ = self.stop.try_send(());
        if let Some(watch) = self.watch.take() {
            watch.join().unwrap();
        }
    }
}

#[test]
fn stopping_a_range_yields_through_held_original_file_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    // The original destructor publishes exactly entered and returned.
    let (events, received) = mpsc::sync_channel(2);
    let gate = Arc::new(Gate {
        armed: AtomicBool::new(false),
        open: Mutex::new(false),
        changed: Condvar::new(),
        events,
        duplicates: AtomicUsize::new(0),
        worker_tls: AtomicUsize::new(0),
        worker_writes: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
        returned: AtomicUsize::new(0),
        stop_replied: AtomicBool::new(false),
        progressed: AtomicBool::new(false),
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
        // Existing native fixture fields. Time never decides successful release.
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
    let guard = ReleaseOnDrop::new(Arc::clone(&gate));
    let shard = runtime.shard_ids()[0];
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 1,
            slice_ns: 50_000,
            spin_ns: 0,
            inline: false,
        },
    )
    .unwrap();
    let (progress, mut proceed) = hyper_rt::sync::channel(1).unwrap();
    let progress_gate = Arc::clone(&gate);
    runtime
        .spawn_on(shard, async move {
            proceed.recv().await.unwrap();
            let held = progress_gate.entered.load(Ordering::SeqCst) == 1
                && progress_gate.returned.load(Ordering::SeqCst) == 0
                && !progress_gate.stop_replied.load(Ordering::SeqCst)
                && !progress_gate.expired.load(Ordering::SeqCst);
            progress_gate.progressed.store(held, Ordering::SeqCst);
            progress_gate.release();
        })
        .unwrap();
    let (pending, was_pending) = mpsc::sync_channel(1);
    let (ended, stopped) = mpsc::sync_channel(1);
    let stopping_gate = Arc::clone(&gate);
    // Only the actual admitted runtime route is held; cold setup Drop stayed normal.
    gate.armed.store(true, Ordering::SeqCst);
    runtime
        .spawn_on(shard, async move {
            {
                let mut client = ranges.client().unwrap();
                client.put_async(b"key", b"value").await.unwrap();
                client.checkpoint_async(7).await.unwrap();
            }
            let mut stop = pin!(ranges.stop_async());
            let mut noted = false;
            let result = poll_fn(|cx| match stop.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    if !noted {
                        pending.try_send(()).unwrap();
                        noted = true;
                    }
                    Poll::Pending
                }
            })
            .await;
            stopping_gate.stop_replied.store(true, Ordering::SeqCst);
            ended.try_send(result).unwrap();
        })
        .unwrap();
    was_pending.recv_timeout(WAIT).unwrap();
    let entered = received.recv_timeout(WAIT).unwrap();
    let premature = stopped.try_recv().ok();
    let replied_while_held = premature.is_some();
    progress.try_send(()).unwrap();
    let result = premature.unwrap_or_else(|| stopped.recv_timeout(WAIT).unwrap());
    let returned = received.recv_timeout(WAIT).unwrap();
    let expired = gate.expired.load(Ordering::SeqCst);
    let progressed = gate.progressed.load(Ordering::SeqCst);
    drop(guard);
    runtime.shutdown().unwrap();
    drop(issuer);
    // Cold fresh owner verifies the acknowledged checkpoint before liveness verdict.
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
    assert!(reopened.get(b"key", &mut value).unwrap());
    assert_eq!(value, b"value");
    reopened.check_references().unwrap();
    match entered {
        Event::Entered {
            duplicates,
            worker_tls,
            worker_writes,
        } => {
            assert_eq!(duplicates, 0, "original Drop preceded duplicate retirement");
            assert_eq!(
                worker_tls, 1,
                "original Drop preceded actual worker TLS exit"
            );
            assert!(
                worker_writes > 0,
                "checkpoint never exercised physical worker writes"
            );
        }
        Event::Returned => panic!("original file returned before entered witness"),
    }
    assert_eq!(returned, Event::Returned);
    assert!(
        !replied_while_held,
        "Stop acknowledged while original file was held"
    );
    result.unwrap();
    assert_eq!(gate.entered.load(Ordering::SeqCst), 1);
    assert_eq!(gate.returned.load(Ordering::SeqCst), 1);
    assert!(!expired, "original file Drop blocked same-shard progress");
    assert!(
        progressed,
        "same-shard task did not run while original file was held"
    );
}
