//! Actual Range terminal retirement must leave its native shard available while worker TLS drops.
//! Source-only proposal: run alone under an external native controller deadline.
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

use std::cell::RefCell;
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

// Existing pool_lifecycle.rs native geometry and failure-only watchdog.
const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
const WAIT: Duration = Duration::from_secs(5);
// One Range actor, one stopping client, and one independently admitted progress task.
const ROLES: usize = 3;
// One worker is enough to expose EOF before native TLS retirement.
const DEPTH: usize = 1;
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    TlsEntered,
    TlsReturned,
}

struct Gate {
    armed: AtomicBool,
    open: Mutex<bool>,
    changed: Condvar,
    events: mpsc::SyncSender<Event>,
    entered: AtomicUsize,
    returned: AtomicUsize,
    progress: AtomicBool,
    expired: AtomicBool,
}
impl Gate {
    fn release(&self) {
        *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
}

struct TlsExit(Arc<Gate>);
impl Drop for TlsExit {
    fn drop(&mut self) {
        self.0.entered.fetch_add(1, Ordering::SeqCst);
        self.0.events.try_send(Event::TlsEntered).unwrap();
        let mut open = self.0.open.lock().unwrap();
        while !*open {
            open = self.0.changed.wait(open).unwrap();
        }
        self.0.returned.fetch_add(1, Ordering::SeqCst);
        self.0.events.try_send(Event::TlsReturned).unwrap();
    }
}
thread_local! {
    static EXIT: RefCell<Option<TlsExit>> = const { RefCell::new(None) };
}

struct File {
    file: DeviceFile,
    gate: Arc<Gate>,
    worker: bool,
}
impl Drop for File {
    fn drop(&mut self) {
        let thread = std::thread::current();
        if self.worker
            && self.gate.armed.load(Ordering::SeqCst)
            && thread
                .name()
                .is_some_and(|name| name.starts_with("mantle-maint-"))
        {
            // The original worker Store file drops before its back sender and EOF.
            // This guard drops only after the worker closure returns. Device duplicates
            // on other threads do not install it, and no previous guard is replaced.
            EXIT.with(|exit| {
                let mut exit = exit.borrow_mut();
                if exit.is_none() {
                    *exit = Some(TlsExit(Arc::clone(&self.gate)));
                }
            });
        }
    }
}
impl BlockFile for File {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], at: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(bytes, at)
    }
    fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
        self.file.write_all_at(bytes, at)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: Arc::clone(&self.gate),
            worker: self.worker,
        })
    }
}
fn open(path: &Path, create: bool, worker: bool, gate: Arc<Gate>) -> Result<File, Error> {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .map(|file| File { file, gate, worker })
    .map_err(|error| Error::Io {
        op: "open a TLS retirement fixture file",
        detail: error.to_string(),
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
        let watch_gate = Arc::clone(&gate);
        let watch = std::thread::spawn(move || {
            if stopped.recv_timeout(WAIT).is_err() {
                watch_gate.expired.store(true, Ordering::SeqCst);
                watch_gate.release();
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
fn stopping_a_range_yields_through_actual_worker_tls_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    // Exactly one entered and one returned witness for the affordable worker.
    let (events, received) = mpsc::sync_channel(2);
    let gate = Arc::new(Gate {
        armed: AtomicBool::new(false),
        open: Mutex::new(false),
        changed: Condvar::new(),
        events,
        entered: AtomicUsize::new(0),
        returned: AtomicUsize::new(0),
        progress: AtomicBool::new(false),
        expired: AtomicBool::new(false),
    });
    let mut db = ShardDb::create(
        open(&path, true, false, Arc::clone(&gate)).unwrap(),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    let run = STORE.page_size * usize::try_from(STORE.extent_pages).unwrap();
    // Existing accepted-one-worker budget: owner plus worker, actual batch and two output roles.
    db.set_memory(2 * (DEPTH + 2) * run).unwrap();
    let issuer = Issuer::start_for(&path, DEPTH, issuer_batches(DEPTH, DEPTH)).unwrap();
    db.attach(&issuer, DEPTH).unwrap();
    let worker_path = path.clone();
    let worker_gate = Arc::clone(&gate);
    db.set_workers(move || open(&worker_path, false, true, Arc::clone(&worker_gate)))
        .unwrap();
    let mut runtime = Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: ROLES,
        timers_per_shard: ROLES,
        interests_per_shard: hyper_rt::runtime::interests_for(ROLES),
        ring_entries: ROLES,
        // Existing ranges_async.rs native fixture timing fields; no time decides success.
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
    let shard = runtime.shard_ids()[0];
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 1,
            slice_ns: 50_000,
            spin_ns: 0,
        },
    )
    .unwrap();
    // Opens the TLS gate before Runtime/Issuer Drop on every failure path.
    let guard = ReleaseOnDrop::new(Arc::clone(&gate));
    let (progress, mut proceed) = hyper_rt::sync::channel(1).unwrap();
    let progress_gate = Arc::clone(&gate);
    runtime
        .spawn_on(shard, async move {
            proceed.recv().await.unwrap();
            // Recorded by a separately admitted task on the exact Range's shard, before release.
            let retained = progress_gate.entered.load(Ordering::SeqCst) == 1
                && progress_gate.returned.load(Ordering::SeqCst) == 0
                && !progress_gate.expired.load(Ordering::SeqCst);
            progress_gate.progress.store(retained, Ordering::SeqCst);
            progress_gate.release();
        })
        .unwrap();
    let (pending, was_pending) = mpsc::sync_channel(1);
    let (ended, stopped) = mpsc::sync_channel(1);
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
            ended.try_send(result).unwrap();
        })
        .unwrap();
    was_pending.recv().unwrap();
    assert_eq!(received.recv().unwrap(), Event::TlsEntered);
    assert!(matches!(stopped.try_recv(), Err(mpsc::TryRecvError::Empty)));
    progress.try_send(()).unwrap();
    let result = stopped.recv().unwrap();
    assert_eq!(received.recv().unwrap(), Event::TlsReturned);
    let expired = gate.expired.load(Ordering::SeqCst);
    let progressed = gate.progress.load(Ordering::SeqCst);
    drop(guard);
    runtime.shutdown().unwrap();
    drop(issuer);
    assert!(
        !expired,
        "same-shard progress was blocked by actual worker TLS retirement"
    );
    assert!(progressed);
    result.unwrap();
    assert_eq!(gate.entered.load(Ordering::SeqCst), 1);
    assert_eq!(gate.returned.load(Ordering::SeqCst), 1);
    let (mut reopened, recovered) = ShardDb::open(
        open(&path, false, false, gate).unwrap(),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(recovered, 7);
    let mut value = Vec::new();
    assert!(reopened.get(b"key", &mut value).unwrap());
    assert_eq!(value, b"value");
    reopened.check_references().unwrap();
}
