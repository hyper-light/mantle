//! Consuming refusal returns ownership; borrowed finish keeps it across cancellation
//! and exposes the file only after actual issuer/native retirement.
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

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::runtime::{LocalRuntime, Runtime, RuntimeConfig};
use hyper_rt::sync::{Sender, channel};
use mantle_engine::Error;
use mantle_engine::branch::Op;
use mantle_engine::memtable::hashed::HashMem;
use mantle_engine::ranges::{Ranges, RangesConfig, StartError};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::{Config, IntoFile, Store};
use mantle_engine::trunk::TrunkConfig;
use std::future::{Future, poll_fn};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 2,
    max_extents: 256,
};
// Existing native issuer fixture failure-only bound; release cannot satisfy success.
const WATCH: Duration = Duration::from_secs(5);

struct Gate {
    armed: AtomicBool,
    entered: AtomicBool,
    expired: AtomicBool,
    copies: AtomicUsize,
    callbacks: AtomicUsize,
    notice: Sender<()>,
    open: Mutex<bool>,
    changed: Condvar,
    fail: bool,
}

impl Gate {
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn hold(&self) -> Result<(), DiskError> {
        self.callbacks.fetch_add(1, Ordering::SeqCst);
        if !self.entered.swap(true, Ordering::SeqCst) {
            self.notice.try_send(()).unwrap();
        }
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
        if self.fail {
            Err(DiskError::Io {
                op: "held store write",
                path: "into_file".into(),
                source: std::io::Error::other("first held store write witness"),
            })
        } else {
            Ok(())
        }
    }
}

struct Watch {
    gate: Arc<Gate>,
    ended: mpsc::SyncSender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Watch {
    fn new(gate: Arc<Gate>) -> Self {
        let (ended, receiver) = mpsc::sync_channel(1);
        let watched = Arc::clone(&gate);
        let thread = std::thread::spawn(move || {
            if receiver.recv_timeout(WATCH).is_err() {
                watched.expired.store(true, Ordering::SeqCst);
            }
            watched.release();
        });
        Self {
            gate,
            ended,
            thread: Some(thread),
        }
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        self.gate.release();
        let _ = self.ended.try_send(());
        self.thread.take().unwrap().join().unwrap();
    }
}

struct File {
    file: DeviceFile,
    gate: Arc<Gate>,
    worker: bool,
}
impl Drop for File {
    fn drop(&mut self) {
        self.gate.copies.fetch_sub(1, Ordering::SeqCst);
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
        if self.worker && self.gate.armed.load(Ordering::SeqCst) {
            self.gate.hold()?;
        }
        self.file.write_all_at(bytes, at)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        let file = self.file.try_clone()?;
        self.gate.copies.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            file,
            gate: Arc::clone(&self.gate),
            worker: true,
        })
    }
}

fn file(path: &Path, create: bool, worker: bool, gate: Arc<Gate>) -> File {
    let file = DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap();
    gate.copies.fetch_add(1, Ordering::SeqCst);
    File { file, gate, worker }
}
fn gate(fail: bool) -> (Arc<Gate>, hyper_rt::sync::ChannelReceiver<()>) {
    let (notice, observed) = channel(1).unwrap();
    (
        Arc::new(Gate {
            armed: AtomicBool::new(false),
            entered: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            copies: AtomicUsize::new(0),
            callbacks: AtomicUsize::new(0),
            notice,
            open: Mutex::new(false),
            changed: Condvar::new(),
            fail,
        }),
        observed,
    )
}
fn runtime() -> LocalRuntime {
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 2,
        timers_per_shard: 2,
        interests_per_shard: 4,
        ring_entries: 2,
        batch: 2,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        pin: false,
        cores: Vec::new(),
        page_bytes: CONFIG.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}

fn store_case(fail: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let (gate, mut observed) = gate(fail);
    let mut store = Store::create(file(&path, true, false, Arc::clone(&gate)), CONFIG).unwrap();
    let old = store.allocate_extent().unwrap();
    let old_root = store.address(old, 0).unwrap();
    store.write_page(old_root, b"durable old page").unwrap();
    store.checkpoint(Some(old_root), 1).unwrap();
    let next = store.allocate_extent().unwrap();
    let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    // Watch opens on failure before any issuer/Store owner can be joined during unwind.
    let watch = Watch::new(Arc::clone(&gate));
    gate.armed.store(true, Ordering::SeqCst);
    let mut run = store.run().unwrap();
    store
        .queue_page(
            &mut run,
            store.address(next, 0).unwrap(),
            b"accepted first page",
        )
        .unwrap();
    store
        .queue_page(
            &mut run,
            store.address(next, 1).unwrap(),
            b"accepted second page",
        )
        .unwrap();
    store.give_run(run);
    observed.blocking_recv().unwrap();
    let generation = store.generation();
    let refs = store.refs().to_vec();
    let before = store.io_stats();
    let copies = gate.copies.load(Ordering::SeqCst);
    let (release, mut released) = channel(1).unwrap();
    let opener = Arc::clone(&gate);
    let mut runtime = runtime();
    runtime
        .spawn(async move {
            released.recv().await.unwrap();
            assert!(!opener.expired.load(Ordering::SeqCst));
            opener.release();
        })
        .unwrap();
    let seen = Arc::clone(&gate);
    let (returned_file, result) = runtime
        .block_on(async move {
            store = match store.into_file() {
                IntoFile::Refused { owner, error } => {
                    assert!(matches!(error, Error::InvalidArgument { .. }));
                    owner
                }
                IntoFile::Finished { .. } => {
                    panic!("unfinished store exposed its file on a runtime")
                }
            };
            assert_eq!(store.generation(), generation);
            assert_eq!(store.refs(), refs);
            assert_eq!(store.io_stats().runs_answered, before.runs_answered);
            assert_eq!(seen.copies.load(Ordering::SeqCst), copies);
            {
                let mut ending = std::pin::pin!(store.finish_async());
                poll_fn(|cx| {
                    assert!(ending.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                let mut foreign = Context::from_waker(Waker::noop());
                assert!(matches!(
                    ending.as_mut().poll(&mut foreign),
                    Poll::Ready(Err(Error::InvalidArgument { .. }))
                ));
            }
            // The refused repoll did not complete close or return an attachment duplicate.
            store = match store.into_file() {
                IntoFile::Refused { owner, error } => {
                    assert!(matches!(error, Error::InvalidArgument { .. }));
                    owner
                }
                IntoFile::Finished { .. } => {
                    panic!("context refusal made an unfinished store extractable")
                }
            };
            assert_eq!(seen.copies.load(Ordering::SeqCst), copies);
            assert!(matches!(
                store.allocate_extent(),
                Err(Error::InvalidArgument { .. })
            ));
            assert_eq!(store.refs(), refs);
            release.try_send(()).unwrap();
            let finished = store.finish_async().await;
            assert!(!seen.expired.load(Ordering::SeqCst));
            assert_eq!(seen.copies.load(Ordering::SeqCst), 1);
            match store.into_file() {
                IntoFile::Finished { file, result } => {
                    if fail {
                        assert!(matches!(&finished, Err(Error::Io { detail, .. })
                        if detail.contains("first held store write witness")));
                        assert!(matches!(&result, Err(Error::Io { detail, .. })
                        if detail.contains("first held store write witness")));
                    } else {
                        finished.unwrap();
                        result.as_ref().unwrap();
                    }
                    (file, result)
                }
                IntoFile::Refused { .. } => panic!("actual retired store refused file extraction"),
            }
        })
        .unwrap();
    assert_eq!(gate.callbacks.load(Ordering::SeqCst), 1);
    assert!(!gate.expired.load(Ordering::SeqCst));
    drop(returned_file);
    assert_eq!(gate.copies.load(Ordering::SeqCst), 0);
    drop(result);
    drop(runtime);
    drop(watch);
    gate.armed.store(false, Ordering::SeqCst);
    let (mut recovered, checkpoint) =
        Store::open(file(&path, false, false, Arc::clone(&gate)), CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 1);
    assert_eq!(checkpoint.root, Some(old_root));
    let mut value = Vec::new();
    recovered.read_page(old_root, &mut value).unwrap();
    assert_eq!(value, b"durable old page");
    if !fail {
        // These bytes landed, but the unchanged checkpoint does not name the new extent.
        for (page, expected) in [
            (0, b"accepted first page".as_slice()),
            (1, b"accepted second page".as_slice()),
        ] {
            value.clear();
            recovered
                .read_page(recovered.address(next, page).unwrap(), &mut value)
                .unwrap();
            assert_eq!(value, expected);
        }
    }
    match recovered.into_file() {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            drop(file);
        }
        IntoFile::Refused { .. } => panic!("cold recovered store refused extraction"),
    }
}

#[test]
fn refusal_and_foreign_repoll_retain_store_until_actual_async_retirement() {
    store_case(false);
}

#[test]
fn physical_write_failure_returns_quiescent_file_and_preserves_first_cause() {
    store_case(true);
}

struct StartOnDrop {
    runtime: Option<Runtime>,
    engines: Option<Vec<(Vec<u8>, ShardDb<File>)>>,
    returned: mpsc::SyncSender<(Runtime, Result<Ranges, StartError<File>>)>,
}
impl Drop for StartOnDrop {
    fn drop(&mut self) {
        assert!(hyper_rt::registry::current_shard().is_some());
        assert!(
            hyper_rt::futures::current_task().is_none(),
            "actual post-poll destructor context"
        );
        let mut runtime = self.runtime.take().unwrap();
        let outcome = Ranges::start(
            &mut runtime,
            self.engines.take().unwrap(),
            RangesConfig {
                clients: 1,
                slice_ns: 50_000,
                spin_ns: 0,
                inline: false,
            },
        );
        self.returned.try_send((runtime, outcome)).unwrap();
    }
}

fn shard_case(start_on_drop: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("shard");
    let (gate, mut observed) = gate(false);
    let memory = CONFIG.page_size;
    let value = vec![7; CONFIG.page_size / 2];
    let mut probe = HashMem::new(memory).unwrap();
    let mut fits = None;
    for i in 0..memory {
        let key = format!("new/{i:08}");
        match probe.insert(key.as_bytes(), Op::Put, &value) {
            Ok(()) => {}
            Err(Error::LimitExceeded { .. }) => {
                fits = Some(i);
                break;
            }
            Err(error) => panic!("public memtable probe refused: {error:?}"),
        }
    }
    let fits = fits.expect("half-page values fill the input-derived arena");
    assert!(fits > 0);
    let mut db = ShardDb::create(
        file(&path, true, false, Arc::clone(&gate)),
        CONFIG,
        memory,
        TrunkConfig {
            fanout: 3,
            leaf_entries: 96,
        },
    )
    .unwrap();
    db.put(b"baseline", b"durable baseline").unwrap();
    db.checkpoint(1).unwrap();
    let worker_path = path.clone();
    let worker_gate = Arc::clone(&gate);
    db.set_workers(move || Ok(file(&worker_path, false, true, Arc::clone(&worker_gate))))
        .unwrap();
    let watch = Watch::new(Arc::clone(&gate));
    gate.armed.store(true, Ordering::SeqCst);
    for i in 0..=fits {
        db.put(format!("new/{i:08}").as_bytes(), &value).unwrap();
    }
    observed.blocking_recv().unwrap();
    let stats = db.stats();
    let key = format!("new/{fits:08}");
    let mut runtime = runtime();
    let seen = Arc::clone(&gate);
    let checked_key = key.clone();
    let db = if start_on_drop {
        // A separate cold runtime is returned with every engine; no cold owner is dropped
        // by the LocalRuntime destructor under test.
        let native = Runtime::start(&RuntimeConfig {
            shards: 1,
            tasks_per_shard: 2,
            timers_per_shard: 2,
            interests_per_shard: 4,
            ring_entries: 2,
            batch: 2,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            pin: false,
            cores: Vec::new(),
            page_bytes: CONFIG.page_size,
            spin_ns: 0,
            wake_tracking: None,
        })
        .unwrap();
        let engines = vec![(Vec::new(), db)];
        let allocation = engines.as_ptr();
        let (returned, response) = mpsc::sync_channel(1);
        let probe = StartOnDrop {
            runtime: Some(native),
            engines: Some(engines),
            returned,
        };
        let (pending, mut pending_seen) = channel(1).unwrap();
        runtime
            .spawn(async move {
                let _probe = probe;
                let mut notified = false;
                poll_fn(|_| {
                    if !notified {
                        pending.try_send(()).unwrap();
                        notified = true;
                    }
                    Poll::<()>::Pending
                })
                .await;
            })
            .unwrap();
        runtime
            .block_on(async move {
                pending_seen.recv().await.unwrap();
            })
            .unwrap();
        let (native, outcome) = response.recv().unwrap();
        let mut engines = match outcome {
            Err(StartError::Refused { ranges, error }) => {
                assert!(matches!(error, Error::InvalidArgument { .. }));
                ranges
            }
            _ => panic!("entered-context startup did not return every original engine"),
        };
        assert_eq!(engines.as_ptr(), allocation);
        assert_eq!(engines.len(), 1);
        native.shutdown().unwrap();
        engines.pop().unwrap().1
    } else {
        runtime
            .block_on(async move {
                let mut db = match db.into_file() {
                    IntoFile::Refused { owner, error } => {
                        assert!(matches!(error, Error::InvalidArgument { .. }));
                        owner
                    }
                    IntoFile::Finished { .. } => {
                        panic!("cold worker owner was consumed on a runtime")
                    }
                };
                assert_eq!(db.stats().0.rotations, stats.0.rotations);
                let refused = db.finish_async().await;
                assert!(matches!(refused, Err(Error::InvalidArgument { .. })));
                let mut actual = Vec::new();
                assert!(db.get(checked_key.as_bytes(), &mut actual).unwrap());
                assert_eq!(actual, value);
                assert!(!seen.expired.load(Ordering::SeqCst));
                assert_eq!(db.stats().0.flushes, stats.0.flushes);
                db
            })
            .unwrap()
    };
    assert!(!gate.expired.load(Ordering::SeqCst));
    gate.release();
    match db.into_file() {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            drop(file);
        }
        IntoFile::Refused { .. } => panic!("returned cold worker owner refused completion"),
    }
    drop(runtime);
    drop(watch);
    gate.armed.store(false, Ordering::SeqCst);
    let (mut recovered, applied) = ShardDb::open(
        file(&path, false, false, gate),
        CONFIG,
        memory,
        TrunkConfig {
            fanout: 3,
            leaf_entries: 96,
        },
    )
    .unwrap();
    assert_eq!(applied, 1);
    let mut value = Vec::new();
    assert!(recovered.get(b"baseline", &mut value).unwrap());
    assert_eq!(value, b"durable baseline");
    assert!(!recovered.get(key.as_bytes(), &mut value).unwrap());
    match recovered.into_file() {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            drop(file);
        }
        IntoFile::Refused { .. } => panic!("cold recovered shard refused extraction"),
    }
}

#[test]
fn unadopted_worker_finish_refuses_without_losing_shard_or_closing_its_input() {
    shard_case(false);
}

#[test]
fn entered_runtime_destructor_startup_refuses_and_returns_every_engine() {
    shard_case(true);
}

// Append to proposed tests/into_file_async.rs, reusing its real File/Gate/Watch.
// This is a public cleanup oracle. Capture its verdict, then release/drain/recover
// before asserting so a semantic RED does not strand a held native callback.
#[test]
fn an_accepted_unused_grant_returns_after_async_finish_is_pending() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("grant-return");
    let (gate, mut observed) = gate(false);
    let mut store = Store::create(file(&path, true, false, Arc::clone(&gate)), CONFIG).unwrap();
    let old = store.allocate_extent().unwrap();
    let root = store.address(old, 0).unwrap();
    store
        .write_page(root, b"durable before grant return")
        .unwrap();
    store.checkpoint(Some(root), 1).unwrap();
    let unused = store.grant(1).unwrap();
    let output = store.allocate_extent().unwrap();
    let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    store.attach(&issuer, 1).unwrap();
    let watch = Watch::new(Arc::clone(&gate));
    gate.armed.store(true, Ordering::SeqCst);
    let mut run = store.run().unwrap();
    for (page, payload) in [
        (0, b"accepted output zero".as_slice()),
        (1, b"accepted output one".as_slice()),
    ] {
        store
            .queue_page(&mut run, store.address(output, page).unwrap(), payload)
            .unwrap();
    }
    store.give_run(run);
    observed.blocking_recv().unwrap();
    assert!(gate.entered.load(Ordering::SeqCst));
    eprintln!("actual native write held before finish/grant return");
    let generation = store.generation();
    let seen = Arc::clone(&gate);
    let mut runtime = runtime();
    let (returned_file, returned, repeated, finished, extracted) = runtime
        .block_on(async move {
            {
                let mut finish = std::pin::pin!(store.finish_async());
                poll_fn(|cx| {
                    assert!(finish.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            let returned = store.grant_back(&unused);
            // The same grant cannot be returned twice; this also checks old held-once validation.
            let repeated = returned.is_ok().then(|| store.grant_back(&unused));
            assert!(!seen.expired.load(Ordering::SeqCst));
            assert_eq!(seen.copies.load(Ordering::SeqCst), 2);
            assert_eq!(store.generation(), generation);
            seen.release();
            let finished = store.finish_async().await;
            assert!(!seen.expired.load(Ordering::SeqCst));
            assert_eq!(seen.copies.load(Ordering::SeqCst), 1);
            match store.into_file() {
                IntoFile::Finished { file, result } => (file, returned, repeated, finished, result),
                IntoFile::Refused { .. } => panic!("a physically retired store refused extraction"),
            }
        })
        .unwrap();
    drop(returned_file);
    drop(runtime);
    drop(watch);
    drop(issuer);
    assert_eq!(gate.callbacks.load(Ordering::SeqCst), 1);
    assert!(!gate.expired.load(Ordering::SeqCst));
    assert_eq!(gate.copies.load(Ordering::SeqCst), 0);
    gate.armed.store(false, Ordering::SeqCst);
    let (mut recovered, checkpoint) =
        Store::open(file(&path, false, false, Arc::clone(&gate)), CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 1);
    assert_eq!(checkpoint.root, Some(root));
    let mut value = Vec::new();
    recovered.read_page(root, &mut value).unwrap();
    assert_eq!(value, b"durable before grant return");
    for (page, expected) in [
        (0, b"accepted output zero".as_slice()),
        (1, b"accepted output one".as_slice()),
    ] {
        value.clear();
        recovered
            .read_page(recovered.address(output, page).unwrap(), &mut value)
            .unwrap();
        assert_eq!(value, expected);
    }
    match recovered.into_file() {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            drop(file);
        }
        IntoFile::Refused { .. } => panic!("cold recovered store refused extraction"),
    }
    finished.unwrap();
    extracted.unwrap();
    returned.expect("closing admission must permit an accepted unused grant's return");
    assert!(matches!(repeated, Some(Err(Error::InvalidArgument { .. }))));
}
