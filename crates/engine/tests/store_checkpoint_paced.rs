//! A retained checkpoint waits for real map, superblock and full-flush callbacks while
//! another task on the same shard progresses. Canceling its borrowed wait resumes the
//! same barrier, and recovery reads the exact checkpoint that became durable.
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

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::combine::{Either, race2};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::{ChannelReceiver, Sender, channel};
use mantle_engine::Error;
use mantle_engine::store::{Config, Store, page};
#[path = "support/shared_sim.rs"]
mod shared_sim;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
// Existing native issuer fixtures' failure watchdog; it cannot satisfy the progress oracle.
const WATCH: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum Stage {
    Map,
    Superblock,
    Flush1,
    Flush2,
}

struct Gate {
    stage: Stage,
    armed: AtomicBool,
    entered: AtomicBool,
    canceled: AtomicBool,
    expired: AtomicBool,
    failed: bool,
    syncs: AtomicUsize,
    progress: AtomicUsize,
    noticed: Sender<()>,
    open: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }

    fn hold(&self) -> Result<(), DiskError> {
        if !self.entered.swap(true, Ordering::SeqCst) {
            self.noticed.try_send(()).unwrap();
        }
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
        if self.failed { Err(refused()) } else { Ok(()) }
    }
}

struct Watch {
    gate: Arc<Gate>,
    ended: std::sync::mpsc::SyncSender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Watch {
    fn new(gate: Arc<Gate>) -> Self {
        let (ended, receiver) = std::sync::mpsc::sync_channel(1);
        let watched = gate.clone();
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
}

fn refused() -> DiskError {
    DiskError::Io {
        op: "checkpoint callback refused by test",
        path: "checkpoint".into(),
        source: std::io::Error::other("checkpoint callback refused by test"),
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
        if self.gate.armed.load(Ordering::SeqCst) {
            for (index, bytes) in bytes.chunks(CONFIG.page_size).enumerate() {
                let address = at / CONFIG.page_size as u64 + index as u64;
                let header = page::verify(bytes, address).unwrap();
                if matches!(
                    (self.gate.stage, header.kind),
                    (Stage::Map, page::Kind::Map) | (Stage::Superblock, page::Kind::Superblock)
                ) {
                    self.gate.hold()?;
                }
            }
        }
        self.file.write_all_at(bytes, at)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        if self.gate.armed.load(Ordering::SeqCst) {
            let sync = self.gate.syncs.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(
                (self.gate.stage, sync),
                (Stage::Flush1, 1) | (Stage::Flush2, 2)
            ) {
                self.gate.hold()?;
            }
        }
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: self.gate.clone(),
        })
    }
}

fn runtime() -> LocalRuntime {
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        // The checkpoint owner and the task that releases its required I/O.
        tasks_per_shard: 2,
        pin: false,
        cores: Vec::new(),
        timers_per_shard: 2,
        interests_per_shard: 4,
        ring_entries: 2,
        batch: 2,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        spin_ns: 0,
        page_bytes: CONFIG.page_size,
        wake_tracking: None,
    })
    .unwrap()
}

async fn checkpoint_call(
    store: &mut Store<File>,
    root: Option<u64>,
    applied: u64,
) -> Result<(), Error> {
    store.checkpoint_async(root, applied).await
}

fn prepared(
    path: &Path,
    stage: Stage,
    failed: bool,
) -> (Store<File>, Arc<Gate>, ChannelReceiver<()>, u64) {
    let (noticed, notice) = channel(1).unwrap();
    let gate = Arc::new(Gate {
        stage,
        armed: AtomicBool::new(false),
        entered: AtomicBool::new(false),
        canceled: AtomicBool::new(false),
        expired: AtomicBool::new(false),
        failed,
        syncs: AtomicUsize::new(0),
        progress: AtomicUsize::new(0),
        noticed,
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let file = File {
        file: DeviceFile::open(
            path,
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        gate: gate.clone(),
    };
    let mut store = Store::create(file, CONFIG).unwrap();
    let old = store.allocate_extent().unwrap();
    let old_root = store.address(old, 0).unwrap();
    store.write_page(old_root, b"old checkpoint").unwrap();
    store.checkpoint(Some(old_root), 1).unwrap();
    let new = store.allocate_extent().unwrap();
    let root = store.address(new, 0).unwrap();
    store.write_page(root, b"new checkpoint").unwrap();
    store.release(old).unwrap();
    (store, gate, notice, root)
}

fn held_barrier(stage: Stage, failed: bool) {
    let directory = tempfile::tempdir().unwrap();
    let (mut store, gate, mut notice, root) =
        prepared(&directory.path().join("store"), stage, failed);
    let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    store.set_write_budget(0);
    // An independently owned span remains live through a failed empty flush; its loan
    // must not be mistaken for a nonexistent flush buffer.
    let span = store.span().unwrap();
    let watch = Watch::new(gate.clone());
    gate.armed.store(true, Ordering::SeqCst);
    let mut runtime = runtime();
    let opener = gate.clone();
    runtime
        .spawn(async move {
            while !opener.canceled.load(Ordering::SeqCst) && !opener.expired.load(Ordering::SeqCst)
            {
                hyper_rt::futures::yield_now().await;
            }
            if !opener.expired.load(Ordering::SeqCst) {
                opener.progress.fetch_add(1, Ordering::SeqCst);
                opener.release();
            }
        })
        .unwrap();
    let observed = gate.clone();
    let (mut store, result) = runtime
        .block_on(async move {
            let first = {
                let mut checkpoint = std::pin::pin!(checkpoint_call(&mut store, Some(root), 2));
                race2(checkpoint.as_mut(), notice.recv()).await
            };
            assert!(
                matches!(first, Either::Second(Ok(()))),
                "required I/O was not suspended: {stage:?}"
            );
            assert!(
                !observed.expired.load(Ordering::SeqCst),
                "held checkpoint blocked its shard"
            );
            {
                let mut resumed = std::pin::pin!(checkpoint_call(&mut store, Some(root), 2));
                std::future::poll_fn(|cx| {
                    assert!(std::future::Future::poll(resumed.as_mut(), cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                let mut foreign = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(matches!(
                    std::future::Future::poll(resumed.as_mut(), &mut foreign),
                    std::task::Poll::Ready(Err(Error::InvalidArgument { .. }))
                ));
            }
            assert_eq!(store.generation(), 2); // The previously acknowledged checkpoint only.
            let refs = store.refs().to_vec();
            assert!(matches!(
                checkpoint_call(&mut store, Some(root), 3).await,
                Err(Error::InvalidArgument { .. })
            ));
            assert!(matches!(
                store.allocate_extent(),
                Err(Error::InvalidArgument { .. })
            ));
            assert!(matches!(
                store.release(root / u64::from(CONFIG.extent_pages)),
                Err(Error::InvalidArgument { .. })
            ));
            assert!(matches!(
                store.write_page(root, b"replayed mutation"),
                Err(Error::InvalidArgument { .. })
            ));
            assert_eq!(store.refs(), refs);
            observed.canceled.store(true, Ordering::SeqCst);
            let result = checkpoint_call(&mut store, Some(root), 2).await;
            assert_eq!(
                store.io_stats().buffers_out,
                1,
                "the separately owned span still holds its one loan"
            );
            store.give_span(span);
            (store, result)
        })
        .unwrap();
    assert!(!gate.expired.load(Ordering::SeqCst));
    assert!(gate.entered.load(Ordering::SeqCst));
    assert_eq!(gate.progress.load(Ordering::SeqCst), 1);
    assert_eq!(store.io_stats().buffers_out, 0);
    if failed {
        assert!(matches!(result, Err(Error::Io { .. })), "{result:?}");
        let refs = store.refs().to_vec();
        let generation = store.generation();
        assert!(matches!(
            store.allocate_extent(),
            Err(Error::InvalidArgument { .. })
        ));
        assert!(matches!(store.grant(1), Err(Error::InvalidArgument { .. })));
        assert!(matches!(
            store.grant_back(&[]),
            Err(Error::InvalidArgument { .. })
        ));
        assert!(matches!(
            store.begin_job(&[], 0, 0),
            Err(Error::InvalidArgument { .. })
        ));
        assert_eq!(store.refs(), refs);
        assert_eq!(store.generation(), generation);
        assert!(matches!(
            store.write_page(root, b"after failure"),
            Err(Error::Io { .. })
        ));
    } else {
        result.unwrap();
        assert_eq!(store.generation(), 3);
    }
    gate.armed.store(false, Ordering::SeqCst);
    drop(watch);
    let (file, landed) = finished_file(store.into_file());
    if !failed {
        landed.unwrap();
    }
    drop(issuer);
    let (mut recovered, checkpoint) = Store::open(file, CONFIG).unwrap();
    assert!(checkpoint.applied == 1 || checkpoint.applied == 2);
    if !failed {
        assert_eq!(checkpoint.applied, 2);
    }
    let mut value = Vec::new();
    recovered
        .read_page(checkpoint.root.unwrap(), &mut value)
        .unwrap();
    assert_eq!(
        value,
        if checkpoint.applied == 1 {
            b"old checkpoint".as_slice()
        } else {
            b"new checkpoint".as_slice()
        }
    );
    let mut refs = recovered
        .refs()
        .iter()
        .enumerate()
        .filter(|(_, count)| **count != 0)
        .map(|(index, _)| index as u64)
        .collect::<Vec<_>>();
    let mut named = vec![0, checkpoint.root.unwrap() / u64::from(CONFIG.extent_pages)];
    named.extend_from_slice(recovered.map_extents());
    refs.sort_unstable();
    named.sort_unstable();
    assert_eq!(refs, named);
}

#[test]
fn held_metadata_and_flushes_resume_after_borrowed_cancellation() {
    for stage in [Stage::Map, Stage::Superblock, Stage::Flush1, Stage::Flush2] {
        held_barrier(stage, false);
    }
}

#[test]
fn each_required_metadata_or_flush_failure_fences_then_recovers_exact_data() {
    for stage in [Stage::Map, Stage::Superblock, Stage::Flush1, Stage::Flush2] {
        held_barrier(stage, true);
    }
}

#[test]
fn an_unattached_async_checkpoint_refuses_before_allocator_changes() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _, _, root) = prepared(&directory.path().join("store"), Stage::Map, false);
    let before = store.refs().to_vec();
    let generation = store.generation();
    let mut runtime = runtime();
    let store = runtime
        .block_on(async move {
            let mut store = store;
            assert!(matches!(
                store.checkpoint_async(Some(root), 2).await,
                Err(Error::InvalidArgument { .. })
            ));
            store
        })
        .unwrap();
    assert_eq!(store.generation(), generation);
    assert_eq!(store.refs(), before);
}

#[test]
fn every_async_checkpoint_crash_cut_recovers_exact_pages_and_references() {
    use hyper_block::sim::{Crash, Fault};
    use shared_sim::SharedSim;
    fn prepared() -> (Store<SharedSim>, SharedSim, u64) {
        let file = SharedSim::new(
            Alignment::new(CONFIG.page_size).unwrap(),
            Alignment::new(512).unwrap(),
            7,
        )
        .unwrap();
        let observed = file.try_clone().unwrap();
        let mut store = Store::create(file, CONFIG).unwrap();
        let old = store.allocate_extent().unwrap();
        let address = store.address(old, 0).unwrap();
        store.write_page(address, b"before barrier").unwrap();
        store.checkpoint(Some(address), 1).unwrap();
        let new = store.allocate_extent().unwrap();
        let address = store.address(new, 0).unwrap();
        store.write_page(address, b"after barrier").unwrap();
        store.release(old).unwrap();
        (store, observed, address)
    }
    fn run(cut: Option<u64>, mode: Crash) -> u64 {
        let directory = tempfile::tempdir().unwrap();
        let (mut store, observed, actual) = prepared();
        let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
        store.attach(&issuer, 1).unwrap();
        let before = observed.stats().unwrap();
        if let Some(cut) = cut {
            observed.inject(Fault::PowerCut { ops: cut }).unwrap();
        }
        let mut runtime = runtime();
        let (store, completed) = runtime
            .block_on(async move {
                let result = store.checkpoint_async(Some(actual), 2).await;
                (store, result.is_ok())
            })
            .unwrap();
        let file = finished_file(store.into_file()).0;
        drop(issuer);
        let after = observed.stats().unwrap();
        let cuts = (after.writes - before.writes) + (after.syncs - before.syncs);
        file.crash(mode).unwrap();
        file.clear_faults().unwrap();
        let (mut recovered, checkpoint) = Store::open(file, CONFIG).unwrap();
        assert!(checkpoint.applied == 1 || checkpoint.applied == 2);
        if completed {
            assert_eq!(checkpoint.applied, 2);
        }
        let mut value = Vec::new();
        recovered
            .read_page(checkpoint.root.unwrap(), &mut value)
            .unwrap();
        assert_eq!(
            value,
            if checkpoint.applied == 1 {
                b"before barrier".as_slice()
            } else {
                b"after barrier".as_slice()
            }
        );
        let held = recovered
            .refs()
            .iter()
            .enumerate()
            .filter(|(_, count)| **count != 0)
            .map(|(index, _)| index as u64)
            .collect::<Vec<_>>();
        let mut named = vec![0, checkpoint.root.unwrap() / u64::from(CONFIG.extent_pages)];
        named.extend_from_slice(recovered.map_extents());
        named.sort_unstable();
        assert_eq!(held, named);
        assert!(recovered.refs().iter().all(|count| *count <= 1));
        cuts
    }
    let cuts = run(None, Crash::KeepAll);
    assert!(cuts > 0);
    eprintln!(
        "async checkpoint measured {cuts} write/flush boundaries; testing each with LoseAll, KeepAll and Random"
    );
    for cut in 0..cuts {
        for mode in [Crash::LoseAll, Crash::KeepAll, Crash::Random] {
            run(Some(cut), mode);
        }
    }
}
