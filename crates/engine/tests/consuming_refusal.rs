//! Historical adapter observes the old tuple honestly; no borrowed async API is emulated.
//! A failure-only guard opens actual held I/O for cleanup and cannot satisfy refusal.
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
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::{Sender, channel};
use mantle_engine::Error;
use mantle_engine::store::{Config, Store};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
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

// Preserve the same inline ownership observation for the old and corrected APIs.
#[allow(clippy::large_enum_variant)]
enum Observed {
    Finished {
        file: File,
        result: Result<(), Error>,
    },
    Refused {
        owner: Store<File>,
        error: Error,
    },
}

fn extract(store: Store<File>) -> Observed {
    match store.into_file() {
        mantle_engine::store::IntoFile::Finished { file, result } => {
            Observed::Finished { file, result }
        }
        mantle_engine::store::IntoFile::Refused { owner, error } => {
            Observed::Refused { owner, error }
        }
    }
}

#[test]
fn consuming_refusal_returns_the_unfinished_store_owner() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let (gate, mut observed) = gate(false);
    let mut store = Store::create(file(&path, true, false, Arc::clone(&gate)), CONFIG).unwrap();
    let old = store.allocate_extent().unwrap();
    let root = store.address(old, 0).unwrap();
    store.write_page(root, b"durable owner witness").unwrap();
    store.checkpoint(Some(root), 1).unwrap();
    let next = store.allocate_extent().unwrap();
    let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let watch = Watch::new(Arc::clone(&gate));
    gate.armed.store(true, Ordering::SeqCst);
    let mut run = store.run().unwrap();
    for page in 0..CONFIG.extent_pages {
        store
            .queue_page(
                &mut run,
                store.address(next, page).unwrap(),
                b"accepted physical write",
            )
            .unwrap();
    }
    store.give_run(run);
    observed.blocking_recv().unwrap();
    assert!(gate.entered.load(Ordering::SeqCst));
    let generation = store.generation();
    let refs = store.refs().to_vec();
    let answered = store.io_stats().runs_answered;
    let copies = gate.copies.load(Ordering::SeqCst);
    let mut rt = runtime();
    let outcome = rt.block_on(async move { extract(store) }).unwrap();
    let expired = gate.expired.load(Ordering::SeqCst);
    // Main is the cold owner again. Open before every possible native cleanup/assertion.
    gate.release();
    let (refused, unchanged, typed) = match outcome {
        Observed::Refused { owner, error } => {
            let unchanged = owner.generation() == generation
                && owner.refs() == refs
                && owner.io_stats().runs_answered == answered
                && gate.copies.load(Ordering::SeqCst) == copies;
            let typed = matches!(error, Error::InvalidArgument { .. });
            match extract(owner) {
                Observed::Finished { file, result } => {
                    result.unwrap();
                    drop(file);
                }
                Observed::Refused { .. } => panic!("cold cleanup refused original owner"),
            }
            (true, unchanged, typed)
        }
        Observed::Finished { file, result } => {
            eprintln!(
                "old consuming call returned Finished: {result:?}; failure_guard_expired={expired}"
            );
            drop(file);
            (false, false, false)
        }
    };
    drop(rt);
    drop(watch);
    // The old consuming owner may already be gone through nonblocking task Drop.
    // This cold issuer join closes every duplicate before recovery and the verdict.
    drop(issuer);
    gate.armed.store(false, Ordering::SeqCst);
    let (mut recovered, checkpoint) =
        Store::open(file(&path, false, false, Arc::clone(&gate)), CONFIG).unwrap();
    assert_eq!(checkpoint.root, Some(root));
    assert_eq!(checkpoint.applied, 1);
    let mut value = Vec::new();
    recovered.read_page(root, &mut value).unwrap();
    assert_eq!(value, b"durable owner witness");
    match extract(recovered) {
        Observed::Finished { file, result } => {
            result.unwrap();
            drop(file);
        }
        Observed::Refused { .. } => panic!("cold recovered owner refused"),
    }
    assert_eq!(gate.copies.load(Ordering::SeqCst), 0);
    assert!(
        refused,
        "consuming refusal returned a file instead of the original Store owner"
    );
    assert!(typed, "consuming refusal lost the typed context error");
    assert!(unchanged, "refused owner was mutated or physically retired");
    assert!(!expired, "failure-only guard cannot satisfy refusal");
}
