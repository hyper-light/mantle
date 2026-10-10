//! A declared worker capacity bounds live file owners across downsize and later demand.
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
use mantle_engine::Error;
use mantle_engine::branch::{Op, filter};
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::pool::{self, Back, Job, Owner, Pool, Spawn, Stream, Work};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
// Two jobs can coexist, and one initial measured-demand seat remains after the first cascade.
const WORKERS: usize = 2;
const DEPTH: usize = 1;
// Existing native worker fixtures' failure guard. A passing run never waits for this deadline.
const WAIT: Duration = Duration::from_secs(5);

struct Files {
    live: AtomicUsize,
    refused: AtomicUsize,
    drops_open: AtomicBool,
    waiting: Mutex<Vec<std::thread::Thread>>,
}
impl Files {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            live: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
            drops_open: AtomicBool::new(true),
            waiting: Mutex::new(Vec::with_capacity(WORKERS * (DEPTH + 1))),
        })
    }
    fn release(&self) {
        self.drops_open.store(true, Ordering::SeqCst);
        for thread in self.waiting.lock().unwrap().iter() {
            thread.unpark();
        }
    }
}
// One setup lease shared by a worker's file and all its issuer duplicates. Its last Drop,
// rather than the pool's removal of a seat, returns the external file resource.
struct Lease(Arc<Files>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
struct File {
    file: DeviceFile,
    files: Arc<Files>,
    lease: Option<Arc<Lease>>,
}
impl Drop for File {
    fn drop(&mut self) {
        if self.lease.is_some() && !self.files.drops_open.load(Ordering::SeqCst) {
            // Register before rechecking so cleanup cannot miss a thread about to park.
            let mut waiting = self.files.waiting.lock().unwrap();
            assert!(waiting.len() < WORKERS * (DEPTH + 1));
            waiting.push(std::thread::current());
            drop(waiting);
            while !self.files.drops_open.load(Ordering::SeqCst) {
                std::thread::park();
            }
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
            files: Arc::clone(&self.files),
            lease: self.lease.as_ref().map(Arc::clone),
        })
    }
}
fn open(path: &Path, create: bool, files: &Arc<Files>, worker: bool) -> Result<File, Error> {
    let lease = if worker {
        if files.live.fetch_add(1, Ordering::SeqCst) >= WORKERS {
            files.live.fetch_sub(1, Ordering::SeqCst);
            files.refused.fetch_add(1, Ordering::SeqCst);
            return Err(Error::LimitExceeded {
                what: "live maintenance file owners",
                limit: u64::try_from(WORKERS).unwrap(),
            });
        }
        Some(Arc::new(Lease(Arc::clone(files))))
    } else {
        None
    };
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .map(|file| File {
        file,
        files: Arc::clone(files),
        lease,
    })
    .map_err(|error| Error::Io {
        op: "open a pool lifecycle file",
        detail: error.to_string(),
    })
}
struct OpenOnDrop {
    files: Arc<Files>,
    stop: mpsc::Sender<()>,
    watch: Option<std::thread::JoinHandle<()>>,
    expired: Arc<AtomicBool>,
}
impl OpenOnDrop {
    fn new(files: Arc<Files>) -> Self {
        let (stop, ended) = mpsc::channel();
        let expired = Arc::new(AtomicBool::new(false));
        let state = Arc::clone(&files);
        let timeout = Arc::clone(&expired);
        let watch = std::thread::spawn(move || {
            if ended.recv_timeout(WAIT).is_err() {
                timeout.store(true, Ordering::SeqCst);
                state.release();
            }
        });
        Self {
            files,
            stop,
            watch: Some(watch),
            expired,
        }
    }
}
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.files.release();
        let _ = self.stop.send(());
        if let Some(watch) = self.watch.take() {
            watch.join().unwrap();
        }
    }
}
fn job(store: &mut Store<File>, number: u8) -> Box<Job> {
    let (full, received) = mpsc::sync_channel(1);
    let mut bytes = Vec::new();
    let key = [number];
    pool::encode(
        &mut bytes,
        &key,
        Op::Put,
        &[number; 100],
        mantle_engine::maplet::hash32(filter::hash(&key)),
    )
    .unwrap();
    full.send(bytes).unwrap();
    drop(full);
    Box::new(Job {
        work: Work::Pack(Stream {
            entries: 1,
            full: received,
        }),
        grant: store
            .grant(usize::try_from(CONFIG.extent_pages).unwrap())
            .unwrap(),
        file_end: store.end(),
        generation: store.generation(),
    })
}
fn take(store: &mut Store<File>, back: Back) -> u8 {
    assert!(back.physical_error.is_none());
    let output = back.result.unwrap();
    store.extend_end(output.end);
    store.grant_back(&output.unused).unwrap();
    assert_eq!(output.parts.len(), 1);
    let branch = &output.parts[0].1;
    let mut value = Vec::new();
    let mut found = None;
    for number in 1..=4u8 {
        if branch.get(store, &[number], &mut value).unwrap() == Some(Op::Put) {
            assert_eq!(value, vec![number; 100]);
            assert!(found.replace(number).is_none());
        }
    }
    assert!(branch.get(store, b"absent", &mut value).unwrap().is_none());
    let number = found.unwrap();
    for extent in &branch.extents {
        store.release(*extent).unwrap();
    }
    number
}

#[test]
fn less_demand_does_not_overlap_replaced_worker_file_lifetimes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let files = Files::new();
    let mut store = Store::create(open(&path, true, &files, false).unwrap(), CONFIG).unwrap();
    let issuer = Issuer::start_for(&path, DEPTH, WORKERS * DEPTH).unwrap();
    let worker_files = Arc::clone(&files);
    let worker_path = path.clone();
    let spawn: Spawn = Box::new(move |seat| {
        let file = open(&worker_path, false, &worker_files, true)?;
        std::thread::Builder::new()
            .name(format!("lifecycle-worker-{}", seat.id))
            .spawn(move || pool::serve(file, CONFIG, seat))
            .map_err(|error| Error::Io {
                op: "start a lifecycle worker",
                detail: error.to_string(),
            })
    });
    let mut pool = Pool::new(spawn, WORKERS).unwrap();
    pool.set_attach(issuer.attacher(), DEPTH);
    // Sender-side owners stay occupied until each Done is taken, even if its worker is fast.
    for number in 1..=2 {
        assert!(
            pool.send(job(&mut store, number), Owner::Pack, true)
                .unwrap()
                .is_ok()
        );
    }
    let mut got = Vec::new();
    for _ in 0..WORKERS {
        let back = pool.take(&mut store, Owner::Pack, true).unwrap().unwrap();
        got.push(take(&mut store, back));
    }
    got.sort_unstable();
    assert_eq!(got, vec![1, 2]);
    assert_eq!(files.live.load(Ordering::SeqCst), WORKERS);
    let release = OpenOnDrop::new(Arc::clone(&files));
    files.drops_open.store(false, Ordering::SeqCst);
    // No prior cascade period exists: the fresh pool's existing one-seat need is unchanged,
    // without sleeping or fabricating a measured duration.
    pool.cascade_started();
    assert_eq!(pool.want(), 1);
    for number in 3..=4 {
        assert!(
            pool.send(job(&mut store, number), Owner::Pack, true)
                .unwrap()
                .is_ok(),
            "declared two-worker capacity remains usable without a third live file owner"
        );
    }
    for _ in 0..WORKERS {
        let back = pool.take(&mut store, Owner::Pack, true).unwrap().unwrap();
        got.push(take(&mut store, back));
    }
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3, 4]);
    assert_eq!(files.refused.load(Ordering::SeqCst), 0);
    assert!(!release.expired.load(Ordering::SeqCst));
    drop(release);
    drop(pool);
    assert_eq!(files.live.load(Ordering::SeqCst), 0);
    let (file, landed) = finished_file(store.into_file());
    landed.unwrap();
    drop(file);
    drop(issuer);
}
