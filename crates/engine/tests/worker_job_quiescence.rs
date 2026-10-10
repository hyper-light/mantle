//! A worker cannot replace an old grant or clear its write failure before physical retirement.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]
use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_engine::Error;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Builder, Op};
use mantle_engine::scan::ScanMerge;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::Source;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};

// The existing store_drain real-device fixture's format, not a raised I/O or memory bound.
const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 64,
};
const DEPTH: usize = 2;
const YOUNGER: &str = "the younger worker job write refused before retirement";
#[derive(Debug, PartialEq, Eq)]
enum Notice {
    Held,
    Failed,
}
struct Gate {
    held: AtomicU64,
    failed: AtomicU64,
    retired: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
    notices: mpsc::SyncSender<Notice>,
}
impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
}
struct OpenOnDrop(Arc<Gate>);
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct File {
    file: DeviceFile,
    gate: Arc<Gate>,
}
impl BlockFile for File {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        if offset == self.gate.held.load(Ordering::SeqCst) {
            self.gate.notices.send(Notice::Held).unwrap();
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.changed.wait(released).unwrap();
            }
            drop(released);
            let result = self.file.write_all_at(buf, offset);
            self.gate.retired.store(true, Ordering::SeqCst);
            return result;
        }
        if offset == self.gate.failed.load(Ordering::SeqCst) {
            self.gate.notices.send(Notice::Failed).unwrap();
            return Err(DiskError::Io {
                op: "worker job failure oracle",
                path: Path::new("").to_path_buf(),
                source: std::io::Error::other(YOUNGER),
            });
        }
        self.file.write_all_at(buf, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: Arc::clone(&self.gate),
        })
    }
}
fn open(path: &Path, create: bool, gate: &Arc<Gate>) -> File {
    File {
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        gate: Arc::clone(gate),
    }
}
fn physical(error: &Error) {
    assert!(
        matches!(error, Error::Io { detail, .. } if detail.contains(YOUNGER)),
        "{error:?}"
    );
}
#[test]
fn a_worker_refuses_a_new_job_until_an_old_held_write_retires_then_reuses_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker-job");
    // Exactly two selected physical callbacks publish at most one notice apiece.
    let (notices, events) = mpsc::sync_channel(DEPTH);
    let gate = Arc::new(Gate {
        held: AtomicU64::new(u64::MAX),
        failed: AtomicU64::new(u64::MAX),
        retired: AtomicBool::new(false),
        released: Mutex::new(false),
        changed: Condvar::new(),
        notices,
    });
    let mut owner = Store::create(open(&path, true, &gate), CONFIG).unwrap();
    // Two output extents and an unused old extent make replacement of the grant observable.
    let old = owner.grant(DEPTH + 1).unwrap();
    let next = owner.grant(DEPTH).unwrap();
    let mut worker = Store::worker(open(&path, false, &gate), CONFIG).unwrap();
    worker
        .begin_job(&old, owner.end(), owner.generation())
        .unwrap();
    let issuer = Issuer::start_for(&path, DEPTH, DEPTH).unwrap();
    assert_eq!(
        issuer.depth(),
        DEPTH,
        "actual held-plus-failed device capability"
    );
    worker.attach(&issuer, DEPTH).unwrap();
    // The guard opens the physical callback before any Store/Issuer teardown on failure.
    let _open = OpenOnDrop(Arc::clone(&gate));
    let older = worker.allocate_extent().unwrap();
    let younger = worker.allocate_extent().unwrap();
    let old_page = worker.address(older, CONFIG.extent_pages - 1).unwrap();
    let young_page = worker.address(younger, CONFIG.extent_pages - 1).unwrap();
    gate.held
        .store(old_page * CONFIG.page_size as u64, Ordering::SeqCst);
    gate.failed
        .store(young_page * CONFIG.page_size as u64, Ordering::SeqCst);
    let mut run = worker.run().unwrap();
    worker
        .queue_page(&mut run, old_page, b"held old output")
        .unwrap();
    worker
        .queue_page(&mut run, young_page, b"failed younger output")
        .unwrap();
    let mut seen = Vec::new();
    for _ in 0..DEPTH {
        seen.push(events.recv().unwrap());
    }
    assert!(seen.contains(&Notice::Held) && seen.contains(&Notice::Failed));
    // A demand on the failed page consumes its actual numbered completion while the old
    // callback remains held; no sleep or repeated speculative readiness polling is needed.
    let mut out = Vec::new();
    let first = worker.read_page(young_page, &mut out).unwrap_err();
    physical(&first);
    assert!(!gate.retired.load(Ordering::SeqCst));
    let before = (worker.end(), worker.generation());
    let refusal = worker.begin_job(&next, before.0 + CONFIG.page_size as u64, before.1 + 1);
    assert!(
        matches!(refusal, Err(Error::InvalidArgument { .. })),
        "{refusal:?}"
    );
    assert_eq!((worker.end(), worker.generation()), before);
    let spare = worker.allocate_extent().unwrap();
    assert!(
        old.contains(&spare) && !next.contains(&spare),
        "old grant survives refusal"
    );
    assert!(!gate.retired.load(Ordering::SeqCst));
    gate.release();
    physical(&worker.drain().unwrap_err());
    assert!(gate.retired.load(Ordering::SeqCst));
    physical(&worker.drain().unwrap_err());
    worker.give_run(run);
    owner.extend_end(worker.end());
    for extent in old {
        owner.release(extent).unwrap();
    }
    worker
        .begin_job(&next, owner.end(), owner.generation())
        .unwrap();
    let values = [
        (b"".as_slice(), vec![]),
        (b"prefix/a".as_slice(), vec![3; 33]),
        (b"prefix/b".as_slice(), vec![7; CONFIG.page_size / 3]),
    ];
    let mut builder = Builder::new(&mut worker, Keys::Exactly(values.len() as u64)).unwrap();
    for (key, value) in &values {
        builder.add(&mut worker, key, Op::Put, value).unwrap();
    }
    let branch = builder.finish(&mut worker).unwrap();
    worker.drain().unwrap();
    owner.extend_end(worker.end());
    owner.grant_back(&worker.unused_grant()).unwrap();
    let sources = [Source::Branch(&branch)];
    let mut scan = ScanMerge::new();
    scan.open(&mut owner, &sources, b"", None, false, false)
        .unwrap();
    for (key, value) in &values {
        assert_eq!(scan.entry(), Some((*key, Op::Put, value.as_slice())));
        scan.next(&mut owner).unwrap();
    }
    assert!(scan.entry().is_none());
    scan.close(&mut owner);
    worker.drain().unwrap();
}
