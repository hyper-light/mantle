//! A store's drain after a failed run (`Store::drain`), over a real file whose writes the test
//! holds, fails or forbids by offset: every run still out is answered before it returns, the
//! first failure is the one reported, no queued run is handed to the device once the store has
//! failed, and each extent buffer's loan comes back exactly once. A maintenance worker's job ends
//! in this drain, and the shard reuses the job's extents once it has the result, so a run still
//! out when it returns could land on an extent written again.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_engine::Error;
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 64,
};

/// An offset no write is at.
const NOWHERE: u64 = u64::MAX;

/// What every duplicate of one file shares, leaked for the test's run: the offsets whose writes the
/// test controls, and what it saw of them.
struct Gate {
    /// A write here waits until `release`, then writes and sets `retired`.
    hold_at: AtomicU64,
    release: AtomicBool,
    retired: AtomicBool,
    /// A write here waits until `go`, then fails.
    fail_at: AtomicU64,
    go: AtomicBool,
    /// A write here sets `release` as it enters, then fails.
    releaser_at: AtomicU64,
    /// A write here must never come; `forbidden_written` says whether one did.
    forbidden_at: AtomicU64,
    forbidden_written: AtomicBool,
}

impl Gate {
    fn leaked() -> &'static Self {
        Box::leak(Box::new(Self {
            hold_at: AtomicU64::new(NOWHERE),
            release: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            fail_at: AtomicU64::new(NOWHERE),
            go: AtomicBool::new(false),
            releaser_at: AtomicU64::new(NOWHERE),
            forbidden_at: AtomicU64::new(NOWHERE),
            forbidden_written: AtomicBool::new(false),
        }))
    }
}

/// Opens every held or failing write on any exit, so the issuer's workers end before it joins
/// them.
struct Open(&'static Gate);

impl Drop for Open {
    fn drop(&mut self) {
        self.0.release.store(true, Ordering::SeqCst);
        self.0.go.store(true, Ordering::SeqCst);
    }
}

struct Gated {
    file: DeviceFile,
    gate: &'static Gate,
}

impl BlockFile for Gated {
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
        let gate = self.gate;
        if offset == gate.hold_at.load(Ordering::SeqCst) {
            while !gate.release.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            let written = self.file.write_all_at(buf, offset);
            gate.retired.store(true, Ordering::SeqCst);
            return written;
        }
        if offset == gate.fail_at.load(Ordering::SeqCst) {
            while !gate.go.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            return Err(refusal("the younger run, rejected by the test"));
        }
        if offset == gate.releaser_at.load(Ordering::SeqCst) {
            gate.release.store(true, Ordering::SeqCst);
            return Err(refusal("the third run, rejected by the test"));
        }
        if offset == gate.forbidden_at.load(Ordering::SeqCst) {
            gate.forbidden_written.store(true, Ordering::SeqCst);
        }
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: self.gate,
        })
    }
}

fn refusal(op: &'static str) -> DiskError {
    DiskError::Io {
        op,
        path: std::path::PathBuf::new(),
        source: std::io::Error::other(op),
    }
}

fn open(path: &Path, create: bool, gate: &'static Gate) -> Gated {
    Gated {
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        gate,
    }
}

/// The byte offset of page `address`.
fn at(address: u64) -> u64 {
    address * CONFIG.page_size as u64
}

/// A worker's store over its own handle on a shard's file, given `extents` for a job: as a
/// maintenance worker holds one ([`Store::worker`], [`Store::begin_job`]).
fn worker_job(
    path: &Path,
    gate: &'static Gate,
    extents: usize,
) -> (Store<Gated>, Store<Gated>, Vec<u64>) {
    let mut owner = Store::create(open(path, true, gate), CONFIG).unwrap();
    let grant = owner.grant(extents).unwrap();
    let mut worker = Store::worker(open(path, false, gate), CONFIG).unwrap();
    worker
        .begin_job(&grant, owner.end(), owner.generation())
        .unwrap();
    (owner, worker, grant)
}

/// The last page of a fresh extent of `store`'s: a page queued there ends its run, which is
/// handed to the issuer at once.
fn last_page(store: &mut Store<Gated>) -> u64 {
    let extent = store.allocate_extent().unwrap();
    store.address(extent, CONFIG.extent_pages - 1).unwrap()
}

#[test]
fn a_drain_after_a_failure_answers_every_run_still_out_and_reports_the_first() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let gate = Gate::leaked();
    let (mut owner, mut worker, grant) = worker_job(&path, gate, 3);
    // Two device workers: the older run holds one while the younger fails on the other, which
    // the issuer then hands the third run. The third's arrival there, after the younger's answer
    // was sent, releases the older: the younger's failure is the first answer the store can take.
    let issuer = Issuer::start_for(&path, 2, 3).unwrap();
    assert_eq!(issuer.depth(), 2, "this fixture needs two device workers");
    worker.attach(&issuer, 3).unwrap();
    let _open = Open(gate);
    let older = last_page(&mut worker);
    let younger = last_page(&mut worker);
    let third = last_page(&mut worker);
    gate.hold_at.store(at(older), Ordering::SeqCst);
    gate.fail_at.store(at(younger), Ordering::SeqCst);
    gate.releaser_at.store(at(third), Ordering::SeqCst);
    let mut run = worker.run().unwrap();
    worker.queue_page(&mut run, older, b"older run").unwrap();
    worker
        .queue_page(&mut run, younger, b"younger run")
        .unwrap();
    worker.queue_page(&mut run, third, b"third run").unwrap();
    // All three are out; only now may the younger fail, so no submission took its answer.
    gate.go.store(true, Ordering::SeqCst);

    let failure = worker.drain().unwrap_err();

    let io = worker.io_stats();
    assert_eq!(
        (io.submitted, io.runs_answered),
        (3, 3),
        "the drain returned with runs still out"
    );
    assert!(gate.retired.load(Ordering::SeqCst));
    assert!(
        matches!(&failure, Error::Io { detail, .. } if detail.contains("the younger run")),
        "the first failure taken is the one reported: {failure:?}"
    );
    // The run being filled is the only loan left; the two failed runs' buffers, which the
    // issuer dropped, are no longer counted out.
    assert_eq!(io.buffers_out, 1, "an extent buffer's loan is not back");
    worker.give_run(run);
    assert_eq!(worker.io_stats().buffers_out, 0);

    let end = worker.end();
    let (file, landed) = finished_file(worker.into_file());
    assert!(landed.is_err(), "the store failed: its handoff says so");
    drop(file);
    drop(issuer);
    // The older run landed whole: the shard's own handle reads it, checksum verified.
    owner.extend_end(end);
    let mut out = Vec::new();
    owner.read_page(older, &mut out).unwrap();
    assert_eq!(out, b"older run");
    for extent in grant {
        owner.release(extent).unwrap();
    }
    owner.checkpoint(None, 1).unwrap();
}

#[test]
fn a_failed_store_hands_no_queued_run_to_the_device() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let gate = Gate::leaked();
    let (_owner, mut worker, _grant) = worker_job(&path, gate, 2);
    // One batch out at a time, and write memory for one run more: the second run waits there.
    let issuer = Issuer::start_for(&path, 1, 1).unwrap();
    worker.attach(&issuer, 1).unwrap();
    worker.set_write_budget(worker.run_bytes());
    let _open = Open(gate);
    let failing = last_page(&mut worker);
    let queued = last_page(&mut worker);
    gate.fail_at.store(at(failing), Ordering::SeqCst);
    gate.forbidden_at.store(at(queued), Ordering::SeqCst);
    let mut run = worker.run().unwrap();
    worker
        .queue_page(&mut run, failing, b"failing run")
        .unwrap();
    worker.queue_page(&mut run, queued, b"queued run").unwrap();
    assert_eq!(worker.io_stats().runs_queued, 1);
    gate.go.store(true, Ordering::SeqCst);

    assert!(worker.drain().is_err());

    let io = worker.io_stats();
    assert_eq!((io.submitted, io.runs_answered), (1, 1));
    // The queued run's buffer is back in the pool, the failed run's loan ended with it.
    assert_eq!(io.buffers_out, 1, "an extent buffer's loan is not back");
    worker.give_run(run);
    assert_eq!(worker.io_stats().buffers_out, 0);
    let (file, landed) = finished_file(worker.into_file());
    assert!(landed.is_err());
    drop(file);
    drop(issuer);
    assert!(
        !gate.forbidden_written.load(Ordering::SeqCst),
        "a run queued before the failure was written after it"
    );
}
