//! Public cold startup refusal and ownership of a job refused before admission.
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
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::branch::{Op, filter};
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::TrunkConfig;
use mantle_engine::trunk::pool::{self, Job, Owner, Pool, Spawn, Stream, Work};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};

fn file(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}
fn runtime() -> Runtime {
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 50_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: CONFIG.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}
fn job(store: &mut Store<DeviceFile>) -> Box<Job> {
    let (send, full) = mpsc::sync_channel(1);
    let mut buf = Vec::new();
    pool::encode(
        &mut buf,
        b"returned-job",
        Op::Put,
        b"input still owned",
        mantle_engine::maplet::hash32(filter::hash(b"returned-job")),
    )
    .unwrap();
    send.send(buf).unwrap();
    drop(send);
    Box::new(Job {
        work: Work::Pack(Stream { entries: 1, full }),
        grant: store
            .grant(usize::try_from(CONFIG.extent_pages).unwrap())
            .unwrap(),
        file_end: store.end(),
        generation: store.generation(),
    })
}
fn spawn(path: &Path, starts: Arc<AtomicUsize>) -> Spawn {
    let path = path.to_path_buf();
    Box::new(move |seat| {
        starts.fetch_add(1, Ordering::SeqCst);
        let native = file(&path, false);
        std::thread::Builder::new()
            .name("startup-oracle-worker".into())
            .spawn(move || pool::serve(native, CONFIG, seat))
            .map_err(|error| Error::Io {
                op: "start the public startup oracle worker",
                detail: error.to_string(),
            })
    })
}
fn consume(store: &mut Store<DeviceFile>, pool: &mut Pool, returned: Box<Job>) {
    assert!(pool.send(returned, Owner::Pack, true).unwrap().is_ok());
    let back = pool.take(store, Owner::Pack, true).unwrap().unwrap();
    assert!(back.physical_error.is_none());
    let output = back.result.unwrap();
    store.extend_end(output.end);
    store.grant_back(&output.unused).unwrap();
    assert!(back.topped.is_empty());
    assert_eq!(output.parts.len(), 1);
    let branch = &output.parts[0].1;
    let mut out = Vec::new();
    assert_eq!(
        branch.get(store, b"returned-job", &mut out).unwrap(),
        Some(Op::Put)
    );
    assert_eq!(out, b"input still owned");
    for extent in &branch.extents {
        store.release(*extent).unwrap();
    }
}

#[test]
fn startup_refusal_returns_the_job_with_its_stream_and_grant_usable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    let mut refused = Pool::new(
        Box::new(|_| {
            Err(Error::Io {
                op: "public cold worker startup refused",
                detail: "injected before any job".into(),
            })
        }),
        1,
    )
    .unwrap();
    let (error, returned) = refused
        .send(job(&mut store), Owner::Pack, true)
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Io {
            op: "public cold worker startup refused",
            ..
        }
    ));
    drop(refused);
    let starts = Arc::new(AtomicUsize::new(0));
    let mut healthy = Pool::new(spawn(&path, Arc::clone(&starts)), 1).unwrap();
    consume(&mut store, &mut healthy, returned);
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

#[test]
fn unprepared_runtime_send_refuses_before_starting_and_returns_the_usable_job() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    let input = job(&mut store);
    let starts = Arc::new(AtomicUsize::new(0));
    let mut pool = Pool::new(spawn(&path, Arc::clone(&starts)), 1).unwrap();
    let runtime = runtime();
    let (done, answer) = mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let (error, returned) = pool.send(input, Owner::Pack, true).unwrap_err();
            assert!(matches!(error, Error::InvalidArgument { .. }));
            done.send((pool, returned)).unwrap();
        })
        .unwrap();
    let (mut pool, returned) = answer.recv().unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    runtime.shutdown().unwrap();
    consume(&mut store, &mut pool, returned);
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

struct Facts {
    clones: AtomicUsize,
    failed: AtomicUsize,
    worker_clone_failure: AtomicBool,
}
struct Tracked {
    native: DeviceFile,
    facts: Arc<Facts>,
    worker: bool,
}
impl BlockFile for Tracked {
    fn alignment(&self) -> Alignment {
        self.native.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.native.len()
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.native.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.native.write_all_at(buf, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.native.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        self.facts.clones.fetch_add(1, Ordering::SeqCst);
        if self.worker && self.facts.worker_clone_failure.load(Ordering::SeqCst) {
            self.facts.failed.fetch_add(1, Ordering::SeqCst);
            return Err(DiskError::Io {
                op: "public worker attachment refused",
                path: Default::default(),
                source: std::io::Error::other("injected worker file duplication failure"),
            });
        }
        Ok(Self {
            native: self.native.try_clone()?,
            facts: Arc::clone(&self.facts),
            worker: self.worker,
        })
    }
}
fn tracked(path: &Path, create: bool, facts: Arc<Facts>, worker: bool) -> Tracked {
    Tracked {
        native: file(path, create),
        facts,
        worker,
    }
}
fn facts(fail: bool) -> Arc<Facts> {
    Arc::new(Facts {
        clones: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
        worker_clone_failure: AtomicBool::new(fail),
    })
}
fn backend(db: &mut ShardDb<Tracked>, issuer: &Issuer, path: &Path, facts: Arc<Facts>) {
    db.attach(issuer, 1).unwrap();
    let path = path.to_path_buf();
    db.set_workers(move || Ok(tracked(&path, false, Arc::clone(&facts), true)))
        .unwrap();
}

#[test]
fn a_worker_attachment_startup_error_refuses_range_placement_and_preserves_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let facts = facts(true);
    let mut db = ShardDb::create(
        tracked(&path, true, Arc::clone(&facts), false),
        CONFIG,
        CONFIG.page_size,
        TRUNK,
    )
    .unwrap();
    db.put(b"durable", b"before startup refusal").unwrap();
    db.checkpoint(1).unwrap();
    let issuer =
        Issuer::start_for(dir.path(), 1, mantle_engine::shard_db::issuer_batches(1, 1)).unwrap();
    backend(&mut db, &issuer, &path, Arc::clone(&facts));
    let mut runtime = runtime();
    assert!(matches!(
        Ranges::start(
            &mut runtime,
            vec![(Vec::new(), db)],
            RangesConfig {
                clients: 1,
                slice_ns: 50_000,
                spin_ns: 0,
            }
        ),
        Err(mantle_engine::ranges::StartError::Failed(Error::Io { .. }))
    ));
    assert!(
        facts.failed.load(Ordering::SeqCst) > 0,
        "the actual worker file attachment refused"
    );
    runtime.shutdown().unwrap();
    drop(issuer);
    let (mut reopened, applied) =
        ShardDb::open(file(&path, false), CONFIG, CONFIG.page_size, TRUNK).unwrap();
    assert_eq!(applied, 1);
    let mut out = Vec::new();
    assert!(reopened.get(b"durable", &mut out).unwrap());
    assert_eq!(out, b"before startup refusal");
    reopened.check_references().unwrap();
}

#[test]
fn late_attachment_refuses_before_cloning_or_changing_a_warm_engine() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let facts = facts(false);
    let mut db = ShardDb::create(
        tracked(&path, true, Arc::clone(&facts), false),
        CONFIG,
        CONFIG.page_size,
        TRUNK,
    )
    .unwrap();
    let issuer =
        Issuer::start_for(dir.path(), 1, mantle_engine::shard_db::issuer_batches(1, 1)).unwrap();
    backend(&mut db, &issuer, &path, Arc::clone(&facts));
    db.put(b"kept", b"before warm attachment refusal").unwrap();
    db.flush().unwrap();
    assert!(db.workers().is_some_and(|(warm, _)| warm > 0));
    let before = (
        db.memory_split(),
        db.memory_held(),
        db.write_budget(),
        db.workers(),
        facts.clones.load(Ordering::SeqCst),
    );
    assert!(matches!(
        db.attach(&issuer, 1),
        Err(Error::InvalidArgument { .. })
    ));
    assert_eq!(
        (
            db.memory_split(),
            db.memory_held(),
            db.write_budget(),
            db.workers(),
            facts.clones.load(Ordering::SeqCst)
        ),
        before
    );
    let mut out = Vec::new();
    assert!(db.get(b"kept", &mut out).unwrap());
    assert_eq!(out, b"before warm attachment refusal");
    db.checkpoint(2).unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    drop(issuer);
    let (mut reopened, applied) =
        ShardDb::open(self::file(&path, false), CONFIG, CONFIG.page_size, TRUNK).unwrap();
    assert_eq!(applied, 2);
    assert!(reopened.get(b"kept", &mut out).unwrap());
    assert_eq!(out, b"before warm attachment refusal");
    reopened.check_references().unwrap();
}
