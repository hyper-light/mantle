//! A native worker's attachment must report its own lifecycle failure through
//! public ShardDb terminal completion. The owner attachment remains healthy.
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
use mantle_engine::Error;
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::{Config, IntoFile};
use mantle_engine::trunk::TrunkConfig;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 2,
    max_extents: 256,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};

#[derive(Default)]
struct Observed {
    files: AtomicUsize,
    worker_writes: AtomicUsize,
    worker_drop_faults: AtomicUsize,
}

struct File {
    file: DeviceFile,
    seen: Arc<Observed>,
    worker: bool,
    duplicate: bool,
}

impl Drop for File {
    fn drop(&mut self) {
        self.seen.files.fetch_sub(1, Ordering::SeqCst);
        if self.worker && self.duplicate {
            self.seen.worker_drop_faults.fetch_add(1, Ordering::SeqCst);
            eprintln!("actual native worker-attachment duplicate Drop fault");
            panic!("worker attachment duplicate Drop witness");
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
        self.file.write_all_at(bytes, at)?;
        if self.worker && self.duplicate {
            self.seen.worker_writes.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        let file = self.file.try_clone()?;
        self.seen.files.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            file,
            seen: Arc::clone(&self.seen),
            worker: self.worker,
            duplicate: true,
        })
    }
}

fn open(path: &Path, create: bool, worker: bool, seen: Arc<Observed>) -> Result<File, Error> {
    let file = DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .map_err(|error| Error::Io {
        op: "open worker lifecycle fixture",
        detail: error.to_string(),
    })?;
    seen.files.fetch_add(1, Ordering::SeqCst);
    Ok(File {
        file,
        seen,
        worker,
        duplicate: false,
    })
}

fn check_saved(db: &mut ShardDb<File>) {
    let mut value = Vec::new();
    assert!(db.get(b"durable", &mut value).unwrap());
    assert_eq!(value, b"old durable value");
    assert!(!db.get(b"worker-output", &mut value).unwrap());
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(!db.scan(b"", None, 2, &mut rows, &mut next).unwrap());
    let actual: Vec<_> = rows
        .iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    assert_eq!(
        actual,
        vec![(b"durable".to_vec(), b"old durable value".to_vec())]
    );
    db.check_references().unwrap();
}

#[test]
fn terminal_finish_reports_worker_attachment_drop_failure_after_exact_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("worker-lifecycle");
    let seen = Arc::new(Observed::default());
    let mut db = ShardDb::create(
        open(&path, true, false, Arc::clone(&seen)).unwrap(),
        CONFIG,
        CONFIG.page_size,
        TRUNK,
    )
    .unwrap();
    db.put(b"durable", b"old durable value").unwrap();
    db.checkpoint(1).unwrap();
    let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    db.attach(&issuer, 1).unwrap();
    let worker_path = path.clone();
    let worker_seen = Arc::clone(&seen);
    db.set_workers(move || open(&worker_path, false, true, Arc::clone(&worker_seen)))
        .unwrap();
    let value = vec![7; CONFIG.page_size / 2];
    db.put(b"worker-output", &value).unwrap();
    db.flush().unwrap();
    let mut actual = Vec::new();
    assert!(db.get(b"worker-output", &mut actual).unwrap());
    assert_eq!(actual, value);
    db.check_references().unwrap();
    assert!(
        seen.worker_writes.load(Ordering::SeqCst) > 0,
        "a real worker attachment must have completed native output before retirement"
    );
    assert_eq!(seen.worker_drop_faults.load(Ordering::SeqCst), 0);
    eprintln!("actual native worker writes completed before terminal cleanup");
    let result = match db.into_file() {
        IntoFile::Finished { file, result } => {
            drop(file);
            result
        }
        IntoFile::Refused { owner, error } => {
            drop(owner);
            panic!("cold fixture refused ownership handoff: {error:?}");
        }
    };
    drop(issuer);
    assert!(seen.worker_drop_faults.load(Ordering::SeqCst) > 0);
    assert_eq!(seen.files.load(Ordering::SeqCst), 0);
    let (mut recovered, applied) = ShardDb::open(
        open(&path, false, false, Arc::clone(&seen)).unwrap(),
        CONFIG,
        CONFIG.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(applied, 1);
    check_saved(&mut recovered);
    match recovered.into_file() {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            drop(file);
        }
        IntoFile::Refused { owner, error } => {
            drop(owner);
            panic!("recovered cold fixture refused handoff: {error:?}");
        }
    }
    assert_eq!(seen.files.load(Ordering::SeqCst), 0);
    assert!(
        matches!(&result, Err(Error::Io { detail, .. }) if detail.contains("Drop")),
        "worker-specific native duplicate Drop failure must reach terminal completion: {result:?}"
    );
}

// Append to the same test file; the cold case and its File wrapper stay unchanged.
// Thread/TLS observation is installed through an actual public native-worker clone callback.
#[derive(Default)]
struct ActorObserved {
    files: Arc<Observed>,
    workers_started: AtomicUsize,
    worker_tls_ended: AtomicUsize,
}
struct WorkerTls(std::cell::RefCell<Option<Arc<ActorObserved>>>);
impl Drop for WorkerTls {
    fn drop(&mut self) {
        if let Some(seen) = self.0.get_mut().take() {
            seen.worker_tls_ended.fetch_add(1, Ordering::SeqCst);
        }
    }
}
std::thread_local! {
    static WORKER_TLS: WorkerTls = const { WorkerTls(std::cell::RefCell::new(None)) };
}
struct ActorFile {
    file: File,
    seen: Arc<ActorObserved>,
}
impl BlockFile for ActorFile {
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
        if self.file.worker && !self.file.duplicate {
            WORKER_TLS.with(|tls| {
                let mut state = tls.0.borrow_mut();
                if state.is_none() {
                    self.seen.workers_started.fetch_add(1, Ordering::SeqCst);
                    *state = Some(Arc::clone(&self.seen));
                }
            });
        }
        Ok(Self {
            file: self.file.try_clone()?,
            seen: Arc::clone(&self.seen),
        })
    }
}
fn actor_file(
    path: &Path,
    create: bool,
    worker: bool,
    seen: Arc<ActorObserved>,
) -> Result<ActorFile, Error> {
    Ok(ActorFile {
        file: open(path, create, worker, Arc::clone(&seen.files))?,
        seen,
    })
}

#[test]
fn async_stop_reports_worker_attachment_failure_only_after_native_tls_and_file_retirement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("actor-worker-lifecycle");
    let seen = Arc::new(ActorObserved::default());
    let mut db = ShardDb::create(
        actor_file(&path, true, false, Arc::clone(&seen)).unwrap(),
        CONFIG,
        CONFIG.page_size,
        TRUNK,
    )
    .unwrap();
    db.put(b"durable", b"old durable value").unwrap();
    db.checkpoint(1).unwrap();
    let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    // Actual declared B plus active-output and handoff run roles, owner and one worker.
    let run_bytes = CONFIG.page_size * usize::try_from(CONFIG.extent_pages).unwrap();
    let memory = 2 * (1 + 2) * run_bytes;
    db.set_memory(memory).unwrap();
    db.attach(&issuer, 1).unwrap();
    let worker_path = path.clone();
    let worker_seen = Arc::clone(&seen);
    db.set_workers(move || actor_file(&worker_path, false, true, Arc::clone(&worker_seen)))
        .unwrap();
    assert_eq!(db.memory_split().1, memory);
    let roles = 2; // Actor and one async caller on the same shard.
    let mut runtime = hyper_rt::Runtime::start(&hyper_rt::RuntimeConfig {
        shards: 1,
        tasks_per_shard: roles,
        timers_per_shard: roles,
        interests_per_shard: hyper_rt::runtime::interests_for(roles),
        ring_entries: roles,
        batch: roles,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        pin: false,
        cores: Vec::new(),
        page_bytes: CONFIG.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    let ranges = mantle_engine::ranges::Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        mantle_engine::ranges::RangesConfig {
            clients: 1,
            slice_ns: 50_000,
            spin_ns: 0,
            inline: false,
        },
    )
    .unwrap();
    assert_eq!(seen.workers_started.load(Ordering::SeqCst), 1);
    let observed = Arc::clone(&seen);
    let (back, reply) = std::sync::mpsc::sync_channel(1);
    let shard = runtime.shard_ids()[0];
    runtime
        .spawn_on(shard, async move {
            let mut client = ranges.client().unwrap();
            let value = vec![7; CONFIG.page_size / 2];
            client.put_async(b"worker-output", &value).await.unwrap();
            client.flush_async().await.unwrap();
            let mut actual = Vec::new();
            assert!(
                client
                    .get_async(b"worker-output", &mut actual)
                    .await
                    .unwrap()
            );
            assert_eq!(actual, value);
            assert!(observed.files.worker_writes.load(Ordering::SeqCst) > 0);
            assert_eq!(observed.files.worker_drop_faults.load(Ordering::SeqCst), 0);
            eprintln!("actual actor worker native writes completed before async Stop");
            drop(client);
            let result = ranges.stop_async().await;
            let live = observed.files.files.load(Ordering::SeqCst);
            let started = observed.workers_started.load(Ordering::SeqCst);
            let ended = observed.worker_tls_ended.load(Ordering::SeqCst);
            back.try_send((result, live, started, ended)).unwrap();
        })
        .unwrap();
    let (result, live_at_reply, started_at_reply, ended_at_reply) = reply.recv().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    assert_eq!(
        live_at_reply, 0,
        "Stop reply must follow all native file retirement"
    );
    assert_eq!(started_at_reply, 1);
    assert_eq!(
        ended_at_reply, started_at_reply,
        "Stop reply must follow actual worker TLS destruction"
    );
    assert!(seen.files.worker_drop_faults.load(Ordering::SeqCst) > 0);
    assert_eq!(seen.files.files.load(Ordering::SeqCst), 0);
    let (mut recovered, applied) = ShardDb::open(
        open(&path, false, false, Arc::clone(&seen.files)).unwrap(),
        CONFIG,
        CONFIG.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(applied, 1);
    check_saved(&mut recovered);
    match recovered.into_file() {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            drop(file);
        }
        IntoFile::Refused { owner, error } => {
            drop(owner);
            panic!("recovered cold actor fixture refused handoff: {error:?}");
        }
    }
    assert_eq!(seen.files.files.load(Ordering::SeqCst), 0);
    assert!(
        matches!(&result, Err(Error::Io { detail, .. }) if detail.contains("Drop")),
        "async Stop must preserve worker-specific native duplicate Drop failure: {result:?}"
    );
}
