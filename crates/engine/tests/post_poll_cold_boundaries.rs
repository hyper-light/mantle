//! Actual post-poll task destruction has an entered shard but no current task.
//! Guard verdicts are checked after returning every cold native owner for cleanup.
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
use mantle_engine::branch::{Op, filter};
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::TrunkConfig;
use mantle_engine::trunk::pool::{self, Job, Owner, Pool, Spawn, Stream, Work};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 2,
    max_extents: 256,
};

struct Gate {
    armed: AtomicBool,
    entered: AtomicBool,
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

const KEY: &[u8] = b"post-poll-public-owner";
const VALUE: &[u8] = b"returned input and exact public value";
type Sent = Result<Result<usize, Box<Job>>, (Error, Box<Job>)>;

struct PoolOnDrop {
    pool: Option<Pool>,
    job: Option<Box<Job>>,
    returned: mpsc::SyncSender<(Pool, Sent)>,
}
impl Drop for PoolOnDrop {
    fn drop(&mut self) {
        assert!(hyper_rt::registry::current_shard().is_some());
        assert!(hyper_rt::futures::current_task().is_none());
        let mut pool = self.pool.take().unwrap();
        let result = pool.send(self.job.take().unwrap(), Owner::Pack, true);
        self.returned.try_send((pool, result)).unwrap();
    }
}

#[test]
fn post_poll_pool_startup_refuses_and_returns_the_exact_job() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pool");
    let (gate, _) = gate(false);
    gate.release();
    let mut store = Store::create(file(&path, true, false, Arc::clone(&gate)), CONFIG).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::clone(&calls);
    let worker_path = path.clone();
    let worker_gate = Arc::clone(&gate);
    let spawn: Spawn = Box::new(move |seat| {
        started.fetch_add(1, Ordering::SeqCst);
        let worker = file(&worker_path, false, true, Arc::clone(&worker_gate));
        std::thread::Builder::new()
            .name("post-poll-pool".into())
            .spawn(move || pool::serve(worker, CONFIG, seat))
            .map_err(|error| Error::Io {
                op: "start post-poll oracle worker",
                detail: error.to_string(),
            })
    });
    let pool = Pool::new(spawn, 1).unwrap();
    let (full, input) = mpsc::sync_channel(1);
    let mut bytes = Vec::new();
    pool::encode(
        &mut bytes,
        KEY,
        Op::Put,
        VALUE,
        mantle_engine::maplet::hash32(filter::hash(KEY)),
    )
    .unwrap();
    full.try_send(bytes).unwrap();
    drop(full);
    let job = Box::new(Job {
        work: Work::Pack(Stream {
            entries: 1,
            full: input,
        }),
        grant: store
            .grant(usize::try_from(CONFIG.extent_pages).unwrap())
            .unwrap(),
        file_end: store.end(),
        generation: store.generation(),
    });
    let pointer = (&*job as *const Job) as usize;
    let grant = job.grant.clone();
    let (returned, answer) = mpsc::sync_channel(1);
    let guard = PoolOnDrop {
        pool: Some(pool),
        job: Some(job),
        returned,
    };
    let mut rt = runtime();
    let (started, mut first_poll) = channel(1).unwrap();
    rt.spawn(async move {
        let _guard = guard;
        started.try_send(()).unwrap();
        std::future::pending::<()>().await;
    })
    .unwrap();
    // The single owner cannot poll this root until the child returned Pending.
    rt.block_on(async move { first_poll.recv().await.unwrap() })
        .unwrap();
    let (mut pool, result) = answer.recv().unwrap();
    let before_cold = calls.load(Ordering::SeqCst);
    let (refused, exact_owner, worker) = match result {
        Err((error, job)) => {
            let refused = matches!(error, Error::InvalidArgument { .. });
            let exact = (&*job as *const Job) as usize == pointer && job.grant == grant;
            let worker = pool.send(job, Owner::Pack, true).unwrap().unwrap();
            (refused, exact, worker)
        }
        Ok(Err(job)) => {
            let worker = pool.send(job, Owner::Pack, true).unwrap().unwrap();
            (false, false, worker)
        }
        Ok(Ok(worker)) => (false, false, worker),
    };
    let _ = worker;
    let back = pool.take(&mut store, Owner::Pack, true).unwrap().unwrap();
    assert!(back.physical_error.is_none());
    let output = back.result.unwrap();
    store.extend_end(output.end);
    store.grant_back(&output.unused).unwrap();
    assert_eq!(output.parts.len(), 1);
    let branch = &output.parts[0].1;
    let mut value = Vec::new();
    assert_eq!(
        branch.get(&mut store, KEY, &mut value).unwrap(),
        Some(Op::Put)
    );
    assert_eq!(value, VALUE);
    for &extent in &branch.extents {
        store.release(extent).unwrap();
    }
    for extent in back.topped {
        store.release(extent).unwrap();
    }
    drop(pool); // All native handles retire on the cold caller before the verdict.
    store.checkpoint(None, 1).unwrap();
    drop(store);
    drop(rt);
    let (reopened, checkpoint) =
        Store::open(file(&path, false, false, Arc::clone(&gate)), CONFIG).unwrap();
    assert_eq!(checkpoint.applied, 1);
    assert!(checkpoint.root.is_none());
    drop(reopened);
    assert_eq!(gate.copies.load(Ordering::SeqCst), 0);
    assert!(
        refused,
        "post-poll Pool startup accepted a synchronous worker admission"
    );
    assert!(
        exact_owner,
        "refusal did not return the exact Job and grants"
    );
    assert_eq!(
        before_cold, 0,
        "post-poll refusal called the native spawn factory"
    );
}

struct ClientOnDrop {
    ranges: Option<Ranges>,
    returned: mpsc::SyncSender<(Ranges, Result<(), Error>)>,
}
impl Drop for ClientOnDrop {
    fn drop(&mut self) {
        assert!(hyper_rt::registry::current_shard().is_some());
        assert!(hyper_rt::futures::current_task().is_none());
        let ranges = self.ranges.take().unwrap();
        let result = {
            let mut client = ranges.client().unwrap();
            client.put(KEY, VALUE)
        };
        self.returned.try_send((ranges, result)).unwrap();
    }
}

#[test]
fn post_poll_client_prepare_refuses_before_request_publication() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ranges");
    let (gate, _) = gate(false);
    gate.release();
    let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
    let mut db = ShardDb::create(
        file(&path, true, false, Arc::clone(&gate)),
        CONFIG,
        CONFIG.page_size,
        TrunkConfig {
            fanout: 3,
            leaf_entries: 96,
        },
    )
    .unwrap();
    db.attach(&issuer, 1).unwrap();
    let worker_path = path.clone();
    let worker_gate = Arc::clone(&gate);
    db.set_workers(move || Ok(file(&worker_path, false, true, Arc::clone(&worker_gate))))
        .unwrap();
    let mut native = Runtime::start(&RuntimeConfig {
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
    let ranges = Ranges::start(
        &mut native,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 1,
            slice_ns: 50_000,
            spin_ns: 0,
            inline: false,
        },
    )
    .unwrap();
    let (returned, answer) = mpsc::sync_channel(1);
    let guard = ClientOnDrop {
        ranges: Some(ranges),
        returned,
    };
    let mut rt = runtime();
    let (started, mut first_poll) = channel(1).unwrap();
    rt.spawn(async move {
        let _guard = guard;
        started.try_send(()).unwrap();
        std::future::pending::<()>().await;
    })
    .unwrap();
    // The single owner cannot poll this root until the child returned Pending.
    rt.block_on(async move { first_poll.recv().await.unwrap() })
        .unwrap();
    let (ranges, result) = answer.recv().unwrap();
    let refused = matches!(result, Err(Error::InvalidArgument { .. }));
    let mut client = ranges.client().unwrap();
    let mut value = Vec::new();
    let present = client.get(KEY, &mut value).unwrap();
    client.put(KEY, VALUE).unwrap();
    value.clear();
    assert!(client.get(KEY, &mut value).unwrap());
    assert_eq!(value, VALUE);
    client.checkpoint(1).unwrap();
    drop(client);
    ranges.stop().unwrap();
    native.shutdown().unwrap();
    drop(rt);
    let (mut reopened, applied) = ShardDb::open(
        file(&path, false, false, Arc::clone(&gate)),
        CONFIG,
        CONFIG.page_size,
        TrunkConfig {
            fanout: 3,
            leaf_entries: 96,
        },
    )
    .unwrap();
    assert_eq!(applied, 1);
    value.clear();
    assert!(reopened.get(KEY, &mut value).unwrap());
    assert_eq!(value, VALUE);
    drop(reopened);
    assert_eq!(gate.copies.load(Ordering::SeqCst), 0);
    assert!(
        refused,
        "post-poll synchronous Client prepare accepted publication"
    );
    assert!(!present, "refused post-poll Client mutated the range");
}
