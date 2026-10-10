//! Public borrowed synchronous API refusal. Artifact only, external per-case guard.
//! Assertions happen after complete cold cleanup, so a baseline semantic RED cannot strand a worker.
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

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_rt::runtime::interests_for;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::branch::{Op, filter};
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::pool::{self, Back, Job, Owner, Pool, Spawn, Stream, Work};

/// Reuse the existing native Pool startup fixture's page/extent shape.
const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
/// One exact row exercises retained input, output bytes and grant ownership.
const KEY: &[u8] = b"retained-public-pool-job";
const VALUE: &[u8] = b"same byte value after runtime refusal";

#[derive(Debug)]
struct Gate {
    first: AtomicBool,
    entered: mpsc::SyncSender<()>,
    open: Mutex<bool>,
    changed: Condvar,
    calls: AtomicUsize,
}

impl Gate {
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn hold(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.first.swap(false, Ordering::SeqCst) {
            let mut open = self.open.lock().unwrap();
            self.entered.try_send(()).unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        }
    }
}

#[derive(Debug)]
struct OpenOnDrop(Arc<Gate>);
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

// One captured owner, with its release guard declared before the potentially joining
// Pool. Field Drop order also protects spawn admission refusal before any first poll.
#[derive(Debug)]
struct Owned {
    guard: OpenOnDrop,
    pool: Pool,
    store: Store<File>,
}

#[derive(Debug)]
struct File {
    native: DeviceFile,
    gate: Arc<Gate>,
    worker: bool,
}
impl BlockFile for File {
    fn alignment(&self) -> Alignment {
        self.native.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.native.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], at: u64) -> Result<(), DiskError> {
        self.native.read_exact_at(bytes, at)
    }
    fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
        if self.worker {
            self.gate.hold();
        }
        self.native.write_all_at(bytes, at)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.native.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            native: self.native.try_clone()?,
            gate: Arc::clone(&self.gate),
            worker: self.worker,
        })
    }
}

fn file(path: &Path, create: bool, worker: bool, gate: &Arc<Gate>) -> File {
    File {
        native: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        gate: Arc::clone(gate),
        worker,
    }
}

fn runtime() -> Runtime {
    let roles = 1; // One transient borrowed API call; the native worker is not a runtime task.
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: roles,
        timers_per_shard: roles,
        interests_per_shard: interests_for(roles),
        ring_entries: roles,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: roles,
        pin: false,
        cores: Vec::new(),
        page_bytes: CONFIG.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}

#[derive(Clone, Copy)]
enum Call {
    Take,
    Buffer,
    Any,
}
#[derive(Debug)]
enum ResultOf {
    Take(Result<Option<Back>, Error>),
    Buffer(Result<Option<Vec<u8>>, Error>),
    Any(Result<bool, Error>),
}
impl ResultOf {
    fn refused(&self) -> bool {
        matches!(
            self,
            Self::Take(Err(Error::InvalidArgument { .. }))
                | Self::Buffer(Err(Error::InvalidArgument { .. }))
                | Self::Any(Err(Error::InvalidArgument { .. }))
        )
    }
}

fn run(call: Call) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let (entered, held) = mpsc::sync_channel(1);
    let gate = Arc::new(Gate {
        first: AtomicBool::new(true),
        entered,
        open: Mutex::new(false),
        changed: Condvar::new(),
        calls: AtomicUsize::new(0),
    });
    let mut store = Store::create(file(&path, true, false, &gate), CONFIG).unwrap();
    let rt = runtime();
    let worker_path = path.clone();
    let worker_gate = Arc::clone(&gate);
    let spawn: Spawn = Box::new(move |seat| {
        let file = file(&worker_path, false, true, &worker_gate);
        std::thread::Builder::new()
            .name("borrowed-pool-guard-worker".into())
            .spawn(move || pool::serve(file, CONFIG, seat))
            .map_err(|error| Error::Io {
                op: "start borrowed API oracle worker",
                detail: error.to_string(),
            })
    });
    let mut pool = Pool::new(spawn, 1).unwrap();
    // No worker has a job yet. Once it may hold writes, release precedes Pool/Runtime Drop.
    let _guard = OpenOnDrop(Arc::clone(&gate));
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
    let worker = pool.send(job, Owner::Pack, true).unwrap().unwrap().worker;
    held.recv().unwrap(); // An actual worker write is now held before the runtime call.
    let (attempted, attempt) = mpsc::sync_channel(1);
    let (back, returned) = mpsc::sync_channel(1);
    let mut owned = Owned {
        guard: OpenOnDrop(Arc::clone(&gate)),
        pool,
        store,
    };
    rt.spawn_on(rt.shard_ids()[0], async move {
        attempted.try_send(()).unwrap();
        let result = match call {
            Call::Take => ResultOf::Take(owned.pool.take(&mut owned.store, Owner::Pack, true)),
            Call::Buffer => ResultOf::Buffer(owned.pool.buffer(&mut owned.store, worker, true)),
            Call::Any => ResultOf::Any(owned.pool.wait_any(&mut owned.store, std::iter::empty())),
        };
        let still_owned = owned.pool.out_for(Owner::Pack);
        back.try_send((owned, result, still_owned)).unwrap();
    })
    .unwrap();
    attempt.recv().unwrap();
    // Also release the historical implementation. Its successful blocking operation is a
    // semantic RED after cleanup, not a timeout. This does not claim sibling progress while held.
    gate.release();
    let (owned, result, still_owned) = returned.recv().unwrap();
    owned.guard.0.release(); // The callback is open before dismantling its captured owner.
    let Owned {
        guard: _operation_guard,
        mut pool,
        mut store,
    } = owned;
    let refused = result.refused();
    let back = match result {
        ResultOf::Take(Ok(Some(back))) => back,
        _ => pool.take(&mut store, Owner::Pack, true).unwrap().unwrap(),
    };
    assert!(back.physical_error.is_none());
    let output = back.result.unwrap();
    store.extend_end(output.end);
    store.grant_back(&output.unused).unwrap();
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
    drop(pool); // Cold cleanup joins the actual worker before any verdict.
    store.checkpoint(None, 1).unwrap();
    let (_, result) = finished_file(store.into_file());
    result.unwrap();
    rt.shutdown().unwrap();
    let (reopened, recovered) = Store::open(file(&path, false, false, &gate), CONFIG).unwrap();
    assert_eq!(recovered.applied, 1);
    assert!(recovered.root.is_none());
    drop(reopened);
    assert!(gate.calls.load(Ordering::SeqCst) > 0);
    assert!(
        refused,
        "a blocking public Pool call consumed a receipt/buffer instead of refusing"
    );
    assert!(
        still_owned,
        "the refusal consumed the outstanding job's ownership"
    );
}

#[test]
fn runtime_take_refuses_before_consuming_a_job_result() {
    run(Call::Take);
}
#[test]
fn runtime_buffer_refuses_before_consuming_a_returned_buffer() {
    run(Call::Buffer);
}
#[test]
fn runtime_wait_any_refuses_before_consuming_a_worker_message() {
    run(Call::Any);
}
