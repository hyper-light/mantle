//! An actual foreign read panic during an accepted maintenance job must return a typed
//! result, physically retire its worker and refuse any later job without losing ownership.
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
use mantle_engine::branch::{Branch, Op, filter};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::{Config, IntoFile, Store};
use mantle_engine::trunk::TrunkConfig;
use mantle_engine::trunk::pool::{self, Job, Owner, Pool, Spawn, Stream, Task, Work};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const WITNESS: &str = "actual accepted maintenance job read panic";

#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
    explicitly_opened: AtomicBool,
}
impl Gate {
    fn release(&self, explicit: bool) {
        if explicit {
            self.explicitly_opened.store(true, Ordering::SeqCst);
        }
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn wait(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
}
struct OpenOnDrop(Arc<Gate>);
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.release(false);
    }
}

struct Facts {
    files: AtomicUsize,
    completed_writes: AtomicUsize,
    armed: AtomicBool,
    panics: AtomicUsize,
    ended: AtomicUsize,
    gate: Arc<Gate>,
    entered: mpsc::SyncSender<bool>,
    panic: mpsc::SyncSender<()>,
    exited: mpsc::SyncSender<()>,
}
struct Tls(std::cell::RefCell<Option<Arc<Facts>>>);
impl Drop for Tls {
    fn drop(&mut self) {
        if let Some(facts) = self.0.get_mut().take() {
            facts.ended.fetch_add(1, Ordering::SeqCst);
            eprintln!("actual maintenance worker TLS ended");
            let _ = facts.exited.try_send(());
        }
    }
}
std::thread_local! {
    static TLS: Tls = const { Tls(std::cell::RefCell::new(None)) };
}
struct File {
    file: DeviceFile,
    facts: Arc<Facts>,
    worker: bool,
    duplicate: bool,
}
impl Drop for File {
    fn drop(&mut self) {
        self.facts.files.fetch_sub(1, Ordering::SeqCst);
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
        if self.worker && !self.duplicate && self.facts.armed.swap(false, Ordering::SeqCst) {
            eprintln!("actual native worker borrowed read held at {at}");
            self.facts.entered.try_send(true).unwrap();
            self.facts.gate.wait();
            self.facts.panics.fetch_add(1, Ordering::SeqCst);
            eprintln!("{WITNESS}: native borrowed page read at {at}");
            self.facts.panic.try_send(()).unwrap();
            panic!("{WITNESS}");
        }
        self.file.read_exact_at(bytes, at)
    }
    fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
        self.file.write_all_at(bytes, at)?;
        if self.worker {
            self.facts.completed_writes.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        if self.worker && !self.duplicate {
            TLS.with(|tls| {
                *tls.0.borrow_mut() = Some(Arc::clone(&self.facts));
            });
        }
        let file = self.file.try_clone()?;
        self.facts.files.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            file,
            facts: Arc::clone(&self.facts),
            worker: self.worker,
            duplicate: true,
        })
    }
}
fn open(path: &Path, create: bool, worker: bool, facts: &Arc<Facts>) -> File {
    let file = DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap();
    facts.files.fetch_add(1, Ordering::SeqCst);
    File {
        file,
        facts: Arc::clone(facts),
        worker,
        duplicate: false,
    }
}
fn spawn(path: &Path, facts: &Arc<Facts>) -> Spawn {
    let path = path.to_path_buf();
    let facts = Arc::clone(facts);
    Box::new(move |seat| {
        let file = open(&path, false, true, &facts);
        std::thread::Builder::new()
            .name("accepted-job-read-panic".into())
            .spawn(move || pool::serve(file, CONFIG, seat))
            .map_err(|error| Error::Io {
                op: "start accepted job panic fixture",
                detail: error.to_string(),
            })
    })
}
fn take_file<O>(outcome: IntoFile<O, File>) -> File {
    match outcome {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            file
        }
        IntoFile::Refused { owner, error } => {
            drop(owner);
            panic!("cold extraction refused: {error:?}")
        }
    }
}
fn packed(store: &mut Store<File>, key: &[u8], value: &[u8]) -> Box<Job> {
    let (send, full) = mpsc::sync_channel(1);
    let mut bytes = Vec::new();
    pool::encode(
        &mut bytes,
        key,
        Op::Put,
        value,
        mantle_engine::maplet::hash32(filter::hash(key)),
    )
    .unwrap();
    send.try_send(bytes).unwrap();
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
fn returned_branch(store: &mut Store<File>, pool: &mut Pool, job: Box<Job>) -> Branch {
    assert!(pool.send(job, Owner::Pack, true).unwrap().is_ok());
    let back = pool.take(store, Owner::Pack, true).unwrap().unwrap();
    assert!(back.physical_error.is_none());
    let output = back.result.unwrap();
    store.extend_end(output.end);
    store.grant_back(&output.unused).unwrap();
    assert!(back.topped.is_empty());
    assert_eq!(output.parts.len(), 1);
    output.parts.into_iter().next().unwrap().1
}
fn check_branch(store: &mut Store<File>, branch: &Branch, key: &[u8], value: &[u8]) {
    let mut out = Vec::new();
    assert_eq!(branch.get(store, key, &mut out).unwrap(), Some(Op::Put));
    assert_eq!(out, value);
}

#[test]
fn an_active_job_read_panic_returns_after_retirement_and_preserves_retry_and_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("active-job");
    let gate = Arc::new(Gate::default());
    let (entered, held) = mpsc::sync_channel(1);
    let (panic, panicked) = mpsc::sync_channel(1);
    let (exited, ended) = mpsc::sync_channel(1);
    let facts = Arc::new(Facts {
        files: AtomicUsize::new(0),
        completed_writes: AtomicUsize::new(0),
        armed: AtomicBool::new(false),
        panics: AtomicUsize::new(0),
        ended: AtomicUsize::new(0),
        gate: Arc::clone(&gate),
        entered,
        panic,
        exited,
    });
    let mut saved = ShardDb::create(
        open(&path, true, false, &facts),
        CONFIG,
        CONFIG.page_size,
        TRUNK,
    )
    .unwrap();
    saved.put(b"durable", b"last acknowledged value").unwrap();
    saved.checkpoint(1).unwrap();
    let file = take_file(saved.into_file());
    let (mut store, recovered) = Store::open(file, CONFIG).unwrap();
    assert_eq!(recovered.applied, 1);
    let issuer = Issuer::start_for(directory.path(), 1, 1).unwrap();
    assert_eq!(issuer.depth(), 1);
    let mut pool = Pool::new(spawn(&path, &facts), 1).unwrap();
    let _open = OpenOnDrop(Arc::clone(&gate));
    pool.set_attach(issuer.attacher(), 1);
    let job = packed(&mut store, b"input", b"unchanged branch contents");
    let input = returned_branch(&mut store, &mut pool, job);
    check_branch(&mut store, &input, b"input", b"unchanged branch contents");
    assert!(facts.completed_writes.load(Ordering::SeqCst) > 0);
    eprintln!("real native worker output completed before active Compact admission");
    let grant = store
        .grant(usize::try_from(CONFIG.extent_pages).unwrap())
        .unwrap();
    let returned_grant = grant.clone();
    facts.armed.store(true, Ordering::SeqCst);
    let job = Box::new(Job {
        work: Work::Compact(Task {
            inputs: vec![input.clone()],
            from: Vec::new(),
            end: None,
            drop_tombstones: false,
            per: input.count,
        }),
        grant,
        file_end: store.end(),
        generation: store.generation(),
    });
    assert!(pool.send(job, Owner::Trunk, true).unwrap().is_ok());
    assert!(
        held.recv().unwrap(),
        "actual native borrowed read was not held"
    );
    assert!(
        pool.take(&mut store, Owner::Trunk, false)
            .unwrap()
            .is_none(),
        "no job completion is valid while the actual borrowed file callback is held"
    );
    gate.release(true);
    panicked.recv().unwrap();
    // Actual native/TLS exit excludes a still-running file callback as the reason for no Done.
    ended.recv().unwrap();
    eprintln!("actual read panic and worker exit observed; taking accepted job result");
    let back = pool.take(&mut store, Owner::Trunk, true).unwrap().unwrap();
    assert!(
        matches!(&back.result, Err(Error::Io { detail, .. }) if detail.contains(WITNESS)),
        "the original read-panic reason must survive: {:?}",
        back.result
    );
    for extent in returned_grant.into_iter().chain(back.topped) {
        store.release(extent).unwrap();
    }
    check_branch(&mut store, &input, b"input", b"unchanged branch contents");
    let retry = packed(&mut store, b"retry", b"returned after unwind");
    let (_, retry) = pool.send(retry, Owner::Pack, true).unwrap_err();
    drop(pool);
    let mut healthy = Pool::new(spawn(&path, &facts), 1).unwrap();
    healthy.set_attach(issuer.attacher(), 1);
    let output = returned_branch(&mut store, &mut healthy, retry);
    check_branch(&mut store, &output, b"retry", b"returned after unwind");
    drop(healthy);
    for extent in input.extents.into_iter().chain(output.extents) {
        store.release(extent).unwrap();
    }
    let file = take_file(store.into_file());
    drop(file);
    drop(issuer);
    assert_eq!(facts.files.load(Ordering::SeqCst), 0);
    assert_eq!(facts.panics.load(Ordering::SeqCst), 1);
    assert!(gate.explicitly_opened.load(Ordering::SeqCst));
    let (mut saved, applied) = ShardDb::open(
        open(&path, false, false, &facts),
        CONFIG,
        CONFIG.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(applied, 1);
    let mut value = Vec::new();
    assert!(saved.get(b"durable", &mut value).unwrap());
    assert_eq!(value, b"last acknowledged value");
    assert!(!saved.get(b"input", &mut value).unwrap());
    assert!(!saved.get(b"retry", &mut value).unwrap());
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(!saved.scan(b"", None, 2, &mut rows, &mut next).unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.get(0),
        Some((&b"durable"[..], &b"last acknowledged value"[..]))
    );
    saved.check_references().unwrap();
    drop(take_file(saved.into_file()));
    assert_eq!(facts.files.load(Ordering::SeqCst), 0);
}

#[test]
fn same_shard_stop_keeps_held_job_ownership_and_reports_its_read_panic_after_retirement() {
    use hyper_rt::combine::{Either, race2};
    use hyper_rt::{Runtime, RuntimeConfig};
    use mantle_engine::ranges::{Ranges, RangesConfig};
    use std::future::Future;
    use std::task::Poll;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("actor-active-job");
    let other_path = directory.path().join("unrelated-actor");
    let gate = Arc::new(Gate::default());
    let (entered, held) = mpsc::sync_channel(1);
    let (panic, panicked) = mpsc::sync_channel(1);
    let (exited, ended) = mpsc::sync_channel(1);
    let facts = Arc::new(Facts {
        files: AtomicUsize::new(0),
        completed_writes: AtomicUsize::new(0),
        armed: AtomicBool::new(false),
        panics: AtomicUsize::new(0),
        ended: AtomicUsize::new(0),
        gate: Arc::clone(&gate),
        entered,
        panic,
        exited,
    });
    let trunk = TrunkConfig {
        fanout: 3,
        leaf_entries: 4,
    };
    let mem = CONFIG.page_size;
    let mut first = ShardDb::create(open(&path, true, false, &facts), CONFIG, mem, trunk).unwrap();
    first.put(b"durable", b"last acknowledged value").unwrap();
    first.checkpoint(1).unwrap();
    let issuer = Issuer::start_for(directory.path(), 1, 2).unwrap();
    assert_eq!(issuer.depth(), 1);
    let reservation =
        2 * (1 + 2) * CONFIG.page_size * usize::try_from(CONFIG.extent_pages).unwrap();
    first.set_memory(reservation).unwrap();
    first.attach(&issuer, 1).unwrap();
    let worker_path = path.clone();
    let worker_facts = Arc::clone(&facts);
    first
        .set_workers(move || Ok(open(&worker_path, false, true, &worker_facts)))
        .unwrap();
    let align = Alignment::new(CONFIG.page_size).unwrap();
    let native = DeviceFile::open(&other_path, true, CachingRequest::Buffered, align).unwrap();
    let mut other = ShardDb::create(native, CONFIG, mem, trunk).unwrap();
    other.set_memory(reservation).unwrap();
    other.attach(&issuer, 1).unwrap();
    let worker_path = other_path.clone();
    other
        .set_workers(move || {
            DeviceFile::open(&worker_path, false, CachingRequest::Buffered, align).map_err(
                |error| Error::Io {
                    op: "open unrelated worker",
                    detail: error.to_string(),
                },
            )
        })
        .unwrap();
    let roles = 4; // Two Range services, retained writer, and Stop/unrelated-progress caller.
    let mut runtime = Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: roles,
        timers_per_shard: roles,
        interests_per_shard: hyper_rt::runtime::interests_for(roles),
        ring_entries: roles,
        step_budget_ns: 50_000,
        timer_tick_ns: 100_000,
        batch: roles,
        pin: false,
        cores: Vec::new(),
        page_bytes: CONFIG.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    // The guard is later than Runtime/Issuer/engine owners and opens before their implicit joins.
    let _open = OpenOnDrop(Arc::clone(&gate));
    let first = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), first)],
        RangesConfig {
            clients: 2,
            slice_ns: 50_000,
            spin_ns: 0,
            inline: false,
        },
    )
    .unwrap();
    let other = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), other)],
        RangesConfig {
            clients: 1,
            slice_ns: 50_000,
            spin_ns: 0,
            inline: false,
        },
    )
    .unwrap();
    let (cancel, mut canceled) = hyper_rt::sync::channel(1).unwrap();
    let (return_range, returned) = mpsc::sync_channel(1);
    let observed = Arc::clone(&facts);
    let shard = runtime.shard_ids()[0];
    facts.armed.store(true, Ordering::SeqCst);
    runtime
        .spawn_on(shard, async move {
            let mut client = first.client().unwrap();
            // A fanout's two leaf cohorts exceed the public leaf compaction entry bound.
            let count = 2 * trunk.leaf_entries * trunk.fanout as u64;
            let value = vec![7; CONFIG.page_size / 2];
            let outcome = {
                let writes = async {
                    for ordinal in 0..count {
                        client.put_async(&ordinal.to_be_bytes(), &value).await?;
                    }
                    client.flush_async().await
                };
                race2(writes, canceled.recv()).await
            };
            if !matches!(outcome, Either::Second(Ok(()))) {
                // An ordinary completed setup before any held callback is not a panic RED.
                let _ = observed.entered.try_send(false);
            }
            drop(client); // Only the borrowed wait/client ends; Range retains the published Request.
            return_range.try_send(first).unwrap();
        })
        .unwrap();
    assert!(
        held.recv().unwrap(),
        "dataset did not reach actual selected worker read"
    );
    assert!(
        facts.completed_writes.load(Ordering::SeqCst) > 0,
        "at least one real packing output must precede the compaction read"
    );
    cancel.blocking_send(()).unwrap();
    let first = returned.recv().unwrap();
    let (progress, progressed) = mpsc::sync_channel(1);
    let (done, answer) = mpsc::sync_channel(1);
    let observed = Arc::clone(&facts);
    runtime
        .spawn_on(shard, async move {
            let mut stop = std::pin::pin!(first.stop_async());
            std::future::poll_fn(|cx| {
                assert!(
                    matches!(stop.as_mut().poll(cx), Poll::Pending),
                    "Stop cannot reply during an actual held native callback"
                );
                Poll::Ready(())
            })
            .await;
            let mut client = other.client().unwrap();
            client
                .put_async(b"progress", b"same shard remains runnable")
                .await
                .unwrap();
            let mut value = Vec::new();
            assert!(client.get_async(b"progress", &mut value).await.unwrap());
            assert_eq!(value, b"same shard remains runnable");
            assert!(!observed.gate.explicitly_opened.load(Ordering::SeqCst));
            drop(client);
            progress.try_send(()).unwrap();
            let result = stop.await;
            let files_at_reply = observed.files.load(Ordering::SeqCst);
            let ended_at_reply = observed.ended.load(Ordering::SeqCst);
            other.stop_async().await.unwrap();
            done.try_send((result, files_at_reply, ended_at_reply))
                .unwrap();
        })
        .unwrap();
    progressed.recv().unwrap();
    assert!(matches!(answer.try_recv(), Err(mpsc::TryRecvError::Empty)));
    gate.release(true);
    panicked.recv().unwrap();
    ended.recv().unwrap();
    eprintln!("actual actor read panic and TLS exit observed after same-shard Pending Stop");
    let (result, files_at_reply, ended_at_reply) = answer.recv().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    assert_eq!(files_at_reply, 0);
    assert_eq!(ended_at_reply, 1);
    assert_eq!(facts.panics.load(Ordering::SeqCst), 1);
    assert!(
        matches!(&result, Err(Error::Io { detail, .. }) if detail.contains(WITNESS)),
        "active job read panic must survive Stop: {result:?}"
    );
    let (mut saved, applied) =
        ShardDb::open(open(&path, false, false, &facts), CONFIG, mem, trunk).unwrap();
    assert_eq!(applied, 1);
    let mut value = Vec::new();
    assert!(saved.get(b"durable", &mut value).unwrap());
    assert_eq!(value, b"last acknowledged value");
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(!saved.scan(b"", None, 2, &mut rows, &mut next).unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.get(0),
        Some((&b"durable"[..], &b"last acknowledged value"[..]))
    );
    saved.check_references().unwrap();
    drop(take_file(saved.into_file()));
    assert_eq!(facts.files.load(Ordering::SeqCst), 0);
}
