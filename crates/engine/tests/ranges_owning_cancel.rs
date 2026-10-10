//! Owning Range cancellation retains external I/O and unanswered request ownership.
//! Source proposal only: requires the cooperative Range/runtime candidate, not ordinary
//! destructive Task cancellation. Native ordering facts decide success; a controller's
//! deadline may only fail the test, never release a successful held operation.
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
use hyper_rt::runtime::interests_for;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::branch::Op;
use mantle_engine::memtable::hashed::HashMem;
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::{ShardDb, issuer_batches};
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;
use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::path::Path;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::task::Poll;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const DEPTH: usize = 2;
const ROWS: usize = 2049;
const YOUNGER: &str = "younger worker write rejected by owning-cancel oracle";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Notice {
    ReadHeld,
    WriteHeld,
    YoungerFailed,
    Forbidden(&'static str),
}
#[derive(Clone, Copy)]
struct Region {
    first: u64,
    past: u64,
}
struct Gate {
    read: AtomicBool,
    writes: AtomicBool,
    selected: AtomicUsize,
    released: Mutex<bool>,
    changed: Condvar,
    live: AtomicUsize,
    active: AtomicUsize,
    early_reuse: AtomicBool,
    old: Mutex<Option<Region>>,
    events: mpsc::SyncSender<Notice>,
    retired: mpsc::SyncSender<()>,
}
impl Gate {
    fn new() -> (Arc<Self>, mpsc::Receiver<Notice>, mpsc::Receiver<()>) {
        // One held fact, one younger failure, and at most three forbidden operation kinds.
        let (events, notices) = mpsc::sync_channel(5);
        let (retired, retirement) = mpsc::sync_channel(1);
        (
            Arc::new(Self {
                read: AtomicBool::new(false),
                writes: AtomicBool::new(false),
                selected: AtomicUsize::new(0),
                released: Mutex::new(false),
                changed: Condvar::new(),
                live: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                early_reuse: AtomicBool::new(false),
                old: Mutex::new(None),
                events,
                retired,
            }),
            notices,
            retirement,
        )
    }
    fn release(&self) {
        *self.released.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
    fn wait(&self) {
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    fn refuse_owner(&self, op: &'static str) -> Result<(), DiskError> {
        if hyper_rt::futures::current_task().is_some() {
            let _ = self.events.try_send(Notice::Forbidden(op));
            return Err(io(op));
        }
        Ok(())
    }
}
struct OpenOnDrop(Arc<Gate>);
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Native {
    file: DeviceFile,
    gate: Arc<Gate>,
    worker: bool,
}
impl Drop for Native {
    fn drop(&mut self) {
        if self.gate.live.fetch_sub(1, Ordering::SeqCst) == 1 {
            assert_eq!(self.gate.active.load(Ordering::SeqCst), 0);
            let _ = self.gate.retired.try_send(());
        }
    }
}
impl BlockFile for Native {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.gate
            .refuse_owner("owner read in owning-cancel oracle")?;
        self.gate.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active(&self.gate.active);
        if self.gate.read.load(Ordering::SeqCst)
            && self.gate.selected.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.gate.events.try_send(Notice::ReadHeld).unwrap();
            self.gate.wait();
        }
        self.file.read_exact_at(bytes, offset)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> Result<(), DiskError> {
        self.gate
            .refuse_owner("owner write in owning-cancel oracle")?;
        self.gate.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active(&self.gate.active);
        if self.worker && self.gate.writes.load(Ordering::SeqCst) {
            let n = self.gate.selected.fetch_add(1, Ordering::SeqCst);
            let past = offset
                .checked_add(u64::try_from(bytes.len()).unwrap())
                .unwrap();
            if n == 0 {
                *self.gate.old.lock().unwrap() = Some(Region {
                    first: offset,
                    past,
                });
                self.gate.events.try_send(Notice::WriteHeld).unwrap();
                self.gate.wait();
            } else {
                if self
                    .gate
                    .old
                    .lock()
                    .unwrap()
                    .is_some_and(|old| offset < old.past && old.first < past)
                {
                    self.gate.early_reuse.store(true, Ordering::SeqCst);
                    return Err(io("held oldest output extent reused"));
                }
                if n == 1 {
                    self.gate.events.try_send(Notice::YoungerFailed).unwrap();
                    return Err(io(YOUNGER));
                }
            }
        }
        self.file.write_all_at(bytes, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.gate
            .refuse_owner("owner sync in owning-cancel oracle")?;
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        let file = self.file.try_clone()?;
        self.gate.live.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            file,
            gate: Arc::clone(&self.gate),
            worker: self.worker,
        })
    }
}
fn io(op: &'static str) -> DiskError {
    DiskError::Io {
        op,
        path: Default::default(),
        source: std::io::Error::other(op),
    }
}
fn native(path: &Path, create: bool, gate: Arc<Gate>, worker: bool) -> Native {
    let file = DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .unwrap();
    gate.live.fetch_add(1, Ordering::SeqCst);
    Native { file, gate, worker }
}
fn db(path: &Path, mem: usize, gate: Arc<Gate>) -> ShardDb<Native> {
    ShardDb::create(native(path, true, gate, false), STORE, mem, TRUNK).unwrap()
}
fn backend(db: &mut ShardDb<Native>, path: &Path, issuer: &Issuer, gate: Arc<Gate>) {
    db.attach(issuer, DEPTH).unwrap();
    let path = path.to_path_buf();
    db.set_workers(move || Ok(native(&path, false, Arc::clone(&gate), true)))
        .unwrap();
}
fn runtime() -> Runtime {
    Runtime::start(&RuntimeConfig {
        shards: 1,
        // Two owner services, one finite driver, one retained request/Stop, one progress task.
        tasks_per_shard: 5,
        timers_per_shard: 0,
        interests_per_shard: interests_for(5),
        ring_entries: 64,
        batch: 5,
        step_budget_ns: 50_000,
        timer_tick_ns: 100_000,
        pin: false,
        cores: Vec::new(),
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}
fn ranges(runtime: &mut Runtime, db: ShardDb<Native>, clients: usize) -> Arc<Ranges> {
    Arc::new(
        Ranges::start(
            runtime,
            vec![(Vec::new(), db)],
            RangesConfig {
                clients,
                slice_ns: 50_000,
                spin_ns: 0,
                inline: false,
            },
        )
        .unwrap(),
    )
}
fn parked_after(shard: hyper_rt::ShardId, before: u64) {
    // No elapsed time admits progress. The caller's external controller only fails deadlock.
    loop {
        if hyper_rt::registry::with_entry(shard.0, |e| {
            e.pulse.waits() > before && e.parking.parked()
        })
        .unwrap()
        {
            return;
        }
        std::thread::yield_now();
    }
}
fn reopen(path: &Path, gate: Arc<Gate>, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let (mut db, index) = ShardDb::open(
        native(path, false, gate, false),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(index, 1);
    let mut out = Vec::new();
    for (key, value) in expected {
        assert!(db.get(key, &mut out).unwrap());
        assert_eq!(&out, value);
    }
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(
        !db.scan(b"", None, expected.len() + 1, &mut rows, &mut next)
            .unwrap()
    );
    assert_eq!(rows.len(), expected.len());
    for ((key, value), (want_key, want_value)) in rows.iter().zip(expected) {
        assert_eq!(key, want_key.as_slice());
        assert_eq!(value, want_value.as_slice());
    }
    let mut from = Vec::new();
    let mut got = Vec::new();
    for _ in 0..=expected.len() {
        rows.clear();
        let more = db.scan(&from, None, 3, &mut rows, &mut next).unwrap();
        got.extend(
            rows.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        if !more {
            assert_eq!(
                got,
                expected
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<Vec<_>>()
            );
            db.check_references().unwrap();
            return;
        }
        assert!(next > from);
        from.clone_from(&next);
    }
    panic!("recovered pagination exceeded its live-row page bound");
}

#[test]
fn owning_cancel_keeps_a_held_query_and_its_orphan_lease_until_physical_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("first");
    let second_path = dir.path().join("second");
    let (gate, events, retired) = Gate::new();
    let mut first = db(&first_path, STORE.page_size, Arc::clone(&gate));
    let mut expected = BTreeMap::new();
    for i in 0..ROWS {
        let key = format!("shared-prefix/{i:08}").into_bytes();
        if i % 7 == 0 {
            first.delete(&key).unwrap();
        } else {
            let value = vec![(i % 251) as u8; [0, 1, 33, STORE.page_size / 3][i % 4]];
            first.put(&key, &value).unwrap();
            expected.insert(key, value);
        }
    }
    first.flush().unwrap();
    while first.owed() {
        assert!(first.maintain(u64::MAX).unwrap() > 0);
    }
    first.checkpoint(1).unwrap();
    first.set_cache(0);
    let (other_gate, _, _) = Gate::new();
    let mut second = db(&second_path, STORE.page_size, Arc::clone(&other_gate));
    let issuer = Issuer::start_for(
        dir.path(),
        DEPTH,
        issuer_batches(DEPTH, DEPTH).checked_mul(2).unwrap(),
    )
    .unwrap();
    assert_eq!(
        issuer.depth(),
        DEPTH,
        "native issuer reduced the held-plus-live fixture capability"
    );
    backend(&mut first, &first_path, &issuer, Arc::clone(&gate));
    backend(&mut second, &second_path, &issuer, other_gate);
    let mut runtime = runtime();
    let shard = runtime.shard_ids()[0];
    let _open = OpenOnDrop(Arc::clone(&gate));
    let first = ranges(&mut runtime, first, 2);
    let other = ranges(&mut runtime, second, 1);
    let source = Arc::clone(&first);
    let driver_gate = Arc::clone(&gate);
    let (done, driver_done) = mpsc::sync_channel(1);
    let target = expected.keys().nth(expected.len() / 2).unwrap().clone();
    runtime
        .spawn_on(shard, async move {
            let mut client = source.client().unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(&target, &mut out).await.unwrap());
            assert_eq!(source.task_ids().len(), source.len());
            let owner = source.task_ids().first().copied().unwrap();
            assert_eq!(owner.shard(), shard);
            driver_gate.read.store(true, Ordering::SeqCst);
            {
                let mut request = pin!(client.get_async(&target, &mut out));
                poll_fn(|cx| {
                    assert!(request.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            drop(client);
            drop(source);
            done.send(owner).unwrap();
        })
        .unwrap();
    let owner = driver_done.recv().unwrap();
    assert_eq!(events.recv().unwrap(), Notice::ReadHeld);
    let before = hyper_rt::registry::with_entry(shard.0, |e| e.pulse.waits()).unwrap();
    let source = Arc::clone(&first);
    let check_gate = Arc::clone(&gate);
    let (inspect, mut inspection) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (checked, mut check_done) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (published, publication) = mpsc::sync_channel(1);
    let (answer, answered) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut client = source.client().unwrap();
            let result = {
                let mut request = pin!(client.stats_async());
                poll_fn(|cx| {
                    assert!(request.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                published.send(()).unwrap();
                inspection.recv().await.unwrap();
                // Cancellation has been requested and another same-shard Range has already run.
                // Gone/Ready here would release unanswered ownership before physical retirement.
                poll_fn(|cx| {
                    assert!(request.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                assert!(check_gate.active.load(Ordering::SeqCst) > 0);
                checked.try_send(()).unwrap();
                request.await
            };
            assert!(matches!(result, Err(Error::Gone { .. })));
            assert_eq!(check_gate.live.load(Ordering::SeqCst), 0);
            drop(client);
            drop(source);
            answer.send(()).unwrap();
        })
        .unwrap();
    publication.recv().unwrap();
    parked_after(shard, before);
    assert!(matches!(
        first.client(),
        Err(Error::LimitExceeded { limit: 2, .. })
    ));
    assert!(retired.try_recv().is_err());
    let probe = Arc::clone(&other);
    let lease_probe = Arc::clone(&first);
    let progress_gate = Arc::clone(&gate);
    let (progress, progressed) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            hyper_rt::futures::cancel(owner).unwrap();
            let mut client = probe.client().unwrap();
            client
                .put_async(b"other", b"before physical release")
                .await
                .unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(b"other", &mut out).await.unwrap());
            assert_eq!(out, b"before physical release");
            inspect.try_send(()).unwrap();
            check_done.recv().await.unwrap();
            assert!(progress_gate.live.load(Ordering::SeqCst) > 0);
            assert!(progress_gate.active.load(Ordering::SeqCst) > 0);
            assert!(
                matches!(
                    lease_probe.client(),
                    Err(Error::LimitExceeded { limit: 2, .. })
                ),
                "the orphan request still owns its admission after owner cancellation"
            );
            drop(lease_probe);
            progress_gate.release();
            drop(client);
            drop(probe);
            progress.send(()).unwrap();
        })
        .unwrap();
    progressed.recv().unwrap();
    answered.recv().unwrap();
    retired.recv().unwrap();
    let mut replacement = first.client().unwrap();
    let mut out = Vec::new();
    assert!(matches!(
        replacement.get(b"known", &mut out),
        Err(Error::Gone { .. })
    ));
    drop(replacement);
    drop(first);
    Arc::try_unwrap(other).unwrap().stop().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    gate.read.store(false, Ordering::SeqCst);
    reopen(&first_path, gate, &expected);
}

#[test]
fn cancellation_during_closing_keeps_the_first_younger_write_error_and_all_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("first");
    let second_path = dir.path().join("second");
    let (gate, events, retired) = Gate::new();
    let run = STORE
        .page_size
        .checked_mul(usize::try_from(STORE.extent_pages).unwrap())
        .unwrap();
    // Two output credits plus a following output: input size derives from the existing
    // depth and format run, while write memory, issuer depth and worker count stay fixed.
    let mem = DEPTH.checked_mul(run).unwrap();
    let mut first = db(&first_path, mem, Arc::clone(&gate));
    first
        .put(b"durable", b"before owning cancellation")
        .unwrap();
    first.checkpoint(1).unwrap();
    // Owner and exactly one worker's attached B+active+handoff roles; no extra worker cap.
    first.set_memory(2 * (DEPTH + 2) * run).unwrap();
    let (other_gate, _, _) = Gate::new();
    let mut second = db(&second_path, STORE.page_size, Arc::clone(&other_gate));
    let issuer = Issuer::start_for(
        dir.path(),
        DEPTH,
        issuer_batches(DEPTH, DEPTH).checked_mul(2).unwrap(),
    )
    .unwrap();
    assert_eq!(
        issuer.depth(),
        DEPTH,
        "native issuer reduced the held-plus-live fixture capability"
    );
    backend(&mut first, &first_path, &issuer, Arc::clone(&gate));
    backend(&mut second, &second_path, &issuer, other_gate);
    let mut capacity = HashMem::new(mem).unwrap();
    let value = vec![7; STORE.page_size / 2];
    let mut input = Vec::new();
    for i in 0..ROWS {
        let key = format!("a/worker/{i:08}").into_bytes();
        match capacity.insert(&key, Op::Put, &value) {
            Ok(()) => input.push(key),
            Err(Error::LimitExceeded { .. }) => {
                input.push(key);
                break;
            }
            Err(error) => panic!("otherwise valid capacity input: {error}"),
        }
    }
    assert!(
        input.len() > DEPTH * usize::try_from(STORE.extent_pages).unwrap(),
        "the full input crosses the existing output credits before its following run"
    );
    let mut runtime = runtime();
    let shard = runtime.shard_ids()[0];
    let _open = OpenOnDrop(Arc::clone(&gate));
    let closing = ranges(&mut runtime, first, 1);
    let other = ranges(&mut runtime, second, 1);
    let source = Arc::clone(&closing);
    let driver_gate = Arc::clone(&gate);
    let (done, driver_done) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut client = source.client().unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(b"durable", &mut out).await.unwrap());
            assert_eq!(source.task_ids().len(), source.len());
            let owner = source.task_ids().first().copied().unwrap();
            assert_eq!(owner.shard(), shard);
            let before = client.stats_async().await.unwrap()[0].0.rotations;
            driver_gate.writes.store(true, Ordering::SeqCst);
            for key in input {
                client.put_async(&key, &value).await.unwrap();
            }
            let after = client.stats_async().await.unwrap()[0].0.rotations;
            assert_eq!(
                after - before,
                1,
                "one acknowledged rotation, no younger unapplied caller"
            );
            drop(client);
            drop(source);
            done.send(owner).unwrap();
        })
        .unwrap();
    let owner = driver_done.recv().unwrap();
    let mut held = false;
    let mut failed = false;
    for _ in 0..2 {
        match events.recv().unwrap() {
            Notice::WriteHeld => held = true,
            Notice::YoungerFailed => failed = true,
            notice => panic!("unexpected native fact: {notice:?}"),
        }
    }
    assert!(held && failed);
    let before = hyper_rt::registry::with_entry(shard.0, |e| e.pulse.waits()).unwrap();
    let closing = Arc::try_unwrap(closing).unwrap();
    let (inspect, mut inspection) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (checked, mut check_done) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (published, publication) = mpsc::sync_channel(1);
    let (done, stopped) = mpsc::sync_channel(1);
    let stop_gate = Arc::clone(&gate);
    runtime
        .spawn_on(shard, async move {
            let mut stopping = pin!(closing.stop_async());
            poll_fn(|cx| {
                assert!(stopping.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            published.send(()).unwrap();
            inspection.recv().await.unwrap();
            poll_fn(|cx| {
                assert!(stopping.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert!(stop_gate.active.load(Ordering::SeqCst) > 0);
            checked.try_send(()).unwrap();
            let result = stopping.await;
            assert_eq!(stop_gate.live.load(Ordering::SeqCst), 0);
            assert_eq!(stop_gate.active.load(Ordering::SeqCst), 0);
            assert!(
                matches!(&result, Err(Error::Io { detail, .. }) if detail.contains(YOUNGER)),
                "first actual physical reason must survive: {result:?}"
            );
            done.send(()).unwrap();
        })
        .unwrap();
    publication.recv().unwrap();
    parked_after(shard, before);
    assert!(retired.try_recv().is_err());
    let probe = Arc::clone(&other);
    let progress_gate = Arc::clone(&gate);
    let (progress, progressed) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            hyper_rt::futures::cancel(owner).unwrap();
            let mut client = probe.client().unwrap();
            client
                .put_async(b"other", b"while old worker write is held")
                .await
                .unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(b"other", &mut out).await.unwrap());
            assert_eq!(out, b"while old worker write is held");
            inspect.try_send(()).unwrap();
            check_done.recv().await.unwrap();
            assert!(progress_gate.active.load(Ordering::SeqCst) > 0);
            progress_gate.release();
            drop(client);
            drop(probe);
            progress.send(()).unwrap();
        })
        .unwrap();
    progressed.recv().unwrap();
    stopped.recv().unwrap();
    retired.recv().unwrap();
    assert!(!gate.early_reuse.load(Ordering::SeqCst));
    Arc::try_unwrap(other).unwrap().stop().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    gate.writes.store(false, Ordering::SeqCst);
    reopen(
        &first_path,
        gate,
        &BTreeMap::from([(b"durable".to_vec(), b"before owning cancellation".to_vec())]),
    );
}
