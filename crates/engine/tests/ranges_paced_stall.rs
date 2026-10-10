//! Public Range request retention, cancellation, ordering and same-shard progress.
//! Run under an external controller deadline; no clock decides a successful verdict.
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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::combine::{Either, race2};
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::branch::Op;
use mantle_engine::memtable::hashed::HashMem;
use mantle_engine::ranges::{Client, Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;
use std::future::{Future, poll_fn};
use std::pin::pin;

// Existing shard_db_workers.rs native shape and ranges_active_order.rs operation bound.
const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const OPS: u64 = 2048;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Held,
    Retired,
    Failed(&'static str, std::io::ErrorKind),
    Forbidden,
    Stopped,
}

#[derive(Clone, Copy)]
struct Region {
    offset: u64,
    end: u64,
}

struct State {
    selected: bool,
    released: bool,
    active: Option<Region>,
    fail: bool,
}

struct Gate {
    armed: AtomicBool,
    reject_producer_write: bool,
    state: Mutex<State>,
    changed: Condvar,
    events: mpsc::SyncSender<Event>,
    failures: AtomicUsize,
    hold_remaining_writes: AtomicBool,
    reused_early: AtomicBool,
    flushed_early: AtomicBool,
    forbidden: AtomicBool,
}

impl Gate {
    fn new(reject_producer_write: bool, fail: bool) -> (Arc<Self>, mpsc::Receiver<Event>) {
        // Each of the five witness kinds is emitted at most once.
        let (events, received) = mpsc::sync_channel(5);
        (
            Arc::new(Self {
                armed: AtomicBool::new(false),
                reject_producer_write,
                state: Mutex::new(State {
                    selected: false,
                    released: false,
                    active: None,
                    fail,
                }),
                changed: Condvar::new(),
                events,
                failures: AtomicUsize::new(0),
                hold_remaining_writes: AtomicBool::new(false),
                reused_early: AtomicBool::new(false),
                flushed_early: AtomicBool::new(false),
                forbidden: AtomicBool::new(false),
            }),
            received,
        )
    }

    fn open(&self) {
        // Cleanup must open the device even if a failed assertion poisoned test state.
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .released = true;
        self.changed.notify_all();
    }

    fn report(&self, event: Event) {
        self.events.try_send(event).unwrap();
    }

    fn write(
        &self,
        worker: bool,
        file: &DeviceFile,
        bytes: &[u8],
        offset: u64,
    ) -> Result<(), DiskError> {
        if !self.armed.load(Ordering::SeqCst) {
            return file.write_all_at(bytes, offset);
        }
        let mut state = self.state.lock().unwrap();
        let thread = std::thread::current();
        let producer = thread.name().is_some_and(|name| {
            name.starts_with("hyper-rt-") || (worker && name.starts_with("mantle-maint-"))
        });
        if self.reject_producer_write && !state.released && producer {
            if !self.forbidden.swap(true, Ordering::SeqCst) {
                self.report(Event::Forbidden);
            }
            return Err(refused("page write on a packing/runtime producer thread"));
        }
        let end = offset
            .checked_add(u64::try_from(bytes.len()).unwrap())
            .unwrap();
        if state
            .active
            .is_some_and(|held| offset < held.end && held.offset < end)
        {
            self.reused_early.store(true, Ordering::SeqCst);
            return Err(refused(
                "held output bytes reused before their write retired",
            ));
        }
        if worker && !state.selected {
            state.selected = true;
            state.active = Some(Region { offset, end });
            self.report(Event::Held);
            while !state.released {
                state = self.changed.wait(state).unwrap();
            }
            let fail = std::mem::take(&mut state.fail);
            drop(state);
            let result = if fail {
                self.failures.fetch_add(1, Ordering::SeqCst);
                self.report(Event::Failed(
                    "one worker page write rejected by test",
                    std::io::ErrorKind::Other,
                ));
                Err(refused("one worker page write rejected by test"))
            } else {
                file.write_all_at(bytes, offset)
            };
            self.state.lock().unwrap().active = None;
            self.report(Event::Retired);
            return result;
        }
        if worker && self.hold_remaining_writes.load(Ordering::SeqCst) {
            while !state.released {
                state = self.changed.wait(state).unwrap();
            }
        }
        drop(state);
        file.write_all_at(bytes, offset)
    }
}

struct OpenOnDrop(Arc<Gate>);
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}
struct Terminal(Arc<Gate>);
impl Drop for Terminal {
    fn drop(&mut self) {
        self.0.report(Event::Stopped);
    }
}

struct Gated {
    file: DeviceFile,
    gate: Arc<Gate>,
    worker: bool,
}
impl BlockFile for Gated {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(bytes, offset)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> Result<(), DiskError> {
        self.gate.write(self.worker, &self.file, bytes, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        if self.gate.state.lock().unwrap().active.is_some() {
            self.gate.flushed_early.store(true, Ordering::SeqCst);
            return Err(refused("checkpoint flush before held output write retired"));
        }
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: Arc::clone(&self.gate),
            worker: self.worker,
        })
    }
}

fn refused(op: &'static str) -> DiskError {
    DiskError::Io {
        op,
        path: PathBuf::new(),
        source: std::io::Error::other(op),
    }
}

fn open(path: &Path, create: bool, worker: bool, gate: Arc<Gate>) -> Gated {
    Gated {
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap(),
        gate,
        worker,
    }
}

fn engine(path: &Path, gate: Arc<Gate>) -> ShardDb<Gated> {
    ShardDb::create(
        open(path, true, false, Arc::clone(&gate)),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap()
}

fn workers(db: &mut ShardDb<Gated>, path: &Path, gate: Arc<Gate>) {
    let path = path.to_path_buf();
    db.set_workers(move || Ok(open(&path, false, true, Arc::clone(&gate))))
        .unwrap();
}

fn cold_complete(db: &mut ShardDb<Gated>) {
    db.flush().unwrap();
    while db.owed() {
        assert!(db.maintain(u64::MAX).unwrap() > 0);
    }
}

fn key(i: u64) -> Vec<u8> {
    match i % 67 {
        0 => Vec::new(),
        1 => vec![0, 1],
        k => format!("a/shared-prefix/object-{k:04}").into_bytes(),
    }
}

fn operation(i: u64) -> (Vec<u8>, Option<Vec<u8>>) {
    let key = key(i);
    let value = if i.is_multiple_of(7) {
        None
    } else {
        let len = [0, 1, 33, STORE.page_size / 3][i as usize % 4];
        Some(vec![(i % 251) as u8; len])
    };
    (key, value)
}

fn record(oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>, key: Vec<u8>, value: Option<Vec<u8>>) {
    match value {
        Some(value) => {
            oracle.insert(key, value);
        }
        None => {
            oracle.remove(&key);
        }
    }
}

// Three stable values exercise exact reads and one-row pagination during a retained mutation.
fn stable() -> BTreeMap<Vec<u8>, Vec<u8>> {
    (0..3)
        .map(|i| {
            (
                format!("a/stable/{i}").into_bytes(),
                format!("before-{i}").into_bytes(),
            )
        })
        .collect()
}

async fn check_stable(client: &mut Client<'_>) {
    let expected = stable();
    let mut out = Vec::new();
    for (key, value) in &expected {
        assert!(client.get_async(key, &mut out).await.unwrap());
        assert_eq!(&out, value);
    }
    let mut from = b"a/stable/".to_vec();
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let mut got = Vec::new();
    for _ in 0..=expected.len() {
        rows.clear();
        let more = client
            .scan_async(&from, Some(b"a/stable0"), 1, &mut rows, &mut next)
            .await
            .unwrap();
        got.extend(
            rows.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        if !more {
            assert_eq!(got, expected.into_iter().collect::<Vec<_>>());
            return;
        }
        assert!(next > from);
        from.clone_from(&next);
    }
    panic!("stable pagination exceeds its live-row bound");
}

fn check_all(db: &mut ShardDb<Gated>, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut value = Vec::new();
    for (key, want) in expected {
        assert!(db.get(key, &mut value).unwrap());
        assert_eq!(&value, want);
    }
    assert_eq!(
        db.get(b"a/stable/0", &mut value).unwrap(),
        expected.contains_key(&b"a/stable/0"[..]),
        "a refused deletion preserves the old value; an applied later deletion wins"
    );
    let mut from = Vec::new();
    let mut rows = Rows::new();
    let mut next = Vec::new();
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
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            );
            db.check_references().unwrap();
            return;
        }
        assert!(next > from);
        from.clone_from(&next);
    }
    panic!("full pagination exceeds its live-row bound");
}

fn retention_case(fail: bool) {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("first");
    let second_path = dir.path().join("second");
    let (gate, events) = Gate::new(true, fail);
    let mut first = engine(&first_path, Arc::clone(&gate));
    for (key, value) in stable() {
        first.put(&key, &value).unwrap();
    }
    cold_complete(&mut first);
    first.checkpoint(0).unwrap();
    let (other, _) = Gate::new(false, false);
    let mut second = engine(&second_path, Arc::clone(&other));
    // Existing attached worker-write fixture: one held device transfer plus a live one.
    let depth = 2;
    let batches = mantle_engine::shard_db::issuer_batches(depth, depth)
        .checked_mul(2)
        .unwrap();
    let issuer = Issuer::start_for(dir.path(), depth, batches).unwrap();
    assert_eq!(
        issuer.depth(),
        depth,
        "device depth refused this fixture's held-plus-live setup"
    );
    first.attach(&issuer, depth).unwrap();
    second.attach(&issuer, depth).unwrap();
    workers(&mut first, &first_path, Arc::clone(&gate));
    workers(&mut second, &second_path, other);
    let mut runtime = Runtime::start(&RuntimeConfig {
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
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    let shard = runtime.shard_ids()[0];
    // Open before runtime/issuer teardown and before scoped work could be joined on failure.
    let _open = OpenOnDrop(Arc::clone(&gate));
    // Four simultaneous clients: writer, canceled put, canceled delete, and read/progress probe.
    let ranges = Arc::new(
        Ranges::start(
            &mut runtime,
            vec![(Vec::new(), first), (b"m".to_vec(), second)],
            RangesConfig {
                clients: 4,
                slice_ns: 50_000,
                spin_ns: 0,
            },
        )
        .unwrap(),
    );
    let before_wait = hyper_rt::registry::with_entry(shard.0, |entry| entry.pulse.waits()).unwrap();
    gate.armed.store(true, Ordering::SeqCst);
    let writer_ranges = Arc::clone(&ranges);
    let writer_gate = Arc::clone(&gate);
    let writer_ended = Arc::new(AtomicBool::new(false));
    let ended = Arc::clone(&writer_ended);
    let (done, completed) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let _terminal = Terminal(writer_gate);
            let mut client = writer_ranges.client().unwrap();
            let mut oracle = BTreeMap::new();
            let mut failure = None;
            for i in 0..OPS {
                let (key, value) = operation(i);
                let applied = match &value {
                    Some(v) => client.put_async(&key, v).await,
                    None => client.delete_async(&key).await,
                };
                if let Err(error) = applied {
                    assert!(fail && matches!(error, mantle_engine::Error::Io { .. }));
                    failure = Some(error);
                    break;
                }
                record(&mut oracle, key, value);
            }
            if fail && failure.is_none() {
                let error = client.flush_async().await.unwrap_err();
                assert!(matches!(error, mantle_engine::Error::Io { .. }));
                failure = Some(error);
            }
            assert_eq!(failure.is_some(), fail);
            drop(client);
            drop(writer_ranges);
            ended.store(true, Ordering::Release);
            done.send((oracle, failure)).unwrap();
        })
        .unwrap();
    assert_eq!(events.recv().unwrap(), Event::Held);
    // Public owner wait/announcement facts, not a sleep or a private engine-work assertion.
    // Pending CPU slices remain runnable; a fresh park with the writer unfinished exercises
    // the event wait. This is not a proof of native syscall residency.
    loop {
        assert!(
            !writer_ended.load(Ordering::Acquire),
            "input did not exercise a retained write"
        );
        if hyper_rt::registry::with_entry(shard.0, |entry| {
            entry.pulse.waits() > before_wait && entry.parking.parked()
        })
        .unwrap()
        {
            break;
        }
        std::thread::yield_now();
    }
    let (published, publications) = mpsc::sync_channel(2);
    let (put_resume, put_continue) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (put_done, put_finished) = mpsc::sync_channel(1);
    let put_ranges = Arc::clone(&ranges);
    let published_put = published.clone();
    runtime
        .spawn_on(shard, async move {
            let mut client = put_ranges.client().unwrap();
            let mut first_pending = false;
            let canceled = {
                let mut pending = pin!(client.put_async(b"a/stable/0", b"queued-before-delete"));
                race2(
                    poll_fn(|cx| {
                        let result = pending.as_mut().poll(cx);
                        first_pending |= result.is_pending();
                        result
                    }),
                    async {},
                )
                .await
            };
            assert!(first_pending && matches!(canceled, Either::Second(())));
            published_put.send("put").unwrap();
            let mut put_continue = put_continue;
            put_continue.recv().await.unwrap();
            // Recovers the original answer; it must not replay a refused mutation.
            let recovered = client.stats_async().await;
            if fail {
                assert!(matches!(recovered, Err(mantle_engine::Error::Io { .. })));
            } else {
                recovered.unwrap();
            }
            drop(client);
            drop(put_ranges);
            put_done.send(()).unwrap();
        })
        .unwrap();
    assert_eq!(publications.recv().unwrap(), "put");
    let (delete_resume, delete_continue) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (delete_done, delete_finished) = mpsc::sync_channel(1);
    let delete_ranges = Arc::clone(&ranges);
    runtime
        .spawn_on(shard, async move {
            let mut client = delete_ranges.client().unwrap();
            let mut first_pending = false;
            let canceled = {
                let mut pending = pin!(client.delete_async(b"a/stable/0"));
                race2(
                    poll_fn(|cx| {
                        let result = pending.as_mut().poll(cx);
                        first_pending |= result.is_pending();
                        result
                    }),
                    async {},
                )
                .await
            };
            assert!(first_pending && matches!(canceled, Either::Second(())));
            published.send("delete").unwrap();
            let mut delete_continue = delete_continue;
            delete_continue.recv().await.unwrap();
            let recovered = client.stats_async().await;
            if fail {
                assert!(matches!(recovered, Err(mantle_engine::Error::Io { .. })));
            } else {
                recovered.unwrap();
            }
            drop(client);
            drop(delete_ranges);
            delete_done.send(()).unwrap();
        })
        .unwrap();
    assert_eq!(publications.recv().unwrap(), "delete");
    let probe_ranges = Arc::clone(&ranges);
    let probe_gate = Arc::clone(&gate);
    let (progress, progressed) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut client = probe_ranges.client().unwrap();
            check_stable(&mut client).await;
            client
                .put_async(b"z-unrelated", b"other range during retained input")
                .await
                .unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(b"z-unrelated", &mut out).await.unwrap());
            assert_eq!(out, b"other range during retained input");
            assert!(
                probe_gate.state.lock().unwrap().active.is_some(),
                "reads progressed before release"
            );
            drop(client);
            drop(probe_ranges);
            progress.send(()).unwrap();
        })
        .unwrap();
    progressed.recv().unwrap();
    assert!(!writer_ended.load(Ordering::Acquire));
    assert!(!gate.flushed_early.load(Ordering::SeqCst));
    gate.open();
    put_resume.blocking_send(()).unwrap();
    delete_resume.blocking_send(()).unwrap();
    put_finished.recv().unwrap();
    delete_finished.recv().unwrap();
    let (mut oracle, failure) = completed.recv().unwrap();
    assert_eq!(failure.is_some(), fail);
    oracle.extend(stable());
    if !fail {
        oracle.remove(b"a/stable/0".as_slice());
    }
    // Three initial stable puts, two canceled-but-owned mutations, and the unrelated put.
    let applied = OPS + 3 + 2 + 1;
    {
        let mut client = ranges.client().unwrap();
        if fail {
            assert!(matches!(
                client.flush(),
                Err(mantle_engine::Error::Io { .. })
            ));
            assert!(matches!(
                client.checkpoint(applied),
                Err(mantle_engine::Error::Io { .. })
            ));
        } else {
            client.flush().unwrap();
            client.checkpoint(applied).unwrap();
        }
    }
    assert_eq!(gate.failures.load(Ordering::SeqCst), usize::from(fail));
    assert!(!gate.reused_early.load(Ordering::SeqCst));
    assert!(!gate.flushed_early.load(Ordering::SeqCst));
    let stopped = Arc::try_unwrap(ranges).unwrap().stop();
    if fail {
        assert!(matches!(stopped, Err(mantle_engine::Error::Io { .. })));
    } else {
        stopped.unwrap();
    }
    runtime.shutdown().unwrap();
    gate.armed.store(false, Ordering::SeqCst);
    let (mut first, index) = ShardDb::open(
        open(&first_path, false, false, gate),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(index, if fail { 0 } else { applied });
    let expected_stable = stable();
    check_all(&mut first, if fail { &expected_stable } else { &oracle });
    let (reopen_gate, _) = Gate::new(false, false);
    let (mut second, index) = ShardDb::open(
        open(&second_path, false, false, reopen_gate),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(index, if fail { 0 } else { applied });
    let mut out = Vec::new();
    assert_eq!(second.get(b"z-unrelated", &mut out).unwrap(), !fail);
    if !fail {
        assert_eq!(out, b"other range during retained input");
    }
    second.check_references().unwrap();
}

#[test]
fn retained_mutations_preserve_read_progress_fifo_cancellation_and_reopen() {
    retention_case(false);
}

#[test]
fn retained_mutations_survive_a_required_worker_write_failure_and_reopen() {
    retention_case(true);
}

/// Native handle lifetime is observable at the BlockFile seam. A closed range must return
/// every file after its physical callbacks finish; the count includes every try_clone.
struct Files {
    live: AtomicUsize,
    writes: AtomicUsize,
    retired: mpsc::SyncSender<()>,
}

struct Tracked {
    file: Gated,
    files: Arc<Files>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        if self.files.live.fetch_sub(1, Ordering::AcqRel) == 1 {
            assert_eq!(self.files.writes.load(Ordering::Acquire), 0);
            // The test may already be unwinding; returning the file must still finish.
            let _ = self.files.retired.try_send(());
        }
    }
}

struct Writing<'a>(&'a AtomicUsize);
impl Drop for Writing<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl BlockFile for Tracked {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(bytes, offset)
    }

    fn write_all_at(&self, bytes: &[u8], offset: u64) -> Result<(), DiskError> {
        self.files.writes.fetch_add(1, Ordering::AcqRel);
        let _writing = Writing(&self.files.writes);
        self.file.write_all_at(bytes, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        let file = self.file.try_clone()?;
        self.files.live.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            file,
            files: Arc::clone(&self.files),
        })
    }
}

fn tracked(path: &Path, create: bool, worker: bool, gate: Arc<Gate>, files: Arc<Files>) -> Tracked {
    let file = open(path, create, worker, gate);
    files.live.fetch_add(1, Ordering::AcqRel);
    Tracked { file, files }
}

fn terminal_case(stop: bool) {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("closing");
    let second_path = dir.path().join("still-live");
    let (gate, events) = Gate::new(true, false);
    gate.hold_remaining_writes.store(true, Ordering::SeqCst);
    let (retired, retirement) = mpsc::sync_channel(1);
    let files = Arc::new(Files {
        live: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        retired,
    });
    // Unique half-page rows: the first full table holds a quarter of this bounded input.
    // It spans far more than the two attached output runs and two extent-sized feed buffers;
    // holding every worker write prevents its input being fed whole before closure.
    let value_bytes = STORE.page_size / 2;
    let mem_bytes = usize::try_from(OPS / 4)
        .unwrap()
        .checked_mul(value_bytes)
        .unwrap();
    let mut first = ShardDb::create(
        tracked(
            &first_path,
            true,
            false,
            Arc::clone(&gate),
            Arc::clone(&files),
        ),
        STORE,
        mem_bytes,
        TRUNK,
    )
    .unwrap();
    first.put(b"a/baseline", b"durable before closure").unwrap();
    first.checkpoint(1).unwrap();
    let worker_path = first_path.clone();
    let worker_gate = Arc::clone(&gate);
    let worker_files = Arc::clone(&files);
    first
        .set_workers(move || {
            Ok(tracked(
                &worker_path,
                false,
                true,
                Arc::clone(&worker_gate),
                Arc::clone(&worker_files),
            ))
        })
        .unwrap();
    let (other_gate, _) = Gate::new(false, false);
    let mut second = engine(&second_path, Arc::clone(&other_gate));
    let depth = 2;
    let batches = mantle_engine::shard_db::issuer_batches(depth, depth)
        .checked_mul(2)
        .unwrap();
    let issuer = Issuer::start_for(dir.path(), depth, batches).unwrap();
    assert_eq!(
        issuer.depth(),
        depth,
        "device depth refused the held-plus-live fixture"
    );
    first.attach(&issuer, depth).unwrap();
    second.attach(&issuer, depth).unwrap();
    workers(&mut second, &second_path, other_gate);
    let mut runtime = Runtime::start(&RuntimeConfig {
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
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    let shard = runtime.shard_ids()[0];
    let _open = OpenOnDrop(Arc::clone(&gate));
    let closing = Arc::new(
        Ranges::start(
            &mut runtime,
            vec![(Vec::new(), first)],
            // The canceled writer's published request and, in the Stop case, its terminal caller.
            RangesConfig {
                clients: 1 + usize::from(stop),
                slice_ns: 50_000,
                spin_ns: 0,
            },
        )
        .unwrap(),
    );
    let other = Arc::new(
        Ranges::start(
            &mut runtime,
            vec![(Vec::new(), second)],
            RangesConfig {
                clients: 1,
                slice_ns: 50_000,
                spin_ns: 0,
            },
        )
        .unwrap(),
    );
    let before_wait = hyper_rt::registry::with_entry(shard.0, |entry| entry.pulse.waits()).unwrap();
    gate.armed.store(true, Ordering::SeqCst);
    let (cancel, mut cancellation) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (canceled, canceled_writer) = mpsc::sync_channel(1);
    let writer_ranges = Arc::clone(&closing);
    runtime
        .spawn_on(shard, async move {
            let mut client = writer_ranges.client().unwrap();
            let result = {
                let writes = async {
                    let value = vec![7; value_bytes];
                    for i in 0..OPS {
                        let key = format!("a/closing/{i:08}");
                        client.put_async(key.as_bytes(), &value).await.unwrap();
                    }
                };
                race2(writes, cancellation.recv()).await
            };
            assert!(
                matches!(result, Either::Second(Ok(()))),
                "input did not leave a retained mutation"
            );
            // No request is replayed: a published canceled request remains owned by the actor.
            drop(client);
            drop(writer_ranges);
            canceled.send(()).unwrap();
        })
        .unwrap();
    assert_eq!(events.recv().unwrap(), Event::Held);
    loop {
        assert!(canceled_writer.try_recv().is_err());
        if hyper_rt::registry::with_entry(shard.0, |entry| {
            entry.pulse.waits() > before_wait && entry.parking.parked()
        })
        .unwrap()
        {
            break;
        }
        std::thread::yield_now();
    }
    cancel.blocking_send(()).unwrap();
    canceled_writer.recv().unwrap();
    let before_close =
        hyper_rt::registry::with_entry(shard.0, |entry| entry.pulse.waits()).unwrap();
    let closing = Arc::try_unwrap(closing).unwrap();
    let (stopped, stop_result) = mpsc::sync_channel(1);
    if stop {
        let (published, publication) = mpsc::sync_channel(1);
        let stop_files = Arc::clone(&files);
        runtime
            .spawn_on(shard, async move {
                let mut stopping = pin!(closing.stop_async());
                poll_fn(|cx| {
                    assert!(stopping.as_mut().poll(cx).is_pending());
                    published.send(()).unwrap();
                    std::task::Poll::Ready(())
                })
                .await;
                let result = stopping.await;
                assert_eq!(
                    stop_files.live.load(Ordering::Acquire),
                    0,
                    "Stop replied before its native handles retired"
                );
                assert_eq!(stop_files.writes.load(Ordering::Acquire), 0);
                stopped.send(result).unwrap();
            })
            .unwrap();
        publication.recv().unwrap();
    } else {
        // Drop the only remaining request senders. EOF must quiesce asynchronously too.
        drop(closing);
    }
    // A new actual park after terminal admission ensures it was serviced before the progress task
    // is submitted; letting that task win a scheduling race would not prove this path.
    loop {
        assert!(retirement.try_recv().is_err());
        if hyper_rt::registry::with_entry(shard.0, |entry| {
            entry.pulse.waits() > before_close && entry.parking.parked()
        })
        .unwrap()
        {
            break;
        }
        std::thread::yield_now();
    }
    let other_task = Arc::clone(&other);
    let other_held = Arc::clone(&gate);
    let (progress, progressed) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut client = other_task.client().unwrap();
            client
                .put_async(b"other", b"progress during closure")
                .await
                .unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(b"other", &mut out).await.unwrap());
            assert_eq!(out, b"progress during closure");
            assert!(other_held.state.lock().unwrap().active.is_some());
            drop(client);
            drop(other_task);
            progress.send(()).unwrap();
        })
        .unwrap();
    progressed.recv().unwrap();
    if stop {
        assert!(
            stop_result.try_recv().is_err(),
            "Stop replied while its write remained held"
        );
    }
    assert!(
        retirement.try_recv().is_err(),
        "range retired before its physical write"
    );
    assert!(files.live.load(Ordering::Acquire) > 0);
    assert!(!gate.reused_early.load(Ordering::SeqCst));
    assert!(!gate.flushed_early.load(Ordering::SeqCst));
    gate.open();
    retirement.recv().unwrap();
    if stop {
        stop_result.recv().unwrap().unwrap();
    }
    assert_eq!(files.live.load(Ordering::Acquire), 0);
    assert_eq!(files.writes.load(Ordering::Acquire), 0);
    assert!(!gate.reused_early.load(Ordering::SeqCst));
    {
        let mut client = other.client().unwrap();
        client.checkpoint(1).unwrap();
    }
    Arc::try_unwrap(other).unwrap().stop().unwrap();
    runtime.shutdown().unwrap();
    gate.armed.store(false, Ordering::SeqCst);
    let (mut recovered, applied) = ShardDb::open(
        open(&first_path, false, false, gate),
        STORE,
        mem_bytes,
        TRUNK,
    )
    .unwrap();
    assert_eq!(applied, 1);
    let expected = BTreeMap::from([(b"a/baseline".to_vec(), b"durable before closure".to_vec())]);
    check_all(&mut recovered, &expected);
}

#[test]
fn dropped_ranges_close_unfinished_feeds_and_retire_after_held_writes() {
    terminal_case(false);
}

#[test]
fn stop_waits_for_held_worker_writes_and_native_handle_retirement_before_reply() {
    terminal_case(true);
}

fn direct_terminal_case(fail: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("direct");
    let (gate, events) = Gate::new(true, fail);
    gate.hold_remaining_writes.store(true, Ordering::SeqCst);
    let (retired, retirement) = mpsc::sync_channel(1);
    let files = Arc::new(Files {
        live: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        retired,
    });
    let value_bytes = STORE.page_size / 2;
    let mem_bytes = usize::try_from(OPS / 4)
        .unwrap()
        .checked_mul(value_bytes)
        .unwrap();
    let value = vec![7; value_bytes];
    // Public capacity behavior supplies the put bound; no entry-header/layout estimate is
    // used. Identical-length keys make the next table fit the same number of entries.
    let mut probe = HashMem::new(mem_bytes).unwrap();
    let mut fits = None;
    for i in 0..OPS {
        let key = format!("a/closing/{i:08}");
        match probe.insert(key.as_bytes(), Op::Put, &value) {
            Ok(()) => {}
            Err(mantle_engine::error::Error::LimitExceeded { .. }) => {
                fits = Some(i);
                break;
            }
            Err(error) => panic!("capacity probe refused valid data: {error:?}"),
        }
    }
    let fits = fits.expect("half-page values must fill the input-derived arena");
    assert!(fits > 0);
    let puts = fits.checked_mul(2).unwrap();
    assert!(
        puts < OPS,
        "the inherited input bound must contain both tables"
    );
    drop(probe);
    let mut db = ShardDb::create(
        tracked(&path, true, false, Arc::clone(&gate), Arc::clone(&files)),
        STORE,
        mem_bytes,
        TRUNK,
    )
    .unwrap();
    db.put(b"a/baseline", b"durable before closure").unwrap();
    db.checkpoint(1).unwrap();
    let before = db.stats().0;
    let worker_path = path.clone();
    let worker_gate = Arc::clone(&gate);
    let worker_files = Arc::clone(&files);
    db.set_workers(move || {
        Ok(tracked(
            &worker_path,
            false,
            true,
            Arc::clone(&worker_gate),
            Arc::clone(&worker_files),
        ))
    })
    .unwrap();
    let depth = 2;
    let issuer = Issuer::start_for(
        dir.path(),
        depth,
        mantle_engine::shard_db::issuer_batches(depth, depth),
    )
    .unwrap();
    assert_eq!(
        issuer.depth(),
        depth,
        "device depth refused the held fixture"
    );
    db.attach(&issuer, depth).unwrap();
    // Keep cleanup inside the scoped closure: it opens before a failing scope joins its
    // thread. No watchdog release can satisfy a successful retirement verdict.
    std::thread::scope(|scope| {
        let _open = OpenOnDrop(Arc::clone(&gate));
        gate.armed.store(true, Ordering::SeqCst);
        for i in 0..puts {
            let key = format!("a/closing/{i:08}");
            db.put(key.as_bytes(), &value).unwrap();
            if gate.state.lock().unwrap().selected {
                break;
            }
        }
        // The first overflow rotates; the remaining puts fit the new table, so none can
        // enter a second rotation's blocking finish. Their pack share uses wait=false.
        assert_eq!(db.stats().0.rotations, before.rotations + 1);
        assert_eq!(events.recv().unwrap(), Event::Held);
        assert_eq!(
            db.stats().0.flushes,
            before.flushes,
            "the held table was not already retired before teardown"
        );
        let (started, entered) = mpsc::sync_channel(1);
        let (handoff, returned) = mpsc::sync_channel(1);
        scope.spawn(move || {
            started.send(()).unwrap();
            assert!(
                handoff.send(finished_file(db.into_file())).is_ok(),
                "native file handoff was dropped"
            );
        });
        entered.recv().unwrap();
        assert!(
            returned.try_recv().is_err(),
            "file returned before its physical write retired"
        );
        assert!(files.writes.load(Ordering::Acquire) > 0);
        assert!(retirement.try_recv().is_err());
        gate.open();
        let (file, landed) = returned.recv().unwrap();
        // Exactly the file handed to us remains: every worker and issuer duplicate retired.
        assert_eq!(files.live.load(Ordering::Acquire), 1);
        assert_eq!(files.writes.load(Ordering::Acquire), 0);
        if fail {
            assert!(
                matches!(&landed, Err(mantle_engine::error::Error::Io { .. })),
                "physical write refusal must survive intentional partial-input cancellation: {landed:?}"
            );
        } else {
            landed.unwrap();
        }
        assert_eq!(gate.failures.load(Ordering::SeqCst), usize::from(fail));
        assert!(!gate.reused_early.load(Ordering::SeqCst));
        assert!(!gate.flushed_early.load(Ordering::SeqCst));
        drop(file);
        retirement.recv().unwrap();
    });
    drop(issuer);
    gate.armed.store(false, Ordering::SeqCst);
    let (mut recovered, applied) =
        ShardDb::open(open(&path, false, false, gate), STORE, mem_bytes, TRUNK).unwrap();
    assert_eq!(applied, 1);
    let expected = BTreeMap::from([(b"a/baseline".to_vec(), b"durable before closure".to_vec())]);
    check_all(&mut recovered, &expected);
}

#[test]
fn direct_into_file_closes_partial_input_before_joining_its_worker() {
    direct_terminal_case(false);
}

#[test]
fn direct_into_file_reports_physical_failure_after_partial_input_and_callback_retirement() {
    direct_terminal_case(true);
}
