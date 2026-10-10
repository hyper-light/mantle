//! Public native worker-write ownership and same-shard progress oracles.
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
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::branch::Op;
use mantle_engine::memtable::hashed::HashMem;
use mantle_engine::ranges::{Client, Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

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

fn check(db: &mut ShardDb<Gated>, oracle: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut value = Vec::new();
    for i in 0..67 {
        let key = key(i);
        assert_eq!(db.get(&key, &mut value).unwrap(), oracle.contains_key(&key));
        if let Some(want) = oracle.get(&key) {
            assert_eq!(&value, want);
        }
    }
    let mut from = Vec::new();
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let mut got = Vec::new();
    for _ in 0..=oracle.len() {
        rows.clear();
        let more = db.scan(&from, None, 3, &mut rows, &mut next).unwrap();
        got.extend(
            rows.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        if !more {
            assert_eq!(
                got,
                oracle
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
    panic!("pagination did not finish within one page per live row plus the end");
}

#[test]
fn held_worker_output_and_one_typed_write_failure_preserve_acknowledged_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let (gate, events) = Gate::new(false, true);
    let mut db = engine(&path, Arc::clone(&gate));
    let mut durable = BTreeMap::new();
    db.put(&key(1), b"last durable value").unwrap();
    durable.insert(key(1), b"last durable value".to_vec());
    db.checkpoint(1).unwrap();
    workers(&mut db, &path, Arc::clone(&gate));
    // The public memtable capacity accepts this complete input without a rotation. The
    // failure is discovered by flush, so every recorded mutation was acknowledged first.
    let mut capacity = HashMem::new(STORE.page_size).unwrap();
    let mut oracle = durable.clone();
    let mut accepted = 0;
    for i in 0..OPS {
        let (k, value) = operation(i);
        let (op, bytes) = value
            .as_ref()
            .map_or((Op::Delete, &[][..]), |v| (Op::Put, v.as_slice()));
        match capacity.insert(&k, op, bytes) {
            Ok(()) => {}
            Err(mantle_engine::Error::LimitExceeded { .. }) => break,
            Err(error) => panic!("capacity refused an otherwise valid input: {error}"),
        }
        match &value {
            Some(v) => db.put(&k, v).unwrap(),
            None => db.delete(&k).unwrap(),
        }
        record(&mut oracle, k, value);
        accepted += 1;
    }
    assert!(
        accepted >= 4,
        "the input includes empty, short and long values"
    );
    check(&mut db, &oracle);
    gate.armed.store(true, Ordering::SeqCst);
    let (mut db, failure) = std::thread::scope(|scope| {
        let worker_gate = Arc::clone(&gate);
        let task = scope.spawn(move || {
            let _terminal = Terminal(worker_gate);
            let result = db.flush();
            (db, result)
        });
        // Opens the physical write before the scope joins on an assertion failure.
        let _open = OpenOnDrop(Arc::clone(&gate));
        assert_eq!(events.recv().unwrap(), Event::Held);
        gate.open();
        task.join().unwrap()
    });
    assert!(matches!(failure, Err(mantle_engine::Error::Io { .. })));
    assert_eq!(gate.failures.load(Ordering::SeqCst), 1);
    assert_eq!(
        events
            .try_iter()
            .filter(|event| matches!(
                event,
                Event::Failed(
                    "one worker page write rejected by test",
                    std::io::ErrorKind::Other
                )
            ))
            .count(),
        1
    );
    assert!(!gate.reused_early.load(Ordering::SeqCst));
    assert!(!gate.flushed_early.load(Ordering::SeqCst));
    check(&mut db, &oracle);
    assert!(matches!(
        db.put(b"b-after-failure", b"must not apply"),
        Err(mantle_engine::Error::Io { .. })
    ));
    assert!(matches!(
        db.delete(&key(1)),
        Err(mantle_engine::Error::Io { .. })
    ));
    assert!(matches!(
        db.checkpoint(2),
        Err(mantle_engine::Error::Io { .. })
    ));
    check(&mut db, &oracle);
    let (file, landed) = finished_file(db.into_file());
    assert!(matches!(landed, Err(mantle_engine::Error::Io { .. })));
    drop(file);
    gate.armed.store(false, Ordering::SeqCst);
    let (mut db, applied) = ShardDb::open(
        open(&path, false, false, gate),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(applied, 1);
    check(&mut db, &durable);
    db.put(b"b-after-reopen", b"healthy after worker refusal")
        .unwrap();
    durable.insert(
        b"b-after-reopen".to_vec(),
        b"healthy after worker refusal".to_vec(),
    );
    db.checkpoint(2).unwrap();
    check(&mut db, &durable);
}

async fn check_client(client: &mut Client<'_>, oracle: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut out = Vec::new();
    for i in 0..67 {
        let key = key(i);
        assert_eq!(
            client.get_async(&key, &mut out).await.unwrap(),
            oracle.contains_key(&key)
        );
        if let Some(value) = oracle.get(&key) {
            assert_eq!(&out, value);
        }
    }
    let mut from = Vec::new();
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let mut got = Vec::new();
    for _ in 0..=oracle.len() {
        rows.clear();
        let more = client
            .scan_async(&from, Some(b"m"), 3, &mut rows, &mut next)
            .await
            .unwrap();
        got.extend(
            rows.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        if !more {
            assert_eq!(
                got,
                oracle
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            );
            return;
        }
        assert!(next > from);
        from.clone_from(&next);
    }
    panic!("range pagination did not finish within its live-row bound");
}

#[test]
fn a_held_worker_page_write_leaves_same_shard_unrelated_range_progress() {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("first");
    let second_path = dir.path().join("second");
    let (gate, events) = Gate::new(true, false);
    let mut first = engine(&first_path, Arc::clone(&gate));
    let (other, _) = Gate::new(false, false);
    let mut second = engine(&second_path, Arc::clone(&other));
    // The existing native worker suite's depth: a held transfer and another transfer.
    // Both ranges use the same device issuer; reserve every submitter's declared batches.
    let depth = 2;
    let batches = mantle_engine::shard_db::issuer_batches(depth, depth)
        .checked_mul(2)
        .unwrap();
    let issuer = Issuer::start_for(dir.path(), depth, batches).unwrap();
    first.attach(&issuer, depth).unwrap();
    second.attach(&issuer, depth).unwrap();
    workers(&mut first, &first_path, Arc::clone(&gate));
    workers(&mut second, &second_path, other);
    let mut runtime = Runtime::start(&RuntimeConfig {
        // The existing native Range fixture's declared bounds/timing, with one shard.
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
    // Opens before Runtime joins, even if the RED synchronous-write witness asserts.
    let _open = OpenOnDrop(Arc::clone(&gate));
    let ranges = Arc::new(
        Ranges::start(
            &mut runtime,
            vec![(Vec::new(), first), (b"m".to_vec(), second)],
            RangesConfig {
                clients: 2,
                slice_ns: 50_000,
                spin_ns: 0,
            },
        )
        .unwrap(),
    );
    gate.armed.store(true, Ordering::SeqCst);
    let writer_ranges = Arc::clone(&ranges);
    let writer_gate = Arc::clone(&gate);
    let (done, completed) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let _terminal = Terminal(writer_gate);
            let mut client = writer_ranges.client().unwrap();
            let mut oracle = BTreeMap::new();
            for i in 0..OPS {
                let (key, value) = operation(i);
                match &value {
                    Some(v) => client.put_async(&key, v).await.unwrap(),
                    None => client.delete_async(&key).await.unwrap(),
                }
                record(&mut oracle, key, value);
            }
            client.flush_async().await.unwrap();
            client.checkpoint_async(OPS).await.unwrap();
            check_client(&mut client, &oracle).await;
            drop(client);
            drop(writer_ranges);
            done.send(oracle).unwrap();
        })
        .unwrap();
    // The old synchronous worker path reports Forbidden and fails promptly. Only a real
    // off-producer held write allows the unrelated-progress part of this oracle to begin.
    assert_eq!(events.recv().unwrap(), Event::Held);
    eprintln!("[MANTLE-AUDIT] issuer-attached worker output is held");
    let probe_ranges = Arc::clone(&ranges);
    let probe_gate = Arc::clone(&gate);
    let (progress, progressed) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut client = probe_ranges.client().unwrap();
            client
                .put_async(b"z-unrelated", b"while first worker output is held")
                .await
                .unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(b"z-unrelated", &mut out).await.unwrap());
            assert_eq!(out, b"while first worker output is held");
            drop(client);
            drop(probe_ranges);
            probe_gate.open();
            progress.send(()).unwrap();
        })
        .unwrap();
    progressed.recv().unwrap();
    eprintln!("[MANTLE-AUDIT] unrelated same-shard range progressed before release");
    let oracle = completed.recv().unwrap();
    assert!(!gate.reused_early.load(Ordering::SeqCst));
    assert!(!gate.flushed_early.load(Ordering::SeqCst));
    Arc::try_unwrap(ranges).unwrap().stop().unwrap();
    runtime.shutdown().unwrap();
    gate.armed.store(false, Ordering::SeqCst);
    let (mut db, applied) = ShardDb::open(
        open(&first_path, false, false, gate),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(applied, OPS);
    check(&mut db, &oracle);
    let (reopen_gate, _) = Gate::new(false, false);
    let (mut second, applied) = ShardDb::open(
        open(&second_path, false, false, reopen_gate),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(applied, OPS);
    let mut out = Vec::new();
    assert!(second.get(b"z-unrelated", &mut out).unwrap());
    assert_eq!(out, b"while first worker output is held");
    second.check_references().unwrap();
}
