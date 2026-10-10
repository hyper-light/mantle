//! Cold Range queries retain one request while other actors progress on the same shard.
//! Run with an external failure-only deadline; no timeout or sleep releases successful I/O.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::path::Path;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::task::Poll;

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

// The existing native Range shape and branch_test's finite leaf/index crossing dataset.
const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const ROWS: usize = 2049;
const SLICE_NS: u64 = 50_000;

type Oracle = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Held,
    Forbidden(&'static str),
}

struct Gate {
    armed: AtomicBool,
    selected: AtomicBool,
    open: Mutex<bool>,
    changed: Condvar,
    fail: AtomicBool,
    reads: AtomicUsize,
    forbidden: AtomicBool,
    events: mpsc::SyncSender<Event>,
}
impl Gate {
    fn new(fail: bool) -> (Arc<Self>, mpsc::Receiver<Event>) {
        // A selected read and a forbidden owner call each report at most once.
        let (events, received) = mpsc::sync_channel(2);
        (
            Arc::new(Self {
                armed: AtomicBool::new(false),
                selected: AtomicBool::new(false),
                open: Mutex::new(true),
                changed: Condvar::new(),
                fail: AtomicBool::new(fail),
                reads: AtomicUsize::new(0),
                forbidden: AtomicBool::new(false),
                events,
            }),
            received,
        )
    }
    fn release(&self) {
        *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
    fn owner_call(&self, operation: &'static str) -> Result<(), DiskError> {
        if hyper_rt::futures::current_task().is_some() {
            if !self.forbidden.swap(true, Ordering::SeqCst) {
                self.events.try_send(Event::Forbidden(operation)).unwrap();
            }
            return Err(refused(operation));
        }
        Ok(())
    }
}
struct OpenOnDrop(Option<Arc<Gate>>);
impl OpenOnDrop {
    fn new(gate: Arc<Gate>) -> Self {
        Self(Some(gate))
    }
    fn disarm(&mut self) {
        self.0 = None;
    }
}
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        if let Some(gate) = &self.0 {
            gate.release();
        }
    }
}
struct Gated {
    file: DeviceFile,
    gate: Arc<Gate>,
}
impl BlockFile for Gated {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.gate.owner_call("a device read on a runtime task")?;
        if self.gate.armed.load(Ordering::Acquire) {
            self.gate.reads.fetch_add(1, Ordering::SeqCst);
            if !self.gate.selected.swap(true, Ordering::SeqCst) {
                let mut open = self.gate.open.lock().unwrap();
                self.gate.events.try_send(Event::Held).unwrap();
                while !*open {
                    open = self.gate.changed.wait(open).unwrap();
                }
                if self.gate.fail.swap(false, Ordering::SeqCst) {
                    return Err(refused("the required point page failed"));
                }
            }
        }
        self.file.read_exact_at(bytes, offset)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> Result<(), DiskError> {
        self.gate.owner_call("a device write on a runtime task")?;
        self.file.write_all_at(bytes, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.gate.owner_call("a device sync on a runtime task")?;
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: Arc::clone(&self.gate),
        })
    }
}
fn refused(operation: &'static str) -> DiskError {
    DiskError::Io {
        op: operation,
        path: std::path::PathBuf::new(),
        source: std::io::Error::other(operation),
    }
}
fn open(path: &Path, create: bool, gate: Arc<Gate>) -> Gated {
    Gated {
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap(),
        gate,
    }
}
fn engine(path: &Path, gate: Arc<Gate>) -> ShardDb<Gated> {
    let mut db = ShardDb::create(
        open(path, true, Arc::clone(&gate)),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    db.set_cache(0);
    db
}
fn workers(db: &mut ShardDb<Gated>, path: &Path, gate: Arc<Gate>, issuer: &Issuer) {
    // Prepopulation/checkpoint were cold and used no Pool. Every worker is still a fresh
    // seat when Range admission validates/prepares it; attach precedes its first job.
    db.attach(issuer, issuer.depth()).unwrap();
    let path = path.to_path_buf();
    db.set_workers(move || Ok(open(&path, false, Arc::clone(&gate))))
        .unwrap();
}
fn key(i: usize) -> Vec<u8> {
    format!("a/shared-prefix/object-{i:05}\0").into_bytes()
}
fn original(i: usize) -> Vec<u8> {
    vec![(i % 251) as u8; 100]
}
fn populate(db: &mut ShardDb<Gated>) -> Oracle {
    let mut oracle = Oracle::new();
    for i in 0..ROWS {
        if i % 7 == 0 {
            db.delete(&key(i)).unwrap();
        } else {
            let value = original(i);
            db.put(&key(i), &value).unwrap();
            oracle.insert(key(i), value);
        }
    }
    db.flush().unwrap();
    db.maintain(u64::MAX).unwrap();
    db.checkpoint(7).unwrap();
    db.maintain(u64::MAX).unwrap();
    assert!(!db.owed(), "cold-query setup leaves no background work");
    db.check_references().unwrap();
    oracle
}
fn runtime() -> Runtime {
    Runtime::start(&RuntimeConfig {
        shards: 1,
        // Two Range owners, one query, one queued mutation, one independent progress task.
        tasks_per_shard: 5,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: SLICE_NS,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}
fn ranges(runtime: &mut Runtime, first: ShardDb<Gated>, second: ShardDb<Gated>) -> Arc<Ranges> {
    let ranges = Arc::new(
        Ranges::start(
            runtime,
            vec![(Vec::new(), first), (b"m".to_vec(), second)],
            RangesConfig {
                clients: 3,
                slice_ns: SLICE_NS,
                spin_ns: 0,
                inline: false,
            },
        )
        .unwrap(),
    );
    // A public round trip proves both actors passed admission before the read gate is armed.
    ranges.client().unwrap().stats().unwrap();
    ranges
}
fn verify(path: &Path, gate: Arc<Gate>, expected: &Oracle, applied: u64) {
    let (mut db, found_applied) =
        ShardDb::open(open(path, false, gate), STORE, STORE.page_size, TRUNK).unwrap();
    assert_eq!(found_applied, applied);
    let mut value = Vec::new();
    for (key, expected) in expected {
        assert!(db.get(key, &mut value).unwrap());
        assert_eq!(&value, expected);
    }
    assert!(!db.get(&key(0), &mut value).unwrap());
    assert!(!db.get(&key(ROWS), &mut value).unwrap());
    let mut at = Vec::new();
    let mut next = Vec::new();
    let mut rows = Rows::new();
    let mut got = Vec::new();
    // Pagination must move on every continuation, bounded by the actual live-row oracle.
    for _ in 0..=expected.len() {
        rows.clear();
        let more = db.scan(&at, None, 3, &mut rows, &mut next).unwrap();
        got.extend(
            rows.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        if !more {
            assert_eq!(got, expected.clone().into_iter().collect::<Vec<_>>());
            db.check_references().unwrap();
            return;
        }
        assert!(next > at);
        at.clone_from(&next);
    }
    panic!("durable pagination exceeded its live-row bound");
}
fn other_progress(runtime: &Runtime, ranges: Arc<Ranges>, gate: Arc<Gate>) {
    let (done, received) = mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut open = OpenOnDrop::new(gate);
            let mut client = ranges.client().unwrap();
            client.put_async(b"z/progress", b"progress").await.unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(b"z/progress", &mut out).await.unwrap());
            assert_eq!(out, b"progress");
            drop(client);
            drop(ranges);
            // Normal completion witnesses progress without releasing the target read.
            open.disarm();
            done.send(()).unwrap();
        })
        .unwrap();
    received.recv().unwrap();
}

fn held_query(fail: bool, scan: bool) {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first"), dir.path().join("second")];
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    assert_eq!(
        issuer.depth(),
        2,
        "held-plus-live setup needs two device workers"
    );
    let (gate, events) = Gate::new(fail);
    let (other, _) = Gate::new(false);
    let mut first = engine(&paths[0], Arc::clone(&gate));
    let mut expected = populate(&mut first);
    let mut second = engine(&paths[1], Arc::clone(&other));
    second.checkpoint(7).unwrap();
    workers(&mut first, &paths[0], Arc::clone(&gate), &issuer);
    workers(&mut second, &paths[1], Arc::clone(&other), &issuer);
    let mut runtime = runtime();
    let _open = OpenOnDrop::new(Arc::clone(&gate));
    let ranges = ranges(&mut runtime, first, second);
    *gate.open.lock().unwrap() = false;
    gate.armed.store(true, Ordering::Release);
    let query = key(ROWS / 2);
    let page_limit = usize::try_from(TRUNK.leaf_entries).unwrap();
    let scan_expected: Vec<_> = expected
        .range(query.clone()..)
        .take(page_limit)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let scan_next = expected
        .range(query.clone()..)
        .nth(page_limit)
        .map(|(key, _)| key.clone());
    let reader_ranges = Arc::clone(&ranges);
    let reader_gate = Arc::clone(&gate);
    let read_query = query.clone();
    let (read_done, read_result) = mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let _open = OpenOnDrop::new(reader_gate);
            let mut client = reader_ranges.client().unwrap();
            let mut out = vec![99];
            let mut rows = Rows::new();
            let mut next = Vec::new();
            let result = if scan {
                client
                    .scan_async(
                        &read_query,
                        Some(&key(ROWS)),
                        page_limit,
                        &mut rows,
                        &mut next,
                    )
                    .await
            } else {
                client.get_async(&read_query, &mut out).await
            };
            drop(client);
            drop(reader_ranges);
            read_done.send((result, out, rows, next)).unwrap();
        })
        .unwrap();
    assert_eq!(events.recv().unwrap(), Event::Held);
    let put_ack = Arc::new(AtomicBool::new(false));
    let ack = Arc::clone(&put_ack);
    let writer_ranges = Arc::clone(&ranges);
    let write_query = query.clone();
    let (published, publication) = mpsc::sync_channel(1);
    let (written, write_done) = mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = writer_ranges.client().unwrap();
            {
                let mut request = pin!(client.put_async(&write_query, b"new-after-held-query"));
                poll_fn(|cx| {
                    assert!(request.as_mut().poll(cx).is_pending());
                    published.send(()).unwrap();
                    Poll::Ready(())
                })
                .await;
                request.await.unwrap();
            }
            ack.store(true, Ordering::Release);
            drop(client);
            drop(writer_ranges);
            written.send(()).unwrap();
        })
        .unwrap();
    publication.recv().unwrap();
    other_progress(&runtime, Arc::clone(&ranges), Arc::clone(&gate));
    assert!(
        !put_ack.load(Ordering::Acquire),
        "a mutation was acknowledged before its prior query ended"
    );
    // The unrelated task completed first; only now may physical target I/O retire.
    gate.release();
    let (result, out, rows, next) = read_result.recv().unwrap();
    if fail {
        assert!(matches!(result, Err(Error::Io { .. })));
        assert_eq!(out, [99]);
        assert!(rows.is_empty());
        assert!(next.is_empty());
    } else if scan {
        assert!(result.unwrap());
        assert_eq!(
            rows.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect::<Vec<_>>(),
            scan_expected
        );
        assert_eq!(Some(next), scan_next);
    } else {
        assert!(result.unwrap());
        assert_eq!(out, original(ROWS / 2));
    }
    write_done.recv().unwrap();
    expected.insert(query.clone(), b"new-after-held-query".to_vec());
    // Read failure is local to the query: repair/reuse and durability still work.
    let mut client = ranges.client().unwrap();
    let mut repaired = Vec::new();
    assert!(client.get(&query, &mut repaired).unwrap());
    assert_eq!(repaired, b"new-after-held-query");
    assert!(client.get(&key(ROWS / 2 + 1), &mut repaired).unwrap());
    assert_eq!(repaired, original(ROWS / 2 + 1));
    client.checkpoint(9).unwrap();
    drop(client);
    assert!(!gate.forbidden.load(Ordering::SeqCst));
    Arc::try_unwrap(ranges).unwrap().stop().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    verify(&paths[0], gate, &expected, 9);
    verify(
        &paths[1],
        other,
        &Oracle::from([(b"z/progress".to_vec(), b"progress".to_vec())]),
        9,
    );
}

#[test]
fn cold_get_yields_and_excludes_queued_mutation_until_exact_checkpoint_reopen() {
    held_query(false, false);
}
#[test]
fn required_get_failure_preserves_typed_reply_then_repair_and_exact_checkpoint_reopen() {
    held_query(true, false);
}

#[test]
fn cold_scan_yields_and_excludes_queued_mutation_without_replaying_its_page() {
    held_query(false, true);
}

#[test]
fn required_scan_failure_returns_owners_then_repairs_and_reopens_exact_data() {
    held_query(true, true);
}

#[test]
fn canceled_client_get_borrow_keeps_one_query_and_finishes_canceled_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first"), dir.path().join("second")];
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    assert_eq!(
        issuer.depth(),
        2,
        "held-plus-live setup needs two device workers"
    );
    let (gate, events) = Gate::new(false);
    let (other, _) = Gate::new(false);
    let mut first = engine(&paths[0], Arc::clone(&gate));
    let expected = populate(&mut first);
    let mut second = engine(&paths[1], Arc::clone(&other));
    second.checkpoint(7).unwrap();
    workers(&mut first, &paths[0], Arc::clone(&gate), &issuer);
    workers(&mut second, &paths[1], Arc::clone(&other), &issuer);
    let mut runtime = runtime();
    let _open = OpenOnDrop::new(Arc::clone(&gate));
    let ranges = ranges(&mut runtime, first, second);
    *gate.open.lock().unwrap() = false;
    gate.armed.store(true, Ordering::Release);
    let (resume, mut resumed) = hyper_rt::sync::channel(1).unwrap();
    let (published, publication) = mpsc::sync_channel(1);
    let (done, completed) = mpsc::sync_channel(1);
    let reader_ranges = Arc::clone(&ranges);
    let reader_gate = Arc::clone(&gate);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let _open = OpenOnDrop::new(Arc::clone(&reader_gate));
            let mut client = reader_ranges.client().unwrap();
            let mut value = vec![99];
            {
                let query = key(ROWS / 2);
                let mut request = pin!(client.get_async(&query, &mut value));
                poll_fn(|cx| {
                    assert!(request.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            assert_eq!(value, [99]);
            published.send(()).unwrap();
            resumed.recv().await.unwrap();
            // Stats reclaims the original reply, without issuing another Get.
            client.stats_async().await.unwrap();
            assert_eq!(
                reader_gate.reads.load(Ordering::SeqCst),
                1,
                "canceling a client borrow replayed the cold page"
            );
            {
                let mut checkpoint = pin!(client.checkpoint_async(9));
                poll_fn(|cx| {
                    assert!(checkpoint.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            // Completes every undispatched range of the same canceled checkpoint.
            client.stats_async().await.unwrap();
            drop(client);
            drop(reader_ranges);
            done.send(()).unwrap();
        })
        .unwrap();
    publication.recv().unwrap();
    assert_eq!(events.recv().unwrap(), Event::Held);
    other_progress(&runtime, Arc::clone(&ranges), Arc::clone(&gate));
    gate.release();
    resume.try_send(()).unwrap();
    completed.recv().unwrap();
    assert!(!gate.forbidden.load(Ordering::SeqCst));
    Arc::try_unwrap(ranges).unwrap().stop().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    verify(&paths[0], gate, &expected, 9);
    verify(
        &paths[1],
        other,
        &Oracle::from([(b"z/progress".to_vec(), b"progress".to_vec())]),
        9,
    );
}

#[test]
fn synchronous_client_calls_refuse_before_mutation_and_async_pagination_reuses_the_client() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first"), dir.path().join("second")];
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    let (gate, _) = Gate::new(false);
    let (other, _) = Gate::new(false);
    let mut first = engine(&paths[0], Arc::clone(&gate));
    let expected = populate(&mut first);
    let mut second = engine(&paths[1], Arc::clone(&other));
    second.checkpoint(7).unwrap();
    workers(&mut first, &paths[0], Arc::clone(&gate), &issuer);
    workers(&mut second, &paths[1], other, &issuer);
    let mut runtime = runtime();
    let ranges = ranges(&mut runtime, first, second);
    let task_ranges = Arc::clone(&ranges);
    let (done, received) = mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = task_ranges.client().unwrap();
            let mut value = vec![99];
            assert!(matches!(
                client.get(&key(ROWS / 2), &mut value),
                Err(Error::InvalidArgument { .. })
            ));
            assert_eq!(value, [99]);
            assert!(matches!(
                client.put(b"a/refused", b"not-published"),
                Err(Error::InvalidArgument { .. })
            ));
            let mut rows = Rows::new();
            rows.push(b"kept", b"untouched");
            let mut next = vec![99];
            assert!(matches!(
                client.scan(b"", None, 3, &mut rows, &mut next),
                Err(Error::InvalidArgument { .. })
            ));
            assert_eq!(rows.get(0), Some((&b"kept"[..], &b"untouched"[..])));
            assert_eq!(rows.len(), 1);
            assert_eq!(next, [99]);
            assert!(client.get_async(&key(ROWS / 2), &mut value).await.unwrap());
            assert_eq!(value, original(ROWS / 2));
            assert!(!client.get_async(b"a/refused", &mut value).await.unwrap());
            let mut at = Vec::new();
            let mut got = Vec::new();
            let mut ended = false;
            for _ in 0..=expected.len() {
                rows.clear();
                let more = client
                    .scan_async(&at, Some(b"m"), 3, &mut rows, &mut next)
                    .await
                    .unwrap();
                got.extend(
                    rows.iter()
                        .map(|(key, value)| (key.to_vec(), value.to_vec())),
                );
                if !more {
                    ended = true;
                    break;
                }
                assert!(next > at);
                at.clone_from(&next);
            }
            assert!(ended, "pagination exceeded its live-row bound");
            assert_eq!(got, expected.into_iter().collect::<Vec<_>>());
            drop(client);
            drop(task_ranges);
            done.send(()).unwrap();
        })
        .unwrap();
    received.recv().unwrap();
    assert!(!gate.forbidden.load(Ordering::SeqCst));
    Arc::try_unwrap(ranges).unwrap().stop().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
}
