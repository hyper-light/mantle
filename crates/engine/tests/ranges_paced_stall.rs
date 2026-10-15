//! Public Range request retention, cancellation, ordering and same-shard progress.
//! Run under an external controller deadline; no clock decides a successful verdict. The
//! retention cases' waits for facts also have a failure guard ([`WAIT`]) that a passing run never
//! reaches: past it the case fails, naming the fact that did not come, and its held write is let
//! go before the runtime's teardown.
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
use std::time::{Duration, Instant};

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
/// The failure guard of a wait for a fact, the native worker fixtures' (pool_lifecycle.rs): a
/// passing run never waits for it, and elapsed time never counts as the fact.
const WAIT: Duration = Duration::from_secs(5);

/// Waits for `fact`, yielding between looks; past the failure guard the test fails, naming it.
fn until(what: &str, mut fact: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !fact() {
        assert!(
            Instant::now() < deadline,
            "{what}: not seen within the failure guard"
        );
        std::thread::yield_now();
    }
}

/// A message from the run's tasks, waited for under the failure guard.
fn received<T>(from: &mpsc::Receiver<T>, what: &str) -> T {
    from.recv_timeout(WAIT)
        .unwrap_or_else(|e| panic!("{what}: not received within the failure guard ({e})"))
}

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

// Three stable rows exercise exact reads and one-row pagination while a worker write is held. No
// request of the test changes them, so each read of them has one answer whenever the range applies
// the mutations it owns.
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

// The one key the canceled put and delete name, outside the stable rows' prefix. Published in that
// order, each is the range's to apply from then on, in its turn: a read in the meantime finds the
// key in one of these states, and never in an earlier one after a later.
const TARGET: &[u8] = b"a/target";
const TARGET_BEFORE: &[u8] = b"before-target";
const TARGET_QUEUED: &[u8] = b"queued-before-delete";
const TARGET_STATES: [Option<&[u8]>; 3] = [Some(TARGET_BEFORE), Some(TARGET_QUEUED), None];

/// A page's start, its rows, the next start it gave and whether more remained, kept for a failure
/// to name (every key and value here is text).
type Page = (String, Vec<(String, String)>, String, bool);

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Every row in [from, end), one row a page, as the cursor gives them: each next start beyond its
/// page's own, and no more pages than `live` rows and the last.
async fn one_row_pages(
    client: &mut Client<'_>,
    from: &[u8],
    end: &[u8],
    live: usize,
) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<Page>) {
    let mut from = from.to_vec();
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let mut got = Vec::new();
    let mut pages = Vec::new();
    for _ in 0..=live {
        rows.clear();
        let more = client
            .scan_async(&from, Some(end), 1, &mut rows, &mut next)
            .await
            .unwrap();
        let page: Vec<(Vec<u8>, Vec<u8>)> = rows
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect();
        pages.push((
            text(&from),
            page.iter()
                .map(|(key, value)| (text(key), text(value)))
                .collect(),
            text(&next),
            more,
        ));
        got.extend(page);
        if !more {
            return (got, pages);
        }
        assert!(next > from, "a next start not beyond its page's: {pages:?}");
        from.clone_from(&next);
    }
    panic!("pagination exceeds its live-row bound: {pages:?}");
}

/// The target's state, its index in [`TARGET_STATES`]: read by a get, or by one-row pages of the
/// target's key alone. Any other answer fails, named.
async fn target_state(client: &mut Client<'_>, by_scan: bool) -> usize {
    let held = if by_scan {
        let (rows, pages) = one_row_pages(client, TARGET, b"a/target\0", 1).await;
        match rows.as_slice() {
            [] => None,
            [(key, value)] if key == TARGET => Some(value.clone()),
            _ => panic!("a scan of the target alone gave {pages:?}"),
        }
    } else {
        let mut value = Vec::new();
        if client.get_async(TARGET, &mut value).await.unwrap() {
            Some(value)
        } else {
            None
        }
    };
    match TARGET_STATES
        .iter()
        .position(|state| *state == held.as_deref())
    {
        Some(state) => state,
        None => panic!(
            "the target holds {:?}, none of its states",
            held.as_deref().map(text)
        ),
    }
}

/// Exact gets and one-row pages of the stable rows, a failure naming the key and what it read, or
/// every page, beside the target's states seen so far.
async fn check_stable(client: &mut Client<'_>, seen: &[usize]) {
    let expected = stable();
    for (key, value) in &expected {
        let mut out = Vec::new();
        let found = client.get_async(key, &mut out).await.unwrap();
        assert!(
            found && out == *value,
            "stable row {} read {:?}; target states seen {seen:?}",
            text(key),
            found.then(|| text(&out))
        );
    }
    let (got, pages) = one_row_pages(client, b"a/stable/", b"a/stable0", expected.len()).await;
    assert_eq!(
        got,
        expected.into_iter().collect::<Vec<_>>(),
        "stable pages (start, rows, next, more) {pages:?}; target states seen {seen:?}"
    );
}

fn check_all(db: &mut ShardDb<Gated>, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut value = Vec::new();
    for (key, want) in expected {
        assert!(db.get(key, &mut value).unwrap());
        assert_eq!(&value, want);
    }
    assert_eq!(
        db.get(TARGET, &mut value).unwrap(),
        expected.contains_key(TARGET),
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
    first.put(TARGET, TARGET_BEFORE).unwrap();
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
        latency_objective_ns: None,
    })
    .unwrap();
    let shard = runtime.shard_ids()[0];
    // Open before runtime/issuer teardown and before scoped work could be joined on failure.
    let _open = OpenOnDrop(Arc::clone(&gate));
    // Five simultaneous clients at most: writer, the failure's flush, canceled put, canceled
    // delete, and read/progress probe.
    let ranges = Arc::new(
        Ranges::start(
            &mut runtime,
            vec![(Vec::new(), first), (b"m".to_vec(), second)],
            RangesConfig {
                clients: 5,
                slice_ns: 50_000,
                spin_ns: 0,
                inline: false,
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
    assert_eq!(
        received(&events, "the first worker write, held"),
        Event::Held
    );
    // Public owner wait/announcement facts, not a sleep or a private engine-work assertion.
    // Pending CPU slices remain runnable; a fresh park with the writer unfinished exercises
    // the event wait. This is not a proof of native syscall residency, nor of any one mutation
    // still pending: the writer's oldest may be applied after it.
    until(
        "a park of the shard with the write held and the writer's input unfinished",
        || {
            assert!(
                !writer_ended.load(Ordering::Acquire),
                "the writer's whole input finished before the shard parked with the write held"
            );
            hyper_rt::registry::with_entry(shard.0, |entry| {
                entry.pulse.waits() > before_wait && entry.parking.parked()
            })
            .unwrap()
        },
    );
    // With the failure, a flush is published before the canceled mutations. It cannot finish
    // while a worker job's write is held (a flush retires every packing and leaves the trunk
    // idle), and the range keeps every later mutation behind it in arrival order: the canceled
    // put and delete are owned and unapplied when the held write fails, whatever the writer's
    // oldest mutation is doing, and each one's own answer is the failure.
    let (flush_published, flush_publication) = mpsc::sync_channel(1);
    let (flush_done, flushed) = mpsc::sync_channel(1);
    if fail {
        let flush_ranges = Arc::clone(&ranges);
        runtime
            .spawn_on(shard, async move {
                let mut client = flush_ranges.client().unwrap();
                let result = {
                    let mut flushing = pin!(client.flush_async());
                    poll_fn(|cx| {
                        assert!(
                            flushing.as_mut().poll(cx).is_pending(),
                            "a flush finished while a worker write was held"
                        );
                        flush_published.send(()).unwrap();
                        std::task::Poll::Ready(())
                    })
                    .await;
                    flushing.await
                };
                drop(client);
                drop(flush_ranges);
                flush_done.send(result).unwrap();
            })
            .unwrap();
        received(
            &flush_publication,
            "the flush's publication before the canceled mutations",
        );
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
                let mut pending = pin!(client.put_async(TARGET, TARGET_QUEUED));
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
            // Recovers the original answer and never replays it. The next request reads the other
            // range, which no fault of this one reaches: it fails only with the canceled request's
            // own answer.
            let mut out = Vec::new();
            let recovered = client.get_async(b"z-recovery", &mut out).await.map(drop);
            drop(client);
            drop(put_ranges);
            put_done.send(recovered).unwrap();
        })
        .unwrap();
    assert_eq!(
        received(&publications, "the canceled put's publication"),
        "put"
    );
    let (delete_resume, delete_continue) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (delete_done, delete_finished) = mpsc::sync_channel(1);
    let delete_ranges = Arc::clone(&ranges);
    runtime
        .spawn_on(shard, async move {
            let mut client = delete_ranges.client().unwrap();
            let mut first_pending = false;
            let canceled = {
                let mut pending = pin!(client.delete_async(TARGET));
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
            let mut out = Vec::new();
            let recovered = client.get_async(b"z-recovery", &mut out).await.map(drop);
            drop(client);
            drop(delete_ranges);
            delete_done.send(recovered).unwrap();
        })
        .unwrap();
    assert_eq!(
        received(&publications, "the canceled delete's publication"),
        "delete"
    );
    let probe_ranges = Arc::clone(&ranges);
    let probe_gate = Arc::clone(&gate);
    let (progress, progressed) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut client = probe_ranges.client().unwrap();
            // The target read around the stable rows' reads, by get and by scan in turn: the owned
            // put and delete reach it in their order, whenever the range applies them.
            let mut seen = vec![
                target_state(&mut client, false).await,
                target_state(&mut client, true).await,
            ];
            check_stable(&mut client, &seen).await;
            seen.push(target_state(&mut client, true).await);
            seen.push(target_state(&mut client, false).await);
            client
                .put_async(b"z-unrelated", b"other range during retained input")
                .await
                .unwrap();
            let mut out = Vec::new();
            assert!(client.get_async(b"z-unrelated", &mut out).await.unwrap());
            assert_eq!(out, b"other range during retained input");
            seen.push(target_state(&mut client, false).await);
            assert!(
                seen.is_sorted(),
                "the target read an earlier state after a later one: {seen:?}"
            );
            if fail {
                assert!(
                    seen.iter().all(|&state| state == 0),
                    "the target changed while the flush before its canceled mutations was held: \
                     {seen:?}"
                );
            }
            assert!(
                probe_gate.state.lock().unwrap().active.is_some(),
                "reads progressed before release"
            );
            drop(client);
            drop(probe_ranges);
            progress.send(()).unwrap();
        })
        .unwrap();
    received(
        &progressed,
        "the probe's reads and its other range's write with the write held",
    );
    assert!(
        !writer_ended.load(Ordering::Acquire),
        "the writer's whole input finished while a worker write was held"
    );
    assert!(!gate.flushed_early.load(Ordering::SeqCst));
    gate.open();
    // Each canceled request's owner recovers its answer by reading the other range, which no
    // fault of this one reaches: a recovery is an error only as the canceled request's own answer.
    put_resume.blocking_send(()).unwrap();
    delete_resume.blocking_send(()).unwrap();
    let recoveries = [
        received(&put_finished, "the canceled put's owner's recovery"),
        received(&delete_finished, "the canceled delete's owner's recovery"),
    ];
    let (mut oracle, failure) = received(&completed, "the writer's last answer after the release");
    assert_eq!(failure.is_some(), fail);
    // The failure's every answer is its first error, the held write's injected one: the flush's,
    // and the canceled mutations' own, owned behind it when the write failed.
    let first_error = failure.as_ref().map(|fault| {
        assert!(
            format!("{fault:?}").contains("one worker page write rejected by test"),
            "{fault:?}"
        );
        format!("{:?}", Err::<(), _>(fault.clone()))
    });
    match &first_error {
        Some(first) => {
            let flush = received(&flushed, "the flush's answer after the release");
            assert_eq!(&format!("{flush:?}"), first, "the flush's answer");
            for recovered in &recoveries {
                assert_eq!(
                    &format!("{recovered:?}"),
                    first,
                    "a canceled mutation's answer"
                );
            }
        }
        None => {
            for recovered in &recoveries {
                assert!(recovered.is_ok(), "{recovered:?}");
            }
        }
    }
    // The target's newest operation is the canceled delete, after the canceled put: none.
    oracle.extend(stable());
    // Four initial puts (the stable rows and the target), two canceled-but-owned mutations, and
    // the unrelated put.
    let applied = OPS + 4 + 2 + 1;
    {
        let mut client = ranges.client().unwrap();
        if let Some(first) = &first_error {
            assert_eq!(&format!("{:?}", client.flush()), first);
            assert_eq!(&format!("{:?}", client.checkpoint(applied)), first);
        } else {
            client.flush().unwrap();
            client.checkpoint(applied).unwrap();
        }
    }
    assert_eq!(gate.failures.load(Ordering::SeqCst), usize::from(fail));
    assert!(!gate.reused_early.load(Ordering::SeqCst));
    assert!(!gate.flushed_early.load(Ordering::SeqCst));
    let stopped = Arc::try_unwrap(ranges).unwrap().stop();
    match &first_error {
        Some(first) => assert_eq!(&format!("{stopped:?}"), first),
        None => stopped.unwrap(),
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
    let mut before = stable();
    before.insert(TARGET.to_vec(), TARGET_BEFORE.to_vec());
    check_all(&mut first, if fail { &before } else { &oracle });
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

/// A mutation published behind a flush that a held worker write keeps from finishing is not
/// applied while it is held. The flush hands the active memtable to packing before it waits, so
/// the fresh one has room for the mutation: only the range's order keeps it out. While the
/// packing's write is held, a get reads the later key absent and the key put before the flush
/// present; let go, the flush and the mutation are both answered Ok, and both keys read back,
/// after a checkpoint and a cold reopen too.
#[test]
fn a_mutation_behind_a_held_flush_is_not_applied_while_it_is_held() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let (gate, events) = Gate::new(true, false);
    let mut db = engine(&path, Arc::clone(&gate));
    for (key, value) in stable() {
        db.put(&key, &value).unwrap();
    }
    cold_complete(&mut db);
    db.checkpoint(0).unwrap();
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
    db.attach(&issuer, depth).unwrap();
    workers(&mut db, &path, Arc::clone(&gate));
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
        latency_objective_ns: None,
    })
    .unwrap();
    let shard = runtime.shard_ids()[0];
    // Open before runtime/issuer teardown and before scoped work could be joined on failure.
    let _open = OpenOnDrop(Arc::clone(&gate));
    // Three simultaneous clients: the flusher, the later mutation's owner, and the reader.
    let ranges = Arc::new(
        Ranges::start(
            &mut runtime,
            vec![(Vec::new(), db)],
            RangesConfig {
                clients: 3,
                slice_ns: 50_000,
                spin_ns: 0,
                inline: false,
            },
        )
        .unwrap(),
    );
    gate.armed.store(true, Ordering::SeqCst);
    let (flush_published, flush_publication) = mpsc::sync_channel(1);
    let (flush_done, flushed) = mpsc::sync_channel(1);
    let flush_ranges = Arc::clone(&ranges);
    runtime
        .spawn_on(shard, async move {
            let mut client = flush_ranges.client().unwrap();
            client
                .put_async(b"a/flushed", b"packed by the flush")
                .await
                .unwrap();
            let result = {
                let mut flushing = pin!(client.flush_async());
                poll_fn(|cx| {
                    assert!(
                        flushing.as_mut().poll(cx).is_pending(),
                        "a flush finished while its packing's write could not have"
                    );
                    flush_published.send(()).unwrap();
                    std::task::Poll::Ready(())
                })
                .await;
                flushing.await
            };
            drop(client);
            drop(flush_ranges);
            flush_done.send(result).unwrap();
        })
        .unwrap();
    received(&flush_publication, "the flush's publication");
    assert_eq!(
        received(&events, "the flush's packing's first write, held"),
        Event::Held
    );
    let (put_published, put_publication) = mpsc::sync_channel(1);
    let (put_done, put_finished) = mpsc::sync_channel(1);
    let put_ranges = Arc::clone(&ranges);
    runtime
        .spawn_on(shard, async move {
            let mut client = put_ranges.client().unwrap();
            let result = {
                let mut putting =
                    pin!(client.put_async(b"a/behind", b"published behind the flush"));
                poll_fn(|cx| {
                    assert!(
                        putting.as_mut().poll(cx).is_pending(),
                        "a put was answered as it was published"
                    );
                    put_published.send(()).unwrap();
                    std::task::Poll::Ready(())
                })
                .await;
                putting.await
            };
            drop(client);
            drop(put_ranges);
            put_done.send(result).unwrap();
        })
        .unwrap();
    received(&put_publication, "the later put's publication");
    // The reader's requests reach the range after the put's: what they read is after the range
    // took the put, deferred behind the flush or not.
    let (read_done, read) = mpsc::sync_channel(1);
    let read_ranges = Arc::clone(&ranges);
    let read_gate = Arc::clone(&gate);
    runtime
        .spawn_on(shard, async move {
            let mut client = read_ranges.client().unwrap();
            let mut out = Vec::new();
            let behind = if client.get_async(b"a/behind", &mut out).await.unwrap() {
                Some(text(&out))
            } else {
                None
            };
            let before = if client.get_async(b"a/flushed", &mut out).await.unwrap() {
                Some(text(&out))
            } else {
                None
            };
            let held = read_gate.state.lock().unwrap().active.is_some();
            drop(client);
            drop(read_ranges);
            read_done.send((behind, before, held)).unwrap();
        })
        .unwrap();
    let (behind, before, held) = received(&read, "the reads while the flush is held");
    assert!(
        held,
        "the packing's write was let go before the reads ended"
    );
    assert_eq!(
        behind, None,
        "the put published behind the held flush was applied before it"
    );
    assert_eq!(before.as_deref(), Some("packed by the flush"));
    gate.open();
    received(&flushed, "the flush's answer after the release").unwrap();
    received(&put_finished, "the later put's answer after the release").unwrap();
    {
        let mut client = ranges.client().unwrap();
        let mut out = Vec::new();
        assert!(client.get(b"a/behind", &mut out).unwrap());
        assert_eq!(out, b"published behind the flush");
        client.flush().unwrap();
        client.checkpoint(2).unwrap();
    }
    assert!(!gate.reused_early.load(Ordering::SeqCst));
    assert!(!gate.flushed_early.load(Ordering::SeqCst));
    Arc::try_unwrap(ranges).unwrap().stop().unwrap();
    runtime.shutdown().unwrap();
    gate.armed.store(false, Ordering::SeqCst);
    let (mut db, index) = ShardDb::open(
        open(&path, false, false, gate),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(index, 2);
    let mut expected = stable();
    expected.insert(b"a/flushed".to_vec(), b"packed by the flush".to_vec());
    expected.insert(b"a/behind".to_vec(), b"published behind the flush".to_vec());
    check_all(&mut db, &expected);
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
        latency_objective_ns: None,
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
                inline: false,
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
                inline: false,
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
