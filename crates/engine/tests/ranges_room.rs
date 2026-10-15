//! A range whose packing or grant waits for its file to grow (docs/research/48 §5) waits for that
//! growth as an event, on a store file grown by segments whose growths on the workers are held and
//! let through one at a time. Once the writer's operation waits for room, the range answers each
//! request with one idle slice and parks between them, never passing again on its own; let the
//! held growth through while the next is held, and the writer goes on with the room it brought,
//! before the next lands; let every growth through, and every value and tombstone reads back
//! exactly, and the reopened store's whole image is the writes' after the checkpoint. A growth that
//! fails while the writer waits for its room ends the range in its fault, the failure injected:
//! every later request is answered with it, none with a value, the stop reports it once the
//! range's writes and workers retire, and the store reopens at its last checkpoint, its whole image
//! exactly the checkpoint's. Facts decide every verdict. Each wait for a fact has a failure guard
//! ([`WAIT`]) that a passing run never reaches: past it the test fails, naming the fact that did
//! not come, and lets every held growth go before the runtime's teardown.
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
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::file::CachingRequest;
use hyper_block::issuer::Issuer;
use hyper_block::segmented::SegmentedFile;
use hyper_rt::ShardId;
use hyper_rt::combine::join2;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::{FlushStats, ShardDb};
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
/// A memtable of one page: the first packings use up the file's first segment within a few
/// hundred operations, so the writer comes to wait for room early in its run.
const MEM: usize = 4096;
const OPS: u64 = 4096;
/// Segments a file may have: past what the workload makes.
const BOUND: usize = 1 << 12;
const SLICE_NS: u64 = 50_000;
/// The failure guard of every wait for a fact, the native worker fixtures' (pool_lifecycle.rs): a
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

/// Growth writes made on the workers, let through by count: the first `allowed` go on, every
/// later one waits in its write until it is allowed or every one is let go. `started` counts the
/// growth writes begun; those from `failing` on fail once let through.
struct Growths {
    state: Mutex<GrowthState>,
    changed: Condvar,
}

struct GrowthState {
    allowed: u64,
    started: u64,
    open: bool,
    failing: u64,
}

impl Growths {
    const fn new(allowed: u64) -> Self {
        Self {
            state: Mutex::new(GrowthState {
                allowed,
                started: 0,
                open: false,
                failing: u64::MAX,
            }),
            changed: Condvar::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, GrowthState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A worker's growth write: counted, then held until it is let through; a failing one fails.
    fn pass(&self) -> Result<(), DiskError> {
        let mut s = self.state();
        let me = s.started;
        s.started += 1;
        self.changed.notify_all();
        while !s.open && me >= s.allowed {
            s = self
                .changed
                .wait(s)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if me >= s.failing {
            return Err(DiskError::Io {
                op: "a test's failed growth",
                path: std::path::PathBuf::new(),
                source: std::io::Error::other("a test's failed growth"),
            });
        }
        Ok(())
    }

    /// Every growth write begun from now on fails.
    fn fail_from_now(&self) {
        let mut s = self.state();
        s.failing = s.started;
    }

    /// Waits until `n` growth writes have begun.
    fn until_started(&self, n: u64) {
        let deadline = Instant::now() + WAIT;
        let mut s = self.state();
        while s.started < n {
            let left = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| panic!("growth write {n}: not begun within the failure guard"));
            s = self
                .changed
                .wait_timeout(s, left)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }

    fn allow(&self, n: u64) {
        self.state().allowed = n;
        self.changed.notify_all();
    }

    fn open(&self) {
        self.state().open = true;
        self.changed.notify_all();
    }
}

/// Lets every growth through when dropped: a failed assertion leaves no worker waiting.
struct Open(&'static Growths);

impl Drop for Open {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// A store file grown by segments; a worker's growth writes pass its test's [`Growths`].
struct Grown {
    file: SegmentedFile,
    growths: Option<&'static Growths>,
}

impl BlockFile for Grown {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn layout_block(&self) -> Alignment {
        self.file.layout_block()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }
    fn read_resident_at(&mut self, buf: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        self.file.read_resident_at(buf, offset)
    }
    fn reads_resident(&self) -> bool {
        self.file.reads_resident()
    }
    fn residency(&self) -> hyper_block::Residency {
        self.file.residency()
    }
    fn set_residency_shadow(&mut self, on: bool) {
        self.file.set_residency_shadow(on);
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file.write_all_at(buf, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn fills_new_space(&self) -> bool {
        self.file.fills_new_space()
    }
    fn grows_by_segments(&self) -> bool {
        self.file.grows_by_segments()
    }
    fn grow(&self, len: u64) -> Result<Option<u64>, DiskError> {
        self.file.grow(len)
    }
    fn write_whole(&self, offset: u64, copies: u64, fill: &[u8]) -> Result<(), DiskError> {
        if let Some(growths) = self.growths {
            growths.pass()?;
        }
        self.file.write_whole(offset, copies, fill)
    }
    fn segment_end(&self, offset: u64) -> Result<Option<u64>, DiskError> {
        self.file.segment_end(offset)
    }
    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        self.file.write_durable_at(buf, offset)
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            growths: self.growths,
        })
    }
}

fn align() -> Alignment {
    Alignment::new(STORE.page_size).unwrap()
}

fn open(path: &Path, growths: Option<&'static Growths>) -> Grown {
    Grown {
        file: SegmentedFile::open(path, CachingRequest::Buffered, align(), BOUND).unwrap(),
        growths,
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("bucket-{}/object-{:05}", i % 5, i % 1500).into_bytes()
}

/// The `i`th operation: a put of a value of one of several lengths, or every seventh a delete.
fn operation(i: u64) -> (Vec<u8>, Option<Vec<u8>>) {
    let value = if i.is_multiple_of(7) {
        None
    } else {
        let len = [1, 33, 100, 700][i as usize % 4];
        Some(vec![(i % 251) as u8; len])
    };
    (key(i), value)
}

fn runtime() -> Runtime {
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 4,
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
        latency_objective_ns: None,
    })
    .unwrap()
}

/// Whether the shard has waited more than `waits` times and is parked now: every task on it waits
/// for an event.
fn parked_since(shard: ShardId, waits: u64) -> bool {
    hyper_rt::registry::with_entry(shard.0, |e| e.pulse.waits() > waits && e.parking.parked())
        .unwrap()
}

fn waits(shard: ShardId) -> u64 {
    hyper_rt::registry::with_entry(shard.0, |e| e.pulse.waits()).unwrap()
}

/// Every row the store holds, in key order: its whole logical image.
fn image(db: &mut ShardDb<Grown>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(
        !db.scan(b"", None, usize::MAX, &mut rows, &mut next)
            .unwrap()
    );
    rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

/// What the writes in `oracle` leave: every key's newest value, deleted keys absent, in key order.
fn expected(oracle: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    oracle
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
        .collect()
}

/// The range's counters, taken by a request the prober makes when asked; the operations the writer
/// has done when they were taken.
struct Probe<'a> {
    ask: &'a hyper_rt::sync::Sender<()>,
    answers: &'a mpsc::Receiver<FlushStats>,
    done: &'static AtomicU64,
    shard: ShardId,
}

impl Probe<'_> {
    /// One request to the range; returns once the range has answered it and the shard has parked
    /// again. The wait for the park is a fact the shard announces, not a time: a range that passed
    /// again on its own instead of waiting for an event would never let it come.
    fn take(&self) -> (FlushStats, u64) {
        let before = waits(self.shard);
        self.ask.blocking_send(()).unwrap();
        let stats = received(self.answers, "the range's answer to a probe");
        let done = self.done.load(Ordering::Acquire);
        until("the shard parked after answering a probe", || {
            parked_since(self.shard, before)
        });
        (stats, done)
    }

    /// Probes until two answers in a row show the writer waiting: no operation done and no rotation
    /// between them, and the range's idle work between them the one maintenance slice its task
    /// takes for the one event between them, the second probe's request, before it parks: work
    /// bounded by events, where a range that passed again on its own would take a slice a pass.
    /// Every probe before that saw another event of the run's (an operation, a rotation, a
    /// worker's message), and the writer's wait for room comes once the held growth's room is
    /// gone; a writer that finished first never waited, which fails the test.
    fn until_waiting(&self) -> FlushStats {
        let deadline = Instant::now() + WAIT;
        let (mut last, mut done) = self.take();
        loop {
            assert!(done < OPS, "the writer finished without waiting for room");
            assert!(
                Instant::now() < deadline,
                "the writer waiting for room: not seen within the failure guard"
            );
            let (now, now_done) = self.take();
            if now_done == done
                && now.rotations == last.rotations
                && now.idle_slices == last.idle_slices + 1
            {
                return now;
            }
            (last, done) = (now, now_done);
        }
    }
}

/// Do: on a store grown by segments, every growth on a worker held, a writer puts and deletes;
/// a prober beside it takes the range's counters on request. Expect: the first growth's write
/// begins; the writer comes to wait for room, and from then on the range answers each request with
/// one idle slice and parks between them. Let the first growth through, the second held: the writer
/// does more operations before the second lands. Let every growth through: the writer finishes;
/// every value and tombstone reads back exactly, after a checkpoint and a cold reopen too. With as
/// many workers as the host's cores.
#[test]
fn a_range_waiting_for_room_parks_and_goes_on_with_each_growth() {
    parks_and_goes_on(false);
}

/// The same with one worker, the memory budget's (ranges_growth_asker.rs's recipe), on any host:
/// the second growth, due ahead of need, leaves the one seat to the packing the writer waits on,
/// whose room the first growth brought.
#[test]
fn with_one_worker_a_range_waiting_for_room_goes_on_with_the_room_each_growth_brings() {
    parks_and_goes_on(true);
}

fn parks_and_goes_on(one_worker: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let growths: &'static Growths = Box::leak(Box::new(Growths::new(0)));
    let done: &'static AtomicU64 = Box::leak(Box::default());
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    let mut runtime = runtime();
    let shard = runtime.shard_ids()[0];
    let created = SegmentedFile::create(&path, CachingRequest::Buffered, align(), BOUND).unwrap();
    let mut db = ShardDb::create(
        Grown {
            file: created,
            growths: None,
        },
        STORE,
        MEM,
        TRUNK,
    )
    .unwrap();
    if one_worker {
        // Enough for the owner and one worker's depth, active output and hand-off roles.
        let run = STORE.page_size * usize::try_from(STORE.extent_pages).unwrap();
        db.set_memory(2 * (issuer.depth() + 2) * run).unwrap();
    }
    db.attach(&issuer, issuer.depth()).unwrap();
    let worker_path = path.clone();
    db.set_workers(move || {
        SegmentedFile::open(&worker_path, CachingRequest::Buffered, align(), BOUND)
            .map(|file| Grown {
                file,
                growths: Some(growths),
            })
            .map_err(|e| mantle_engine::Error::Io {
                op: "open a worker's file",
                detail: e.to_string(),
            })
    })
    .unwrap();
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 2,
            slice_ns: SLICE_NS,
            spin_ns: 0,
            inline: true,
        },
    )
    .unwrap();
    // Declared after the runtime, it drops first: a failed assertion lets every held growth go
    // before the runtime's teardown joins the workers.
    let _open = Open(growths);
    let (ask, mut asked) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (answer, answers) = mpsc::sync_channel(1);
    let (finish, finished) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut writer = ranges.client().unwrap();
            let mut prober = ranges.client().unwrap();
            let mut oracle = BTreeMap::new();
            let write = async {
                for i in 0..OPS {
                    let (key, value) = operation(i);
                    match &value {
                        Some(v) => writer.put_async(&key, v).await.unwrap(),
                        None => writer.delete_async(&key).await.unwrap(),
                    }
                    oracle.insert(key, value);
                    done.store(i + 1, Ordering::Release);
                }
            };
            // Until the test closes its end, each ask is answered with the range's counters.
            let probe = async {
                while asked.recv().await.is_ok() {
                    let stats = prober.stats_async().await.unwrap();
                    answer.send(stats[0].0).unwrap();
                }
            };
            join2(write, probe).await;
            drop(prober);
            writer.flush_async().await.unwrap();
            writer.checkpoint_async(OPS).await.unwrap();
            drop(writer);
            ranges.stop_async().await.unwrap();
            finish.send(oracle).unwrap();
        })
        .unwrap();
    let probe = Probe {
        ask: &ask,
        answers: &answers,
        done,
        shard,
    };
    // The first growth on a worker begins its write and is held there.
    growths.until_started(1);
    // The writer comes to wait for room; the range parks between requests.
    let waiting = probe.until_waiting();
    // The held growth goes, the next is held: the writer goes on with the room the first brought,
    // with no request of the test's to wake the range, before the next growth lands.
    let at = done.load(Ordering::Acquire);
    growths.allow(1);
    until(
        "the writer's next operation after the first growth landed",
        || done.load(Ordering::Acquire) > at,
    );
    let (after, _) = probe.take();
    assert!(after.rotations >= waiting.rotations);
    // Every growth goes: the writer finishes, the range checkpoints and stops.
    growths.open();
    drop(ask);
    let oracle = received(&finished, "the writer's finished run");
    runtime.shutdown().unwrap();
    drop(issuer);
    let (mut db, applied) = ShardDb::open(open(&path, None), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    let mut out = Vec::new();
    for (key, value) in &oracle {
        let found = db.get(key, &mut out).unwrap();
        match value {
            Some(v) => assert!(found && &out == v, "{key:?}"),
            None => assert!(!found, "{key:?} reads a deleted value"),
        }
    }
    assert!(
        image(&mut db) == expected(&oracle),
        "the reopened image is not the writes'"
    );
    db.check_references().unwrap();
}

/// Do: on a store grown by segments, a writer puts and deletes, checkpoints at `CHECKPOINT`
/// operations, and goes on; every growth from the checkpoint on fails. Expect: the writer's
/// operation that waits for that growth's room is answered with an I/O error, the range's fault;
/// a get after it is answered with the same fault, not a value; the stop reports it; reopened, the
/// store holds exactly what the checkpoint held.
#[test]
fn a_growth_that_fails_while_the_writer_waits_for_room_ends_the_range_in_its_fault() {
    const CHECKPOINT: u64 = 512;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let growths: &'static Growths = Box::leak(Box::new(Growths::new(u64::MAX)));
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    let mut runtime = runtime();
    let shard = runtime.shard_ids()[0];
    let created = SegmentedFile::create(&path, CachingRequest::Buffered, align(), BOUND).unwrap();
    let mut db = ShardDb::create(
        Grown {
            file: created,
            growths: None,
        },
        STORE,
        MEM,
        TRUNK,
    )
    .unwrap();
    db.attach(&issuer, issuer.depth()).unwrap();
    let worker_path = path.clone();
    db.set_workers(move || {
        SegmentedFile::open(&worker_path, CachingRequest::Buffered, align(), BOUND)
            .map(|file| Grown {
                file,
                growths: Some(growths),
            })
            .map_err(|e| mantle_engine::Error::Io {
                op: "open a worker's file",
                detail: e.to_string(),
            })
    })
    .unwrap();
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 1,
            slice_ns: SLICE_NS,
            spin_ns: 0,
            inline: true,
        },
    )
    .unwrap();
    let _open = Open(growths);
    let (checkpointed, at_checkpoint) = mpsc::sync_channel(1);
    let (go, mut going) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (finish, finished) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut writer = ranges.client().unwrap();
            let mut oracle = BTreeMap::new();
            for i in 0..CHECKPOINT {
                let (key, value) = operation(i);
                match &value {
                    Some(v) => writer.put_async(&key, v).await.unwrap(),
                    None => writer.delete_async(&key).await.unwrap(),
                }
                oracle.insert(key, value);
            }
            writer.checkpoint_async(CHECKPOINT).await.unwrap();
            checkpointed.send(()).unwrap();
            going.recv().await.unwrap();
            let mut failed = None;
            for i in CHECKPOINT..OPS {
                // Not recorded in the oracle: written after the checkpoint, none is durable.
                let (key, value) = operation(i);
                let applied = match &value {
                    Some(v) => writer.put_async(&key, v).await,
                    None => writer.delete_async(&key).await,
                };
                if let Err(error) = applied {
                    failed = Some(error);
                    break;
                }
            }
            let mut out = Vec::new();
            let (key, _) = operation(1);
            let read = writer.get_async(&key, &mut out).await.map(|_| out);
            drop(writer);
            // The stop answers once the range's writes and workers have retired.
            let stopped = ranges.stop_async().await;
            finish.send((oracle, failed, read, stopped)).unwrap();
        })
        .unwrap();
    received(&at_checkpoint, "the writer's checkpoint");
    growths.fail_from_now();
    go.blocking_send(()).unwrap();
    let (oracle, failed, read, stopped) = received(&finished, "the writer's run after the fault");
    runtime.shutdown().unwrap();
    drop(issuer);
    // The fault is the failure injected, and every later answer is that fault.
    let failed = failed.expect("no operation waited for a growth that failed");
    assert!(
        matches!(&failed, mantle_engine::Error::Io { .. })
            && format!("{failed:?}").contains("a test's failed growth"),
        "{failed:?}"
    );
    assert_eq!(
        format!("{:?}", read),
        format!("{:?}", Err::<(), _>(failed.clone()))
    );
    assert_eq!(
        format!("{:?}", stopped),
        format!("{:?}", Err::<(), _>(failed))
    );
    let (mut db, applied) = ShardDb::open(open(&path, None), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, CHECKPOINT);
    let mut out = Vec::new();
    for (key, value) in &oracle {
        let found = db.get(key, &mut out).unwrap();
        match value {
            Some(v) => assert!(found && &out == v, "{key:?}"),
            None => assert!(!found, "{key:?} reads a deleted value"),
        }
    }
    assert!(
        image(&mut db) == expected(&oracle),
        "the reopened image is not the checkpoint's"
    );
    db.check_references().unwrap();
}
