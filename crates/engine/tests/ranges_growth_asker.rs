//! A range whose one maintenance worker (a memory budget for the owner's and one worker's write
//! roles, as in shard_worker_memory.rs) packs memtables of values of a few kilobytes: an entry of
//! 3,000 bytes fills most of a page alone, so a packing writes more pages than its grant held, and
//! the merges of branches whose big and mid entries interleave do too. The job asks for more
//! extents and waits, holding the pool's one seat. When the shard's admitted extents cannot answer,
//! the file's growth goes through that worker ([`Pool::send_growth`]); without it no worker is free
//! to grow the file and the range waits for a message no worker sends: the unchanged hybrid and
//! the repair with that route taken out both stop at the first round's flush.
//! - The writer's rounds of puts and deletes finish, inline and through the range's channel;
//!   flushed, checkpointed, stopped and reopened cold, every key reads its newest value or is gone,
//!   and the store's image is exactly the writes'.
//! - A growth write that fails ends the range in that fault: the operation that waited for it, a
//!   later read and the stop each answer with it, and the store reopens at its checkpoint exactly.
//! - A stop asked while a growth write is held answers only after that write has finished, and
//!   the store reopens at its checkpoint exactly.
//!
//! Each wait for a fact has a failure guard ([`WAIT`]) a passing run never reaches.
//!
//! [`Pool::send_growth`]: mantle_engine::trunk::pool::Pool::send_growth
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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::file::CachingRequest;
use hyper_block::issuer::Issuer;
use hyper_block::segmented::SegmentedFile;
use hyper_rt::combine::{Either, race2};
use hyper_rt::{Runtime, RuntimeConfig, ShardId};
use mantle_engine::Error;
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::{ShardDb, issuer_batches};
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
/// A memtable holds a round's rows.
const MEM: usize = 1 << 20;
/// The root's leaf settles on its branches' count, not its entries'.
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 1 << 20,
};
/// The issuer's depth, a shard's and a worker's.
const DEPTH: usize = 2;
/// Segments a file may have: past what a run makes.
const BOUND: usize = 1 << 12;
const SLICE_NS: u64 = 50_000;
/// The failure guard of every wait for a fact (pool_lifecycle.rs): a passing run never waits for
/// it, and elapsed time never counts as the fact.
const WAIT: Duration = Duration::from_secs(5);
/// Rows a round, and rounds: even rounds put the even keys with values of [`BIG`] bytes, odd
/// rounds the odd keys with values of [`MID`]; from the third round on, a round also deletes every
/// fifth key the round two before put.
const ROWS: u64 = 200;
const ROUNDS: u64 = 6;
const BIG: usize = 3000;
const MID: usize = 1500;
/// The values of the checkpointed round before a fault or a stop: small, so its packing asks for
/// nothing.
const SMALL: usize = 100;

fn align() -> Alignment {
    Alignment::new(STORE.page_size).unwrap()
}

fn key(k: u64) -> Vec<u8> {
    format!("key-{k:06}").into_bytes()
}

fn value(r: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|b| (r as u8).wrapping_mul(31).wrapping_add(b as u8))
        .collect()
}

/// Round `r`'s operations: its puts, then its deletes.
fn round(r: u64) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let (odd, len) = if r.is_multiple_of(2) {
        (0, BIG)
    } else {
        (1, MID)
    };
    let mut ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..ROWS)
        .map(|i| (key(2 * i + odd), Some(value(r, len))))
        .collect();
    if r >= 2 {
        ops.extend((0..ROWS).step_by(5).map(|i| (key(2 * i + odd), None)));
    }
    ops
}

/// Growth writes on the workers' files: passed through until the gate is armed; once armed,
/// counted as they begin and finish, held until it opens when it holds, failed when it fails.
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Clone, Copy, Debug)]
struct GateState {
    armed: bool,
    hold: bool,
    fail: bool,
    open: bool,
    begun: u64,
    finished: u64,
}

impl Gate {
    fn leak() -> &'static Self {
        Box::leak(Box::new(Self {
            state: Mutex::new(GateState {
                armed: false,
                hold: false,
                fail: false,
                open: false,
                begun: 0,
                finished: 0,
            }),
            changed: Condvar::new(),
        }))
    }

    fn state(&self) -> MutexGuard<'_, GateState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn now(&self) -> GateState {
        *self.state()
    }

    fn arm(&self, hold: bool, fail: bool) {
        let mut s = self.state();
        (s.armed, s.hold, s.fail) = (true, hold, fail);
        self.changed.notify_all();
    }

    fn open(&self) {
        self.state().open = true;
        self.changed.notify_all();
    }

    fn write(&self, write: impl FnOnce() -> Result<(), DiskError>) -> Result<(), DiskError> {
        let mut s = self.state();
        if !s.armed {
            drop(s);
            return write();
        }
        s.begun += 1;
        self.changed.notify_all();
        while s.hold && !s.open {
            s = self
                .changed
                .wait(s)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        let fail = s.fail;
        drop(s);
        let result = if fail {
            Err(DiskError::Io {
                op: "a test's failed growth write",
                path: PathBuf::new(),
                source: std::io::Error::other("a test's failed growth write"),
            })
        } else {
            write()
        };
        let mut s = self.state();
        s.finished += 1;
        self.changed.notify_all();
        result
    }

    /// Waits until `fact` holds of the gate, failing the test past the guard.
    fn until(&self, what: &str, fact: impl Fn(&GateState) -> bool) {
        let deadline = Instant::now() + WAIT;
        let mut s = self.state();
        while !fact(&s) {
            let left = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| panic!("{what}: not seen within the failure guard ({:?})", *s));
            s = self
                .changed
                .wait_timeout(s, left)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }
}

/// Opens the gate when dropped: a failed assertion leaves no worker held in a growth write.
struct Open(&'static Gate);

impl Drop for Open {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// The store's file grown by segments; a worker's growth writes pass its gate.
struct Grown {
    file: SegmentedFile,
    gate: Option<&'static Gate>,
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
        match self.gate {
            Some(gate) => gate.write(|| self.file.write_whole(offset, copies, fill)),
            None => self.file.write_whole(offset, copies, fill),
        }
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
            gate: self.gate,
        })
    }
}

fn open(path: &Path, gate: Option<&'static Gate>) -> Grown {
    Grown {
        file: SegmentedFile::open(path, CachingRequest::Buffered, align(), BOUND).unwrap(),
        gate,
    }
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

/// A range on a new store at `path` grown by segments, its maintenance worker the memory budget's
/// one, its growth writes passing `gate`; clients `inline` or through the range's channel.
fn start(path: &Path, gate: &'static Gate, inline: bool) -> (Runtime, Issuer, Ranges) {
    let mut runtime = runtime();
    let created = SegmentedFile::create(path, CachingRequest::Buffered, align(), BOUND).unwrap();
    let mut db = ShardDb::create(
        Grown {
            file: created,
            gate: None,
        },
        STORE,
        MEM,
        TRUNK,
    )
    .unwrap();
    let run = STORE.page_size * usize::try_from(STORE.extent_pages).unwrap();
    // Enough for the owner and one worker's depth, active output and hand-off roles: one worker.
    db.set_memory(2 * (DEPTH + 2) * run).unwrap();
    let issuer =
        Issuer::start_for(path.parent().unwrap(), DEPTH, issuer_batches(DEPTH, DEPTH)).unwrap();
    db.attach(&issuer, DEPTH).unwrap();
    let worker_path = path.to_path_buf();
    db.set_workers(move || {
        SegmentedFile::open(&worker_path, CachingRequest::Buffered, align(), BOUND)
            .map(|file| Grown {
                file,
                gate: Some(gate),
            })
            .map_err(|e| Error::Io {
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
            inline,
        },
    )
    .unwrap();
    (runtime, issuer, ranges)
}

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

fn received<T>(from: &mpsc::Receiver<T>, what: &str) -> T {
    from.recv_timeout(WAIT)
        .unwrap_or_else(|e| panic!("{what}: not received within the failure guard ({e})"))
}

/// Whether the shard has waited more than `waits` times and is parked now.
fn parked_since(shard: ShardId, waits: u64) -> bool {
    hyper_rt::registry::with_entry(shard.0, |e| e.pulse.waits() > waits && e.parking.parked())
        .unwrap()
}

fn waits(shard: ShardId) -> u64 {
    hyper_rt::registry::with_entry(shard.0, |e| e.pulse.waits()).unwrap()
}

/// What the writes in `oracle` leave: every key's newest value, deleted keys absent, in key order.
fn expected(oracle: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    oracle
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
        .collect()
}

/// The store at `path` reopened cold: it applied `applied` operations, every key in `oracle`
/// reads its newest value or is gone, and its whole image is exactly the writes'.
fn reopened(path: &Path, applied: u64, oracle: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) {
    let (mut db, at) = ShardDb::open(open(path, None), STORE, MEM, TRUNK).unwrap();
    assert_eq!(at, applied);
    let mut out = Vec::new();
    for (k, v) in oracle {
        let found = db.get(k, &mut out).unwrap();
        match v {
            Some(v) => assert!(found && &out == v, "{k:?} does not read its newest value"),
            None => assert!(!found, "{k:?} reads a deleted value"),
        }
    }
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(
        !db.scan(b"", None, usize::MAX, &mut rows, &mut next)
            .unwrap()
    );
    let image: Vec<(Vec<u8>, Vec<u8>)> =
        rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
    assert!(
        image == expected(oracle),
        "the reopened image is not the writes'"
    );
    db.check_references().unwrap();
}

/// Do: rounds of puts and deletes, each round flushed, then a checkpoint and the stop, inline or
/// through the range's channel. Expect: every round flushed; reopened cold, the store applied
/// every operation and reads exactly what they left.
fn progress(inline: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let gate = Gate::leak();
    let rounds: &'static AtomicU64 = Box::leak(Box::default());
    let (runtime, issuer, ranges) = start(&path, gate, inline);
    let shard = runtime.shard_ids()[0];
    let _open = Open(gate);
    let (finish, finished) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut writer = ranges.client().unwrap();
            let mut oracle = BTreeMap::new();
            let mut applied = 0;
            for r in 0..ROUNDS {
                for (k, v) in round(r) {
                    match &v {
                        Some(v) => writer.put_async(&k, v).await.unwrap(),
                        None => writer.delete_async(&k).await.unwrap(),
                    }
                    oracle.insert(k, v);
                    applied += 1;
                }
                writer.flush_async().await.unwrap();
                rounds.store(r + 1, Ordering::Release);
            }
            writer.checkpoint_async(applied).await.unwrap();
            drop(writer);
            ranges.stop_async().await.unwrap();
            finish.send((oracle, applied)).unwrap();
        })
        .unwrap();
    for r in 1..=ROUNDS {
        until(&format!("round {r} flushed"), || {
            rounds.load(Ordering::Acquire) >= r
        });
    }
    let (oracle, applied) = received(&finished, "the writer's checkpoint and the range's stop");
    runtime.shutdown().unwrap();
    drop(issuer);
    reopened(&path, applied, &oracle);
}

#[test]
fn a_range_whose_one_worker_waits_for_extents_grows_through_it_and_goes_on() {
    progress(true);
}

#[test]
fn a_range_whose_one_worker_waits_for_extents_goes_on_through_its_channel() {
    progress(false);
}

/// The checkpointed round before a fault or a stop: small values the even keys, flushed and
/// checkpointed. Returns what it wrote and the operations it applied.
async fn checkpointed(
    writer: &mut mantle_engine::ranges::Client<'_>,
) -> (BTreeMap<Vec<u8>, Option<Vec<u8>>>, u64) {
    let mut oracle = BTreeMap::new();
    for i in 0..ROWS {
        let (k, v) = (key(2 * i), value(99, SMALL));
        writer.put_async(&k, &v).await.unwrap();
        oracle.insert(k, Some(v));
    }
    writer.flush_async().await.unwrap();
    writer.checkpoint_async(ROWS).await.unwrap();
    (oracle, ROWS)
}

/// Do: a checkpointed round; then, every growth write from here failing, puts of big values past
/// one memtable, so a rotation hands a full one to packing behind the writer; once a growth write
/// has failed, a read, a put and the stop. Expect: the read and the put answer with the injected
/// write failure, the range's fault, and the stop with the same; reopened cold, the store holds
/// exactly the checkpoint's image.
#[test]
fn a_failed_growth_write_ends_the_range_in_that_fault() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let gate = Gate::leak();
    let (runtime, issuer, ranges) = start(&path, gate, true);
    let shard = runtime.shard_ids()[0];
    let _open = Open(gate);
    let (at_checkpoint, at_checkpoint_rx) = mpsc::sync_channel(1);
    let (go, mut going) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (put, put_rx) = mpsc::sync_channel(1);
    let (after, mut afterwards) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (finish, finished) = mpsc::sync_channel(1);
    // Rows of big values past one memtable: one rotation behind the writer.
    let rows = u64::try_from(MEM / BIG).unwrap() + ROWS;
    runtime
        .spawn_on(shard, async move {
            let mut writer = ranges.client().unwrap();
            let (oracle, applied) = checkpointed(&mut writer).await;
            at_checkpoint.send(()).unwrap();
            going.recv().await.unwrap();
            let mut early = None;
            for i in 0..rows {
                // Not recorded in the oracle: written after the checkpoint, none is durable.
                if let Err(error) = writer.put_async(&key(2 * i), &value(7, BIG)).await {
                    early = Some(error);
                    break;
                }
            }
            put.send(()).unwrap();
            afterwards.recv().await.unwrap();
            let mut out = Vec::new();
            let read = writer.get_async(&key(0), &mut out).await.map(|_| out);
            let wrote = writer.put_async(&key(1), &value(8, SMALL)).await;
            drop(writer);
            let stopped = ranges.stop_async().await;
            finish
                .send((oracle, applied, early, read, wrote, stopped))
                .unwrap();
        })
        .unwrap();
    received(&at_checkpoint_rx, "the writer's checkpoint");
    gate.arm(false, true);
    go.blocking_send(()).unwrap();
    received(&put_rx, "the writer's puts answered");
    gate.until("a growth write failed", |s| s.finished >= 1);
    after.blocking_send(()).unwrap();
    let (oracle, applied, early, read, wrote, stopped) =
        received(&finished, "the writer's run after the fault");
    runtime.shutdown().unwrap();
    drop(issuer);
    let fault = |what: &str, answer: String| {
        assert!(
            answer.contains("a test's failed growth write"),
            "{what} answered {answer}, not the injected write failure"
        );
    };
    if let Some(error) = early {
        fault("a put before the fault was seen", format!("{error:?}"));
    }
    fault("a read after the fault", format!("{read:?}"));
    fault("a put after the fault", format!("{wrote:?}"));
    fault("the stop", format!("{stopped:?}"));
    reopened(&path, applied, &oracle);
}

/// Do: a checkpointed round; then, every growth write from here held, puts of big values past one
/// memtable, so a rotation hands a full one to packing behind the writer, which is idle once its
/// puts are answered; once a growth write is held, the writer goes and the range is asked to stop;
/// once the shard has parked with the stop asked, the held writes are let go. Expect: the stop
/// answers only after every held growth write has finished; reopened cold, the store holds
/// exactly the checkpoint's image.
#[test]
fn a_stop_asked_while_a_growth_write_is_held_answers_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let gate = Gate::leak();
    let (runtime, issuer, ranges) = start(&path, gate, true);
    let shard = runtime.shard_ids()[0];
    let _open = Open(gate);
    let (at_checkpoint, at_checkpoint_rx) = mpsc::sync_channel(1);
    let (go, mut going) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (put, put_rx) = mpsc::sync_channel(1);
    let (stop, mut stopping) = hyper_rt::sync::channel::<()>(1).unwrap();
    let (asked, asked_rx) = mpsc::sync_channel(1);
    let (finish, finished) = mpsc::sync_channel(1);
    // Rows of big values past one memtable: one rotation behind the writer.
    let rows = u64::try_from(MEM / BIG).unwrap() + ROWS;
    runtime
        .spawn_on(shard, async move {
            let mut writer = ranges.client().unwrap();
            let (oracle, applied) = checkpointed(&mut writer).await;
            at_checkpoint.send(()).unwrap();
            going.recv().await.unwrap();
            for i in 0..rows {
                // Not recorded in the oracle: written after the checkpoint, none is durable.
                writer.put_async(&key(2 * i), &value(7, BIG)).await.unwrap();
            }
            put.send(()).unwrap();
            stopping.recv().await.unwrap();
            drop(writer);
            asked.send(()).unwrap();
            let stopped = ranges.stop_async().await;
            let at_stop = gate.now();
            finish.send((oracle, applied, stopped, at_stop)).unwrap();
        })
        .unwrap();
    received(&at_checkpoint_rx, "the writer's checkpoint");
    gate.arm(true, false);
    go.blocking_send(()).unwrap();
    received(&put_rx, "the writer's puts answered");
    gate.until("a growth write held", |s| s.begun >= 1);
    let before = waits(shard);
    stop.blocking_send(()).unwrap();
    received(&asked_rx, "the stop asked");
    until("the shard parked with the stop asked", || {
        parked_since(shard, before)
    });
    assert_eq!(
        gate.now().finished,
        0,
        "a held growth write finished before its release"
    );
    gate.open();
    let (oracle, applied, stopped, at_stop) = received(&finished, "the stop's answer");
    runtime.shutdown().unwrap();
    drop(issuer);
    assert!(
        at_stop.begun >= 1 && at_stop.finished == at_stop.begun,
        "the stop answered before the held growth writes finished: {at_stop:?}, its answer \
         {stopped:?}"
    );
    stopped.unwrap();
    reopened(&path, applied, &oracle);
}
