//! A budget that follows the machine's free memory (docs/research/64 §5). While the machine has
//! memory to spare, the budget is the shard's share of its capacity and the kept memtables fill
//! the idle part. When the machine's free memory falls below the room the budget keeps free, the
//! next look lowers the budget to what the shard holds less what the machine lacks: the kept
//! memtables give their memory back and the caches are trimmed at once, so the regions and the
//! kept memtables hold no more than the lower budget. When the machine frees memory again the
//! budget returns to its bound. Idle maintenance looks at the machine as the workers' results
//! come back, so the budget falls with no write to rotate. Every read stays exact throughout.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const MEM: usize = 8 * 1024;
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 4,
    leaf_entries: 128,
};
const KEYS: u64 = 2000;
/// The shard's share of the machine: room for every memtable the test fills, kept, beside the
/// page cache and write memory.
const BOUND: usize = 256 << 20;

/// The machine's free memory as the test sets it: the only reader of it is [`free`].
static FREE: AtomicU64 = AtomicU64::new(0);

fn free() -> Option<u64> {
    Some(FREE.load(Ordering::Relaxed))
}

/// The microseconds the process's tasks have stalled on memory, as the test sets it: the only
/// reader of it is [`stalls`].
static STALLS: AtomicU64 = AtomicU64::new(0);

fn stalls() -> Option<u64> {
    Some(STALLS.load(Ordering::Relaxed))
}

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:06}", k % 5).into_bytes()
}

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap()
}

fn workers(db: &mut ShardDb<DeviceFile>, path: &Path) {
    let path: PathBuf = path.to_path_buf();
    db.set_workers(move || {
        DeviceFile::open(
            &path,
            false,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .map_err(|e| mantle_engine::Error::Io {
            op: "open a worker's file",
            detail: e.to_string(),
        })
    })
    .unwrap();
}

type Oracle = BTreeMap<u64, Vec<u8>>;

/// Puts and gets from `x` until `rotations` more memtables have rotated, every get checked against
/// the oracle: the generator's state after.
fn run(db: &mut ShardDb<DeviceFile>, oracle: &mut Oracle, mut x: u64, rotations: u64) -> u64 {
    let until = db.stats().0.rotations + rotations;
    let mut value = Vec::new();
    let mut i = 0u64;
    while db.stats().0.rotations < until {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = x % KEYS;
        if i.is_multiple_of(3) {
            let v = format!("v{i}-{}", "x".repeat((x % 40) as usize)).into_bytes();
            db.put(&key(k), &v).unwrap();
            oracle.insert(k, v);
        } else {
            let found = db.get(&key(k), &mut value).unwrap();
            match oracle.get(&k) {
                Some(v) => assert!(found && &value == v, "key {k} at {i}"),
                None => assert!(!found, "key {k} at {i}"),
            }
        }
        i += 1;
        assert!(i < 1_000_000, "no rotation");
    }
    x
}

/// What the budget's regions and the kept memtables hold.
fn held(db: &ShardDb<DeviceFile>) -> usize {
    let (cache, write, records) = db.memory_held();
    cache + write + records + usize::try_from(db.stats().0.kept_bytes).unwrap()
}

#[test]
fn a_budget_that_follows_the_machine_gives_memory_back_when_the_machine_runs_short() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    FREE.store(1 << 40, Ordering::Relaxed);
    db.follow(BOUND, 1, free, stalls).unwrap();
    workers(&mut db, &path);
    let mut oracle = Oracle::new();
    let mut x = 0x5851_f42d_4c95_7f2du64;
    x = run(&mut db, &mut oracle, x, 40);
    assert_eq!(db.budget(), Some(BOUND), "a machine with memory to spare");
    let kept = db.stats().0.kept;
    assert!(kept > 0, "no memtable was kept");
    assert!(held(&db) < BOUND, "the test's memtables fill the bound");

    // The machine has nothing free: the next rotation's look lowers the budget to what the shard
    // holds at most, below its bound; nothing grows while nothing is free, so the budget never
    // rises again, the regions and the kept memtables hold no more than it, and the kept
    // memtables, a new memtable's arena not to be had, give memory back.
    FREE.store(0, Ordering::Relaxed);
    x = run(&mut db, &mut oracle, x, 1);
    let mut short = db.budget().unwrap();
    assert!(short < BOUND, "budget {short} stayed at its bound");
    for _ in 0..4 {
        assert!(
            db.trimming() || held(&db) <= short,
            "held {} above the budget {short}",
            held(&db)
        );
        x = run(&mut db, &mut oracle, x, 1);
        let now = db.budget().unwrap();
        assert!(
            now <= short,
            "the budget rose to {now} from {short} with nothing free"
        );
        short = now;
    }
    assert!(
        db.stats().0.kept < kept,
        "kept {} of {kept}",
        db.stats().0.kept
    );

    // The machine frees memory: the budget returns to its bound.
    FREE.store(1 << 40, Ordering::Relaxed);
    x = run(&mut db, &mut oracle, x, 1);
    assert_eq!(db.budget(), Some(BOUND), "the machine has memory again");

    // The machine runs short with no write to rotate: idle maintenance looks at it as the
    // workers' results come back, and the budget falls with no rotation.
    x = run(&mut db, &mut oracle, x, 1);
    assert!(db.owed(), "nothing owed to wait on");
    FREE.store(0, Ordering::Relaxed);
    let rotations = db.stats().0.rotations;
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    assert_eq!(db.stats().0.rotations, rotations, "a rotation in idle time");
    assert!(
        db.budget().unwrap() < BOUND,
        "idle maintenance never looked at the machine"
    );
    let (cache, write, records) = db.memory_held();
    assert!(
        db.trimming() || held(&db) <= db.budget().unwrap(),
        "held {} above the budget {}: cache {cache} write {write} records {records} kept {} ({} memtables) index {:?} split {:?}",
        held(&db),
        db.budget().unwrap(),
        db.stats().0.kept_bytes,
        db.stats().0.kept,
        db.index_bytes(),
        db.memory_split()
    );
    FREE.store(1 << 40, Ordering::Relaxed);
    run(&mut db, &mut oracle, x, 1);
    let mut value = Vec::new();
    for (k, v) in &oracle {
        assert!(db.get(&key(*k), &mut value).unwrap(), "key {k} lost");
        assert_eq!(&value, v, "key {k}");
    }
}

/// The second test's machine: its own free memory and stall count, since tests run at once.
static FREE_2: AtomicU64 = AtomicU64::new(1 << 40);
static STALLS_2: AtomicU64 = AtomicU64::new(0);
static STALLING_2: AtomicBool = AtomicBool::new(false);

fn free_2() -> Option<u64> {
    Some(FREE_2.load(Ordering::Relaxed))
}

/// While the machine stalls, every read finds 20 ms more stall than the last: every window a tick
/// long passes its allowance and none is quiet, so the ceiling only falls, however long a look
/// takes to come.
fn stalls_2() -> Option<u64> {
    if STALLING_2.load(Ordering::Relaxed) {
        Some(STALLS_2.fetch_add(20_000, Ordering::Relaxed) + 20_000)
    } else {
        Some(STALLS_2.load(Ordering::Relaxed))
    }
}

/// A machine that reports memory free while the process's tasks stall on memory, as when the
/// kernel swaps the shard's own pages out (docs/research/64 §16): once a window's stall passes
/// its allowance, the budget falls a tuning step under what the shard holds and the shard gives
/// the memory back, though the free memory read never fell. Every read stays exact.
#[test]
fn a_budget_gives_memory_back_while_the_process_stalls_on_memory() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    db.follow(BOUND, 1, free_2, stalls_2).unwrap();
    workers(&mut db, &path);
    let mut oracle = Oracle::new();
    let mut x = 0x2545_f491_4f6c_dd1du64;
    x = run(&mut db, &mut oracle, x, 40);
    assert_eq!(db.budget(), Some(BOUND), "a machine with memory to spare");

    // The tasks stall: the first look a tick after the window began finds the window past its
    // allowance. Bounded by rotations, each a look.
    STALLING_2.store(true, Ordering::Relaxed);
    let mut rotations = 0u64;
    while db.budget().unwrap() >= BOUND {
        x = run(&mut db, &mut oracle, x, 1);
        rotations += 1;
        assert!(
            rotations < 1_000_000,
            "the budget never fell under a stalling machine"
        );
    }
    // Each fall puts the ceiling a tuning step under what the shard held at that look, and the
    // floor (the write memory and the caches' indexes) is a small part of that holding here: the
    // budget is no more than the holding the last fall saw.
    let budget = db.budget().unwrap();
    let flush = db.stats().0;
    assert!(
        flush.pressure_falls > 0,
        "the budget fell with no fall counted"
    );
    assert!(
        u64::try_from(budget).unwrap() <= flush.pressure_held,
        "budget {budget} above the {} the shard held at the last fall",
        flush.pressure_held
    );
    assert!(
        db.trimming() || held(&db) <= budget,
        "held {} above the budget {budget}",
        held(&db)
    );
    let mut value = Vec::new();
    for (k, v) in &oracle {
        assert!(db.get(&key(*k), &mut value).unwrap(), "key {k} lost");
        assert_eq!(&value, v, "key {k}");
    }
}
