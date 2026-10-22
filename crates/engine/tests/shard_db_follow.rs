//! A budget that follows the machine's free memory (docs/research/65). While the machine has
//! memory to spare, the budget is its bound and the kept memtables fill the idle part. When the
//! machine's free memory falls to what the budget leaves free (its reserve, and the most the
//! memory outside the shard's account grew between two looks), the next look lowers the budget to
//! what the shard holds less what the machine lacks: the kept memtables give their memory back and
//! the caches are trimmed at once, so the regions and the kept memtables hold no more than the
//! lower budget. When the machine frees memory again the budget returns to its bound. Idle
//! maintenance looks at the machine as the workers' results come back, so the budget falls with no
//! write to rotate. Every read stays exact throughout.
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
use std::sync::atomic::{AtomicU64, Ordering};

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
/// What the budget leaves free of the machine's free memory beyond its own growth: a free
/// reading of this much is a machine with nothing to spare.
const RESERVE: u64 = 64 << 20;

/// The machine's free memory as each test sets it: the only readers of them are [`free`] and
/// [`free_reserved`].
static FREE: AtomicU64 = AtomicU64::new(0);
static FREE_RESERVED: AtomicU64 = AtomicU64::new(0);

fn free() -> Option<u64> {
    Some(FREE.load(Ordering::Relaxed))
}

fn free_reserved() -> Option<u64> {
    Some(FREE_RESERVED.load(Ordering::Relaxed))
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

/// Every key the oracle holds reads back as it was last written.
fn exact(db: &mut ShardDb<DeviceFile>, oracle: &Oracle) {
    let mut value = Vec::new();
    for (k, v) in oracle {
        assert!(db.get(&key(*k), &mut value).unwrap(), "key {k} lost");
        assert_eq!(&value, v, "key {k}");
    }
}

#[test]
fn a_budget_that_follows_the_machine_gives_memory_back_when_the_machine_runs_short() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    FREE.store(1 << 40, Ordering::Relaxed);
    db.follow(BOUND, 1, 0, free).unwrap();
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
    exact(&mut db, &oracle);
}

/// The reserve is the machine's, not the shard's: a machine whose free memory is no more than the
/// reserve has nothing to spare, and the budget falls as it does with none free; with the reserve
/// and more free, the budget takes only what lies past the reserve.
#[test]
fn a_budget_that_follows_the_machine_leaves_its_reserve_free() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    FREE_RESERVED.store(1 << 40, Ordering::Relaxed);
    let reserve = usize::try_from(RESERVE).unwrap();
    db.follow(BOUND, 1, reserve, free_reserved).unwrap();
    workers(&mut db, &path);
    let mut oracle = Oracle::new();
    let mut x = 0x2545_f491_4f6c_dd1du64;
    x = run(&mut db, &mut oracle, x, 40);
    assert_eq!(db.budget(), Some(BOUND), "a machine with memory to spare");
    let kept = db.stats().0.kept;
    assert!(kept > 0, "no memtable was kept");

    // Free memory of the reserve alone: nothing to spare.
    FREE_RESERVED.store(RESERVE, Ordering::Relaxed);
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
            "the budget rose to {now} from {short} with only the reserve free"
        );
        short = now;
    }
    assert!(
        db.stats().0.kept < kept,
        "kept {} of {kept}",
        db.stats().0.kept
    );

    // The reserve and a little more free: the budget is at most what the shard holds and that
    // little, never its bound.
    let more = 1u64 << 20;
    FREE_RESERVED.store(RESERVE + more, Ordering::Relaxed);
    x = run(&mut db, &mut oracle, x, 1);
    let budget = db.budget().unwrap();
    assert!(budget < BOUND, "budget {budget} took the reserve");
    FREE_RESERVED.store(1 << 40, Ordering::Relaxed);
    run(&mut db, &mut oracle, x, 1);
    assert_eq!(db.budget(), Some(BOUND), "the machine has memory again");
    exact(&mut db, &oracle);
}
