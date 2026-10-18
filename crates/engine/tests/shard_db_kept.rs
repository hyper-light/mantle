//! Packed memtables kept readable for gets (research/51). With a memory budget and maintenance
//! workers, a memtable whose branch is in the trunk stays readable, newest last, in the memory
//! the budget leaves idle, and one index over them, built on the workers, answers a get in one
//! probe. Against a `BTreeMap`, every key reads exactly what was written throughout: puts,
//! deletions and overwrites across many memtables, the kept memtables answering gets with the
//! newest version, a deletion in a newer one hiding an older one's value, before and after the
//! index covers them. Once maintenance is done the index covers every kept memtable, and while a
//! shard made empty has given none back they hold every record the trunk does, so a key none
//! holds is answered absent without the trunk. A smaller budget has them give their memory back,
//! the index's entries for them then missing and absent keys read from the trunk; without a
//! budget or without workers none is kept; reopened, the engine keeps none and reads the
//! trunk.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
/// Keys the random fill never writes: written in the first memtables alone, so an index that
/// took two of those memtables' lists in the wrong order would answer the older value.
const STICKY: std::ops::Range<u64> = KEYS..KEYS + 64;
/// Room for every memtable the tests fill, kept, beside the page cache and write memory.
const BUDGET: usize = 256 << 20;

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

type Oracle = BTreeMap<u64, Option<Vec<u8>>>;

fn check_all(db: &mut ShardDb<DeviceFile>, oracle: &Oracle) {
    let mut value = Vec::new();
    for k in (0..KEYS).chain(STICKY) {
        let found = db.get(&key(k), &mut value).unwrap();
        match oracle.get(&k) {
            Some(Some(v)) => {
                assert!(found, "key {k} lost");
                assert_eq!(&value, v, "key {k}");
            }
            _ => assert!(!found, "key {k} reads a value it does not hold"),
        }
    }
}

/// Puts and deletes from `x` until `rotations` more memtables have rotated: the generator's
/// state after.
fn fill(db: &mut ShardDb<DeviceFile>, oracle: &mut Oracle, mut x: u64, rotations: u64) -> u64 {
    let until = db.stats().0.rotations + rotations;
    let mut i = 0u64;
    while db.stats().0.rotations < until {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = x % KEYS;
        if x.is_multiple_of(7) {
            db.delete(&key(k)).unwrap();
            oracle.insert(k, None);
        } else {
            let v = format!("v{i}-{}", "x".repeat((x % 40) as usize)).into_bytes();
            db.put(&key(k), &v).unwrap();
            oracle.insert(k, Some(v));
        }
        i += 1;
        assert!(i < 1_000_000, "no rotation");
    }
    x
}

/// Maintenance to its end: every index job back.
fn settle(db: &mut ShardDb<DeviceFile>) {
    while db.owed() {
        db.idle_step(64).unwrap();
    }
}

#[test]
fn kept_memtables_answer_gets_through_one_index_and_every_read_is_exact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    db.set_memory(BUDGET).unwrap();
    workers(&mut db, &path);
    let mut oracle = Oracle::new();
    // The sticky keys, written in the first memtable and again in the second, never after.
    let mut x = 0x9e37_79b9_7f4a_7c15;
    for round in 0..2u8 {
        for k in STICKY {
            let v = vec![b's', round, (k % 251) as u8];
            db.put(&key(k), &v).unwrap();
            oracle.insert(k, Some(v));
        }
        x = fill(&mut db, &mut oracle, x, 1);
    }
    // Read while memtables are kept and their index is still out: exact.
    x = fill(&mut db, &mut oracle, x, 12);
    check_all(&mut db, &oracle);
    x = fill(&mut db, &mut oracle, x, 12);
    db.flush().unwrap();
    settle(&mut db);
    let kept = db.stats().0;
    assert!(kept.kept >= 12, "too few memtables kept: {kept:?}");
    assert_eq!(
        kept.kept_covered, kept.kept,
        "the index does not cover every kept memtable: {kept:?}"
    );
    check_all(&mut db, &oracle);
    let read = db.stats().0;
    assert!(
        read.kept_hits > kept.kept_hits,
        "no get answered by a kept memtable: {read:?}"
    );
    // A shard made empty that gave none back: its kept memtables hold every record the trunk
    // does, and a key none holds was read as absent without the trunk.
    assert!(
        read.kept_absent > kept.kept_absent,
        "no absent key answered by the kept memtables: {read:?}"
    );

    // A key written, deleted and written again, each in its own memtable: kept newest last, so
    // the deletion hides the first value and the second write shows.
    let k = KEYS / 2;
    db.put(&key(k), b"first").unwrap();
    oracle.insert(k, Some(b"first".to_vec()));
    x = fill(&mut db, &mut oracle, x, 2);
    db.delete(&key(k)).unwrap();
    oracle.insert(k, None);
    x = fill(&mut db, &mut oracle, x, 2);
    db.flush().unwrap();
    check_all(&mut db, &oracle);
    db.put(&key(k), b"second").unwrap();
    oracle.insert(k, Some(b"second".to_vec()));
    x = fill(&mut db, &mut oracle, x, 2);
    db.flush().unwrap();
    check_all(&mut db, &oracle);
    settle(&mut db);
    check_all(&mut db, &oracle);

    // The least budget the engine takes: the kept memtables give their memory back, and every
    // key still reads exactly, now from the trunk.
    let before = db.stats().0;
    let mut least = BUDGET;
    while db.set_memory(least / 2).is_ok() {
        least /= 2;
    }
    db.set_memory(least).unwrap();
    let after = db.stats().0;
    assert!(
        after.kept_bytes < before.kept_bytes,
        "a smaller budget kept as much: {before:?} {after:?}"
    );
    // Memtables given back: the trunk holds records no kept one does, and an absent key is read
    // from it.
    check_all(&mut db, &oracle);
    assert_eq!(db.stats().0.kept_absent, after.kept_absent);
    let _ = fill(&mut db, &mut oracle, x, 3);
    check_all(&mut db, &oracle);
    settle(&mut db);
    check_all(&mut db, &oracle);

    // Reopened: nothing kept, every key read from the trunk.
    db.flush().unwrap();
    db.checkpoint(1).unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    let (mut db, _) = ShardDb::open(open(&path, false), STORE, MEM, TRUNK).unwrap();
    db.set_memory(BUDGET).unwrap();
    assert_eq!(db.stats().0.kept, 0);
    check_all(&mut db, &oracle);
    db.check_references().unwrap();
}

/// Under a budget of a few hundred pages, with workers, the kept memtables, their key lists and
/// their indexes hold only what the page cache, write memory and the record cache leave: after
/// every operation the regions and the kept memtables together hold no more than the budget,
/// while no region is still giving memory back; memtables are kept, and every read is exact.
#[test]
fn kept_memtables_hold_only_the_budget_s_idle_memory() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    let budget = 512 * STORE.page_size;
    db.set_memory(budget).unwrap();
    workers(&mut db, &path);
    let mut oracle = Oracle::new();
    let mut value = Vec::new();
    let mut kept_most = 0u64;
    let mut x = 0x5851_f42d_4c95_7f2du64;
    for step in 0..30_000u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = x % KEYS;
        if step % 3 == 0 {
            let v = format!("v{step}").into_bytes();
            db.put(&key(k), &v).unwrap();
            oracle.insert(k, Some(v));
        } else {
            let found = db.get(&key(k), &mut value).unwrap();
            match oracle.get(&k) {
                Some(Some(v)) => assert!(found && &value == v, "key {k} at step {step}"),
                _ => assert!(!found, "key {k} at step {step}"),
            }
        }
        let (cache, write, records) = db.memory_held();
        let kept = db.stats().0.kept_bytes;
        kept_most = kept_most.max(db.stats().0.kept);
        assert!(
            db.trimming() || cache + write + records + usize::try_from(kept).unwrap() <= budget,
            "step {step}: kept {kept} beside cache {cache} write {write} records {records}: {}",
            db.diag_budget()
        );
    }
    assert!(kept_most > 0, "no memtable was kept");
    settle(&mut db);
    check_all(&mut db, &oracle);
}

/// Without maintenance workers no index over kept memtables can be built, and a get's probes
/// would grow with them: none is kept.
#[test]
fn without_workers_no_memtable_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    db.set_memory(BUDGET).unwrap();
    let mut oracle = Oracle::new();
    let _ = fill(&mut db, &mut oracle, 0x2545_f491_4f6c_dd1d, 12);
    db.flush().unwrap();
    assert_eq!(db.stats().0.kept, 0);
    check_all(&mut db, &oracle);
}

/// Without a memory budget there is no idle memory to keep a memtable in: none is kept.
#[test]
fn without_a_budget_no_memtable_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    let mut oracle = Oracle::new();
    let _ = fill(&mut db, &mut oracle, 0x5851_f42d_4c95_7f2d, 12);
    db.flush().unwrap();
    assert_eq!(db.stats().0.kept, 0);
    check_all(&mut db, &oracle);
}
