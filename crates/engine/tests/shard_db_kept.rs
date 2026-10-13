//! Packed memtables kept readable for gets (research/51). With a memory budget, a memtable whose
//! branch is in the trunk stays readable from its own index, newest last, in the memory the
//! budget leaves idle. Against a `BTreeMap`, every key reads exactly what was written throughout:
//! puts, deletions and overwrites across many memtables, the kept memtables answering gets with
//! the newest version, a deletion in a newer one hiding an older one's value. A smaller budget
//! has them give their memory back; without a budget none is kept; reopened, the engine keeps
//! none and reads the trunk. With maintenance workers and without, so memtables packed on
//! workers and on the shard are both kept.
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
    for k in 0..KEYS {
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

fn run(with_workers: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    db.set_memory(BUDGET).unwrap();
    if with_workers {
        workers(&mut db, &path);
    }
    let mut oracle = Oracle::new();
    let mut x = fill(&mut db, &mut oracle, 0x9e37_79b9_7f4a_7c15, 24);
    db.flush().unwrap();
    let kept = db.stats().0;
    assert!(kept.kept >= 1, "no memtable kept: {kept:?}");
    check_all(&mut db, &oracle);
    let read = db.stats().0;
    assert!(
        read.kept_hits > kept.kept_hits,
        "no get answered by a kept memtable: {read:?}"
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
    check_all(&mut db, &oracle);
    let _ = fill(&mut db, &mut oracle, x, 3);
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

#[test]
fn memtables_packed_on_workers_are_kept_and_every_read_is_exact() {
    run(true);
}

#[test]
fn memtables_packed_on_the_shard_are_kept_and_every_read_is_exact() {
    run(false);
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
