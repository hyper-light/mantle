//! Memtables sorted where they are packed (research/40 §4): with maintenance workers, a full
//! memtable's copy of its arena goes to a worker that sorts and packs it, and the shard orders a
//! memtable only for the scans that read it. Against a `BTreeMap`, a fill with no scan moves no
//! entry of order work on the shard, and its memtables are packed by workers, the shard packing
//! one only when its place is needed and no worker has taken it: a rotation's stall, or the
//! flush. Which, the live host decides: workers that keep up take every memtable, and on one CPU
//! a worker runs only when the shard waits. A scan has the memtable it reads ordered from then
//! on, and the next one, while scans may still come; a memtable filled with no scan since the
//! last rotation is not ordered again. Every key and every scan reads exactly what was written
//! throughout, frozen memtables included.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::rows::Rows;
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
const KEYS: u64 = 3000;

/// Keys under a few shared prefixes, so the sort's eight-byte prefixes tie and order by key.
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

fn check_all<F: BlockFile>(db: &mut ShardDb<F>, oracle: &BTreeMap<u64, Option<Vec<u8>>>) {
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

fn check_scan<F: BlockFile>(db: &mut ShardDb<F>, oracle: &BTreeMap<u64, Option<Vec<u8>>>) {
    let want: BTreeMap<Vec<u8>, Vec<u8>> = oracle
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (key(*k), v.clone())))
        .collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = want.into_iter().collect();
    let mut got = Vec::new();
    let mut from = Vec::new();
    loop {
        let mut rows = Rows::new();
        let mut next = Vec::new();
        let more = db.scan(&from, None, 61, &mut rows, &mut next).unwrap();
        got.extend(rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
        if !more {
            break;
        }
        from = next;
    }
    assert_eq!(got, want);
}

/// Puts and deletes from `x` until `rotations` more memtables have rotated: the generator's
/// state after.
fn fill(
    db: &mut ShardDb<DeviceFile>,
    oracle: &mut BTreeMap<u64, Option<Vec<u8>>>,
    mut x: u64,
    rotations: u64,
) -> u64 {
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

#[test]
fn memtables_are_sorted_on_workers_and_ordered_on_the_shard_only_for_scans() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    workers(&mut db, &path);
    let mut oracle = BTreeMap::new();
    // Every memtable keeps a copy of its arena from its start, the first given one with the
    // workers.
    let mut x = fill(&mut db, &mut oracle, 0x9e37_79b9_7f4a_7c15, 1);
    let start = db.stats().0;
    x = fill(&mut db, &mut oracle, x, 12);
    let scan_free = db.stats().0;
    assert_eq!(
        scan_free.order_moves, start.order_moves,
        "a fill no scan read was ordered on the shard"
    );
    db.flush().unwrap();
    let flushed = db.stats().0;
    assert!(
        flushed.fed > start.fed,
        "no memtable packed by a worker: {start:?} {flushed:?}"
    );
    // The shard packed the rest, each at a rotation that stalled for its place, or at the flush:
    // the packing memtable and the active one.
    let by_shard = (flushed.flushes - start.flushes)
        - (flushed.fed - start.fed)
        - (flushed.repacked - start.repacked);
    assert!(
        by_shard <= flushed.stalls - start.stalls + 2,
        "the shard packed memtables a worker could take: {start:?} {flushed:?}"
    );
    check_all(&mut db, &oracle);
    check_scan(&mut db, &oracle);
    // A scan into the active memtable, then puts: it is ordered as it fills from here.
    x = fill(&mut db, &mut oracle, x, 0);
    for _ in 0..50 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = x % KEYS;
        db.put(&key(k), b"after").unwrap();
        oracle.insert(k, Some(b"after".to_vec()));
    }
    check_scan(&mut db, &oracle);
    let scanned = db.stats().0;
    x = fill(&mut db, &mut oracle, x, 1);
    let first = db.stats().0;
    assert!(
        first.order_moves > scanned.order_moves,
        "the memtable a scan read was not ordered as it filled"
    );
    // The next memtable too: a scan came since the last rotation.
    x = fill(&mut db, &mut oracle, x, 1);
    let second = db.stats().0;
    assert!(
        second.order_moves > first.order_moves,
        "the memtable after a scan was not ordered"
    );
    // With no scan since, the ones after are not: their workers sort them.
    x = fill(&mut db, &mut oracle, x, 2);
    let after = db.stats().0;
    assert_eq!(
        after.order_moves, second.order_moves,
        "a memtable no scan read was ordered"
    );
    // Reads exact throughout, with memtables frozen and sorting on workers: every key, and a
    // scan that seals them.
    check_all(&mut db, &oracle);
    check_scan(&mut db, &oracle);
    let _ = fill(&mut db, &mut oracle, x, 3);
    check_all(&mut db, &oracle);
    check_scan(&mut db, &oracle);
    db.flush().unwrap();
    check_all(&mut db, &oracle);
    check_scan(&mut db, &oracle);
    db.check_references().unwrap();
}
