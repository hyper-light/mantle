//! A worker's job begins where the shard's last branch of its life ended (docs/design/
//! engine-structure.md §3): the shard hands the job that tail with its grant, the extent held once
//! for the job, and takes the hold back once the job's first branch holds the extent, or as the
//! job is dropped. The counts stay exact: after a fill on a worker, with checkpoints and deletes,
//! every extent is held as often as branches, the allocator's map, the trunk's image and the
//! superblocks name it, before and after the store is opened again, and every row reads back.
#![cfg(test)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use super::*;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use std::collections::BTreeMap;
use std::path::Path;

const PAGE: usize = 4096;
/// The node suite's cell's extents, four pages: a packing of a small memtable takes two.
const CONFIG: Config = Config {
    page_size: PAGE,
    extent_pages: 4,
    max_extents: 1 << 12,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
/// The cell's memtables: a packing is a page or two.
const MEM: usize = 4096;
const KEYS: u64 = 1500;
const OPS: u64 = 6000;

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:06}", k % 5).into_bytes()
}

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap()
}

fn check(db: &mut ShardDb<DeviceFile>, oracle: &BTreeMap<u64, Option<Vec<u8>>>) {
    for k in 0..KEYS {
        let mut value = Vec::new();
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

#[test]
fn jobs_begin_in_the_tails_the_shard_hands_them_and_every_extent_is_counted_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), CONFIG, MEM, TRUNK).unwrap();
    let again = path.clone();
    db.set_workers_within(
        move || {
            DeviceFile::open(
                &again,
                false,
                CachingRequest::Buffered,
                Alignment::new(PAGE).unwrap(),
            )
            .map_err(|e| Error::Io {
                op: "open a worker's file",
                detail: e.to_string(),
            })
        },
        1,
        "",
    )
    .unwrap();
    let mut oracle = BTreeMap::new();
    let mut x = 0x9e37_79b9_7f4a_7c15_u64;
    for i in 0..OPS {
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
        if (i + 1) % 1500 == 0 {
            db.checkpoint(i + 1).unwrap();
            // A checkpoint is taken with no job out: every hold a job was handed is back.
            db.check_references().unwrap();
        }
    }
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    db.flush().unwrap();
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    let handed = db.store.tails_handed();
    assert!(handed > 0, "no job was handed a tail");
    db.check_references().unwrap();
    check(&mut db, &oracle);
    db.checkpoint(OPS).unwrap();
    db.check_references().unwrap();
    let file = match db.into_file() {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            file
        }
        IntoFile::Refused { error, .. } => panic!("{error:?}"),
    };
    drop(file);
    let (mut db, applied) = ShardDb::open(open(&path, false), CONFIG, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    db.check_references().unwrap();
    check(&mut db, &oracle);
}
