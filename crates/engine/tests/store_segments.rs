//! A shard whose store file grows by segments (hyper-block's `SegmentedFile`; docs/research/48),
//! against a `BTreeMap`: puts and deletes with tiny memtables, so the file grows by many segments,
//! checkpoints along the way, compactions on workers or on the shard. Every key reads its newest
//! value between operations; reopened, the store reads every key its last checkpoint holds and
//! holds exactly the extents the engine names. A segment's file never changes length once made
//! (the segmented file refuses a write past one), so a workload that completes wrote nothing
//! past any segment: every write landed in a segment written whole before its extents were handed
//! out. A segment added and not written whole before a crash is written again before any use.
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

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::CachingRequest;
use hyper_block::issuer::Issuer;
use hyper_block::segmented::SegmentedFile;
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
const EXTENT: u64 = 4096 * 8;
const MEM: usize = 4 * 1024;
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const KEYS: u64 = 1500;
const OPS: u64 = 12_000;
/// Segments a file may have: past what the workload makes, which the tests count.
const BOUND: usize = 1 << 12;

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:06}", k % 5).into_bytes()
}

fn align() -> Alignment {
    Alignment::new(4096).unwrap()
}

fn create(path: &Path) -> SegmentedFile {
    SegmentedFile::create(path, CachingRequest::Buffered, align(), BOUND).unwrap()
}

fn open(path: &Path) -> SegmentedFile {
    SegmentedFile::open(path, CachingRequest::Buffered, align(), BOUND).unwrap()
}

/// The shard's workers, each over the segments opened again as the shard's were.
fn workers(db: &mut ShardDb<SegmentedFile>, path: &Path) {
    let path: PathBuf = path.to_path_buf();
    db.set_workers(move || {
        SegmentedFile::open(&path, CachingRequest::Buffered, align(), BOUND).map_err(|e| {
            mantle_engine::Error::Io {
                op: "open a worker's file",
                detail: e.to_string(),
            }
        })
    })
    .unwrap();
}

fn check<F: BlockFile>(db: &mut ShardDb<F>, oracle: &BTreeMap<u64, Option<Vec<u8>>>, k: u64) {
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

/// Puts and deletes, a checkpoint every 1500, every key read every 50 operations.
fn workload(
    db: &mut ShardDb<SegmentedFile>,
    oracle: &mut BTreeMap<u64, Option<Vec<u8>>>,
    ops: std::ops::Range<u64>,
    seed: u64,
) {
    let mut x = seed;
    for i in ops {
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
        if (i + 1) % 1_500 == 0 {
            db.checkpoint(i).unwrap();
        }
        check(db, oracle, k);
        for j in (i % 50..KEYS).step_by(50) {
            check(db, oracle, j);
        }
    }
}

/// The segment files beside `path`, each its length.
fn segment_lengths(path: &Path) -> Vec<u64> {
    let mut lengths = Vec::new();
    for k in 1usize.. {
        let mut name = path.as_os_str().to_owned();
        name.push(format!(".{k}"));
        match std::fs::metadata(PathBuf::from(name)) {
            Ok(m) => lengths.push(m.len()),
            Err(_) => break,
        }
    }
    lengths
}

/// Runs the workload over a file grown by segments, with workers or without and with an issuer
/// or without, checkpoints, reopens and checks every key and the extents named.
fn run(with_workers: bool, with_issuer: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(create(&path), STORE, MEM, TRUNK).unwrap();
    let issuer = with_issuer.then(|| Issuer::start(dir.path(), 2).unwrap());
    if let Some(issuer) = &issuer {
        db.attach(issuer, 2).unwrap();
    }
    if with_workers {
        workers(&mut db, &path);
    }
    let mut oracle = BTreeMap::new();
    workload(&mut db, &mut oracle, 0..OPS, 0x2545_f491_4f6c_dd1d);
    db.checkpoint(OPS).unwrap();
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    db.check_references().unwrap();
    let seg = db.segments().unwrap();
    assert!(seg.pending.is_none() || with_workers, "{seg:?}");
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    // The file grew by segments, each a whole number of extents; the base holds nothing.
    let lengths = segment_lengths(&path);
    assert!(lengths.len() >= 3, "{lengths:?}");
    assert!(
        lengths.iter().all(|&l| l > 0 && l % EXTENT == 0),
        "{lengths:?}"
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    let (mut db, applied) = ShardDb::open(open(&path), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    db.check_references().unwrap();
    // Reopened, it grows on as it did.
    if with_workers {
        workers(&mut db, &path);
    }
    workload(
        &mut db,
        &mut oracle,
        OPS..OPS + 3_000,
        0x9e37_79b9_7f4a_7c15,
    );
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    db.check_references().unwrap();
    drop(issuer);
}

#[test]
fn every_key_reads_its_newest_value_on_a_file_grown_by_segments_on_workers() {
    run(true, false);
}

#[test]
fn every_key_reads_its_newest_value_on_a_file_grown_by_segments_through_the_issuer() {
    run(true, true);
}

#[test]
fn a_shard_without_workers_grows_its_file_in_place() {
    run(false, false);
}

/// A crash after a segment was added and before it was written whole: reopened, the store holds
/// it booked past its admitted extents and writes it whole again before any of it is used, and
/// the workload goes on into it.
#[test]
fn a_segment_added_and_not_written_whole_is_written_again_before_use() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(create(&path), STORE, MEM, TRUNK).unwrap();
    let mut oracle = BTreeMap::new();
    workload(&mut db, &mut oracle, 0..3_000, 0x2545_f491_4f6c_dd1d);
    db.checkpoint(3_000).unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    // The segment a growth added before the crash, its blocks allocated and never written.
    let before = segment_lengths(&path);
    let crashed = open(&path);
    let (_, start) = crashed.add(4 * EXTENT).unwrap();
    drop(crashed);
    let (mut db, applied) = ShardDb::open(open(&path), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, 3_000);
    // Written whole again and admitted at open, before any use: nothing is booked past them.
    let seg = db.segments().unwrap();
    assert!(seg.pending.is_none(), "{seg:?}");
    assert!(
        seg.admitted * EXTENT >= start + 4 * EXTENT,
        "{seg:?} {start}"
    );
    assert_eq!(
        segment_lengths(&path)[..before.len() + 1].len(),
        before.len() + 1
    );
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    workload(&mut db, &mut oracle, 3_000..6_000, 0x9e37_79b9_7f4a_7c15);
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    db.check_references().unwrap();
}
