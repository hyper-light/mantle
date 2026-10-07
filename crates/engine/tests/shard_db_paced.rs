//! The shard's paced maintenance (docs/design/engine-structure.md §6) against a `BTreeMap`: with
//! memtables of a few KiB and a fanout of 3, puts rotate memtables, pack them a slice at a time,
//! queue packed branches and run the trunk's cascades a slice at a time, all interleaved. Every
//! key reads its newest value between any two operations, wherever it then lives: the memtable,
//! the one being packed, a pending branch, or a tree whose cascade is part done. Checkpoints
//! along the way free extents for reuse. Flushed at the end, the store holds exactly the extents
//! the engine names.
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
use hyper_block::issuer::Issuer;
use hyper_block::sim::SimFile;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;
use std::collections::BTreeMap;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const MEM: usize = 4 * 1024;
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const KEYS: u64 = 1500;
const OPS: u64 = 12_000;

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:06}", k % 5).into_bytes()
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

#[test]
fn every_key_reads_its_newest_value_between_any_two_operations() {
    run(0);
}

/// The same with a page cache of 16 pages: pages cached and evicted all through the run.
#[test]
fn a_small_page_cache_evicting_all_the_time_reads_every_key_right() {
    run(16);
}

/// The same with a cache larger than the store ever grows, so a page stays cached until its
/// extent is freed and its address written again: a read never returns a page an address held
/// before.
#[test]
fn a_cache_never_serves_a_page_its_address_no_longer_holds() {
    run(4_096);
}

/// The same on a real file whose runs go to a two-worker issuer, two out at once, with no cache:
/// a read of a page whose run is in flight waits for it, a checkpoint waits for every run, and
/// the store reopened from the file reads every key the last checkpoint holds.
#[test]
fn runs_submitted_to_the_issuer_read_back_and_reopen_whole() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let align = Alignment::new(4096).unwrap();
    let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align).unwrap();
    let issuer = Issuer::start(dir.path(), 2).unwrap();
    let (mut db, oracle) = run_on(file, 0, Some(&issuer));
    let (_, _, io) = db.stats();
    assert!(io.submitted > 0 && io.reads > 0, "{io:?}");
    db.checkpoint(OPS).unwrap();
    drop(db.into_file());
    let file = DeviceFile::open(&path, false, CachingRequest::Buffered, align).unwrap();
    let (mut db, applied) = ShardDb::open(file, STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    db.check_references().unwrap();
}

fn run(cache: usize) {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 17).unwrap();
    let (db, _) = run_on(file, cache, None);
    let (_, _, io) = db.stats();
    // Every page enters the cache as it is written: a cache larger than the store serves every
    // read; a small one serves some and misses others.
    match cache {
        0 => assert_eq!((io.cache_hits, io.cache_misses), (0, 0)),
        // Compaction's scans take their pages from the cache too: no read reaches the device.
        4_096 => assert!(
            io.cache_hits > 0 && io.cache_misses == 0 && io.span_cache_hits > 0 && io.reads == 0,
            "{io:?}"
        ),
        _ => assert!(io.cache_hits > 0 && io.cache_misses > 0, "{io:?}"),
    }
}

/// The workload on `file`, its runs given to `issuer` when there is one; the engine flushed
/// and checked at the end, and the oracle of what every key holds.
fn run_on<F: BlockFile + 'static>(
    file: F,
    cache: usize,
    issuer: Option<&Issuer>,
) -> (ShardDb<F>, BTreeMap<u64, Option<Vec<u8>>>) {
    let mut db = ShardDb::create(file, STORE, MEM, TRUNK).unwrap();
    db.set_cache(cache);
    if let Some(issuer) = issuer {
        db.attach(issuer, 2).unwrap();
    }
    let mut oracle: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
    let mut x = 0x2545_f491_4f6c_dd1du64;
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
        // A checkpoint now and then: extents the trunk released are freed once it is durable,
        // and reused by later writes at the addresses they held.
        if (i + 1) % 1_500 == 0 {
            db.checkpoint(i).unwrap();
        }
        check(&mut db, &oracle, k);
        // A sweep of 1/50 of the keys each op: every key read every 50 ops, while each memtable
        // (about 60 entries) is packed and queued.
        for j in (i % 50..KEYS).step_by(50) {
            check(&mut db, &oracle, j);
        }
    }
    let (flush, trunk, _) = db.stats();
    eprintln!("{flush:?}\n{trunk:?}");
    // The workload did what it is for: many memtables packed in slices, cascades with leaf
    // compactions and splits.
    assert!(flush.flushes > 100, "{}", flush.flushes);
    assert!(trunk.leaf_compactions > 0 && trunk.splits > 0, "{trunk:?}");
    db.flush().unwrap();
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    db.check_references().unwrap();
    // Extent buffers come back to the store's pool and are taken again: a fresh allocation is
    // a page fault for each of its pages.
    let (_, _, io) = db.stats();
    assert!(
        io.buffers_fresh * 10 < io.buffers_taken,
        "{} fresh of {} taken",
        io.buffers_fresh,
        io.buffers_taken
    );
    (db, oracle)
}
