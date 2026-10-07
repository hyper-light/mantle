//! The shard's paced maintenance (docs/design/engine-structure.md §6) against a `BTreeMap`: with
//! memtables of a few KiB and a fanout of 3, puts rotate memtables, pack them a slice at a time,
//! queue packed branches and run the trunk's cascades a slice at a time, all interleaved. Every
//! key reads its newest value between any two operations, wherever it then lives: the memtable,
//! the one being packed, a pending branch, or a tree whose cascade is part done. Flushed at the
//! end, the store holds exactly the extents the engine names.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::buf::Alignment;
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

fn check(db: &mut ShardDb<SimFile>, oracle: &BTreeMap<u64, Option<Vec<u8>>>, k: u64) {
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
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 17).unwrap();
    let mut db = ShardDb::create(file, STORE, MEM, TRUNK).unwrap();
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
}
