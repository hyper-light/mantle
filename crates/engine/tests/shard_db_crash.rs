//! A shard's engine through crashes (docs/design/engine-structure.md §7, step E4c): a
//! deterministic workload of puts and deletes, each a Raft index, with flushes into the trunk and
//! checkpoints, on hyper-block's simulated device. Power is cut after every write and flush, the
//! unflushed sectors each lost, all lost or all kept, and the engine reopened. The checkpoint it
//! recovers must be the last acknowledged or the one in flight; every key must read as the
//! workload left it at that checkpoint's index; the store must hold exactly the extents the
//! engine names; and the engine must go on from there.
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

use hyper_block::buf::Alignment;
use hyper_block::sim::{Crash, Fault, SimFile};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 1 << 14,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 48,
};
/// A small memtable, so the workload flushes often and the trunk grows.
const MEM: usize = 6 * 1024;
const KEYS: u32 = 400;
const OPS: u64 = 1200;
/// A checkpoint every this many operations.
const EVERY: u64 = 150;

fn key(k: u32) -> Vec<u8> {
    format!("bucket/obj-{k:06}").into_bytes()
}

/// Operation `i` (its Raft index is `i + 1`): a put or a delete of a key.
fn op(i: u64) -> (u32, Option<Vec<u8>>) {
    let mut x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x5bd1_e995;
    x ^= x >> 29;
    let k = (x % u64::from(KEYS)) as u32;
    if x.is_multiple_of(7) {
        (k, None)
    } else {
        (
            k,
            Some(format!("v{i}-{}", "x".repeat((x % 40) as usize)).into_bytes()),
        )
    }
}

/// The state after the first `n` operations.
fn state(n: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut m = BTreeMap::new();
    for i in 0..n {
        let (k, v) = op(i);
        match v {
            Some(v) => {
                m.insert(key(k), v);
            }
            None => {
                m.remove(&key(k));
            }
        }
    }
    m
}

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(4096).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// Runs operations `from..OPS`, checkpointing every `EVERY`; the last index acknowledged by a
/// checkpoint, and the file if the run stopped on an error.
fn run(
    mut db: ShardDb<SimFile>,
    from: u64,
    mut acked: u64,
) -> (u64, Result<ShardDb<SimFile>, SimFile>) {
    for i in from..OPS {
        let (k, v) = op(i);
        let done = match v {
            Some(v) => db.put(&key(k), &v),
            None => db.delete(&key(k)),
        };
        if done.is_err() {
            return (acked, Err(db.into_file().0));
        }
        let applied = i + 1;
        if applied.is_multiple_of(EVERY) {
            if db.checkpoint(applied).is_err() {
                return (acked, Err(db.into_file().0));
            }
            acked = applied;
        }
    }
    (acked, Ok(db))
}

fn check(db: &mut ShardDb<SimFile>, applied: u64) {
    let expected = state(applied);
    let mut value = Vec::new();
    for k in 0..KEYS {
        let found = db.get(&key(k), &mut value).unwrap();
        match expected.get(&key(k)) {
            Some(v) => {
                assert!(found, "key {k} lost at applied {applied}");
                assert_eq!(&value, v, "key {k} at applied {applied}");
            }
            None => assert!(!found, "key {k} present at applied {applied}"),
        }
    }
    db.check_references().unwrap();
}

#[test]
fn every_crash_point_recovers_a_checkpoint_whose_state_reads_back() {
    // The clean run: its writes and flushes counted (the cut points), then its reads.
    let db = ShardDb::create(sim(1), STORE, MEM, TRUNK).unwrap();
    let (acked, done) = run(db, 0, 0);
    let db = done.ok().unwrap();
    assert_eq!(acked, OPS - OPS % EVERY);
    let file = db.into_file().0;
    let stats = file.stats().unwrap();
    let ops = stats.writes + stats.syncs;
    let (mut db, applied) = ShardDb::open(file, STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, acked);
    check(&mut db, applied);
    let (height, _, leaves) = {
        let db = ShardDb::create(sim(1), STORE, MEM, TRUNK).unwrap();
        let (_, done) = run(db, 0, 0);
        done.ok().unwrap().shape().unwrap()
    };
    assert!(
        height >= 2 && leaves >= 2,
        "the workload grows a trunk: height {height} leaves {leaves}"
    );
    let mut cases = 0;
    for cut in 0..ops {
        for (mode, seed) in [
            (Crash::Random, 21),
            (Crash::LoseAll, 0),
            (Crash::KeepAll, 0),
        ] {
            let file = sim(seed ^ cut);
            file.inject(Fault::PowerCut { ops: cut }).unwrap();
            let Ok(db) = ShardDb::create(file, STORE, MEM, TRUNK) else {
                continue;
            };
            let (acked, stopped) = run(db, 0, 0);
            let Err(file) = stopped else {
                panic!("cut {cut}: the power cut stopped the run")
            };
            file.crash(mode).unwrap();
            file.clear_faults().unwrap();
            let (mut db, applied) = ShardDb::open(file, STORE, MEM, TRUNK)
                .unwrap_or_else(|e| panic!("cut {cut} {mode:?}: {e}"));
            assert!(
                applied == acked || applied == acked + EVERY,
                "cut {cut} {mode:?}: recovered {applied} with {acked} acknowledged"
            );
            check(&mut db, applied);
            // The engine goes on: the log's entries after `applied` replayed, then more.
            let (_, done) = run(db, applied, applied);
            let mut db = done.ok().unwrap();
            db.checkpoint(OPS).unwrap();
            check(&mut db, OPS);
            cases += 1;
        }
    }
    assert!(cases > ops);
}
