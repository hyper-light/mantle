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

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use std::collections::BTreeMap;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Crash, Fault, SimFile};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::{Consolidation, TrunkConfig};

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
            return (acked, Err(finished_file(db.into_file()).0));
        }
        let applied = i + 1;
        if applied.is_multiple_of(EVERY) {
            if db.checkpoint(applied).is_err() {
                return (acked, Err(finished_file(db.into_file()).0));
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
    let file = finished_file(db.into_file()).0;
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

/// Consolidation changes the trunk's layout only: the checkpoint before it and any after it
/// recover the same logical state, keys whose last operation deleted them included. Adapted
/// from Codex's bounded-consolidation sweep, with the consolidation the one seeks pay for.
fn consolidation_baseline(
    seed: u64,
    mem: usize,
    trunk: TrunkConfig,
) -> (SimFile, BTreeMap<Vec<u8>, Vec<u8>>, u64) {
    let applied = u64::from(KEYS);
    let mut db = ShardDb::create(sim(seed), STORE, mem, trunk).unwrap();
    for i in 0..applied {
        let (k, v) = op(i);
        match v {
            Some(v) => db.put(&key(k), &v).unwrap(),
            None => db.delete(&key(k)).unwrap(),
        }
    }
    db.checkpoint(applied).unwrap();
    let expected = state(applied);
    // A repeated value gives an index pivot a packed branch without changing the golden state.
    let (k, v) = expected.first_key_value().unwrap();
    db.put(k, v).unwrap();
    db.checkpoint(applied).unwrap();
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    (file, expected, applied)
}

/// Rows as keys and values.
type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Every row by pages of `limit`, the scan's errors returned.
fn scan_all(
    db: &mut ShardDb<SimFile>,
    from: &[u8],
    limit: usize,
) -> Result<Pairs, mantle_engine::Error> {
    let mut rows = mantle_engine::rows::Rows::new();
    let mut next = Vec::new();
    let mut got = Vec::new();
    let mut at = from.to_vec();
    for _ in 0..=KEYS {
        rows.clear();
        let more = db.scan(&at, None, limit, &mut rows, &mut next)?;
        got.extend(rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
        if !more {
            break;
        }
        assert!(next > at);
        at.clone_from(&next);
    }
    Ok(got)
}

fn check_consolidated_recovery(db: &mut ShardDb<SimFile>, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    check(db, u64::from(KEYS));
    let all: Vec<_> = expected
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(scan_all(db, &[], TRUNK.fanout).unwrap(), all);
    // Starts inside a range and beyond the tree, matching no key.
    for absent in [
        b"bucket/obj-000000/missing".as_slice(),
        b"zz/absent".as_slice(),
    ] {
        let want: Vec<_> = all
            .iter()
            .filter(|(k, _)| k.as_slice() >= absent)
            .cloned()
            .collect();
        assert_eq!(
            scan_all(db, absent, usize::try_from(KEYS).unwrap()).unwrap(),
            want
        );
    }
    db.check_references().unwrap();
}

/// Seeks across every key pay for every pivot's consolidation, idle time runs it, and a
/// checkpoint records it: `acknowledged` once the checkpoint returned.
fn consolidate_and_checkpoint(
    db: &mut ShardDb<SimFile>,
    applied: u64,
    acknowledged: &mut bool,
) -> Result<(), mantle_engine::Error> {
    db.set_consolidation(Consolidation::Always);
    scan_all(db, &[], 1)?;
    db.maintain(u64::MAX)?;
    db.checkpoint(applied + 1)?;
    *acknowledged = true;
    db.land()
}

#[test]
fn consolidation_seeks_paid_for_recovers_the_same_state_at_every_crash_point() {
    // Standard leaves keep branches at index pivots; leaves of a fanout's entries split the
    // consolidated leaves into many parts and the root more than once. Every cut is one of the
    // clean consolidation's writes and flushes, as in the sweep above.
    for (mem, trunk) in [
        (MEM, TRUNK),
        (
            MEM * usize::try_from(TRUNK.leaf_entries).unwrap(),
            TrunkConfig {
                leaf_entries: u64::try_from(TRUNK.fanout).unwrap(),
                ..TRUNK
            },
        ),
    ] {
        let (file, expected, baseline) = consolidation_baseline(1, mem, trunk);
        let before = file.stats().unwrap();
        let (mut db, applied) = ShardDb::open(file, STORE, mem, trunk).unwrap();
        assert_eq!(applied, baseline);
        let mut acknowledged = false;
        consolidate_and_checkpoint(&mut db, baseline, &mut acknowledged).unwrap();
        assert!(acknowledged);
        let (_, stats, _) = db.stats();
        assert!(stats.consolidations > 0, "{stats:?}");
        assert!(!db.owed());
        let (file, landed) = finished_file(db.into_file());
        landed.unwrap();
        let after = file.stats().unwrap();
        let (mut db, applied) = ShardDb::open(file, STORE, mem, trunk).unwrap();
        assert_eq!(applied, baseline + 1);
        check_consolidated_recovery(&mut db, &expected);
        let cuts = (after.writes - before.writes) + (after.syncs - before.syncs);
        assert!(cuts > 0);
        let mut cases = 0usize;
        for (mode, seed) in [
            (Crash::Random, 21),
            (Crash::LoseAll, 0),
            (Crash::KeepAll, 0),
        ] {
            for fault in (0..cuts)
                .map(|ops| Fault::PowerCut { ops })
                .chain([Fault::WriteError, Fault::SyncError])
            {
                let (file, expected, baseline) = consolidation_baseline(seed, mem, trunk);
                file.inject(fault.clone()).unwrap();
                let (mut db, applied) = ShardDb::open(file, STORE, mem, trunk).unwrap();
                assert_eq!(applied, baseline);
                let mut acknowledged = false;
                let result = consolidate_and_checkpoint(&mut db, baseline, &mut acknowledged);
                assert!(result.is_err(), "{fault:?} {mode:?}: the fault stopped it");
                let file = finished_file(db.into_file()).0;
                file.crash(mode).unwrap();
                file.clear_faults().unwrap();
                let (mut db, applied) = ShardDb::open(file, STORE, mem, trunk).unwrap();
                assert!(
                    applied == baseline || applied == baseline + 1,
                    "{fault:?} {mode:?}: recovered checkpoint {applied}"
                );
                if acknowledged {
                    assert_eq!(applied, baseline + 1);
                }
                check_consolidated_recovery(&mut db, &expected);
                // The recovered engine finishes the same consolidation and reopens to it.
                let mut done = false;
                consolidate_and_checkpoint(&mut db, baseline, &mut done).unwrap();
                let (file, landed) = finished_file(db.into_file());
                landed.unwrap();
                let (mut db, applied) = ShardDb::open(file, STORE, mem, trunk).unwrap();
                assert_eq!(applied, baseline + 1);
                check_consolidated_recovery(&mut db, &expected);
                cases += 1;
            }
        }
        println!(
            "consolidation leaf_entries {}: {cuts} write/flush cuts, {cases} crash/fault cases",
            trunk.leaf_entries
        );
    }
}
