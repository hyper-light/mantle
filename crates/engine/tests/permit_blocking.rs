//! A shard whose owner states that it may wait on the device (`store::permit_blocking`: a node's
//! metadata shard, docs/design/node.md §1.2) runs the engine as a thread with no shard does: its
//! writes pay their shares of the trunk's maintenance synchronously, reading at once with no
//! issuer, and a range's every row deleted and written again, as a snapshot's install does, reads
//! back. Given maintenance workers, its compactions and packings run on them and the shard waits
//! for their results where it must, as a plain thread does, though hyper-rt's channel refuses a
//! shard's thread a blocking take. A shard whose owner did not state it refuses the engine's
//! synchronous calls. Each test runs on a thread of its own, since the statement is its thread's.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use mantle_engine::Error;
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;
use std::path::Path;

/// The engine's own small test configuration (`tests/pool_startup.rs`).
const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
/// Rows enough that their writes pass the memtable many times over and the trunk compacts.
const ROWS: u64 = 1_100;

/// A shard of one task, sized as hyper-rt-transport's test harness sizes one (`tests/exchanges.rs`).
fn shard() -> LocalRuntime {
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 2,
        interests_per_shard: interests_for(1),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

fn file(path: &Path) -> DeviceFile {
    opened(path, true).unwrap()
}

fn opened(path: &Path, create: bool) -> Result<DeviceFile, Error> {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .map_err(|e| Error::Io {
        op: "open a worker's file",
        detail: e.to_string(),
    })
}

/// A row as a Name range's are sized: a version's key and its record.
fn row(i: u64, generation: u8) -> (Vec<u8>, Vec<u8>) {
    (
        format!("v/b/k{i:06}/{}", "0".repeat(24)).into_bytes(),
        vec![generation; 120],
    )
}

fn every_row(db: &mut ShardDb<DeviceFile>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (mut rows, mut next) = (Rows::new(), Vec::new());
    db.scan(&[], None, usize::MAX, &mut rows, &mut next)
        .unwrap();
    rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

#[test]
fn a_shard_that_may_wait_runs_the_engine_synchronously() {
    std::thread::spawn(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        mantle_engine::store::permit_blocking();
        shard()
            .block_on(async move {
                let mut db = ShardDb::create(file(&path), CONFIG, CONFIG.page_size, TRUNK).unwrap();
                for i in 0..ROWS {
                    let (k, v) = row(i, 1);
                    db.put(&k, &v).unwrap();
                    db.checkpoint(i + 1).unwrap();
                }
                let first = every_row(&mut db);
                assert_eq!(first.len() as u64, ROWS);
                for (key, _) in &first {
                    db.delete(key).unwrap();
                }
                for i in 0..ROWS {
                    let (k, v) = row(i, 2);
                    db.put(&k, &v).unwrap();
                }
                db.checkpoint(ROWS + 1).unwrap();
                let second: Vec<_> = (0..ROWS).map(|i| row(i, 2)).collect();
                assert_eq!(every_row(&mut db), second);
            })
            .unwrap();
    })
    .join()
    .unwrap();
}

/// With workers, every checkpoint waits for the packing and the compactions out on them, which
/// took a worker's message with a blocking take hyper-rt refuses on a shard's thread: the first
/// checkpoint failed, "take a worker's message", and fenced the store. The shard now waits on the
/// count of messages landed; every row reads back, the trunk compacted on the workers, and the
/// store opened again stands at its last checkpoint.
#[test]
fn a_shard_that_may_wait_runs_its_maintenance_on_workers() {
    std::thread::spawn(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        mantle_engine::store::permit_blocking();
        let at = path.clone();
        shard()
            .block_on(async move {
                let mut db = ShardDb::create(file(&at), CONFIG, CONFIG.page_size, TRUNK).unwrap();
                let again = at.clone();
                db.set_workers_within(move || opened(&again, false), 2, "")
                    .unwrap();
                for i in 0..ROWS {
                    let (k, v) = row(i, 1);
                    db.put(&k, &v).unwrap();
                    if i % 7 == 0 {
                        db.checkpoint(i + 1).unwrap();
                    }
                }
                db.checkpoint(ROWS).unwrap();
                let (_, trunk, _) = db.stats();
                assert!(
                    trunk.pivot_compactions + trunk.leaf_compactions > 0,
                    "{trunk:?}"
                );
                let (seats, _) = db.workers().unwrap();
                assert!((1..=2).contains(&seats), "{seats} seats");
                let first: Vec<_> = (0..ROWS).map(|i| row(i, 1)).collect();
                assert_eq!(every_row(&mut db), first);
            })
            .unwrap();
        let reopened = shard()
            .block_on(async move {
                let (mut db, applied) = ShardDb::open(
                    opened(&path, false).unwrap(),
                    CONFIG,
                    CONFIG.page_size,
                    TRUNK,
                )
                .unwrap();
                (applied, every_row(&mut db).len() as u64)
            })
            .unwrap();
        assert_eq!(reopened, (ROWS, ROWS));
    })
    .join()
    .unwrap();
}

#[test]
fn a_shard_whose_owner_did_not_state_it_refuses_a_synchronous_store() {
    std::thread::spawn(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        let created = shard()
            .block_on(async move {
                ShardDb::create(file(&path), CONFIG, CONFIG.page_size, TRUNK).map(|_| ())
            })
            .unwrap();
        assert!(
            matches!(created, Err(Error::InvalidArgument { .. })),
            "a synchronous store made on a shard: {created:?}"
        );
    })
    .join()
    .unwrap();
}
