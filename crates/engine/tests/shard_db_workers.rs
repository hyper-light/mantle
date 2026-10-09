//! The shard's trunk compactions on maintenance workers (`ShardDb::set_workers`), against a
//! `BTreeMap`: with memtables of a few KiB and a fanout of 3, cascades start every few hundred
//! puts and their compactions run on other threads, over their own handles on the file, while
//! puts and reads go on. Every key reads its newest value between any two operations, wherever
//! it then lives, and every scan reads exactly the live keys. Checkpoints along the way free
//! extents the workers' outputs replaced; the store reopened reads every key the last
//! checkpoint holds and holds exactly the extents the engine names.
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

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap()
}

/// The shard's workers, each over the file opened again as the shard's was.
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

/// The whole keyspace, page by page of `limit` rows, reads exactly the live keys in order.
fn check_scan<F: BlockFile>(
    db: &mut ShardDb<F>,
    oracle: &BTreeMap<u64, Option<Vec<u8>>>,
    limit: usize,
) {
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
        let more = db.scan(&from, None, limit, &mut rows, &mut next).unwrap();
        got.extend(rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
        if !more {
            break;
        }
        from = next;
    }
    assert_eq!(got, want);
}

/// The workload: puts and deletes, a checkpoint every 1500, every key read every 50 operations
/// and a scan of everything every 997; idle slices between operations when `idle`. Returns
/// the oracle.
fn workload(db: &mut ShardDb<DeviceFile>, idle: bool, seed: u64) -> BTreeMap<u64, Option<Vec<u8>>> {
    let mut oracle: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
    let mut x = seed;
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
        if (i + 1) % 1_500 == 0 {
            db.checkpoint(i).unwrap();
        }
        if idle && x.is_multiple_of(3) {
            db.idle_step(1 + x % 50).unwrap();
        }
        check(db, &oracle, k);
        for j in (i % 50..KEYS).step_by(50) {
            check(db, &oracle, j);
        }
        if i % 997 == 0 {
            check_scan(db, &oracle, 1 + (x % 97) as usize);
        }
    }
    oracle
}

/// Runs the workload with workers, checkpoints, reopens without them and checks every key, the
/// scan and the extents named.
fn run(issuer: bool, idle: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    let issuer = issuer.then(|| Issuer::start(dir.path(), 2).unwrap());
    if let Some(issuer) = &issuer {
        db.attach(issuer, 2).unwrap();
    }
    workers(&mut db, &path);
    let oracle = workload(&mut db, idle, 0x2545_f491_4f6c_dd1d);
    let (flush, trunk, _) = db.stats();
    // The workload did what it is for: many cascades, their pivot and leaf compactions and
    // splits all on the workers.
    assert!(flush.flushes > 100, "{}", flush.flushes);
    // Most memtables packed on workers; one finds none free only when every worker is busy.
    assert!(flush.fed > 0, "{flush:?}");
    // And memtables frozen whole, awaiting their branches while the next filled, read
    // meanwhile by every check above.
    assert!(flush.frozen_most >= 1, "{flush:?}");
    assert!(
        trunk.pivot_compactions > 0 && trunk.leaf_compactions > 0 && trunk.splits > 0,
        "{trunk:?}"
    );
    let (held, want) = db.workers().unwrap();
    assert!(held >= 1 && want >= 1, "{held} {want}");
    db.checkpoint(OPS).unwrap();
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    check_scan(&mut db, &oracle, 64);
    db.check_references().unwrap();
    let (file, landed) = db.into_file();
    landed.unwrap();
    drop(file);
    let (mut db, applied) = ShardDb::open(open(&path, false), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    check_scan(&mut db, &oracle, 64);
    db.check_references().unwrap();
}

#[test]
fn every_key_reads_its_newest_value_with_compactions_on_workers() {
    run(false, false);
}

/// The same with the shard's runs on a two-worker issuer: a job waits for its inputs' runs to
/// land before a worker reads them.
#[test]
fn a_job_reads_its_inputs_only_once_their_runs_have_landed() {
    run(true, false);
}

/// The same with idle slices between operations, which wait for a worker's message rather
/// than spin.
#[test]
fn idle_slices_take_the_workers_results_and_keep_every_read_exact() {
    run(true, true);
}

/// The same workload, inline and on workers, leaves the same rows: the workers' compactions
/// write what the shard's own would. (Their cascades differ: workers finish one sooner, so the
/// next takes fewer pending branches.)
#[test]
fn workers_and_inline_compactions_hold_the_same_data() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("inline");
    let b = dir.path().join("workers");
    let mut inline = ShardDb::create(open(&a, true), STORE, MEM, TRUNK).unwrap();
    let mut remote = ShardDb::create(open(&b, true), STORE, MEM, TRUNK).unwrap();
    workers(&mut remote, &b);
    let one = workload(&mut inline, false, 0x9e37_79b9_7f4a_7c15);
    let two = workload(&mut remote, false, 0x9e37_79b9_7f4a_7c15);
    assert_eq!(one, two);
    inline.flush().unwrap();
    remote.flush().unwrap();
    let rows = |db: &mut ShardDb<DeviceFile>| {
        let mut rows = Rows::new();
        let mut next = Vec::new();
        assert!(
            !db.scan(b"", None, usize::MAX, &mut rows, &mut next)
                .unwrap()
        );
        rows.iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>()
    };
    assert_eq!(rows(&mut inline), rows(&mut remote));
}
