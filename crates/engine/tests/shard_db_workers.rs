//! The shard's trunk compactions on maintenance workers (`ShardDb::set_workers`), against a
//! `BTreeMap`: with memtables of a few KiB and a fanout of 3, the root takes branches every few
//! hundred puts and the trunk's compactions run on other threads, over their own handles on the file, while
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

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::{Consolidation, TrunkConfig};
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

/// The entries the trunk counts as held, as branches enter and leave it, are those its shape walks.
fn held_is_the_shapes(db: &ShardDb<DeviceFile>) {
    let t = db.stats().1;
    assert_eq!(
        t.held_entries,
        t.shape.iter().map(|l| l.entries).sum::<u64>(),
        "{t:?}"
    );
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
    // The workload did what it is for: much maintenance, its pivot and leaf compactions and
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
    held_is_the_shapes(&db);
    db.checkpoint(OPS).unwrap();
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    check_scan(&mut db, &oracle, 64);
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    let (mut db, applied) = ShardDb::open(open(&path, false), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    held_is_the_shapes(&db);
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

/// A consolidation is priced at a compacted key's measured cost, so that cost is measured wherever
/// compactions run: with the trunk's compactions on workers, every one of them is timed and its
/// merged keys counted, and seeks that pay for a leaf's consolidation get it in idle time, with
/// no drain asking for it.
#[test]
fn seeks_buy_consolidations_priced_by_the_workers_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    workers(&mut db, &path);
    let oracle = workload(&mut db, false, 0x9e37_79b9_7f4a_7c15);
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    let (_, trunk, _) = db.stats();
    // A merge reads every entry it writes: every compacted entry was priced.
    assert!(
        trunk.merge_units >= trunk.entries_written && trunk.merge_ns > 0,
        "{trunk:?}"
    );
    // Seeks pay rent for the sources a consolidation would spare them, and idle time
    // consolidates the leaves they paid for.
    while db.stats().1.consolidations == 0 {
        check_scan(&mut db, &oracle, 7);
        while db.owed() {
            db.idle_step(64).unwrap();
        }
    }
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    check_scan(&mut db, &oracle, 64);
    db.check_references().unwrap();
}

/// Seeks buy consolidations until no descent holds more than the one source a consolidated path
/// keeps, whatever shape the fill left, an index pivot of one branch over a leaf of one included
/// (research/50 §14): with consolidation at the first seek with a source to spare, passes of
/// scanning every key and draining end when one pays for nothing, and then a scan opens at most one
/// trunk source a segment, every read exact throughout.
#[test]
fn seeks_consolidate_every_descent_to_one_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    db.set_consolidation(Consolidation::Always);
    workers(&mut db, &path);
    let oracle = workload(&mut db, false, 0x2545_f491_4f6c_dd1d);
    db.flush().unwrap();
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    let paid = |db: &ShardDb<DeviceFile>| {
        let (_, t, _) = db.stats();
        t.paid_index + t.paid_leaves
    };
    // A pass that pays consolidates what it paid for: an index pivot's flush moves its branches
    // into its child, whose settle the next pass pays for and which merges them into one. Every
    // two passes that pay merge at least two branches into one, and the trunk holds no more
    // branches than the entries written, so the passes are bounded by twice those.
    let bound = 2 * (OPS + 1);
    let mut passes = 0u64;
    loop {
        let before = paid(&db);
        check_scan(&mut db, &oracle, 7);
        while db.owed() {
            db.idle_step(64).unwrap();
        }
        if paid(&db) == before {
            break;
        }
        passes += 1;
        assert!(passes <= bound, "seeks kept paying after {passes} passes");
    }
    let (_, t0, _) = db.stats();
    check_scan(&mut db, &oracle, 7);
    let (_, t1, _) = db.stats();
    let sources = (t1.charged_leaf_sources + t1.charged_index_sources)
        - (t0.charged_leaf_sources + t0.charged_index_sources);
    let segments = t1.scan_segments - t0.scan_segments;
    assert!(
        segments > 0 && sources <= segments,
        "{sources} trunk sources over {segments} segments after {passes} passes"
    );
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    db.check_references().unwrap();
}

/// A leaf's settle takes the in-flight bundles it finds among its inputs (research/53 §5): a
/// consolidation's flushes bring each paid leaf the data above it, and the leaf settles that with
/// its own in one merge rather than compacting it into its bundle first and settling it again. The
/// fill's last flush leaves data at the root, over keys every leaf already holds older versions
/// of, so the consolidations, the quiet shard's after the drain or the seeks' passes, bring some
/// leaf in-flight data: settles take it, every read stays exact, and every reference is accounted.
#[test]
fn a_leafs_settle_takes_the_in_flight_bundles_a_consolidation_brings() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    db.set_consolidation(Consolidation::Always);
    workers(&mut db, &path);
    let oracle = workload(&mut db, false, 0x5bd1_e995_9e37_79b9);
    // Counted from before the drain: a quiet shard whose writes paid for it consolidates there.
    let before = db.stats().1.settle_inflight_entries;
    db.flush().unwrap();
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    // Passes of scanning every key and draining, as seeks_consolidate_every_descent_to_one_source
    // bounds them, until one pays for nothing.
    let paid = |db: &ShardDb<DeviceFile>| {
        let (_, t, _) = db.stats();
        t.paid_index + t.paid_leaves
    };
    let bound = 2 * (OPS + 1);
    let mut passes = 0u64;
    loop {
        let paid_before = paid(&db);
        check_scan(&mut db, &oracle, 7);
        while db.owed() {
            db.idle_step(64).unwrap();
        }
        if paid(&db) == paid_before {
            break;
        }
        passes += 1;
        assert!(passes <= bound, "seeks kept paying after {passes} passes");
    }
    let took = db.stats().1.settle_inflight_entries - before;
    assert!(
        took > 0,
        "no settle took in-flight entries over {passes} consolidating passes"
    );
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    check_scan(&mut db, &oracle, 64);
    db.check_references().unwrap();
}

/// A quiet shard consolidates its whole trunk once its writes since the last whole consolidation
/// cover one, and no sooner (research/53 §11): the workload's writes, many times the entries the
/// trunk holds, cover one, so its drain leaves the layout consolidated with no seek paying; a
/// burst of a few puts into that trunk covers none, so its drain consolidates nothing whole and
/// leaves its layout to the seeks' rent. Every read exact and every reference accounted after.
#[test]
fn a_quiet_shard_consolidates_what_its_writes_paid_for_and_no_more() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    workers(&mut db, &path);
    let mut oracle = workload(&mut db, false, 0x0f0f_3c3c_5a5a_9696);
    held_is_the_shapes(&db);
    db.flush().unwrap();
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    held_is_the_shapes(&db);
    let whole = db.stats().1.whole_consolidations;
    assert!(
        whole >= 1,
        "the workload's writes bought no whole consolidation"
    );
    // A burst far smaller than the trunk: its writes cover no whole consolidation.
    for k in 0..8u64 {
        let v = format!("burst-{k}").into_bytes();
        db.put(&key(k), &v).unwrap();
        oracle.insert(k, Some(v));
    }
    db.flush().unwrap();
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    held_is_the_shapes(&db);
    assert_eq!(
        db.stats().1.whole_consolidations,
        whole,
        "a burst of eight puts bought a whole consolidation"
    );
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    check_scan(&mut db, &oracle, 64);
    db.check_references().unwrap();
}

/// The pages a worker's compaction writes come back cached where they were written: the shard's
/// cache lends each job room, the worker keeps its pages in it as it writes them, and the shard
/// takes them with the job's branches. With a cache that holds the store and nothing read before
/// maintenance ends, reading every key after it reads no page from the device.
#[test]
fn a_workers_pages_are_cached_where_they_were_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    db.set_cache(1 << 16);
    workers(&mut db, &path);
    let mut oracle: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
    let mut x = 0x5851_f42d_4c95_7f2du64;
    for i in 0..OPS {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = x % KEYS;
        let v = format!("v{i}-{}", "x".repeat((x % 40) as usize)).into_bytes();
        db.put(&key(k), &v).unwrap();
        oracle.insert(k, Some(v));
    }
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    let (_, trunk, io) = db.stats();
    assert!(
        trunk.pivot_compactions > 0 && trunk.leaf_compactions > 0,
        "{trunk:?}"
    );
    let before = io.device_reads;
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    let (_, _, io) = db.stats();
    assert_eq!(io.device_reads, before, "{io:?}");
    db.check_references().unwrap();
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
/// write what the shard's own would. (Their maintenance differs: workers finish a task sooner, so
/// the root takes its pending branches in other batches.)
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

/// Do: on workers, with memtables of 64 KiB and leaves of 1024 entries (so a settle's share of
/// its leaves is larger than a leaf of each input, the most its cuts can tell keys apart by), put
/// and delete keys sharing a 23-byte prefix (`a/shared-prefix/object-`, so the cuts are found
/// past the bytes every key holds); checkpoint, reopen.
/// Expect: every key reads its newest value and a scan reads exactly the live keys, before and
/// after the reopen, every extent named once; the trunk holds what its shape walks; and where the
/// workers have more than one core (the pool's rule: the cores the OS reports less the shard's),
/// a settle that made several leaves ran as several tasks, one a leaf, so the settles' tasks
/// outnumber them, and where they have one, each settle ran as one.
#[test]
fn a_settle_runs_as_one_task_a_leaf_on_the_workers_cores() {
    const MEM: usize = 64 * 1024;
    const TRUNK: TrunkConfig = TrunkConfig {
        fanout: 3,
        leaf_entries: 1024,
    };
    const KEYS: u64 = 6000;
    const OPS: u64 = 40_000;
    let shared = |k: u64| format!("a/shared-prefix/object-{k:06}").into_bytes();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    workers(&mut db, &path);
    let mut oracle: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    let mut x = 0x2545_f491_4f6c_dd1d_u64;
    for i in 0..OPS {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = shared(x % KEYS);
        if x.is_multiple_of(7) {
            db.delete(&k).unwrap();
            oracle.insert(k, None);
        } else {
            let v = format!("v{i}-{}", "x".repeat((x % 40) as usize)).into_bytes();
            db.put(&k, &v).unwrap();
            oracle.insert(k, Some(v));
        }
    }
    let check_all = |db: &mut ShardDb<DeviceFile>| {
        for (k, v) in &oracle {
            let mut value = Vec::new();
            let got = if db.get(k, &mut value).unwrap() {
                Some(value)
            } else {
                None
            };
            assert_eq!(&got, v);
        }
        let want: Vec<(Vec<u8>, Vec<u8>)> = oracle
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect();
        let mut rows = Rows::new();
        let mut next = Vec::new();
        assert!(
            !db.scan(b"", None, usize::MAX, &mut rows, &mut next)
                .unwrap()
        );
        let got: Vec<(Vec<u8>, Vec<u8>)> =
            rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        assert_eq!(got, want);
    };
    check_all(&mut db);
    let (_, trunk, _) = db.stats();
    assert!(trunk.leaf_compactions > 0 && trunk.splits > 0, "{trunk:?}");
    let cores = std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .saturating_sub(1)
        .max(1);
    if cores > 1 {
        assert!(trunk.settle_tasks > trunk.leaf_compactions, "{trunk:?}");
    } else {
        assert_eq!(trunk.settle_tasks, trunk.leaf_compactions, "{trunk:?}");
    }
    held_is_the_shapes(&db);
    db.checkpoint(OPS).unwrap();
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    let (mut db, applied) = ShardDb::open(open(&path, false), STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    held_is_the_shapes(&db);
    check_all(&mut db);
    db.check_references().unwrap();
}
