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
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::{TrunkConfig, ViewChoice};
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

/// A page of `ShardDb::scan` as owned rows, and its continuation.
fn scan_page<F: BlockFile>(
    db: &mut ShardDb<F>,
    from: &[u8],
    end: Option<&[u8]>,
    limit: usize,
    out: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<Option<Vec<u8>>, mantle_engine::Error> {
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let more = db.scan(from, end, limit, &mut rows, &mut next)?;
    out.extend(rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
    Ok(if more { Some(next) } else { None })
}

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:06}", k % 5).into_bytes()
}

/// A scan of `[key(a), key(b))` with `limit` reads exactly the oracle's live keys there, in order,
/// and continues from the right key; and the whole keyspace, page by page, reads every live key.
fn check_scan<F: BlockFile>(
    db: &mut ShardDb<F>,
    oracle: &BTreeMap<u64, Option<Vec<u8>>>,
    a: u64,
    b: u64,
    limit: usize,
) {
    let live: BTreeMap<Vec<u8>, Vec<u8>> = oracle
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (key(*k), v.clone())))
        .collect();
    // Keys order by their bytes, not their numbers.
    let (ka, kb) = (key(a), key(b));
    let (lo, hi) = if ka <= kb { (ka, kb) } else { (kb, ka) };
    let want: Vec<(Vec<u8>, Vec<u8>)> = live
        .range(lo.clone()..hi.clone())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut got = Vec::new();
    let next = scan_page(db, &lo, Some(&hi), limit, &mut got).unwrap();
    let page: Vec<_> = want.iter().take(limit).cloned().collect();
    assert_eq!(got, page, "scan [{a}, {b}) limit {limit}");
    // The continuation lies past the page's last key and no further than the next live key: a key
    // whose newest entry is a deletion may be where the next page starts, and reads nothing.
    match want.get(limit) {
        Some((k, _)) => {
            let c = next.expect("a page that filled continues");
            assert!(
                c.as_slice() <= k.as_slice(),
                "continuation past the next live key"
            );
            if let Some((last, _)) = got.last() {
                assert!(
                    c.as_slice() > last.as_slice(),
                    "continuation at or before the page's end"
                );
            }
        }
        None => {
            if let Some(c) = next {
                // A deleted key left in range: the next page reads nothing.
                let mut rest = Vec::new();
                assert_eq!(
                    scan_page(db, &c, Some(&hi), limit.max(1), &mut rest).unwrap(),
                    None
                );
                assert!(
                    rest.is_empty(),
                    "a page past the range's live keys read {rest:?}"
                );
            }
        }
    }
    // The whole keyspace in pages of `limit`.
    let mut all = Vec::new();
    let mut from = Vec::new();
    while let Some(k) = scan_page(db, &from, None, limit.max(1), &mut all).unwrap() {
        from = k;
    }
    let every: Vec<_> = live.into_iter().collect();
    assert_eq!(all, every, "the keyspace page by page");
}

/// A narrow scan, `[key(k), key(k + 5))` (key `k` and nothing else of its bucket), reads exactly
/// the oracle's live keys there: most of the trunk's sources hold nothing in it, and their range
/// filters pass them over unread.
fn check_narrow<F: BlockFile>(
    db: &mut ShardDb<F>,
    oracle: &BTreeMap<u64, Option<Vec<u8>>>,
    k: u64,
) {
    let (lo, hi) = (key(k), key(k + 5));
    let want: Vec<(Vec<u8>, Vec<u8>)> = oracle
        .iter()
        .filter_map(|(n, v)| v.as_ref().map(|v| (key(*n), v.clone())))
        .filter(|(kk, _)| kk.as_slice() >= lo.as_slice() && kk.as_slice() < hi.as_slice())
        .collect();
    let mut got = Vec::new();
    assert_eq!(scan_page(db, &lo, Some(&hi), 16, &mut got).unwrap(), None);
    assert_eq!(got, want, "narrow scan at {k}");
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
    let (mut db, oracle) = run_on(file, 0, Some(&issuer), false);
    let (_, _, io) = db.stats();
    assert!(io.submitted > 0 && io.reads > 0, "{io:?}");
    db.checkpoint(OPS).unwrap();
    let (file, landed) = db.into_file();
    landed.unwrap();
    drop(file);
    let file = DeviceFile::open(&path, false, CachingRequest::Buffered, align).unwrap();
    let (mut db, applied) = ShardDb::open(file, STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    db.check_references().unwrap();
}

/// The same with the shard's idle time spent on maintenance between operations, slices of 1 to
/// 50 keys (`ShardDb::idle_step`): every key still reads its newest value, and idle slices alone
/// pay every debt to the end.
#[test]
fn a_record_cache_never_answers_with_a_version_older_than_one_written() {
    // Every key read between every two operations, puts and deletes among them, flushes and
    // compactions moving the originals: the cache's replicas must never be read once stale. A
    // cache of 4 KiB holds a few dozen records, so evictions and second chances run throughout.
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 31).unwrap();
    run_with(file, 0, None, false, 4 * 1024);
}

#[test]
fn a_record_read_then_rewritten_and_flushed_reads_its_new_version() {
    // The sequence that would expose a stale replica: a key read from the trunk (and cached),
    // rewritten, and flushed, so the memtable no longer answers and the trunk holds the new
    // version; and deleted the same way.
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 37).unwrap();
    let mut db = ShardDb::create(file, STORE, MEM, TRUNK).unwrap();
    db.set_record_cache(64 * 1024);
    let k = key(7);
    let mut out = Vec::new();
    db.put(&k, b"first").unwrap();
    db.checkpoint(1).unwrap();
    assert!(db.get(&k, &mut out).unwrap());
    assert_eq!(out, b"first");
    db.put(&k, b"second").unwrap();
    db.checkpoint(2).unwrap();
    assert!(db.get(&k, &mut out).unwrap());
    assert_eq!(
        out, b"second",
        "a cached version older than the one written"
    );
    db.delete(&k).unwrap();
    db.checkpoint(3).unwrap();
    assert!(
        !db.get(&k, &mut out).unwrap(),
        "a cached version of a deleted key"
    );
}

#[test]
fn idle_slices_between_operations_keep_every_read_exact_and_pay_every_debt() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 17).unwrap();
    run_on(file, 16, None, true);
}

fn run(cache: usize) {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 17).unwrap();
    let (db, _) = run_on(file, cache, None, false);
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
    idle: bool,
) -> (ShardDb<F>, BTreeMap<u64, Option<Vec<u8>>>) {
    run_with(file, cache, issuer, idle, 0)
}

/// [`run_on`] with a cache of `records` bytes of hot records.
fn run_with<F: BlockFile + 'static>(
    file: F,
    cache: usize,
    issuer: Option<&Issuer>,
    idle: bool,
    records: usize,
) -> (ShardDb<F>, BTreeMap<u64, Option<Vec<u8>>>) {
    let mut db = ShardDb::create(file, STORE, MEM, TRUNK).unwrap();
    db.set_cache(cache);
    db.set_record_cache(records);
    // The idle variant asserts views are rebuilt: rebuilt whenever they can be, not as measured
    // costs choose, so the test does not depend on timing.
    if idle {
        db.set_view_choice(ViewChoice::Rebuild);
    }
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
        // A scan now and then, wherever the keys then live.
        if i % 37 == 0 {
            check_scan(
                &mut db,
                &oracle,
                x % KEYS,
                (x >> 20) % KEYS,
                (x % 97) as usize,
            );
        }
        // A sweep of 1/50 of the keys each op: every key read every 50 ops, while each memtable
        // (about 60 entries) is packed and queued.
        for j in (i % 50..KEYS).step_by(50) {
            check(&mut db, &oracle, j);
        }
    }
    // Narrow scans over the trunk as the workload left it: every row exact, and sources passed
    // over by their range filters.
    let (skipped_before, _) = db.scan_filtered();
    for k in 0..KEYS {
        check_narrow(&mut db, &oracle, k);
    }
    let (skipped, opened) = db.scan_filtered();
    assert!(
        skipped > skipped_before,
        "no source ruled out: skipped {skipped} opened {opened}"
    );
    let (flush, trunk, _) = db.stats();
    eprintln!("{flush:?}\n{trunk:?}\nscan sources skipped {skipped} opened {opened}");
    // The workload did what it is for: many memtables packed in slices, cascades with leaf
    // compactions and splits.
    assert!(flush.flushes > 100, "{}", flush.flushes);
    assert!(trunk.leaf_compactions > 0 && trunk.splits > 0, "{trunk:?}");
    if idle {
        // Idle slices alone pay every debt, each slice doing some work while one is owed.
        assert!(db.owed());
        let mut steps = 0u64;
        while db.owed() {
            assert!(db.idle_step(7).unwrap() > 0, "a debt owed and no work done");
            steps += 1;
            assert!(steps < 1_000_000, "idle slices never paid the debts");
        }
        for k in 0..KEYS {
            check(&mut db, &oracle, k);
        }
        // The bundles' REMIX views are built in idle time with the rest, and scans read
        // bundles through them exactly.
        let (_, trunk, _) = db.stats();
        assert!(trunk.views_built > 0, "{trunk:?}");
        for (a, b, limit) in [(0, KEYS - 1, 1), (3, KEYS / 2, 7), (KEYS / 3, KEYS - 1, 64)] {
            check_scan(&mut db, &oracle, a, b, limit);
        }
        // More writes with every view built: flushes add runs to bundles that have views, and
        // idle slices rebuild those views from the older ones; every read and scan stays exact.
        let mut y = 0x6a09_e667_f3bc_c908u64;
        for i in 0..OPS / 2 {
            y ^= y << 13;
            y ^= y >> 7;
            y ^= y << 17;
            let k = y % KEYS;
            if y.is_multiple_of(7) {
                db.delete(&key(k)).unwrap();
                oracle.insert(k, None);
            } else {
                let v = format!("w{i}-{}", "y".repeat((y % 30) as usize)).into_bytes();
                db.put(&key(k), &v).unwrap();
                oracle.insert(k, Some(v));
            }
            check(&mut db, &oracle, k);
            // Idle time now and then pays everything owed, views too: the next flush into a
            // bundle with a view then has it rebuilt.
            if i % 50 == 0 {
                db.maintain(u64::MAX).unwrap();
            }
            if i % 97 == 0 {
                let (a, b) = ((y >> 8) % KEYS, (y >> 32) % KEYS);
                check_scan(
                    &mut db,
                    &oracle,
                    a.min(b),
                    a.max(b) + 1,
                    1 + (y % 19) as usize,
                );
            }
        }
        let mut steps = 0u64;
        while db.owed() {
            assert!(db.idle_step(7).unwrap() > 0, "a debt owed and no work done");
            steps += 1;
            assert!(steps < 1_000_000, "idle slices never paid the debts");
        }
        let (_, trunk, _) = db.stats();
        assert!(trunk.views_rebuilt > 0, "{trunk:?}");
        for k in 0..KEYS {
            check(&mut db, &oracle, k);
        }
        for (a, b, limit) in [(0, KEYS - 1, 1), (3, KEYS / 2, 7), (KEYS / 3, KEYS - 1, 64)] {
            check_scan(&mut db, &oracle, a, b, limit);
        }
    }
    db.flush().unwrap();
    // One unbounded idle call pays everything left, the views of every bundle of two runs or
    // more among it.
    db.maintain(u64::MAX).unwrap();
    assert!(!db.owed());
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    check_scan(&mut db, &oracle, 0, KEYS - 1, 13);
    db.check_references().unwrap();
    check_scan(&mut db, &oracle, 0, KEYS, 1_000);
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

/// A wider trunk (fanout 6), maintenance paid in idle time every so many writes: flushes add
/// several runs to a bundle with a view before the next idle time, so its rebuild merges them in a
/// run at a time, each into the view of the last. Every read and scan stays exact.
#[test]
fn views_rebuilt_over_several_added_runs_read_exactly() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 23).unwrap();
    let trunk = TrunkConfig {
        fanout: 6,
        leaf_entries: 400,
    };
    let mut db = ShardDb::create(file, STORE, MEM, trunk).unwrap();
    db.set_view_choice(ViewChoice::Rebuild);
    let mut oracle: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
    let mut x = 0x510e_527f_ade6_82d1u64;
    for i in 0..OPS {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = x % KEYS;
        if x.is_multiple_of(9) {
            db.delete(&key(k)).unwrap();
            oracle.insert(k, None);
        } else {
            let v = format!("c{i}-{}", "z".repeat((x % 35) as usize)).into_bytes();
            db.put(&key(k), &v).unwrap();
            oracle.insert(k, Some(v));
        }
        check(&mut db, &oracle, k);
        if i % 400 == 0 {
            db.maintain(u64::MAX).unwrap();
        }
        if i % 113 == 0 {
            let (a, b) = ((x >> 8) % KEYS, (x >> 32) % KEYS);
            check_scan(
                &mut db,
                &oracle,
                a.min(b),
                a.max(b) + 1,
                1 + (x % 17) as usize,
            );
        }
    }
    db.maintain(u64::MAX).unwrap();
    let (_, trunk, _) = db.stats();
    assert!(trunk.views_rebuilt > 0, "{trunk:?}");
    assert!(trunk.views_merged > trunk.views_rebuilt, "{trunk:?}");
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    check_scan(&mut db, &oracle, 0, KEYS - 1, 11);
}

/// Views written with a checkpoint are read back with the store: the reopened shard holds as many,
/// loaded, not rebuilt, and every read and scan through them is exact; the store holds exactly the
/// extents the engine names, the views' among them.
#[test]
fn views_saved_with_a_checkpoint_are_loaded_on_reopen() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 29).unwrap();
    let (mut db, oracle) = run_on(file, 16, None, true);
    db.maintain(u64::MAX).unwrap();
    let views = db.views();
    assert!(views > 0);
    db.checkpoint(OPS).unwrap();
    db.check_references().unwrap();
    let (file, landed) = db.into_file();
    landed.unwrap();
    let (mut db, applied) = ShardDb::open(file, STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    assert_eq!(db.views(), views);
    let (_, trunk, _) = db.stats();
    assert_eq!(trunk.views_built, 0, "{trunk:?}");
    db.check_references().unwrap();
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
    for (a, b, limit) in [(0, KEYS - 1, 1), (5, KEYS / 2, 9), (KEYS / 4, KEYS - 1, 50)] {
        check_scan(&mut db, &oracle, a, b, limit);
    }
    // Written again with another checkpoint, the store still holds exactly what is named.
    db.checkpoint(OPS + 1).unwrap();
    db.check_references().unwrap();
    // Writes that change the bundles drop their saved views; the next checkpoint releases the
    // extents only the last image named, so the store again holds exactly what is named.
    let mut oracle = oracle;
    for i in 0..OPS / 2 {
        let k = (i * 7919) % KEYS;
        let v = format!("r{i}").into_bytes();
        db.put(&key(k), &v).unwrap();
        oracle.insert(k, Some(v));
    }
    db.maintain(u64::MAX).unwrap();
    db.checkpoint(OPS + 2).unwrap();
    db.check_references().unwrap();
    for k in 0..KEYS {
        check(&mut db, &oracle, k);
    }
}

#[test]
fn the_write_budget_is_the_last_cycles_writes_within_the_owners_cap() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 29).unwrap();
    let mut db = ShardDb::create(file, STORE, MEM, TRUNK).unwrap();
    let run = STORE.page_size * STORE.extent_pages as usize;
    for cap in [1_000 * run, 3 * run] {
        db.set_write_budget(cap);
        let mut rotations = 0;
        let mut rotated = db.stats().0.rotations;
        let mut cycle_start = db.stats().2.pages_written;
        let mut x = 0x1234_5678_9abc_def0u64;
        while rotations < 6 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let before = db.stats().2.pages_written;
            db.put(&key(x % KEYS), b"value").unwrap();
            let r = db.stats().0.rotations;
            if r != rotated {
                // A rotation set the budget from the cycle that ended as this put began.
                let written = usize::try_from(before - cycle_start).unwrap() * STORE.page_size;
                let budget = db.write_budget();
                assert!(budget <= cap, "cap {cap}: budget {budget}");
                assert!(
                    budget <= written,
                    "cap {cap}: budget {budget} past {written} written"
                );
                assert!(
                    budget + run > written.min(cap),
                    "cap {cap}: budget {budget} for {written}"
                );
                cycle_start = before;
                rotated = r;
                rotations += 1;
            }
        }
    }
}
