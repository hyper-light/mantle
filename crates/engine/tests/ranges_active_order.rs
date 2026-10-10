//! Active-table idle ordering preserves newest values, scans and durable recovery.
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

use std::collections::BTreeMap;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const OPS: u64 = 2048;

fn runtime() -> Runtime {
    // The existing Range fixtures' shape: one shard and a 50us maintenance slice.
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 50_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap()
}

fn scan(
    oracle: &BTreeMap<Vec<u8>, Vec<u8>>,
    from: &[u8],
    end: Option<&[u8]>,
    limit: usize,
    mut page: impl FnMut(&[u8], Option<&[u8]>, usize, &mut Rows, &mut Vec<u8>) -> Result<bool, Error>,
) {
    let expected: Vec<_> = oracle
        .iter()
        .filter(|(key, _)| key.as_slice() >= from && end.is_none_or(|end| key.as_slice() < end))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let mut at = from.to_vec();
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let mut got = Vec::new();
    for _ in 0..=expected.len() {
        rows.clear();
        let more = page(&at, end, limit, &mut rows, &mut next).unwrap();
        assert!(rows.len() <= limit);
        got.extend(
            rows.iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        if !more {
            assert_eq!(got, expected);
            return;
        }
        assert!(next > at, "pagination must move");
        at.clone_from(&next);
    }
    panic!("pagination exceeded its bound of one page per live row plus the end");
}

fn run(mem: usize, depth: usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("active.store");
    let align = Alignment::new(4096).unwrap();
    let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align).unwrap();
    let issuer = Issuer::start_for(
        &path,
        depth,
        mantle_engine::shard_db::issuer_batches(depth, depth),
    )
    .unwrap();
    let mut db = ShardDb::create(file, STORE, mem, TRUNK).unwrap();
    db.set_cache(usize::try_from(STORE.extent_pages).unwrap());
    db.attach(&issuer, depth).unwrap();
    db.set_write_budget(depth * usize::try_from(STORE.extent_pages).unwrap() * STORE.page_size)
        .unwrap();
    let worker_path = path.clone();
    db.set_workers(move || {
        DeviceFile::open(&worker_path, false, CachingRequest::Buffered, align).map_err(|error| {
            Error::Io {
                op: "open an active-order worker",
                detail: error.to_string(),
            }
        })
    })
    .unwrap();
    let mut runtime = runtime();
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 1,
            slice_ns: 50_000,
            spin_ns: 0,
        },
    )
    .unwrap();
    let mut client = ranges.client().unwrap();
    // Long common prefixes first; shorter, empty and binary keys shrink the shared prefix
    // after order work has begun. Later rounds overwrite and delete every kind of key.
    let mut keys: Vec<_> = (0..256)
        .map(|key| format!("tenant/shared-prefix/object-{key:04}").into_bytes())
        .collect();
    keys.extend([
        Vec::new(),
        vec![0],
        vec![0, 0],
        vec![0, 1],
        vec![0xff],
        b"tenant".to_vec(),
        b"tenant\0".to_vec(),
    ]);
    let mut oracle = BTreeMap::new();
    let mut out = Vec::new();
    for op in 0..OPS {
        let key = &keys[usize::try_from(op).unwrap() % keys.len()];
        if op % 5 == 0 {
            client.delete(key).unwrap();
            oracle.remove(key);
        } else {
            let value = op.to_le_bytes();
            client.put(key, &value).unwrap();
            oracle.insert(key.clone(), value.to_vec());
        }
        let found = client.get(key, &mut out).unwrap();
        assert_eq!(found, oracle.contains_key(key));
        if let Some(value) = oracle.get(key) {
            assert_eq!(&out, value);
        }
        if op % 32 == 0 {
            // A public request boundary between bursts allows idle slices; no timer or
            // private work/run-layout observation is needed for the correctness oracle.
            client.stats().unwrap();
            scan(
                &oracle,
                b"tenant/shared-prefix/object-0000\0",
                None,
                3,
                |from, end, limit, rows, next| client.scan(from, end, limit, rows, next),
            );
            scan(
                &oracle,
                b"",
                Some(b"tenant/shared-prefix/object-0180"),
                17,
                |from, end, limit, rows, next| client.scan(from, end, limit, rows, next),
            );
        }
        if op + 1 == OPS / 2 {
            client.checkpoint(op + 1).unwrap();
        }
    }
    scan(&oracle, b"", None, 1, |from, end, limit, rows, next| {
        client.scan(from, end, limit, rows, next)
    });
    client.flush().unwrap();
    client.checkpoint(OPS).unwrap();
    drop(client);
    ranges.stop().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    let file = DeviceFile::open(&path, false, CachingRequest::Buffered, align).unwrap();
    let (mut db, applied) = ShardDb::open(file, STORE, mem, TRUNK).unwrap();
    assert_eq!(applied, OPS);
    db.check_references().unwrap();
    for key in &keys {
        let found = db.get(key, &mut out).unwrap();
        assert_eq!(found, oracle.contains_key(key));
        if let Some(value) = oracle.get(key) {
            assert_eq!(&out, value);
        }
    }
    scan(&oracle, b"", None, 3, |from, end, limit, rows, next| {
        db.scan(from, end, limit, rows, next)
    });
    // Explicit full maintenance retains its public completion contract, regardless of the
    // narrower background owner policy. Exercise a new active tail after recovery too.
    let updated = oracle.keys().next().cloned().unwrap();
    let removed = oracle.keys().next_back().cloned().unwrap();
    db.put(&updated, b"updated-after-reopen").unwrap();
    oracle.insert(updated, b"updated-after-reopen".to_vec());
    db.delete(&removed).unwrap();
    oracle.remove(&removed);
    db.put(b"\0new-active-tail", b"new-after-reopen").unwrap();
    oracle.insert(b"\0new-active-tail".to_vec(), b"new-after-reopen".to_vec());
    assert!(db.owed());
    db.idle_step(u64::MAX).unwrap();
    db.maintain(u64::MAX).unwrap();
    assert!(!db.owed());
    scan(&oracle, b"", None, 3, |from, end, limit, rows, next| {
        db.scan(from, end, limit, rows, next)
    });
    db.checkpoint(OPS + 3).unwrap();
    db.check_references().unwrap();
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    let file = DeviceFile::open(&path, false, CachingRequest::Buffered, align).unwrap();
    let (mut db, applied) = ShardDb::open(file, STORE, mem, TRUNK).unwrap();
    assert_eq!(applied, OPS + 3);
    db.check_references().unwrap();
    scan(&oracle, b"", None, 17, |from, end, limit, rows, next| {
        db.scan(from, end, limit, rows, next)
    });
}

#[test]
fn unflushed_active_table_idle_and_scans_keep_newest_versions() {
    run(1 << 20, 1);
}

#[test]
fn rotated_active_table_idle_with_issuer_recovers_the_same_golden() {
    run(4 << 10, 2);
}
