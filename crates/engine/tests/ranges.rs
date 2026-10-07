//! A node's ranges on a real hyper-rt runtime (docs/design/engine-structure.md §2, step E2):
//! four ranges on two shards, so a shard's ranges share its thread, each range's engine small
//! enough that maintenance runs all through. Client threads write disjoint keys at once, and
//! each checks every read, and its own keys in scans that page across ranges, against its own
//! oracle: exact, whatever the other threads do meanwhile.
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
use hyper_block::sim::SimFile;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::Error;
use mantle_engine::ranges::{Client, Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

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
const THREADS: u64 = 4;
const KEYS: u64 = 2_000;
const OPS: u64 = 6_000;
/// A test harness's fixed slice: the runtime's step budget below.
const SLICE_NS: u64 = 50_000;

fn config(shards: u16) -> RuntimeConfig {
    RuntimeConfig {
        shards,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: SLICE_NS,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

fn ranges_config(clients: usize) -> RangesConfig {
    RangesConfig {
        clients,
        slice_ns: SLICE_NS,
        spin_ns: SLICE_NS,
    }
}

fn key(k: u64, t: u64) -> Vec<u8> {
    format!("k{k:05}-t{t}").into_bytes()
}

fn engines(starts: &[&str]) -> Vec<(Vec<u8>, ShardDb<SimFile>)> {
    starts
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let align = Alignment::new(4096).unwrap();
            let file = SimFile::new(align, Alignment::new(512).unwrap(), 17 + i as u64).unwrap();
            let mut db = ShardDb::create(file, STORE, MEM, TRUNK).unwrap();
            db.set_cache(16);
            (s.as_bytes().to_vec(), db)
        })
        .collect()
}

/// Thread `t`'s keys in a scan page of `[from, end)`, paged `limit` rows at a time, against its
/// oracle.
fn check_scan(
    client: &mut Client<'_>,
    oracle: &BTreeMap<u64, Option<Vec<u8>>>,
    t: u64,
    from: u64,
    end: u64,
    limit: usize,
) {
    let (from_key, end_key) = (key(from, 0), key(end, 0));
    let mut page = Rows::new();
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut at = from_key.clone();
    let mut next = Vec::new();
    let mut pages = 0;
    loop {
        page.clear();
        let more = client
            .scan(&at, Some(&end_key), limit, &mut page, &mut next)
            .unwrap();
        assert!(page.len() <= limit);
        rows.extend(page.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
        pages += 1;
        assert!(pages < 100_000, "a scan that never ends");
        if !more {
            break;
        }
        assert!(next > at || !page.is_empty(), "a page that does not move");
        at.clone_from(&next);
    }
    for w in rows.windows(2) {
        assert!(w[0].0 < w[1].0, "rows out of order");
    }
    let mine: Vec<(Vec<u8>, Vec<u8>)> = rows
        .into_iter()
        .filter(|(k, _)| k.ends_with(format!("-t{t}").as_bytes()))
        .collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = oracle
        .range(from..end)
        .filter_map(|(&k, v)| v.as_ref().map(|v| (key(k, t), v.clone())))
        .collect();
    assert_eq!(mine, want, "thread {t} scan [{from}, {end}) by {limit}");
}

fn worker(ranges: &Ranges, t: u64) -> BTreeMap<u64, Option<Vec<u8>>> {
    let mut client = ranges.client().unwrap();
    let mut oracle: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
    let mut x = 0x2545_f491_4f6c_dd1du64 ^ (t + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut value = Vec::new();
    for i in 0..OPS {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = x % KEYS;
        if x.is_multiple_of(7) {
            client.delete(&key(k, t)).unwrap();
            oracle.insert(k, None);
        } else {
            let v = format!("v{i}-{t}-{}", "x".repeat((x % 40) as usize)).into_bytes();
            client.put(&key(k, t), &v).unwrap();
            oracle.insert(k, Some(v));
        }
        let probe = (x >> 20) % KEYS;
        for k in [k, probe] {
            let found = client.get(&key(k, t), &mut value).unwrap();
            match oracle.get(&k) {
                Some(Some(v)) => assert!(found && &value == v, "thread {t} key {k}"),
                _ => assert!(!found, "thread {t} key {k} reads a value it does not hold"),
            }
        }
        if i % 37 == 0 {
            let a = (x >> 8) % KEYS;
            let b = (x >> 32) % KEYS;
            let (from, end) = (a.min(b), a.max(b) + 1);
            check_scan(&mut client, &oracle, t, from, end, 1 + (x % 23) as usize);
        }
    }
    oracle
}

#[test]
fn clients_on_many_threads_read_their_own_writes_across_ranges_and_shards() {
    let runtime = Runtime::start(&config(2)).unwrap();
    let starts = ["", "k00500", "k01000", "k01500"];
    let ranges = Ranges::start(
        &runtime,
        engines(&starts),
        ranges_config(THREADS as usize + 1),
    )
    .unwrap();
    let oracles: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let ranges = &ranges;
                s.spawn(move || worker(ranges, t))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut client = ranges.client().unwrap();
    let stats = client.stats().unwrap();
    assert_eq!(stats.len(), starts.len());
    for (flush, trunk, _) in &stats {
        assert!(flush.flushes > 10, "{flush:?}");
        assert!(trunk.leaf_compactions > 0, "{trunk:?}");
    }
    client.flush().unwrap();
    client.checkpoint(1).unwrap();
    // Every key of every thread, whole scans from the first key to past the last, by pages.
    for (t, oracle) in oracles.iter().enumerate() {
        check_scan(&mut client, oracle, t as u64, 0, KEYS, 97);
        let mut value = Vec::new();
        for (&k, v) in oracle {
            let found = client.get(&key(k, t as u64), &mut value).unwrap();
            match v {
                Some(v) => assert!(found && &value == v, "thread {t} key {k}"),
                None => assert!(!found, "thread {t} key {k}"),
            }
        }
    }
    drop(client);
    ranges.stop().unwrap();
    runtime.shutdown().unwrap();
}

#[test]
fn clients_past_the_bound_are_refused_and_admitted_once_one_goes() {
    let runtime = Runtime::start(&config(1)).unwrap();
    let ranges = Ranges::start(&runtime, engines(&[""]), ranges_config(2)).unwrap();
    let a = ranges.client().unwrap();
    let mut b = ranges.client().unwrap();
    assert!(matches!(
        ranges.client(),
        Err(Error::LimitExceeded { limit: 2, .. })
    ));
    drop(a);
    let mut c = ranges.client().unwrap();
    b.put(b"k", b"v").unwrap();
    let mut value = Vec::new();
    assert!(c.get(b"k", &mut value).unwrap());
    assert_eq!(value, b"v");
    drop((b, c));
    ranges.stop().unwrap();
    runtime.shutdown().unwrap();
}

#[test]
fn a_range_whose_shard_is_gone_is_reported_gone() {
    let runtime = Runtime::start(&config(1)).unwrap();
    let ranges = Ranges::start(&runtime, engines(&["", "m"]), ranges_config(1)).unwrap();
    let mut client = ranges.client().unwrap();
    client.put(b"a", b"1").unwrap();
    runtime.shutdown().unwrap();
    assert!(matches!(client.put(b"z", b"2"), Err(Error::Gone { .. })));
    let mut value = Vec::new();
    assert!(matches!(
        client.get(b"a", &mut value),
        Err(Error::Gone { .. })
    ));
}

#[test]
fn ranges_must_ascend_from_the_empty_key() {
    let runtime = Runtime::start(&config(1)).unwrap();
    for starts in [&["a"][..], &["", "m", "c"], &["", "m", "m"]] {
        assert!(matches!(
            Ranges::start(&runtime, engines(starts), ranges_config(1)),
            Err(Error::InvalidArgument { .. })
        ));
    }
    runtime.shutdown().unwrap();
}
