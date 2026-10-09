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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
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

#[derive(Default)]
struct ReadGate {
    held: AtomicBool,
    entered: AtomicUsize,
    shard: Mutex<Option<std::thread::ThreadId>>,
}

struct Gated {
    file: DeviceFile,
    gate: Arc<ReadGate>,
}

impl BlockFile for Gated {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        if self.gate.held.load(Ordering::SeqCst) {
            // A regression to a synchronous owner read must fail, rather than holding the
            // shard that needs to answer the probe request below.
            if *self.gate.shard.lock().unwrap() == Some(std::thread::current().id()) {
                return Err(DiskError::Io {
                    op: "range read blocked its runtime shard",
                    path: std::path::PathBuf::new(),
                    source: std::io::Error::other("range read blocked its runtime shard"),
                });
            }
            self.gate.entered.fetch_add(1, Ordering::SeqCst);
            while self.gate.held.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        }
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: Arc::clone(&self.gate),
        })
    }
}

struct OpenOnDrop(Arc<ReadGate>);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.held.store(false, Ordering::SeqCst);
    }
}

fn native_db(path: &std::path::Path, gate: Arc<ReadGate>) -> ShardDb<Gated> {
    let file = Gated {
        file: DeviceFile::open(
            path,
            true,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        gate,
    };
    ShardDb::create(file, STORE, MEM, TRUNK).unwrap()
}

/// Both engines share one runtime thread. A required maintenance read on the first range
/// leaves that thread available to answer the first range's stats request and another
/// range's put/get. The completion then resumes exact values and a durable checkpoint.
#[test]
fn a_held_maintenance_read_leaves_the_shard_available_for_range_requests() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(ReadGate::default());
    let first_path = dir.path().join("first");
    let mut first = native_db(&first_path, Arc::clone(&gate));
    let mut oracle = BTreeMap::new();
    // Values span separate leaves. Several full memtables leave a cascade owed after puts,
    // so idle preparation needs cold input pages rather than merely tidying the memtable.
    for i in 0..TRUNK.leaf_entries {
        let key = format!("a{i:04}").into_bytes();
        let value = vec![i as u8; STORE.page_size / 2];
        first.put(&key, &value).unwrap();
        oracle.insert(key, value);
    }
    first.land().unwrap();
    assert!(first.owed());
    first.set_cache(0);
    first.set_write_budget(STORE.page_size * STORE.extent_pages as usize);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    first.attach(&issuer, 1).unwrap();
    let second = native_db(&dir.path().join("second"), Arc::default());
    let runtime = Runtime::start(&config(1)).unwrap();
    let (thread, on_shard) = std::sync::mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            thread.send(std::thread::current().id()).unwrap();
        })
        .unwrap();
    *gate.shard.lock().unwrap() = Some(on_shard.recv().unwrap());
    // Open the device before Runtime/Issuer teardown if an oracle fails while it is held.
    let open = OpenOnDrop(Arc::clone(&gate));
    gate.held.store(true, Ordering::SeqCst);
    let ranges = Ranges::start(
        &runtime,
        vec![(Vec::new(), first), (b"m".to_vec(), second)],
        ranges_config(1),
    )
    .unwrap();
    let mut client = ranges.client().unwrap();
    // Every response is evidence that the shard runs while preparation advances. Each probe
    // gives the range at least one idle slice (the client waits for each answer, so none is
    // queued behind it), and each slice does at least one unit of the owed cascade or stops at
    // a read still in flight, which is what this waits for. That cascade moves each of its
    // inputs through at most one compaction a pivot it passes, at most one an input: the inputs
    // squared bound the probes by the work owed, not by how fast slices run, and an absent cold
    // read fails rather than waiting forever for it.
    for _ in 0..oracle.len().saturating_mul(oracle.len()) {
        client.stats().unwrap();
        if gate.entered.load(Ordering::SeqCst) > 0 {
            break;
        }
    }
    assert!(gate.entered.load(Ordering::SeqCst) > 0);
    client.stats().unwrap();
    client
        .put(b"z-unrelated", b"progress while read is held")
        .unwrap();
    let mut out = Vec::new();
    assert!(client.get(b"z-unrelated", &mut out).unwrap());
    assert_eq!(out, b"progress while read is held");
    gate.held.store(false, Ordering::SeqCst);
    client.flush().unwrap();
    client.checkpoint(1).unwrap();
    for (key, expected) in &oracle {
        assert!(client.get(key, &mut out).unwrap());
        assert_eq!(&out, expected);
    }
    drop(client);
    ranges.stop().unwrap();
    runtime.shutdown().unwrap();
    drop(open);
    let file = Gated {
        file: DeviceFile::open(
            &first_path,
            false,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        gate,
    };
    let (mut recovered, applied) = ShardDb::open(file, STORE, MEM, TRUNK).unwrap();
    assert_eq!(applied, 1);
    recovered.check_references().unwrap();
    for (key, expected) in &oracle {
        assert!(recovered.get(key, &mut out).unwrap());
        assert_eq!(&out, expected);
    }
}
