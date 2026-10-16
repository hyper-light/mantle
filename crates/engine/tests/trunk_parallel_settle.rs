//! A leaf's split merged as a job a part on maintenance workers (research/58 §4): batches of puts
//! and deletes with values large enough that a few fill a page, so every leaf a settle splits
//! spans pages enough to cut each part at; each batch packed into a branch as a memtable would
//! be, incorporated, and the trunk's maintenance run to its end on a pool of workers over the
//! store's file, an idle step at a time, each taking one worker's message. After every batch every key reads its newest operation, absent keys read
//! nothing, and the store holds exactly the extents of the branches the trunk names. The settles
//! that split ran as several jobs, a part each: the first is the root leaf's, whose one input is
//! the first batch.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::Error;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Builder, Op};
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::pool::{self, Pool, Spawn};
use mantle_engine::trunk::{Trunk, TrunkConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 64,
};
/// A value of this many bytes: a page holds a handful, so a branch of a leaf's entries spans
/// tens of pages.
const VALUE: usize = 600;
const BATCHES: usize = 30;
const BATCH: usize = 100;
const KEYS: u64 = 2000;
/// An idle step's budget in work units, a few pages' merging: past it a step returns, so the
/// steps take the jobs' results one at a time.
const STEP: u64 = 256;
/// Idle steps a batch's maintenance may take: past any it takes, each step taking a result or
/// merging a budget.
const STEPS: usize = 1 << 20;

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:08}", k % 3).into_bytes()
}

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

/// Every extent the store holds is named by exactly one branch of the trunk, or is the
/// superblock's or the store's map's, and every one the trunk names is held.
fn check_refs(store: &Store<DeviceFile>, trunk: &Trunk) {
    let mut named: BTreeSet<u64> = BTreeSet::new();
    for b in trunk.branches() {
        for e in b.extents {
            assert!(named.insert(e), "extent {e} named by two branches");
        }
    }
    let held: BTreeSet<u64> = store
        .refs()
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(e, _)| e as u64)
        .collect();
    // The superblock's and the store's own map's extents besides.
    let mut expected = named.clone();
    expected.insert(0);
    expected.extend(store.map_extents());
    assert_eq!(held, expected, "the store's extents against the trunk's");
}

/// Every key of the oracle reads its newest operation, and keys never written read nothing.
fn check_reads(
    store: &mut Store<DeviceFile>,
    trunk: &mut Trunk,
    oracle: &BTreeMap<Vec<u8>, (Op, Vec<u8>)>,
) {
    let mut value = Vec::new();
    for (k, (op, v)) in oracle {
        let got = trunk.get(store, k, &mut value).unwrap();
        match op {
            Op::Put => {
                assert_eq!(got, Some(Op::Put), "key {:?}", String::from_utf8_lossy(k));
                assert_eq!(&value, v, "key {:?}", String::from_utf8_lossy(k));
            }
            // A tombstone may be dropped at a leaf: absent and deleted read alike.
            Op::Delete => assert_ne!(
                got,
                Some(Op::Put),
                "deleted {:?}",
                String::from_utf8_lossy(k)
            ),
        }
    }
    for k in KEYS..KEYS + 50 {
        assert_eq!(trunk.get(store, &key(k), &mut value).unwrap(), None);
    }
}

/// Do: a trunk on a pool of workers over the store's file; batches of puts and deletes, values a
/// few to a page, each packed, incorporated and the maintenance run to its end an idle step at a
/// time. Expect: every key
/// reads its newest operation after every batch, the store's extents are exactly the trunk's,
/// and the settles that split ran as several jobs, a part each.
#[test]
fn a_split_merges_as_a_job_a_part_on_the_workers_and_every_key_reads_its_newest_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(open(&path, true), CONFIG).unwrap();
    let mut trunk = Trunk::new(TRUNK).unwrap();
    let worker_path = path.clone();
    let spawn: Spawn = Box::new(move |seat| {
        let file = open(&worker_path, false);
        std::thread::Builder::new()
            .name(format!("settle-worker-{}", seat.id))
            .spawn(move || pool::serve(file, CONFIG, seat))
            .map_err(|error| Error::Io {
                op: "start a settle worker",
                detail: error.to_string(),
            })
    });
    trunk.set_pool(Pool::new(spawn, Pool::cores()).unwrap());
    let mut oracle: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..BATCHES {
        // The memtable's view of a batch: the last operation of each key, in key order.
        let mut mem: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
        for _ in 0..BATCH {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let entry = if x.is_multiple_of(7) {
                (Op::Delete, Vec::new())
            } else {
                (Op::Put, vec![(x % 251) as u8; VALUE])
            };
            mem.insert(key(x % KEYS), entry);
        }
        let mut b = Builder::new(&mut store, Keys::Exactly(mem.len() as u64)).unwrap();
        for (k, (op, v)) in &mem {
            b.add(&mut store, k, *op, v).unwrap();
        }
        let branch = b.finish(&mut store).unwrap();
        trunk.incorporate(&mut store, branch).unwrap();
        // The maintenance an idle shard runs, each step waiting for one worker's message: the
        // trunk applies what is done between the results of a settle's jobs.
        let mut steps = 0;
        while !trunk.is_idle() {
            trunk.idle_step(&mut store, STEP).unwrap();
            steps += 1;
            assert!(steps < STEPS, "maintenance not done within {STEPS} steps");
        }
        oracle.extend(mem);
        check_reads(&mut store, &mut trunk, &oracle);
        check_refs(&store, &trunk);
    }
    let t = trunk.stats();
    assert!(
        t.splits > 0 && t.parallel_settles > 0 && t.parallel_settle_jobs >= 2 * t.parallel_settles,
        "{t:?}"
    );
}
