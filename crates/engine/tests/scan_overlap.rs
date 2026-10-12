//! A scan's sources start their leaf reads together (`ScanMerge::open_async`): with every page
//! served by the device, a seek over K branches hands all K leaf reads to the issuer before it
//! takes any, rather than one source's read after another's, and still reads every row exactly.
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

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::ranges::{Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::{ShardDb, issuer_batches};
use mantle_engine::store::Config;
use mantle_engine::trunk::{Consolidation, TrunkConfig};

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
/// Branches the scan merges, each holding keys across the whole range.
const BRANCHES: usize = 4;
/// A fanout past the branches, so no compaction folds them together.
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 2 * BRANCHES,
    leaf_entries: 1 << 20,
};
/// Keys a branch: leaves enough that the first key's leaf is one of many.
const KEYS: usize = 500;
/// A memtable that holds a branch's keys whole, so each flush makes one branch.
const MEMTABLE: usize = 1 << 20;

/// A file the OS never reports in memory: every demand read goes to the device's issuer.
struct Unresident(DeviceFile);

impl BlockFile for Unresident {
    fn alignment(&self) -> Alignment {
        self.0.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.0.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.0.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.0.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.0.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self(self.0.try_clone()?))
    }
}

#[test]
fn a_seek_hands_every_sources_leaf_read_to_the_device_before_it_takes_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let file = DeviceFile::open(
        &path,
        true,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .unwrap();
    let mut db = ShardDb::create(Unresident(file), STORE, MEMTABLE, TRUNK).unwrap();
    db.set_consolidation(Consolidation::Never);
    // Branch b holds keys k with k % BRANCHES == b: every branch has a leaf at the first key.
    let mut oracle = BTreeMap::new();
    for b in 0..BRANCHES {
        for i in 0..KEYS {
            let key = format!("{:08}", i * BRANCHES + b).into_bytes();
            let value = vec![b as u8; 100];
            db.put(&key, &value).unwrap();
            oracle.insert(key, value);
        }
        db.flush().unwrap();
    }
    db.set_cache(0);
    let issuer =
        Issuer::start_for(dir.path(), BRANCHES, issuer_batches(BRANCHES, BRANCHES)).unwrap();
    db.attach(&issuer, BRANCHES).unwrap();
    // The runtime path runs maintenance on workers; this trunk is settled and owes them none.
    let worker_path = path.clone();
    db.set_workers(move || {
        let file = DeviceFile::open(
            &worker_path,
            false,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).map_err(|error| mantle_engine::Error::Io {
                op: "align a scan overlap worker",
                detail: error.to_string(),
            })?,
        )
        .map_err(|error| mantle_engine::Error::Io {
            op: "open a scan overlap worker",
            detail: error.to_string(),
        })?;
        Ok(Unresident(file))
    })
    .unwrap();
    let held_before = db.stats().2.reads_held_most;
    let mut runtime = Runtime::start(&RuntimeConfig {
        shards: 1,
        // One Range and one client task.
        tasks_per_shard: 2,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 50_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    let shard = runtime.shard_ids()[0];
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 1,
            slice_ns: 50_000,
            spin_ns: 0,
            inline: false,
        },
    )
    .unwrap();
    let (done, result) = std::sync::mpsc::sync_channel(1);
    let want: Vec<(Vec<u8>, Vec<u8>)> = oracle
        .iter()
        .take(10)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    runtime
        .spawn_on(shard, async move {
            let mut client = ranges.client().unwrap();
            let mut rows = Rows::new();
            let mut next = Vec::new();
            client
                .scan_async(b"", None, 10, &mut rows, &mut next)
                .await
                .unwrap();
            let got: Vec<(Vec<u8>, Vec<u8>)> =
                rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
            let held = client.stats_async().await.unwrap()[0].2.reads_held_most;
            drop(client);
            ranges.stop_async().await.unwrap();
            done.send((got, held)).unwrap();
        })
        .unwrap();
    let (got, held) = result.recv().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    assert_eq!(got, want);
    // Nothing was handed to the issuer before the scan; the scan held one read a branch at once.
    assert_eq!(held_before, 0);
    assert_eq!(held, BRANCHES as u64);
}
