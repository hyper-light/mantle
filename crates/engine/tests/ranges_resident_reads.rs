//! An async range reads a demand page the OS holds in memory on its own shard, through its
//! issuer attachment's own duplicate (`Attached::read_resident_at`): no worker reads it, and no
//! thread is woken for it. Its original file belongs to its native retirement owner, so the
//! store holds none to read with.
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
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::ranges::{Ranges, RangesConfig};
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
const ROWS: usize = 2049;
const SLICE_NS: u64 = 50_000;

/// What every duplicate of one file counts, while armed: reads made from memory, and reads made
/// any other way (a worker's, or a blocking one).
#[derive(Default)]
struct Counts {
    armed: AtomicBool,
    resident: AtomicUsize,
    elsewhere: AtomicUsize,
}

/// A file on disk that says it can tell what the OS holds, and reads every page from memory.
struct Memory {
    file: DeviceFile,
    counts: &'static Counts,
}

impl BlockFile for Memory {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> Result<(), DiskError> {
        if self.counts.armed.load(Ordering::SeqCst) {
            self.counts.elsewhere.fetch_add(1, Ordering::SeqCst);
        }
        self.file.read_exact_at(bytes, offset)
    }
    fn reads_resident(&self) -> bool {
        true
    }
    fn read_resident_at(&mut self, bytes: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        self.file.read_exact_at(bytes, offset)?;
        if self.counts.armed.load(Ordering::SeqCst) {
            self.counts.resident.fetch_add(1, Ordering::SeqCst);
        }
        Ok(true)
    }
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file.write_all_at(bytes, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            counts: self.counts,
        })
    }
}

fn open(path: &Path, create: bool, counts: &'static Counts) -> Memory {
    Memory {
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap(),
        counts,
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("a/shared-prefix/object-{i:05}\0").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    vec![(i % 251) as u8; 100]
}

/// Do: fill a range's file cold, so every page is on disk and none in a memtable or the cache
/// (none is kept), then serve gets of every key from an async client on the range's shard.
/// Expect: every answer is the oracle's; every page a get reads is read from memory on the shard,
/// none by a worker or a blocking read; the store counts exactly those reads.
#[test]
fn an_async_range_reads_its_resident_pages_on_its_own_shard() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let counts: &'static Counts = Box::leak(Box::default());
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    let mut db = ShardDb::create(open(&path, true, counts), STORE, STORE.page_size, TRUNK).unwrap();
    db.set_cache(0);
    let mut oracle = BTreeMap::new();
    for i in 0..ROWS {
        db.put(&key(i), &value(i)).unwrap();
        oracle.insert(key(i), value(i));
    }
    db.flush().unwrap();
    db.maintain(u64::MAX).unwrap();
    db.checkpoint(1).unwrap();
    db.maintain(u64::MAX).unwrap();
    assert!(!db.owed(), "the cold fill leaves no background work");
    db.attach(&issuer, issuer.depth()).unwrap();
    let worker_path = path.clone();
    db.set_workers(move || Ok(open(&worker_path, false, counts)))
        .unwrap();
    let mut runtime = Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 3,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: SLICE_NS,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: STORE.page_size,
        spin_ns: 0,
        wake_tracking: None,
    })
    .unwrap();
    let ranges = Ranges::start(
        &mut runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 2,
            slice_ns: SLICE_NS,
            spin_ns: 0,
        },
    )
    .unwrap();
    let (done, finished) = mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = ranges.client().unwrap();
            counts.armed.store(true, Ordering::SeqCst);
            let mut out = Vec::new();
            for (key, expected) in &oracle {
                assert!(client.get_async(key, &mut out).await.unwrap());
                assert_eq!(&out, expected);
            }
            counts.armed.store(false, Ordering::SeqCst);
            let io = client.stats_async().await.unwrap()[0].2;
            drop(client);
            ranges.stop_async().await.unwrap();
            done.send(io).unwrap();
        })
        .unwrap();
    let io = finished.recv().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    let resident = counts.resident.load(Ordering::SeqCst);
    assert!(resident > 0, "the gets read pages from the file");
    assert_eq!(counts.elsewhere.load(Ordering::SeqCst), 0);
    assert_eq!(io.resident_reads, resident as u64);
}
