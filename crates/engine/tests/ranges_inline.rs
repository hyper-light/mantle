//! A client on its range's own shard makes a put or a get inline on the range's lent engine when
//! it needs no wait (docs/design/engine-structure.md, "Same-shard clients"); one that would wait
//! takes the request path, and a client that makes nothing but inline operations still yields
//! its shard.
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

/// A file on disk whose every page read made through the issuer is counted, and that reads from
/// memory what the OS holds when `resident`, or says it holds nothing.
struct Counted {
    file: DeviceFile,
    reads: &'static AtomicUsize,
    resident: bool,
}

impl BlockFile for Counted {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.file.read_exact_at(bytes, offset)
    }
    fn reads_resident(&self) -> bool {
        self.resident && self.file.reads_resident()
    }
    fn read_resident_at(&mut self, bytes: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        if self.resident {
            self.file.read_resident_at(bytes, offset)
        } else {
            Ok(false)
        }
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
            reads: self.reads,
            resident: self.resident,
        })
    }
}

fn open(path: &Path, create: bool, reads: &'static AtomicUsize, resident: bool) -> Counted {
    Counted {
        resident,
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap(),
        reads,
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("a/shared-prefix/object-{i:05}\0").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    vec![(i % 251) as u8; 100]
}

fn runtime() -> Runtime {
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 4,
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
    .unwrap()
}

/// A range over a fresh file whose memtable holds `memtable` bytes, attached to `issuer`, with its
/// workers, started on `runtime`.
fn range(
    runtime: &mut Runtime,
    issuer: &Issuer,
    path: &Path,
    (reads, resident, memtable): (&'static AtomicUsize, bool, usize),
    fill: impl FnOnce(&mut ShardDb<Counted>),
) -> Ranges {
    let mut db =
        ShardDb::create(open(path, true, reads, resident), STORE, memtable, TRUNK).unwrap();
    db.set_cache(0);
    fill(&mut db);
    db.attach(issuer, issuer.depth()).unwrap();
    let worker_path = path.to_path_buf();
    db.set_workers(move || Ok(open(&worker_path, false, reads, resident)))
        .unwrap();
    Ranges::start(
        runtime,
        vec![(Vec::new(), db)],
        RangesConfig {
            clients: 3,
            slice_ns: SLICE_NS,
            spin_ns: 0,
            inline: true,
        },
    )
    .unwrap()
}

/// Do: from a client task on the range's shard, with a memtable that holds every row, put every
/// key, then get every key, then checkpoint; reopen the file cold. Expect: every operation was
/// completed inline (none waits for room or for the device), every answer is the oracle's, and the
/// reopened range holds every key: an inline put lands as one through the request does.
#[test]
fn a_same_shard_client_puts_and_gets_inline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let reads: &'static AtomicUsize = Box::leak(Box::default());
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    let mut runtime = runtime();
    let ranges = range(&mut runtime, &issuer, &path, (reads, true, 1 << 20), |_| {});
    let (done, finished) = mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = ranges.client().unwrap();
            for i in 0..ROWS {
                client.put_async(&key(i), &value(i)).await.unwrap();
            }
            let mut out = Vec::new();
            for i in 0..ROWS {
                assert!(client.get_async(&key(i), &mut out).await.unwrap());
                assert_eq!(out, value(i));
            }
            assert!(!client.get_async(&key(ROWS), &mut out).await.unwrap());
            let inlined = client.inlined();
            client.flush_async().await.unwrap();
            client.checkpoint_async(1).await.unwrap();
            drop(client);
            ranges.stop_async().await.unwrap();
            done.send(inlined).unwrap();
        })
        .unwrap();
    let inlined = finished.recv().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    assert_eq!(inlined, (2 * ROWS + 1) as u64);
    let (mut db, applied) = ShardDb::open(
        open(&path, false, reads, true),
        STORE,
        STORE.page_size,
        TRUNK,
    )
    .unwrap();
    assert_eq!(applied, 1);
    let mut out = Vec::new();
    for i in 0..ROWS {
        assert!(db.get(&key(i), &mut out).unwrap());
        assert_eq!(out, value(i));
    }
}

/// Do: fill the range cold, so its pages are on disk and none in memory a read can take (the
/// file says the OS holds nothing), then get every key from a client on the range's shard.
/// Expect: no get is made inline (each needs a device read), every answer is the oracle's, and
/// the issuer read the pages.
#[test]
fn an_inline_get_that_needs_the_device_takes_the_request_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let reads: &'static AtomicUsize = Box::leak(Box::default());
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    let mut runtime = runtime();
    let mut oracle = BTreeMap::new();
    let ranges = range(
        &mut runtime,
        &issuer,
        &path,
        (reads, false, STORE.page_size),
        |db| {
            for i in 0..ROWS {
                db.put(&key(i), &value(i)).unwrap();
                oracle.insert(key(i), value(i));
            }
            db.flush().unwrap();
            db.maintain(u64::MAX).unwrap();
            db.checkpoint(1).unwrap();
            db.maintain(u64::MAX).unwrap();
            assert!(!db.owed());
        },
    );
    let before = reads.load(Ordering::SeqCst);
    let (done, finished) = mpsc::sync_channel(1);
    runtime
        .spawn_on(runtime.shard_ids()[0], async move {
            let mut client = ranges.client().unwrap();
            let mut out = Vec::new();
            for (key, expected) in &oracle {
                assert!(client.get_async(key, &mut out).await.unwrap());
                assert_eq!(&out, expected);
            }
            let inlined = client.inlined();
            drop(client);
            ranges.stop_async().await.unwrap();
            done.send(inlined).unwrap();
        })
        .unwrap();
    let inlined = finished.recv().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    assert_eq!(inlined, 0);
    assert!(reads.load(Ordering::SeqCst) > before);
}

/// Do: on one shard, a client task that puts every key, then gets them inline in a loop,
/// counting its gets, until a flag is set; and a second task that sets the flag once it sees the
/// count past many turns' worth, yielding to wait for it. Expect: the client stops: a get of a key
/// its memtable holds never waits and never grows memory, so it never takes the request path, and
/// only its turns yield the shard; it yields turn after turn, not once, so the second task sees
/// the count.
#[test]
fn a_loop_of_inline_operations_yields_its_shard() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("range");
    let reads: &'static AtomicUsize = Box::leak(Box::default());
    let flag: &'static AtomicBool = Box::leak(Box::default());
    let count: &'static AtomicUsize = Box::leak(Box::default());
    let issuer = Issuer::start_for(dir.path(), 2, 2).unwrap();
    let mut runtime = runtime();
    // A memtable that holds every key, so no get needs a page.
    let ranges = range(&mut runtime, &issuer, &path, (reads, true, 1 << 20), |_| {});
    let shard = runtime.shard_ids()[0];
    let (done, finished) = mpsc::sync_channel(1);
    runtime
        .spawn_on(shard, async move {
            let mut client = ranges.client().unwrap();
            for i in 0..ROWS {
                client.put_async(&key(i), &value(i)).await.unwrap();
            }
            let mut out = Vec::new();
            let mut gets = 0usize;
            while !flag.load(Ordering::SeqCst) {
                assert!(client.get_async(&key(gets % ROWS), &mut out).await.unwrap());
                gets += 1;
                count.store(gets, Ordering::SeqCst);
            }
            let inlined = client.inlined();
            drop(client);
            ranges.stop_async().await.unwrap();
            done.send((gets, inlined)).unwrap();
        })
        .unwrap();
    runtime
        .spawn_on(shard, async move {
            // Many turns' worth of gets: a client that yielded only its first turn never lets
            // this task see the count.
            while count.load(Ordering::SeqCst) < 16 * ROWS {
                hyper_rt::futures::yield_now().await;
            }
            flag.store(true, Ordering::SeqCst);
        })
        .unwrap();
    let (gets, inlined) = finished.recv().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    assert!(gets >= 16 * ROWS);
    // Every put and every get completed inline: no request ever yielded for the client.
    assert_eq!(inlined, (ROWS + gets) as u64);
}
