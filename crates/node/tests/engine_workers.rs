//! A range's engine opened as the node opens it (`process::engine`): on a shard whose owner states
//! it may wait on the device (`mantle_engine::store::permit_blocking`), with its maintenance workers
//! (`Stored::set_workers`, `derive::ENGINE_WORKERS`). Its applies hand every packing and compaction
//! to the workers, so the shard's thread writes no page while it applies: under small PUTs those
//! writes, inline, were a quarter of the node's busiest shard (node.md §1.2). Without workers the
//! same applies write their pages on the shard. A checkpoint still returns only once durable: the
//! store opened again holds every row through it.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;
use mantle_meta::engine::{Engine, Rows, Write};
use mantle_node::derive::ENGINE_WORKERS;
use mantle_range::Stored;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::ThreadId;

/// The engine's own small test configuration (crates/engine/tests/permit_blocking.rs): memtables
/// of a page, so a range's rows rotate them every few dozen and the trunk compacts.
const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 4096,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const ROWS: u64 = 1_100;
/// A checkpoint every so many entries, as a range's compaction asks one when the log's room runs
/// short: the applies between rotate the memtable many times and the trunk compacts.
const CHECKPOINT: u64 = 250;

/// Writes counted by where they ran: on the thread that opened the engine, the shard's, or on
/// any other, its workers'.
struct Writes {
    shard: ThreadId,
    on_shard: AtomicU64,
    elsewhere: AtomicU64,
}

/// The store's file, counting each write by its thread.
struct Counted {
    file: DeviceFile,
    writes: Arc<Writes>,
}

impl BlockFile for Counted {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        let at = if std::thread::current().id() == self.writes.shard {
            &self.writes.on_shard
        } else {
            &self.writes.elsewhere
        };
        at.fetch_add(1, Ordering::SeqCst);
        self.file.write_all_at(buf, offset)
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }
}

fn counted(path: &Path, create: bool, writes: &Arc<Writes>) -> Counted {
    Counted {
        file: DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        writes: Arc::clone(writes),
    }
}

/// A shard of one task, as the engine's own test of a shard that may wait sizes one.
fn shard() -> LocalRuntime {
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 2,
        interests_per_shard: interests_for(1),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

/// A row as a Name range's are sized: a version's key and its record.
fn row(i: u64) -> (Vec<u8>, Vec<u8>) {
    (
        format!("v/b/k{i:06}/{}", "0".repeat(24)).into_bytes(),
        vec![7; 120],
    )
}

/// Applies `ROWS` entries of a row each, a checkpoint every `CHECKPOINT` and at the end, on a
/// shard that may wait, with `workers` maintenance workers or none: the writes the shard
/// made while applying and in all, the workers' writes, and the trunk's compactions and those that
/// held their inputs on the shard at once.
fn run(workers: usize) -> (u64, u64, u64, u64, u64) {
    std::thread::spawn(move || {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        mantle_engine::store::permit_blocking();
        let writes = Arc::new(Writes {
            shard: std::thread::current().id(),
            on_shard: AtomicU64::new(0),
            elsewhere: AtomicU64::new(0),
        });
        let (at, counts) = (path.clone(), Arc::clone(&writes));
        let (applying, compactions, inline) = shard()
            .block_on(async move {
                let mut e =
                    Stored::create(counted(&at, true, &counts), CONFIG, CONFIG.page_size, TRUNK)
                        .unwrap();
                if workers > 0 {
                    let (again, worker_counts) = (at.clone(), Arc::clone(&counts));
                    e.set_workers(move || Ok(counted(&again, false, &worker_counts)), workers)
                        .unwrap();
                }
                let mut applying = 0;
                for i in 0..ROWS {
                    let (k, v) = row(i);
                    let before = counts.on_shard.load(Ordering::SeqCst);
                    e.apply(i + 1, vec![Write::Put(k, v)]).unwrap();
                    applying += counts.on_shard.load(Ordering::SeqCst) - before;
                    if (i + 1) % CHECKPOINT == 0 {
                        e.persist().unwrap();
                        assert_eq!(e.durable(), i + 1);
                    }
                }
                e.persist().unwrap();
                let trunk = e.trunk_stats();
                (
                    applying,
                    trunk.pivot_compactions + trunk.leaf_compactions,
                    trunk.open_most,
                )
            })
            .unwrap();
        // Opened again, the store stands at its last checkpoint and holds every row.
        let reopened = Arc::clone(&writes);
        shard()
            .block_on(async move {
                let mut e = Stored::open(
                    counted(&path, false, &reopened),
                    CONFIG,
                    CONFIG.page_size,
                    TRUNK,
                )
                .unwrap();
                assert_eq!(e.durable(), ROWS);
                for i in 0..ROWS {
                    let (k, v) = row(i);
                    assert_eq!(e.get(&k).unwrap(), Some(v), "row {i}");
                }
            })
            .unwrap();
        (
            applying,
            writes.on_shard.load(Ordering::SeqCst),
            writes.elsewhere.load(Ordering::SeqCst),
            compactions,
            inline,
        )
    })
    .join()
    .unwrap()
}

#[test]
fn an_engine_with_workers_writes_no_page_on_its_shard_while_it_applies() {
    let (applying, _, workers_wrote, compactions, inline) = run(ENGINE_WORKERS);
    assert!(compactions > 0, "the trunk compacted nothing");
    assert_eq!(inline, 0, "a compaction held its inputs on the shard");
    assert_eq!(applying, 0, "the shard wrote {applying} pages applying");
    assert!(workers_wrote > 0, "the workers wrote nothing");
    // Inline, the same applies write their packings' and compactions' pages on the shard.
    let (applying, _, elsewhere, compactions, _) = run(0);
    assert!(compactions > 0, "the trunk compacted nothing inline");
    assert!(applying > 0, "the shard wrote no page applying inline");
    assert_eq!(elsewhere, 0);
}

/// The entries a follower applied before a snapshot, the rows of the image it installs, and the
/// image's index: the clear and the puts rotate the page-sized memtable over a hundred times.
const APPLIED: u64 = 1_100;
const IMAGE: u64 = 2_000;
const INDEX: u64 = APPLIED + 500;

/// A follower's engine, opened as the node opens it (`process::engine`) with its worker on a
/// shard that may wait, applies `APPLIED` entries with a checkpoint every `CHECKPOINT`, then
/// installs a leader's snapshot as `RangeMachine::install` does: every row cleared and the
/// image's put, then persisted. The persist's result, with the engine's own error where it
/// fenced (`Stored::fault`), and the store's path in a directory kept until the result is read.
fn install_on_a_shard(
    image: Vec<(Vec<u8>, Vec<u8>)>,
) -> (Result<(), String>, tempfile::TempDir, std::path::PathBuf) {
    std::thread::spawn(move || {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        mantle_engine::store::permit_blocking();
        let writes = Arc::new(Writes {
            shard: std::thread::current().id(),
            on_shard: AtomicU64::new(0),
            elsewhere: AtomicU64::new(0),
        });
        let (at, counts) = (path.clone(), Arc::clone(&writes));
        let persisted = shard()
            .block_on(async move {
                let mut e =
                    Stored::create(counted(&at, true, &counts), CONFIG, CONFIG.page_size, TRUNK)
                        .unwrap();
                let (again, worker_counts) = (at.clone(), Arc::clone(&counts));
                e.set_workers(
                    move || Ok(counted(&again, false, &worker_counts)),
                    ENGINE_WORKERS,
                )
                .unwrap();
                for i in 0..APPLIED {
                    let (k, v) = row(i);
                    e.apply(i + 1, vec![Write::Put(k, v)]).unwrap();
                    if (i + 1) % CHECKPOINT == 0 {
                        e.persist().unwrap();
                    }
                }
                e.install(INDEX, image).unwrap();
                let persisted = e
                    .persist()
                    .map_err(|error| format!("{error}: the engine's {:?}", e.fault()));
                if persisted.is_ok() {
                    assert_eq!((e.applied(), e.durable()), (INDEX, INDEX));
                }
                persisted
            })
            .unwrap();
        (persisted, dir, path)
    })
    .join()
    .unwrap()
}

/// The image's rows: the entries' keys and as many more, each value of the image's own.
fn image() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..IMAGE)
        .map(|i| {
            let (k, _) = row(i);
            (k, vec![2; 120])
        })
        .collect()
}

/// A snapshot installed over applied entries on a shard with its worker persists: the persist
/// that a node's follower failed with "the engine failed to persist a snapshot" once its engines
/// had workers (CI run 44527895012) returns durable at the image's index.
#[test]
fn an_image_installed_on_a_shard_with_workers_persists() {
    let (persisted, _dir, _) = install_on_a_shard(image());
    persisted.unwrap();
}

/// The store opened again after the install stands at the image's index and holds the image's
/// rows, every one, and none of the entries' own values.
#[test]
fn a_store_opened_after_an_install_holds_every_row_of_the_image() {
    let (persisted, _dir, path) = install_on_a_shard(image());
    persisted.unwrap();
    std::thread::spawn(move || {
        mantle_engine::store::permit_blocking();
        let writes = Arc::new(Writes {
            shard: std::thread::current().id(),
            on_shard: AtomicU64::new(0),
            elsewhere: AtomicU64::new(0),
        });
        shard()
            .block_on(async move {
                let mut e = Stored::open(
                    counted(&path, false, &writes),
                    CONFIG,
                    CONFIG.page_size,
                    TRUNK,
                )
                .unwrap();
                assert_eq!((e.applied(), e.durable()), (INDEX, INDEX));
                assert_eq!(e.image().unwrap(), image());
            })
            .unwrap();
    })
    .join()
    .unwrap();
}
