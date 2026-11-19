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

/// The store of the node suite's cells (crates/mantle/tests/support/cell.rs): pages and memtables
/// of 4 KiB, extents of four pages, 256 extents.
const CELL: Config = Config {
    max_extents: 256,
    ..CONFIG
};
/// The rows of an image the cell's store holds through its install on the shard alone, with
/// room to spare: 180 of its 256 extents at the most. With its worker, before each job's grant
/// waited for room, the install fenced at the store's limit.
const CELL_IMAGE: u64 = 6_000;

/// A follower's engine in a store of `config`, opened as the node opens it (`process::engine`)
/// with `workers` workers on a shard that may wait, applies `APPLIED` entries with a checkpoint
/// every `CHECKPOINT`, then installs a leader's snapshot as `RangeMachine::install` does: every
/// row cleared and the image's put, then persisted. The install's and persist's result, with
/// the engine's own error where it fenced (`Stored::fault`), and the store's path in a directory
/// kept until the result is read.
fn install_on_a_shard(
    image: Vec<(Vec<u8>, Vec<u8>)>,
    config: Config,
    workers: usize,
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
                    Stored::create(counted(&at, true, &counts), config, config.page_size, TRUNK)
                        .unwrap();
                if workers > 0 {
                    let (again, worker_counts) = (at.clone(), Arc::clone(&counts));
                    e.set_workers(move || Ok(counted(&again, false, &worker_counts)), workers)
                        .unwrap();
                }
                for i in 0..APPLIED {
                    let (k, v) = row(i);
                    e.apply(i + 1, vec![Write::Put(k, v)]).unwrap();
                    if (i + 1) % CHECKPOINT == 0 {
                        e.persist().unwrap();
                    }
                }
                let persisted = e
                    .install(INDEX, image)
                    .and_then(|()| e.persist())
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
    image_of(IMAGE)
}

/// An image of `rows` rows, keyed as the entries are, each value of the image's own.
fn image_of(rows: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..rows)
        .map(|i| {
            let (k, _) = row(i);
            (k, vec![2; 120])
        })
        .collect()
}

/// The extents the store at `path` grew to: its file's length in extents of `config`, since the
/// store grows its file by its writes and reuses a freed extent before it grows.
fn extents_of(path: &Path, config: Config) -> u64 {
    let extent = u64::try_from(config.page_size).unwrap() * u64::from(config.extent_pages);
    std::fs::metadata(path).unwrap().len().div_ceil(extent)
}

/// A snapshot installed over applied entries on a shard with its worker persists: the persist
/// that a node's follower failed with "the engine failed to persist a snapshot" once its engines
/// had workers (CI run 44527895012) returns durable at the image's index.
#[test]
fn an_image_installed_on_a_shard_with_workers_persists() {
    let (persisted, _dir, _) = install_on_a_shard(image(), CONFIG, ENGINE_WORKERS);
    persisted.unwrap();
}

/// The store opened again after the install stands at the image's index and holds the image's
/// rows, every one, and none of the entries' own values.
#[test]
fn a_store_opened_after_an_install_holds_every_row_of_the_image() {
    let (persisted, _dir, path) = install_on_a_shard(image(), CONFIG, ENGINE_WORKERS);
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

/// An install the cell's store holds on the shard alone holds with its worker too: the node
/// suite's followers fenced installing a snapshot, "LimitExceeded { what: \"extents of a
/// store\", limit: 256 }", once their engines had workers. Each job's grant, sized to what it
/// reads, went out whatever the store held, and stayed whole until its pass applied (ten
/// compactions holding 130 extents for the few they wrote); and packings cut at the root's
/// pivots grew its bundles past every bound while their flush waited (194 branches of an extent
/// each). Here the shard alone grows the store to `alone` extents; with its worker the same
/// install persists, and the store opened again stands at the image's index with every row.
#[test]
fn an_install_the_store_holds_on_the_shard_alone_it_holds_with_a_worker() {
    let (persisted, _alone_dir, path) = install_on_a_shard(image_of(CELL_IMAGE), CELL, 0);
    persisted.unwrap();
    let alone = extents_of(&path, CELL);
    let (persisted, _dir, path) = install_on_a_shard(image_of(CELL_IMAGE), CELL, ENGINE_WORKERS);
    if let Err(error) = persisted {
        panic!(
            "with its worker the install fenced where the shard alone took {alone} of {} \
             extents: {error}",
            CELL.max_extents
        );
    }
    std::thread::spawn(move || {
        mantle_engine::store::permit_blocking();
        let writes = Arc::new(Writes {
            shard: std::thread::current().id(),
            on_shard: AtomicU64::new(0),
            elsewhere: AtomicU64::new(0),
        });
        shard()
            .block_on(async move {
                let mut e =
                    Stored::open(counted(&path, false, &writes), CELL, CELL.page_size, TRUNK)
                        .unwrap();
                assert_eq!((e.applied(), e.durable()), (INDEX, INDEX));
                assert_eq!(e.image().unwrap(), image_of(CELL_IMAGE));
            })
            .unwrap();
    })
    .join()
    .unwrap();
}

/// The rows of a PUT burst a metadata range's engine takes: 40,000, within the 20,000 to 60,000
/// a cell's three ranges take for 20,000 small PUTs.
const BURST: u64 = 40_000;

/// Row `i` of the burst in the order many clients' PUTs interleave: a Name range's version key
/// and record, sized as [`row`]'s, the keys spread over the range by a stride prime to the burst.
fn burst_row(i: u64) -> (Vec<u8>, Vec<u8>) {
    row(i * 7_919 % BURST)
}

/// What the gets and one-row seeks of every row cost through the trunk, each a thousandth of a
/// source a row, and the trunk's shape: the branches its pivots' bundles and in-flight lists
/// hold, the most one bundle holds, and the in-flight bundles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadCost {
    jobs: u64,
    get_sources: u64,
    get_probes: u64,
    get_reads: u64,
    seek_sources: u64,
    seek_opened: u64,
    height: usize,
    branches: u64,
    most: u64,
    inflight: u64,
    pivots: u64,
    parted: u64,
    whole: u64,
    parted_entries: u64,
    flushes: u64,
    pivot_compactions: u64,
    leaf_compactions: u64,
    whole_consolidations: u64,
    consolidations: u64,
    entries_written: u64,
}

/// The trunk's maintenance jobs applied: its compactions, flushes and splits.
fn jobs(t: &mantle_engine::trunk::TrunkStats) -> u64 {
    t.pivot_compactions + t.leaf_compactions + t.flushes + t.splits
}

/// Reads every row of the burst back, a get and a one-row seek as a range's lookups make, and
/// counts what they cost a row through the trunk, in thousandths.
fn read_cost<F: BlockFile + 'static>(db: &mut mantle_engine::shard_db::ShardDb<F>) -> ReadCost {
    let before = db.stats().1;
    let mut value = Vec::new();
    let mut rows = mantle_engine::rows::Rows::new();
    for i in 0..BURST {
        let (k, v) = row(i);
        assert!(db.get(&k, &mut value).unwrap(), "row {i}");
        assert_eq!(value, v);
        let mut end = k.clone();
        end.push(0);
        rows.clear();
        db.scan_rows(&k, Some(&end), 1, &mut rows).unwrap();
        assert_eq!(rows.len(), 1);
    }
    let after = db.stats().1;
    let per = |a: u64, b: u64| (a - b) * 1_000 / BURST;
    let levels = after.shape;
    ReadCost {
        jobs: jobs(&after),
        get_sources: per(after.get_sources, before.get_sources),
        get_probes: per(after.get_probes, before.get_probes),
        get_reads: per(after.get_reads, before.get_reads),
        seek_sources: per(after.scan_sources, before.scan_sources),
        seek_opened: per(after.scan_opened, before.scan_opened),
        height: db.shape().unwrap().0,
        branches: levels.iter().map(|l| l.branches).sum(),
        most: levels.iter().map(|l| l.most).max().unwrap_or(0),
        inflight: levels.iter().map(|l| l.inflight).sum(),
        pivots: levels.iter().map(|l| l.pivots).sum(),
        parted: after.packings_parted,
        whole: after.packings_inflight,
        parted_entries: after.parted_entries,
        flushes: after.flushes,
        pivot_compactions: after.pivot_compactions,
        leaf_compactions: after.leaf_compactions,
        whole_consolidations: after.whole_consolidations,
        consolidations: after.consolidations,
        entries_written: after.entries_written,
    }
}

/// Passes of idle time and reads after the burst's maintenance is done: a quiet trunk's reads pay
/// for its consolidation (mantle-engine `Trunk::charge_get`), and the passes show what it settles
/// to. Three: the engine's own runs of a quiet trunk read and left idle settled it in two to four
/// passes (`shard_db::read_rent`, 2026-11-19).
const SETTLE_PASSES: usize = 3;

/// A range's engine under the node's derived configuration (`derive::engine` of a cell's rules on
/// a device of 4 KiB writes), opened as `process::engine` opens it, with `workers` maintenance
/// workers or none, takes the burst; then its read costs, at once and after each doubling of the
/// maintenance jobs applied since, until none is owed, and after each of [`SETTLE_PASSES`] passes
/// of idle time once its reads have charged the quiet trunk.
fn burst_costs(workers: usize) -> Vec<ReadCost> {
    std::thread::spawn(move || {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        mantle_engine::store::permit_blocking();
        let (_, rules) = mantle_node::derive::cell(3).unwrap();
        let engine = mantle_node::derive::engine(4096, 1 << 30, &rules).unwrap();
        shard()
            .block_on(async move {
                let open = |create: bool| {
                    DeviceFile::open(
                        &path,
                        create,
                        CachingRequest::Buffered,
                        Alignment::new(4096).unwrap(),
                    )
                };
                let mut db = mantle_engine::shard_db::ShardDb::create(
                    open(true).unwrap(),
                    engine.store,
                    engine.memtable_bytes,
                    engine.trunk,
                )
                .unwrap();
                if workers > 0 {
                    let again = path.clone();
                    db.set_workers_within(
                        move || {
                            DeviceFile::open(
                                &again,
                                false,
                                CachingRequest::Buffered,
                                Alignment::new(4096).unwrap(),
                            )
                            .map_err(|e| mantle_engine::Error::Io {
                                op: "open",
                                detail: e.to_string(),
                            })
                        },
                        workers,
                    )
                    .unwrap();
                }
                for i in 0..BURST {
                    let (k, v) = burst_row(i);
                    db.put(&k, &v).unwrap();
                }
                let start = jobs(&db.stats().1);
                let mut costs = vec![read_cost(&mut db)];
                let mut next = 1;
                // Each idle step applies at most a pass's jobs; the trunk's queue is bounded by
                // its nodes, so the steps end.
                for _ in 0..1_000_000 {
                    if !db.owed() {
                        break;
                    }
                    db.idle_step(engine.trunk.leaf_entries).unwrap();
                    if jobs(&db.stats().1) - start >= next {
                        costs.push(read_cost(&mut db));
                        next *= 2;
                    }
                }
                assert!(!db.owed(), "maintenance never caught up");
                costs.push(read_cost(&mut db));
                // Read once idle, the trunk is owed the consolidation its reads paid for: idle
                // time spends it, and the reads after list what the layout settles to.
                for _ in 0..SETTLE_PASSES {
                    for _ in 0..1_000_000 {
                        if !db.owed() {
                            break;
                        }
                        db.idle_step(engine.trunk.leaf_entries).unwrap();
                    }
                    assert!(!db.owed(), "maintenance never caught up");
                    costs.push(read_cost(&mut db));
                }
                costs
            })
            .unwrap()
    })
    .join()
    .unwrap()
}

/// A get or a seek through a range's engine, with its worker or inline, lists no more sources than
/// the trunk's bound for its height while maintenance is busy (`TrunkConfig::sources_most`), and,
/// once none is owed, holds no in-flight bundle and no pivot bundle past `fanout + 1` branches
/// (docs/design/engine-structure.md, Flush). The counts are printed, worker beside inline: how far
/// the worker's idle layout differs from inline's depends on its pace (engine-structure.md;
/// the commit that added this test), so no run's comparison is asserted.
#[test]
fn reads_with_a_worker_or_inline_stay_within_the_trunks_bounds() {
    let (_, rules) = mantle_node::derive::cell(3).unwrap();
    let engine = mantle_node::derive::engine(4096, 1 << 30, &rules).unwrap();
    eprintln!("engine {engine:?}");
    let inline = burst_costs(0);
    let worker = burst_costs(ENGINE_WORKERS);
    for (mode, costs) in [("inline", &inline), ("worker", &worker)] {
        for c in costs {
            eprintln!("{mode} {c:?}");
            let most = engine.trunk.sources_most(c.height) as u64 * 1_000;
            assert!(c.get_sources <= most, "{mode} {c:?} past {most}");
            assert!(c.seek_sources <= most, "{mode} {c:?} past {most}");
        }
        let idle = costs.last().unwrap();
        assert_eq!(idle.inflight, 0, "{mode} {idle:?}");
        assert!(
            idle.most <= engine.trunk.fanout as u64 + 1,
            "{mode} {idle:?}"
        );
    }
}
