//! Gets through a quiet trunk pay for its consolidation, as seeks do. A burst leaves the trunk
//! quiet with pivot bundles of several branches, and only seeks' rent, or writes once the memtable
//! empties, ever settled it: a range serving GETs listed those sources on every get from then on
//! (mantle-node's tests/engine_workers.rs: a loaded worker's quiet trunk 5.0-10.4 sources a get
//! against inline's 2.6-2.8). A worker's merge here measured about three times the shard's a key
//! (29-32 us against 9.3 us, debug build, 2026-11-19), so seeks' rent bought its pivots about
//! three times later. The trunk's hook ([`Trunk::hold_dispatch`]) holds the worker behind through
//! the burst: a step that does not wait hands out no job, so jobs go out only when a put waits
//! for room. The gets' rent for the sources past the one a settled path keeps pays for
//! consolidating the pivots they passed, spent in idle time.
#![cfg(test)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use super::*;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use std::path::Path;

/// The node's derived engine for a cell's rules on a device of 4 KiB writes
/// (`mantle_node::derive::engine(4096, 1 << 30, ..)`): the page a Name row's bounds need, a
/// memtable of one row at the bounds, SplinterDB's fanout and a leaf of a memtable's rows.
const PAGE: usize = 24_576;
const MEM: usize = 20_840;
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 8,
    leaf_entries: 1_894,
};
const ROWS: u64 = 20_000;

fn config() -> Config {
    Config {
        page_size: PAGE,
        extent_pages: 2,
        max_extents: 1 << 14,
    }
}

/// Row `i`, sized as a Name range's version key and record.
fn row(i: u64) -> (Vec<u8>, Vec<u8>) {
    (
        format!("v/b/k{i:06}/{}", "0".repeat(24)).into_bytes(),
        vec![7; 120],
    )
}

/// Row `i` of the burst, in the order many clients' PUTs interleave: spread over the range by a
/// stride prime to it.
fn burst_row(i: u64) -> (Vec<u8>, Vec<u8>) {
    row(i * 7_919 % ROWS)
}

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap()
}

/// How the trunk's maintenance runs through the burst: on the shard, or on a worker held behind
/// ([`Trunk::hold_dispatch`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Upkeep {
    Inline,
    Behind,
}

/// A shard that took the burst under `upkeep` and then ran idle until it owed nothing.
fn burst(dir: &Path, upkeep: Upkeep) -> ShardDb<DeviceFile> {
    let path = dir.join("store");
    let mut db = ShardDb::create(open(&path, true), config(), MEM, TRUNK).unwrap();
    if upkeep == Upkeep::Behind {
        db.set_workers_within(
            move || {
                DeviceFile::open(
                    &path,
                    false,
                    CachingRequest::Buffered,
                    Alignment::new(4096).unwrap(),
                )
                .map_err(|e| Error::Io {
                    op: "open a worker's file",
                    detail: e.to_string(),
                })
            },
            1,
            "",
        )
        .unwrap();
        db.trunk.hold_dispatch = true;
    }
    for i in 0..ROWS {
        let (k, v) = burst_row(i);
        db.put(&k, &v).unwrap();
    }
    db.trunk.hold_dispatch = false;
    idle(&mut db);
    db
}

/// Idle steps until nothing is owed: each applies at most a pass's jobs, and the trunk's queue is
/// bounded by its nodes, so the steps end.
fn idle(db: &mut ShardDb<DeviceFile>) {
    for _ in 0..1_000_000 {
        if !db.owed() {
            return;
        }
        db.idle_step(TRUNK.leaf_entries).unwrap();
    }
    panic!("maintenance never caught up");
}

/// What every row's get cost through the trunk: the sources its path listed, in thousandths a
/// get the trunk answered.
fn read(db: &mut ShardDb<DeviceFile>) -> u64 {
    let before = db.stats().1;
    let mut value = Vec::new();
    for i in 0..ROWS {
        let (k, v) = row(i);
        assert!(db.get(&k, &mut value).unwrap(), "row {i}");
        assert_eq!(value, v);
    }
    let after = db.stats().1;
    let gets = after.gets - before.gets;
    assert!(gets > 0, "no get reached the trunk");
    (after.get_sources - before.get_sources) * 1_000 / gets
}

/// The trunk's branches, and the most one pivot's bundle holds.
fn shape(db: &ShardDb<DeviceFile>) -> (u64, u64) {
    let levels = db.stats().1.shape;
    (
        levels.iter().map(|l| l.branches).sum(),
        levels.iter().map(|l| l.most).max().unwrap_or(0),
    )
}

/// Read passes over every row, each followed by idle time, until a pass's gets each list one
/// source, the settled path, or [`PASSES`] passes: the sources a get listed each pass. Rent and
/// price are both measured, so how many passes pay for a pivot follows the machine.
fn settle(db: &mut ShardDb<DeviceFile>, upkeep: Upkeep) -> Vec<u64> {
    let mut listed = Vec::new();
    for _ in 0..PASSES {
        let sources = read(db);
        listed.push(sources);
        if sources == 1_000 {
            break;
        }
        let before = db.stats().1;
        idle(db);
        let after = db.stats().1;
        eprintln!(
            "{upkeep:?} read {sources} sources a get (thousandths); then {:?} branches, most, \
             {} consolidations, {} entries written",
            shape(db),
            after.consolidations_started - before.consolidations_started,
            after.entries_written - before.entries_written,
        );
    }
    listed
}

/// Read passes a test allows a trunk to settle in: its pivots were paid in at most four on a
/// loaded host in a debug build, a worker's measured merge three times the shard's (2026-11-19,
/// `reads_settle_a_quiet_trunk_a_worker_left_behind`).
const PASSES: usize = 8;

/// With its worker held behind through the burst, the trunk falls quiet listing at least twice
/// the sources a get its settled layout lists (4.0 against 1.0 in every run on 2026-11-19); its
/// gets' rent pays for its consolidation, and once read and idle it lists within a quarter source
/// of what inline maintenance's trunk settles to under the same reads.
#[test]
fn reads_settle_a_quiet_trunk_a_worker_left_behind() {
    let mut figures = Vec::new();
    for upkeep in [Upkeep::Inline, Upkeep::Behind] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = burst(dir.path(), upkeep);
        let t = db.stats().1;
        eprintln!(
            "{upkeep:?} quiet: {:?} branches, most; {} pivot compactions, {} flushes",
            shape(&db),
            t.pivot_compactions,
            t.flushes,
        );
        let listed = settle(&mut db, upkeep);
        figures.push((upkeep, listed));
    }
    let (_, inline) = &figures[0];
    let (_, behind) = &figures[1];
    let (first, settled) = (behind[0], *behind.last().unwrap());
    assert!(
        first >= 2 * settled,
        "behind: the quiet trunk's first reads list {first}, its settled {settled}: no excess"
    );
    let inline_settled = *inline.last().unwrap();
    assert!(
        settled <= inline_settled + 250,
        "behind settles to {settled} a get, inline to {inline_settled}"
    );
}

/// A quiet trunk nobody reads is not rewritten: its gets' rent is what pays for its
/// consolidation, and with none charged, idle steps find nothing owed and write nothing.
#[test]
fn a_quiet_trunk_nobody_reads_is_not_rewritten() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = burst(dir.path(), Upkeep::Behind);
    let before = db.stats().1;
    assert!(shape(&db).0 > 8, "the burst left a trunk to settle");
    for _ in 0..1_000 {
        assert!(!db.owed());
        assert_eq!(db.idle_step(TRUNK.leaf_entries).unwrap(), 0);
    }
    let after = db.stats().1;
    assert_eq!(after.entries_written, before.entries_written);
    assert_eq!(after.consolidations_started, before.consolidations_started);
    // Reads pay: the same trunk, read, is owed its consolidation.
    assert!(
        (0..PASSES).any(|_| {
            read(&mut db);
            db.owed()
        }),
        "the reads paid for nothing"
    );
}
