//! A range's engine in the node suite's cell's store (crates/mantle/tests/support/cell.rs's
//! `STATED`: 256 extents of four 4 KiB pages, 4 KiB memtables, fanout 3, 96 entries a leaf, one
//! worker), driven as a node drives it (`Stored::apply_now`, `Stored::upkeep`,
//! `Stored::persist_now`): each entry taken once the memtable has room (`ShardDb::room_now`), the
//! node's idle steps (`ShardDb::idle_step_now`) while it waits, and a checkpoint made without
//! waiting every `CHECKPOINT` entries and whenever the engine waits for extents only a checkpoint
//! frees. Maintenance never repeats a pass that changes nothing and waits on nothing
//! (`Trunk::still`): it makes progress, or states it waits for the checkpoint, or that it is at
//! its store's bound, and the node stops stepping it.
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
use std::collections::BTreeMap;
use std::path::Path;

const PAGE: usize = 4096;
const CONFIG: Config = Config {
    page_size: PAGE,
    extent_pages: 4,
    max_extents: 256,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const MEM: usize = 4096;
/// A node's turn's units of maintenance at most (`upkeep::step`'s budget).
const UNITS: u64 = 64;
/// The entries a checkpoint is asked every, as a range's log compaction asks one.
const CHECKPOINT: u64 = 500;
/// Passes in a row that change nothing and wait on nothing: one is a look that found nothing
/// to do; a second without an event between is the spin this module's test closes.
const STILL_MOST: u64 = 2;
/// The turns one entry may wait for room: the drive's loop bound (CLAUDE.md §2), the node
/// suite's fill's (crates/node/tests/engine_workers.rs `TURNS_MOST`).
const TURNS_MOST: u64 = 1_000_000;

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap()
}

/// The engine at `path` with `workers` workers.
fn engine(path: &Path, workers: usize) -> ShardDb<DeviceFile> {
    let mut db = ShardDb::create(open(path, true), CONFIG, MEM, TRUNK).unwrap();
    if workers == 0 {
        return db;
    }
    let again = path.to_path_buf();
    db.set_workers_within(
        move || {
            DeviceFile::open(
                &again,
                false,
                CachingRequest::Buffered,
                Alignment::new(PAGE).unwrap(),
            )
            .map_err(|e| Error::Io {
                op: "open a worker's file",
                detail: e.to_string(),
            })
        },
        workers,
        "",
    )
    .unwrap();
    db
}

/// What the drive ends at: every entry taken, or the engine at its store's extent bound.
#[derive(Debug, PartialEq, Eq)]
enum End {
    Taken,
    Bound(u64),
}

/// Takes the rows `row(i)` makes for entries `1..=entries` as a node's drive applies them,
/// checking after every step that maintenance repeats no pass that changes nothing; returns
/// how it ended and the rows acknowledged.
fn drive(
    db: &mut ShardDb<DeviceFile>,
    entries: u64,
    row: impl Fn(u64) -> (Vec<u8>, Vec<u8>),
) -> (End, BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut acknowledged = BTreeMap::new();
    // A checkpoint begun without waiting and not yet durable, at its index.
    let mut out: Option<u64> = None;
    let mut applied = 0u64;
    let still = |db: &ShardDb<DeviceFile>, at: &str| {
        assert!(
            db.trunk.still < STILL_MOST,
            "{at}: {} passes in a row changed nothing and waited on nothing; stalled for room {}, \
             at its bound {}, idle work owed {}: {}",
            db.trunk.still,
            db.trunk.stalled_for_room(&db.store),
            db.at_extent_bound(),
            db.idle_owed(),
            db.trunk.still_state(&db.store),
        );
    };
    // `Stored::persist_now`: one out is asked again until it completes.
    let persist = |db: &mut ShardDb<DeviceFile>, out: &mut Option<u64>, applied: u64| {
        let at = match *out {
            Some(_) if db.checkpointing().is_none() => {
                *out = None;
                return;
            }
            Some(at) => at,
            None => applied,
        };
        *out = if db.checkpoint_now(at).unwrap() {
            None
        } else {
            Some(at)
        };
    };
    for i in 1..=entries {
        let (k, v) = row(i);
        let bytes = crate::memtable::hashed::entry_bytes(k.len(), v.len()).unwrap();
        let mut turns = 0u64;
        while !db.room_now(bytes, 1).unwrap() {
            still(db, &format!("entry {i}, waiting"));
            // The node's turns: its idle steps while one does work and work stays owed. One
            // that waits on a job out has the worker's message wake the node's task
            // (`Pool::wake_on_message`): here, with no checkpoint out, the idle step that waits
            // for it.
            for _ in 0..UNITS {
                let (done, owed) = db.idle_step_now(UNITS).unwrap();
                still(db, &format!("entry {i}, its idle step"));
                if owed && done == 0 && db.trunk.workers_out() && out.is_none() {
                    db.idle_step(UNITS).unwrap();
                    still(db, &format!("entry {i}, its wait"));
                }
                if done == 0 || !owed {
                    break;
                }
            }
            if out.is_some() || db.wants_checkpoint() {
                persist(db, &mut out, applied);
            }
            if out.is_none() && db.at_extent_bound() {
                return (End::Bound(i), acknowledged);
            }
            turns += 1;
            assert!(
                turns < TURNS_MOST,
                "entry {i} neither found room nor met the store's extent bound: {}",
                db.trunk.still_state(&db.store),
            );
        }
        db.put(&k, &v).unwrap();
        db.release_loaded();
        applied = i;
        acknowledged.insert(k, v);
        let (_, _) = db.idle_step_now(UNITS).unwrap();
        still(db, &format!("entry {i}, its turn"));
        if out.is_some() || i % CHECKPOINT == 0 || db.wants_checkpoint() {
            persist(db, &mut out, applied);
        }
    }
    (End::Taken, acknowledged)
}

/// The cell's objects' rows, as a layer's range takes them: names past every earlier one in the
/// order the cell puts them (`o1`, `o2`, ...), each a row of its own, so the store fills with
/// rows that live, and each put a value of the cell's lengths' sizes.
fn object_row(i: u64) -> (Vec<u8>, Vec<u8>) {
    let lens = [16usize, 24, 96, 160, 200];
    let len = lens[usize::try_from(i).unwrap() % lens.len()];
    (
        format!("b/o{i}").into_bytes(),
        vec![u8::try_from(i % 251).unwrap(); len],
    )
}

/// The node suite's engine fill (crates/node/tests/engine_workers.rs `fill_row`): 3,000 keys
/// rewritten over and over, each value naming its entry.
fn fill_row(i: u64) -> (Vec<u8>, Vec<u8>) {
    let k = i * 7_919 % 3_000;
    (
        format!("bucket-{}/obj-{k:06}", k % 5).into_bytes(),
        i.to_le_bytes().repeat(15),
    )
}

fn reads_back(db: &mut ShardDb<DeviceFile>, acknowledged: &BTreeMap<Vec<u8>, Vec<u8>>) {
    for (k, v) in acknowledged {
        let mut value = Vec::new();
        assert!(db.get(k, &mut value).unwrap(), "{k:?} lost");
        assert_eq!(&value, v);
    }
}

/// Takes `entries` of `row` on an engine with `workers` workers, then checkpoints it and reads
/// every acknowledged row back; how the drive ended.
fn fill(workers: usize, entries: u64, row: impl Fn(u64) -> (Vec<u8>, Vec<u8>)) -> End {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut db = engine(&path, workers);
    let (end, acknowledged) = drive(&mut db, entries, row);
    let applied = match end {
        End::Taken => entries,
        End::Bound(at) => at - 1,
    };
    db.checkpoint(applied).unwrap();
    reads_back(&mut db, &acknowledged);
    end
}

/// The shard alone, its passes its own and none a worker's: the node suite's fill met maintenance
/// waiting for the checkpoint (`Trunk::stalled_for_room`) at entry 10,449 while seeks had paid
/// for a consolidation, and the idle step stepped the stalled trunk, reported a unit done and
/// was asked again, each pass changing nothing. The idle step now leaves a stalled trunk, and
/// its consolidation, for the checkpoint, which frees the room.
#[test]
fn the_shard_alone_never_repeats_a_pass_that_changes_nothing_as_its_store_fills() {
    assert_eq!(fill(0, 24_000, fill_row), End::Taken);
}

/// The same with the cell's worker, its jobs' timing the run's own.
#[test]
fn an_engine_with_its_worker_never_repeats_a_pass_that_changes_nothing_as_its_store_fills() {
    assert_eq!(fill(1, 24_000, fill_row), End::Taken);
}

/// Rows that all live fill the store to its extent bound: the engine states it
/// (`ShardDb::at_extent_bound`), every acknowledged row reads back, and no pass on the way
/// repeated one that changed nothing.
#[test]
fn a_store_filled_with_live_rows_states_its_bound_and_never_repeats_a_pass_that_changes_nothing() {
    for workers in [0, 1] {
        assert!(matches!(fill(workers, 60_000, object_row), End::Bound(_)));
    }
}
