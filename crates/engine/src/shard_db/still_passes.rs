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
    drive_rows(db, entries, |i| {
        let (k, v) = row(i);
        vec![(k, Some(v))]
    })
}

/// [`drive`] for entries that each write `rows(i)`: a value put, or none for a deletion.
fn drive_rows(
    db: &mut ShardDb<DeviceFile>,
    entries: u64,
    rows: impl Fn(u64) -> Vec<(Vec<u8>, Option<Vec<u8>>)>,
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
        let made = rows(i);
        let bytes = made
            .iter()
            .map(|(k, v)| {
                crate::memtable::hashed::entry_bytes(k.len(), v.as_ref().map_or(0, Vec::len))
                    .unwrap()
            })
            .sum();
        let keys = u64::try_from(made.len()).unwrap();
        let mut turns = 0u64;
        while !db.room_now(bytes, keys).unwrap() {
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
        for (k, v) in made {
            match v {
                Some(v) => {
                    db.put(&k, &v).unwrap();
                    acknowledged.insert(k, v);
                }
                None => {
                    db.delete(&k).unwrap();
                    acknowledged.remove(&k);
                }
            }
        }
        db.release_loaded();
        applied = i;
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

/// The cell's block range's entries, as its range applies them (crates/meta/src/block.rs): a
/// block's write (its row, keyed by its identity, which spreads the keys; a reverse row for each
/// of its two chunks, keyed by volume first; and its row in the queue of unsettled blocks, keyed
/// by its deadline), two renewals that rewrite its row, and the sweep's settling of a block
/// written eight before, its queue row deleted. The write is about the 699 bytes the cell's
/// refused entry took (`room_now`'s ask at its bound, 2026-11-21).
fn block_entry(i: u64) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let n = i / 4;
    let id = |n: u64| n.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17);
    let block = vec![u8::try_from(i % 251).unwrap(); 450];
    match i % 4 {
        0 => vec![
            (format!("b/{:016x}", id(n)).into_bytes(), Some(block)),
            (
                format!("r/{}/{:016x}/0", n % 3, id(n)).into_bytes(),
                Some(vec![1; 40]),
            ),
            (
                format!("r/{}/{:016x}/1", (n + 1) % 3, id(n)).into_bytes(),
                Some(vec![2; 40]),
            ),
            (format!("q/{n:020}").into_bytes(), Some(vec![3; 16])),
        ],
        1 | 2 => vec![(format!("b/{:016x}", id(n)).into_bytes(), Some(block))],
        _ => match n.checked_sub(8) {
            Some(old) => vec![(format!("q/{old:020}").into_bytes(), None)],
            None => vec![(format!("b/{:016x}", id(n)).into_bytes(), Some(block))],
        },
    }
}

/// The cell's block range met its store's bound at about 1,786 entries with 70 tasks owed, each
/// asking more than the two extents the store had beside the checkpoint's room (the node suite's
/// sustained-writes cell, 2026-11-21). Driven with the block layer's entries, the store's bound
/// was stated with work owed that no pass had tried in the room it had: the tasks a checkpoint
/// dropped, planned anew after the step that would have tried them, and every task asking three
/// extents where it wrote one. Now maintenance near the bound is room-sized: a step fits while
/// the room of the least owed task is free, the tasks too large for the room left are planned
/// anew room-sized once a room, and the engine states its bound only once every owed task has
/// been tried and none fits (`ShardDb::at_extent_bound`). The drive, bounded by its turns, never
/// repeats a pass that changes nothing, and every acknowledged row reads back.
#[test]
fn the_cells_block_range_steps_room_sized_and_states_its_bound_only_once_no_step_fits() {
    for workers in [0, 1] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut db = engine(&path, workers);
        let (end, acknowledged) = drive_rows(&mut db, 40_000, block_entry);
        let End::Bound(at) = end else {
            panic!("{workers} workers: the cell's store took every entry");
        };
        // At the bound every owed task was tried, and none fits the room the store has.
        assert!(
            db.trunk.is_idle() || db.trunk.stalled_for_room(&db.store),
            "{workers} workers: the bound at entry {at} stated with owed work untried: {}",
            db.trunk.still_state(&db.store)
        );
        assert_eq!(
            db.trunk.fitting_task(&db.store),
            None,
            "{workers} workers: the bound at entry {at} stated with a task that fits: {}",
            db.trunk.still_state(&db.store),
        );
        let stats = db.trunk.stats();
        eprintln!(
            "{workers} workers: the bound at entry {at}, {} extents free; {} live pages in {} \
             extents; {} compactions room-sized, {} leaves compacted for a settle that did not \
             fit, {} replans",
            db.store.capacity(),
            stats.live_pages,
            stats.live_extents,
            stats.room_sized,
            stats.room_sized_leaves,
            stats.room_replans
        );
        assert!(
            stats.room_replans > 0 && stats.room_sized_leaves > 0,
            "{workers} workers: no room-sized step on the way to the bound: {stats:?}"
        );
        db.checkpoint(at - 1).unwrap();
        reads_back(&mut db, &acknowledged);
    }
}
