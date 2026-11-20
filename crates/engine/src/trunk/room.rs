//! Maintenance at the store's extents: a trunk whose tasks wait for room is saved with its
//! maintenance owed (image format 8 and on), and opened again it holds every row and every extent
//! it names, as often as it names it; a compaction that opened its inputs and wrote nothing,
//! dropped for that save, leaves no extent held.
#![cfg(test)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]
use super::*;
use crate::branch::filter::Keys;
use crate::branch::{Builder, Op};
use crate::store::Config;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use std::collections::{BTreeMap, BTreeSet};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 4096,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
/// Rows a branch holds, and the key space the rounds rewrite.
const ROWS: u64 = 200;
const KEYS: u64 = 1_200;

type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

fn file(path: &std::path::Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

/// Round `round`'s branch: [`ROWS`] rows of the key space from a start the round moves, each
/// valued by the round, so a read tells which version it found.
fn branch(store: &mut Store<DeviceFile>, rows: &mut Rows, round: u64) -> Branch {
    let first = round * 337 % (KEYS - ROWS);
    let mut b = Builder::new(store, Keys::Exactly(ROWS)).unwrap();
    for k in first..first + ROWS {
        let key = format!("k{k:06}").into_bytes();
        let value = format!("{round:06}/{k:06}").into_bytes().repeat(6);
        b.add(store, &key, Op::Put, &value).unwrap();
        rows.insert(key, value);
    }
    b.finish(store).unwrap()
}

/// Every row reads back its newest version, and the store holds exactly the extents the
/// trunk, its image and the allocator's map name, each as often as it is named.
fn check(store: &mut Store<DeviceFile>, trunk: &mut Trunk, rows: &Rows) {
    let mut value = Vec::new();
    for (k, v) in rows {
        assert_eq!(trunk.get(store, k, &mut value).unwrap(), Some(Op::Put));
        assert_eq!(&value, v);
    }
    assert_eq!(held(store), named(store, trunk));
}

/// How often each extent is named: by each branch naming it, once by the superblocks, the
/// allocator's map and the trunk's image, none of which shares one.
fn named(store: &Store<DeviceFile>, trunk: &Trunk) -> BTreeMap<u64, u32> {
    let mut named = BTreeMap::from([(0, 1)]);
    for &e in store.map_extents().iter().chain(trunk.image_extents()) {
        assert!(named.insert(e, 1).is_none(), "an extent named twice");
    }
    let sole: BTreeSet<u64> = named.keys().copied().collect();
    for b in trunk.branches() {
        for &e in &b.extents {
            assert!(
                !sole.contains(&e),
                "a branch names a map's or an image's extent"
            );
            *named.entry(e).or_default() += 1;
        }
    }
    named
}

fn held(store: &Store<DeviceFile>) -> BTreeMap<u64, u32> {
    store
        .refs()
        .iter()
        .enumerate()
        .filter(|(_, c)| **c > 0)
        .map(|(e, &c)| (e as u64, c))
        .collect()
}

/// A trunk of `rounds` branches drained, then `owed` more added while the next checkpoint's
/// room is more than the store holds, so every task waits for room and none begins.
fn owing(store: &mut Store<DeviceFile>, rows: &mut Rows, rounds: u64, owed: u64) -> Trunk {
    let mut trunk = Trunk::new(TRUNK).unwrap();
    for round in 0..rounds {
        trunk.add(branch(store, rows, round));
        trunk.drain(store).unwrap();
    }
    trunk.set_checkpoint_room(CONFIG.max_extents);
    for round in rounds..rounds + owed {
        trunk.add(branch(store, rows, round));
        trunk.step(store, u64::MAX).unwrap();
    }
    trunk
}

#[test]
fn a_trunk_saved_with_its_maintenance_owed_opens_with_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    let mut rows = Rows::new();
    let mut trunk = owing(&mut store, &mut rows, 24, 6);
    assert!(trunk.stalled_for_room(&store), "no task waits for room");
    trunk.drop_unstarted(&mut store).unwrap();
    assert!(trunk.saveable());
    let owed = !trunk.pending.is_empty() || trunk.inflight_nodes > 0;
    assert!(owed, "the trunk owes no maintenance to save");
    let root = trunk.save(&mut store).unwrap();
    store.checkpoint(Some(root), 1).unwrap();
    check(&mut store, &mut trunk, &rows);
    let (closed, landed) = crate::store::cold_file(store.into_file());
    landed.unwrap();
    drop(closed);

    let (mut store, recovered) = Store::open(file(&path, false), CONFIG).unwrap();
    assert_eq!(recovered.applied, 1);
    let mut trunk = Trunk::load(&mut store, recovered.root.unwrap()).unwrap();
    trunk.check_idle_counts().unwrap();
    assert!(!trunk.is_idle(), "the image lost the maintenance owed");
    check(&mut store, &mut trunk, &rows);
    // The owed maintenance, planned anew, runs to its end over the rows the image named.
    trunk.drain(&mut store).unwrap();
    assert!(trunk.is_idle());
    let root = trunk.save(&mut store).unwrap();
    store.checkpoint(Some(root), 2).unwrap();
    check(&mut store, &mut trunk, &rows);
}

#[test]
fn a_compaction_dropped_after_opening_its_inputs_holds_no_extent_or_read_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    let mut rows = Rows::new();
    let mut trunk = owing(&mut store, &mut rows, 24, 0);
    let lent = store.lent();
    let refs = store.refs().to_vec();
    let free = store.capacity();
    // The next checkpoint's room more than the store holds: a compaction planned over two
    // inputs or more waits for room before it opens any.
    trunk.set_checkpoint_room(CONFIG.max_extents);
    let mut at = None;
    for rounds in (24..64).step_by(3) {
        for round in rounds..rounds + 3 {
            trunk.add(branch(&mut store, &mut rows, round));
        }
        trunk.step(&mut store, u64::MAX).unwrap();
        at = trunk.tasks.iter().position(|t| {
            matches!(&t.run, Running::Inline(c) if c.opening() && !c.holds_inputs())
                && task_inputs(&trunk.nodes[t.node], &t.what).unwrap().count() > 1
        });
        if at.is_some() {
            break;
        }
    }
    let at = at.expect("no compaction of two inputs or more waits for room");
    let added = store.refs().to_vec();
    // One unit of its step, as the shard's would be given before the room ran short: its
    // first input opened, its opening not done, nothing written.
    let Trunk { nodes, tasks, .. } = &mut trunk;
    let NodeTask {
        node, what, run, ..
    } = &mut tasks[at];
    let Running::Inline(c) = run else {
        panic!("the task found runs inline")
    };
    c.step(&mut store, task_inputs(&nodes[*node], what).unwrap(), 1)
        .unwrap();
    assert!(c.holds_inputs() && c.opening());
    assert_ne!(store.lent(), lent, "the opened input lent no buffer");
    trunk.step(&mut store, u64::MAX).unwrap();
    assert!(
        trunk.stalled_for_room(&store),
        "the compaction did not wait for room"
    );
    trunk.drop_unstarted(&mut store).unwrap();
    assert!(trunk.tasks.is_empty());
    assert_eq!(store.lent(), lent, "the dropped opening kept read buffers");
    assert_eq!(
        store.refs(),
        added.as_slice(),
        "the dropped compaction held extents"
    );
    assert!(!store.io_outstanding());
    assert!(refs.len() <= added.len() && free >= store.capacity());
    check(&mut store, &mut trunk, &rows);
    // Planned anew with room, it runs to its end.
    trunk.set_checkpoint_room(0);
    trunk.drain(&mut store).unwrap();
    assert!(trunk.is_idle());
    check(&mut store, &mut trunk, &rows);
}
