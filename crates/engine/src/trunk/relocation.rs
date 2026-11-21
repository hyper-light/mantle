//! Cleaning's relocation (docs/design/engine-structure.md §3): a copy of one branch replaces it
//! where it stands, every entry kept, read back by gets and in order before and after the store is
//! opened again; the original's extents are released, those it held alone freed at the next
//! checkpoint, and every extent stays counted as often as it is named. A relocation whose branch
//! is gone as it applies is discarded and leaves no extent held; and the cleaner's reserve lets a
//! relocation open where every other task waits for room.
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

/// The node suite's cell's extents: four 4 KiB pages.
const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 4096,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const ROWS: u64 = 200;
const KEYS: u64 = 1_200;

type Rows = BTreeMap<Vec<u8>, Vec<u8>>;
/// A branch's entries in key order: key, operation, value.
type Entries = Vec<(Vec<u8>, Op, Vec<u8>)>;

fn file(path: &std::path::Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

/// Round `round`'s branch: [`ROWS`] rows from a start the round moves, valued by the round.
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

/// A trunk of `rounds` branches, each drained in: leaves settled, inner nodes' bundles holding
/// branches, so it has branches of both lives.
fn trunk(store: &mut Store<DeviceFile>, rows: &mut Rows, rounds: u64) -> Trunk {
    let mut trunk = Trunk::new(TRUNK).unwrap();
    for round in 0..rounds {
        trunk.add(branch(store, rows, round));
        trunk.drain(store).unwrap();
    }
    trunk
}

/// Every row reads back its newest version by a get, and the store holds exactly the extents
/// the trunk, its image and the allocator's map name, each as often as it is named.
fn check(store: &mut Store<DeviceFile>, trunk: &mut Trunk, rows: &Rows) {
    let mut value = Vec::new();
    for (k, v) in rows {
        assert_eq!(trunk.get(store, k, &mut value).unwrap(), Some(Op::Put));
        assert_eq!(&value, v);
    }
    assert_eq!(held(store), named(store, trunk));
}

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

/// Every entry of `b` in key order, by a cursor from its first key.
fn scan(store: &mut Store<DeviceFile>, b: &Branch) -> Entries {
    let mut out = Vec::new();
    let mut c = b.seek(store, b"").unwrap();
    while c.valid() {
        out.push((c.key().to_vec(), c.op(), c.value().to_vec()));
        c.next(b, store).unwrap();
    }
    c.give_back(store);
    out
}

/// The node and branch to move: a leaf's bundle's branch whose extents it shares with no other,
/// so its relocation frees them.
fn alone(trunk: &Trunk) -> (usize, Branch) {
    let mut count: BTreeMap<u64, u32> = BTreeMap::new();
    for b in trunk.branches() {
        for &e in &b.extents {
            *count.entry(e).or_default() += 1;
        }
    }
    trunk
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.leaf)
        .flat_map(|(n, node)| {
            node.pivots
                .iter()
                .flat_map(|p| p.bundle.branches().iter())
                .map(move |b| (n, b))
        })
        .find(|(_, b)| b.extents.iter().any(|e| count[e] == 1))
        .map(|(n, b)| (n, b.clone()))
        .expect("no leaf's branch holds an extent alone")
}

/// The branch in `trunk` with `count` entries and `entries`, other than `not`'s root.
fn copy_of(store: &mut Store<DeviceFile>, trunk: &Trunk, not: u64, entries: &Entries) -> Branch {
    let candidates: Vec<Branch> = trunk
        .branches()
        .into_iter()
        .filter(|b| b.root != not && b.count == entries.len() as u64)
        .collect();
    candidates
        .into_iter()
        .find(|b| scan(store, b) == *entries)
        .expect("no copy holds the branch's entries")
}

#[test]
fn a_relocated_branch_keeps_every_entry_and_its_extents_are_freed_at_the_next_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    let mut rows = Rows::new();
    let mut trunk = trunk(&mut store, &mut rows, 24);
    // A checkpoint names every extent: one released after it is free only at the next.
    let root = trunk.save(&mut store).unwrap();
    store.checkpoint(Some(root), 1).unwrap();
    let (n, b) = alone(&trunk);
    let entries = scan(&mut store, &b);
    assert!(!entries.is_empty());
    let before = held(&store);
    trunk.plan_relocation(n, b.root).unwrap();
    // Its node plans nothing else and takes no other relocation while it is out.
    assert!(trunk.plan_relocation(n, b.root).is_err());
    trunk.drain(&mut store).unwrap();
    let stats = trunk.stats();
    assert_eq!((stats.relocations, stats.relocations_discarded), (1, 0));
    assert!(
        trunk.branches().iter().all(|x| x.root != b.root),
        "the original is still named"
    );
    let copy = copy_of(&mut store, &trunk, b.root, &entries);
    check(&mut store, &mut trunk, &rows);
    // The original's extents are released once each; those it held alone wait for the next
    // checkpoint, which frees them.
    let after = held(&store);
    let copy_extents: BTreeSet<u64> = copy.extents.iter().copied().collect();
    for e in &b.extents {
        let was = before[e];
        let now = after.get(e).copied().unwrap_or(0);
        let gained = u32::from(copy_extents.contains(e));
        assert_eq!(now, was - 1 + gained, "extent {e}");
    }
    let freed: Vec<u64> = b
        .extents
        .iter()
        .copied()
        .filter(|e| before[e] == 1 && !copy_extents.contains(e))
        .collect();
    assert!(!freed.is_empty(), "the relocation freed no extent");
    let root = trunk.save(&mut store).unwrap();
    store.checkpoint(Some(root), 2).unwrap();
    assert_eq!(store.pending_extents(), 0);
    for e in &freed {
        assert!(!held(&store).contains_key(e), "extent {e} not freed");
    }
    check(&mut store, &mut trunk, &rows);
    assert_eq!(scan(&mut store, &copy), entries);
    let (closed, landed) = crate::store::cold_file(store.into_file());
    landed.unwrap();
    drop(closed);

    let (mut store, recovered) = Store::open(file(&path, false), CONFIG).unwrap();
    assert_eq!(recovered.applied, 2);
    let mut trunk = Trunk::load(&mut store, recovered.root.unwrap()).unwrap();
    trunk.check_idle_counts().unwrap();
    check(&mut store, &mut trunk, &rows);
    let copy = copy_of(&mut store, &trunk, b.root, &entries);
    assert_eq!(scan(&mut store, &copy), entries);
}

#[test]
fn a_relocation_whose_branch_is_gone_as_it_applies_leaves_no_trace() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    let mut rows = Rows::new();
    let mut trunk = trunk(&mut store, &mut rows, 24);
    let (n, b) = alone(&trunk);
    let before = held(&store);
    trunk.plan_relocation(n, b.root).unwrap();
    // The copy written in full, then its task names a branch the node no longer holds.
    let (_, seq) = trunk.tasks.next_inline(None).unwrap();
    {
        let mut task = trunk.tasks.get_mut(seq).unwrap();
        let NodeTask {
            node, what, run, ..
        } = &mut *task;
        let Running::Inline(c) = run else {
            panic!("a relocation not inline")
        };
        let inputs = task_inputs(&trunk.nodes[*node], what).unwrap();
        c.step(&mut store, inputs, u64::MAX).unwrap();
        assert!(c.is_done());
        let Running::Inline(c) = std::mem::replace(run, Running::Finished(Vec::new())) else {
            panic!("the relocation's run changed under it")
        };
        *run = Running::Finished((*c).finish(&mut store).unwrap());
        *what = What::Relocate { root: u64::MAX };
    }
    assert_ne!(held(&store), before, "the copy wrote nothing");
    trunk.apply_done(&mut store).unwrap();
    let stats = trunk.stats();
    assert_eq!((stats.relocations, stats.relocations_discarded), (0, 1));
    assert_eq!(trunk.clean_room(), 0);
    assert_eq!(
        held(&store),
        before,
        "the discarded copy left an extent held"
    );
    assert!(trunk.branches().iter().any(|x| x.root == b.root));
    check(&mut store, &mut trunk, &rows);
    trunk.drain(&mut store).unwrap();
    check(&mut store, &mut trunk, &rows);
}

#[test]
fn the_cleaners_reserve_lets_a_relocation_open_where_every_other_task_waits_for_room() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    let mut rows = Rows::new();
    let mut trunk = trunk(&mut store, &mut rows, 24);
    let (n, b) = alone(&trunk);
    let ask = (b.pages_between(b"", None) as u64).div_ceil(u64::from(CONFIG.extent_pages));
    // Branches that owe maintenance, written first: what they take is gone from the store.
    let owed: Vec<Branch> = (24..27).map(|r| branch(&mut store, &mut rows, r)).collect();
    // The checkpoint's room is all the store has free but the copy's: every task the owed
    // branches plan waits for room, and the relocation, the reserve's alone, does not.
    let free = store.capacity();
    trunk.set_checkpoint_room(free - ask - trunk.image_extents_now(&store).unwrap());
    trunk.plan_relocation(n, b.root).unwrap();
    trunk.set_clean_room(ask);
    for b in owed {
        trunk.add(b);
    }
    trunk.step(&mut store, u64::MAX).unwrap();
    let stats = trunk.stats();
    assert_eq!(stats.relocations, 1, "the relocation waited: {stats:?}");
    assert!(trunk.room_waited() > 0, "no other task waited for room");
    trunk.set_checkpoint_room(0);
    trunk.drain(&mut store).unwrap();
    check(&mut store, &mut trunk, &rows);
}
