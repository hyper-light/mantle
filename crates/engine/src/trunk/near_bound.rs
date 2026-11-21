//! A store near its bound (docs/design/engine-structure.md §3, near the bound): once the extents
//! the store may still hand out beside the checkpoint's room are fewer than the pages above the
//! leaves fill, the index pivots flush their bundles as soon as they hold a branch, until those
//! pages fit that room; far from its bound the trunk keeps SplinterDB's flush past the fanout.
//! Every row reads back, and the store holds exactly the extents the trunk names.
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
use std::collections::BTreeMap;

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

fn file(path: &std::path::Path) -> DeviceFile {
    DeviceFile::open(
        path,
        true,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

/// Round `round`'s branch: [`ROWS`] rows from a start the round moves, valued by the round, so
/// later rounds supersede earlier rounds' versions.
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

/// Every row reads back its newest version, and the store holds each extent exactly as often as
/// the trunk's branches, its image and the allocator's map name it.
fn check(store: &mut Store<DeviceFile>, trunk: &mut Trunk, rows: &Rows) {
    let mut value = Vec::new();
    for (k, v) in rows {
        assert_eq!(trunk.get(store, k, &mut value).unwrap(), Some(Op::Put));
        assert_eq!(&value, v);
    }
    let mut named: BTreeMap<u64, u32> = BTreeMap::from([(0, 1)]);
    for &e in store.map_extents().iter().chain(trunk.image_extents()) {
        *named.entry(e).or_default() += 1;
    }
    for b in trunk.branches() {
        for &e in &b.extents {
            *named.entry(e).or_default() += 1;
        }
    }
    let held: BTreeMap<u64, u32> = store
        .refs()
        .iter()
        .enumerate()
        .filter(|(_, c)| **c > 0)
        .map(|(e, &c)| (e as u64, c))
        .collect();
    assert_eq!(held, named);
    trunk.check_live().unwrap();
}

#[test]
fn a_store_near_its_bound_flushes_its_index_pivots_until_the_pages_above_the_leaves_fit_its_room() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(file(&dir.path().join("store")), CONFIG).unwrap();
    let per = u64::from(CONFIG.extent_pages);
    let mut rows = Rows::new();
    let mut trunk = Trunk::new(TRUNK).unwrap();
    for round in 0..24 {
        trunk.add(branch(&mut store, &mut rows, round));
        trunk.drain(&mut store).unwrap();
    }
    // Far from its bound the index pivots keep up to `fanout` branches each.
    let above = trunk.above_leaves_pages();
    assert!(above > 0, "the index pivots hold no bundle to flush");
    assert!(trunk.room_free(&store) * per >= trunk.live_total);
    assert_eq!(trunk.stats().near_bound_flushes, 0);
    assert_eq!(trunk.stats().near_bound_looks, 0);
    check(&mut store, &mut trunk, &rows);

    // Other data takes the store's extents until fewer are free than those pages fill, and the
    // next rounds come in.
    let mut taken = Vec::new();
    while trunk.room_free(&store) >= above.div_ceil(per) {
        taken.push(store.allocate_extent().unwrap());
    }
    for round in 24..28 {
        trunk.add(branch(&mut store, &mut rows, round));
        trunk.drain(&mut store).unwrap();
    }
    let stats = trunk.stats();
    eprintln!(
        "{above} pages above the leaves before, {} after; {} flushes near the bound, {} extents \
         free",
        trunk.above_leaves_pages(),
        stats.near_bound_flushes,
        trunk.room_free(&store)
    );
    assert!(stats.near_bound_looks > 0);
    assert!(stats.near_bound_flushes > 0);
    let left = trunk.above_leaves_pages();
    assert!(
        left < above,
        "{left} pages above the leaves, {above} before"
    );
    assert!(
        left.div_ceil(per) <= trunk.room_free(&store),
        "{left} pages above the leaves with {} extents free",
        trunk.room_free(&store)
    );
    for e in taken {
        store.release(e).unwrap();
    }
    check(&mut store, &mut trunk, &rows);
}
