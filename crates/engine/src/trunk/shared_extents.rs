//! Small branches share extents (docs/design/engine-structure.md §3): a branch begins in the
//! pages the branch built before it in the same store left in its last extent. The parts of a
//! packing, written one after another, so take the extents their pages fill, not one each; each
//! extent is freed when the last branch naming it goes, in whatever order they go; and a
//! checkpoint opened again counts each extent's references exactly.
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

/// The node suite's cell's store: extents of four 4 KiB pages.
const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 4096,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
/// Each part's rows: parts of one page and of several, so parts begin at every page of an
/// extent and some run on over an extent's end.
const PARTS: [u64; 9] = [3, 40, 1, 25, 7, 60, 2, 2, 90];

fn file(path: &std::path::Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

fn row(part: usize, k: u64) -> (Vec<u8>, Vec<u8>) {
    (
        format!("p{part:03}/k{k:06}").into_bytes(),
        format!("{part:03}/{k:06}").into_bytes().repeat(4),
    )
}

/// Part `part` of a packing: `rows` rows of its own keys, built in `store` after the parts before
/// it, as a packing's worker builds its parts.
fn part(store: &mut Store<DeviceFile>, part: usize, rows: u64) -> Branch {
    let mut b = Builder::new(store, Keys::Exactly(rows)).unwrap();
    for k in 0..rows {
        let (key, value) = row(part, k);
        b.add(store, &key, Op::Put, &value).unwrap();
    }
    b.finish(store).unwrap()
}

fn parts(store: &mut Store<DeviceFile>, rows: &[u64]) -> Vec<Branch> {
    rows.iter()
        .enumerate()
        .map(|(i, &n)| part(store, i, n))
        .collect()
}

/// The pages a branch wrote: its tree's, then its filter's and index's.
fn pages(b: &Branch) -> u64 {
    b.filter_start + u64::from(b.filter_pages)
}

/// How often `branches` name each extent.
fn named<'b>(branches: impl Iterator<Item = &'b Branch>) -> BTreeMap<u64, u32> {
    let mut named = BTreeMap::new();
    for b in branches {
        for &e in &b.extents {
            *named.entry(e).or_default() += 1;
        }
    }
    named
}

/// The store's count of each extent it holds but the superblocks', the allocator map's and the
/// trunk image's.
fn held(store: &Store<DeviceFile>, besides: &[u64]) -> BTreeMap<u64, u32> {
    store
        .refs()
        .iter()
        .enumerate()
        .skip(1)
        .filter(|&(e, &c)| c > 0 && !besides.contains(&(e as u64)))
        .map(|(e, &c)| (e as u64, c))
        .collect()
}

/// Every row of each part `(i, branch, rows)` reads back, by a get and by a scan in page order
/// from its first key.
fn reads_back(store: &mut Store<DeviceFile>, parts: &[(usize, &Branch, u64)]) {
    let mut value = Vec::new();
    for &(i, b, n) in parts {
        for k in 0..n {
            let (key, v) = row(i, k);
            assert_eq!(b.get(store, &key, &mut value).unwrap(), Some(Op::Put));
            assert_eq!(value, v, "part {i} row {k}");
        }
        let mut c = b.seek(store, &row(i, 0).0).unwrap();
        let mut seen = 0;
        while c.valid() {
            assert_eq!(c.key(), row(i, seen).0.as_slice(), "part {i} scanned");
            seen += 1;
            c.next(b, store).unwrap();
        }
        assert_eq!(seen, n, "part {i} scanned short");
        c.give_back(store);
    }
}

#[test]
fn the_parts_of_a_packing_take_the_extents_their_pages_fill_not_one_each() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(file(&dir.path().join("store"), true), CONFIG).unwrap();
    let parts = parts(&mut store, &PARTS);
    let per = u64::from(CONFIG.extent_pages);
    let total: u64 = parts.iter().map(pages).sum();
    let extents = named(parts.iter());
    assert_eq!(
        extents.len() as u64,
        total.div_ceil(per),
        "{} parts of {total} pages",
        parts.len()
    );
    assert!(
        (extents.len() as u64) < parts.len() as u64,
        "the parts took an extent each"
    );
    // Each part began where the one before it ended.
    let mut at = 0u64;
    for b in &parts {
        assert_eq!(u64::from(b.first), at % per);
        at += pages(b);
    }
    let map = store.map_extents().to_vec();
    assert_eq!(
        held(&store, &map),
        extents,
        "an extent counted other than its parts"
    );
    let each: Vec<_> = parts
        .iter()
        .zip(PARTS)
        .enumerate()
        .map(|(i, (b, n))| (i, b, n))
        .collect();
    reads_back(&mut store, &each);
}

#[test]
fn each_shared_extent_is_freed_when_its_last_part_goes_in_any_order() {
    let rows = [3u64, 20, 2, 9];
    let ways: usize = (1..=rows.len()).product();
    for way in 0..ways {
        // The order, `way` read as a factorial number.
        let mut left: Vec<usize> = (0..rows.len()).collect();
        let mut order = Vec::new();
        let mut x = way;
        for m in (1..=rows.len()).rev() {
            order.push(left.remove(x % m));
            x /= m;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::create(file(&dir.path().join("store"), true), CONFIG).unwrap();
        let built = parts(&mut store, &rows);
        assert!(
            named(built.iter()).values().any(|&n| n > 1),
            "no extent shared"
        );
        let map = store.map_extents().to_vec();
        let mut live: Vec<usize> = (0..rows.len()).collect();
        for &p in &order {
            for &e in &built[p].extents {
                store.release(e).unwrap();
            }
            live.retain(|&q| q != p);
            assert_eq!(
                held(&store, &map),
                named(live.iter().map(|&q| &built[q])),
                "order {order:?}: after part {p}"
            );
            let each: Vec<_> = live.iter().map(|&q| (q, &built[q], rows[q])).collect();
            reads_back(&mut store, &each);
        }
        // The last extent written was freed with its last part: the next branch begins a fresh
        // extent rather than pages of one handed out again.
        let next = part(&mut store, 0, 1);
        assert_eq!(next.first, 0, "order {order:?}");
    }
}

#[test]
fn a_checkpoint_opened_again_counts_each_shared_extent_as_often_as_it_is_named() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    let mut trunk = Trunk::new(TRUNK).unwrap();
    let built = parts(&mut store, &PARTS);
    let expected = named(built.iter());
    assert!(expected.values().any(|&n| n > 1), "no extent shared");
    for b in built.iter().cloned() {
        trunk.add(b);
    }
    let root = trunk.save(&mut store).unwrap();
    store.checkpoint(Some(root), 1).unwrap();
    let (closed, landed) = crate::store::cold_file(store.into_file());
    landed.unwrap();
    drop(closed);

    let (mut store, recovered) = Store::open(file(&path, false), CONFIG).unwrap();
    assert_eq!(recovered.applied, 1);
    let mut trunk = Trunk::load(&mut store, recovered.root.unwrap()).unwrap();
    let besides: Vec<u64> = store
        .map_extents()
        .iter()
        .chain(trunk.image_extents())
        .copied()
        .collect();
    assert_eq!(named(trunk.branches().iter()), expected);
    assert_eq!(held(&store, &besides), expected);
    let mut value = Vec::new();
    for (i, &n) in PARTS.iter().enumerate() {
        for k in 0..n {
            let (key, v) = row(i, k);
            assert_eq!(
                trunk.get(&mut store, &key, &mut value).unwrap(),
                Some(Op::Put)
            );
            assert_eq!(value, v);
        }
    }
    // Compacted and checkpointed again, the parts' extents go as their last reader does, and the
    // store holds what the trunk names, each as often.
    trunk.drain(&mut store).unwrap();
    let root = trunk.save(&mut store).unwrap();
    store.checkpoint(Some(root), 2).unwrap();
    let besides: Vec<u64> = store
        .map_extents()
        .iter()
        .chain(trunk.image_extents())
        .copied()
        .collect();
    assert_eq!(held(&store, &besides), named(trunk.branches().iter()));
}
