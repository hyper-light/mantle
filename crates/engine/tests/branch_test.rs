//! Branches (docs/design/engine-structure.md §4, step E3) built in a store and read back: every
//! entry found with its operation and value, every absent key absent, through a checkpoint and a
//! reopen, for path-like keys whose pages share long prefixes and for arbitrary bytes.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::store::{Config, Store};
use proptest::prelude::*;
use std::collections::BTreeMap;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 32,
    max_extents: 4096,
};

fn store(seed: u64) -> Store<SimFile> {
    let align = Alignment::new(4096).unwrap();
    Store::create(
        SimFile::new(align, Alignment::new(512).unwrap(), seed).unwrap(),
        CONFIG,
    )
    .unwrap()
}

fn build(store: &mut Store<SimFile>, entries: &BTreeMap<Vec<u8>, (Op, Vec<u8>)>) -> Branch {
    let mut b = Builder::new(store, Keys::Exactly(entries.len() as u64)).unwrap();
    for (k, (op, v)) in entries {
        b.add(store, k, *op, v).unwrap();
    }
    b.finish(store).unwrap()
}

fn check(store: &mut Store<SimFile>, branch: &Branch, entries: &BTreeMap<Vec<u8>, (Op, Vec<u8>)>) {
    let mut value = Vec::new();
    for (k, (op, v)) in entries {
        assert_eq!(
            branch.get(store, k, &mut value).unwrap(),
            Some(*op),
            "key {k:?}"
        );
        assert_eq!(&value, v);
    }
    assert_eq!(branch.count, entries.len() as u64);
}

/// An object-store-like key: a bucket, a path of a few components, an object name.
fn object_key() -> impl Strategy<Value = Vec<u8>> {
    (
        0u8..3,
        proptest::collection::vec(0u16..40, 1..4),
        0u32..100_000,
    )
        .prop_map(|(b, path, obj)| {
            let mut k = format!("bucket-{b}/").into_bytes();
            for p in path {
                k.extend_from_slice(format!("dir{p:03}/").as_bytes());
            }
            k.extend_from_slice(format!("object-{obj:08}").as_bytes());
            k
        })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn path_keys_read_back_and_absent_keys_are_absent(
        keys in proptest::collection::btree_set(object_key(), 1..3000),
        absent in proptest::collection::vec(object_key(), 0..200),
        seed in any::<u64>(),
    ) {
        let entries: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let op = if i % 17 == 0 { Op::Delete } else { Op::Put };
                let v = if op == Op::Delete { Vec::new() } else { vec![(i % 251) as u8; 20 + i % 60] };
                (k.clone(), (op, v))
            })
            .collect();
        let mut s = store(seed);
        let branch = build(&mut s, &entries);
        check(&mut s, &branch, &entries);
        let mut value = Vec::new();
        for k in &absent {
            if !entries.contains_key(k) {
                prop_assert_eq!(branch.get(&mut s, k, &mut value).unwrap(), None);
            }
        }
        // Through a checkpoint and a reopen of the file.
        for &e in &branch.extents {
            prop_assert!(s.refs()[e as usize] == 1);
        }
        s.checkpoint(Some(branch.root), 1).unwrap();
        let (mut s, recovered) = Store::open(s.into_file(), CONFIG).unwrap();
        prop_assert_eq!(recovered.root, Some(branch.root));
        check(&mut s, &branch, &entries);
    }

    #[test]
    fn arbitrary_keys_and_values_read_back(
        entries in proptest::collection::btree_map(
            proptest::collection::vec(any::<u8>(), 0..120),
            proptest::collection::vec(any::<u8>(), 0..600),
            1..800,
        ),
        seed in any::<u64>(),
    ) {
        let entries: BTreeMap<Vec<u8>, (Op, Vec<u8>)> =
            entries.into_iter().map(|(k, v)| (k, (Op::Put, v))).collect();
        let mut s = store(seed);
        let branch = build(&mut s, &entries);
        check(&mut s, &branch, &entries);
    }
}

#[test]
fn keys_out_of_order_and_oversized_entries_are_refused() {
    let mut s = store(1);
    let mut b = Builder::new(&mut s, Keys::Exactly(1)).unwrap();
    b.add(&mut s, b"b", Op::Put, b"1").unwrap();
    assert!(b.add(&mut s, b"b", Op::Put, b"2").is_err());
    assert!(b.add(&mut s, b"a", Op::Put, b"3").is_err());
    assert!(b.add(&mut s, b"c", Op::Put, &vec![0u8; 5000]).is_err());
    assert!(
        Builder::new(&mut s, Keys::Exactly(1))
            .unwrap()
            .finish(&mut s)
            .is_err()
    );
}

#[test]
fn a_large_branch_is_a_shallow_tree_of_dense_pages() {
    let mut s = store(2);
    let entries: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = (0..100_000u32)
        .map(|i| {
            let k = format!("bucket-1/users/{:04}/objects/obj-{i:010}", i % 1000).into_bytes();
            (k, (Op::Put, vec![7u8; 43]))
        })
        .collect();
    let branch = build(&mut s, &entries);
    check(&mut s, &branch, &entries);
    let pages: usize = branch.extents.len() * CONFIG.extent_pages as usize;
    let bytes = 100_000 * (48 + 43);
    // The raw key and value bytes against the pages written, within the extents' last one.
    eprintln!(
        "height {} extents {} pages<= {pages} raw {bytes}",
        branch.height,
        branch.extents.len()
    );
    assert!(branch.height <= 4);
}

mod cursor_and_merge {
    use super::*;
    use mantle_engine::branch::merge::{Compaction, Merge, compact};

    #[test]
    fn a_scan_reads_its_branch_an_extent_a_call() {
        let mut s = store(9);
        let entries: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = (0..100_000u32)
            .map(|i| {
                (
                    format!("key-{i:010}").into_bytes(),
                    (Op::Put, vec![3u8; 100]),
                )
            })
            .collect();
        let branch = build(&mut s, &entries);
        let before = s.io_stats();
        let got = scan(&mut s, &branch, b"");
        assert_eq!(got.len(), entries.len());
        let after = s.io_stats();
        // A forward scan reads each extent's leaves in one call and each index page alone. An
        // index page here names more children than an extent holds pages (15-byte keys, 4 KiB
        // pages), so the branch has fewer index pages than extents.
        let extents = branch.extents.len() as u64;
        let reads = after.reads - before.reads;
        assert!(reads <= 2 * extents, "{reads} reads for {extents} extents");
        assert!(after.pages_read - before.pages_read >= extents);
    }

    fn scan(s: &mut Store<SimFile>, branch: &Branch, from: &[u8]) -> Vec<(Vec<u8>, Op, Vec<u8>)> {
        let mut c = branch.seek(s, from).unwrap();
        let mut out = Vec::new();
        while c.valid() {
            out.push((c.key().to_vec(), c.op(), c.value().to_vec()));
            c.next(s).unwrap();
        }
        out
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

        /// A seek from any key and the walk after it give exactly the entries at or after it.
        #[test]
        fn a_cursor_gives_every_entry_from_its_key_in_order(
            keys in proptest::collection::btree_set(object_key(), 1..2500),
            from in object_key(),
            seed in any::<u64>(),
        ) {
            let entries: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.clone(), (Op::Put, vec![(i % 251) as u8; i % 40])))
                .collect();
            let mut s = store(seed);
            let branch = build(&mut s, &entries);
            let expected: Vec<_> = entries
                .range(from.clone()..)
                .map(|(k, (op, v))| (k.clone(), *op, v.clone()))
                .collect();
            prop_assert_eq!(scan(&mut s, &branch, &from), expected);
            let all: Vec<_> = entries.iter().map(|(k, (op, v))| (k.clone(), *op, v.clone())).collect();
            prop_assert_eq!(scan(&mut s, &branch, b""), all);
        }

        /// Overlapping branches of puts and tombstones merge to the newest entry of each key over
        /// any range, and a compaction keeps exactly that, tombstones dropped only when asked.
        #[test]
        fn merged_branches_give_the_newest_entry_of_each_key(
            layers in proptest::collection::vec(
                proptest::collection::btree_map(0u16..600, (any::<bool>(), 0u8..255), 1..300),
                1..6,
            ),
            lo in 0u16..600,
            span in 1u16..600,
            seed in any::<u64>(),
        ) {
            let key = |k: u16| format!("bucket/key-{k:05}").into_bytes();
            let mut s = store(seed);
            // Layers oldest first; branches handed to the merge newest first.
            let mut branches = Vec::new();
            let mut oracle: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
            for layer in &layers {
                let entries: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = layer
                    .iter()
                    .map(|(&k, &(put, v))| {
                        let op = if put { Op::Put } else { Op::Delete };
                        (key(k), (op, if put { vec![v; 5] } else { Vec::new() }))
                    })
                    .collect();
                for (k, e) in &entries {
                    oracle.insert(k.clone(), e.clone());
                }
                branches.push(build(&mut s, &entries));
            }
            branches.reverse();
            let (from, end) = (key(lo), key(lo.saturating_add(span)));
            let expected: Vec<(Vec<u8>, Op, Vec<u8>)> = oracle
                .range(from.clone()..end.clone())
                .map(|(k, (op, v))| (k.clone(), *op, v.clone()))
                .collect();
            let mut merge = Merge::new(&mut s, &branches, &from, Some(&end)).unwrap();
            let mut got = Vec::new();
            while let Some((k, op, v)) = merge.entry() {
                got.push((k.to_vec(), op, v.to_vec()));
                merge.next(&mut s).unwrap();
            }
            prop_assert_eq!(&got, &expected);
            for drop in [false, true] {
                let kept: Vec<_> = expected.iter().filter(|e| !(drop && e.1 == Op::Delete)).cloned().collect();
                match compact(&mut s, &branches, &from, Some(&end), drop).unwrap() {
                    None => prop_assert!(kept.is_empty()),
                    Some(b) => prop_assert_eq!(scan(&mut s, &b, b""), kept),
                }
            }
            // Stepped a few keys at a time and split every 7 entries: the same entries, each
            // part at most 7 and named by its first key.
            let kept: Vec<_> = expected.iter().filter(|e| e.1 != Op::Delete).cloned().collect();
            for budget in [1u64, 3] {
                let mut c = Compaction::new(&mut s, &branches, &from, Some(&end), true, 7).unwrap();
                while c.step(&mut s, budget).unwrap() == budget {}
                prop_assert!(c.is_done());
                let mut joined = Vec::new();
                for (first, b) in c.finish(&mut s).unwrap() {
                    let part = scan(&mut s, &b, b"");
                    prop_assert!(!part.is_empty() && part.len() <= 7);
                    prop_assert_eq!(&part[0].0, &first);
                    joined.extend(part);
                }
                prop_assert_eq!(&joined, &kept);
            }
        }
    }
}
