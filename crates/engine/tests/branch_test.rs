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

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Fault, SimFile};
use mantle_engine::Error;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op, tree_pages_most};
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
        let (mut s, recovered) = Store::open(finished_file(s.into_file()).0, CONFIG).unwrap();
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

    /// A packing's cache loan holds every tree page its pieces write: the bound counts what a
    /// memtable holds (its entries, their key and value bytes, its longest key and value) and the
    /// pieces its cuts make, and no packing of those entries writes more.
    #[test]
    fn a_packings_tree_pages_stay_within_its_cache_loan(
        entries in proptest::collection::btree_map(
            proptest::collection::vec(any::<u8>(), 0..120),
            proptest::collection::vec(any::<u8>(), 0..600),
            1..3000,
        ),
        cuts in proptest::collection::vec(any::<u16>(), 0..8),
        seed in any::<u64>(),
    ) {
        let list: Vec<(&Vec<u8>, &Vec<u8>)> = entries.iter().collect();
        let at = cuts.iter().map(|&c| usize::from(c) % list.len()).collect();
        let mut s = store(seed);
        let (pages, most) = pack(&mut s, &list, at);
        prop_assert!(pages <= most, "{} tree pages past the bound {}", pages, most);
    }
}

/// `list` packed as a memtable is, cut at the entry positions `at` (as the root's pivot keys fall
/// in it), one branch a range: the tree pages written, and the bound a packing's loan is sized by,
/// given the entries' key and value bytes with no head, the least an arena holds them in.
fn pack(store: &mut Store<SimFile>, list: &[(&Vec<u8>, &Vec<u8>)], mut at: Vec<usize>) -> (usize, usize) {
    at.sort_unstable();
    at.dedup();
    let mut pages = 0usize;
    let mut begun = 0usize;
    for end in at.iter().copied().chain(std::iter::once(list.len())) {
        if end <= begun {
            continue;
        }
        let piece = &list[begun..end];
        let mut b = Builder::new(store, Keys::Exactly(piece.len() as u64)).unwrap();
        for (k, v) in piece {
            b.add(store, k, Op::Put, v).unwrap();
        }
        pages += b.finish(store).unwrap().counts.len();
        begun = end;
    }
    let bytes = list.iter().map(|(k, v)| k.len() + v.len()).sum();
    let key = list.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    let value = list.iter().map(|(_, v)| v.len()).max().unwrap_or(0);
    let most = tree_pages_most(list.len(), bytes, key, value, at.len() + 1, store.page_capacity());
    (pages, most)
}

/// The bench's shape, 16-byte keys and 100-byte values cut in nine: the bound holds, and how far
/// past the pages written it lends is printed.
#[test]
fn a_bench_shaped_packing_stays_within_its_cache_loan() {
    let entries: BTreeMap<Vec<u8>, Vec<u8>> = (0..200_000u64)
        .map(|i| {
            let k = i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes();
            ([k, k].concat(), vec![(i % 251) as u8; 100])
        })
        .collect();
    let list: Vec<(&Vec<u8>, &Vec<u8>)> = entries.iter().collect();
    let at = (1..9).map(|i| i * list.len() / 9).collect();
    let mut s = store(5);
    let (pages, most) = pack(&mut s, &list, at);
    eprintln!("tree pages {pages} bound {most}");
    assert!(pages <= most, "{pages} tree pages past the bound {most}");
}

#[test]
fn keys_out_of_order_and_oversized_entries_are_refused() {
    let mut s = store(1);
    let mut b = Builder::new(&mut s, Keys::Exactly(5)).unwrap();
    b.add(&mut s, b"b", Op::Put, b"1").unwrap();
    assert!(matches!(
        b.add(&mut s, b"b", Op::Put, b"2"),
        Err(Error::Corruption { .. })
    ));
    assert!(matches!(
        b.add(&mut s, b"a", Op::Put, b"3"),
        Err(Error::Corruption { .. })
    ));
    for op in [Op::Put, Op::Delete] {
        assert!(matches!(
            b.add(&mut s, b"c", op, &vec![0u8; 5000]),
            Err(Error::LimitExceeded { .. })
        ));
    }
    let too_long = vec![b'z'; s.page_capacity() + 1];
    for op in [Op::Put, Op::Delete] {
        assert!(matches!(
            b.add(&mut s, &too_long, op, &[]),
            Err(Error::LimitExceeded { .. })
        ));
    }
    assert!(
        Builder::new(&mut s, Keys::Exactly(1))
            .unwrap()
            .finish(&mut s)
            .is_err()
    );

    // A refused entry leaves the builder usable; empty values and tombstones survive a
    // page crossing and durable reopen with the larger accepted values.
    let value = vec![0xff; s.page_capacity() / 2];
    b.add(&mut s, b"c", Op::Put, &value).unwrap();
    b.add(&mut s, b"d", Op::Put, &[]).unwrap();
    b.add(&mut s, b"e", Op::Delete, &[]).unwrap();
    b.add(&mut s, b"f", Op::Put, &value).unwrap();
    let branch = b.finish(&mut s).unwrap();
    let entries = BTreeMap::from([
        (b"b".to_vec(), (Op::Put, b"1".to_vec())),
        (b"c".to_vec(), (Op::Put, value.clone())),
        (b"d".to_vec(), (Op::Put, Vec::new())),
        (b"e".to_vec(), (Op::Delete, Vec::new())),
        (b"f".to_vec(), (Op::Put, value)),
    ]);
    check(&mut s, &branch, &entries);
    s.checkpoint(Some(branch.root), 1).unwrap();
    let (file, finished) = finished_file(s.into_file());
    finished.unwrap();
    let (mut s, recovered) = Store::open(file, CONFIG).unwrap();
    assert_eq!(recovered.root, Some(branch.root));
    check(&mut s, &branch, &entries);
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
            c.next(branch, s).unwrap();
        }
        c.give_back(s);
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
                merge.next(&mut s, &branches).unwrap();
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
                while c.step(&mut s, &branches, budget).unwrap() == budget {}
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

/// The pages a branch holds in a key range bound what a compaction of that range writes from
/// it: a loan of the cache's memory sized by them holds every page the job writes.
#[test]
fn a_compaction_of_a_range_writes_no_more_pages_than_its_input_holds_there() {
    let mut s = store(17);
    let entries: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = (0..5_000u32)
        .map(|k| {
            (
                format!("key-{k:08}").into_bytes(),
                (Op::Put, vec![b'v'; (k % 120) as usize]),
            )
        })
        .collect();
    let branch = build(&mut s, &entries);
    assert!(branch.counts.len() > 50, "{}", branch.counts.len());
    let whole = branch.pages_between(b"", None);
    assert_eq!(
        whole,
        branch.counts.len() + usize::from(branch.height) - 1 + branch.filter_pages as usize + 1
    );
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..40 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let a = x % 5_000;
        let b = a + 1 + (x >> 32) % (5_000 - a);
        let from = format!("key-{a:08}").into_bytes();
        let end = format!("key-{b:08}").into_bytes();
        let bound = branch.pages_between(&from, Some(&end));
        let inputs = [branch.clone()];
        let mut c = mantle_engine::branch::merge::Compaction::new(
            &mut s,
            &inputs,
            &from,
            Some(&end),
            false,
            u64::MAX,
        )
        .unwrap();
        c.step(&mut s, &inputs, u64::MAX).unwrap();
        let parts = c.finish(&mut s).unwrap();
        let written: usize = parts
            .iter()
            .map(|(_, b)| b.counts.len() + b.filter_pages as usize)
            .sum();
        let shape: Vec<(usize, u32, u8, usize)> = parts
            .iter()
            .map(|(_, b)| {
                (
                    b.counts.iter().filter(|&&n| n > 0).count(),
                    b.filter_pages,
                    b.height,
                    b.counts.iter().filter(|&&n| n == 0).count(),
                )
            })
            .collect();
        let first = branch.leaf_of(&from).unwrap();
        let last = branch.leaf_of(&end).unwrap();
        assert!(
            written <= bound,
            "[{a}, {b}): wrote {written} of a bound {bound}: parts (leaves, trailer, height, index) {shape:?}; input pages {first}..={last}, height {}, trailer {}, total {}",
            branch.height,
            branch.filter_pages,
            branch.counts.len()
        );
        assert!(
            bound < whole || b - a > 4_000,
            "[{a}, {b}): {bound} of {whole}"
        );
    }
}

#[test]
fn binary_entries_cross_index_levels_and_reopen_with_their_exact_values() {
    let mut s = store(37);
    let key = |n: u32| {
        let mut key = vec![0, 255];
        key.extend_from_slice(&n.to_be_bytes());
        key.extend_from_slice(&[0, 255]);
        key
    };
    let mut entries = BTreeMap::from([(Vec::new(), (Op::Put, Vec::new()))]);
    // Half-page values require more leaves than one index page can name. Empty rows and
    // binary tombstones between them exercise the same fixed header with different tails.
    for n in 0u32..2048 {
        let op = if n % 7 == 0 { Op::Delete } else { Op::Put };
        let value = if n % 11 == 0 {
            Vec::new()
        } else if op == Op::Delete {
            vec![0, 255]
        } else {
            let mut value = vec![(n % 256) as u8; s.page_capacity() / 2];
            value[0] = 0;
            value[1] = 255;
            value
        };
        entries.insert(key(n), (op, value));
    }
    let branch = build(&mut s, &entries);
    let mut descriptor = Vec::new();
    branch.encode(&mut descriptor).unwrap();
    s.checkpoint(Some(branch.root), 1).unwrap();
    let (file, finished) = finished_file(s.into_file());
    finished.unwrap();
    let (mut s, recovered) = Store::open(file, CONFIG).unwrap();
    assert_eq!(recovered.root, Some(branch.root));
    let (branch, used) = Branch::decode(&mut s, &descriptor).unwrap();
    assert_eq!(used, descriptor.len());
    check(&mut s, &branch, &entries);
    let mut cursor = branch.seek(&mut s, b"").unwrap();
    for (key, (op, value)) in &entries {
        assert!(cursor.valid());
        assert_eq!(cursor.key(), key);
        assert_eq!(cursor.op(), *op);
        assert_eq!(cursor.value(), value);
        cursor.next(&branch, &mut s).unwrap();
    }
    assert!(!cursor.valid());
    cursor.give_back(&mut s);

    // The required medium read remains a typed failure after healthy reads reuse buffers.
    let target = key(1024);
    let page = u64::from(branch.leaf_of(&target).unwrap());
    let address = branch.page_address(&s, page).unwrap();
    let (file, finished) = finished_file(s.into_file());
    finished.unwrap();
    file.inject(Fault::ReadError {
        offset: address * CONFIG.page_size as u64,
        len: CONFIG.page_size as u64,
    })
    .unwrap();
    let (mut s, _) = Store::open(file, CONFIG).unwrap();
    let mut value = Vec::new();
    assert!(matches!(
        branch.get(&mut s, &target, &mut value),
        Err(Error::Io { .. })
    ));
    assert_eq!(branch.get(&mut s, b"", &mut value).unwrap(), Some(Op::Put));
    assert!(value.is_empty());
    assert!(matches!(
        branch.get(&mut s, &target, &mut value),
        Err(Error::Io { .. })
    ));
}
