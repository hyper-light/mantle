//! The trunk (docs/design/engine-structure.md §4, step E4) against a `BTreeMap`: batches of puts
//! and deletes, each packed into a branch as a memtable would be and incorporated, with a small
//! fanout and small leaves so flushes, leaf splits and node splits all happen. After every batch
//! every key reads its newest operation, absent keys read nothing, and the store holds exactly
//! the extents of the branches the trunk names: none leaked, none freed while named.
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
use mantle_engine::branch::{Builder, Op};
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::{Trunk, TrunkConfig};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};

fn key(k: u32) -> Vec<u8> {
    format!("bucket-{}/dir{:03}/obj-{k:08}", k % 3, k % 97).into_bytes()
}

fn check_refs(store: &Store<SimFile>, trunk: &Trunk) {
    let mut named: BTreeSet<u64> = BTreeSet::new();
    for b in trunk.branches() {
        for e in b.extents {
            assert!(named.insert(e), "extent {e} named by two branches");
        }
    }
    let held: BTreeSet<u64> = store
        .refs()
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(e, _)| e as u64)
        .collect();
    let mut expected = named.clone();
    expected.insert(0);
    expected.extend(store.map_extents());
    assert_eq!(held, expected);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn the_trunk_reads_the_newest_entry_of_every_key(
        batches in proptest::collection::vec(
            proptest::collection::vec((0u32..3000, 0u8..10, any::<u8>()), 1..400),
            1..40,
        ),
        seed in any::<u64>(),
    ) {
        let align = Alignment::new(4096).unwrap();
        let file = SimFile::new(align, Alignment::new(512).unwrap(), seed).unwrap();
        let mut store = Store::create(file, CONFIG).unwrap();
        let mut trunk = Trunk::new(TrunkConfig { fanout: 3, leaf_entries: 64 }).unwrap();
        let mut oracle: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
        for batch in &batches {
            // The memtable's view of a batch: the last operation of each key, in key order.
            let mut mem: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
            for &(k, kind, v) in batch {
                let e = if kind < 2 { (Op::Delete, Vec::new()) } else { (Op::Put, vec![v; 1 + (k % 50) as usize]) };
                mem.insert(key(k), e);
            }
            let mut b = Builder::new(store.page_capacity()).unwrap();
            for (k, (op, v)) in &mem {
                b.add(&mut store, k, *op, v).unwrap();
            }
            let branch = b.finish(&mut store).unwrap();
            trunk.incorporate(&mut store, branch).unwrap();
            oracle.extend(mem);
            check_refs(&store, &trunk);
        }
        let mut value = Vec::new();
        for (k, (op, v)) in &oracle {
            let got = trunk.get(&mut store, k, &mut value).unwrap();
            // A tombstone may be compacted away at a leaf: absent and deleted read alike.
            match op {
                Op::Put => {
                    prop_assert_eq!(got, Some(Op::Put), "key {:?}", String::from_utf8_lossy(k));
                    prop_assert_eq!(&value, v);
                }
                Op::Delete => prop_assert!(got != Some(Op::Put), "deleted key {:?} reads a put", String::from_utf8_lossy(k)),
            }
        }
        for k in 3000..3050 {
            prop_assert_eq!(trunk.get(&mut store, &key(k), &mut value).unwrap(), None);
        }
    }
}

/// A heavier deterministic workload: the tree must deepen and split, and read right after
/// every batch.
#[test]
fn a_growing_trunk_splits_deepens_and_reads_after_every_batch() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 9).unwrap();
    let mut store = Store::create(file, CONFIG).unwrap();
    let mut trunk = Trunk::new(TrunkConfig {
        fanout: 4,
        leaf_entries: 128,
    })
    .unwrap();
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    let mut value = Vec::new();
    for batch in 0..120u32 {
        let mut mem: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for _ in 0..200 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = (x % 20_000) as u32;
            mem.insert(key(k), format!("v{batch}-{k}").into_bytes());
        }
        let mut b = Builder::new(store.page_capacity()).unwrap();
        for (k, v) in &mem {
            b.add(&mut store, k, Op::Put, v).unwrap();
        }
        let branch = b.finish(&mut store).unwrap();
        trunk.incorporate(&mut store, branch).unwrap();
        oracle.extend(mem);
        check_refs(&store, &trunk);
        for (k, v) in &oracle {
            assert_eq!(trunk.get(&mut store, k, &mut value).unwrap(), Some(Op::Put));
            assert_eq!(&value, v);
        }
    }
    let (height, nodes, leaves) = trunk.shape().unwrap();
    eprintln!(
        "height {height} nodes {nodes} leaves {leaves} keys {}",
        oracle.len()
    );
    assert!(height >= 3, "height {height}");
    // Internal pivot bundles buffer up to `fanout` branches each before flushing (the
    // size-tiered Bε-tree), so many keys sit above the leaves; the leaves still split.
    assert!(leaves >= 10, "leaves {leaves}");
}
