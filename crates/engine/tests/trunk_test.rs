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
use mantle_engine::branch::filter::Keys;
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
            let mut b = Builder::new(&mut store, Keys::Exactly(mem.len() as u64)).unwrap();
            for (k, (op, v)) in &mem {
                b.add(&mut store, k, *op, v).unwrap();
            }
            let branch = b.finish(&mut store).unwrap();
            trunk.incorporate(&mut store, branch).unwrap();
            oracle.extend(mem);
            check_refs(&store, &trunk);
            // A node takes a flush only below `fanout` live in-flight bundles a pivot, so it holds
            // at most `3 fanout + 1` (the last flush before a split landing on a full node).
            let most = trunk.stats().inflight_most;
            prop_assert!(most <= 3 * 3 + 1, "a node held {} live in-flight bundles", most);
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

fn check_reads(
    store: &mut Store<SimFile>,
    trunk: &mut Trunk,
    oracle: &BTreeMap<Vec<u8>, (Op, Vec<u8>)>,
) -> Result<(), TestCaseError> {
    let mut value = Vec::new();
    for (k, (op, v)) in oracle {
        let got = trunk.get(store, k, &mut value).unwrap();
        match op {
            Op::Put => {
                prop_assert_eq!(got, Some(Op::Put), "key {:?}", String::from_utf8_lossy(k));
                prop_assert_eq!(&value, v);
            }
            Op::Delete => prop_assert!(got != Some(Op::Put), "deleted key reads a put"),
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// Maintenance a slice at a time: each batch is added as pending, then a few steps of a
    /// small budget run, maintenance left part done between them, and every key reads its newest
    /// entry after every step. Drained at the end, the trunk holds exactly its branches'
    /// extents.
    #[test]
    fn reads_are_exact_between_any_maintenance_steps(
        batches in proptest::collection::vec(
            (proptest::collection::vec((0u32..2000, 0u8..10, any::<u8>()), 1..300), 1u64..40, 0usize..4),
            1..30,
        ),
        seed in any::<u64>(),
    ) {
        let align = Alignment::new(4096).unwrap();
        let file = SimFile::new(align, Alignment::new(512).unwrap(), seed).unwrap();
        let mut store = Store::create(file, CONFIG).unwrap();
        let mut trunk = Trunk::new(TrunkConfig { fanout: 3, leaf_entries: 64 }).unwrap();
        let mut oracle: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
        for (batch, budget, steps) in &batches {
            let mut mem: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
            for &(k, kind, v) in batch {
                let e = if kind < 2 { (Op::Delete, Vec::new()) } else { (Op::Put, vec![v; 1 + (k % 50) as usize]) };
                mem.insert(key(k), e);
            }
            let mut b = Builder::new(&mut store, Keys::Exactly(mem.len() as u64)).unwrap();
            for (k, (op, v)) in &mem {
                b.add(&mut store, k, *op, v).unwrap();
            }
            trunk.add(b.finish(&mut store).unwrap());
            oracle.extend(mem);
            check_reads(&mut store, &mut trunk, &oracle)?;
            for _ in 0..*steps {
                let used = trunk.step(&mut store, *budget).unwrap();
                // A step merges its whole budget unless the maintenance ran out.
                prop_assert!(used == *budget || trunk.is_idle(), "{} of {}", used, budget);
                check_reads(&mut store, &mut trunk, &oracle)?;
            }
        }
        trunk.drain(&mut store).unwrap();
        prop_assert!(trunk.is_idle());
        prop_assert_eq!(trunk.debt(), 0);
        check_refs(&store, &trunk);
        check_reads(&mut store, &mut trunk, &oracle)?;
    }
}

/// Maintenance one merged key a step, every key read after every step, with leaves small enough
/// that settles split them: a step never ends between a node's split and its parent's taking
/// the new nodes in (keys in them were unreachable when it did).
#[test]
fn reads_are_exact_after_every_single_key_step_through_splits() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 23).unwrap();
    let mut store = Store::create(file, CONFIG).unwrap();
    let mut trunk = Trunk::new(TrunkConfig {
        fanout: 2,
        leaf_entries: 16,
    })
    .unwrap();
    let mut oracle: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
    let mut x = 0x0123_4567_89ab_cdefu64;
    for batch in 0..40u32 {
        let mut mem: BTreeMap<Vec<u8>, (Op, Vec<u8>)> = BTreeMap::new();
        for _ in 0..30 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = (x % 600) as u32;
            mem.insert(key(k), (Op::Put, format!("v{batch}-{k}").into_bytes()));
        }
        let mut b = Builder::new(&mut store, Keys::Exactly(mem.len() as u64)).unwrap();
        for (k, (op, v)) in &mem {
            b.add(&mut store, k, *op, v).unwrap();
        }
        trunk.add(b.finish(&mut store).unwrap());
        oracle.extend(mem);
        while !trunk.is_idle() {
            trunk.step(&mut store, 1).unwrap();
            check_reads(&mut store, &mut trunk, &oracle).unwrap();
        }
    }
    assert!(trunk.stats().splits > 4, "{:?}", trunk.stats());
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
        let mut b = Builder::new(&mut store, Keys::Exactly(mem.len() as u64)).unwrap();
        for (k, v) in &mem {
            b.add(&mut store, k, Op::Put, v).unwrap();
        }
        let branch = b.finish(&mut store).unwrap();
        trunk.incorporate(&mut store, branch).unwrap();
        oracle.extend(mem);
        check_refs(&store, &trunk);
        let most = trunk.stats().inflight_most;
        assert!(
            most <= 3 * 4 + 1,
            "a node held {most} live in-flight bundles"
        );
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

/// One batch, a memtable's view of it, packed as `Trunk::add_parts` takes it: a branch for each
/// range between consecutive `cuts` (the first range every key below the second cut), each named
/// by its index into `cuts`, empty ranges skipped. With no cuts, one branch over every key.
fn pack_at(
    store: &mut Store<SimFile>,
    mem: &BTreeMap<Vec<u8>, Vec<u8>>,
    cuts: &[Vec<u8>],
) -> Vec<(usize, mantle_engine::branch::Branch)> {
    let part_of = |k: &[u8]| {
        cuts.get(1..)
            .map_or(0, |c| c.partition_point(|x| x.as_slice() <= k))
    };
    let entries: Vec<_> = mem.iter().collect();
    let mut parts = Vec::new();
    let mut at = 0;
    while at < entries.len() {
        let j = part_of(entries[at].0);
        let end = at
            + entries[at..]
                .iter()
                .take_while(|(k, _)| part_of(k) == j)
                .count();
        let mut b = Builder::new(store, Keys::Exactly((end - at) as u64)).unwrap();
        for (k, v) in &entries[at..end] {
            b.add(store, k, Op::Put, v).unwrap();
        }
        parts.push((j, b.finish(store).unwrap()));
        at = end;
    }
    parts
}

/// Batches packed at the root's pivot keys go into its pivots' bundles as they are, never
/// written again by a root compaction of one input (research/57 §7): with no compaction out and
/// no in-flight bundle left between batches, every batch packed while the root is an index node
/// is taken whole, its entries counted in `parted_entries`, and after every batch every key reads
/// its newest value and the store holds exactly the extents the trunk names.
#[test]
fn batches_packed_at_the_roots_pivots_enter_its_bundles_as_they_are() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 11).unwrap();
    let mut store = Store::create(file, CONFIG).unwrap();
    let mut trunk = Trunk::new(TrunkConfig {
        fanout: 4,
        leaf_entries: 128,
    })
    .unwrap();
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut x = 0x2545_f491_4f6c_dd1du64;
    let mut value = Vec::new();
    let mut cuts = Vec::new();
    let mut cut_entries = 0u64;
    for batch in 0..120u32 {
        let mut mem: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for _ in 0..200 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = (x % 20_000) as u32;
            mem.insert(key(k), format!("v{batch}-{k}").into_bytes());
        }
        trunk.root_cuts(&mut cuts);
        let parts = pack_at(&mut store, &mem, &cuts);
        if cuts.is_empty() {
            for (_, b) in parts {
                trunk.incorporate(&mut store, b).unwrap();
            }
        } else {
            cut_entries += mem.len() as u64;
            trunk.add_parts(cuts.clone(), parts, None);
            trunk.drain(&mut store).unwrap();
        }
        oracle.extend(mem);
        check_refs(&store, &trunk);
        for (k, v) in &oracle {
            assert_eq!(trunk.get(&mut store, k, &mut value).unwrap(), Some(Op::Put));
            assert_eq!(&value, v);
        }
    }
    assert!(cut_entries > 0, "the root never became an index node");
    assert_eq!(trunk.stats().parted_entries, cut_entries);
}

/// Batches packed at keys that are not the root's pivot keys enter the root as one in-flight
/// bundle and are compacted, never placed as a pivot's: every batch's cuts drop one of the root's
/// pivot keys, so no pivot's range is one branch's, and no entry is counted in
/// `parted_entries`; every key reads its newest value after every batch, and the store holds
/// exactly the extents the trunk names.
#[test]
fn batches_packed_at_other_keys_enter_in_flight_and_are_compacted() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 13).unwrap();
    let mut store = Store::create(file, CONFIG).unwrap();
    let mut trunk = Trunk::new(TrunkConfig {
        fanout: 4,
        leaf_entries: 128,
    })
    .unwrap();
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut x = 0x5bd1_e995_9e37_79b9u64;
    let mut value = Vec::new();
    let mut cuts = Vec::new();
    let mut stale = 0u32;
    for batch in 0..120u32 {
        let mut mem: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for _ in 0..200 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = (x % 20_000) as u32;
            mem.insert(key(k), format!("v{batch}-{k}").into_bytes());
        }
        trunk.root_cuts(&mut cuts);
        if cuts.len() > 1 {
            // The root's last pivot key dropped: its last branch spans two pivots.
            cuts.pop();
            stale += 1;
            let parts = pack_at(&mut store, &mem, &cuts);
            trunk.add_parts(cuts.clone(), parts, None);
            trunk.drain(&mut store).unwrap();
        } else {
            for (_, b) in pack_at(&mut store, &mem, &[]) {
                trunk.incorporate(&mut store, b).unwrap();
            }
        }
        oracle.extend(mem);
        check_refs(&store, &trunk);
        for (k, v) in &oracle {
            assert_eq!(trunk.get(&mut store, k, &mut value).unwrap(), Some(Op::Put));
            assert_eq!(&value, v);
        }
    }
    assert!(stale > 0, "the root never had two pivots");
    assert_eq!(trunk.stats().parted_entries, 0);
}
