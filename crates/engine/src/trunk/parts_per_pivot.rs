//! A packing's parts are placed per pivot ([`Trunk::places`]), on a trunk whose pool's two seats
//! are a test's relays ([`relay`]): with one pivot's compaction held out and every other's
//! applied, a packing cut at the root's pivot keys puts every other pivot's part straight into
//! its bundle and sends only the held pivot's in flight. Every key reads its newest value at once and after the
//! maintenance runs to its end, and the store holds exactly the trunk's extents. Facts decide
//! every verdict, each wait for one under the relays' failure guard.
#![cfg(test)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::cast_possible_truncation
)]
use super::*;
use crate::branch::filter::Keys;
use crate::branch::{Builder, Op};
use crate::trunk::pool::relay::{self, Kind, OpenOnDrop, Relays};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use std::collections::BTreeMap;
use std::path::Path;

const CONFIG: crate::store::Config = crate::store::Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 64,
};
/// The first batch's entries: past what the root leaf holds, so its settle makes the root an
/// index node over several leaves.
const FIRST: u64 = 256;
/// A step's budget: past any merge here, short of the whole budget, whose step waits for every
/// job out ([`Trunk::step`]), the held one with it.
const STEP: u64 = 1 << 40;

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

fn key(k: u64) -> Vec<u8> {
    format!("obj-{k:06}").into_bytes()
}

/// One branch of `keys` at `round`'s value, the oracle updated.
fn branch(
    store: &mut Store<DeviceFile>,
    keys: &[u64],
    round: u8,
    oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) -> Branch {
    let mut b = Builder::new(store, Keys::Exactly(keys.len() as u64)).unwrap();
    for &k in keys {
        let value = vec![round; 64];
        b.add(store, &key(k), Op::Put, &value).unwrap();
        oracle.insert(key(k), value);
    }
    b.finish(store).unwrap()
}

fn check_reads(
    store: &mut Store<DeviceFile>,
    trunk: &mut Trunk,
    oracle: &BTreeMap<Vec<u8>, Vec<u8>>,
) {
    let mut value = Vec::new();
    for (k, v) in oracle {
        assert_eq!(
            trunk.get(store, k, &mut value).unwrap(),
            Some(Op::Put),
            "{}",
            String::from_utf8_lossy(k)
        );
        assert_eq!(&value, v, "{}", String::from_utf8_lossy(k));
    }
}

/// Do: a trunk on two relayed seats; a first batch settled to its end, the root an index node;
/// the next compaction job held on its seat; a branch over every key in flight, its pivots'
/// compactions out, one held and the rest run and applied on the other seat; then a packing cut
/// at the root's pivot keys, a part a pivot, added and taken in by a step. Expect: every part
/// but the held pivot's placed straight into its pivot's bundle, counted as parted, the held
/// pivot's in flight; every key reads its newest value. Let the held job go and run the maintenance to its end: every key
/// still reads its newest value, and the store holds exactly the trunk's extents.
#[test]
fn a_packing_s_parts_are_placed_per_pivot_beside_a_compaction_out() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(open(&path, true), CONFIG).unwrap();
    let mut trunk = Trunk::new(TRUNK).unwrap();
    let relays = Relays::leak();
    let worker = path.clone();
    trunk.set_pool(
        Pool::new(
            relay::spawn(move || Ok(open(&worker, false)), CONFIG, relays),
            2,
        )
        .unwrap(),
    );
    // Declared after the trunk, it drops first: a failed assertion lets the held job go before
    // the pool's teardown joins the workers.
    let _open = OpenOnDrop(relays);
    let mut oracle = BTreeMap::new();
    let all: Vec<u64> = (0..FIRST).collect();
    let first = branch(&mut store, &all, 1, &mut oracle);
    trunk.add(first);
    trunk.drain(&mut store).unwrap();
    let mut cuts = Vec::new();
    trunk.root_cuts(&mut cuts);
    assert!(
        cuts.len() > 1,
        "the root an index node of pivots: {:?}",
        trunk.levels()
    );
    // The keys of each pivot's range, by the root's pivot keys.
    let pivot_of = |k: u64| cuts[1..].partition_point(|c| c.as_slice() <= key(k).as_slice());
    let mut ranges: Vec<Vec<u64>> = vec![Vec::new(); cuts.len()];
    for k in 0..FIRST {
        ranges[pivot_of(k)].push(k);
    }
    assert!(
        ranges.iter().all(|r| !r.is_empty()),
        "every pivot holds keys"
    );
    // A branch over every key in flight: a compaction for each pivot, the first job out held on
    // its seat, the others run on the other seat and applied, each idle step taking one result.
    relays.hold_job(Kind::Compact);
    let busy = branch(&mut store, &all, 2, &mut oracle);
    let compacted = trunk.stats().pivot_compactions;
    trunk.add(busy);
    trunk.step(&mut store, STEP).unwrap();
    relays.until("a pivot's compaction held on its seat", |s| {
        !s.held_jobs.is_empty()
    });
    let others = (cuts.len() - 1) as u64;
    for _ in 0..others {
        trunk.idle_step(&mut store, STEP).unwrap();
    }
    assert_eq!(
        trunk.stats().pivot_compactions - compacted,
        others,
        "every pivot's compaction but the held one applied: {:?}",
        trunk.stats()
    );
    let root = trunk.root;
    let held = trunk
        .tasks
        .iter()
        .find_map(|t| match &t.what {
            What::Compact { key, .. } if t.node == root && t.run.out() => Some(key.clone()),
            _ => None,
        })
        .expect("the held pivot's compaction out");
    let busy_pivot = cuts.iter().position(|c| *c == held).unwrap();
    // A packing cut at the root's pivot keys, a part a pivot.
    let before = trunk.stats();
    let mut parts = Vec::new();
    for (j, keys) in ranges.iter().enumerate() {
        parts.push((j, branch(&mut store, keys, 3, &mut oracle)));
    }
    let others: u64 = parts
        .iter()
        .filter(|(j, _)| *j != busy_pivot)
        .map(|(_, b)| b.count)
        .sum();
    trunk.add_parts(cuts.clone(), parts, None);
    trunk.step(&mut store, STEP).unwrap();
    let after = trunk.stats();
    assert_eq!(
        after.parted_entries - before.parted_entries,
        others,
        "every part but the held pivot's ({busy_pivot}) placed: before {before:?}; after \
         {after:?}"
    );
    assert!(
        !relays.seen().held_jobs.is_empty(),
        "the held pivot's compaction still held"
    );
    check_reads(&mut store, &mut trunk, &oracle);
    relays.open();
    trunk.drain(&mut store).unwrap();
    check_reads(&mut store, &mut trunk, &oracle);
    let mut named = std::collections::BTreeSet::new();
    for b in trunk.branches() {
        for e in b.extents {
            assert!(named.insert(e), "extent {e} named by two branches");
        }
    }
    named.insert(0);
    named.extend(store.map_extents());
    let held: std::collections::BTreeSet<u64> = store
        .refs()
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(e, _)| e as u64)
        .collect();
    assert_eq!(held, named, "the store's extents against the trunk's");
}
