//! A settle that splits a leaf runs as a job a part on the workers and applies once every job is
//! back (research/58 §4), on a trunk whose pool's seats are a test's relays ([`relay`]): one
//! job's result held on its seat while the other's comes back and is taken, the settle is not
//! applied; let the held one go, and it applies, its leaves holding exactly what was written.
//! Facts decide every verdict, each wait for one under the relays' failure guard.
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
use crate::trunk::pool::relay::{self, Event, Kind, OpenOnDrop, Relays, Seen};
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
/// A value of this many bytes: a page holds a handful, so the batch spans tens of pages.
const VALUE: usize = 600;
/// The batch's entries: past the root leaf's `leaf_entries`, so it settles into two leaves.
const ENTRIES: u64 = 100;
/// A step's budget in work units, a few pages' merging.
const STEP: u64 = 256;

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

/// The seat whose result is held, and whether the other settle job's result has reached the
/// trunk's channel: the seat's first delivery after its result.
fn held_and_other_delivered(seen: &Seen) -> bool {
    let [held] = seen.held_dones[..] else {
        return false;
    };
    seen.events
        .iter()
        .skip_while(|e| {
            !matches!(
                e,
                Event::Done {
                    seat,
                    kind: Kind::Compact
                } if *seat != held
            )
        })
        .skip(1)
        .any(|e| matches!(e, Event::Delivered { seat } if *seat != held))
}

/// Do: a trunk on two relayed seats over the store's file; one batch past the root leaf's
/// entries, so the root settles into two leaves as two jobs; the first job's result held on its
/// seat. One idle step, which hands both out and waits for one result: the other's.
/// Expect: the settle is not applied: the root is still the leaf it was, no settle counted, one
/// job done and one out. Let the held result go and run the maintenance to its end: the settle
/// applies once, as two jobs, the root an index node over two leaves, and every key reads what
/// was written.
#[test]
fn a_split_settle_applies_once_every_job_is_back() {
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
    // Declared after the trunk, it drops first: a failed assertion lets the held result go
    // before the pool's teardown joins the workers.
    let _open = OpenOnDrop(relays);
    let mut oracle = BTreeMap::new();
    let mut b = Builder::new(&mut store, Keys::Exactly(ENTRIES)).unwrap();
    for k in 0..ENTRIES {
        let key = format!("obj-{k:06}").into_bytes();
        let value = vec![k as u8; VALUE];
        b.add(&mut store, &key, Op::Put, &value).unwrap();
        oracle.insert(key, value);
    }
    let branch = b.finish(&mut store).unwrap();
    relays.hold_done(&[Kind::Compact]);
    // Pending, taken into the root by the next step: no step here waits on the workers.
    trunk.add(branch);
    let root = trunk.root;
    // One idle step: the settle planned, both jobs out (a waiting step admits the cores), and a
    // wait for one message, which with the first job's result held is the other's.
    trunk.idle_step(&mut store, STEP).unwrap();
    let seen = relays.until("one job's result held and the other's delivered", |s| {
        held_and_other_delivered(s)
    });
    let settles: Vec<(bool, bool)> = trunk
        .tasks
        .iter()
        .filter(|t| t.node == root && matches!(t.what, What::Settle { .. }))
        .map(|t| (t.run.done(), t.run.out()))
        .collect();
    assert!(
        trunk.node(root).unwrap().leaf
            && trunk.stats().parallel_settles == 0
            && settles.len() == 2
            && settles.iter().filter(|(done, _)| *done).count() == 1
            && settles.iter().filter(|(_, out)| *out).count() == 1,
        "with one job back and one out, the settle unapplied: settles {settles:?}; {:?}; {seen:?}",
        trunk.stats()
    );
    relays.open();
    trunk.drain(&mut store).unwrap();
    let t = trunk.stats();
    assert!(
        t.parallel_settles == 1 && t.parallel_settle_jobs == 2,
        "the settle applied once, as two jobs: {t:?}"
    );
    let root = trunk.node(trunk.root).unwrap();
    assert!(
        !root.leaf && root.pivots.len() == 2,
        "the root an index node over two leaves: {} pivots, leaf {}",
        root.pivots.len(),
        root.leaf
    );
    let mut value = Vec::new();
    for (k, v) in &oracle {
        assert_eq!(trunk.get(&mut store, k, &mut value).unwrap(), Some(Op::Put));
        assert_eq!(&value, v);
    }
}
