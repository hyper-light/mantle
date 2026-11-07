//! What keeping a replica's pairs told costs once nothing changes (`Owner::pairs`): an owner calls
//! it after every drive that may have changed the replica's configuration, so on a group whose
//! membership stands it runs every turn, and mantle's loop called it about 64 times a second at
//! rest and 2,948 times a native PUT of 64 KiB (mantle docs/research/80, "The sites"). Counted by
//! hyper-measure's allocator: a call whose configuration names the peers already attached allocates
//! nothing; a change still attaches and detaches as before.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

mod support;

use std::task::Waker;
use std::time::Duration;

use hyper_durable::{Bytes, Owner, Replica};
use hyper_liveness::{Liveness, Settings as LiveSettings};
use hyper_measure::alloc;
use hyper_raft::proto::ConfState;
use hyper_timing::Exposure;
use support::cluster::settings;
use support::{Kv, SimStore};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The calls counted once the pairs are attached.
const CALLS: u64 = 1_000;

#[test]
fn pairs_unchanged_allocate_nothing() {
    let configuration = ConfState {
        voters: vec![1, 2, 3],
        ..ConfState::default()
    };
    let replica: Replica<SimStore, Kv, Bytes> = Replica::open(
        &settings(1, 7),
        SimStore::new(3),
        Kv::new(configuration, false),
        Bytes::new(u64::MAX),
    )
    .unwrap();
    let mut owner = Owner::new(vec![Waker::noop().clone()]);
    let handle = owner.insert(replica).map_err(|_| ()).unwrap();
    let mut liveness = Liveness::new(LiveSettings {
        local: 1,
        run: 1,
        max_peers: 3,
        history: Exposure::new(),
        resolution: Duration::from_nanos(1),
    })
    .unwrap();
    owner.pairs(handle, &mut liveness, false).unwrap();
    assert!(
        alloc::installed(),
        "the counting allocator is not installed"
    );
    alloc::begin();
    for _ in 0..CALLS {
        owner.pairs(handle, &mut liveness, false).unwrap();
    }
    let counts = alloc::end();
    assert_eq!(
        (counts.allocations, counts.reallocations, counts.bytes),
        (0, 0, 0),
        "{CALLS} calls with the configuration unchanged: {counts:?}"
    );
    // Gone, its pairs are detached: the replica's peers no longer attached.
    owner.pairs(handle, &mut liveness, true).unwrap();
    owner.pairs(handle, &mut liveness, false).unwrap();
}
