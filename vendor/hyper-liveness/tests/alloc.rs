//! The allocation law (`CLAUDE.md` §1a): once every pair is configured, a heartbeat sent and a
//! heartbeat taken allocate nothing, whatever the groups the pairs share. The benchmark's world
//! (`benches/support/world.rs`) drives the nodes; `cargo bench --bench allocs` reports the counts.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_macros,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::indexing_slicing,
    missing_docs
)]

use std::time::Duration;

use hyper_measure::alloc;

#[path = "../benches/support/world.rs"]
mod world;
use world::World;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

#[test]
fn a_configured_heartbeat_allocates_nothing() {
    assert!(alloc::installed());
    for (nodes, groups) in [(2usize, 1u32), (3, 1), (5, 64)] {
        let mut world = World::new(nodes, groups, 0x2545_F491_4F6C_DD1D);
        world.warm();
        world.sent = 0;
        world.taken = 0;
        alloc::begin();
        world.run(Duration::from_secs(30));
        let counts = alloc::end();
        assert!(
            world.sent > 0 && world.taken > 0,
            "{} {}",
            world.sent,
            world.taken
        );
        assert_eq!(
            (counts.allocations, counts.reallocations),
            (0, 0),
            "{nodes} nodes, {groups} groups: {counts:?}"
        );
    }
}
