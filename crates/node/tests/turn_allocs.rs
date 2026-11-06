//! What a node's loop allocates at rest, counted exactly by hyper-measure's counting allocator on
//! the deterministic simulation's one thread (`support/cell.rs`): a cell of three, its range led and
//! its links judged, left with no request for ten seconds of its virtual clock, every node turning
//! for its heartbeats, its timers and its leader's beats.
//!
//! And the same cell serving its three clients' writes and reads of two keys, 40 operations each,
//! counted from the clients' start to their last answer, every turn taking the transport's events
//! of their exchanges and the replicas' messages of their entries.
//!
//! A turn keeps the storage of what it takes across turns (research/80, step 5): the transport's
//! events, the replicas it drives and the Raft messages they give, and the plane's messages its
//! wait takes, which it copies into a buffer it keeps; the messages a drain of the plane finds it
//! takes where they were opened, copying none. Each of those was a buffer taken or made and
//! dropped each turn, and each plane message a copy of its own.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_measure::alloc;

#[allow(dead_code)]
#[path = "support/cell.rs"]
mod cell;
#[allow(dead_code)]
#[path = "support/history.rs"]
mod history;
#[allow(dead_code)]
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[allow(dead_code)]
#[path = "../../range/tests/support/store.rs"]
mod store;
use cell::Sim;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

const SECOND: u64 = 1_000_000_000;

/// A cell of three at rest: what ten seconds of its loops ask of the allocator, every node's,
/// the network's and the test's taking of their reports together, once a second of rest has
/// grown every buffer a turn keeps.
fn at_rest(seed: u64) -> alloc::Counts {
    let mut cell = Sim::new(seed);
    cell.leader();
    cell.judged();
    cell.converged();
    cell.run_for(SECOND);
    alloc::begin();
    cell.run_for(10 * SECOND);
    alloc::end()
}

/// 477,128 allocations before the turn kept its buffers (`drive`'s three lists, 189,808 of
/// them; the plane's message copies, 119,301); 168,019 now, the transport's, QUIC's, the
/// stream's and the simulated network's own. The same every run: nothing at rest draws at random.
#[test]
fn a_cell_at_rest_turns_without_remaking_what_a_turn_keeps() {
    let counts = at_rest(1);
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (168_019, 0),
        "{counts:?}"
    );
}

/// The cell's allocations while its three clients ask `ops` operations each.
fn serving(seed: u64, ops: u64) -> alloc::Counts {
    let mut cell = Sim::new(seed);
    cell.leader();
    cell.judged();
    cell.open_bucket();
    cell.converged();
    cell.run_for(SECOND);
    alloc::begin();
    let stops = cell.start_workload(ops);
    cell.done_by(ops, 4);
    let counts = alloc::end();
    drop(stops);
    counts
}

/// 77,323 allocations before (the events' buffer 395 of them, `drive`'s lists 19,870, the plane's
/// copies 11,970); 45,088 or 45,089 now. Not one number: the clients draw their identities and
/// nonces at random, and an encoding a byte longer moves a buffer's growth by one allocation now
/// and then (45,088 in six runs of seven, 45,089 in one; reallocations 6,372 to 6,377), so the
/// count is held within two of those.
#[test]
fn a_cell_serving_writes_turns_without_remaking_what_a_turn_keeps() {
    let counts = serving(1, 40);
    assert!(
        (45_086..=45_091).contains(&counts.allocations),
        "{counts:?}"
    );
}
