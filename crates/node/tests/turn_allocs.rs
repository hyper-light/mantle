//! What a node's loop allocates at rest, counted exactly by hyper-measure's counting allocator on
//! the deterministic simulation's one thread (`support/cell.rs`): a cell of three, its range led and
//! its links judged, left with no request for ten seconds of its virtual clock, every node turning
//! for its heartbeats, its timers and its leader's beats.
//!
//! And the same cell serving its three clients' writes and reads of two keys, 40 operations each,
//! counted from the clients' start to their last answer, every turn taking the transport's events
//! of their exchanges and the replicas' messages of their entries.
//!
//! A turn keeps the storage of what it takes across turns (research/71, step 5): the transport's
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
    let total = alloc::end();
    total.less(&alloc::read_aside())
}

/// 477,128 allocations before the turn kept its buffers (`drive`'s three lists, 189,808 of
/// them; the plane's message copies, 119,301); 168,019 after, 96,388 since hyper-raft 3d6640f
/// (a group's pairs told in place, a packet's credit recorded without allocating, a stream's queue
/// of pieces kept), and 96,375 since its fb74b59, whose driver reads its sockets before it judges
/// its timers: the transport's, QUIC's, the stream's and the simulated network's own. The same
/// every run and on every host: nothing at rest draws at random, and a node's QUIC endpoint and
/// its liveness stream keep the simulation's clock. With the host's, read as the endpoint was made
/// and for the clock's resolution, the count was the machine's: at fb74b59 96,375 on a Linux
/// x86_64 host, 94,195 on a macOS aarch64 one and 65,102 in a Linux aarch64 VM on that.
#[test]
fn a_cell_at_rest_turns_without_remaking_what_a_turn_keeps() {
    let counts = at_rest(1);
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (96_375, 0),
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
    let total = alloc::end();
    drop(stops);
    total.less(&alloc::read_aside())
}

/// 77,323 allocations before the turn kept its buffers (the events' buffer 395 of them, `drive`'s
/// lists 19,870, the plane's copies 11,970); 45,088 or 45,089 after, and 41,182 since the range's
/// messages, rows and keys are each made at their lengths, an entry's overlay keeps the rows it
/// is given and a put's commands go to their ranges moved (reallocations 6,372 to 6,377 before,
/// 2,303 to 2,307 then), 33,564 since hyper-raft 3d6640f's cuts (2,306 reallocations), and 34,094
/// since its fb74b59, whose driver reads its sockets before it judges its timers: the clients'
/// run sends 502 more datagrams (15,040), each one the simulated network's copy. The clients draw
/// their identities and nonces at random, which moved a buffer's growth by an allocation now and
/// then while encodings grew; made at their lengths, they no longer do, so the count is exact.
/// On Linux each socket, a simulated one too, holds `recvmmsg` and `sendmmsg` headers (hyper-rt's
/// `Batched`, 11 allocations, and its receive `Ring`, 6), and the three clients make theirs inside
/// the count: 51 that macOS and Windows, which hold none, do not make. 360 more since a session
/// keeps an answer only where its row holds it (research/87 §1): each command applied runs over
/// its entry's writes in an overlay of its own, taken in once its answer is kept, whose map makes
/// its first node, one allocation for each of the 120 puts' three commands.
#[test]
fn a_cell_serving_writes_turns_without_remaking_what_a_turn_keeps() {
    const SERVING: u64 = if cfg!(target_os = "linux") {
        34_454
    } else {
        34_403
    };
    let counts = serving(1, 40);
    assert_eq!(counts.allocations, SERVING, "{counts:?}");
}
