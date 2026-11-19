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
/// of pieces kept), 96,375 since its fb74b59, whose driver reads its sockets before it judges its
/// timers: the transport's, QUIC's, the stream's and the simulated network's own, and 95,102 since
/// its 17371e2, whose liveness stream's sender floor is never below the stream's own proof, from
/// the poll that asks a liveness write to the poll that takes its answer (65cdd84): the cell
/// answers that write once its shard's clock has passed the ask, so the proof outlasts the writes'
/// flushes and the floor rose, and the ten seconds send 446 fewer heartbeats and ask 217 fewer
/// liveness writes, while the plane carries 758 more of the ranges' Raft frames. The same every run
/// and on every host: nothing at rest draws at random, and a node's QUIC endpoint and its liveness
/// stream keep the simulation's clock. With the host's, read as the endpoint was made and for the
/// clock's resolution, the count was the machine's: at fb74b59 96,375 on a Linux x86_64 host,
/// 94,195 on a macOS aarch64 one and 65,102 in a Linux aarch64 VM on that. 95,642 since hyper-raft
/// a575a44, whose default-mode floor takes the log writes' flush, the stream's proof and `G` each
/// as the mean of the samples within their tail as each came (RFC 6298's bound over the samples
/// before it): a proof the cell answered late, its shard's clock past the ask by more than the
/// proofs' tail, no longer raises the floor, and the ten seconds ask 4,042 liveness writes where
/// they asked 4,011, 31 more heartbeats' proofs, and count 540 allocations more, reallocations 0
/// both. The rule changes no allocation site: what moved is when the stream sends and asks its
/// writes, and the work that schedule gives. 95,768 since the cell's nodes hold the budget a node
/// derives (`derive`'s transport class), the least their statement holds (`NodeSettings::stated`,
/// `bounds::least_transport`; 517,485 bytes on this Mac), where they had stated four receive
/// chunks for each of their five peers by hand (1,310,540): at the floor a window's ceiling is
/// QUIC's initial window, 12,000 bytes, below a chunk's 65,527, so a node holds no receive chunk,
/// as no node at its floor does, and copies each datagram into a buffer of its own: 126 more
/// allocations than the hand-stated chunk a peer gave, reallocations 0 both. The floor's windows
/// are the same on every host, the receive ring and the connections' state held apart before them
/// (`bounds::ring_growth`, `bounds::connections_state`); with those held apart, the hand-stated
/// budget had held no chunk on Linux since the ring was, nor here since the state was.
#[test]
fn a_cell_at_rest_turns_without_remaking_what_a_turn_keeps() {
    let counts = at_rest(1);
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (95_768, 0),
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
/// run sends 502 more datagrams (15,040), each one the simulated network's copy; 34,034 since its
/// 294678c, whose certificate decoder streams into the certificate's buffer (hyper-tls VENDORED.md
/// §11): the run's 12 Brotli decodes of a node's certificate, 5 allocations fewer each. The clients
/// draw their identities and nonces at random, which moved a buffer's growth by an allocation now
/// and then while encodings grew; made at their lengths, they no longer do, so the count is exact.
/// On Linux each socket, a simulated one too, holds `recvmmsg` and `sendmmsg` headers (hyper-rt's
/// `Batched`, 11 allocations, and its receive `Ring`, 6), and the three clients make theirs inside
/// the count: 51 that macOS and Windows, which hold none, do not make. 360 more since a session
/// keeps an answer only where its row holds it (research/87 §1): each command applied runs over
/// its entry's writes in an overlay of its own, taken in once its answer is kept, whose map makes
/// its first node, one allocation for each of the 120 puts' three commands. 6 fewer since a peer's
/// connection is granted only the streams the node serves it, not the chunks the node asks of
/// peers itself (`bounds::Stated::asked`): hyper-quic builds a connection's stream tables in fewer
/// steps of growth. 52 more since hyper-raft 17371e2, whose liveness stream's sender floor takes
/// the stream's own proof (the cell at rest's count): the clients' run asks 10 more liveness
/// writes and sends 6 fewer heartbeats, its plane carries 144 fewer Raft frames, and it ends
/// 61.8 ms sooner on the virtual clock. On Linux, 34,388 then and 34,440 since: CI run
/// 44239790476's linux-aarch64 crates leg counted 34,440 at 0ae45327. The clients' TLS drew from
/// the system as each dial built its hello, the seed of the order its extensions are encoded in
/// among it (hyper-tls `ClientHelloInput::new`), so the hybrid key share's 1,216 octets were
/// appended at another length each run and the hello's buffer grew in other steps: 2,311 to 2,318
/// reallocations in 45 runs on this Mac, every allocation the same. A client given a seed draws its
/// TLS's from it (`mantle_node::client`'s `Seeded`), so its hellos are the seed's and the run's
/// reallocations the same every run: 2,316 here. A node's QUIC endpoint drew its connection IDs
/// from the thread's generator though it was seeded, and the table it finds its connections in is
/// laid out by their values: about one run in 16, its deletions left a node's table to grow from 32
/// buckets to 64 (2,120 bytes) where other layouts rehashed it in place, and the run counted 34,390.
/// The IDs are the seed's since hyper-raft d4bd299 (hyper-quic VENDORED.md §52), and every run
/// counts 34,389 allocations, 2,316 reallocations and 15,409,464 bytes: 32 runs alone and 72 with
/// 6 or 12 at once on this Mac. The bytes are 1,008 more than with the thread's IDs, 336 for each
/// of the three clients' endpoints made inside the count: hyper-quic's endpoint holds its IDs'
/// generator, 2,632 bytes where it was 2,312, and its configuration's and its own hashed generators
/// are 32 bytes where they were 24. At mantle f768f91f, before hyper-raft a575a44, this Mac counted
/// 33,931 (2,008 reallocations, 16,057,317 bytes) every run: the 34,389 no longer held there, a
/// drift of the commits since b834a708 that a575a44 did not make. 33,493 since a575a44, whose
/// default-mode floor takes its flush, its proof and `G` each within their tail as each came (the
/// cell at rest's count): the clients' run asks 420 liveness writes where it asked 481, ends
/// 1.0 ms later on the virtual clock (862.4 ms against 861.4 ms), and counts 438 allocations
/// fewer, 2,050 reallocations and 15,797,631 bytes. On Linux 33,544, this Mac's and the 51 its
/// sockets' batched headers add, derived and not yet counted there. 33,494 since a peer's exchange
/// takes its reply's room with its request's head (hyper-transport's `Core::admit_prefix`, a local
/// change): the room is reserved at the longest head's frame, 82 bytes, as the request's head
/// arrives, where the reply's was reserved at its own head's length as it was given, and the
/// budget's spare buffers go round in another order: one allocation and 322 bytes more,
/// reallocations 2,050 both. On Linux 33,545, derived as before and not yet counted there.
/// 35,075 since the cell's nodes hold the budget a node derives, its floor (the cell at rest's
/// count), where they held a receive chunk a peer of a budget stated by hand: each datagram the
/// clients' run carries to a node is copied into a buffer of its own, and no window grows past
/// QUIC's initial window: 1,581 allocations and 1,764,056 bytes more (17,562,009), reallocations
/// 2,051. With the state held apart, the hand-stated budget's windows of 64,870 bytes had held no
/// chunk either, and its run counted 35,255, carrying 7,021 datagrams where the floor's windows
/// carry 6,873. On Linux 35,126, this Mac's and the 51 its sockets' batched headers add, derived
/// and not yet counted there: the floor's windows are this Mac's.
#[test]
fn a_cell_serving_writes_turns_without_remaking_what_a_turn_keeps() {
    const SERVING: u64 = if cfg!(target_os = "linux") {
        35_126
    } else {
        35_075
    };
    let counts = serving(1, 40);
    assert_eq!(counts.allocations, SERVING, "{counts:?}");
}
