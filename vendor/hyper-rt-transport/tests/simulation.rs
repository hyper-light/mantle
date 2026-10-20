//! hyper-transport's endpoints and hyper-datagram's plane driven by this crate's `Driver` and
//! `PlaneSocket` on hyper-rt's simulation runtime (docs/runtime.md §11, §14): each node is a task on a
//! simulated shard of one `SimRuntime`, its driver bound through `Driver::bind` and its plane's socket
//! through `PlaneSocket::bind` on the fabric's ports, its endpoint authenticated by mutual TLS and its
//! QUIC layer seeded (`Endpoint::new_seeded`). A node always has a next deadline (its keep-alive, its
//! idle timer, its owner's tick), so `run_until_idle` never returns from a world of them: the test runs
//! the world in slices with `SimRuntime::run_until` and waits on each node's report of what it saw.
//!
//! Two clients, each on a shard of its own, dial a server on a third, over a path of 1 ms ± 0.25 ms
//! one way. Each asks a request with a body and reads its reply, every byte checked by the shared
//! helpers; sends a frame on a lane, which the server echoes; and keys the datagram plane from its
//! connection's TLS exporter, as the server does on its side, and sends a sealed message, which the
//! server echoes. The run is the same from one seed: a second run gives every node's report, the
//! virtual times in it, the clock's end and the fabric's counts alike. TLS draws its own randomness
//! from AWS-LC, so the bytes differ from run to run; their sizes, and so the flow, do not: Ed25519
//! signatures are of one length (`common::Pki::issue`), and both runs present the same certificates,
//! whose bytes move a handshake's sizes since hyper-tls compresses them (RFC 8879).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    dead_code,
    missing_docs
)]

#[path = "../../hyper-transport/tests/common/mod.rs"]
mod common;

use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::mpsc::{Sender, channel};
use std::task::Poll;
use std::time::Duration;

use common::*;
use hyper_datagram::{
    AdmitAll, EXPORTER_LABEL, ExporterSecret, Plane, PlaneLimits, Role as PlaneRole, SECRET_BYTES,
};
use hyper_rt::runtime::RuntimeConfig;
use hyper_rt::sim::{SimFabricStats, SimPath, SimRuntime};
use hyper_rt_transport::{Driver, Io, IoStats, PlaneSocket};
use hyper_transport::{Config, Endpoint, Event, Fixed, PeerId};

type Drv = Driver<Mantle, Fixed, Book>;

/// Datagrams a system call carries: one QUIC initial window (RFC 9002 §7.2).
const IO: Io = Io { batch: 10 };
/// The plane's bounds: the other nodes' epochs, and a replay window of RFC 4303's 64 widened to
/// 1,024 (hyper-tokio's end-to-end test's).
const PLANE: PlaneLimits = PlaneLimits {
    max_peers: 4,
    epochs_per_peer: 2,
    window_limit: 1_024,
};
/// The server is node 2; the clients nodes 1 and 3.
const SERVER: PeerId = 2;
const CLIENTS: [PeerId; 2] = [1, 3];
/// Shape: the owner's own cadence, which every wait races, 1 ms (hyper-tokio's end-to-end test's,
/// tokio's timer granularity): the driver's future is dropped mid-wait at each, and a client sends
/// its sealed message again at each until the echo comes.
const TICK_NS: u64 = 1_000_000;
/// Shape: the slice the test runs the world in, one tick.
const SLICE_NS: u64 = TICK_NS;
/// Shape: the path between every two nodes, 1 ms ± 0.25 ms one way, in order: datagrams in flight
/// across slices' ends, each arrival drawn from the fabric's seeded generator.
const PATH: SimPath = SimPath::in_order(1_000_000, 250_000);
/// Shape: a request's body and its reply's, four of the helpers' 64 KiB pieces: past one stream
/// window's first grant, so flow control moves.
const BODY: u64 = 4 * PIECE as u64;
/// Shape: a frame of 512 bytes, the replication class's.
const FRAME: usize = 512;
/// Shape: a plane message of 48 bytes, a heartbeat's.
const MESSAGE: usize = 48;
/// The period exchanges are judged at.
const PERIOD: Duration = Duration::from_secs(2);
/// The budget each endpoint holds its bytes against.
const BUDGET: u64 = 512 << 20;
/// The seed of the runs.
const SEED: u64 = 1;

/// One shard a node, wheels ticking each microsecond.
fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 3,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 1_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Node `peer`'s endpoint: mutual TLS from `issued`, every other node known, its QUIC layer seeded
/// with the node's own seed, its paths no larger than Ethernet's payload.
fn endpoint(issued: &Issued, peer: PeerId) -> Endpoint<Mantle, Fixed, Book> {
    let (certificate, key) = &issued.issued[(peer - 1) as usize];
    let config = Config {
        tls: hyper_transport::tls::Tls::mutual(
            &credentials(&issued.pki.root, certificate, key),
            true,
        )
        .unwrap(),
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    let now = hyper_sim::Anchor::new().instant(0).unwrap();
    let mut endpoint = Endpoint::new_seeded(
        config,
        Fixed::new(BUDGET, 64),
        issued.book(),
        now,
        seed_of(peer),
    )
    .unwrap();
    endpoint.set_default_path_ceiling(u16::try_from(LINK_PAYLOAD).unwrap());
    endpoint
}

/// A node on its shard: its transport's driver, its plane and the plane's socket.
struct Node {
    peer: PeerId,
    driver: Drv,
    socket: PlaneSocket,
    plane: Plane,
}

impl Node {
    /// Binds `endpoint`'s driver and a plane socket on the current shard's fabric ports.
    fn bind(endpoint: Endpoint<Mantle, Fixed, Book>, peer: PeerId) -> Self {
        let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
        Self {
            peer,
            driver: Driver::bind(endpoint, any, IO).unwrap(),
            socket: PlaneSocket::bind(any, IO).unwrap(),
            plane: Plane::new(peer, PLANE).unwrap(),
        }
    }

    /// Keys the plane for `peer` from the connection's TLS exporter, this node being `role` on it.
    fn key_plane(&mut self, peer: PeerId, role: PlaneRole) {
        let endpoint = self.driver.endpoint();
        let mut secret = [0u8; SECRET_BYTES];
        let epoch = endpoint
            .export_keying_material(peer, EXPORTER_LABEL, b"", &mut secret)
            .unwrap();
        let path = endpoint.path(peer).unwrap();
        self.plane
            .install_epoch(
                peer,
                u32::try_from(epoch).unwrap(),
                &ExporterSecret::new(secret),
                role,
            )
            .unwrap();
        self.plane
            .set_path(peer, usize::from(path.max_datagram))
            .unwrap();
    }

    /// Seals `message` for `peer` and sends it to `to`.
    fn seal(&mut self, peer: PeerId, message: &[u8], to: SocketAddr) {
        self.plane.queue(peer, message).unwrap();
        self.socket.flush(
            &mut self.plane,
            |_| Some(to),
            |peer, refusal| panic!("the plane refused {peer}: {refusal:?}"),
        );
    }
}

/// What a race ended on.
enum Woke {
    Event(Event<Mantle>),
    /// The messages of the datagrams the plane opened: each with its sender and the address its
    /// datagram came from.
    Plane(Vec<(PeerId, SocketAddr, Vec<u8>)>),
    Tick,
}

/// The driver's next event, the plane's next batch, or the owner's tick, whichever comes first; the
/// others' futures are dropped.
async fn next(node: &mut Node) -> Woke {
    let Node {
        driver,
        socket,
        plane,
        ..
    } = node;
    let mut opened = Vec::new();
    let woke = {
        let mut event = pin!(driver.event());
        let mut receive = pin!(socket.receive(plane, &AdmitAll, |arrival, result| {
            if let Ok(datagram) = result {
                for message in datagram.messages() {
                    opened.push((datagram.sender, arrival.from, message.to_vec()));
                }
            }
        }));
        let mut tick = pin!(hyper_rt::futures::sleep(TICK_NS));
        poll_fn(|context| {
            if let Poll::Ready(event) = event.as_mut().poll(context) {
                return Poll::Ready(Some(Woke::Event(event.unwrap())));
            }
            if let Poll::Ready(received) = receive.as_mut().poll(context) {
                received.unwrap();
                return Poll::Ready(None);
            }
            if tick.as_mut().poll(context).is_ready() {
                return Poll::Ready(Some(Woke::Tick));
            }
            Poll::Pending
        })
        .await
    };
    woke.unwrap_or(Woke::Plane(opened))
}

/// What the server held once every client had connected.
#[derive(Debug, PartialEq, Eq)]
struct Held {
    /// When the last client connected, on the virtual clock.
    at_ns: u64,
    /// The connections its endpoint held, and the sockets of their own its driver gave them.
    connections: usize,
    peer_sockets: usize,
}

/// The server: answers every request, echoes every frame on its lane and every sealed message to the
/// address it came from, keying its plane for each peer as the peer connects, and reports what it
/// holds once every client has connected. Runs forever.
async fn serve(mut node: Node, held: Sender<Held>) {
    let mut server = Server::new();
    let mut told = false;
    loop {
        match next(&mut node).await {
            Woke::Event(first) => {
                let mut event = Some(first);
                while let Some(taken) = event.take().or_else(|| node.driver.endpoint().poll_event())
                {
                    if let Event::Connected { peer, .. } = taken {
                        node.key_plane(peer, PlaneRole::Acceptor);
                    }
                    server.on_event(node.driver.endpoint(), taken);
                }
            }
            Woke::Plane(messages) => {
                for (sender, from, message) in messages {
                    node.seal(sender, &message, from);
                }
            }
            Woke::Tick => {}
        }
        server.advance_all(node.driver.endpoint());
        for (peer, lane, frame) in server.frames.drain(..) {
            node.driver
                .endpoint()
                .send_frame(peer, lane, Kind::Append, &frame)
                .unwrap();
        }
        if !told && server.connected.len() == CLIENTS.len() {
            told = true;
            held.send(Held {
                at_ns: hyper_rt::futures::now_ns(),
                connections: node.driver.endpoint().stats().connections,
                peer_sockets: node.driver.peer_sockets(),
            })
            .unwrap();
        }
    }
}

/// What a client saw, sent once it has seen it all.
#[derive(Debug, PartialEq, Eq)]
struct Report {
    peer: PeerId,
    /// When its connection was up, and when it had seen it all, on the virtual clock.
    connected_ns: u64,
    done_ns: u64,
    /// Its exchange's reply length, the bytes of it read, and its request body's bytes written.
    reply: Option<u64>,
    read: u64,
    written: u64,
    /// The frame echoed on its lane, and the sealed message echoed on the plane.
    frame: Vec<u8>,
    message: Vec<u8>,
    /// Sealed datagrams it sent until the echo came: the server keys its plane on its own
    /// `Connected`, which may follow the client's, and drops what it cannot open yet.
    sealed: u32,
    /// What its driver's socket and its plane's socket did.
    io: IoStats,
    plane_io: IoStats,
}

/// A client: dials the server, then asks, sends a frame and seals a message, and reports once its
/// reply is whole and both echoes came. Keeps its connection after.
async fn ask(mut node: Node, server: SocketAddr, server_plane: SocketAddr, report: Sender<Report>) {
    let mut asker = Asker::new();
    let now = node.driver.clock().unwrap();
    node.driver.endpoint().connect(now, SERVER, server).unwrap();
    let frame = vec![u8::try_from(node.peer).unwrap(); FRAME];
    let message = [u8::try_from(node.peer).unwrap(); MESSAGE];
    let mut connected_ns = None;
    let mut echoed = None;
    let mut sealed = 0u32;
    let mut reported = false;
    loop {
        let woke = next(&mut node).await;
        let tick = matches!(woke, Woke::Tick);
        match woke {
            Woke::Event(first) => {
                asker.clock = node.driver.clock().unwrap();
                let mut event = Some(first);
                while let Some(taken) = event.take().or_else(|| node.driver.endpoint().poll_event())
                {
                    asker.on_event(node.driver.endpoint(), taken);
                }
            }
            Woke::Plane(messages) => {
                for (sender, from, message) in messages {
                    assert_eq!((sender, from), (SERVER, server_plane));
                    echoed = Some(message);
                }
            }
            Woke::Tick => {}
        }
        if connected_ns.is_none() && asker.connected.contains(&SERVER) {
            connected_ns = Some(hyper_rt::futures::now_ns());
            node.key_plane(SERVER, PlaneRole::Initiator);
            let now = node.driver.clock().unwrap();
            let seed = u8::try_from(node.peer).unwrap();
            asker
                .ask(
                    node.driver.endpoint(),
                    now,
                    SERVER,
                    (Kind::Put, Class::Request),
                    seed,
                    Some(BODY),
                    PERIOD,
                )
                .unwrap();
            node.driver
                .endpoint()
                .send_frame(SERVER, 0, Kind::Append, &frame)
                .unwrap();
            node.seal(SERVER, &message, server_plane);
            sealed += 1;
        } else if connected_ns.is_some() && echoed.is_none() && tick {
            node.seal(SERVER, &message, server_plane);
            sealed += 1;
        }
        asker.advance_all(node.driver.endpoint());
        let done = asker.finished() && !asker.frames.is_empty() && echoed.is_some();
        if done && !reported {
            reported = true;
            let asked = &asker.asked[0];
            report
                .send(Report {
                    peer: node.peer,
                    connected_ns: connected_ns.unwrap(),
                    done_ns: hyper_rt::futures::now_ns(),
                    reply: asked.reply,
                    read: asked.read,
                    written: asked.written,
                    frame: asker.frames[0].1.clone(),
                    message: echoed.clone().unwrap(),
                    sealed,
                    io: node.driver.stats(),
                    plane_io: node.socket.stats(),
                })
                .unwrap();
        }
    }
}

/// What one run saw: every client's report, what the server held, the clock when the last report came
/// in, and the fabric's counts.
#[derive(Debug, PartialEq, Eq)]
struct Run {
    reports: Vec<Report>,
    held: Held,
    ended_ns: u64,
    fabric: SimFabricStats,
}

/// One run from `seed`: the server bound at time zero, the clients spawned on it, the world run a
/// slice at a time until every client reported, within the endpoints' idle timeout. Each node is the
/// future `spawn_on` takes, which is `Send`: its plane's receive among its awaits.
fn run(seed: u64, issued: &Issued) -> Run {
    let mut sim = SimRuntime::new(&config(), seed).unwrap();
    sim.set_path(PATH);
    let shards = sim.shard_ids();
    let (bound, addresses) = channel();
    let (holds, held) = channel();
    let server = endpoint(issued, SERVER);
    sim.spawn_on(shards[0], async move {
        let node = Node::bind(server, SERVER);
        bound
            .send((
                node.driver.local_addr().unwrap(),
                node.socket.local_addr().unwrap(),
            ))
            .unwrap();
        serve(node, holds).await;
    })
    .unwrap();
    sim.run_until(0);
    let (server_address, server_plane) = addresses.try_recv().expect("the server bound at once");
    let (reported, reports) = channel();
    for (shard, peer) in shards[1..].iter().zip(CLIENTS) {
        let endpoint = endpoint(issued, peer);
        let reported = reported.clone();
        sim.spawn_on(*shard, async move {
            let node = Node::bind(endpoint, peer);
            ask(node, server_address, server_plane, reported).await;
        })
        .unwrap();
    }
    let guard = u64::try_from(limits().idle_timeout.as_nanos()).unwrap();
    let mut seen = Vec::new();
    while seen.len() < CLIENTS.len() {
        assert!(
            sim.now_ns() < guard,
            "{} of {} clients reported within the idle timeout: {seen:?}",
            seen.len(),
            CLIENTS.len()
        );
        sim.run_until(sim.now_ns() + SLICE_NS);
        seen.extend(reports.try_iter());
    }
    seen.sort_by_key(|report| report.peer);
    Run {
        reports: seen,
        held: held.try_recv().expect("the server held both clients"),
        ended_ns: sim.now_ns(),
        fabric: sim.stats(),
    }
}

/// Do: run three nodes on three simulated shards, two clients dialing a server, each client asking a
/// request of 256 KiB, sending a frame and sealing a message; then run them again from the same seed,
/// and once from another. Expect: each client's reply whole and checked, its frame and its message
/// echoed; the server's listener holding both connections with no socket of their own (the fabric's
/// ports are one socket's each); every datagram the fabric carried delivered, none dropped; the second
/// run the first (every report, its virtual times, the clock's end and the fabric's counts); and the
/// other seed's draws another run.
#[test]
fn nodes_on_simulated_shards_exchange_requests_frames_and_sealed_datagrams_alike_from_one_seed() {
    let issued = Issued::new(3);
    let first = run(SEED, &issued);
    assert_eq!(
        (first.held.connections, first.held.peer_sockets),
        (CLIENTS.len(), 0),
        "{:?}",
        first.held
    );
    for report in &first.reports {
        assert_eq!(report.reply, Some(BODY), "{report:?}");
        assert_eq!((report.read, report.written), (BODY, BODY), "{report:?}");
        assert_eq!(
            report.frame,
            vec![u8::try_from(report.peer).unwrap(); FRAME]
        );
        assert_eq!(
            report.message,
            vec![u8::try_from(report.peer).unwrap(); MESSAGE]
        );
        assert!(report.connected_ns < report.done_ns, "{report:?}");
    }
    let fabric = first.fabric;
    assert_eq!(
        fabric,
        SimFabricStats {
            delivered: fabric.delivered,
            ..SimFabricStats::default()
        },
        "nothing dropped on the path"
    );
    let second = run(SEED, &issued);
    assert_eq!(second, first, "one seed, one run");
    let other = run(SEED + 1, &issued);
    assert_ne!(
        other
            .reports
            .iter()
            .map(|report| report.done_ns)
            .collect::<Vec<_>>(),
        first
            .reports
            .iter()
            .map(|report| report.done_ns)
            .collect::<Vec<_>>(),
        "another seed draws other delays"
    );
    for report in &first.reports {
        eprintln!(
            "node {}: connected at {} ns, done at {} ns; {} sealed; transport {} datagrams sent, {} \
             received; plane {} sent, {} received",
            report.peer,
            report.connected_ns,
            report.done_ns,
            report.sealed,
            report.io.sent,
            report.io.received,
            report.plane_io.sent,
            report.plane_io.received
        );
    }
    eprintln!("ended at {} ns; {:?}", first.ended_ns, first.fabric);
}
