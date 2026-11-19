//! What the transmit side of an endpoint holding thousands of connections costs a turn and a
//! datagram: `cargo bench --manifest-path vendor/hyper-transport/Cargo.toml --bench transmit`
//! (docs/benchmarks.md, "transmit").
//!
//! One server endpoint holds a connection from each of `CLIENTS` client endpoints, joined by an
//! in-memory wire that delivers each datagram the instant it is sent. Each turn, `ASKING` of the
//! clients, a different run of them each turn, sends the server a request with a 16-byte head and
//! no body, and the server answers each with a 16-byte head. What is timed is the server's
//! transmit side alone, as a driver's turn runs it (hyper-rt-transport `Driver::send`, `flush`):
//! `poll_transmit` until it has nothing, `poll_timeout`, and `handle_timeout` when a timer is due,
//! after the turn's datagrams arrived and after its replies were given. The clients' work, the
//! server's receive side and its events are outside the time. The clock moves `STEP` a turn.
//!
//! Printed as a row of docs/benchmarks.md's table: the median turn's time, the time a tenth of the
//! turns came under, the mean, the datagrams a turn, and the median turn's time a datagram. Each
//! turn is timed alone, so a turn another process on the host lengthened is an outlier the median
//! leaves out.

#![allow(
    clippy::unwrap_in_result,
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::string_slice,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    missing_docs,
    dead_code
)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::*;
use hyper_transport::tls::Tls;
use hyper_transport::{Config, Endpoint, Event, Fixed, Limits, PeerId, Progress, RECEIVE_CHUNK};

/// Client connections the server holds.
const CLIENTS: usize = 2_048;
/// Clients that ask a turn: the run of them moves on by this each turn.
const ASKING: usize = 512;
/// Turns timed, after `WARM` untimed.
const TURNS: usize = 400;
const WARM: usize = 40;
/// What the clock moves a turn.
const STEP: Duration = Duration::from_micros(200);
const HEAD: [u8; 16] = [7; 16];

fn address(peer: PeerId) -> SocketAddr {
    SocketAddr::from(([10, 0, (peer >> 8) as u8, peer as u8], 4433))
}

fn peer_at(address: SocketAddr) -> PeerId {
    match address.ip() {
        std::net::IpAddr::V4(ip) => {
            let [_, _, high, low] = ip.octets();
            (PeerId::from(high) << 8) | PeerId::from(low)
        }
        std::net::IpAddr::V6(_) => panic!("only v4 addresses here"),
    }
}

fn node(issued: &Issued, peer: PeerId, book: Book, limits: Limits, now: Instant) -> Node<Mantle> {
    let (certificate, key) = &issued.issued[(peer - 1) as usize];
    let config = Config {
        tls: Tls::mutual(&credentials(&issued.pki.root, certificate, key), true).unwrap(),
        role: Role::Node,
        limits,
        congestion: Default::default(),
    };
    let mut node =
        Endpoint::new_seeded(config, Fixed::new(1 << 32, 64), book, now, seed_of(peer)).unwrap();
    node.set_default_path_ceiling(u16::try_from(LINK_PAYLOAD).unwrap());
    node
}

struct World {
    now: Instant,
    server: Node<Mantle>,
    clients: Vec<Node<Mantle>>,
    /// Datagrams in flight: from, to, bytes.
    wire: VecDeque<(SocketAddr, SocketAddr, Vec<u8>)>,
    out: Vec<u8>,
    /// The server's transmit side: time and datagrams.
    took: Duration,
    datagrams: u64,
}

impl World {
    /// The server's transmit side, timed: what it sends until it has nothing, its next deadline,
    /// and its timers due.
    #[allow(
        clippy::disallowed_methods,
        reason = "a benchmark measures real time on the host"
    )]
    fn server_sends(&mut self, timed: bool) {
        let began = Instant::now();
        let mut sent = 0u64;
        loop {
            while let Some(transmit) = self.server.poll_transmit(self.now, &mut self.out) {
                self.wire.push_back((
                    address(1),
                    transmit.destination,
                    self.out[..transmit.size].to_vec(),
                ));
                sent += 1;
            }
            if self
                .server
                .poll_timeout()
                .is_some_and(|due| due <= self.now)
            {
                self.server.handle_timeout(self.now);
                continue;
            }
            break;
        }
        let took = began.elapsed();
        if timed {
            self.took += took;
            self.datagrams += sent;
        }
    }

    fn clients_send(&mut self) {
        for (at, client) in self.clients.iter_mut().enumerate() {
            let from = address(at as PeerId + 2);
            while let Some(transmit) = client.poll_transmit(self.now, &mut self.out) {
                self.wire.push_back((
                    from,
                    transmit.destination,
                    self.out[..transmit.size].to_vec(),
                ));
            }
            if client.poll_timeout().is_some_and(|due| due <= self.now) {
                client.handle_timeout(self.now);
                while let Some(transmit) = client.poll_transmit(self.now, &mut self.out) {
                    self.wire.push_back((
                        from,
                        transmit.destination,
                        self.out[..transmit.size].to_vec(),
                    ));
                }
            }
        }
    }

    /// Delivers every datagram in flight; whether any was.
    fn deliver(&mut self) -> bool {
        let moved = !self.wire.is_empty();
        while let Some((from, to, bytes)) = self.wire.pop_front() {
            let peer = peer_at(to);
            if peer == 1 {
                self.server.handle_datagram(self.now, from, None, &bytes);
            } else {
                self.clients[(peer - 2) as usize].handle_datagram(self.now, from, None, &bytes);
            }
        }
        moved
    }

    /// The server answers every request it has.
    fn serve(&mut self) {
        while let Some(event) = self.server.poll_event() {
            if let Event::Request { exchange, .. } = event {
                self.server.reply(exchange, &HEAD, None).unwrap();
                self.server.end(exchange);
            }
        }
    }

    /// The clients take their replies.
    fn take_replies(&mut self) -> usize {
        let mut replies = 0;
        for client in &mut self.clients {
            while let Some(event) = client.poll_event() {
                if let Event::Reply { exchange, .. } = event {
                    client.end(exchange);
                    replies += 1;
                }
            }
        }
        replies
    }

    /// One turn: the asking clients' requests reach the server, it answers, its replies reach
    /// the clients, and their acknowledgements come back, until the wire is quiet.
    fn turn(&mut self, first: usize, timed: bool) -> usize {
        self.now += STEP;
        let progress = Progress::new(Duration::from_secs(10)).unwrap();
        for at in 0..ASKING {
            let client = &mut self.clients[(first + at) % CLIENTS];
            client
                .open(self.now, 1, Kind::Get, &HEAD, None, progress)
                .unwrap();
        }
        let mut replies = 0;
        for _ in 0..64 {
            self.clients_send();
            self.deliver();
            self.serve();
            self.server_sends(timed);
            let moved = self.deliver();
            replies += self.take_replies();
            if !moved && self.wire.is_empty() {
                break;
            }
        }
        replies
    }
}

#[allow(
    clippy::disallowed_methods,
    reason = "a benchmark measures real time on the host"
)]
fn main() {
    let issued = Issued::new(CLIENTS as PeerId + 1);
    let now = Instant::now();
    let mut wide = limits();
    wide.admission.pending = CLIENTS;
    wide.admission.identities = CLIENTS + 1;
    wide.admission.connections = CLIENTS + 1;
    wide.exchanges = CLIENTS * 4;
    let server = node(&issued, 1, issued.book(), wide, now);
    let mut narrow = limits();
    narrow.window_ceiling = 1 << 20;
    narrow.receive_chunks = (1 << 20) / RECEIVE_CHUNK + 1;
    let clients = (0..CLIENTS)
        .map(|at| {
            // A client knows the server alone.
            let mut book = Book::default();
            book.add(&issued.issued[0].0, 1, Role::Node);
            node(&issued, at as PeerId + 2, book, narrow, now)
        })
        .collect();
    let mut world = World {
        now,
        server,
        clients,
        wire: VecDeque::new(),
        out: Vec::with_capacity(1 << 16),
        took: Duration::ZERO,
        datagrams: 0,
    };
    for client in &mut world.clients {
        client.connect(now, 1, address(1)).unwrap();
    }
    let mut connected = 0;
    for _ in 0..1_000 {
        world.clients_send();
        world.deliver();
        while let Some(event) = world.server.poll_event() {
            connected += usize::from(matches!(event, Event::Connected { .. }));
        }
        world.server_sends(false);
        world.deliver();
        for client in &mut world.clients {
            while client.poll_event().is_some() {}
        }
        if connected == CLIENTS && world.wire.is_empty() {
            break;
        }
        world.now += Duration::from_millis(1);
    }
    assert_eq!(connected, CLIENTS, "every client connected");
    let mut first = 0;
    for _ in 0..WARM {
        world.turn(first, false);
        first = (first + ASKING) % CLIENTS;
    }
    let mut replies = 0;
    let mut turns = Vec::with_capacity(TURNS);
    for _ in 0..TURNS {
        let before = world.took;
        replies += world.turn(first, true);
        turns.push(world.took - before);
        first = (first + ASKING) % CLIENTS;
    }
    assert_eq!(replies, TURNS * ASKING, "every request answered");
    turns.sort_unstable();
    let micros = |at: usize| turns[at].as_secs_f64() * 1e6;
    let datagrams = world.datagrams as f64 / TURNS as f64;
    println!(
        "| {CLIENTS} connections, {ASKING} asking a turn | {:.1} µs | {:.1} µs | {:.1} µs | {:.1} | {:.0} ns |",
        micros(TURNS / 2),
        micros(TURNS / 10),
        world.took.as_secs_f64() * 1e6 / TURNS as f64,
        datagrams,
        micros(TURNS / 2) * 1e3 / datagrams,
    );
}
