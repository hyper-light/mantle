//! What the tests, the end-to-end scenarios and the benchmarks share: a certificate authority, a
//! project's classes (mantle's: control, request, bulk), a directory, and an in-memory network
//! that drives two endpoints on the caller's clock.

#![allow(
    clippy::unwrap_in_result,
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::string_slice,
    clippy::cast_possible_truncation,
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    missing_docs,
    unreachable_pub
)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{
    AdmissionLimits, Authenticated, Budget, Classes, Config, Directory, Endpoint, Event, Fixed,
    Limits, PeerId, Refusal, Transmit,
};

/// The deployment's authority, issuing one certificate per node.
pub struct Pki {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    pub root: Vec<u8>,
}

impl Pki {
    pub fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let root = params.self_signed(&key).unwrap().der().to_vec();
        Self {
            issuer: rcgen::Issuer::new(params, key),
            root,
        }
    }
    /// A certificate for `name` and its PKCS #8 key, both DER: Ed25519, whose signatures are of
    /// one length (RFC 8032 §5.1.6), where ECDSA's DER encoding is 70 to 72 bytes and moved a
    /// handshake's sizes from one run to the next.
    pub fn issue(&self, name: &str) -> (Vec<u8>, Vec<u8>) {
        let params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let certificate = params.signed_by(&key, &self.issuer).unwrap();
        (certificate.der().to_vec(), key.serialize_der())
    }
}

pub fn credentials(root: &[u8], certificate: &[u8], key: &[u8]) -> Credentials {
    Credentials {
        chain: vec![CertificateDer::from(certificate.to_vec())],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.to_vec())),
        roots: vec![CertificateDer::from(root.to_vec())],
    }
}

/// The name a node's certificate carries.
pub fn name(peer: PeerId) -> String {
    format!("node-{peer}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    Control,
    Request,
    Bulk,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Vote,
    Get,
    Put,
    Snapshot,
    Append,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Node,
    Client,
}

const KINDS: [Kind; 5] = [
    Kind::Vote,
    Kind::Get,
    Kind::Put,
    Kind::Snapshot,
    Kind::Append,
];

fn kind_code(kind: Kind) -> u16 {
    KINDS.iter().position(|known| *known == kind).unwrap() as u16 + 1
}

fn kind_of(code: u16) -> Option<Kind> {
    KINDS.get(usize::from(code).checked_sub(1)?).copied()
}

fn rank(class: Class) -> u8 {
    match class {
        Class::Control => 0,
        Class::Request => 1,
        Class::Bulk => 2,
    }
}

/// mantle's classes: a vote and an append are control and only a node sends them; a get and a put
/// are requests from anyone; a snapshot is bulk and only a node sends it.
pub struct Mantle;

/// The frame bounds of [`Mantle`].
pub const CONTROL_BOUND: u64 = 1 << 20;
pub const REQUEST_BOUND: u64 = 64 << 20;
pub const BULK_BOUND: u64 = 1 << 30;

fn mantle_class(kind: Kind, role: Role) -> Option<Class> {
    match (kind, role) {
        (Kind::Vote | Kind::Append, Role::Node) => Some(Class::Control),
        (Kind::Get | Kind::Put, _) => Some(Class::Request),
        (Kind::Snapshot, Role::Node) => Some(Class::Bulk),
        _ => None,
    }
}

impl Classes for Mantle {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;
    const RANKS: u8 = 3;
    fn rank(class: Class) -> u8 {
        rank(class)
    }
    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        mantle_class(kind, role)
    }
    fn frame_bound(class: Class) -> u64 {
        match class {
            Class::Control => CONTROL_BOUND,
            Class::Request => REQUEST_BOUND,
            Class::Bulk => BULK_BOUND,
        }
    }
    fn kind_code(kind: Kind) -> u16 {
        kind_code(kind)
    }
    fn kind_of(code: u16) -> Option<Kind> {
        kind_of(code)
    }
}

/// Where an [`Embedding`] append carries its own CRC-32C: of its bytes from 8 on, stated
/// little-endian in its first four (a bus batch's shape).
pub const EMBEDDED: hyper_transport::Embedded = hyper_transport::Embedded {
    covered_from: 8,
    at: 0,
};

/// [`Mantle`] whose appends carry their own checksum ([`Classes::embedded_checksum`]).
pub struct Embedding;

impl Classes for Embedding {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;
    const RANKS: u8 = 3;
    fn rank(class: Class) -> u8 {
        rank(class)
    }
    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        mantle_class(kind, role)
    }
    fn frame_bound(class: Class) -> u64 {
        Mantle::frame_bound(class)
    }
    fn kind_code(kind: Kind) -> u16 {
        kind_code(kind)
    }
    fn kind_of(code: u16) -> Option<Kind> {
        kind_of(code)
    }
    fn embedded_checksum(kind: Kind) -> Option<hyper_transport::Embedded> {
        (kind == Kind::Append).then_some(EMBEDDED)
    }
}

/// [`Mantle`] with a request bound of [`STRICT_REQUEST_BOUND`]: a receiver whose bound is below its
/// sender's.
pub struct Strict;
pub const STRICT_REQUEST_BOUND: u64 = 64 << 10;

impl Classes for Strict {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;
    const RANKS: u8 = 3;
    fn rank(class: Class) -> u8 {
        rank(class)
    }
    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        mantle_class(kind, role)
    }
    fn frame_bound(class: Class) -> u64 {
        match class {
            Class::Request => STRICT_REQUEST_BOUND,
            other => Mantle::frame_bound(other),
        }
    }
    fn kind_code(kind: Kind) -> u16 {
        kind_code(kind)
    }
    fn kind_of(code: u16) -> Option<Kind> {
        kind_of(code)
    }
}

/// [`Mantle`] that lets a client send a snapshot: a sender that believes a role may send what its
/// receiver says it may not.
pub struct Lax;

impl Classes for Lax {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;
    const RANKS: u8 = 3;
    fn rank(class: Class) -> u8 {
        rank(class)
    }
    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        match (kind, role) {
            (Kind::Snapshot, Role::Client) => Some(Class::Bulk),
            _ => mantle_class(kind, role),
        }
    }
    fn frame_bound(class: Class) -> u64 {
        Mantle::frame_bound(class)
    }
    fn kind_code(kind: Kind) -> u16 {
        kind_code(kind)
    }
    fn kind_of(code: u16) -> Option<Kind> {
        kind_of(code)
    }
}

/// [`Mantle`] whose every role shares the stream pool ([`hyper_transport::StreamCredit::Pooled`]).
pub struct Pooled;

impl Classes for Pooled {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;
    const RANKS: u8 = 3;
    fn rank(class: Class) -> u8 {
        rank(class)
    }
    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        mantle_class(kind, role)
    }
    fn frame_bound(class: Class) -> u64 {
        Mantle::frame_bound(class)
    }
    fn kind_code(kind: Kind) -> u16 {
        kind_code(kind)
    }
    fn kind_of(code: u16) -> Option<Kind> {
        kind_of(code)
    }
    fn pooled(_role: Role) -> bool {
        true
    }
}

/// Which certificate is which peer.
#[derive(Default)]
pub struct Book {
    entries: Vec<(Vec<u8>, PeerId, Role)>,
    names: Vec<(PeerId, String)>,
}

impl Book {
    pub fn add(&mut self, certificate: &[u8], peer: PeerId, role: Role) {
        self.entries.push((certificate.to_vec(), peer, role));
        self.names.push((peer, name(peer)));
    }
}

impl Directory for Book {
    type Role = Role;
    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Role)> {
        let certificate = authenticated.certificate?;
        self.entries
            .iter()
            .find(|(known, _, _)| known == certificate)
            .map(|(_, peer, role)| (*peer, *role))
    }
    fn server_name(&self, peer: PeerId) -> Option<&str> {
        self.names
            .iter()
            .find(|(known, _)| *known == peer)
            .map(|(_, name)| name.as_str())
    }
}

/// Bounds for the tests: small enough to reach, large enough to work.
pub fn limits() -> Limits {
    Limits {
        admission: AdmissionLimits {
            pending: 8,
            identities: 8,
            connections: 16,
            per_identity: 2,
        },
        streams_per_connection: 16,
        stream_credit: hyper_transport::StreamCredit::Fixed,
        ask_queue: hyper_transport::AskQueue::Table,
        exchanges: 64,
        lanes_per_peer: 4,
        lane_window: 128,
        max_head: 16 << 10,
        window_ceiling: 16 << 20,
        serve_period: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(10),
        keep_alive: Some(Duration::from_secs(2)),
        max_responses: 16,
        // A full 16 MiB window of unread data, packed: 16 MiB / 65,527 B, and one.
        receive_chunks: (16 << 20) / hyper_transport::RECEIVE_CHUNK + 1,
    }
}

/// What a driver needs of an endpoint, whatever its classes.
pub trait Drive {
    fn datagram(&mut self, now: Instant, from: SocketAddr, bytes: &[u8]);
    fn transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> Option<Transmit>;
    fn timeout(&mut self) -> Option<Instant>;
    fn fire(&mut self, now: Instant);
    /// The driver measured its timer granularity: what has a use for it takes it.
    fn granularity(&mut self, _granularity: Duration) {}
    /// What the scheduling core read that differed from its fold, with the oracle on
    /// (hyper-transport src/tally.rs); a driver without one has none.
    #[cfg(feature = "oracle")]
    fn mismatches(&self) -> u64 {
        0
    }
}

impl<C: Classes, B: Budget<C::Class>, D: Directory<Role = C::Role>> Drive for Endpoint<C, B, D> {
    fn datagram(&mut self, now: Instant, from: SocketAddr, bytes: &[u8]) {
        self.handle_datagram(now, from, None, bytes);
    }
    fn transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> Option<Transmit> {
        self.poll_transmit(now, out)
    }
    fn timeout(&mut self) -> Option<Instant> {
        self.poll_timeout()
    }
    fn fire(&mut self, now: Instant) {
        self.handle_timeout(now);
    }
    #[cfg(feature = "oracle")]
    fn mismatches(&self) -> u64 {
        self.oracle_mismatches()
    }
    fn granularity(&mut self, granularity: Duration) {
        self.set_granularity(granularity);
    }
}

/// A node of the in-memory network.
pub type Node<C> = Endpoint<C, Fixed, Book>;

/// What the network schedules on the world: a datagram's arrival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arrival {
    Datagram(hyper_sim::net::Ticket),
}

/// Two endpoints, `a` and `b`, joined by hyper-sim's network on its world (`docs/sim.md` §3.4):
/// node 0 is `a`, node 1 is `b`, the zero path between them, so a datagram arrives the instant it
/// is sent and in the order sent, and time moves only to the next timer when nothing is in flight.
/// The world's virtual time is the caller's clock from the instant it gave. A datagram's buffer is
/// reused, so the network allocates nothing once warm.
pub struct Net<A: Drive, B: Drive> {
    pub now: Instant,
    pub a: A,
    pub b: B,
    pub a_address: SocketAddr,
    pub b_address: SocketAddr,
    /// Whether datagrams to `b` are lost: `b` is gone.
    pub b_dead: bool,
    /// The datagrams each side has sent, `a`'s then `b`'s.
    pub datagrams: [u64; 2],
    /// The datagrams of each side's that were past [`LINK_PAYLOAD`] and dropped on the wire.
    pub oversized: [u64; 2],
    /// The caller's instant at the world's time zero.
    start: Instant,
    world: hyper_sim::World<Arrival>,
    wire: hyper_sim::net::Net<Vec<u8>>,
    spare: Vec<Vec<u8>>,
    out: Vec<u8>,
}

/// Format: the most a datagram carries on the links these tests model: Ethernet's MTU (1,500,
/// IEEE 802.3) less IPv4's 20-byte header (RFC 791) and UDP's 8 (RFC 768). A datagram past it is
/// dropped on the wire, as a router drops a packet past its next hop's MTU that may not be
/// fragmented (RFC 8899 §3), and a connected node is told it as its driver reads it from the
/// kernel (`Endpoint::set_path_ceiling`).
pub const LINK_PAYLOAD: usize = 1_472;

/// `a` and `b` as nodes of the world.
const A: hyper_sim::NodeId = hyper_sim::NodeId(0);
const B: hyper_sim::NodeId = hyper_sim::NodeId(1);

impl<A: Drive, B: Drive> Net<A, B> {
    pub fn new(now: Instant, a: A, b: B) -> Self {
        Self::of(now, a, b, hyper_sim::Source::Seed(0))
    }

    /// `b`'s datagrams to `a` through a bottleneck of their own, as `shaped` puts `a`'s.
    pub fn shape_back(&mut self, rate_bits_per_second: u64, queue_bytes: u64, one_way: Duration) {
        let link = self
            .wire
            .add_link(hyper_sim::net::Link::drop_tail(
                rate_bits_per_second,
                queue_bytes,
            ))
            .unwrap();
        let one_way = u64::try_from(one_way.as_nanos()).unwrap();
        self.wire
            .set_pair_path(
                B,
                A,
                hyper_sim::net::Path::in_order(one_way, 0).through(link),
            )
            .unwrap();
    }

    /// The network with `a`'s datagrams to `b` through a bottleneck that serializes
    /// `rate_bits_per_second` and holds `queue_bytes`, `one_way` beyond it, and `b`'s to `a` in
    /// `one_way`: what `a` sends waits in the queue after it has left `a`.
    pub fn shaped(
        now: Instant,
        a: A,
        b: B,
        rate_bits_per_second: u64,
        queue_bytes: u64,
        one_way: Duration,
    ) -> Self {
        let mut net = Self::with_links(now, a, b, hyper_sim::Source::Seed(0), 2);
        let one_way = u64::try_from(one_way.as_nanos()).unwrap();
        let link = net
            .wire
            .add_link(hyper_sim::net::Link::drop_tail(
                rate_bits_per_second,
                queue_bytes,
            ))
            .unwrap();
        net.wire
            .set_pair_path(
                A,
                B,
                hyper_sim::net::Path::in_order(one_way, 0).through(link),
            )
            .unwrap();
        net.wire
            .set_pair_path(B, A, hyper_sim::net::Path::in_order(one_way, 0))
            .unwrap();
        net
    }

    /// The network with its world's decisions drawn from `source`: the zero path draws none, and
    /// the run-twice check ([`twice_on`]) holds it to that.
    pub fn of(now: Instant, a: A, b: B, source: hyper_sim::Source) -> Self {
        Self::with_links(now, a, b, source, 0)
    }

    fn with_links(now: Instant, a: A, b: B, source: hyper_sim::Source, links: usize) -> Self {
        let limits = hyper_sim::Limits {
            events: 1 << 12,
            nodes: 2,
            streams: 8,
            steps: u64::MAX,
            trace_words: 0,
        };
        let mut world =
            hyper_sim::World::new(source, hyper_sim::Discipline::Ordered, limits).unwrap();
        world.node(hyper_sim::Clock::default()).unwrap();
        world.node(hyper_sim::Clock::default()).unwrap();
        let wire = hyper_sim::net::Net::new(hyper_sim::net::NetLimits {
            flows: 2,
            links,
            nats: 0,
            link_messages: if links == 0 { 0 } else { 1 << 12 },
            messages: 1 << 12,
            bytes: 1 << 26,
        });
        Self {
            now,
            a,
            b,
            a_address: "127.0.0.1:4433".parse().unwrap(),
            b_address: "127.0.0.1:4434".parse().unwrap(),
            b_dead: false,
            datagrams: [0; 2],
            oversized: [0; 2],
            start: now,
            world,
            wire,
            spare: Vec::with_capacity(64),
            out: Vec::with_capacity(1500),
        }
    }

    /// Hands every datagram `side` has to the network; whether there was any.
    fn send_all(&mut self, from_a: bool) -> bool {
        let mut any = false;
        loop {
            let transmit = if from_a {
                self.a.transmit(self.now, &mut self.out)
            } else {
                self.b.transmit(self.now, &mut self.out)
            };
            let Some(transmit) = transmit else {
                return any;
            };
            self.datagrams[usize::from(!from_a)] += 1;
            if transmit.size > LINK_PAYLOAD {
                self.oversized[usize::from(!from_a)] += 1;
                any = true;
                continue;
            }
            let mut bytes = self.spare.pop().unwrap_or_default();
            bytes.clear();
            bytes.extend_from_slice(&self.out[..transmit.size]);
            let pair = if from_a { (A, B) } else { (B, A) };
            self.wire
                .send(
                    &mut self.world,
                    pair,
                    bytes,
                    transmit.size,
                    Arrival::Datagram,
                )
                .unwrap();
            any = true;
        }
    }

    /// Delivers every datagram due now, in the order sent; whether there was any.
    fn deliver_due(&mut self) -> bool {
        let mut any = false;
        while self
            .world
            .earliest()
            .is_some_and(|due| due <= self.world.now())
        {
            let hyper_sim::Step::Event {
                node,
                event: Arrival::Datagram(ticket),
            } = self.world.next(&mut hyper_sim::Fifo).unwrap()
            else {
                panic!("only datagrams are scheduled");
            };
            let delivery = self
                .wire
                .deliver(&mut self.world, ticket, Arrival::Datagram)
                .unwrap()
                .unwrap();
            if node == B {
                if !self.b_dead {
                    self.b.datagram(self.now, self.a_address, &delivery.payload);
                }
            } else {
                self.a.datagram(self.now, self.b_address, &delivery.payload);
            }
            self.spare.push(delivery.payload);
            any = true;
        }
        any
    }

    /// Moves every datagram until neither side has any; returns whether any moved.
    pub fn exchange(&mut self) -> bool {
        let mut moved = false;
        for _ in 0..1_000_000 {
            let sent_a = self.send_all(true);
            let sent_b = self.send_all(false);
            let delivered = self.deliver_due();
            if !(sent_a || sent_b || delivered) {
                return moved;
            }
            moved = true;
        }
        panic!("the network never went quiet");
    }

    /// Neither node's scheduling core read anything that differed from the fold it replaced
    /// (hyper-transport src/tally.rs): checked whenever a run's condition holds.
    fn oracle_holds(&self) {
        #[cfg(feature = "oracle")]
        for (side, mismatches) in [("a", self.a.mismatches()), ("b", self.b.mismatches())] {
            assert_eq!(
                mismatches, 0,
                "node {side}: the core read something that differed from its fold"
            );
        }
    }

    /// Moves the world's clock to the caller's instant `to`, which nothing in flight precedes.
    fn move_to(&mut self, to: Instant) {
        let at = u64::try_from(to.saturating_duration_since(self.start).as_nanos()).unwrap();
        self.world.advance(at.max(self.world.now())).unwrap();
        self.now = self.now.max(to);
    }

    /// When the earliest datagram in flight arrives, by the caller's clock: what a move of the
    /// clock may not pass.
    fn in_flight(&self) -> Option<Instant> {
        self.world
            .earliest()
            .map(|at| self.start + Duration::from_nanos(at))
    }

    /// Fires the earliest timer of either side, or delivers the earliest datagram in flight,
    /// moving the clock to it.
    pub fn advance(&mut self) -> bool {
        let next = [
            self.a.timeout(),
            if self.b_dead { None } else { self.b.timeout() },
            self.in_flight(),
        ]
        .into_iter()
        .flatten()
        .min();
        let Some(next) = next else {
            return false;
        };
        self.move_to(next);
        self.a.fire(self.now);
        if !self.b_dead {
            self.b.fire(self.now);
        }
        true
    }

    /// Moves the clock to the earliest timer of either side, or by `most` if that comes first: an
    /// owner that acts at least every `most` sees time pass no faster than that.
    pub fn advance_within(&mut self, most: Duration) {
        let next = [
            self.a.timeout(),
            if self.b_dead { None } else { self.b.timeout() },
            self.in_flight(),
        ]
        .into_iter()
        .flatten()
        .min()
        .map_or(self.now + most, |next| next.min(self.now + most));
        self.move_to(next);
        self.a.fire(self.now);
        if !self.b_dead {
            self.b.fire(self.now);
        }
    }

    /// The run's record: the digest of every arrival the world ordered, and its trace.
    pub fn finish(self) -> hyper_sim::Record {
        self.world.finish()
    }

    /// Runs the network until `done` holds, exchanging datagrams and firing timers; `done` is
    /// asked after every exchange, so it can poll events and act on them. Panics after `turns`
    /// turns of the clock: a test's bound on a protocol that did not end.
    pub fn until(&mut self, turns: usize, mut done: impl FnMut(&mut Self) -> bool) {
        for _ in 0..turns {
            self.exchange();
            if done(self) {
                self.oracle_holds();
                return;
            }
            if !self.exchange() && !self.advance() {
                if done(self) {
                    self.oracle_holds();
                    return;
                }
                panic!("nothing is left to happen");
            }
        }
        panic!("the condition never held in {turns} turns");
    }
}

/// `scenario` through hyper-sim's run-twice check (docs/sim.md §3.9): twice from `seed` and once
/// from the first run's trace, each on the network `scenario` builds from the source it is given,
/// to one digest. The endpoints' keys and nonces are their own draws, outside the world, so this
/// also holds that what the network orders does not depend on them.
pub fn twice_on<A: Drive, B: Drive>(
    seed: u64,
    mut scenario: impl FnMut(hyper_sim::Source) -> Net<A, B>,
) -> hyper_sim::Record {
    hyper_sim::twice(seed, |source| {
        Ok::<_, std::convert::Infallible>(scenario(source).finish())
    })
    .unwrap()
}

/// The parts of two nodes that know each other: node 1 a client of node 2, or both nodes.
pub struct Pair {
    pub pki: Pki,
    pub one: (Vec<u8>, Vec<u8>),
    pub two: (Vec<u8>, Vec<u8>),
}

impl Pair {
    pub fn new() -> Self {
        let pki = Pki::new();
        let one = pki.issue(&name(1));
        let two = pki.issue(&name(2));
        Self { pki, one, two }
    }
    pub fn book(&self, one: Role, two: Role) -> Book {
        let mut book = Book::default();
        book.add(&self.one.0, 1, one);
        book.add(&self.two.0, 2, two);
        book
    }
    pub fn node<C: Classes<Role = Role>>(
        &self,
        which: PeerId,
        role: Role,
        book: Book,
        limits: Limits,
        budget: u64,
        now: Instant,
    ) -> Node<C> {
        let (certificate, key) = if which == 1 { &self.one } else { &self.two };
        let config = Config {
            tls: Tls::mutual(&credentials(&self.pki.root, certificate, key), true).unwrap(),
            role,
            limits,
            congestion: Default::default(),
        };
        let mut node =
            Endpoint::new_seeded(config, Fixed::new(budget, 64), book, now, seed_of(which))
                .unwrap();
        node.set_default_path_ceiling(u16::try_from(LINK_PAYLOAD).unwrap());
        node
    }
}

/// The QUIC layer's seed for node `peer` of the tests: its own, the same in every run, so a run's
/// handshakes take the same sizes from one run to the next (`Endpoint::new_seeded`).
pub fn seed_of(peer: PeerId) -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&peer.to_le_bytes());
    seed
}

/// Every event of `endpoint` so far.
pub fn events<C: Classes, B: Budget<C::Class>, D: Directory<Role = C::Role>>(
    endpoint: &mut Endpoint<C, B, D>,
) -> Vec<Event<C>> {
    std::iter::from_fn(|| endpoint.poll_event()).collect()
}

/// The byte at `offset` of a body seeded `seed`: what every body in the tests is made of, so that
/// a receiver checks every byte without holding the body.
pub fn pattern(seed: u64, offset: u64) -> u8 {
    (offset.wrapping_mul(31).wrapping_add(seed) % 251) as u8
}

/// Fills `into` with the pattern from `offset`.
pub fn fill(seed: u64, offset: u64, into: &mut [u8]) {
    for (at, byte) in into.iter_mut().enumerate() {
        *byte = pattern(seed, offset + at as u64);
    }
}

/// The piece the tests read a body in, and write one in.
pub const PIECE: usize = 64 << 10;

/// One exchange as the side that answers it sees it.
#[derive(Debug)]
struct Served {
    exchange: hyper_transport::ExchangeId,
    class: Class,
    seed: u64,
    /// The request body read so far, and its length.
    read: u64,
    body: u64,
    replied: bool,
    /// The reply body written so far, and its length.
    written: u64,
    reply: u64,
    /// When the request was whole, by the server's `now`.
    whole: Option<Instant>,
}

/// A node that answers every request: it reads the request's body, checking every byte, and
/// replies with a head of `ok:` and the request's head, and a body as long as the request's.
/// Bulk bodies are left unread while `hold_bulk` is set.
pub struct Server {
    served: Vec<Served>,
    pub hold_bulk: bool,
    /// How long the server takes to answer once it has a request whole, by `now`.
    pub think: Duration,
    /// A request body longer than this is never read (0: every body is read): a peer that holds
    /// a stream it accepted, its bytes in its buffer and none of its credit given back.
    pub unread_above: u64,
    /// The request bodies the server reads, by `now`: `bytes` at the start of each `every` (none:
    /// all that has arrived, as it arrives). A peer that takes what it is sent at its own pace and
    /// in its own steps, the credit it gives back coming in those steps.
    pub pace: Option<(u64, Duration)>,
    /// When the paced reads began, by `now`, and what they have read since.
    paced: Option<(Instant, u64)>,
    /// The clock the think time is read against, as the test last set it.
    pub now: Option<Instant>,
    pub answered: u64,
    pub frames: Vec<(PeerId, u32, Vec<u8>)>,
    pub connected: Vec<PeerId>,
    /// Each connection that closed, and why when either side said.
    pub closed: Vec<(PeerId, Option<Refusal>)>,
    pub refused: Vec<(hyper_transport::Refusal, bool)>,
    piece: Vec<u8>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            served: Vec::new(),
            hold_bulk: false,
            think: Duration::ZERO,
            unread_above: 0,
            pace: None,
            paced: None,
            now: None,
            answered: 0,
            frames: Vec::new(),
            connected: Vec::new(),
            closed: Vec::new(),
            refused: Vec::new(),
            piece: vec![0; PIECE],
        }
    }

    /// Handles every event of `node` and moves every exchange as far as it goes.
    pub fn serve<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
    ) {
        while let Some(event) = node.poll_event() {
            self.on_event(node, event);
        }
        self.advance_all(node);
    }

    /// Handles one event of `node`; [`Server::advance_all`] then moves the exchanges on.
    pub fn on_event<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
        event: Event<C>,
    ) {
        match event {
            Event::Request {
                exchange,
                class,
                body,
                ..
            } => {
                let head = node.head(exchange).unwrap();
                let seed = u64::from(head.first().copied().unwrap_or(0));
                if seed == u64::from(RELEASE) {
                    self.hold_bulk = false;
                }
                self.served.push(Served {
                    exchange,
                    class,
                    seed,
                    read: 0,
                    body: body.unwrap_or(0),
                    replied: false,
                    written: 0,
                    reply: body.unwrap_or(0),
                    whole: None,
                });
            }
            Event::Frame {
                peer, lane, frame, ..
            } => {
                self.frames.push((peer, lane, frame.bytes().to_vec()));
                node.release(frame);
            }
            Event::Connected { peer, .. } => self.connected.push(peer),
            Event::Closed { peer, refusal, .. } => self.closed.push((peer, refusal)),
            Event::Refused {
                exchange,
                refusal,
                by_peer,
            } => {
                self.refused.push((refusal, by_peer));
                self.served.retain(|served| served.exchange != exchange);
            }
            _ => {}
        }
    }

    /// Moves every exchange as far as it goes.
    pub fn advance_all<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
    ) {
        let mut served = std::mem::take(&mut self.served);
        served.retain_mut(|exchange| !self.advance(node, exchange));
        self.served = served;
    }

    /// The most of a body the server reads now: a piece, or what its pace has left of it.
    fn piece(&mut self) -> u64 {
        let (Some((bytes, every)), Some(now)) = (self.pace, self.now) else {
            return PIECE as u64;
        };
        let (from, read) = *self.paced.get_or_insert((now, 0));
        let steps = u64::try_from(now.duration_since(from).as_nanos() / every.as_nanos()).unwrap();
        (bytes * (steps + 1)).saturating_sub(read).min(PIECE as u64)
    }

    /// What a paced read took, counted against the pace.
    fn paced_read(&mut self, got: &Result<usize, Refusal>) {
        if let (Some(_), Ok(got), Some((_, read))) = (self.pace, got, self.paced.as_mut()) {
            *read += *got as u64;
        }
    }

    /// Moves one exchange on; whether it is done.
    fn advance<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
        served: &mut Served,
    ) -> bool {
        if served.class == Class::Bulk && self.hold_bulk {
            return false;
        }
        if self.unread_above > 0 && served.body > self.unread_above {
            return false;
        }
        while served.read < served.body {
            let piece = self.piece();
            if piece == 0 {
                return false;
            }
            let Ok(mut into) = node.reserve(served.class, piece) else {
                return false;
            };
            let got = node.read_body(served.exchange, &mut into);
            self.paced_read(&got);
            for (at, byte) in into.bytes().iter().enumerate() {
                assert_eq!(
                    *byte,
                    pattern(served.seed, served.read + at as u64),
                    "a request byte"
                );
            }
            node.release(into);
            match got {
                Ok(0) => return false,
                Ok(got) => served.read += got as u64,
                Err(_) => return true,
            }
        }
        if !node.body_complete(served.exchange) && served.body > 0 {
            let mut empty = hyper_transport::Reservation::default();
            let _ = node.read_body(served.exchange, &mut empty);
            if !node.body_complete(served.exchange) {
                return false;
            }
        }
        if !served.replied && !self.think.is_zero() {
            // The request is whole: the answer is given once the server has thought.
            let Some(now) = self.now else {
                return false;
            };
            let whole = *served.whole.get_or_insert(now);
            if now < whole + self.think {
                return false;
            }
        }
        if !served.replied {
            let mut head = b"ok:".to_vec();
            head.extend_from_slice(node.head(served.exchange).unwrap_or(&[]));
            let body = (served.reply > 0 || served.body > 0).then_some(served.reply);
            if node.reply(served.exchange, &head, body).is_err() {
                return true;
            }
            served.replied = true;
        }
        while served.written < served.reply {
            let length = (served.reply - served.written).min(PIECE as u64) as usize;
            fill(served.seed, served.written, &mut self.piece[..length]);
            match node.write_body(served.exchange, &self.piece[..length]) {
                Ok(0) => return false,
                Ok(took) => served.written += took as u64,
                Err(_) => return true,
            }
        }
        node.end(served.exchange);
        self.answered += 1;
        true
    }
}

/// One exchange as the side that asked sees it.
#[derive(Debug, Clone)]
pub struct Asked {
    pub exchange: hyper_transport::ExchangeId,
    pub class: Class,
    pub seed: u64,
    pub head: Vec<u8>,
    pub written: u64,
    pub body: u64,
    pub reply: Option<u64>,
    pub read: u64,
    pub done: bool,
    pub refused: Option<(hyper_transport::Refusal, bool)>,
    /// A body write was refused for want of credit.
    pub blocked: bool,
    /// When the reply's head arrived, and when the exchange was opened, by the driver's clock.
    pub opened: Option<Instant>,
    pub answered: Option<Instant>,
}

/// A node that asks: it writes request bodies as credit allows, reads replies, checking every
/// byte, and ends each exchange once its reply is whole.
pub struct Asker {
    pub asked: Vec<Asked>,
    pub frames: Vec<(u32, Vec<u8>)>,
    /// The driver's clock, as it last said.
    pub clock: Instant,
    pub events: Vec<String>,
    pub connected: Vec<PeerId>,
    /// Each dial that failed, and why when either side said.
    pub unreachable: Vec<(PeerId, Option<Refusal>)>,
    /// Each connection that closed, and why when either side said.
    pub closed: Vec<(PeerId, Option<Refusal>)>,
    piece: Vec<u8>,
}

impl Asker {
    pub fn new() -> Self {
        Self {
            asked: Vec::new(),
            frames: Vec::new(),
            clock: hyper_sim::Anchor::new().instant(0).unwrap(),
            events: Vec::new(),
            connected: Vec::new(),
            unreachable: Vec::new(),
            closed: Vec::new(),
            piece: vec![0; PIECE],
        }
    }

    /// Asks `peer` a `kind` with a head starting with `seed` and a body of `body` bytes.
    pub fn ask<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
        now: Instant,
        peer: PeerId,
        (kind, class): (Kind, Class),
        seed: u8,
        body: Option<u64>,
        period: Duration,
    ) -> Result<usize, hyper_transport::Refusal> {
        let head = vec![seed, 1, 2, 3];
        let progress = hyper_transport::Progress::new(period).unwrap();
        let exchange = node.open(now, peer, kind, &head, body, progress)?;
        self.asked.push(Asked {
            exchange,
            class,
            seed: u64::from(seed),
            head,
            written: 0,
            body: body.unwrap_or(0),
            reply: None,
            read: 0,
            done: false,
            refused: None,
            blocked: false,
            opened: Some(now),
            answered: None,
        });
        Ok(self.asked.len() - 1)
    }

    pub fn drive<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
    ) {
        while let Some(event) = node.poll_event() {
            self.on_event(node, event);
        }
        self.advance_all(node);
    }

    /// Handles one event of `node`; [`Asker::advance_all`] then moves the exchanges on.
    pub fn on_event<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
        event: Event<C>,
    ) {
        self.events.push(format!("{event:?}"));
        match event {
            Event::Reply { exchange, body } => {
                if let Some(asked) = self
                    .asked
                    .iter_mut()
                    .find(|asked| asked.exchange == exchange)
                {
                    asked.reply = Some(body.unwrap_or(0));
                    asked.answered = Some(self.clock);
                    let mut head = b"ok:".to_vec();
                    head.extend_from_slice(&asked.head);
                    assert_eq!(node.head(exchange), Some(&head[..]), "the reply's head");
                }
            }
            Event::Refused {
                exchange,
                refusal,
                by_peer,
            } => {
                if let Some(asked) = self
                    .asked
                    .iter_mut()
                    .find(|asked| asked.exchange == exchange)
                {
                    asked.refused = Some((refusal, by_peer));
                    asked.done = true;
                }
            }
            Event::Connected { peer, .. } => self.connected.push(peer),
            Event::Unreachable { peer, refusal } => self.unreachable.push((peer, refusal)),
            Event::Frame { lane, frame, .. } => {
                self.frames.push((lane, frame.bytes().to_vec()));
                node.release(frame);
            }
            Event::Closed { peer, refusal, .. } => self.closed.push((peer, refusal)),
            _ => {}
        }
    }

    /// Moves every exchange as far as it goes.
    pub fn advance_all<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
    ) {
        for at in 0..self.asked.len() {
            self.advance(node, at);
        }
    }

    fn advance<C: Classes<Kind = Kind, Class = Class, Role = Role>>(
        &mut self,
        node: &mut Node<C>,
        at: usize,
    ) {
        let asked = &mut self.asked[at];
        if asked.done {
            return;
        }
        while asked.written < asked.body {
            let length = (asked.body - asked.written).min(PIECE as u64) as usize;
            fill(asked.seed, asked.written, &mut self.piece[..length]);
            match node.write_body(asked.exchange, &self.piece[..length]) {
                Ok(0) => {
                    asked.blocked = true;
                    break;
                }
                Err(_) => break,
                Ok(took) => asked.written += took as u64,
            }
        }
        let Some(reply) = asked.reply else {
            return;
        };
        while asked.read < reply {
            let Ok(mut into) = node.reserve(asked.class, PIECE as u64) else {
                return;
            };
            let got = node.read_body(asked.exchange, &mut into);
            for (offset, byte) in into.bytes().iter().enumerate() {
                assert_eq!(
                    *byte,
                    pattern(asked.seed, asked.read + offset as u64),
                    "a reply byte"
                );
            }
            node.release(into);
            match got {
                Ok(0) | Err(_) => return,
                Ok(got) => asked.read += got as u64,
            }
        }
        if reply > 0 && !node.body_complete(asked.exchange) {
            let mut empty = hyper_transport::Reservation::default();
            let _ = node.read_body(asked.exchange, &mut empty);
            if !node.body_complete(asked.exchange) {
                return;
            }
        }
        if asked.written == asked.body {
            node.end(asked.exchange);
            asked.done = true;
        }
    }

    pub fn finished(&self) -> bool {
        self.asked.iter().all(|asked| asked.done)
    }
}

/// The seed of a request that releases a [`Server`]'s held bulk bodies.
pub const RELEASE: u8 = 255;

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
        .collect()
}

/// A certificate for each of nodes `1..=n` from one authority: made once for every run of a
/// scenario.
pub struct Issued {
    pub pki: Pki,
    pub issued: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Issued {
    pub fn new(count: PeerId) -> Self {
        let pki = Pki::new();
        let issued = (1..=count).map(|peer| pki.issue(&name(peer))).collect();
        Self { pki, issued }
    }

    /// The book every node of these credentials knows the others by, each a node.
    pub fn book(&self) -> Book {
        let mut book = Book::default();
        for (at, (certificate, _)) in self.issued.iter().enumerate() {
            book.add(certificate, at as PeerId + 1, Role::Node);
        }
        book
    }
}

/// The address of node `peer` of a [`Mesh`].
pub fn mesh_address(peer: PeerId) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 4432 + peer as u16))
}

fn mesh_peer(address: SocketAddr) -> PeerId {
    PeerId::from(address.port() - 4432)
}

/// Nodes `1..=n` on hyper-sim's network, node `i` at world node `i - 1`, every path the zero one: a
/// datagram arrives the instant it is sent, in the order sent; [`Net`] for more than two.
pub struct Mesh<N: Drive> {
    pub now: Instant,
    start: Instant,
    pub nodes: Vec<N>,
    world: hyper_sim::World<Arrival>,
    wire: hyper_sim::net::Net<Vec<u8>>,
    out: Vec<u8>,
}

impl<N: Drive> Mesh<N> {
    /// `nodes`, made at `now`, on a world drawing from `source`.
    pub fn new(nodes: Vec<N>, now: Instant, source: hyper_sim::Source) -> Self {
        let count = nodes.len();
        let limits = hyper_sim::Limits {
            events: 1 << 12,
            nodes: count,
            streams: 8,
            steps: u64::MAX,
            trace_words: 0,
        };
        let mut world =
            hyper_sim::World::new(source, hyper_sim::Discipline::Ordered, limits).unwrap();
        for _ in 0..count {
            world.node(hyper_sim::Clock::default()).unwrap();
        }
        let wire = hyper_sim::net::Net::new(hyper_sim::net::NetLimits {
            flows: count * count,
            links: 0,
            nats: 0,
            link_messages: 0,
            messages: 1 << 12,
            bytes: 1 << 26,
        });
        Self {
            now,
            start: now,
            nodes,
            world,
            wire,
            out: Vec::with_capacity(1500),
        }
    }

    /// The run's record: the digest of every arrival the world ordered.
    pub fn finish(self) -> hyper_sim::Record {
        self.world.finish()
    }

    pub fn node(&mut self, peer: PeerId) -> &mut N {
        &mut self.nodes[(peer - 1) as usize]
    }

    /// Moves every datagram until no node has any.
    pub fn exchange(&mut self) {
        for _ in 0..1_000_000 {
            let mut moved = false;
            for at in 0..self.nodes.len() {
                while let Some(transmit) = self.nodes[at].transmit(self.now, &mut self.out) {
                    let to = mesh_peer(transmit.destination) - 1;
                    let bytes = self.out[..transmit.size].to_vec();
                    self.wire
                        .send(
                            &mut self.world,
                            (hyper_sim::NodeId(at as u32), hyper_sim::NodeId(to as u32)),
                            bytes,
                            transmit.size,
                            Arrival::Datagram,
                        )
                        .unwrap();
                    moved = true;
                }
            }
            while self
                .world
                .earliest()
                .is_some_and(|due| due <= self.world.now())
            {
                let hyper_sim::Step::Event {
                    event: Arrival::Datagram(ticket),
                    ..
                } = self.world.next(&mut hyper_sim::Fifo).unwrap()
                else {
                    panic!("only datagrams are scheduled");
                };
                let delivery = self
                    .wire
                    .deliver(&mut self.world, ticket, Arrival::Datagram)
                    .unwrap()
                    .unwrap();
                let from = mesh_address(delivery.from.0 as PeerId + 1);
                self.nodes[delivery.to.0 as usize].datagram(self.now, from, &delivery.payload);
                moved = true;
            }
            if !moved {
                return;
            }
        }
        panic!("the network never went quiet");
    }

    /// Moves the clock to the earliest timer of any node, or to `limit` if that comes first, and
    /// fires every node's timers due.
    pub fn advance_until(&mut self, limit: Option<Instant>) {
        let timer = self
            .nodes
            .iter_mut()
            .filter_map(|node| node.timeout())
            .min();
        let next = match (timer, limit) {
            (Some(timer), Some(limit)) => timer.min(limit),
            (Some(timer), None) => timer,
            (None, Some(limit)) => limit,
            (None, None) => panic!("a node keeps a timer: its keep-alive"),
        };
        let at = u64::try_from(next.saturating_duration_since(self.start).as_nanos()).unwrap();
        self.world.advance(at.max(self.world.now())).unwrap();
        self.now = self.now.max(next);
        for node in &mut self.nodes {
            node.fire(self.now);
        }
    }

    /// Moves the clock to the earliest timer of any node and fires every node's timers.
    pub fn advance(&mut self) {
        self.advance_until(None);
    }
}
