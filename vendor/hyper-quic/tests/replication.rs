//! A leader's appends to one follower and the follower's answers, over one hyper-quic connection at
//! a distance that loses datagrams, on hyper-sim's network in virtual time.
//!
//! The leader writes each append, as it falls due, to one ordered stream, and the follower answers
//! each append it has read whole on one ordered stream of its own: the lanes a Raft leader and its
//! follower keep (focal's bulk lane for appends, its urgent lane for their answers). A datagram
//! lost on either stream holds every later append or answer behind it until the loss is repaired,
//! and an append counts from when it fell due to when its answer is read, so what the loss costs
//! the appends behind it shows in their answers' times.
//!
//! The load is focal's replication as its compare runs measured it (80 ms one way between nodes,
//! 3% of each node's datagrams lost, its leader sending each of two followers about 110 appends
//! of 7.5 KB a second): at that rate a 3% loss is about twenty a second on each stream, each
//! holding the stream a round trip until it is retransmitted, so most appends wait on a repair,
//! and the commit, the first follower's answer, took 1.8 times the loss-free path's at the median.
//! The connections leave BBR's startup first, filling a bottleneck with a catch-up, as focal's do:
//! an application-limited flow never ends its startup, and in startup BBR ignores its recovery. A
//! follower still catching up is sent none of the appends falling due meanwhile, and a run is
//! measured from when the leader has read every follower's answer to its catch-up: appends written
//! behind it measured its drain, and at 10% loss, where the code's repairs and retransmissions take
//! half as much of the bottleneck again, the drain outlasted a fixed ten-second warm-up (the coded
//! commit's 99th percentile 1.1 to 1.3 s from ten seconds, 258 to 294 ms from fifteen).
//!
//! A run is its seed's: the world orders every arrival and timer, the appends fall due at times
//! drawn from the seed, and the endpoints' keys and nonces change no size and no time.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    missing_docs
)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use hyper_quic::congestion::{BbrConfig, Congestion};
use hyper_quic::rustls::RootCertStore;
use hyper_quic::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use hyper_quic::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig,
    Event, ServerConfig, StreamId, TransportConfig, VarInt,
};
use hyper_sim::net::{Link, Loss, Net, NetLimits, Path, Ticket};
use hyper_sim::{
    Clock, Discipline, Fifo, Limits, NodeId, Record, SimError, Source, Step, World, twice,
};

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;
/// The endpoints' datagrams with path MTU discovery off: RFC 9000 §14's smallest maximum.
const DATAGRAM: u64 = 1_200;
/// The smallest datagram an endpoint sends (`congestion.rs`).
const SMALLEST_DATAGRAM: u64 = 21;
/// An append's header: its length, its number and when it fell due.
const APPEND_HEADER: usize = 4 + 8 + 8;
/// An answer: the append's number.
const ANSWER_HEADER: usize = 8;

#[derive(Clone, Copy, Debug)]
struct Scenario {
    one_way_ns: u64,
    /// What each direction loses, parts per million, independently.
    loss_ppm: u32,
    /// Appends falling due a second, as a Poisson process.
    appends_per_second: u64,
    append_bytes: usize,
    answer_bytes: usize,
    /// The followers, each its own connection from the leader; an append commits once the first
    /// answers, a group of three's majority with the leader.
    followers: usize,
    /// The forward erasure correction's window, both endpoints' (`TransportConfig::fec_window`).
    fec_window: Option<u32>,
    /// Whether the endpoints search the path's MTU (RFC 8899), as focal's do: their datagrams grow
    /// past RFC 9000's 1,200 bytes, which a pacer or a window cuts a datagram back to.
    mtud: bool,
    /// Whether a transmit's datagrams are lost together, as netem loses a segmented send (it
    /// decides each socket buffer's loss before it segments one, `net/sched/sch_netem.c`'s
    /// `netem_enqueue`), the loss focal's compare runs apply; else each is lost on its own.
    batch_loss: bool,
    /// The datagrams a connection makes at once, each but the last its first's size (GSO): what
    /// focal's driver asks for (hyper-rt-transport's `SEGMENTS_A_TRANSMIT`, Linux's
    /// `UDP_MAX_SEGMENTS`), whether its socket segments them or sends them one a call.
    segments: usize,
    /// Each direction's bottleneck, bits a second, with a queue of one bandwidth-delay product:
    /// none, or a link a bulk transfer fills, so BBR measures the path and leaves its startup.
    rate_bits_per_second: Option<u64>,
    /// What the leader sends a follower first, as one append: a catch-up that fills the path.
    catch_up_bytes: usize,
    seconds: u64,
    seed: u64,
}

impl Scenario {
    fn end(&self) -> u64 {
        self.seconds * SECOND
    }

    /// The bytes one direction of one connection holds at most: its bottleneck's queue, what has
    /// left the link and propagates, and a datagram on the wire (`congestion.rs`'s
    /// `bytes_one_way`); with no bottleneck, all the run sends.
    fn bytes_one_way(&self) -> u64 {
        match self.rate_bits_per_second {
            Some(rate) => {
                let bits = u128::from(rate) * u128::from(self.one_way_ns) / u128::from(SECOND);
                u64::try_from(bits / 8).unwrap() + DATAGRAM + self.queue_bytes()
            }
            None => {
                let appends = self.appends_per_second * self.seconds;
                appends * (self.append_bytes + self.answer_bytes) as u64
                    + self.catch_up_bytes as u64
            }
        }
    }

    /// The messages every direction holds at once, at the smallest datagram's size.
    fn messages(&self) -> usize {
        let directions = 2 * self.followers as u64;
        usize::try_from(directions * self.bytes_one_way() / SMALLEST_DATAGRAM + 1).unwrap()
    }

    /// The decisions a run records: a loss draw per datagram, on a path that loses, and the
    /// datagrams each direction no more than its bottleneck carries over the run at the smallest
    /// size, beside what it holds at the start (`congestion.rs`'s `trace_words`).
    fn trace_words(&self) -> usize {
        if self.loss_ppm == 0 {
            return 0;
        }
        let carried = match self.rate_bits_per_second {
            Some(rate) => u128::from(rate) * u128::from(self.end()) / (8 * u128::from(SECOND)),
            None => u128::from(self.bytes_one_way()),
        };
        let datagrams =
            (u64::try_from(carried).unwrap() + self.bytes_one_way()) / SMALLEST_DATAGRAM;
        usize::try_from(2 * self.followers as u64 * datagrams).unwrap()
    }

    /// A bottleneck's queue: one bandwidth-delay product, and four datagrams at least.
    fn queue_bytes(&self) -> u64 {
        self.rate_bits_per_second.map_or(0, |rate| {
            let bits = u128::from(rate) * u128::from(2 * self.one_way_ns) / u128::from(SECOND);
            u64::try_from(bits / 8).unwrap().max(4 * DATAGRAM)
        })
    }

    fn name(&self) -> String {
        format!(
            "{} ms one way, {:.1}% loss, {}/s of {} B, {} followers{}{}{}, seed {}",
            self.one_way_ns / MS,
            f64::from(self.loss_ppm) / 10_000.0,
            self.appends_per_second,
            self.append_bytes,
            self.followers,
            self.fec_window
                .map_or_else(String::new, |window| format!(", coded over {window}")),
            self.rate_bits_per_second
                .map_or_else(String::new, |rate| format!(", {} Mbit/s", rate / 1_000_000)),
            if self.batch_loss {
                ", each transmit lost whole"
            } else {
                ""
            },
            self.seed
        )
    }
}

/// focal's replication (the module's documentation): two followers, each sent 110 appends of
/// 7.5 KB a second over 80 ms one way, answered by 64 bytes each, the endpoints searching the
/// path's MTU and making their datagrams in batches of 64 as focal's do, behind a 20 Mbit/s bottleneck
/// (three times the appends' 6.6 Mbit/s) that an 8 MB catch-up fills first, long enough for BBR
/// to find the bottleneck's rate; for 25 seconds, measured from the catch-ups' answers to two
/// seconds before the end.
fn replication(loss_ppm: u32, fec_window: Option<u32>, seed: u64) -> Scenario {
    Scenario {
        one_way_ns: 80 * MS,
        loss_ppm,
        appends_per_second: 110,
        append_bytes: 7_500,
        answer_bytes: 64,
        followers: 2,
        fec_window,
        mtud: true,
        batch_loss: false,
        segments: 64,
        rate_bits_per_second: Some(20_000_000),
        catch_up_bytes: 8_000_000,
        seconds: 25,
        seed,
    }
}

/// When each append falls due: a Poisson process at the scenario's rate from the seed (xorshift64*,
/// Vigna 2016), from the run's first second to its end.
fn schedule(scenario: &Scenario) -> Vec<u64> {
    let mut state = scenario.seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };
    let mean = SECOND as f64 / scenario.appends_per_second as f64;
    let mut due = Vec::new();
    let mut at = SECOND as f64;
    while at < scenario.end() as f64 {
        due.push(at as u64);
        let uniform = ((next() >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
        at += -uniform.ln() * mean;
    }
    due
}

struct Pki {
    certificate: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}

impl Pki {
    /// The same Ed25519 identity every run (`congestion.rs`'s `Pki::new`).
    fn new() -> Self {
        let mut pkcs8 = vec![
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
            0x04, 0x20,
        ];
        pkcs8.extend_from_slice(&[0x5a; 32]);
        let signing_key =
            rcgen::KeyPair::try_from(&PrivatePkcs8KeyDer::from(pkcs8.clone())).unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.serial_number = Some(rcgen::SerialNumber::from(0x0123_4567_89ab_cdef_u64));
        let cert = params.self_signed(&signing_key).unwrap();
        Self {
            certificate: cert.der().clone(),
            key: PrivatePkcs8KeyDer::from(pkcs8).into(),
        }
    }
}

/// BBR, focal's law, and flow control that never binds: the leader's appends are held only by the
/// law and the network.
fn transport(scenario: &Scenario) -> TransportConfig {
    let window = 64 * 1024 * 1024;
    let mut transport = TransportConfig::default();
    transport
        .stream_receive_window(VarInt::from_u64(window).unwrap())
        .receive_window(VarInt::from_u64(window).unwrap())
        .send_window(window)
        .mtu_discovery_config(scenario.mtud.then(hyper_quic::MtuDiscoveryConfig::default))
        .fec_window(scenario.fec_window)
        .congestion(Congestion::Bbr(BbrConfig::default()));
    transport
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    Arrive(Ticket),
    End,
}

struct Side {
    node: NodeId,
    address: SocketAddr,
    endpoint: Endpoint,
    connection: Option<(ConnectionHandle, Connection)>,
    /// The stream this side writes, once open, and what it has not yet taken.
    out: Option<StreamId>,
    unsent: Vec<u8>,
    /// The stream this side reads, once its peer opened it, and what is read but not yet whole.
    inbound: Option<StreamId>,
    partial: Vec<u8>,
}

impl Side {
    fn new(node: NodeId, address: SocketAddr, endpoint: Endpoint) -> Self {
        Self {
            node,
            address,
            endpoint,
            connection: None,
            out: None,
            unsent: Vec::new(),
            inbound: None,
            partial: Vec::new(),
        }
    }

    /// Writes what this side has not yet written to its stream, opening it first.
    fn flush(&mut self) {
        let Some((_, connection)) = &mut self.connection else {
            return;
        };
        let id = match self.out {
            Some(id) => id,
            None => {
                let Some(id) = connection.streams().open(Dir::Uni) else {
                    return;
                };
                self.out = Some(id);
                id
            }
        };
        let mut sent = 0;
        while sent < self.unsent.len() {
            match connection.send_stream(id).write(&self.unsent[sent..]) {
                Ok(written) if written > 0 => sent += written,
                _ => break,
            }
        }
        self.unsent.drain(..sent);
    }

    /// Everything the peer wrote that this side has read, after its partial bytes.
    fn read(&mut self) {
        let Some((_, connection)) = &mut self.connection else {
            return;
        };
        while let Some(id) = connection.streams().accept(Dir::Uni) {
            self.inbound = Some(id);
        }
        let Some(id) = self.inbound else {
            return;
        };
        let mut stream = connection.recv_stream(id);
        let Ok(mut chunks) = stream.read(true) else {
            return;
        };
        while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
            self.partial.extend_from_slice(&chunk.bytes);
        }
        let _ = chunks.finalize();
    }
}

/// The leader's connection to one follower: nodes `2k` (the leader's side) and `2k + 1`.
struct Pair {
    leader: Side,
    follower: Side,
    connected: bool,
    /// Whether the catch-up was written.
    caught_up: bool,
    /// When the leader read the follower's answer to its catch-up: the appends falling due before
    /// are not written.
    answered_catch_up: Option<u64>,
    /// The next append to write.
    next: usize,
    /// The leader's packets lost and sources and repairs sent, and the follower's rebuilt and
    /// left to retransmission, when the measurement began.
    at_start: Option<Counts>,
    /// When each append's answer was read, and when the follower read it whole.
    answered: Vec<Option<u64>>,
    delivered: Vec<Option<u64>>,
}

/// What a run measured.
#[derive(Clone, Debug, Default)]
struct Measured {
    /// Each append due once measured: from due to its first answer read (its commit),
    /// nanoseconds, sorted.
    committed: Vec<u64>,
    /// From due to the first follower's answer of each, per follower, sorted.
    answered: Vec<Vec<u64>>,
    /// From due to read whole by the first follower.
    delivered: Vec<u64>,
    /// Appends due once measured and not committed by the end.
    uncommitted: usize,
    /// What the leaders and followers counted once measured.
    counts: Counts,
    closed: Option<String>,
}

/// The leaders' packets lost and the code's sources and repairs, and what the followers rebuilt
/// and left to retransmission.
#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    lost: u64,
    sources: u64,
    repairs: u64,
    /// The leaders' repairs dropped, too large for a datagram they could send.
    dropped: u64,
    rebuilt: u64,
    left: u64,
}

impl Counts {
    fn of(pair: &Pair) -> Self {
        let mut counts = Self::default();
        if let Some((_, connection)) = &pair.leader.connection {
            let stats = connection.stats();
            counts.lost = stats.path.lost_packets;
            counts.sources = stats.frame_tx.fec_source;
            counts.repairs = stats.frame_tx.fec_repair;
            counts.dropped = stats.fec_repairs_dropped;
        }
        if let Some((_, connection)) = &pair.follower.connection {
            let stats = connection.stats();
            counts.rebuilt = stats.fec_rebuilt;
            counts.left = stats.fec_unrebuilt;
        }
        counts
    }

    fn since(self, then: Self) -> Self {
        Self {
            lost: self.lost - then.lost,
            sources: self.sources - then.sources,
            repairs: self.repairs - then.repairs,
            dropped: self.dropped - then.dropped,
            rebuilt: self.rebuilt - then.rebuilt,
            left: self.left - then.left,
        }
    }

    fn add(&mut self, other: Self) {
        self.lost += other.lost;
        self.sources += other.sources;
        self.repairs += other.repairs;
        self.dropped += other.dropped;
        self.rebuilt += other.rebuilt;
        self.left += other.left;
    }
}

impl Measured {
    fn at(sorted: &[u64], per_mille: usize) -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        let rank = (sorted.len() * per_mille)
            .div_ceil(1_000)
            .clamp(1, sorted.len());
        sorted[rank - 1]
    }
}

struct Run {
    scenario: Scenario,
    epoch: Instant,
    world: World<Ev>,
    net: Net<Vec<u8>>,
    pairs: Vec<Pair>,
    /// When each append falls due, by number.
    due: Vec<u64>,
    /// When the leader had read every follower's answer to its catch-up: the run is measured from
    /// then.
    start: Option<u64>,
    closed: Option<String>,
    scratch: Vec<u8>,
}

impl Run {
    fn new(scenario: Scenario, pki: &Pki, source: Source) -> Self {
        let nodes = 2 * scenario.followers;
        let messages = scenario.messages();
        let limits = Limits {
            // Each message in flight's arrival, and the run's end
            events: messages + 1,
            nodes,
            // The run's own, and each direction of each connection its loss and its delay draws
            // (`congestion.rs`)
            streams: 3 * nodes + 1,
            steps: u64::MAX,
            trace_words: scenario.trace_words(),
        };
        let mut world = World::new(source, Discipline::Ordered, limits).unwrap();
        for _ in 0..nodes {
            world.node(Clock::default()).unwrap();
        }
        let epoch = world.instant(NodeId(0)).unwrap();
        let mut net = Net::new(NetLimits {
            flows: nodes,
            links: nodes,
            nats: 0,
            link_messages: usize::try_from(scenario.queue_bytes() / SMALLEST_DATAGRAM + 1).unwrap(),
            messages,
            bytes: messages * DATAGRAM as usize,
        });
        let path =
            Path::in_order(scenario.one_way_ns, 0).with_loss(Loss::random(scenario.loss_ppm));
        let through = |net: &mut Net<Vec<u8>>| match scenario.rate_bits_per_second {
            Some(rate) => path.through(
                net.add_link(Link::drop_tail(rate, scenario.queue_bytes()))
                    .unwrap(),
            ),
            None => path,
        };
        let rng = |node: NodeId| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&scenario.seed.to_le_bytes());
            bytes[8..12].copy_from_slice(&node.0.to_le_bytes());
            Some(bytes)
        };
        let due = schedule(&scenario);
        let mut pairs = Vec::with_capacity(scenario.followers);
        for index in 0..scenario.followers {
            let (leader, follower) = (
                NodeId(2 * u32::try_from(index).unwrap()),
                NodeId(2 * u32::try_from(index).unwrap() + 1),
            );
            let (out, back) = (through(&mut net), through(&mut net));
            net.set_pair_path(leader, follower, out).unwrap();
            net.set_pair_path(follower, leader, back).unwrap();
            let mut server_config =
                ServerConfig::with_single_cert(vec![pki.certificate.clone()], pki.key.clone_key())
                    .unwrap();
            server_config.transport_config(transport(&scenario));
            pairs.push(Pair {
                leader: Side::new(
                    leader,
                    format!("10.0.{index}.1:4433").parse().unwrap(),
                    Endpoint::new(EndpointConfig::default(), None, scenario.mtud, rng(leader))
                        .unwrap(),
                ),
                follower: Side::new(
                    follower,
                    format!("10.0.{index}.2:4433").parse().unwrap(),
                    Endpoint::new(
                        EndpointConfig::default(),
                        Some(server_config),
                        scenario.mtud,
                        rng(follower),
                    )
                    .unwrap(),
                ),
                connected: false,
                caught_up: false,
                answered_catch_up: None,
                next: 0,
                at_start: None,
                answered: vec![None; due.len()],
                delivered: vec![None; due.len()],
            });
        }
        Self {
            scenario,
            epoch,
            world,
            net,
            pairs,
            due,
            start: None,
            closed: None,
            scratch: Vec::with_capacity(2 * DATAGRAM as usize),
        }
    }

    fn at(&self, now: u64) -> Instant {
        self.epoch + Duration::from_nanos(now)
    }

    /// The pair `node` belongs to, and whether it is the leader's side.
    fn locate(node: NodeId) -> (usize, bool) {
        (
            usize::try_from(node.0 / 2).unwrap(),
            node.0.is_multiple_of(2),
        )
    }

    fn connect(&mut self, pki: &Pki) {
        for index in 0..self.pairs.len() {
            let mut roots = RootCertStore::empty();
            roots.add(pki.certificate.clone()).unwrap();
            let mut config = ClientConfig::with_root_certificates(roots).unwrap();
            config.transport_config(transport(&self.scenario));
            let pair = &mut self.pairs[index];
            let handle = pair.leader.endpoint.insert_client_config(config).unwrap();
            let connection = pair
                .leader
                .endpoint
                .connect(self.epoch, handle, pair.follower.address, "localhost", None)
                .unwrap();
            pair.leader.connection = Some(connection);
            let node = pair.leader.node;
            self.serve(node);
        }
    }

    /// The leader's application on one connection: each append as it falls due, then the answers
    /// read.
    fn lead(scenario: &Scenario, due: &[u64], pair: &mut Pair, now: u64) {
        if pair.connected && !pair.caught_up {
            pair.caught_up = true;
            if scenario.catch_up_bytes > 0 {
                let length = u32::try_from(scenario.catch_up_bytes).unwrap();
                let unsent = &mut pair.leader.unsent;
                unsent.extend_from_slice(&length.to_le_bytes());
                unsent.extend_from_slice(&u64::MAX.to_le_bytes());
                unsent.extend_from_slice(&now.to_le_bytes());
                unsent.resize(unsent.len() + scenario.catch_up_bytes - APPEND_HEADER, 0xc3);
            } else {
                pair.answered_catch_up = Some(now);
            }
        }
        if pair.connected {
            while pair.next < due.len() && due[pair.next] <= now {
                let number = pair.next as u64;
                if pair.answered_catch_up.is_none() {
                    // Behind the catch-up it would measure the catch-up's drain
                    pair.next += 1;
                    continue;
                }
                let length = u32::try_from(scenario.append_bytes).unwrap();
                let unsent = &mut pair.leader.unsent;
                unsent.extend_from_slice(&length.to_le_bytes());
                unsent.extend_from_slice(&number.to_le_bytes());
                unsent.extend_from_slice(&due[pair.next].to_le_bytes());
                unsent.resize(unsent.len() + scenario.append_bytes - APPEND_HEADER, 0xa5);
                pair.next += 1;
            }
            pair.leader.flush();
        }
        pair.leader.read();
        let answer = scenario.answer_bytes;
        let mut taken = 0;
        while pair.leader.partial.len() - taken >= answer {
            let number = u64::from_le_bytes(
                pair.leader.partial[taken..taken + ANSWER_HEADER]
                    .try_into()
                    .unwrap(),
            );
            taken += answer;
            if number == u64::MAX {
                pair.answered_catch_up.get_or_insert(now);
                continue;
            }
            pair.answered[usize::try_from(number).unwrap()] = Some(now);
        }
        pair.leader.partial.drain(..taken);
    }

    /// The follower's application: each append read whole, answered.
    fn follow(scenario: &Scenario, pair: &mut Pair, now: u64) {
        pair.follower.read();
        let mut taken = 0;
        loop {
            let rest = &pair.follower.partial[taken..];
            if rest.len() < APPEND_HEADER {
                break;
            }
            let length = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            assert!(
                length == scenario.append_bytes || length == scenario.catch_up_bytes,
                "the follower read a length of {length} at its {}th byte: {:02x?}",
                taken,
                &rest[..rest.len().min(32)]
            );
            if rest.len() < length {
                break;
            }
            let number = u64::from_le_bytes(rest[4..12].try_into().unwrap());
            if let Some(delivered) = usize::try_from(number)
                .ok()
                .and_then(|number| pair.delivered.get_mut(number))
            {
                *delivered = Some(now);
            }
            let unsent = &mut pair.follower.unsent;
            unsent.extend_from_slice(&number.to_le_bytes());
            unsent.resize(unsent.len() + scenario.answer_bytes - ANSWER_HEADER, 0x5a);
            taken += length;
        }
        pair.follower.partial.drain(..taken);
        pair.follower.flush();
    }

    /// What `node` does after it was told something: its connection's events to its endpoint, its
    /// application, what it has to send, and its timer.
    fn serve(&mut self, node: NodeId) {
        let now = self.world.now();
        let at = self.at(now);
        let (index, is_leader) = Self::locate(node);
        let Self {
            scenario,
            epoch,
            world,
            net,
            pairs,
            due,
            start,
            closed,
            scratch,
        } = self;
        let pair = &mut pairs[index];
        {
            let side = if is_leader {
                &mut pair.leader
            } else {
                &mut pair.follower
            };
            let Some((handle, connection)) = &mut side.connection else {
                return;
            };
            while let Some(event) = connection.poll_endpoint_events() {
                if let Some(event) = side.endpoint.handle_event(*handle, event) {
                    connection.handle_event(event, side.endpoint.configs_mut());
                }
            }
            while let Some(event) = connection.poll() {
                match event {
                    Event::Connected if is_leader => pair.connected = true,
                    Event::ConnectionLost { reason } => {
                        let who = if is_leader { "leader" } else { "follower" };
                        closed.get_or_insert(format!("{who} {index}: {reason}"));
                    }
                    _ => {}
                }
            }
        }
        if is_leader {
            Self::lead(scenario, due, pair, now);
        } else {
            Self::follow(scenario, pair, now);
        }
        let (side, peer) = if is_leader {
            (&mut pair.leader, pair.follower.node)
        } else {
            (&mut pair.follower, pair.leader.node)
        };
        let Some((_, connection)) = &mut side.connection else {
            return;
        };
        loop {
            scratch.clear();
            let Some(transmit) =
                connection.poll_transmit(at, scenario.segments, scratch, side.endpoint.configs())
            else {
                break;
            };
            // Each datagram of a batch on its own, as the network carries them, or the batch as
            // one, lost whole; each message its datagrams' size first
            let segment = transmit.segment_size.unwrap_or(transmit.size);
            let batch = &scratch[..transmit.size];
            let messages = if scenario.batch_loss {
                batch.chunks(batch.len().max(1))
            } else {
                batch.chunks(segment)
            };
            for message in messages {
                let mut payload = u32::try_from(segment).unwrap().to_le_bytes().to_vec();
                payload.extend_from_slice(message);
                net.send(world, (node, peer), payload, message.len(), Ev::Arrive)
                    .unwrap();
            }
        }
        let timer = connection
            .poll_timeout()
            .map(|due| u64::try_from(due.saturating_duration_since(*epoch).as_nanos()).unwrap());
        // The leader's side wakes for its next append too
        let append =
            (is_leader && pair.connected && pair.next < due.len()).then(|| due[pair.next].max(now));
        let deadline = match (timer, append) {
            (Some(t), Some(a)) => Some(t.min(a)),
            (t, a) => t.or(a),
        };
        world.wake(node, deadline).unwrap();
        // Measured from when every follower has answered its catch-up
        if start.is_none() && pairs.iter().all(|pair| pair.answered_catch_up.is_some()) {
            *start = Some(now);
            for pair in pairs.iter_mut() {
                pair.at_start = Some(Counts::of(pair));
            }
        }
    }

    fn arrive(&mut self, node: NodeId, ticket: Ticket) {
        let at = self.at(self.world.now());
        let Some(delivery) = self
            .net
            .deliver(&mut self.world, ticket, Ev::Arrive)
            .unwrap()
        else {
            return;
        };
        let (index, is_leader) = Self::locate(node);
        let Self { pairs, scratch, .. } = self;
        let pair = &mut pairs[index];
        let (side, from) = if is_leader {
            (&mut pair.leader, pair.follower.address)
        } else {
            (&mut pair.follower, pair.leader.address)
        };
        let (segment, datagrams) = delivery.payload.split_at(4);
        let segment = u32::from_le_bytes(segment.try_into().unwrap()) as usize;
        for datagram in datagrams.chunks(segment.max(1)) {
            scratch.clear();
            let bytes = BytesMut::from(datagram);
            match side.endpoint.handle(at, from, None, None, bytes, scratch) {
                Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                    if let Some((own, connection)) = &mut side.connection
                        && *own == handle
                    {
                        connection.handle_event(event, side.endpoint.configs_mut());
                    }
                }
                Some(DatagramEvent::NewConnection(incoming)) => {
                    scratch.clear();
                    let accepted = side
                        .endpoint
                        .accept(incoming, at, scratch, None, None)
                        .unwrap_or_else(|error| panic!("the follower refused: {:?}", error.cause));
                    side.connection = Some(accepted);
                }
                Some(DatagramEvent::Response(transmit)) => {
                    panic!("a stateless response on an established path: {transmit:?}")
                }
                None => {}
            }
        }
        self.serve(node);
    }

    fn fire(&mut self, node: NodeId) {
        let at = self.at(self.world.now());
        let (index, is_leader) = Self::locate(node);
        let pair = &mut self.pairs[index];
        let side = if is_leader {
            &mut pair.leader
        } else {
            &mut pair.follower
        };
        if let Some((_, connection)) = &mut side.connection {
            connection.handle_timeout(at);
        }
        self.serve(node);
    }

    fn run(mut self, pki: &Pki) -> (Measured, Record) {
        let end = self.scenario.end();
        self.world.schedule(end, NodeId(0), Ev::End).unwrap();
        self.connect(pki);
        loop {
            match self.world.next(&mut Fifo).unwrap() {
                Step::Event {
                    node,
                    event: Ev::Arrive(ticket),
                } => self.arrive(node, ticket),
                Step::Event { event: Ev::End, .. } | Step::Idle => break,
                Step::Wake { node } => self.fire(node),
                Step::Spent => panic!("{}: the run spent its steps", self.scenario.name()),
            }
        }
        // A run in which a follower never answered its catch-up measures nothing
        let start = self.start.unwrap_or(end);
        let closed = self.closed.clone().or_else(|| {
            self.start
                .is_none()
                .then(|| "a follower never answered its catch-up".to_string())
        });
        let mut measured = Measured {
            closed,
            answered: vec![Vec::new(); self.pairs.len()],
            ..Measured::default()
        };
        for (number, due) in self.due.iter().enumerate() {
            if *due < start || *due >= end - 2 * SECOND {
                continue;
            }
            let mut first: Option<u64> = None;
            for (index, pair) in self.pairs.iter().enumerate() {
                if let Some(at) = pair.answered[number] {
                    measured.answered[index].push(at - due);
                    first = Some(first.map_or(at, |first| first.min(at)));
                }
            }
            match first {
                Some(at) => measured.committed.push(at - due),
                None => measured.uncommitted += 1,
            }
            if let Some(at) = self.pairs[0].delivered[number] {
                measured.delivered.push(at - due);
            }
        }
        measured.committed.sort_unstable();
        measured.delivered.sort_unstable();
        for answered in &mut measured.answered {
            answered.sort_unstable();
        }
        for pair in &self.pairs {
            let at_start = pair.at_start.unwrap_or_default();
            measured.counts.add(Counts::of(pair).since(at_start));
        }
        // What a replay must reproduce: every commit's time and the code's counts
        for committed in &measured.committed {
            self.world.observe(*committed);
        }
        self.world.observe(measured.counts.sources);
        self.world.observe(measured.counts.repairs);
        self.world.observe(measured.counts.rebuilt);
        (measured, self.world.finish())
    }
}

fn measure(scenario: Scenario, pki: &Pki) -> Measured {
    Run::new(scenario, pki, Source::Seed(scenario.seed))
        .run(pki)
        .0
}

/// The run-twice check (`docs/sim.md` §3.9): a coded run over a lossy path gives one digest from its
/// seed twice and from its trace, its commits' times and the code's counts among what it observes.
#[test]
fn a_coded_run_replays_from_its_seed_and_from_its_trace() {
    let pki = Pki::new();
    let path = Scenario {
        seconds: 12,
        ..replication(30_000, Some(128), 3)
    };
    twice(path.seed, |source| -> Result<Record, SimError> {
        Ok(Run::new(path, &pki, source).run(&pki).1)
    })
    .unwrap();
}

fn millis(ns: u64) -> f64 {
    ns as f64 / MS as f64
}

fn row(path: &Scenario, m: &Measured) -> String {
    let counts = m.counts;
    format!(
        "| {} | {:.0} / {:.0} / {:.0} | {:.0} / {:.0} | {} | {} | {} / {} | {} / {} |",
        path.name(),
        millis(Measured::at(&m.committed, 500)),
        millis(Measured::at(&m.committed, 900)),
        millis(Measured::at(&m.committed, 990)),
        millis(Measured::at(&m.delivered, 500)),
        millis(Measured::at(&m.delivered, 990)),
        m.uncommitted,
        counts.lost,
        counts.sources,
        counts.repairs,
        counts.rebuilt,
        counts.left,
    )
}

/// The code repairs what replication loses. **The rule, fixed before any run:** over seeds 1 and
/// 2 of focal's replication at 3% loss each way, every append commits; without the code the
/// commit's median is more than one and a half times the loss-free path's (the head-of-line wait
/// this scenario reproduces); with it, over a window of 128 sources, the median is within a tenth
/// of the loss-free path's, the 99th percentile within two and a half times its median, and the
/// followers rebuild at least four in five of the sources lost while measured.
#[test]
fn the_code_repairs_what_replication_loses() {
    let pki = Pki::new();
    for seed in 1..=2 {
        let clean = measure(replication(0, Some(128), seed), &pki);
        let base = Measured::at(&clean.committed, 500);
        let plain_path = replication(30_000, None, seed);
        let plain = measure(plain_path, &pki);
        let coded_path = replication(30_000, Some(128), seed);
        let coded = measure(coded_path, &pki);
        for (path, m) in [(&plain_path, &plain), (&coded_path, &coded)] {
            assert!(m.closed.is_none(), "{}: {:?}", path.name(), m.closed);
            assert_eq!(m.uncommitted, 0, "{}", row(path, m));
        }
        assert!(
            Measured::at(&plain.committed, 500) * 2 > base * 3,
            "the scenario does not hold its appends: {}",
            row(&plain_path, &plain)
        );
        repaired(base, &coded_path, &coded);
    }
}

/// What a coded run is held to beside the loss-free path's median commit, `base`: every append
/// committed, the median within a tenth of `base` and the 99th percentile within two and a half
/// times it, no repair dropped, and at least four in five of the sources lost rebuilt.
fn repaired(base: u64, path: &Scenario, coded: &Measured) {
    assert!(
        coded.closed.is_none(),
        "{}: {:?}",
        path.name(),
        coded.closed
    );
    assert_eq!(coded.uncommitted, 0, "{}", row(path, coded));
    assert!(
        Measured::at(&coded.committed, 500) * 10 <= base * 11,
        "loss-free median {base} ns: {}",
        row(path, coded)
    );
    assert!(
        Measured::at(&coded.committed, 990) * 2 <= base * 5,
        "loss-free median {base} ns: {}",
        row(path, coded)
    );
    // A datagram its pacer or window cuts is never shorter than the repair due: none is dropped,
    // though the sources grew past the smallest datagram with the path's MTU
    assert_eq!(coded.counts.dropped, 0, "{}", row(path, coded));
    // Of the sources lost: the leader's losses count its repairs and its MTU probes too, which
    // nothing rebuilds
    let sources_lost = coded.counts.rebuilt + coded.counts.left;
    assert!(
        coded.counts.rebuilt * 5 >= sources_lost * 4,
        "{}",
        row(path, coded)
    );
}

/// The code repairs what a segmented send loses whole. **The rule, fixed before any run:** over
/// seeds 1 and 2 of focal's replication at 3% loss each way, each transmit of up to 64 datagrams
/// lost or kept whole, as netem loses focal's (`net/sched/sch_netem.c`'s `netem_enqueue` decides
/// each socket buffer's loss before it segments one), the coded run is held to what the
/// datagram-by-datagram one is (`repaired`). Its losses come in bursts of a transmit's
/// datagrams: with single losses' spans, and a repair in the transmit of the sources before it,
/// the followers rebuilt 78% of their losses (focal's: 77 to 82%), and with the leader's
/// recovery window conserved at what its last burst left in flight, the uncoded commit's median
/// was 182 to 208 ms (163 loss-free).
#[test]
fn the_code_repairs_what_a_segmented_send_loses_whole() {
    let pki = Pki::new();
    for seed in 1..=2 {
        let clean = measure(replication(0, Some(128), seed), &pki);
        let base = Measured::at(&clean.committed, 500);
        let coded_path = Scenario {
            batch_loss: true,
            ..replication(30_000, Some(128), seed)
        };
        let coded = measure(coded_path, &pki);
        repaired(base, &coded_path, &coded);
    }
}

/// A path that loses nothing is not coded: once the catch-up's losses at the bottleneck are past,
/// no packet is a source and none is a repair.
#[test]
fn a_path_that_loses_nothing_is_not_coded() {
    let pki = Pki::new();
    let path = replication(0, Some(128), 1);
    let m = measure(path, &pki);
    assert_eq!(m.uncommitted, 0, "{}", row(&path, &m));
    assert_eq!(m.counts.lost, 0, "{}", row(&path, &m));
    assert_eq!(
        (m.counts.sources, m.counts.repairs),
        (0, 0),
        "{}",
        row(&path, &m)
    );
}

/// The appends' commits over focal's replication at 0 to 10% loss each way, coded and not.
#[test]
#[ignore = "a measurement, run by hand"]
fn print_the_appends_commits_under_loss() {
    let pki = Pki::new();
    println!(
        "| Path | committed p50 / p90 / p99 ms | delivered p50 / p99 ms | uncommitted | leader packets lost | sources / repairs | rebuilt / left |"
    );
    println!("|---|---|---|---|---|---|---|");
    for batch_loss in [false, true] {
        for loss in [0, 5_000, 10_000, 30_000, 50_000, 100_000] {
            for window in [None, Some(64), Some(128)] {
                for seed in 1..=2 {
                    let path = Scenario {
                        seconds: 40,
                        batch_loss,
                        ..replication(loss, window, seed)
                    };
                    let m = measure(path, &pki);
                    println!("{}", row(&path, &m));
                    assert!(m.closed.is_none(), "{}: {:?}", path.name(), m.closed);
                }
            }
        }
    }
}
