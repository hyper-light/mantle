//! The congestion controllers hyper-quic runs, measured beside one another on hyper-sim's network
//! (`docs/transport.md` §4d): focal's F39 grids, ported.
//!
//! Real hyper-quic endpoints (TLS 1.3, packet protection, loss recovery, pacing) exchange over
//! hyper-sim's network in virtual time. Each flow is a client node and a server node, and every
//! flow's datagrams cross one bottleneck link each way, the dumbbell of RFC 5166: a stated rate, a
//! queue of one bandwidth-delay product, drop-tail or managed, the IP header's ECN field carried.
//! Each client sends one transfer as fast as its law lets it, from the run's start to its end.
//!
//! A run is its seed's: the world orders every arrival and timer, no path draws, and the endpoints'
//! own keys and nonces change no size and no time, so each check is exact for the seeds it names,
//! and the run-twice check (`docs/sim.md` §3.9) holds the harness to that.
//!
//! The gate runs one path for a few seconds. focal's grid, thirty seconds a run, is a measurement run
//! by hand (`--ignored`), and is what `docs/benchmarks.md` records.

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
    clippy::panic_in_result_fn,
    clippy::unwrap_in_result,
    missing_docs
)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use hyper_quic::congestion::{BbrConfig, Congestion, Copa, CopaConfig, CubicConfig, NewRenoConfig};
use hyper_quic::rustls::RootCertStore;
use hyper_quic::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use hyper_quic::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, EcnCodepoint, Endpoint,
    EndpointConfig, Event, ServerConfig, StreamEvent, StreamId, TransportConfig, VarInt,
};
use hyper_sim::net::{Fate, Link, Loss, Marking, Net, NetLimits, Path, Ticket};
use hyper_sim::{
    Clock, Discipline, Fifo, Limits, NodeId, Record, SimError, Source, Step, World, twice,
};

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;
/// The endpoints' datagrams with path MTU discovery off: RFC 9000 §14's smallest maximum.
const DATAGRAM: u64 = 1_200;
/// The smallest datagram an endpoint sends: header protection samples 16 bytes from 4 past the
/// packet number's start (RFC 9001 §5.4.2), after a byte of flags. It bounds how many messages a
/// link's queue and a path hold.
const SMALLEST_DATAGRAM: u64 = 21;
/// What the transfer hands its stream at a time: any size, since it writes until the stream refuses.
static CHUNK: [u8; 65_536] = [7; 65_536];
/// The seeds a scenario is judged over, focal's count: over eight, focal's spread of the
/// incumbent's share at 1 Mbit/s and 100 ms under CoDel held a law at its bar clear of the floor.
/// Each seed's run is a case of its own here, checked exactly.
const HARM_SEEDS: u64 = 8;
/// RFC 8289's CoDel at its defaults: a target of 5 ms (§4.4) over an interval of 100 ms (§4.3).
const CODEL: Marking = Marking::CoDel {
    target_ns: 5 * MS,
    interval_ns: 100 * MS,
};
/// A step at one datagram: the low threshold DCTCP's switches mark at (RFC 8257 §3.1), where a
/// classic sender starves itself.
const STEP: Marking = Marking::Step {
    threshold_bytes: DATAGRAM,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Law {
    NewReno,
    Cubic,
    Bbr,
    Copa,
}

impl Law {
    /// The laws focal's grids judge: the loss-based incumbents and Copa.
    const ALL: [Self; 3] = [Self::NewReno, Self::Cubic, Self::Copa];
    /// Every law hyper-quic ships, slates' bake-off's field.
    const BAKEOFF: [Self; 4] = [Self::NewReno, Self::Cubic, Self::Bbr, Self::Copa];

    fn congestion(self) -> Congestion {
        match self {
            Self::NewReno => Congestion::NewReno(NewRenoConfig::default()),
            Self::Cubic => Congestion::Cubic(CubicConfig::default()),
            Self::Bbr => Congestion::Bbr(BbrConfig::default()),
            Self::Copa => Congestion::Copa(CopaConfig::default()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Scenario {
    rate_bits_per_second: u64,
    rtt_ns: u64,
    seconds: u64,
    seed: u64,
    marking: Marking,
    /// The bottleneck's queue in thousandths of a bandwidth-delay product.
    buffer_permille: u64,
    /// What each direction's path loses.
    loss: Loss,
    /// Each datagram's propagation varies by up to this either way, and may overtake an earlier one.
    jitter_ns: u64,
    /// At this time the bottleneck's rate becomes the second value, both ways.
    step: Option<(u64, u64)>,
    /// Where measurement starts, if not a quarter of the run.
    warm_ns: Option<u64>,
}

impl Scenario {
    fn bdp_bytes(&self) -> u64 {
        let bits = u128::from(self.rate_bits_per_second) * u128::from(self.rtt_ns) / 1_000_000_000;
        u64::try_from(bits / 8).unwrap()
    }

    /// The configured share of a bandwidth-delay product (one by default), and four datagrams at
    /// least.
    fn queue_bytes(&self) -> u64 {
        (self.bdp_bytes() * self.buffer_permille / 1_000).max(4 * DATAGRAM)
    }

    /// The decisions a run records: a draw for loss and one for delay at most per datagram, on a
    /// path that draws them, and the datagrams each way no more than the bottleneck carries over
    /// the run at its peak rate, each of the smallest size, beside what it holds at the start.
    fn trace_words(&self) -> usize {
        let draws = u64::from(self.loss != Loss::NONE) + u64::from(self.jitter_ns != 0);
        let carried = u128::from(self.peak_rate()) * u128::from(self.end()) / 8_000_000_000;
        let datagrams =
            (u64::try_from(carried).unwrap() + self.bytes_one_way()) / SMALLEST_DATAGRAM;
        usize::try_from(2 * draws * datagrams).unwrap()
    }

    /// The bottleneck's largest rate over the run.
    fn peak_rate(&self) -> u64 {
        self.step.map_or(self.rate_bits_per_second, |(_, rate)| {
            rate.max(self.rate_bits_per_second)
        })
    }

    /// The most bytes one direction holds: its full queue, the datagram on the wire among them, and
    /// what has left the link and propagates. Datagrams leave the link one after another at its
    /// rate, so those that left within a propagation's longest time (half the round trip and the
    /// jitter) hold that time at the peak rate and the first of them whole: a datagram is in flight
    /// until it arrives, and on a thin path its propagation carries a fraction of one (80 bytes at
    /// 64 kbit/s and 20 ms, against a 1,200-byte datagram).
    fn bytes_one_way(&self) -> u64 {
        let bits = u128::from(self.peak_rate()) * u128::from(self.rtt_ns + 2 * self.jitter_ns)
            / 1_000_000_000;
        u64::try_from(bits / 16).unwrap() + DATAGRAM + self.queue_bytes()
    }

    /// Measured from a quarter of the run unless stated: slow start and the first oscillations
    /// are over.
    fn warm(&self) -> u64 {
        self.warm_ns.unwrap_or(self.seconds * SECOND / 4)
    }

    fn end(&self) -> u64 {
        self.seconds * SECOND
    }

    /// The bytes the link carries between the warm-up and the end, the step's rate after it.
    fn could_carry(&self) -> u64 {
        let (warm, end) = (self.warm(), self.end());
        let span = |from: u64, to: u64, rate: u64| {
            u128::from(rate) * u128::from(to.saturating_sub(from)) / 1_000_000_000
        };
        let bits = match self.step {
            Some((at, rate)) => {
                let at = at.clamp(warm, end);
                span(warm, at, self.rate_bits_per_second) + span(at, end, rate)
            }
            None => span(warm, end, self.rate_bits_per_second),
        };
        u64::try_from(bits / 8).unwrap()
    }

    /// The bottleneck's rate and the round trip: "64k 20ms", "10M 100ms".
    fn link_name(&self) -> String {
        let rate = self.rate_bits_per_second;
        let rate = if rate >= 1_000_000 {
            format!("{}M", rate / 1_000_000)
        } else {
            format!("{}k", rate / 1_000)
        };
        format!("{rate} {}ms", self.rtt_ns / MS)
    }

    fn name(&self) -> String {
        let marking = match self.marking {
            Marking::Off => String::new(),
            Marking::Step { threshold_bytes } => {
                format!(" step {}p", threshold_bytes.div_ceil(DATAGRAM))
            }
            Marking::CoDel {
                target_ns,
                interval_ns,
            } => format!(" codel {}/{}ms", target_ns / MS, interval_ns / MS),
        };
        format!("{} seed {}{marking}", self.link_name(), self.seed)
    }
}

fn scenario(rate_bits_per_second: u64, rtt_ms: u64, seconds: u64) -> Scenario {
    Scenario {
        rate_bits_per_second,
        rtt_ns: rtt_ms * MS,
        seconds,
        seed: 1,
        marking: Marking::Off,
        buffer_permille: 1_000,
        loss: Loss::NONE,
        jitter_ns: 0,
        step: None,
        warm_ns: None,
    }
}

struct Pki {
    certificate: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}

impl Pki {
    /// The same identity every run, so a run is its seed's. rcgen's default identity is a random
    /// ECDSA P-256 key and serial: an ECDSA signature takes a random nonce and its DER encoding is
    /// 70 to 72 bytes as the nonce falls (RFC 8446 §4.2.3, RFC 3279 §2.2.3), and rustls compresses
    /// the certificate (RFC 8879), so the server's flight changed length by bytes run to run. With
    /// SecP384r1MLKEM1024's larger share first, a byte more moved a datagram's boundary and the run's
    /// timing, and the run-twice check failed. An Ed25519 key from a fixed seed (RFC 8410 §7's
    /// PKCS#8 form) signs deterministically (RFC 8032 §5.1.6).
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

/// The law, and flow control that never binds before it does: four times what the path and its
/// queue hold, so a window the law opens is never stopped by a credit.
fn transport(law: Law, scenario: &Scenario) -> TransportConfig {
    let window = 4 * (scenario.bdp_bytes() + scenario.queue_bytes());
    let mut transport = TransportConfig::default();
    transport
        .stream_receive_window(VarInt::from_u64(window).unwrap())
        .receive_window(VarInt::from_u64(window).unwrap())
        .send_window(window)
        .mtu_discovery_config(None)
        .congestion(law.congestion());
    transport
}

/// A datagram on the network, with the ECN codepoint its IP header carries.
#[derive(Clone, Debug)]
struct Datagram {
    bytes: Vec<u8>,
    ecn: Option<EcnCodepoint>,
}

/// What the world orders: a datagram's arrival, and the run's own marks in time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    Arrive(Ticket),
    Warm,
    Sample,
    /// The bottleneck's rate changes (`Scenario::step`).
    Step,
    End,
}

/// One connection of a run: its law, when its client stops handing its transfer more, whether it
/// is a ping flow (a request of [`PING_BYTES`] every gap, each answered by as many) rather than a
/// bulk transfer, and its round trip where it differs from the scenario's. A ping flow's server
/// lets every ping the run sends be open at once unless `streams` states a limit: the law's queue,
/// not the stream limit (RFC 9000 §4.6), is what holds a ping back, as slates' bake-off had it.
#[derive(Clone, Copy, Debug)]
struct FlowSpec {
    law: Law,
    stop_at: Option<u64>,
    ping_gap_ns: Option<u64>,
    rtt_ns: Option<u64>,
    streams: Option<u64>,
}

impl FlowSpec {
    const fn bulk(law: Law) -> Self {
        Self {
            law,
            stop_at: None,
            ping_gap_ns: None,
            rtt_ns: None,
            streams: None,
        }
    }
}

/// A ping's request and its reply, a small metadata operation's (slates' bake-off).
const PING_BYTES: usize = 64;

struct Side {
    node: NodeId,
    address: SocketAddr,
    endpoint: Endpoint,
    connection: Option<(ConnectionHandle, Connection)>,
}

/// What a flow measures.
#[derive(Default)]
struct State {
    connected: bool,
    transfer: Option<StreamId>,
    inbound: Option<StreamId>,
    received: u64,
    received_at_warm: u64,
    closed: Option<(u64, String)>,
    /// How long each of the client's datagrams after the warm-up waits in the bottleneck's queue.
    queued: Vec<u64>,
    /// The client's datagrams the server received marked Congestion Experienced.
    marked: u64,
    /// Whether the client's last datagram was ECN-capable.
    ecn: bool,
    /// Samples of a Copa client's mode: competing, of all taken; and of those in the run's last
    /// quarter.
    competing: u64,
    samples: u64,
    competing_late: u64,
    samples_late: u64,
    /// When the client stops handing its transfer more: what it handed already still goes.
    stop_at: Option<u64>,
    /// A ping flow's gap between requests, and when the next is due.
    ping_gap: Option<u64>,
    next_ping: u64,
    /// Whether the stream limit held the next ping back: it waits for the peer's credit, which a
    /// datagram brings, and not for a timer.
    ping_blocked: bool,
    /// The client's pings awaiting their replies: each stream, when it was due, the bytes back.
    pings: Vec<(StreamId, u64, usize)>,
    /// Each ping's latency, due to answered, for those due after the warm-up.
    ping_latencies: Vec<u64>,
    /// The server's requests being read: each stream and the bytes read.
    requests: Vec<(StreamId, usize)>,
    /// When the run starts measuring, for the pings.
    warm: u64,
}

/// One connection: flow `f` is nodes `2f` (the client) and `2f + 1` (the server).
struct Flow {
    law: Law,
    client: Side,
    server: Side,
    state: State,
}

impl Flow {
    fn new(index: u32, spec: &FlowSpec, scenario: &Scenario, pki: &Pki) -> Self {
        let rng = |node: NodeId| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&scenario.seed.to_le_bytes());
            bytes[8..12].copy_from_slice(&node.0.to_le_bytes());
            Some(bytes)
        };
        let mut server_config =
            ServerConfig::with_single_cert(vec![pki.certificate.clone()], pki.key.clone_key())
                .unwrap();
        let mut transport = transport(spec.law, scenario);
        if let Some(gap) = spec.ping_gap_ns {
            let streams = spec.streams.unwrap_or(scenario.end() / gap + 1);
            transport.max_concurrent_bidi_streams(VarInt::from_u64(streams).unwrap());
        }
        server_config.transport_config(transport);
        let (client, server) = (NodeId(2 * index), NodeId(2 * index + 1));
        Self {
            law: spec.law,
            client: Side {
                node: client,
                address: format!("10.0.{index}.1:4433").parse().unwrap(),
                endpoint: Endpoint::new(EndpointConfig::default(), None, false, rng(client))
                    .unwrap(),
                connection: None,
            },
            server: Side {
                node: server,
                address: format!("10.0.{index}.2:4433").parse().unwrap(),
                endpoint: Endpoint::new(
                    EndpointConfig::default(),
                    Some(server_config),
                    false,
                    rng(server),
                )
                .unwrap(),
                connection: None,
            },
            state: State::default(),
        }
    }
}

/// A ping client's application: a request each gap on a stream of its own, each reply's latency
/// from when its request was due.
fn ping_client(connection: &mut Connection, state: &mut State, now: u64, gap: u64) {
    state.ping_blocked = false;
    while state.next_ping <= now {
        let Some(id) = connection.streams().open(Dir::Bi) else {
            // The stream limit holds it back: it goes when the peer's credit arrives, its latency
            // from when it was due
            state.ping_blocked = true;
            break;
        };
        let mut send = connection.send_stream(id);
        assert_eq!(send.write(&[0x50; PING_BYTES]).unwrap(), PING_BYTES);
        send.finish().unwrap();
        state.pings.push((id, state.next_ping, 0));
        state.next_ping += gap;
    }
    let warm = state.warm;
    let latencies = &mut state.ping_latencies;
    state.pings.retain_mut(|(id, due, got)| {
        let mut stream = connection.recv_stream(*id);
        if let Ok(mut chunks) = stream.read(true) {
            while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
                *got += chunk.bytes.len();
            }
            let _ = chunks.finalize();
        }
        if *got < PING_BYTES {
            return true;
        }
        if *due >= warm {
            latencies.push(now - *due);
        }
        false
    });
}

/// A ping server's application: each request answered as it is read whole.
fn ping_server(connection: &mut Connection, state: &mut State) {
    while let Some(id) = connection.streams().accept(Dir::Bi) {
        state.requests.push((id, 0));
    }
    state.requests.retain_mut(|(id, got)| {
        let mut stream = connection.recv_stream(*id);
        if let Ok(mut chunks) = stream.read(true) {
            while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
                *got += chunk.bytes.len();
            }
            let _ = chunks.finalize();
        }
        if *got < PING_BYTES {
            return true;
        }
        let mut send = connection.send_stream(*id);
        assert_eq!(send.write(&[0x51; PING_BYTES]).unwrap(), PING_BYTES);
        send.finish().unwrap();
        false
    });
}

/// The client's application: the transfer, written until the stream refuses more, or the pings.
fn client_events(connection: &mut Connection, state: &mut State, now: u64) {
    while let Some(event) = connection.poll() {
        match event {
            Event::Connected => {
                state.connected = true;
                state.next_ping = now;
            }
            Event::ConnectionLost { reason } => {
                state
                    .closed
                    .get_or_insert((now, format!("client: {reason}")));
            }
            _ => {}
        }
    }
    if !state.connected || state.stop_at.is_some_and(|stop| now >= stop) {
        return;
    }
    if let Some(gap) = state.ping_gap {
        ping_client(connection, state, now, gap);
        return;
    }
    let id = *state
        .transfer
        .get_or_insert_with(|| connection.streams().open(Dir::Uni).unwrap());
    while matches!(connection.send_stream(id).write(&CHUNK), Ok(written) if written > 0) {}
}

/// The server's application: the transfer, read as it comes.
fn server_events(connection: &mut Connection, state: &mut State, now: u64) {
    while let Some(event) = connection.poll() {
        match event {
            Event::ConnectionLost { reason } => {
                state
                    .closed
                    .get_or_insert((now, format!("server: {reason}")));
            }
            Event::Stream(StreamEvent::Opened { dir: Dir::Uni }) => {
                while let Some(id) = connection.streams().accept(Dir::Uni) {
                    state.inbound = Some(id);
                }
            }
            _ => {}
        }
    }
    if state.ping_gap.is_some() {
        ping_server(connection, state);
        return;
    }
    let Some(id) = state.inbound else {
        return;
    };
    let mut stream = connection.recv_stream(id);
    let Ok(mut chunks) = stream.read(true) else {
        return;
    };
    while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        state.received += chunk.bytes.len() as u64;
    }
    let _ = chunks.finalize();
}

/// What one flow carried and kept.
#[derive(Clone, Debug, Default)]
struct Measured {
    /// A ping flow's answered pings due after the warm-up, and their latency's 99th percentile.
    pings: usize,
    ping_p99_ns: u64,
    /// What the transfer delivered after the warm-up, of what the link could carry then, in parts
    /// per million.
    carried_ppm: u64,
    /// How long the client's datagrams waited in the bottleneck's queue after the warm-up.
    queue_p50_ns: u64,
    queue_p99_ns: u64,
    marked: u64,
    closed: Option<(u64, String)>,
    /// Whether the client's last datagram was ECN-capable: the connection stops marking its own
    /// when the path fails its validation (RFC 9000 §13.4.2).
    ecn: bool,
    /// A Copa client's share of samples in the competing mode, in parts per million.
    competing_ppm: u64,
    /// Its samples in the competing mode in the run's last quarter, and all it took there.
    competing_late: u64,
    samples_late: u64,
}

struct Run {
    scenario: Scenario,
    /// The world's time zero as the endpoints' instant.
    epoch: Instant,
    world: World<Ev>,
    net: Net<Datagram>,
    flows: Vec<Flow>,
    /// The bottleneck each way.
    links: (hyper_sim::net::LinkId, hyper_sim::net::LinkId),
    scratch: Vec<u8>,
}

impl Run {
    /// One connection for each flow, all through the one bottleneck in each direction.
    fn new(
        scenario: Scenario,
        specs: &[FlowSpec],
        pki: &Pki,
        source: Source,
    ) -> Result<Self, SimError> {
        let nodes = 2 * specs.len();
        // Every message either direction holds, at the smallest datagram's size.
        let messages = usize::try_from(2 * scenario.bytes_one_way() / SMALLEST_DATAGRAM).unwrap();
        let limits = Limits {
            // Each message in flight's arrival, and the run's own marks: the warm-up, the end, the
            // next sample and the step
            events: messages + 4,
            nodes,
            // The run's own, and each direction of each flow its loss and its delay draws
            streams: 3 * nodes + 1,
            steps: u64::MAX,
            trace_words: scenario.trace_words(),
        };
        let mut world = World::new(source, Discipline::Ordered, limits)?;
        for _ in 0..nodes {
            world.node(Clock::default())?;
        }
        let epoch = world.instant(NodeId(0))?;
        let mut net = Net::new(NetLimits {
            flows: nodes,
            links: 2,
            nats: 0,
            link_messages: usize::try_from(scenario.queue_bytes() / SMALLEST_DATAGRAM + 1).unwrap(),
            messages,
            bytes: usize::try_from(2 * scenario.bytes_one_way()).unwrap(),
        });
        let link = Link {
            marking: scenario.marking,
            ..Link::drop_tail(scenario.rate_bits_per_second, scenario.queue_bytes())
        };
        let up = net.add_link(link)?;
        let down = net.add_link(link)?;
        let flows: Vec<Flow> = specs
            .iter()
            .enumerate()
            .map(|(index, spec)| {
                let mut flow = Flow::new(u32::try_from(index).unwrap(), spec, &scenario, pki);
                flow.state.stop_at = spec.stop_at;
                flow.state.ping_gap = spec.ping_gap_ns;
                flow.state.warm = scenario.warm();
                flow
            })
            .collect();
        for (flow, spec) in flows.iter().zip(specs) {
            let one_way = spec.rtt_ns.unwrap_or(scenario.rtt_ns) / 2;
            let path = match scenario.jitter_ns {
                0 => Path::in_order(one_way, 0),
                jitter => Path::reordering(one_way, jitter),
            }
            .with_loss(scenario.loss);
            net.set_pair_path(flow.client.node, flow.server.node, path.through(up))?;
            net.set_pair_path(flow.server.node, flow.client.node, path.through(down))?;
        }
        Ok(Self {
            scenario,
            epoch,
            world,
            net,
            flows,
            links: (up, down),
            scratch: Vec::with_capacity(2 * DATAGRAM as usize),
        })
    }

    fn at(&self, now: u64) -> Instant {
        self.epoch + Duration::from_nanos(now)
    }

    /// Every client connects at time zero.
    fn connect(&mut self, pki: &Pki) -> Result<(), SimError> {
        let epoch = self.epoch;
        for index in 0..self.flows.len() {
            let flow = &mut self.flows[index];
            let mut roots = RootCertStore::empty();
            roots.add(pki.certificate.clone()).unwrap();
            let mut config = ClientConfig::with_root_certificates(roots).unwrap();
            config.transport_config(transport(flow.law, &self.scenario));
            let handle = flow.client.endpoint.insert_client_config(config).unwrap();
            let connection = flow
                .client
                .endpoint
                .connect(epoch, handle, flow.server.address, "localhost", None)
                .unwrap();
            flow.client.connection = Some(connection);
            let node = flow.client.node;
            self.serve(node)?;
        }
        Ok(())
    }

    /// What `node` does after it was told something: its connection's events to its endpoint, its
    /// application, what it has to send onto the network, and its timer, which it returns.
    fn serve(&mut self, node: NodeId) -> Result<Option<u64>, SimError> {
        let now = self.world.now();
        let at = self.at(now);
        let Self {
            scenario,
            epoch,
            world,
            net,
            flows,
            scratch,
            ..
        } = self;
        let is_client = node.0.is_multiple_of(2);
        let flow = &mut flows[usize::try_from(node.0 / 2).unwrap()];
        let peer = if is_client {
            flow.server.node
        } else {
            flow.client.node
        };
        let side = if is_client {
            &mut flow.client
        } else {
            &mut flow.server
        };
        let state = &mut flow.state;
        let Some((handle, connection)) = &mut side.connection else {
            return Ok(None);
        };
        while let Some(event) = connection.poll_endpoint_events() {
            if let Some(event) = side.endpoint.handle_event(*handle, event) {
                connection.handle_event(event, side.endpoint.configs_mut());
            }
        }
        if is_client {
            client_events(connection, state, now);
        } else {
            server_events(connection, state, now);
        }
        loop {
            scratch.clear();
            let Some(transmit) = connection.poll_transmit(at, 1, scratch, side.endpoint.configs())
            else {
                break;
            };
            assert!(transmit.segment_size.is_none());
            let size = transmit.size;
            let capable = matches!(transmit.ecn, Some(EcnCodepoint::Ect0 | EcnCodepoint::Ect1));
            let datagram = Datagram {
                bytes: scratch[..size].to_vec(),
                ecn: transmit.ecn,
            };
            let fate = if capable {
                net.send_ecn(
                    world,
                    (node, peer),
                    datagram,
                    size,
                    Ev::Arrive,
                    |datagram| {
                        datagram.ecn = Some(EcnCodepoint::Ce);
                    },
                )?
            } else {
                net.send(world, (node, peer), datagram, size, Ev::Arrive)?
            };
            if !is_client {
                continue;
            }
            state.ecn = capable;
            // The wait in the queue: the arrival less the propagation, the datagram's own time on
            // the link and its sending.
            if let Fate::Arrives { at: arrives } = fate
                && now >= scenario.warm()
            {
                let serialization = (size as u64 * 8 * SECOND)
                    .div_ceil(scenario.rate_bits_per_second)
                    .min(arrives);
                state.queued.push(
                    arrives
                        .saturating_sub(now)
                        .saturating_sub(scenario.rtt_ns / 2)
                        .saturating_sub(serialization),
                );
            }
        }
        let timer = connection
            .poll_timeout()
            .map(|due| u64::try_from(due.saturating_duration_since(*epoch).as_nanos()).unwrap());
        // A ping client wakes for its next request too, unless the stream limit holds that back:
        // the credit's arrival serves it then
        let ping =
            (is_client && state.connected && state.ping_gap.is_some() && !state.ping_blocked)
                .then_some(state.next_ping.max(now));
        let deadline = match (timer, ping) {
            (Some(t), Some(p)) => Some(t.min(p)),
            (t, p) => t.or(p),
        };
        world.wake(node, deadline)?;
        Ok(deadline)
    }

    /// A datagram arrives at `node`.
    fn arrive(&mut self, node: NodeId, ticket: Ticket) -> Result<(), SimError> {
        let at = self.at(self.world.now());
        let Self {
            world,
            net,
            flows,
            scratch,
            ..
        } = self;
        let Some(delivery) = net.deliver(world, ticket, Ev::Arrive)? else {
            return Ok(());
        };
        let is_client = node.0.is_multiple_of(2);
        let flow = &mut flows[usize::try_from(node.0 / 2).unwrap()];
        let from = if is_client {
            flow.server.address
        } else {
            flow.client.address
        };
        if !is_client && delivery.payload.ecn == Some(EcnCodepoint::Ce) {
            flow.state.marked += 1;
        }
        let side = if is_client {
            &mut flow.client
        } else {
            &mut flow.server
        };
        scratch.clear();
        let bytes = BytesMut::from(delivery.payload.bytes.as_slice());
        match side
            .endpoint
            .handle(at, from, None, delivery.payload.ecn, bytes, scratch)
        {
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
                    .unwrap_or_else(|error| panic!("the server refused: {:?}", error.cause));
                side.connection = Some(accepted);
            }
            Some(DatagramEvent::Response(transmit)) => {
                panic!("a stateless response on an established path: {transmit:?}")
            }
            None => {}
        }
        self.serve(node).map(drop)
    }

    /// `node`'s timer fired. A wake does what is due at its time: a node that asked to wake again
    /// at the same time would be woken there again and again, and the run's clock would stop.
    fn fire(&mut self, node: NodeId) -> Result<(), SimError> {
        let now = self.world.now();
        let at = self.at(now);
        let flow = &mut self.flows[usize::try_from(node.0 / 2).unwrap()];
        let side = if node.0.is_multiple_of(2) {
            &mut flow.client
        } else {
            &mut flow.server
        };
        if let Some((_, connection)) = &mut side.connection {
            connection.handle_timeout(at);
        }
        let deadline = self.serve(node)?;
        assert!(
            deadline.is_none_or(|due| due > now),
            "{}: node {} woke at {now} ns and asked to wake then again",
            self.scenario.name(),
            node.0
        );
        Ok(())
    }

    /// Each Copa client's mode, read off its controller.
    fn sample(&mut self) {
        let late = self.world.now() >= self.scenario.end() / 4 * 3;
        for flow in &mut self.flows {
            let Some((_, connection)) = &flow.client.connection else {
                continue;
            };
            let Ok(copa) = connection
                .congestion_state()
                .clone_box()
                .into_any()
                .downcast::<Copa>()
            else {
                continue;
            };
            flow.state.samples += 1;
            flow.state.samples_late += u64::from(late);
            if copa.competitive() {
                flow.state.competing += 1;
                flow.state.competing_late += u64::from(late);
            }
        }
    }

    fn run(mut self, pki: &Pki) -> Result<(Vec<Measured>, Record), SimError> {
        let (warm, end) = (self.scenario.warm(), self.scenario.end());
        // Ten samples a round trip: a mode that holds for five round trips is seen fifty times.
        let sample_every = self.scenario.rtt_ns / 10;
        self.world.schedule(warm, NodeId(0), Ev::Warm)?;
        self.world.schedule(end, NodeId(0), Ev::End)?;
        self.world.schedule(sample_every, NodeId(0), Ev::Sample)?;
        if let Some((at, _)) = self.scenario.step {
            self.world.schedule(at, NodeId(0), Ev::Step)?;
        }
        self.connect(pki)?;
        loop {
            match self.world.next(&mut Fifo)? {
                Step::Event {
                    node,
                    event: Ev::Arrive(ticket),
                } => self.arrive(node, ticket)?,
                Step::Event {
                    event: Ev::Warm, ..
                } => {
                    for flow in &mut self.flows {
                        flow.state.received_at_warm = flow.state.received;
                    }
                }
                Step::Event {
                    event: Ev::Sample, ..
                } => {
                    self.sample();
                    self.world.after(sample_every, NodeId(0), Ev::Sample)?;
                }
                Step::Event {
                    event: Ev::Step, ..
                } => {
                    if let Some((_, rate)) = self.scenario.step {
                        let link = Link {
                            marking: self.scenario.marking,
                            ..Link::drop_tail(rate, self.scenario.queue_bytes())
                        };
                        self.net.set_link(self.links.0, link);
                        self.net.set_link(self.links.1, link);
                    }
                }
                Step::Event { event: Ev::End, .. } | Step::Idle => break,
                Step::Wake { node } => self.fire(node)?,
                Step::Spent => panic!("{}: the run spent its steps", self.scenario.name()),
            }
        }
        let stats = self.net.stats();
        assert_eq!(
            stats.dropped_capacity,
            0,
            "{}: the network's bounds bound the run",
            self.scenario.name()
        );
        let could = self.scenario.could_carry();
        let mut measured = Vec::with_capacity(self.flows.len());
        for flow in &mut self.flows {
            let state = &mut flow.state;
            state.queued.sort_unstable();
            let at = |per_cent: usize| {
                let count = state.queued.len();
                if count == 0 {
                    return 0;
                }
                state.queued[(count * per_cent).div_ceil(100).clamp(1, count) - 1]
            };
            let carried = state.received - state.received_at_warm;
            // A ping still unanswered at the end counts at what it has waited, so a stall shows
            for (_, due, _) in &state.pings {
                if *due >= state.warm {
                    state.ping_latencies.push(end - due);
                }
            }
            state.ping_latencies.sort_unstable();
            let pings = state.ping_latencies.len();
            let ping_p99_ns = if pings > 0 {
                state.ping_latencies[(pings * 99).div_ceil(100).clamp(1, pings) - 1]
            } else {
                0
            };
            let m = Measured {
                pings,
                ping_p99_ns,
                carried_ppm: carried * 1_000_000 / could.max(1),
                queue_p50_ns: at(50),
                queue_p99_ns: at(99),
                marked: state.marked,
                closed: state.closed.clone(),
                ecn: state.ecn,
                competing_ppm: state.competing * 1_000_000 / state.samples.max(1),
                competing_late: state.competing_late,
                samples_late: state.samples_late,
            };
            self.world.observe(m.carried_ppm);
            self.world.observe(m.queue_p99_ns);
            self.world.observe(m.marked);
            measured.push(m);
        }
        Ok((measured, self.world.finish()))
    }
}

/// Flows of these laws at once through the scenario's bottleneck, each stopping where it states,
/// the world's decisions from `source`.
fn run(
    scenario: Scenario,
    laws: &[(Law, Option<u64>)],
    pki: &Pki,
    source: Source,
) -> Result<(Vec<Measured>, Record), SimError> {
    let specs: Vec<FlowSpec> = laws
        .iter()
        .map(|(law, stop_at)| FlowSpec {
            stop_at: *stop_at,
            ..FlowSpec::bulk(*law)
        })
        .collect();
    run_flows(scenario, &specs, pki, source)
}

/// These flows at once through the scenario's bottleneck, the world's decisions from `source`.
fn run_flows(
    scenario: Scenario,
    specs: &[FlowSpec],
    pki: &Pki,
    source: Source,
) -> Result<(Vec<Measured>, Record), SimError> {
    Run::new(scenario, specs, pki, source)?.run(pki)
}

fn compete(scenario: Scenario, laws: &[Law], pki: &Pki) -> Vec<Measured> {
    let laws: Vec<(Law, Option<u64>)> = laws.iter().map(|law| (*law, None)).collect();
    run(scenario, &laws, pki, Source::Seed(scenario.seed))
        .unwrap()
        .0
}

fn measure(scenario: Scenario, law: Law, pki: &Pki) -> Measured {
    compete(scenario, &[law], pki).remove(0)
}

fn percent(ppm: u64) -> f64 {
    ppm as f64 / 10_000.0
}

fn millis(ns: u64) -> f64 {
    ns as f64 / MS as f64
}

/// The run-twice check (`docs/sim.md` §3.9): Copa beside NewReno under CoDel gives one digest from
/// its seed twice and from its trace.
#[test]
fn a_run_replays_from_its_seed_and_from_its_trace() {
    let pki = Pki::new();
    let path = Scenario {
        marking: CODEL,
        ..scenario(10_000_000, 20, 2)
    };
    twice(path.seed, |source| {
        run(
            path,
            &[(Law::Copa, None), (Law::NewReno, None)],
            &pki,
            source,
        )
        .map(|(_, record)| record)
    })
    .unwrap();
}

/// Each law alone carries its transfer, and Copa keeps the queue short. **The rule, fixed before
/// any run:** on a drop-tail path each law carries between three tenths of the link and all of it
/// after the warm-up and keeps its connection, and Copa's queue's 99th percentile is shorter than
/// NewReno's, which fills the queue (focal's `every_law_carries_a_transfer_and_answers_beside_it`
/// and `copa_answers_within_the_path_and_a_short_queue`). The share of the run Copa alone judges
/// itself competing is reported: focal measured 13.5% alone at 100 Mbit/s and 20 ms (its finding
/// 2), which A1 and A2 answer.
fn alone(paths: &[(u64, u64)], seconds: u64) {
    let pki = Pki::new();
    println!("| Path | Law | carried | queue p50 / p99 ms | competing alone |");
    println!("|---|---|---|---|---|");
    let mut failed = Vec::new();
    for (rate, rtt) in paths {
        let path = scenario(*rate, *rtt, seconds);
        let mut queue = BTreeMap::new();
        for law in Law::ALL {
            let m = measure(path, law, &pki);
            println!(
                "| {} | {law:?} | {:.1}% | {:.2} / {:.2} | {} |",
                path.name(),
                percent(m.carried_ppm),
                millis(m.queue_p50_ns),
                millis(m.queue_p99_ns),
                if law == Law::Copa {
                    format!("{:.1}%", percent(m.competing_ppm))
                } else {
                    "—".to_owned()
                },
            );
            if m.closed.is_some() {
                failed.push(format!("{} {law:?}: closed: {m:?}", path.name()));
            }
            if !(300_000..=1_000_000).contains(&m.carried_ppm) {
                failed.push(format!("{} {law:?}: carried {m:?}", path.name()));
            }
            queue.insert(law, m.queue_p99_ns);
        }
        if queue[&Law::Copa] >= queue[&Law::NewReno] {
            failed.push(format!(
                "{}: Copa's queue is not shorter: {queue:?}",
                path.name()
            ));
        }
    }
    assert!(failed.is_empty(), "{failed:#?}");
}

/// The least an incumbent's flow should carry beside a newcomer: what it carries beside the
/// deployed standard that harms it most, a flow of its own kind or CUBIC (RFC 9438), the worse-off
/// of each pair. A newcomer may harm an incumbent no more than the incumbent harms itself (Ware,
/// Mukerjee, Seshan and Sherry, HotNets 2019), and the IETF admits a sender no more aggressive than
/// CUBIC (RFC 8511 §5).
fn harm_bar(path: Scenario, incumbent: Law, pki: &Pki) -> u64 {
    let own = compete(path, &[incumbent, incumbent], pki)
        .iter()
        .map(|m| m.carried_ppm)
        .min()
        .unwrap();
    if incumbent == Law::Cubic {
        return own;
    }
    own.min(compete(path, &[Law::Cubic, incumbent], pki)[1].carried_ppm)
}

/// What Copa and an incumbent carry beside each other in one run, against the incumbent's bar.
#[derive(Debug)]
struct Beside {
    copa: u64,
    incumbent: u64,
    bar: u64,
    marks: u64,
    /// Whether Copa's datagrams stayed ECN-capable.
    ecn: bool,
    /// Copa's share of the run in the competing mode, in parts per million.
    competing: u64,
}

impl Beside {
    fn of(path: Scenario, incumbent: Law, pki: &Pki) -> Self {
        let bar = harm_bar(path, incumbent, pki);
        let pair = compete(path, &[Law::Copa, incumbent], pki);
        for m in &pair {
            assert_eq!(m.closed, None, "{}: {m:?}", path.name());
        }
        Self {
            copa: pair[0].carried_ppm,
            incumbent: pair[1].carried_ppm,
            bar,
            marks: pair[0].marked,
            ecn: pair[0].ecn,
            competing: pair[0].competing_ppm,
        }
    }

    /// Either flow carried less than a tenth of the other: focal's harness's stall.
    fn stalled(&self) -> bool {
        self.copa * 10 < self.incumbent || self.incumbent * 10 < self.copa
    }

    /// The incumbent carried nine tenths of its bar at least.
    fn within_bar(&self) -> bool {
        self.incumbent * 10 >= self.bar * 9
    }

    fn row(&self) -> String {
        format!(
            "{:.1}% / {:.1}% (bar {:.1}%, {:.3} of it; marks {}; competing {:.1}%)",
            percent(self.copa),
            percent(self.incumbent),
            percent(self.bar),
            self.incumbent as f64 / self.bar.max(1) as f64,
            self.marks,
            percent(self.competing)
        )
    }
}

/// Copa beside NewReno and beside CUBIC through one bottleneck, managed by CoDel or by nothing or by
/// a step (focal's F39). **The rule, fixed before any run:** in every seed, with CoDel or without a
/// manager, neither flow carries less than a tenth of the other; under either manager Copa's
/// datagrams stay ECN-capable, so the manager marks where it would drop; under CoDel, the queue
/// manager a sender of ECT(0) meets (RFC 7567, RFC 8289), CoDel marks Copa and each incumbent
/// carries nine tenths of its bar at least ([`harm_bar`]), seed by seed, exactly: focal judged the
/// seeds' mean against its spread. Without a manager and under the step, the shares beside the bar
/// are reported: there focal measured Copa to take more than the bar at long round trips (its
/// finding 1), which B answers, and a classic sender starves itself under the step.
fn shares(paths: &[(u64, u64)], seconds: u64) {
    let pki = Pki::new();
    println!(
        "| Path | Incumbent | Copa / incumbent carried (bar, share of it; marks; Copa competing) |"
    );
    println!("|---|---|---|");
    let mut failed = Vec::new();
    for (rate, rtt) in paths {
        for marking in [CODEL, Marking::Off, STEP] {
            for incumbent in [Law::NewReno, Law::Cubic] {
                for seed in 1..=HARM_SEEDS {
                    let path = Scenario {
                        seed,
                        marking,
                        ..scenario(*rate, *rtt, seconds)
                    };
                    let beside = Beside::of(path, incumbent, &pki);
                    println!("| {} | {incumbent:?} | {} |", path.name(), beside.row());
                    let named = format!("{} beside {incumbent:?}: {}", path.name(), beside.row());
                    if marking != STEP && beside.stalled() {
                        failed.push(format!("a stall: {named}"));
                    }
                    if marking != Marking::Off && !beside.ecn {
                        failed.push(format!("Copa stopped sending ECN: {named}"));
                    }
                    if marking == CODEL && beside.marks == 0 {
                        failed.push(format!("CoDel marked nothing: {named}"));
                    }
                    if marking == CODEL && !beside.within_bar() {
                        failed.push(format!("under nine tenths of its bar: {named}"));
                    }
                }
            }
        }
    }
    assert!(failed.is_empty(), "{failed:#?}");
}

/// Copa beside NewReno or CUBIC that stops at the run's half: Copa stops competing (focal's finding
/// 3: in focal's model, before A2, `1/δ` rose to 125 after the competitor left and the competing
/// mode never ended). **The rule, fixed before any run:** in every seed, on a drop-tail path, Copa
/// competed while the incumbent sent (it was seen), and no sample in the run's last quarter, a
/// quarter of the run after the incumbent stopped, finds Copa competing.
fn leaves(paths: &[(u64, u64)], seconds: u64) {
    let pki = Pki::new();
    println!("| Path | Incumbent | Copa competing before / in the last quarter |");
    println!("|---|---|---|");
    let mut failed = Vec::new();
    for (rate, rtt) in paths {
        for incumbent in [Law::NewReno, Law::Cubic] {
            for seed in 1..=HARM_SEEDS {
                let path = Scenario {
                    seed,
                    ..scenario(*rate, *rtt, seconds)
                };
                let pair = run(
                    path,
                    &[(Law::Copa, None), (incumbent, Some(path.end() / 2))],
                    &pki,
                    Source::Seed(seed),
                )
                .unwrap()
                .0;
                let copa = &pair[0];
                let named = format!("{} beside {incumbent:?}", path.name());
                println!(
                    "| {} | {incumbent:?} | {:.1}% / {} of {} |",
                    path.name(),
                    percent(copa.competing_ppm),
                    copa.competing_late,
                    copa.samples_late
                );
                if copa.competing_ppm == 0 {
                    failed.push(format!("never competed: {named}"));
                }
                if copa.competing_late > 0 {
                    failed.push(format!(
                        "competed after its competitor left: {} of {} samples: {named}",
                        copa.competing_late, copa.samples_late
                    ));
                }
            }
        }
    }
    assert!(failed.is_empty(), "{failed:#?}");
}

/// focal's grid of round trips and rates, as its `copa_answers_within_the_path_and_a_short_queue`
/// and its finding 2 ran them.
const ALONE_GRID: [(u64, u64); 5] = [
    (1_000_000, 20),
    (1_000_000, 100),
    (10_000_000, 20),
    (10_000_000, 100),
    (100_000_000, 20),
];
/// focal's grid for harm, as its `copa_shares_a_bottleneck_with_newreno_and_cubic` ran it.
const HARM_GRID: [(u64, u64); 4] = [
    (1_000_000, 100),
    (10_000_000, 20),
    (10_000_000, 100),
    (100_000_000, 20),
];

#[test]
fn every_law_carries_its_transfer_alone_and_copa_keeps_the_queue_short() {
    alone(&[(10_000_000, 20)], 8);
}

#[test]
#[ignore = "focal's grid, thirty seconds a run: a measurement, run by hand"]
fn every_law_alone_over_focals_grid() {
    alone(&ALONE_GRID, 30);
}

#[test]
fn copa_shares_a_bottleneck_with_newreno_and_cubic() {
    shares(&[(10_000_000, 20)], 10);
}

#[test]
#[ignore = "focal's grid, thirty seconds a run: a measurement, run by hand"]
fn copa_shares_a_bottleneck_over_focals_grid() {
    shares(&HARM_GRID, 30);
}

#[test]
fn copa_stops_competing_once_its_competitor_leaves() {
    leaves(&[(10_000_000, 20)], 10);
}

#[test]
#[ignore = "focal's grid, thirty seconds a run: a measurement, run by hand"]
fn copa_stops_competing_over_focals_grid() {
    leaves(&HARM_GRID, 30);
}

/// A ping's request on the wire at most: [`PING_BYTES`] in a STREAM frame (a type byte, an 8-byte
/// stream id, a 2-byte length; RFC 9000 §19.8) in a short-header packet (a flags byte, a 20-byte
/// connection id at most, a 4-byte packet number; §17.3.1) sealed with a 16-byte tag (RFC 9001
/// §5.3). The ping flow's load is held under its share of the link by it.
const PING_WIRE_BYTES: u64 = PING_BYTES as u64 + 1 + 8 + 2 + 1 + 20 + 4 + 16;
/// slates' bake-off's shape: the pings a run collects after its warm-up, so the p99 is the tenth
/// worst; the ping flow's share of the link, per mille, light enough to measure the bulk flow's
/// queue rather than add its own; the warm-up, twenty round trips and five seconds at least.
const PINGS: u64 = 1_000;
const PING_LOAD_PERMILLE: u64 = 10;
const WARMUP_RTTS: u64 = 20;
const MIN_WARMUP_NS: u64 = 5 * SECOND;
/// A bulk flow that carried less than this share of the link, in parts per million, stalled
/// (slates' bake-off: under 1% of the link's capacity).
const STALL_PPM: u64 = 10_000;
/// slates' fairness floor: Jain's index of two flows of one law.
const FAIRNESS_FLOOR: f64 = 0.9;

/// One scenario of the bake-off: its path, and the second bulk flow beside the first if any (its
/// round trip, and its law where it is not the law under test).
#[derive(Clone)]
struct Trial {
    name: String,
    path: Scenario,
    second: Option<(u64, Option<Law>)>,
}

/// slates' bake-off's grid (`slates` `docs/wip/BENCHMARKS.md`, 2026-09-28): rate × round trip ×
/// random loss, then the extras at 10 Mbit/s and 100 ms.
fn bakeoff_trials() -> Vec<Trial> {
    let mut trials = Vec::new();
    for rate in [64_000, 1_000_000, 10_000_000, 100_000_000] {
        for rtt in [20, 100, 300] {
            for loss_ppm in [0, 1_000, 10_000, 50_000] {
                trials.push(Trial {
                    name: format!("loss {}%", percent(u64::from(loss_ppm))),
                    path: Scenario {
                        loss: Loss::random(loss_ppm),
                        ..scenario(rate, rtt, 0)
                    },
                    second: None,
                });
            }
        }
    }
    let base = scenario(10_000_000, 100, 0);
    // A Gilbert chain at a mean of 1%: bursts of four datagrams on average (leaving the bad state
    // at 1/4 a datagram), entered at 0.01/0.99 of that, every datagram lost in it.
    let bursty = Loss::bursty(2_525, 250_000, 1_000_000);
    for (name, path, second) in [
        (
            "buffer 1/4 BDP",
            Scenario {
                buffer_permille: 250,
                ..base
            },
            None,
        ),
        (
            "buffer 4 BDP",
            Scenario {
                buffer_permille: 4_000,
                ..base
            },
            None,
        ),
        (
            "reordering ±10 ms",
            Scenario {
                jitter_ns: 10 * MS,
                ..base
            },
            None,
        ),
        (
            "burst loss 1%",
            Scenario {
                loss: bursty,
                ..base
            },
            None,
        ),
        (
            "step to 2M",
            Scenario {
                step: Some((0, 2_000_000)),
                ..base
            },
            None,
        ),
        ("same-RTT fairness", base, Some((100 * MS, None))),
        ("RTT fairness 20/100 ms", base, Some((20 * MS, None))),
        ("beside CUBIC", base, Some((100 * MS, Some(Law::Cubic)))),
    ] {
        trials.push(Trial {
            name: name.to_owned(),
            path,
            second,
        });
    }
    trials
}

/// One law's run of a trial at a seed: the bulk flow, the ping flow, and the second bulk flow.
struct Cell {
    ping_p99_ns: u64,
    carried_ppm: u64,
    stalled: bool,
    jain: Option<f64>,
}

fn bakeoff_cell(trial: &Trial, law: Law, seed: u64, pki: &Pki) -> Cell {
    let path = trial.path;
    let warm = (WARMUP_RTTS * path.rtt_ns).max(MIN_WARMUP_NS);
    let gap =
        PING_WIRE_BYTES * 8 * SECOND * 1_000 / (path.rate_bits_per_second * PING_LOAD_PERMILLE);
    let end = warm + PINGS * gap + 2 * path.rtt_ns;
    let path = Scenario {
        seed,
        seconds: end.div_ceil(SECOND),
        warm_ns: Some(warm),
        // The step comes halfway through the measured span
        step: path.step.map(|(_, rate)| (warm + PINGS * gap / 2, rate)),
        ..path
    };
    let mut specs = vec![
        FlowSpec::bulk(law),
        FlowSpec {
            ping_gap_ns: Some(gap),
            ..FlowSpec::bulk(law)
        },
    ];
    if let Some((rtt, other)) = trial.second {
        specs.push(FlowSpec {
            rtt_ns: Some(rtt),
            ..FlowSpec::bulk(other.unwrap_or(law))
        });
    }
    let measured = run_flows(path, &specs, pki, Source::Seed(seed)).unwrap().0;
    let (bulk, ping) = (&measured[0], &measured[1]);
    let stalled = measured.iter().any(|m| m.closed.is_some())
        || bulk.carried_ppm < STALL_PPM
        || ping.pings == 0;
    let jain = match trial.second {
        Some((_, None)) => {
            let (a, b) = (bulk.carried_ppm as f64, measured[2].carried_ppm as f64);
            Some((a + b).powi(2) / (2.0 * (a * a + b * b)).max(f64::MIN_POSITIVE))
        }
        _ => None,
    };
    Cell {
        ping_p99_ns: ping.ping_p99_ns,
        carried_ppm: bulk.carried_ppm,
        stalled,
        jain,
    }
}

/// The bake-off's thinnest path, 64 kbit/s and 20 ms, runs within the bounds the harness derives
/// for its network, with its bulk flow and its ping flow. A datagram there is on the wire for 150 ms
/// and then in flight whole for the 10 ms of its propagation, where the rate carries 80 bytes, and
/// the handshake fills both directions' queues at once.
#[test]
fn the_thinnest_path_runs_within_the_networks_bounds() {
    let pki = Pki::new();
    let path = scenario(64_000, 20, 10);
    let gap =
        PING_WIRE_BYTES * 8 * SECOND * 1_000 / (path.rate_bits_per_second * PING_LOAD_PERMILLE);
    for law in Law::BAKEOFF {
        let specs = [
            FlowSpec::bulk(law),
            FlowSpec {
                ping_gap_ns: Some(gap),
                ..FlowSpec::bulk(law)
            },
        ];
        run_flows(path, &specs, &pki, Source::Seed(1)).unwrap();
    }
}

/// A ping the stream limit holds back waits for the peer's credit (RFC 9000 §4.6), and the run's
/// clock moves on. The ping client once asked to wake at the present for its overdue ping while the
/// limit refused it, and the world woke it there again and again: the bake-off stopped at 10 Mbit/s
/// and 300 ms, where pings answered but not yet credited back held QUIC's default hundred streams.
/// Two streams at once hold a ping a millisecond back for most of the run; more than two pings
/// answered or waiting is the credit coming back and the held pings going on it.
#[test]
fn a_ping_held_by_the_stream_limit_waits_for_credit() {
    let pki = Pki::new();
    let path = Scenario {
        warm_ns: Some(0),
        ..scenario(10_000_000, 20, 2)
    };
    let specs = [FlowSpec {
        ping_gap_ns: Some(MS),
        streams: Some(2),
        ..FlowSpec::bulk(Law::Cubic)
    }];
    let measured = run_flows(path, &specs, &pki, Source::Seed(1)).unwrap().0;
    assert!(measured[0].pings > 2, "{:?}", measured[0]);
}

/// A slow start's overshoot at 100 Mbit/s and 100 ms keeps its connection. With a window of four
/// times the path and its queue, the drop-tail queue loses every other datagram of the overshoot's
/// last round trip, and the receiver held 1,025 spans of its stream when upstream's fixed bound
/// refused the next and closed the connection with "too many gaps in stream buffer"
/// (`crates/hyper-quic/VENDORED.md` §26).
#[test]
fn a_slow_starts_overshoot_at_a_large_window_keeps_its_connection() {
    let pki = Pki::new();
    let m = measure(scenario(100_000_000, 100, 2), Law::NewReno, &pki);
    assert!(m.closed.is_none(), "{m:?}");
}

fn geomean(ratios: &[f64]) -> f64 {
    (ratios.iter().map(|r| r.ln()).sum::<f64>() / ratios.len().max(1) as f64).exp()
}

/// slates' congestion bake-off on hyper-quic. **The rule, fixed before any run (slates'):**
/// 1. a law is disqualified if any run stalls (its bulk flow under 1% of the link, a connection
///    lost, or no ping answered), or two flows of the law score Jain's index below 0.9;
/// 2. primary: the ping p99 under load, as the geometric mean over every trial and seed of the law's
///    p99 over the best law's there, its worst reported beside it;
/// 3. secondary: the bulk goodput, as the geometric mean of the best law's over the law's.
///
/// Each line is a CSV row (trial, path, seed, law, ping p99 ms, carried %, stalled, Jain); the
/// ranking follows. `SEEDS` is 3, slates' count.
#[test]
#[ignore = "slates' bake-off, 56 trials of four laws at three seeds: a measurement, run by hand"]
fn the_congestion_bakeoff_over_slates_grid() {
    let pki = Pki::new();
    let trials = bakeoff_trials();
    // Per law: its p99 and goodput ratios to the best, its worst p99 ratio and where, its reasons
    // for disqualification
    let mut p99_ratios: BTreeMap<Law, Vec<f64>> = BTreeMap::new();
    let mut goodput_ratios: BTreeMap<Law, Vec<f64>> = BTreeMap::new();
    let mut worst: BTreeMap<Law, (f64, String)> = BTreeMap::new();
    let mut out: BTreeMap<Law, Vec<String>> = BTreeMap::new();
    println!("trial,path,seed,law,ping_p99_ms,carried_pct,stalled,jain");
    for trial in &trials {
        for seed in 1..=3 {
            let cells: Vec<(Law, Cell)> = Law::BAKEOFF
                .iter()
                .map(|law| (*law, bakeoff_cell(trial, *law, seed, &pki)))
                .collect();
            for (law, cell) in &cells {
                println!(
                    "{},{},{seed},{law:?},{:.3},{:.2},{},{}",
                    trial.name,
                    trial.path.link_name(),
                    millis(cell.ping_p99_ns),
                    percent(cell.carried_ppm),
                    cell.stalled,
                    cell.jain.map_or(String::new(), |j| format!("{j:.3}"))
                );
                if cell.stalled {
                    out.entry(*law).or_default().push(format!(
                        "stalled: {} {} seed {seed}",
                        trial.name,
                        trial.path.link_name()
                    ));
                }
                if let Some(jain) = cell.jain
                    && jain < FAIRNESS_FLOOR
                {
                    out.entry(*law)
                        .or_default()
                        .push(format!("Jain {jain:.3}: {} seed {seed}", trial.name));
                }
            }
            let best_p99 = cells
                .iter()
                .map(|(_, c)| c.ping_p99_ns.max(1))
                .min()
                .unwrap();
            let best_goodput = cells
                .iter()
                .map(|(_, c)| c.carried_ppm.max(1))
                .max()
                .unwrap();
            for (law, cell) in &cells {
                let p99 = cell.ping_p99_ns.max(1) as f64 / best_p99 as f64;
                p99_ratios.entry(*law).or_default().push(p99);
                goodput_ratios
                    .entry(*law)
                    .or_default()
                    .push(best_goodput as f64 / cell.carried_ppm.max(1) as f64);
                let w = worst.entry(*law).or_insert((0.0, String::new()));
                if p99 > w.0 {
                    *w = (
                        p99,
                        format!("{} {} seed {seed}", trial.name, trial.path.link_name()),
                    );
                }
            }
        }
    }
    println!();
    println!(
        "| law | ping p99 vs best (geomean) | worst p99 vs best | goodput shortfall (geomean) | disqualified by |"
    );
    println!("|---|---|---|---|---|");
    for law in Law::BAKEOFF {
        let reasons = out.get(&law).map_or_else(
            || "—".to_owned(),
            |r| format!("{} ({} runs)", r[0], r.len()),
        );
        println!(
            "| {law:?} | {:.3} | {:.2} ({}) | {:.3} | {reasons} |",
            geomean(&p99_ratios[&law]),
            worst[&law].0,
            worst[&law].1,
            geomean(&goodput_ratios[&law]),
        );
    }
}
