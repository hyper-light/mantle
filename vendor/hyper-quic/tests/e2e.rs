//! hyper-quic between real processes over real UDP sockets on loopback (CLAUDE.md §1a), and against
//! unmodified upstream quinn-proto 0.11.18 in the other process: the interoperation oracle slates'
//! plan asks for (its `docs/wip/transport-quic.md` §4, stage 5).
//!
//! This binary is every process. Run as a test it is the client (or, for one scenario, the server)
//! and spawns itself as the peer process with `HQ_SERVER` or `HQ_CLIENT` set, and as a relay with
//! `HQ_RELAY` set; the processes share nothing but the kernel's sockets. Every scenario runs three
//! ways: hyper-quic on both sides, a hyper-quic client against an upstream server, and an upstream
//! client against a hyper-quic server.
//!
//! - `handshake`: a full handshake, then a resumed one whose first bytes go in 0-RTT data, which
//!   both sides must report accepted (the interop runner's `resumption` and `zerortt` cases);
//! - `streams`: three bidirectional streams at once, of 2, 3 and 5 MiB each way (its `transfer`
//!   case's files);
//! - `lossy`: 2 MiB each way through a relay process that drops one datagram in fifty in each
//!   direction (its `transferloss` case's 2 % rate) and swaps one in fifty with the next;
//! - `migration`: 2 MiB each way while the client moves to a new socket and tells its connection
//!   (an active migration, RFC 9000 §9 and §9.5: a new connection ID on the new path), and the
//!   server validates the new path (§8.2) (its `connectionmigration` case);
//! - `rebinding`: the same, the client's socket replaced without its connection knowing, as a NAT
//!   rebinding does (§9.3) (its `rebind-port` case);
//! - `killed-server`: the server process is killed with SIGKILL mid-upload; the client's
//!   connection ends timed out once its idle timeout has passed with nothing heard (§10.1), and a
//!   new server process is reached after;
//! - `killed-client`: the client process is killed mid-upload; the server's connection ends the
//!   same way, and the server serves the next client.
//!
//! Every stream carries a pattern no lost, duplicated or misplaced byte can match, checked byte by
//! byte on both sides.
//!
//! Every wait is for a fact: a datagram, a timer the connection set, an event it reports. A wait
//! ends when its fact holds, when the connection reports its end, or, by the protocol's own law,
//! when the connection has moved no stream byte for as long as its idle timeout: QUIC itself ends a
//! connection that hears nothing for that long (RFC 9000 §10.1), so one that hears its peer and
//! still moves nothing for as long is stuck. Time the test did not spend listening (its process
//! starved) counts toward neither.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::type_complexity,
    missing_docs
)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::net::{SocketAddr, UdpSocket};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use bytes::BytesMut;

/// The most a turn waits on its socket when no timer is due: a bound on one turn of the loop, not
/// on any outcome, which arrives as a datagram or a timer and ends the wait first.
const IDLE_TURN: Duration = Duration::from_millis(50);
/// The largest UDP payload (RFC 9000 §18.2's `max_udp_payload_size` ceiling): the receive buffer.
const MAX_DATAGRAM: usize = 65_527;
/// The smallest datagram that carries a full-size QUIC packet: RFC 9000 §14.1's 1,200 bytes. The
/// relay's schedule counts these only, the datagrams that carry data once a transfer runs.
const FULL_SIZE: usize = 1_200;
/// One datagram in this many is dropped in each direction: the 2 % loss of the QUIC interop
/// runner's `transferloss` case (quic-interop-runner `testcases_quic.py`, `740c05a`).
const DROP_EVERY: u64 = 50;
/// The position within each [`DROP_EVERY`] at which a datagram is held until the next one in its
/// direction has passed: half-way, so that a hold never meets a drop.
const HOLD_AT: u64 = DROP_EVERY / 2;
/// The control datagram after which the relay reports what it did and ends. A datagram of exactly
/// these 13 bytes is no QUIC packet: the shortest packet is a header byte, a connection ID, a packet
/// number and a 16-byte AEAD tag (RFC 9000 §17.3, RFC 9001 §5.3), longer than this with the 8-byte
/// connection IDs both implementations issue.
const RELAY_REPORT: &[u8] = b"relay: report";
/// What the server reports once a stream's upload has brought it this many bytes, for the killed
/// scenarios to kill a process mid-stream on a fact.
const PROGRESS_MARK: usize = 1 << 20;

/// Which implementation drives one side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Imp {
    Hyper,
    Upstream,
}

impl Imp {
    fn name(self) -> &'static str {
        match self {
            Self::Hyper => "hyper",
            Self::Upstream => "upstream",
        }
    }
    fn parse(name: &str) -> Self {
        match name {
            "hyper" => Self::Hyper,
            "upstream" => Self::Upstream,
            _ => panic!("no implementation {name}"),
        }
    }
}

/// The stream body for stream `index`: byte `at` of it.
fn pattern(index: u64, at: usize) -> u8 {
    (at % 251) as u8 ^ ((index * 37) as u8).wrapping_add((at >> 8) as u8)
}

/// A connection event, as either implementation reports it: its end, with its reason, or any
/// other, which the drivers act on by moving the streams.
#[derive(Debug)]
enum Event {
    Lost(String),
    Other,
}

/// What a scenario reads of a connection's statistics.
#[derive(Clone, Copy, Debug, Default)]
struct Stats {
    lost_packets: u64,
    path_challenges_sent: u64,
    path_responses_sent: u64,
    connection_ids_retired: u64,
}

/// A QUIC endpoint with at most one connection at a time, either implementation.
trait Quic {
    type Stream: Copy + Ord + Debug;
    /// Whether the implementation's own driver sends what the datagrams it took produced before
    /// it fires the timers due, the order a turn then takes (`Wire::turn`).
    const TRANSMIT_BEFORE_TIMERS: bool;
    fn index(stream: Self::Stream) -> u64;
    fn connect(&mut self, now: Instant, server: SocketAddr);
    /// Takes one datagram; a response the endpoint sends at once, into `out`, and its destination.
    fn datagram(
        &mut self,
        now: Instant,
        from: SocketAddr,
        data: &[u8],
        out: &mut Vec<u8>,
    ) -> Option<SocketAddr>;
    /// The next datagram to send, into `out`, and its destination.
    fn transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> Option<SocketAddr>;
    fn timeout(&mut self) -> Option<Instant>;
    fn fire(&mut self, now: Instant);
    fn event(&mut self) -> Option<Event>;
    fn open(&mut self) -> Option<Self::Stream>;
    fn accept(&mut self) -> Option<Self::Stream>;
    /// Writes as much of `data` as the stream takes now.
    fn write(&mut self, stream: Self::Stream, data: &[u8]) -> usize;
    fn finish(&mut self, stream: Self::Stream);
    /// Hands what the stream has to `sink`; whether it has finished.
    fn read(&mut self, stream: Self::Stream, sink: &mut dyn FnMut(&[u8])) -> bool;
    fn stats(&self) -> Stats;
    fn remote(&self) -> SocketAddr;
    fn local_address_changed(&mut self);
    fn has_0rtt(&self) -> bool;
    fn accepted_0rtt(&self) -> bool;
    fn is_handshaking(&self) -> bool;
    /// Whether a client has confirmed the handshake: it received HANDSHAKE_DONE (RFC 9001 §4.1.2)
    fn confirmed(&self) -> bool;
    fn close(&mut self, now: Instant);
    /// Whether the endpoint still holds a connection that has not drained.
    fn live(&self) -> bool;
}

/// hyper-quic's side.
mod hyper {
    use super::*;
    use hyper_quic::{
        ClientConfig, ClientConfigHandle, Connection, ConnectionHandle, DatagramEvent, Dir,
        Endpoint, EndpointConfig, ServerConfig, StreamId, VarInt,
    };

    pub(super) struct Side {
        endpoint: Endpoint,
        client: Option<ClientConfigHandle>,
        conn: Option<(ConnectionHandle, Connection)>,
    }

    pub(super) fn server(certificate: &[u8], key: &[u8]) -> Side {
        let config = ServerConfig::with_single_cert(
            vec![certificate.to_vec().into()],
            hyper_quic::rustls::pki_types::PrivatePkcs8KeyDer::from(key.to_vec()).into(),
        )
        .unwrap();
        Side {
            endpoint: Endpoint::new(EndpointConfig::default(), Some(config), true, None).unwrap(),
            client: None,
            conn: None,
        }
    }

    pub(super) fn client(certificate: &[u8]) -> Side {
        let mut endpoint = Endpoint::new(EndpointConfig::default(), None, true, None).unwrap();
        let mut roots = hyper_quic::rustls::RootCertStore::empty();
        roots.add(certificate.to_vec().into()).unwrap();
        let client = endpoint
            .insert_client_config(ClientConfig::with_root_certificates(roots).unwrap())
            .unwrap();
        Side {
            endpoint,
            client: Some(client),
            conn: None,
        }
    }

    impl Quic for Side {
        type Stream = StreamId;
        // hyper-quic's consumers drive it either way, and a timer fired after the datagrams but
        // before their answer is the harder order (`a_rebinding_leaves_no_stale_probe_timeout`).
        const TRANSMIT_BEFORE_TIMERS: bool = false;
        fn index(stream: StreamId) -> u64 {
            stream.index()
        }
        fn connect(&mut self, now: Instant, server: SocketAddr) {
            let client = self.client.unwrap();
            self.conn = Some(
                self.endpoint
                    .connect(now, client, server, "localhost", None)
                    .unwrap(),
            );
        }
        fn datagram(
            &mut self,
            now: Instant,
            from: SocketAddr,
            data: &[u8],
            out: &mut Vec<u8>,
        ) -> Option<SocketAddr> {
            out.clear();
            match self
                .endpoint
                .handle(now, from, None, None, BytesMut::from(data), out)
            {
                Some(DatagramEvent::NewConnection(incoming)) => {
                    match self.endpoint.accept(incoming, now, out, None, None) {
                        Ok(conn) => {
                            self.conn = Some(conn);
                            None
                        }
                        Err(error) => error.response.map(|t| {
                            out.truncate(t.size);
                            t.destination
                        }),
                    }
                }
                Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                    if let Some((own, conn)) = &mut self.conn
                        && *own == handle
                    {
                        conn.handle_event(event, self.endpoint.configs_mut());
                    }
                    None
                }
                Some(DatagramEvent::Response(t)) => {
                    out.truncate(t.size);
                    Some(t.destination)
                }
                None => None,
            }
        }
        fn transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> Option<SocketAddr> {
            let (handle, conn) = self.conn.as_mut()?;
            while let Some(event) = conn.poll_endpoint_events() {
                if let Some(event) = self.endpoint.handle_event(*handle, event) {
                    conn.handle_event(event, self.endpoint.configs_mut());
                }
            }
            out.clear();
            conn.poll_transmit(now, 1, out, self.endpoint.configs())
                .map(|t| {
                    out.truncate(t.size);
                    t.destination
                })
        }
        fn timeout(&mut self) -> Option<Instant> {
            self.conn.as_mut()?.1.poll_timeout()
        }
        fn fire(&mut self, now: Instant) {
            if let Some((_, conn)) = &mut self.conn {
                conn.handle_timeout(now);
            }
        }
        fn event(&mut self) -> Option<Event> {
            let conn = &mut self.conn.as_mut()?.1;
            let Some(event) = conn.poll() else {
                // A connection is forgotten only once it has drained and said why it ended, and
                // after `transmit` gave the endpoint its last events.
                if conn.is_drained() {
                    self.conn = None;
                }
                return None;
            };
            Some(match event {
                hyper_quic::Event::ConnectionLost { reason } => Event::Lost(format!("{reason:?}")),
                _ => Event::Other,
            })
        }
        fn open(&mut self) -> Option<StreamId> {
            self.conn.as_mut()?.1.streams().open(Dir::Bi)
        }
        fn accept(&mut self) -> Option<StreamId> {
            self.conn.as_mut()?.1.streams().accept(Dir::Bi)
        }
        fn write(&mut self, stream: StreamId, data: &[u8]) -> usize {
            let Some((_, conn)) = &mut self.conn else {
                return 0;
            };
            conn.send_stream(stream).write(data).unwrap_or(0)
        }
        fn finish(&mut self, stream: StreamId) {
            if let Some((_, conn)) = &mut self.conn {
                conn.send_stream(stream).finish().unwrap();
            }
        }
        fn read(&mut self, stream: StreamId, sink: &mut dyn FnMut(&[u8])) -> bool {
            let Some((_, conn)) = &mut self.conn else {
                return false;
            };
            let mut recv = conn.recv_stream(stream);
            let Ok(mut chunks) = recv.read(true) else {
                return false;
            };
            let mut finished = false;
            loop {
                match chunks.next(usize::MAX) {
                    Ok(Some(chunk)) => sink(&chunk.bytes),
                    Ok(None) => {
                        finished = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
            let _ = chunks.finalize();
            finished
        }
        fn stats(&self) -> Stats {
            let Some((_, conn)) = &self.conn else {
                return Stats::default();
            };
            let stats = conn.stats();
            Stats {
                lost_packets: stats.path.lost_packets,
                path_challenges_sent: stats.frame_tx.path_challenge,
                path_responses_sent: stats.frame_tx.path_response,
                connection_ids_retired: stats.frame_tx.retire_connection_id,
            }
        }
        fn remote(&self) -> SocketAddr {
            self.conn.as_ref().unwrap().1.remote_address()
        }
        fn local_address_changed(&mut self) {
            self.conn.as_mut().unwrap().1.local_address_changed();
        }
        fn has_0rtt(&self) -> bool {
            self.conn.as_ref().unwrap().1.has_0rtt()
        }
        fn confirmed(&self) -> bool {
            self.conn
                .as_ref()
                .is_some_and(|(_, conn)| conn.stats().frame_rx.handshake_done > 0)
        }
        fn accepted_0rtt(&self) -> bool {
            self.conn.as_ref().unwrap().1.accepted_0rtt()
        }
        fn is_handshaking(&self) -> bool {
            self.conn.as_ref().unwrap().1.is_handshaking()
        }
        fn close(&mut self, now: Instant) {
            if let Some((_, conn)) = &mut self.conn {
                conn.close(now, VarInt::from_u32(0), bytes::Bytes::new());
            }
        }
        fn live(&self) -> bool {
            self.conn.is_some()
        }
    }
}

/// Upstream quinn-proto's side: the same, with its configurations shared by `Arc`.
mod upstream {
    use std::sync::Arc;

    use super::*;
    use upstream_quinn_proto::{
        ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig,
        ServerConfig, StreamId, VarInt,
    };

    pub(super) struct Side {
        endpoint: Endpoint,
        client: Option<ClientConfig>,
        conn: Option<(ConnectionHandle, Connection)>,
    }

    pub(super) fn server(certificate: &[u8], key: &[u8]) -> Side {
        let config = ServerConfig::with_single_cert(
            vec![certificate.to_vec().into()],
            upstream_rustls::pki_types::PrivatePkcs8KeyDer::from(key.to_vec()).into(),
        )
        .unwrap();
        Side {
            endpoint: Endpoint::new(
                Arc::new(EndpointConfig::default()),
                Some(Arc::new(config)),
                true,
                None,
            ),
            client: None,
            conn: None,
        }
    }

    pub(super) fn client(certificate: &[u8]) -> Side {
        let mut roots = upstream_rustls::RootCertStore::empty();
        roots.add(certificate.to_vec().into()).unwrap();
        Side {
            endpoint: Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None),
            client: Some(ClientConfig::with_root_certificates(Arc::new(roots)).unwrap()),
            conn: None,
        }
    }

    impl Quic for Side {
        type Stream = StreamId;
        // quinn's `ConnectionDriver::poll` (quinn/src/connection.rs) processes the connection's
        // events, then transmits, then drives its timer. quinn-proto 0.11.18, and quinn's main
        // at d45b513, keep the loss-detection timer armed for the old path across `migrate`, and
        // it asserts if that timer fires before anything is sent on the new path: an order its own
        // driver never takes, since the first send re-arms the timer.
        const TRANSMIT_BEFORE_TIMERS: bool = true;
        fn index(stream: StreamId) -> u64 {
            stream.index()
        }
        fn connect(&mut self, now: Instant, server: SocketAddr) {
            let client = self.client.clone().unwrap();
            self.conn = Some(
                self.endpoint
                    .connect(now, client, server, "localhost")
                    .unwrap(),
            );
        }
        fn datagram(
            &mut self,
            now: Instant,
            from: SocketAddr,
            data: &[u8],
            out: &mut Vec<u8>,
        ) -> Option<SocketAddr> {
            out.clear();
            match self
                .endpoint
                .handle(now, from, None, None, BytesMut::from(data), out)
            {
                Some(DatagramEvent::NewConnection(incoming)) => {
                    match self.endpoint.accept(incoming, now, out, None) {
                        Ok(conn) => {
                            self.conn = Some(conn);
                            None
                        }
                        Err(error) => error.response.map(|t| {
                            out.truncate(t.size);
                            t.destination
                        }),
                    }
                }
                Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                    if let Some((own, conn)) = &mut self.conn
                        && *own == handle
                    {
                        conn.handle_event(event);
                    }
                    None
                }
                Some(DatagramEvent::Response(t)) => {
                    out.truncate(t.size);
                    Some(t.destination)
                }
                None => None,
            }
        }
        fn transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> Option<SocketAddr> {
            let (handle, conn) = self.conn.as_mut()?;
            while let Some(event) = conn.poll_endpoint_events() {
                if let Some(event) = self.endpoint.handle_event(*handle, event) {
                    conn.handle_event(event);
                }
            }
            out.clear();
            conn.poll_transmit(now, 1, out).map(|t| {
                out.truncate(t.size);
                t.destination
            })
        }
        fn timeout(&mut self) -> Option<Instant> {
            self.conn.as_mut()?.1.poll_timeout()
        }
        fn fire(&mut self, now: Instant) {
            if let Some((_, conn)) = &mut self.conn {
                conn.handle_timeout(now);
            }
        }
        fn event(&mut self) -> Option<Event> {
            let conn = &mut self.conn.as_mut()?.1;
            let Some(event) = conn.poll() else {
                // A connection is forgotten only once it has drained and said why it ended, and
                // after `transmit` gave the endpoint its last events.
                if conn.is_drained() {
                    self.conn = None;
                }
                return None;
            };
            Some(match event {
                upstream_quinn_proto::Event::ConnectionLost { reason } => {
                    Event::Lost(format!("{reason:?}"))
                }
                _ => Event::Other,
            })
        }
        fn open(&mut self) -> Option<StreamId> {
            self.conn.as_mut()?.1.streams().open(Dir::Bi)
        }
        fn accept(&mut self) -> Option<StreamId> {
            self.conn.as_mut()?.1.streams().accept(Dir::Bi)
        }
        fn write(&mut self, stream: StreamId, data: &[u8]) -> usize {
            let Some((_, conn)) = &mut self.conn else {
                return 0;
            };
            conn.send_stream(stream).write(data).unwrap_or(0)
        }
        fn finish(&mut self, stream: StreamId) {
            if let Some((_, conn)) = &mut self.conn {
                conn.send_stream(stream).finish().unwrap();
            }
        }
        fn read(&mut self, stream: StreamId, sink: &mut dyn FnMut(&[u8])) -> bool {
            let Some((_, conn)) = &mut self.conn else {
                return false;
            };
            let mut recv = conn.recv_stream(stream);
            let Ok(mut chunks) = recv.read(true) else {
                return false;
            };
            let mut finished = false;
            loop {
                match chunks.next(usize::MAX) {
                    Ok(Some(chunk)) => sink(&chunk.bytes),
                    Ok(None) => {
                        finished = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
            let _ = chunks.finalize();
            finished
        }
        fn stats(&self) -> Stats {
            let Some((_, conn)) = &self.conn else {
                return Stats::default();
            };
            let stats = conn.stats();
            Stats {
                lost_packets: stats.path.lost_packets,
                path_challenges_sent: stats.frame_tx.path_challenge,
                path_responses_sent: stats.frame_tx.path_response,
                connection_ids_retired: stats.frame_tx.retire_connection_id,
            }
        }
        fn remote(&self) -> SocketAddr {
            self.conn.as_ref().unwrap().1.remote_address()
        }
        fn local_address_changed(&mut self) {
            self.conn.as_mut().unwrap().1.local_address_changed();
        }
        fn has_0rtt(&self) -> bool {
            self.conn.as_ref().unwrap().1.has_0rtt()
        }
        fn confirmed(&self) -> bool {
            self.conn
                .as_ref()
                .is_some_and(|(_, conn)| conn.stats().frame_rx.handshake_done > 0)
        }
        fn accepted_0rtt(&self) -> bool {
            self.conn.as_ref().unwrap().1.accepted_0rtt()
        }
        fn is_handshaking(&self) -> bool {
            self.conn.as_ref().unwrap().1.is_handshaking()
        }
        fn close(&mut self, now: Instant) {
            if let Some((_, conn)) = &mut self.conn {
                conn.close(now, VarInt::from_u32(0), bytes::Bytes::new());
            }
        }
        fn live(&self) -> bool {
            self.conn.is_some()
        }
    }
}

/// One endpoint's socket, driven a turn at a time; the socket can be replaced, as a client that
/// moves does.
struct Wire {
    socket: UdpSocket,
    buffer: Vec<u8>,
    out: Vec<u8>,
    /// When a datagram last arrived, and from where.
    heard: Option<(Instant, SocketAddr)>,
    /// The time the turns have spent listening on the socket, all told: the quiet rule counts the
    /// part of it since the streams last moved.
    listened: Duration,
}

impl Wire {
    fn bind() -> Self {
        Self {
            socket: UdpSocket::bind("127.0.0.1:0").unwrap(),
            buffer: vec![0; MAX_DATAGRAM],
            out: Vec::with_capacity(MAX_DATAGRAM),
            heard: None,
            listened: Duration::ZERO,
        }
    }
    fn address(&self) -> SocketAddr {
        self.socket.local_addr().unwrap()
    }
    /// Moves to a new socket on a new port; the old one is closed.
    fn rebind(&mut self) {
        self.socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    }
    fn flush(&mut self, quic: &mut impl Quic) {
        while let Some(destination) = quic.transmit(Instant::now(), &mut self.out) {
            // A datagram the kernel refuses is a lost datagram: QUIC recovers it.
            let _ = self.socket.send_to(&self.out, destination);
        }
    }
    /// Sends what is due, waits for a datagram until the next timer without taking it
    /// (`hyper_measure::wait::arrives`), takes everything that has arrived, fires the timers due,
    /// and sends again; an implementation whose driver sends before its timers sends in between.
    fn turn<Q: Quic>(&mut self, quic: &mut Q) {
        self.flush(quic);
        let began = Instant::now();
        let wait = quic
            .timeout()
            .map_or(IDLE_TURN, |due| due.saturating_duration_since(began))
            .min(IDLE_TURN);
        if !wait.is_zero() {
            self.socket.set_nonblocking(false).unwrap();
            let listening = Instant::now();
            let _ = hyper_measure::wait::arrives(&self.socket, Some(wait), &mut self.buffer);
            self.listened += listening.elapsed();
        }
        self.socket.set_nonblocking(true).unwrap();
        loop {
            match self.socket.recv_from(&mut self.buffer) {
                Ok((length, from)) => {
                    let now = Instant::now();
                    self.heard = Some((now, from));
                    let data = self.buffer[..length].to_vec();
                    if let Some(destination) = quic.datagram(now, from, &data, &mut self.out) {
                        let _ = self.socket.send_to(&self.out, destination);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                // A reset (Windows, after a send to a closed port) and the like: the socket is
                // still good, and what else arrived is still to be read.
                Err(_) => {}
            }
        }
        if Q::TRANSMIT_BEFORE_TIMERS {
            self.flush(quic);
        }
        let now = Instant::now();
        if quic.timeout().is_some_and(|due| due <= now) {
            quic.fire(now);
        }
        self.flush(quic);
    }
}

/// The idle timeout both implementations run with by default, `TransportConfig`'s 30 s:
/// RFC 9308 §3.2 finds timeouts shorter than 30 s make transient interruptions (a virtual
/// machine's migration, lost coverage) harder to survive. The quiet rule's period.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The streams one side of a connection carries: what each has received and sent, and the uploads
/// still to open, which a client opens once the server's stream limit allows (RFC 9000 §4.6: none
/// before its transport parameters arrive, unless remembered for 0-RTT).
struct Streams<S> {
    open: BTreeMap<S, Flow>,
    to_open: Vec<usize>,
    /// Whether streams in `open` were opened before the handshake ended, on 0-RTT keys.
    early: bool,
    /// The streams opened again after the server rejected the 0-RTT data they were opened with.
    reopened: usize,
    /// The most bytes of each stream written so far, where a scenario holds the rest back.
    hold: Option<usize>,
}

#[derive(Default)]
struct Flow {
    /// Bytes received, each checked against the pattern.
    received: usize,
    /// Whether the peer finished the stream.
    ended: bool,
    /// Bytes to send, and sent.
    to_send: usize,
    sent: usize,
    finished: bool,
}

impl<S: Copy + Ord + Debug> Streams<S> {
    fn new() -> Self {
        Self {
            open: BTreeMap::new(),
            to_open: Vec::new(),
            early: false,
            reopened: 0,
            hold: None,
        }
    }
    /// Moves every stream as far as it goes now; whether any byte moved. `index` is the stream's
    /// pattern; a byte that differs fails the test.
    fn pump<Q: Quic<Stream = S>>(&mut self, quic: &mut Q) -> bool {
        let mut moved = false;
        if self.early && !quic.is_handshaking() {
            self.early = false;
            if !quic.accepted_0rtt() {
                // The server rejected the early data, as one that holds none of the sessions a
                // ticket names does: the transport has discarded every stream opened on it, and
                // the client resets its own state for them (RFC 9001 §4.6.2). They open again,
                // in order, on 1-RTT keys, from their first byte.
                let opened = std::mem::take(&mut self.open);
                self.reopened += opened.len();
                self.to_open
                    .splice(0..0, opened.into_values().map(|flow| flow.to_send));
                moved = true;
            }
        }
        while let Some(&size) = self.to_open.first() {
            let Some(stream) = quic.open() else { break };
            self.early |= quic.is_handshaking();
            self.to_open.remove(0);
            self.open.insert(
                stream,
                Flow {
                    to_send: size,
                    ..Flow::default()
                },
            );
            moved = true;
        }
        for (&stream, flow) in &mut self.open {
            let index = Q::index(stream);
            if !flow.ended {
                let mut received = flow.received;
                let ended = quic.read(stream, &mut |bytes: &[u8]| {
                    for (offset, byte) in bytes.iter().enumerate() {
                        let at = received + offset;
                        assert_eq!(*byte, pattern(index, at), "stream {index} byte {at}");
                    }
                    received += bytes.len();
                });
                moved |= received != flow.received || ended;
                flow.received = received;
                flow.ended = ended;
            }
            let to_send = self
                .hold
                .map_or(flow.to_send, |hold| hold.min(flow.to_send));
            while flow.sent < to_send {
                let end = to_send.min(flow.sent + 65_536);
                let chunk: Vec<u8> = (flow.sent..end).map(|at| pattern(index, at)).collect();
                let written = quic.write(stream, &chunk);
                if written == 0 {
                    break;
                }
                flow.sent += written;
                moved = true;
            }
            if flow.sent == flow.to_send && !flow.finished && flow.to_send > 0 {
                quic.finish(stream);
                flow.finished = true;
                moved = true;
            }
        }
        moved
    }
}

/// Turns `quic` until `done` holds of it, its socket and its streams. Fails, with the
/// connection's own words, when the connection ends first, or when no stream byte moved for the
/// idle timeout of listening (the module's quiet rule), or for `quiet` more when the wait's fact is
/// the connection's own end.
fn drive<Q: Quic>(
    wire: &mut Wire,
    quic: &mut Q,
    streams: &mut Streams<Q::Stream>,
    what: &str,
    mut done: impl FnMut(&mut Q, &mut Wire, &mut Streams<Q::Stream>) -> bool,
) -> Result<(), String> {
    let mut moved_at = wire.listened;
    loop {
        wire.turn(quic);
        while let Some(event) = quic.event() {
            if let Event::Lost(reason) = event {
                return Err(reason);
            }
        }
        if streams.pump(quic) {
            moved_at = wire.listened;
        }
        if done(quic, wire, &mut *streams) {
            return Ok(());
        }
        let quiet = wire.listened - moved_at;
        if quiet > IDLE_TIMEOUT {
            return Err(format!(
                "{what}: no stream byte moved for {quiet:?} of listening"
            ));
        }
    }
}

/// Turns `quic` until its connection ends, which a connection whose peer was killed does by its
/// idle timeout (RFC 9000 §10.1): the end's reason, and how long after the peer's last datagram
/// it came. The failure guard is a second idle timeout of listening past the first, far past
/// when the connection's own timer is due.
fn until_lost<Q: Quic>(wire: &mut Wire, quic: &mut Q) -> (String, Duration) {
    let began = wire.listened;
    loop {
        wire.turn(quic);
        while let Some(event) = quic.event() {
            if let Event::Lost(reason) = event {
                let heard = wire.heard.map_or(Duration::ZERO, |(at, _)| at.elapsed());
                return (reason, heard);
            }
        }
        assert!(
            wire.listened - began < IDLE_TIMEOUT * 2,
            "the connection outlived its idle timeout twice over"
        );
    }
}

/// Lets a closed connection drain, so that the endpoint holds nothing of it.
fn drain<Q: Quic>(wire: &mut Wire, quic: &mut Q) {
    while quic.live() {
        wire.turn(quic);
        while quic.event().is_some() {}
    }
}

// ---------------------------------------------------------------------------------------------
// The processes.

/// The test PKI: one self-signed certificate for `localhost`, made by the test and handed to the
/// processes it spawns.
struct Pki {
    certificate: Vec<u8>,
    key: Vec<u8>,
}

impl Pki {
    fn new() -> Self {
        let made = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        Self {
            certificate: made.cert.der().to_vec(),
            key: made.signing_key.serialize_der(),
        }
    }
    fn from_env() -> Self {
        Self {
            certificate: unhex(&std::env::var("HQ_CERT").unwrap()),
            key: unhex(&std::env::var("HQ_KEY").unwrap()),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

/// A connection's streams to open, each with the bytes it uploads.
fn streams_of<S: Copy + Ord + Debug>(sizes: &[usize]) -> Streams<S> {
    let mut streams = Streams::new();
    streams.to_open = sizes.to_vec();
    streams
}

/// Whether every stream has opened, uploaded all it had and read the whole echo of it.
fn echoed<S>(streams: &Streams<S>) -> bool {
    streams.to_open.is_empty()
        && streams
            .open
            .values()
            .all(|flow| flow.finished && flow.ended && flow.received == flow.to_send)
}

/// The server: serves one connection after another, echoing every stream: once a stream's upload
/// ends it sends as many bytes back, in the stream's pattern. It reports a connection's first
/// stream to bring [`PROGRESS_MARK`] bytes, and each connection's end with what it saw: `report`
/// takes each line (`Some`), and is asked between turns while no connection has begun whether to
/// wait on (`None`), so that a server the test drives stops when the client it waits for is gone.
/// It ends when `report` says so.
fn serve<Q: Quic>(quic: &mut Q, wire: &mut Wire, mut report: impl FnMut(Option<String>) -> bool) {
    loop {
        let mut streams = Streams::new();
        let mut marked = false;
        let mut begun = false;
        loop {
            wire.turn(quic);
            let mut lost = None;
            while let Some(event) = quic.event() {
                if let Event::Lost(reason) = event {
                    // What the connection saw, read as it ends: once it has drained, the next
                    // poll forgets it.
                    // A server installs 0-RTT keys only when it accepts the client's early data:
                    // its `has_0rtt` is that acceptance (a client's `accepted_0rtt` is the
                    // client's view).
                    let stats = quic.stats();
                    let heard = wire.heard.map_or(Duration::ZERO, |(at, _)| at.elapsed());
                    lost = Some(format!(
                        "served {reason} 0rtt={} remote={} challenges={} lost={} heard={}",
                        quic.has_0rtt(),
                        quic.remote(),
                        stats.path_challenges_sent,
                        stats.lost_packets,
                        heard.as_millis()
                    ));
                }
            }
            if let Some(line) = lost {
                drain(wire, quic);
                if !report(Some(line)) {
                    return;
                }
                break;
            }
            if !quic.live() {
                if !begun && !report(None) {
                    return;
                }
                continue;
            }
            begun = true;
            while let Some(stream) = quic.accept() {
                streams.open.insert(stream, Flow::default());
            }
            streams.pump(quic);
            for flow in streams.open.values_mut() {
                if flow.ended && flow.to_send == 0 {
                    flow.to_send = flow.received;
                }
            }
            if !marked
                && streams
                    .open
                    .values()
                    .any(|flow| flow.received >= PROGRESS_MARK)
            {
                marked = true;
                if !report(Some("progress".into())) {
                    return;
                }
            }
        }
    }
}

/// The process that serves with `imp`.
fn server_process(imp: Imp) {
    let pki = Pki::from_env();
    let mut wire = Wire::bind();
    println!("listening {}", wire.address().port());
    std::io::stdout().flush().unwrap();
    // The server process reports each connection's end, and serves until the test ends it; a
    // stream's progress matters only to a server the test drives itself (killed-client).
    let print = |line: Option<String>| {
        if let Some(line) = line.filter(|line| line != "progress") {
            println!("{line}");
        }
        std::io::stdout().flush().is_ok()
    };
    match imp {
        Imp::Hyper => serve(
            &mut hyper::server(&pki.certificate, &pki.key),
            &mut wire,
            print,
        ),
        Imp::Upstream => serve(
            &mut upstream::server(&pki.certificate, &pki.key),
            &mut wire,
            print,
        ),
    }
}

/// The client process of the killed-client scenario: `upload` sends one stream without end until
/// the process is killed; otherwise one mebibyte is echoed and the process ends saying so.
fn client_process(imp: Imp, server: SocketAddr, upload: bool) {
    let pki = Pki::from_env();
    let mut wire = Wire::bind();
    println!("client {}", wire.address().port());
    std::io::stdout().flush().unwrap();
    fn run<Q: Quic>(mut quic: Q, wire: &mut Wire, server: SocketAddr, upload: bool) {
        quic.connect(Instant::now(), server);
        let size = if upload { usize::MAX } else { MIB };
        let mut streams = streams_of(&[size]);
        let outcome = drive(wire, &mut quic, &mut streams, "client", |_, _, streams| {
            echoed(streams)
        });
        quic.close(Instant::now());
        drain(wire, &mut quic);
        println!("echoed {outcome:?}");
        std::io::stdout().flush().unwrap();
    }
    match imp {
        Imp::Hyper => run(hyper::client(&pki.certificate), &mut wire, server, upload),
        Imp::Upstream => run(
            upstream::client(&pki.certificate),
            &mut wire,
            server,
            upload,
        ),
    }
}

/// The relay: forwards between the client, whoever sends to it, and the server, dropping one
/// full-size datagram in [`DROP_EVERY`] in each direction and holding one, at [`HOLD_AT`], until
/// the next datagram in its direction has passed. On [`RELAY_REPORT`] it says what it did and
/// ends.
fn relay_process(server: SocketAddr) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    println!("relaying {}", socket.local_addr().unwrap().port());
    std::io::stdout().flush().unwrap();
    let mut buffer = vec![0; MAX_DATAGRAM];
    let mut client: Option<SocketAddr> = None;
    // Per direction, to the server and to the client: full-size datagrams seen, dropped, held,
    // and the one held now.
    let mut seen = [0u64; 2];
    let mut dropped = [0u64; 2];
    let mut held = [0u64; 2];
    let mut holding: [Option<Vec<u8>>; 2] = [None, None];
    loop {
        // Every wait is for a datagram; the test ends the relay with its report, or kills it.
        let Ok((length, from)) = socket.recv_from(&mut buffer) else {
            continue;
        };
        let data = &buffer[..length];
        if data == RELAY_REPORT {
            println!(
                "relayed dropped={},{} held={},{} full-size={},{}",
                dropped[0], dropped[1], held[0], held[1], seen[0], seen[1]
            );
            std::io::stdout().flush().unwrap();
            return;
        }
        let (direction, destination) = if from == server {
            let Some(client) = client else { continue };
            (1, client)
        } else {
            client = Some(from);
            (0, server)
        };
        if length >= FULL_SIZE {
            seen[direction] += 1;
            if seen[direction] % DROP_EVERY == 0 {
                dropped[direction] += 1;
                continue;
            }
            if seen[direction] % DROP_EVERY == HOLD_AT && holding[direction].is_none() {
                held[direction] += 1;
                holding[direction] = Some(data.to_vec());
                continue;
            }
        }
        let _ = socket.send_to(data, destination);
        if let Some(late) = holding[direction].take() {
            let _ = socket.send_to(&late, destination);
        }
    }
}

/// A peer process, the lines it reports, and where it listens.
struct Peer {
    child: Child,
    lines: BufReader<ChildStdout>,
    port: u16,
}

impl Peer {
    fn spawn(pki: &Pki, role: &str, value: String) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .env(role, value)
            .env("HQ_CERT", hex(&pki.certificate))
            .env("HQ_KEY", hex(&pki.key))
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap());
        let first = Self::read(&mut lines);
        let port = first
            .rsplit(' ')
            .next()
            .and_then(|port| port.parse().ok())
            .unwrap_or_else(|| panic!("{role}: {first:?}"));
        Self { child, lines, port }
    }
    fn address(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.port))
    }
    /// The next line the process reports: a fact it states once it holds, or the end of its
    /// output if its process ended.
    fn line(&mut self) -> String {
        Self::read(&mut self.lines)
    }
    fn read(lines: &mut BufReader<ChildStdout>) -> String {
        let mut line = String::new();
        lines.read_line(&mut line).unwrap();
        line.trim().to_owned()
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// What a server's report line says.
struct Served {
    reason: String,
    zero_rtt: bool,
    remote: SocketAddr,
    challenges: u64,
    lost: u64,
    heard: Duration,
}

impl Served {
    fn parse(line: &str) -> Self {
        let rest = line
            .strip_prefix("served ")
            .unwrap_or_else(|| panic!("not a report: {line:?}"));
        let field = |name: &str| -> &str {
            rest.split(' ')
                .find_map(|part| part.strip_prefix(name))
                .unwrap_or_else(|| panic!("no {name} in {line:?}"))
        };
        Self {
            reason: rest.split(" 0rtt=").next().unwrap().to_owned(),
            zero_rtt: field("0rtt=") == "true",
            remote: field("remote=").parse().unwrap(),
            challenges: field("challenges=").parse().unwrap(),
            lost: field("lost=").parse().unwrap(),
            heard: Duration::from_millis(field("heard=").parse().unwrap()),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The scenarios, with the test process as the client.

/// Bytes a mebibyte.
const MIB: usize = 1 << 20;
/// What the killed scenarios upload before the kill: twice the default stream window (quinn's
/// `STREAM_RWND`, 1,250,000 bytes), so that by flow control (RFC 9000 §4.1) the receiver has taken
/// at least a window of it when the kill comes: the kill falls mid-stream.
const KILL_AFTER: usize = 2 * 1_250_000;

/// The application's code a client closes with: no error (RFC 9000 §20.2 leaves the space to the
/// application).
const CLOSED: &str = "ApplicationClosed(ApplicationClose { error_code: 0, reason: b\"\" })";

/// One connection to `server`: uploads `sizes`, one stream each, reads every echo, closes and
/// drains. `step` runs after every turn with the streams, as a scenario's hook. Its statistics as
/// the transfer ended.
fn exchange<Q: Quic>(
    quic: &mut Q,
    wire: &mut Wire,
    server: SocketAddr,
    sizes: &[usize],
    mut step: impl FnMut(&mut Q, &mut Wire, &mut Streams<Q::Stream>),
) -> Result<Stats, String> {
    quic.connect(Instant::now(), server);
    let mut streams = streams_of(sizes);
    let mut stats = Stats::default();
    drive(
        wire,
        quic,
        &mut streams,
        "exchange",
        |quic, wire, streams| {
            step(quic, wire, streams);
            stats = quic.stats();
            echoed(streams)
        },
    )?;
    quic.close(Instant::now());
    drain(wire, quic);
    Ok(stats)
}

fn handshake<Q: Quic>(mut quic: Q, server: &mut Peer) -> String {
    let mut wire = Wire::bind();
    let address = server.address();
    exchange(&mut quic, &mut wire, address, &[MIB], |_, _, _| {}).unwrap();
    let full = Served::parse(&server.line());
    assert_eq!(full.reason, CLOSED);
    assert!(
        !full.zero_rtt,
        "the first connection had no ticket to send 0-RTT with"
    );

    // The second connection resumes, and its first bytes go before the handshake ends: 0-RTT.
    quic.connect(Instant::now(), address);
    assert!(quic.has_0rtt(), "the ticket allows 0-RTT data");
    let mut streams = streams_of(&[MIB]);
    assert!(quic.is_handshaking());
    streams.pump(&mut quic);
    let early = streams
        .open
        .values()
        .next()
        .expect("the remembered limits open the stream before the handshake")
        .sent;
    assert!(early > 0, "0-RTT data was written before the handshake");
    drive(
        &mut wire,
        &mut quic,
        &mut streams,
        "0-RTT",
        |_, _, streams| echoed(streams),
    )
    .unwrap();
    assert!(quic.accepted_0rtt(), "the server accepted the 0-RTT data");
    quic.close(Instant::now());
    drain(&mut wire, &mut quic);
    let resumed = Served::parse(&server.line());
    assert_eq!(resumed.reason, CLOSED);
    assert!(resumed.zero_rtt, "the server took the 0-RTT data");
    format!(
        "a full handshake, then a resumed one whose stream took {early} bytes before the handshake ended, its 0-RTT data accepted by both sides"
    )
}

fn streams<Q: Quic>(mut quic: Q, server: &mut Peer) -> String {
    let mut wire = Wire::bind();
    let sizes = [2 * MIB, 3 * MIB, 5 * MIB];
    exchange(&mut quic, &mut wire, server.address(), &sizes, |_, _, _| {}).unwrap();
    assert_eq!(Served::parse(&server.line()).reason, CLOSED);
    "2, 3 and 5 MiB each way on three streams at once".into()
}

fn lossy<Q: Quic>(mut quic: Q, server: &mut Peer, pki: &Pki) -> String {
    let mut relay = Peer::spawn(pki, "HQ_RELAY", server.address().to_string());
    let mut wire = Wire::bind();
    let stats = exchange(
        &mut quic,
        &mut wire,
        relay.address(),
        &[2 * MIB],
        |_, _, _| {},
    )
    .unwrap();
    let served = Served::parse(&server.line());
    assert_eq!(served.reason, CLOSED);
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .send_to(RELAY_REPORT, relay.address())
        .unwrap();
    let relayed = relay.line();
    let counts: Vec<u64> = relayed
        .split(|c: char| !c.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .map(|part| part.parse().unwrap())
        .collect();
    let [
        to_server_dropped,
        to_client_dropped,
        to_server_held,
        to_client_held,
        ..,
    ] = counts[..]
    else {
        panic!("the relay's report: {relayed:?}");
    };
    // Every datagram the relay dropped was a full-size one, which carries data once the transfer
    // runs: each a loss its sender must detect (RFC 9002 §6.1).
    assert!(
        to_server_dropped >= 1 && to_client_dropped >= 1,
        "{relayed}"
    );
    assert!(to_server_held >= 1 && to_client_held >= 1, "{relayed}");
    assert!(stats.lost_packets >= 1, "the client recovered no loss");
    assert!(served.lost >= 1, "the server recovered no loss");
    format!(
        "2 MiB each way, the relay {relayed}; packets lost and recovered: client {}, server {}",
        stats.lost_packets, served.lost
    )
}

/// The client moves mid-transfer: once it has written half its upload and confirmed the handshake
/// (RFC 9000 §9), it takes a new socket, telling its connection when `active` (RFC 9000 §9.5: a new
/// connection ID on the new path), and writes the other half from there.
fn migration<Q: Quic>(mut quic: Q, server: &mut Peer, active: bool) -> String {
    let mut wire = Wire::bind();
    let before = wire.address();
    let mut moved = None;
    let stats = exchange(
        &mut quic,
        &mut wire,
        server.address(),
        &[2 * MIB],
        |quic, wire, streams| {
            // The upload's second half waits for the move, so the client has data to send from
            // its new address: unannounced, its next datagram is how the server learns of it.
            if moved.is_none() {
                streams.hold = Some(MIB);
            }
            // The stream opens once the server's transport parameters allow it.
            let Some(flow) = streams.open.values().next() else {
                return;
            };
            // Mid-transfer, once the client has confirmed the handshake (RFC 9000 §9: no migration
            // before it): before then the server may still lack the client's Finished, and
            // discards a new address's packets as a handshake's strays. A mebibyte written can
            // come first, the stream taking it at once.
            if moved.is_none() && flow.sent >= MIB && quic.confirmed() {
                assert!(!flow.finished, "the move falls mid-transfer");
                wire.rebind();
                if active {
                    quic.local_address_changed();
                }
                moved = Some(wire.address());
                streams.hold = None;
            }
        },
    )
    .unwrap();
    let after = moved.unwrap();
    let served = Served::parse(&server.line());
    assert_eq!(served.reason, CLOSED);
    assert_eq!(
        served.remote, after,
        "the server follows the client to its new address"
    );
    assert!(
        served.challenges >= 1,
        "the server validated the new path (RFC 9000 §8.2)"
    );
    assert!(
        stats.path_responses_sent >= 1,
        "the client answered the path's challenge"
    );
    if active {
        assert!(
            stats.connection_ids_retired >= 1,
            "the client took a new connection ID for its new path"
        );
    }
    format!(
        "2 MiB each way, the client moved from {before} to {after} mid-transfer{}; the server sent {} PATH_CHALLENGE, the client {} PATH_RESPONSE{}",
        if active {
            ", saying so"
        } else {
            " unannounced"
        },
        served.challenges,
        stats.path_responses_sent,
        if active {
            format!(", {} connection IDs retired", stats.connection_ids_retired)
        } else {
            String::new()
        }
    )
}

/// The server process is killed with SIGKILL mid-upload; the client's connection ends by its
/// idle timeout, no sooner than that after the server's last datagram. A new server process
/// answers after: the client resumes the killed server's session and sends 0-RTT data on it, which
/// the new process, holding none of the killed one's sessions, rejects; the client opens its
/// stream again and the exchange completes.
fn killed_server<Q: Quic>(mut quic: Q, server: &mut Peer, pki: &Pki, imp: Imp) -> String {
    let mut wire = Wire::bind();
    quic.connect(Instant::now(), server.address());
    let mut streams = streams_of(&[usize::MAX]);
    drive(
        &mut wire,
        &mut quic,
        &mut streams,
        "upload",
        |_, _, streams| {
            // The stream opens once the server's transport parameters allow it.
            streams
                .open
                .values()
                .next()
                .is_some_and(|flow| flow.sent >= KILL_AFTER)
        },
    )
    .unwrap();
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    let (reason, after_heard) = until_lost(&mut wire, &mut quic);
    assert_eq!(reason, "TimedOut");
    assert!(
        after_heard >= IDLE_TIMEOUT,
        "timed out {after_heard:?} after the server's last datagram, inside the idle timeout"
    );
    drain(&mut wire, &mut quic);
    let mut next = Peer::spawn(pki, "HQ_SERVER", imp.name().into());
    quic.connect(Instant::now(), next.address());
    assert!(
        quic.has_0rtt(),
        "the killed server's ticket allows 0-RTT data"
    );
    let mut streams = streams_of(&[MIB]);
    streams.pump(&mut quic);
    let early = streams
        .open
        .values()
        .next()
        .expect("the remembered limits open the stream before the handshake")
        .sent;
    assert!(early > 0, "0-RTT data was written before the handshake");
    drive(
        &mut wire,
        &mut quic,
        &mut streams,
        "after the kill",
        |_, _, streams| echoed(streams),
    )
    .unwrap();
    assert!(
        !quic.accepted_0rtt(),
        "a new server process accepted the killed one's 0-RTT data"
    );
    assert_eq!(streams.reopened, 1, "the rejected stream opened again");
    quic.close(Instant::now());
    drain(&mut wire, &mut quic);
    let served = Served::parse(&next.line());
    assert_eq!(served.reason, CLOSED);
    assert!(!served.zero_rtt, "the new server took no 0-RTT data");
    format!(
        "the server killed mid-upload; the client timed out {after_heard:?} after its last datagram; the next server process rejected the {early} bytes of 0-RTT data the killed one's ticket allowed, and answered the stream opened again"
    )
}

/// The client process is killed mid-upload; this process's server sees its connection end by its
/// idle timeout, no sooner than that after the client's last datagram, and serves the next client.
fn killed_client<Q: Quic>(mut quic: Q, pki: &Pki, client: Imp) -> String {
    let mut wire = Wire::bind();
    let server = wire.address();
    let mut uploader = Peer::spawn(
        pki,
        "HQ_CLIENT",
        format!("{}:{server}:upload", client.name()),
    );
    let mut lines = Vec::new();
    serve(&mut quic, &mut wire, |line| match line {
        // Killed once the server holds a mebibyte of its stream: mid-stream, on a fact.
        Some(line) if line == "progress" => {
            uploader.child.kill().unwrap();
            uploader.child.wait().unwrap();
            true
        }
        Some(line) => {
            lines.push(line);
            false
        }
        // The uploader has not connected yet: wait while its process runs.
        None => uploader.child.try_wait().unwrap().is_none(),
    });
    let timed_out = Served::parse(lines.first().expect("the uploader never connected"));
    assert_eq!(timed_out.reason, "TimedOut");
    assert!(
        timed_out.heard >= IDLE_TIMEOUT,
        "timed out {:?} after the client's last datagram, inside the idle timeout",
        timed_out.heard
    );
    let mut next = Peer::spawn(pki, "HQ_CLIENT", format!("{}:{server}:echo", client.name()));
    let mut echoed_lines = Vec::new();
    serve(&mut quic, &mut wire, |line| match line {
        Some(line) if line == "progress" => true,
        Some(line) => {
            echoed_lines.push(line);
            false
        }
        None => next.child.try_wait().unwrap().is_none(),
    });
    assert_eq!(next.line(), "echoed Ok(())");
    let echoed = Served::parse(
        echoed_lines
            .first()
            .expect("the next client never connected"),
    );
    assert_eq!(echoed.reason, CLOSED);
    format!(
        "the client killed mid-upload; the server timed out {:?} after its last datagram, then served the next client",
        timed_out.heard
    )
}

fn run(scenario: &str, client: Imp, server: Imp, pki: &Pki) -> String {
    macro_rules! with_client {
        ($run:ident $(, $arg:expr)*) => {
            match client {
                Imp::Hyper => $run(hyper::client(&pki.certificate) $(, $arg)*),
                Imp::Upstream => $run(upstream::client(&pki.certificate) $(, $arg)*),
            }
        };
    }
    if scenario == "killed-client" {
        return match server {
            Imp::Hyper => killed_client(hyper::server(&pki.certificate, &pki.key), pki, client),
            Imp::Upstream => {
                killed_client(upstream::server(&pki.certificate, &pki.key), pki, client)
            }
        };
    }
    let mut peer = Peer::spawn(pki, "HQ_SERVER", server.name().into());
    match scenario {
        "handshake" => with_client!(handshake, &mut peer),
        "streams" => with_client!(streams, &mut peer),
        "lossy" => with_client!(lossy, &mut peer, pki),
        "migration" => with_client!(migration, &mut peer, true),
        "rebinding" => with_client!(migration, &mut peer, false),
        "killed-server" => with_client!(killed_server, &mut peer, pki, server),
        _ => unreachable!("no scenario {scenario}"),
    }
}

fn main() {
    if let Ok(imp) = std::env::var("HQ_SERVER") {
        server_process(Imp::parse(&imp));
        return;
    }
    if let Ok(client) = std::env::var("HQ_CLIENT") {
        let mut parts = client.splitn(2, ':');
        let imp = Imp::parse(parts.next().unwrap());
        let (server, mode) = parts.next().unwrap().rsplit_once(':').unwrap();
        client_process(imp, server.parse().unwrap(), mode == "upload");
        return;
    }
    if let Ok(server) = std::env::var("HQ_RELAY") {
        relay_process(server.parse().unwrap());
        return;
    }
    let only = std::env::args()
        .skip(1)
        .find(|argument| !argument.starts_with('-'));
    let pki = Pki::new();
    let every = [
        (Imp::Hyper, Imp::Hyper),
        (Imp::Hyper, Imp::Upstream),
        (Imp::Upstream, Imp::Hyper),
    ];
    // A killed peer tests the survivor: hyper-quic is the survivor in these pairs.
    let hyper_client = [(Imp::Hyper, Imp::Hyper), (Imp::Hyper, Imp::Upstream)];
    let hyper_server = [(Imp::Hyper, Imp::Hyper), (Imp::Upstream, Imp::Hyper)];
    let scenarios: [(&str, &[(Imp, Imp)]); 7] = [
        ("handshake", &every),
        ("streams", &every),
        ("lossy", &every),
        ("migration", &every),
        ("rebinding", &every),
        ("killed-server", &hyper_client),
        ("killed-client", &hyper_server),
    ];
    for (scenario, pairs) in scenarios {
        if only.as_deref().is_some_and(|only| !scenario.contains(only)) {
            continue;
        }
        for &(client, server) in pairs {
            let began = Instant::now();
            let report = run(scenario, client, server, &pki);
            println!(
                "e2e {scenario}: ok in {:?}: {} client, {} server: {report}",
                began.elapsed(),
                client.name(),
                server.name()
            );
        }
    }
}
