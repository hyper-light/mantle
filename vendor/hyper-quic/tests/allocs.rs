//! What a stream's transfer allocates at each of its steps, counted by hyper-measure's allocator:
//! the sender's writes, the receiver's handling of each datagram, and the receiver's packets that
//! carry its flow control credit back (MAX_DATA, MAX_STREAM_DATA). The windows are mantle's, 12,000
//! bytes (mantle's transport limits at its least budget, docs/research/80 "The sites"), so the
//! receiver grants credit every few packets, as mantle's nodes do through a native PUT.
//!
//! A client writes a body to a server through the public API only, the two endpoints joined by an
//! in-memory network that hands each datagram over as a view of a buffer it shares with the others
//! its sender sent at once, as a receive batch's are; time advances only to fire a timer. Each step
//! is counted alone: the counts are exact, the workload being fixed.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation
)]

use std::collections::VecDeque;
use std::net::{Ipv6Addr, SocketAddr};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use hyper_measure::alloc::{self, Counting, Counts};
use hyper_quic::rustls::RootCertStore;
use hyper_quic::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use hyper_quic::{
    ClientConfig, ClientConfigHandle, Connection, ConnectionHandle, DatagramEvent, Dir, Endpoint,
    EndpointConfig, Event, ServerConfig, StreamId, TransportConfig, VarInt,
};

#[global_allocator]
static GLOBAL: Counting = Counting;

/// mantle's connection and stream windows at its least transport budget.
const WINDOW: u32 = 12_000;
/// The body written, after a warm-up of the same size.
const BODY: usize = 1 << 20;
/// What one write offers: a piece of the body, as mantle's node writes a chunk's.
const PIECE: usize = 4_096;

/// Each step's allocations, counted apart.
#[derive(Default, Debug, Clone, Copy)]
struct Steps {
    /// The client's first write on its stream, which makes the stream's send state.
    first_write: Counts,
    /// The client's writes after its first.
    write: Counts,
    /// The server's handling of each datagram it receives (`Endpoint::handle` and the event it
    /// hands its connection).
    receive: Counts,
    /// The server's transmissions: its acknowledgements and the credit they carry back.
    transmit: Counts,
}

fn add(into: &mut Counts, counts: Counts) {
    into.allocations += counts.allocations;
    into.reallocations += counts.reallocations;
    into.bytes += counts.bytes;
}

/// Counts what `f` allocates into `into`, the count of the rest of the harness left off.
fn count<R>(into: &mut Counts, f: impl FnOnce() -> R) -> R {
    alloc::begin();
    let r = f();
    add(into, alloc::end());
    r
}

struct Peer {
    endpoint: Endpoint,
    addr: SocketAddr,
    conn: Option<(ConnectionHandle, Connection)>,
    inbound: VecDeque<BytesMut>,
    connected: bool,
}

impl Peer {
    fn new(endpoint: Endpoint, port: u16) -> Self {
        Self {
            endpoint,
            addr: SocketAddr::new(Ipv6Addr::LOCALHOST.into(), port),
            conn: None,
            inbound: VecDeque::new(),
            connected: false,
        }
    }

    fn conn(&mut self) -> &mut Connection {
        &mut self.conn.as_mut().unwrap().1
    }
}

struct Net {
    now: Instant,
    client: Peer,
    server: Peer,
    buf: Vec<u8>,
    /// Whether the server's steps are counted.
    counting: bool,
    steps: Steps,
}

impl Net {
    fn new(client: Endpoint, server: Endpoint) -> Self {
        Self {
            now: hyper_sim::Anchor::new().instant(0).unwrap(),
            client: Peer::new(client, 4433),
            server: Peer::new(server, 4434),
            buf: Vec::with_capacity(1500),
            counting: false,
            steps: Steps::default(),
        }
    }

    fn connect(&mut self, config: ClientConfigHandle) {
        let (ch, conn) = self
            .client
            .endpoint
            .connect(self.now, config, self.server.addr, "localhost", None)
            .unwrap();
        self.client.conn = Some((ch, conn));
    }

    /// Moves every datagram either side has to send, until neither has any; returns whether any
    /// datagram moved. Time does not advance.
    fn exchange(&mut self) -> bool {
        let mut moved = false;
        loop {
            let mut any = false;
            let mut none = Counts::default();
            any |= transmit(
                self.now,
                &mut self.client,
                &mut self.server.inbound,
                &mut self.buf,
                &mut none,
            );
            let mut sent = Counts::default();
            any |= transmit(
                self.now,
                &mut self.server,
                &mut self.client.inbound,
                &mut self.buf,
                &mut sent,
            );
            let mut received = Counts::default();
            any |= deliver(
                self.now,
                &mut self.server,
                self.client.addr,
                &mut self.client.inbound,
                &mut self.buf,
                &mut received,
            );
            any |= deliver(
                self.now,
                &mut self.client,
                self.server.addr,
                &mut self.server.inbound,
                &mut self.buf,
                &mut none,
            );
            if self.counting {
                add(&mut self.steps.transmit, sent);
                add(&mut self.steps.receive, received);
            }
            for peer in [&mut self.client, &mut self.server] {
                if let Some((_, conn)) = &mut peer.conn {
                    while let Some(event) = conn.poll() {
                        if let Event::Connected = event {
                            peer.connected = true;
                        }
                    }
                }
            }
            if !any {
                return moved;
            }
            moved = true;
        }
    }

    fn handshake(&mut self) {
        for _ in 0..1000 {
            self.exchange();
            if self.client.connected && self.server.connected {
                return;
            }
            self.advance();
        }
        panic!("handshake did not complete");
    }

    fn advance(&mut self) {
        let next = [&mut self.client, &mut self.server]
            .into_iter()
            .filter_map(|p| p.conn.as_mut().and_then(|(_, c)| c.poll_timeout()))
            .min();
        let Some(next) = next else { return };
        self.now = self.now.max(next);
        for peer in [&mut self.client, &mut self.server] {
            if let Some((_, conn)) = &mut peer.conn {
                conn.handle_timeout(self.now);
            }
        }
    }
}

fn transmit(
    now: Instant,
    from: &mut Peer,
    to: &mut VecDeque<BytesMut>,
    buf: &mut Vec<u8>,
    counts: &mut Counts,
) -> bool {
    let mut batch = BytesMut::new();
    let Some((ch, conn)) = &mut from.conn else {
        return false;
    };
    let mut any = false;
    while let Some(event) = conn.poll_endpoint_events() {
        if let Some(event) = from.endpoint.handle_event(*ch, event) {
            conn.handle_event(event, from.endpoint.configs_mut());
        }
    }
    buf.clear();
    loop {
        let configs = from.endpoint.configs();
        let Some(t) = count(counts, || conn.poll_transmit(now, 1, buf, configs)) else {
            break;
        };
        // Each datagram a view of one buffer, as a receive batch's are (hyper-rt's `Ring`).
        batch.reserve(t.size);
        batch.extend_from_slice(&buf[..t.size]);
        to.push_back(batch.split_to(t.size));
        buf.clear();
        any = true;
    }
    any
}

fn deliver(
    now: Instant,
    peer: &mut Peer,
    remote: SocketAddr,
    reply_to: &mut VecDeque<BytesMut>,
    buf: &mut Vec<u8>,
    counts: &mut Counts,
) -> bool {
    let mut any = false;
    while let Some(datagram) = peer.inbound.pop_front() {
        any = true;
        buf.clear();
        let event = count(counts, || {
            peer.endpoint.handle(now, remote, None, None, datagram, buf)
        });
        match event {
            Some(DatagramEvent::NewConnection(incoming)) => {
                let mut out = Vec::new();
                let (ch, conn) = peer
                    .endpoint
                    .accept(incoming, now, &mut out, None, None)
                    .unwrap();
                peer.conn = Some((ch, conn));
            }
            Some(DatagramEvent::ConnectionEvent(ch, event)) => {
                if let Some((own, conn)) = &mut peer.conn
                    && *own == ch
                {
                    let configs = peer.endpoint.configs_mut();
                    count(counts, || conn.handle_event(event, configs));
                }
            }
            Some(DatagramEvent::Response(t)) => reply_to.push_back(BytesMut::from(&buf[..t.size])),
            None => {}
        }
    }
    any
}

fn certificate() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    (cert.cert.into(), key.into())
}

fn transport() -> TransportConfig {
    let mut transport = TransportConfig::default();
    transport
        .stream_receive_window(VarInt::from_u32(WINDOW))
        .receive_window(VarInt::from_u32(WINDOW));
    transport
}

/// A connected client and server with mantle's windows.
fn net() -> Net {
    let (cert, key) = certificate();
    let mut server_config =
        ServerConfig::with_single_cert(vec![cert.clone()], key.clone_key()).unwrap();
    server_config.transport_config(transport());
    let server = Endpoint::new(EndpointConfig::default(), Some(server_config), true, None).unwrap();
    let client = Endpoint::new(EndpointConfig::default(), None, true, None).unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(cert).unwrap();
    let mut client_config = ClientConfig::with_root_certificates(roots).unwrap();
    client_config.transport_config(transport());
    let mut net = Net::new(client, server);
    let config = net
        .client
        .endpoint
        .insert_client_config(client_config)
        .unwrap();
    net.connect(config);
    net.handshake();
    net
}

/// How the client hands its body to the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Write {
    /// `SendStream::write` of a borrowed piece.
    Borrowed,
    /// `SendStream::write_chunks` of an owned piece.
    Owned,
}

/// The client writes `BODY` bytes on a new stream, the server reading them as they arrive; the
/// client's writes and the server's steps are counted where `counting`. Returns the number of
/// writes that took bytes.
fn transfer(net: &mut Net, how: Write, counting: bool) -> u64 {
    let id: StreamId = net.client.conn().streams().open(Dir::Bi).unwrap();
    let piece = Bytes::from(vec![7u8; PIECE]);
    let (mut written, mut read, mut writes) = (0usize, 0usize, 0u64);
    net.counting = counting;
    let mut accepted = None;
    for _ in 0..1_000_000 {
        while written < BODY {
            let take = (BODY - written).min(PIECE);
            let mut write = Counts::default();
            let took = match how {
                Write::Borrowed => {
                    let mut stream = net.client.conn().send_stream(id);
                    count(&mut write, || stream.write(&piece[..take]))
                }
                Write::Owned => {
                    let mut chunks = [piece.slice(..take)];
                    let mut stream = net.client.conn().send_stream(id);
                    count(&mut write, || stream.write_chunks(&mut chunks))
                        .map(|written| written.bytes)
                }
            };
            if counting {
                if writes == 0 {
                    add(&mut net.steps.first_write, write);
                } else {
                    add(&mut net.steps.write, write);
                }
            }
            match took {
                Ok(took) if took > 0 => {
                    written += took;
                    writes += 1;
                }
                _ => break,
            }
        }
        if written == BODY {
            let _ = net.client.conn().send_stream(id).finish();
        }
        let moved = net.exchange();
        let server = net.server.conn();
        if accepted.is_none() {
            accepted = server.streams().accept(Dir::Bi);
        }
        if let Some(stream) = accepted {
            let mut recv = server.recv_stream(stream);
            if let Ok(mut chunks) = recv.read(true) {
                while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
                    read += chunk.bytes.len();
                }
                let _ = chunks.finalize();
            }
        }
        if read == BODY && written == BODY {
            net.exchange();
            net.counting = false;
            return writes;
        }
        if !moved {
            net.advance();
        }
    }
    panic!("the transfer never completed: {written} written, {read} read");
}

/// The server's steps through a transfer of a borrowed body, after one to warm up.
fn counted(how: Write) -> (Net, u64) {
    let mut net = net();
    transfer(&mut net, how, false);
    let writes = transfer(&mut net, how, true);
    assert!(
        alloc::installed(),
        "the counting allocator is not installed"
    );
    (net, writes)
}

/// A packet that carries the receiver's credit back (MAX_DATA, MAX_STREAM_DATA) records what it
/// carried for its loss recovery without allocating: before, its record was boxed for the MAX_DATA
/// frame and held a hash set for the MAX_STREAM_DATA frame, two allocations a packet (mantle's
/// native PUT of 8 MiB: 2,608 boxes and 2,629 sets an operation).
#[test]
fn credit_sent_back_allocates_nothing() {
    let (mut net, _) = counted(Write::Borrowed);
    let stats = net.server.conn().stats();
    eprintln!(
        "server transmissions: {:?}; MAX_DATA {} MAX_STREAM_DATA {} packets {}",
        net.steps.transmit,
        stats.frame_tx.max_data,
        stats.frame_tx.max_stream_data,
        stats.udp_tx.datagrams
    );
    assert_eq!(
        (
            net.steps.transmit.allocations,
            net.steps.transmit.reallocations
        ),
        (0, 0),
        "the server's transmissions through a transfer: {:?}",
        net.steps.transmit
    );
}

/// A datagram cut from a buffer its batch shares is handled without allocating: its packet is
/// split from its header, opened and its stream data kept as views of that buffer. What the
/// transfer's datagrams allocate is the new stream's receive state as its first datagrams arrive,
/// once a stream: two allocations and two reallocations.
#[test]
fn a_received_datagram_is_handled_without_allocating() {
    let (net, _) = counted(Write::Borrowed);
    eprintln!("server receives: {:?}", net.steps.receive);
    assert_eq!(
        (
            net.steps.receive.allocations,
            net.steps.receive.reallocations
        ),
        (2, 2),
        "the server's handling of every datagram of a transfer: {:?}",
        net.steps.receive
    );
}

/// A write of an owned piece hands it to the stream without copying it, and a stream whose data
/// was all acknowledged takes its next write without allocating: before, the queue of unacknowledged
/// pieces gave its storage back once emptied, so each write allocated it again (mantle's native PUT
/// of 8 MiB: 1,745 an operation). A stream's first write makes its send state and its queue, two
/// allocations once a stream.
#[test]
fn an_owned_write_allocates_nothing() {
    let (net, writes) = counted(Write::Owned);
    let (first, rest) = (net.steps.first_write, net.steps.write);
    eprintln!("client owned writes: {writes}, the first {first:?}, the rest {rest:?}");
    assert_eq!(
        (first.allocations, first.reallocations),
        (2, 0),
        "the first write"
    );
    assert_eq!(
        (rest.allocations, rest.reallocations, rest.bytes),
        (0, 0, 0),
        "{} owned writes after the first",
        writes - 1
    );
}

/// A write of a borrowed piece copies it, one allocation of its bytes a write and no more.
#[test]
fn a_borrowed_write_allocates_its_copy_alone() {
    let (net, writes) = counted(Write::Borrowed);
    let (first, rest) = (net.steps.first_write, net.steps.write);
    eprintln!("client borrowed writes: {writes}, the first {first:?}, the rest {rest:?}");
    assert_eq!(
        (first.allocations, first.reallocations),
        (3, 0),
        "the first write: its copy, the stream's send state and its queue"
    );
    assert_eq!(
        (rest.allocations, rest.reallocations, rest.bytes),
        (writes - 1, 0, (BODY - PIECE) as u64),
        "{} borrowed writes after the first",
        writes - 1
    );
}
