//! A client and a server endpoint, driven through the public API only, complete a handshake and
//! exchange data; and the allocations one handshake makes, counted by a counting allocator and
//! printed for comparison with the same test run against 526c2cc, where configurations were
//! shared by `Arc` (the commit message records both).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cognitive_complexity
)]

use std::collections::VecDeque;
use std::net::{Ipv6Addr, SocketAddr};
use std::time::Instant;

use bytes::BytesMut;
use hyper_measure::alloc::{self, Counting, Counts};
use hyper_quic::rustls::RootCertStore;
use hyper_quic::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use hyper_quic::{
    ClientConfig, ClientConfigHandle, Connection, ConnectionHandle, DatagramEvent, Dir, Endpoint,
    EndpointConfig, Event, ServerConfig, StreamEvent,
};

#[global_allocator]
static GLOBAL: Counting = Counting;

fn measure<R>(f: impl FnOnce() -> R) -> (R, Counts) {
    alloc::begin();
    let r = f();
    (r, alloc::end())
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
}

struct Net {
    now: Instant,
    client: Peer,
    server: Peer,
    buf: Vec<u8>,
}

impl Net {
    fn new(client: Endpoint, server: Endpoint) -> Self {
        Self {
            now: hyper_sim::Anchor::new().instant(0).unwrap(),
            client: Peer::new(client, 4433),
            server: Peer::new(server, 4434),
            buf: Vec::with_capacity(1500),
        }
    }

    fn connect(&mut self, config: ClientConfigHandle) {
        let (ch, conn) = self
            .client
            .endpoint
            .connect(self.now, config, self.server.addr, "localhost", None)
            .unwrap();
        self.client.conn = Some((ch, conn));
        self.client.connected = false;
        self.server.conn = None;
        self.server.connected = false;
    }

    /// Moves every datagram either side has to send, until neither has any; returns whether any
    /// datagram moved. Time does not advance.
    fn exchange(&mut self) -> bool {
        let mut moved = false;
        loop {
            let mut any = false;
            any |= transmit(
                self.now,
                &mut self.client,
                &mut self.server.inbound,
                &mut self.buf,
            );
            any |= transmit(
                self.now,
                &mut self.server,
                &mut self.client.inbound,
                &mut self.buf,
            );
            any |= deliver(
                self.now,
                &mut self.server,
                self.client.addr,
                &mut self.client.inbound,
                &mut self.buf,
            );
            any |= deliver(
                self.now,
                &mut self.client,
                self.server.addr,
                &mut self.server.inbound,
                &mut self.buf,
            );
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

    /// Exchanges datagrams, then fires the earliest timer, until both sides are connected and
    /// the network is quiet.
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

fn transmit(now: Instant, from: &mut Peer, to: &mut VecDeque<BytesMut>, buf: &mut Vec<u8>) -> bool {
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
    while let Some(t) = conn.poll_transmit(now, 1, buf, from.endpoint.configs()) {
        to.push_back(BytesMut::from(&buf[..t.size]));
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
) -> bool {
    let mut any = false;
    while let Some(datagram) = peer.inbound.pop_front() {
        any = true;
        buf.clear();
        match peer.endpoint.handle(now, remote, None, None, datagram, buf) {
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
                    conn.handle_event(event, peer.endpoint.configs_mut());
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

fn endpoints(cert: &CertificateDer<'static>, key: &PrivateKeyDer<'static>) -> (Endpoint, Endpoint) {
    let server_config =
        ServerConfig::with_single_cert(vec![cert.clone()], key.clone_key()).unwrap();
    let server = Endpoint::new(EndpointConfig::default(), Some(server_config), true, None).unwrap();
    let client = Endpoint::new(EndpointConfig::default(), None, true, None).unwrap();
    (client, server)
}

fn client_config(cert: &CertificateDer<'static>) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    ClientConfig::with_root_certificates(roots).unwrap()
}

#[test]
fn handshake_and_exchange_data() {
    let (cert, key) = certificate();
    let (client, server) = endpoints(&cert, &key);
    let mut net = Net::new(client, server);
    let config = net
        .client
        .endpoint
        .insert_client_config(client_config(&cert))
        .unwrap();
    net.connect(config);
    net.handshake();

    let stream = {
        let (_, conn) = net.client.conn.as_mut().unwrap();
        let stream = conn.streams().open(Dir::Bi).unwrap();
        conn.send_stream(stream).write(b"ping").unwrap();
        conn.send_stream(stream).finish().unwrap();
        stream
    };
    let mut received = Vec::new();
    for _ in 0..100 {
        net.exchange();
        let (_, conn) = net.server.conn.as_mut().unwrap();
        if let Some(id) = conn.streams().accept(Dir::Bi) {
            assert_eq!(id, stream);
        }
        let mut recv = conn.recv_stream(stream);
        if let Ok(mut chunks) = recv.read(true) {
            while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
                received.extend_from_slice(&chunk.bytes);
            }
            let _ = chunks.finalize();
        }
        if received == b"ping" {
            break;
        }
        net.advance();
    }
    assert_eq!(received, b"ping");

    {
        let (_, conn) = net.server.conn.as_mut().unwrap();
        conn.send_stream(stream).write(b"pong").unwrap();
        conn.send_stream(stream).finish().unwrap();
    }
    let mut reply = Vec::new();
    for _ in 0..100 {
        net.exchange();
        let (_, conn) = net.client.conn.as_mut().unwrap();
        while let Some(event) = conn.poll() {
            let _ = matches!(event, Event::Stream(StreamEvent::Readable { .. }));
        }
        let mut recv = conn.recv_stream(stream);
        if let Ok(mut chunks) = recv.read(true) {
            while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
                reply.extend_from_slice(&chunk.bytes);
            }
            let _ = chunks.finalize();
        }
        if reply == b"pong" {
            break;
        }
        net.advance();
    }
    assert_eq!(reply, b"pong");
}

#[test]
fn allocations_per_handshake() {
    let (cert, key) = certificate();
    let (client, server) = endpoints(&cert, &key);
    let mut net = Net::new(client, server);

    // The first handshake pays for one-time initialisation (aws-lc, the provider tables).
    let warm = net
        .client
        .endpoint
        .insert_client_config(client_config(&cert))
        .unwrap();
    net.connect(warm);
    net.handshake();
    net.exchange();

    // Both measured connections use one configuration, so the second resumes the first's session
    let config = net
        .client
        .endpoint
        .insert_client_config(client_config(&cert))
        .unwrap();
    let ((), full) = measure(|| {
        net.connect(config);
        net.handshake();
        net.exchange();
    });
    let ((), resumed) = measure(|| {
        net.connect(config);
        net.handshake();
        net.exchange();
    });
    assert!(
        alloc::installed(),
        "the counting allocator is not installed"
    );
    eprintln!("hyper-quic full handshake, client and server: {full:?}");
    eprintln!("hyper-quic resumed handshake, client and server: {resumed:?}");
}
