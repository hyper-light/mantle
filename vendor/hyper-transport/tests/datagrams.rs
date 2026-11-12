//! What an endpoint tells its peers of QUIC DATAGRAM frames (RFC 9221): nothing. No owner of
//! the transport sends or reads one, so an endpoint advertises no `max_datagram_frame_size`, and
//! hyper-quic holds no receive allowance for them on any of its connections. Each test reads the
//! parameter as the peer received it, from a bare hyper-quic connection on the other end, whose
//! `datagrams().max_size()` is `None` exactly when the peer sent no such parameter.

#![allow(
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    missing_docs
)]

mod common;

use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use common::*;
use hyper_quic::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use hyper_quic::{
    ClientConfig, ClientConfigHandle, Connection, ConnectionHandle, DatagramEvent, ServerConfig,
};
use hyper_transport::tls::Tls;
use hyper_transport::{Event, PeerId, Transmit};

const TURNS: usize = 10_000;
/// The test's endpoints' memory budget: room for a handshake, nothing more is sent.
const BUDGET: u64 = 64 << 20;

/// One bare hyper-quic connection, a client or a server, with hyper-quic's default transport,
/// which advertises DATAGRAM support: the peer whose view of the other end the tests read.
struct Bare {
    quic: hyper_quic::Endpoint,
    client: Option<ClientConfigHandle>,
    conn: Option<(ConnectionHandle, Connection)>,
    /// A datagram the endpoint answered on its own (a stateless reset, a version negotiation).
    response: Option<Vec<u8>>,
    connected: bool,
    scratch: Vec<u8>,
}

impl Bare {
    /// Node `which` of `pair`, a client that dials, or a server that accepts, with its certificate.
    fn new(pair: &Pair, which: PeerId, serves: bool) -> Self {
        let (certificate, key) = if which == 1 { &pair.one } else { &pair.two };
        let tls = Tls::mutual(&credentials(&pair.pki.root, certificate, key), serves).unwrap();
        let server = tls.server.map(|tls| {
            ServerConfig::with_crypto(Box::new(QuicServerConfig::try_from(tls).unwrap()))
        });
        let mut quic =
            hyper_quic::Endpoint::new(Default::default(), server, true, Some(seed_of(which)))
                .unwrap();
        let client = (!serves).then(|| {
            let crypto = QuicClientConfig::try_from(tls.client).unwrap();
            quic.insert_client_config(ClientConfig::new(Box::new(crypto)))
                .unwrap()
        });
        Self {
            quic,
            client,
            conn: None,
            response: None,
            connected: false,
            scratch: Vec::new(),
        }
    }

    fn dial(&mut self, now: Instant, to: SocketAddr, peer: PeerId) {
        let client = self.client.unwrap();
        let dialed = self.quic.connect(now, client, to, &name(peer), None);
        self.conn = Some(dialed.unwrap());
    }

    /// The largest DATAGRAM frame payload the peer said it takes, or `None` where it sent no
    /// `max_datagram_frame_size` (RFC 9221 §3).
    fn peer_datagrams(&mut self) -> Option<usize> {
        self.conn.as_mut().unwrap().1.datagrams().max_size()
    }

    /// Passes the connection's events to the endpoint and back, and notes its handshake's end.
    fn pump(&mut self) {
        let Some((handle, conn)) = self.conn.as_mut() else {
            return;
        };
        while let Some(event) = conn.poll_endpoint_events() {
            if let Some(back) = self.quic.handle_event(*handle, event) {
                conn.handle_event(back, self.quic.configs_mut());
            }
        }
        while let Some(event) = conn.poll() {
            if let hyper_quic::Event::Connected = event {
                self.connected = true;
            }
        }
    }
}

impl Drive for Bare {
    fn datagram(&mut self, now: Instant, from: SocketAddr, bytes: &[u8]) {
        let mut buffer = std::mem::take(&mut self.scratch);
        buffer.clear();
        let data = BytesMut::from(bytes);
        match self.quic.handle(now, from, None, None, data, &mut buffer) {
            Some(DatagramEvent::NewConnection(incoming)) => {
                let accepted = self.quic.accept(incoming, now, &mut buffer, None, None);
                self.conn = Some(accepted.ok().unwrap());
            }
            Some(DatagramEvent::ConnectionEvent(_, event)) => {
                if let Some((_, conn)) = self.conn.as_mut() {
                    conn.handle_event(event, self.quic.configs_mut());
                }
            }
            Some(DatagramEvent::Response(transmit)) => {
                self.response = Some(buffer[..transmit.size].to_vec());
            }
            None => {}
        }
        self.scratch = buffer;
        self.pump();
    }
    fn transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> Option<Transmit> {
        out.clear();
        self.response.take().map_or_else(
            || {
                let (_, conn) = self.conn.as_mut()?;
                conn.poll_transmit(now, 1, out, self.quic.configs())
            },
            |_| None,
        )
    }
    fn timeout(&mut self) -> Option<Instant> {
        self.conn.as_mut()?.1.poll_timeout()
    }
    fn fire(&mut self, now: Instant) {
        if let Some((_, conn)) = self.conn.as_mut() {
            conn.handle_timeout(now);
        }
        self.pump();
    }
}

fn now() -> Instant {
    hyper_sim::Anchor::new().instant(0).unwrap()
}

/// The reading itself: a bare client and a bare server, both with hyper-quic's default transport,
/// each see the other advertise DATAGRAM frames. Without this, a `None` below could be the reading
/// failing rather than the parameter missing.
#[test]
fn bare_hyper_quic_advertises_datagrams_so_the_reading_sees_them() {
    let pair = Pair::new();
    let mut net = Net::new(now(), Bare::new(&pair, 1, false), Bare::new(&pair, 2, true));
    let (at, to) = (net.now, net.b_address);
    net.a.dial(at, to, 2);
    net.until(TURNS, |net| net.a.connected && net.b.connected);
    assert!(
        net.a.peer_datagrams().is_some(),
        "the bare server advertises DATAGRAM frames"
    );
    assert!(
        net.b.peer_datagrams().is_some(),
        "the bare client advertises DATAGRAM frames"
    );
}

/// A node that accepts a connection sends no `max_datagram_frame_size`.
#[test]
fn a_node_accepting_a_connection_advertises_no_datagrams() {
    let pair = Pair::new();
    let book = pair.book(Role::Node, Role::Node);
    let node = pair.node::<Mantle>(2, Role::Node, book, limits(), BUDGET, now());
    let mut net = Net::new(now(), Bare::new(&pair, 1, false), node);
    let (at, to) = (net.now, net.b_address);
    net.a.dial(at, to, 2);
    net.until(TURNS, |net| net.a.connected);
    assert_eq!(net.a.peer_datagrams(), None);
}

/// A node that dials sends no `max_datagram_frame_size`.
#[test]
fn a_node_dialing_a_connection_advertises_no_datagrams() {
    let pair = Pair::new();
    let book = pair.book(Role::Node, Role::Node);
    let node = pair.node::<Mantle>(1, Role::Node, book, limits(), BUDGET, now());
    let mut net = Net::new(now(), node, Bare::new(&pair, 2, true));
    let (at, to) = (net.now, net.b_address);
    net.a.connect(at, 2, to).unwrap();
    net.until(TURNS, |net| {
        while let Some(event) = net.a.poll_event() {
            assert!(
                !matches!(event, Event::Unreachable { .. }),
                "the dial failed: {event:?}"
            );
        }
        net.b.connected
    });
    assert_eq!(net.b.peer_datagrams(), None);
}
