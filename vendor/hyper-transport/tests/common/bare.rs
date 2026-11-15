//! A bare hyper-quic connection beside the shared helpers (`common`): a peer that runs hyper-quic's
//! default transport and a node's TLS, for the tests that read what a node tells such a peer or send
//! it what no node would (datagrams.rs, certificates.rs). It is a module of its own, named by
//! `#[path]` from the tests that use it, so that `common`, which hyper-rt-transport's tests include
//! too, needs nothing of hyper-quic.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    dead_code,
    missing_docs,
    unreachable_pub
)]

use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use hyper_quic::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use hyper_quic::{
    ClientConfig, ClientConfigHandle, Connection, ConnectionHandle, DatagramEvent, ServerConfig,
};
use hyper_transport::tls::Tls;
use hyper_transport::{PeerId, Transmit};

use crate::common::{Drive, Pair, credentials, name, seed_of};

/// One bare hyper-quic connection, a client or a server, with hyper-quic's default transport (which
/// advertises DATAGRAM support) and TLS as a node's: the peer whose view of the other end a test
/// reads, or one that sends what no node would.
pub struct Bare {
    quic: hyper_quic::Endpoint,
    client: Option<ClientConfigHandle>,
    conn: Option<(ConnectionHandle, Connection)>,
    /// A datagram the endpoint answered on its own (a stateless reset, a version negotiation).
    response: Option<Vec<u8>>,
    pub connected: bool,
    /// Why the connection ended, once it did.
    pub lost: Option<hyper_quic::ConnectionError>,
    scratch: Vec<u8>,
}

impl Bare {
    /// Node `which` of `pair`, a client that dials, or a server that accepts, with its certificate.
    pub fn new(pair: &Pair, which: PeerId, serves: bool) -> Self {
        Self::with(pair, which, serves, seed_of(which), |_| {})
    }

    /// As [`Bare::new`], its endpoint drawing from `seed`, its TLS client as `client` leaves it.
    pub fn with(
        pair: &Pair,
        which: PeerId,
        serves: bool,
        seed: [u8; 32],
        client: impl FnOnce(&mut hyper_quic::rustls::ClientConfig),
    ) -> Self {
        let (certificate, key) = if which == 1 { &pair.one } else { &pair.two };
        let mut tls = Tls::mutual(&credentials(&pair.pki.root, certificate, key), serves).unwrap();
        client(&mut tls.client);
        let server = tls.server.map(|tls| {
            ServerConfig::with_crypto(Box::new(QuicServerConfig::try_from(tls).unwrap()))
        });
        let mut quic =
            hyper_quic::Endpoint::new(Default::default(), server, true, Some(seed)).unwrap();
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
            lost: None,
            scratch: Vec::new(),
        }
    }

    pub fn dial(&mut self, now: Instant, to: SocketAddr, peer: PeerId) {
        let client = self.client.unwrap();
        let dialed = self.quic.connect(now, client, to, &name(peer), None);
        self.conn = Some(dialed.unwrap());
    }

    /// The largest DATAGRAM frame payload the peer said it takes, or `None` where it sent no
    /// `max_datagram_frame_size` (RFC 9221 §3).
    pub fn peer_datagrams(&mut self) -> Option<usize> {
        self.conn.as_mut().unwrap().1.datagrams().max_size()
    }

    /// Passes the connection's events to the endpoint and back, and notes its handshake's end.
    pub fn pump(&mut self) {
        let Some((handle, conn)) = self.conn.as_mut() else {
            return;
        };
        while let Some(event) = conn.poll_endpoint_events() {
            if let Some(back) = self.quic.handle_event(*handle, event) {
                conn.handle_event(back, self.quic.configs_mut());
            }
        }
        while let Some(event) = conn.poll() {
            match event {
                hyper_quic::Event::Connected => self.connected = true,
                hyper_quic::Event::ConnectionLost { reason } => self.lost = Some(reason),
                _ => {}
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
