//! The TLS an owner builds and swaps, and what a handshake tells the directory and the owner
//! (docs/transport.md §4a, "The owner's TLS"): a second protocol whose peers present no
//! certificate and are named by it, a dial told why the peer refused it, and a certificate and
//! roots replaced while connections made under the last ones stay up.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    missing_docs
)]

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::*;
use hyper_quic::rustls::{self, RootCertStore, server::WebPkiClientVerifier};
use hyper_transport::tls::{ALPN, CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{
    Authenticated, Config, Directory, Endpoint, Event, Fixed, PeerId, Progress, Refusal,
};

const TURNS: usize = 10_000;
const PERIOD: Duration = Duration::from_secs(2);
/// The second protocol the tests' server serves: guests, who present no certificate.
const GUEST: &[u8] = b"guest/1";
/// The first peer id a guest is named by; each guest is its own peer.
const FIRST_GUEST: PeerId = 100;

fn provider() -> &'static rustls::crypto::CryptoProvider {
    hyper_quic::crypto::rustls::node_provider()
}

fn roots(root: &[u8]) -> RootCertStore {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(root.to_vec())).unwrap();
    roots
}

fn private(der: &[u8]) -> PrivateKeyDer<'static> {
    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der.to_vec()))
}

/// A server side that presents `own`, serves `protocols`, and lets a client without a certificate
/// in: what such a client may do is its directory's to say.
fn open_server(root: &[u8], own: &(Vec<u8>, Vec<u8>), protocols: &[&[u8]]) -> rustls::ServerConfig {
    let verifier = WebPkiClientVerifier::builder_with_provider(roots(root), provider())
        .allow_unauthenticated()
        .build()
        .unwrap();
    let mut tls = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![CertificateDer::from(own.0.clone())], private(&own.1))
        .unwrap();
    tls.alpn_protocols = protocols.iter().map(|protocol| protocol.to_vec()).collect();
    tls
}

/// A client side that presents no certificate and offers `protocol`, verifying the server under
/// `root`.
fn guest_client(root: &[u8], protocol: &[u8]) -> rustls::ClientConfig {
    let mut tls = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots(root))
        .with_no_client_auth();
    tls.alpn_protocols = vec![protocol.to_vec()];
    tls
}

/// What one handshake told a directory.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Seen {
    certificate: Option<Vec<u8>>,
    protocol: Vec<u8>,
    dialed: Option<PeerId>,
}

/// A directory that keeps what each handshake told it. A peer without a certificate on [`GUEST`]
/// is a client, each its own peer; one with a certificate is named by the book, or, on [`GUEST`],
/// is the server it dialed.
#[derive(Default)]
struct Told {
    book: Book,
    seen: Vec<Seen>,
    guests: PeerId,
}

impl Directory for Told {
    type Role = Role;
    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Role)> {
        self.seen.push(Seen {
            certificate: authenticated.certificate.map(<[u8]>::to_vec),
            protocol: authenticated.protocol.to_vec(),
            dialed: authenticated.dialed,
        });
        match (authenticated.certificate, authenticated.protocol) {
            (None, GUEST) => {
                let guest = FIRST_GUEST + self.guests;
                self.guests += 1;
                Some((guest, Role::Client))
            }
            (None, _) => None,
            (Some(_), GUEST) => authenticated.dialed.map(|peer| (peer, Role::Node)),
            (Some(_), _) => self.book.identify(authenticated),
        }
    }
    fn server_name(&self, peer: PeerId) -> Option<&str> {
        self.book.server_name(peer)
    }
}

type Open = Endpoint<Mantle, Fixed, Told>;

fn now() -> Instant {
    hyper_sim::Anchor::new().instant(0).unwrap()
}

/// Node 2 as a server of both protocols, knowing node 1 by its certificate.
fn server(pair: &Pair, now: Instant) -> Open {
    let mut told = Told::default();
    told.book.add(&pair.one.0, 1, Role::Node);
    let mutual = Tls::mutual(
        &credentials(&pair.pki.root, &pair.two.0, &pair.two.1),
        false,
    )
    .unwrap();
    let tls = Tls {
        server: Some(open_server(&pair.pki.root, &pair.two, &[ALPN, GUEST])),
        client: mutual.client,
    };
    let config = Config {
        tls,
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    Endpoint::new(config, Fixed::new(64 << 20, 64), told, now).unwrap()
}

/// A guest of node 2: no certificate, [`GUEST`] offered, and `role` as its own idea of itself.
fn guest(pair: &Pair, role: Role, now: Instant) -> Open {
    let mut told = Told::default();
    told.book.add(&pair.two.0, 2, Role::Node);
    // A guest never listens, so its server side is never used; it still names a protocol.
    let tls = Tls {
        server: None,
        client: guest_client(&pair.pki.root, GUEST),
    };
    let config = Config {
        tls,
        role,
        limits: limits(),
        congestion: Default::default(),
    };
    Endpoint::new(config, Fixed::new(64 << 20, 64), told, now).unwrap()
}

fn all<D: Directory<Role = Role>>(endpoint: &mut Endpoint<Mantle, Fixed, D>) -> Vec<Event<Mantle>> {
    std::iter::from_fn(|| endpoint.poll_event()).collect()
}

#[test]
fn a_guest_without_a_certificate_is_named_by_its_protocol_and_held_to_its_role() {
    let pair = Pair::new();
    let now = now();
    // The guest believes it is a node; the server's directory says it is a client.
    let mut net = Net::new(now, guest(&pair, Role::Node, now), server(&pair, now));
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    let (mut up_a, mut up_b) = (false, false);
    net.until(TURNS, |net| {
        up_a |= all(&mut net.a)
            .iter()
            .any(|event| matches!(event, Event::Connected { peer: 2, .. }));
        up_b |= all(&mut net.b).iter().any(|event| {
            matches!(
                event,
                Event::Connected {
                    peer: FIRST_GUEST,
                    role: Role::Client,
                    epoch: 1
                }
            )
        });
        up_a && up_b
    });
    // What each side's directory was told: the server, no certificate on the guest protocol from
    // a connection it did not dial; the guest, the server's certificate on the protocol it
    // offered, from its dial of peer 2.
    let told = net.b.directory_seen();
    assert_eq!(
        told,
        vec![Seen {
            certificate: None,
            protocol: GUEST.to_vec(),
            dialed: None
        }]
    );
    assert_eq!(
        net.a.directory_seen(),
        vec![Seen {
            certificate: Some(pair.two.0.clone()),
            protocol: GUEST.to_vec(),
            dialed: Some(2)
        }]
    );
    // A get, which a client may send, is served; a vote, which only a node may send, is refused
    // by the server whatever the guest believes of itself.
    let progress = Progress::new(PERIOD).unwrap();
    let get = net
        .a
        .open(net.now, 2, Kind::Get, &[1, 2, 3], None, progress)
        .unwrap();
    let vote = net
        .a
        .open(net.now, 2, Kind::Vote, &[4, 5, 6], None, progress)
        .unwrap();
    let mut asked = Vec::new();
    let mut refused = None;
    let mut replied = false;
    net.until(TURNS, |net| {
        for event in all(&mut net.b) {
            if let Event::Request {
                exchange,
                peer,
                kind,
                ..
            } = event
            {
                asked.push((peer, kind));
                net.b.reply(exchange, b"ok", None).unwrap();
            }
        }
        for event in all(&mut net.a) {
            match event {
                Event::Reply { exchange, .. } if exchange == get => {
                    replied = true;
                    net.a.end(exchange);
                }
                Event::Refused {
                    exchange, refusal, ..
                } if exchange == vote => refused = Some(refusal),
                _ => {}
            }
        }
        replied && refused.is_some()
    });
    assert_eq!(asked, vec![(FIRST_GUEST, Kind::Get)]);
    assert_eq!(refused, Some(Refusal::Kind));
}

#[test]
fn a_client_without_a_certificate_on_the_main_protocol_is_refused_and_told() {
    let pair = Pair::new();
    let now = now();
    let mut told = Told::default();
    told.book.add(&pair.two.0, 2, Role::Node);
    // No certificate, and the protocol a guest is not named on.
    let tls = Tls {
        server: None,
        client: guest_client(&pair.pki.root, ALPN),
    };
    let config = Config {
        tls,
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    let anonymous = Endpoint::new(config, Fixed::new(64 << 20, 64), told, now).unwrap();
    let mut net = Net::new(now, anonymous, server(&pair, now));
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    let mut why = None;
    net.until(TURNS, |net| {
        let _ = all(&mut net.b);
        for event in all(&mut net.a) {
            if let Event::Closed { refusal, .. } | Event::Unreachable { refusal, .. } = event {
                why = Some(refusal);
            }
        }
        why.is_some()
    });
    assert_eq!(why, Some(Some(Refusal::Identity)));
    let stats = net.b.stats().admission;
    assert_eq!(
        (stats.identities, stats.admitted, stats.refused_identity),
        (0, 0, 1)
    );
}

#[test]
fn a_dial_whose_certificate_the_peer_does_not_know_is_told_why() {
    let pair = Pair::new();
    let now = now();
    let a = pair.node::<Mantle>(
        1,
        Role::Node,
        pair.book(Role::Node, Role::Node),
        limits(),
        64 << 20,
        now,
    );
    // Node 2 knows nobody.
    let b = pair.node::<Mantle>(2, Role::Node, Book::default(), limits(), 64 << 20, now);
    let mut net = Net::new(now, a, b);
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    let mut asker = Asker::new();
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        let _ = events(&mut net.b);
        !asker.unreachable.is_empty() || !asker.closed.is_empty()
    });
    // TLS 1.3's client is done before the server has read its certificate (RFC 8446 §4.4.4):
    // the dial was up, and closed for its identity.
    let told: Vec<_> = asker.unreachable.iter().chain(&asker.closed).collect();
    assert_eq!(told, vec![&(2, Some(Refusal::Identity))]);
}

#[test]
fn a_peer_refused_after_it_was_admitted_is_told_why() {
    let pair = Pair::new();
    let now = now();
    let a = pair.node::<Mantle>(
        1,
        Role::Node,
        pair.book(Role::Node, Role::Node),
        limits(),
        64 << 20,
        now,
    );
    let b = pair.node::<Mantle>(
        2,
        Role::Node,
        pair.book(Role::Node, Role::Node),
        limits(),
        64 << 20,
        now,
    );
    let mut net = Net::new(now, a, b);
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    let mut asker = Asker::new();
    let mut server = Vec::new();
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        server.extend(events(&mut net.b));
        !asker.connected.is_empty()
            && server
                .iter()
                .any(|event| matches!(event, Event::Connected { peer: 1, .. }))
    });
    // Node 1's grant is withdrawn on node 2, which refuses it for its identity: node 1 is told so,
    // as a dial whose certificate was refused at its handshake is.
    let at = net.now;
    net.b.refuse(at, 1, Refusal::Identity);
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        let _ = events(&mut net.b);
        !asker.closed.is_empty()
    });
    assert_eq!(asker.closed, vec![(2, Some(Refusal::Identity))]);
    assert!(asker.unreachable.is_empty(), "{:?}", asker.unreachable);
}

#[test]
fn a_server_whose_certificate_does_not_verify_is_unreachable_for_its_identity() {
    let pair = Pair::new();
    let now = now();
    // Node 1 trusts another authority than the one that issued node 2's certificate.
    let stranger = Pki::new();
    let config = Config {
        tls: Tls::mutual(&credentials(&stranger.root, &pair.one.0, &pair.one.1), true).unwrap(),
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    let a = Endpoint::<Mantle, _, _>::new(
        config,
        Fixed::new(64 << 20, 64),
        pair.book(Role::Node, Role::Node),
        now,
    )
    .unwrap();
    let b = pair.node::<Mantle>(
        2,
        Role::Node,
        pair.book(Role::Node, Role::Node),
        limits(),
        64 << 20,
        now,
    );
    let mut net = Net::new(now, a, b);
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    let mut asker = Asker::new();
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        let _ = events(&mut net.b);
        !asker.unreachable.is_empty() || !asker.closed.is_empty()
    });
    // Its own verifier refused the server: the dial never came up.
    assert_eq!(asker.unreachable, vec![(2, Some(Refusal::Identity))]);
    assert!(asker.closed.is_empty(), "{:?}", asker.closed);
    assert!(asker.connected.is_empty());
}

#[test]
fn a_dial_refused_at_the_peers_identity_bound_is_told_it_was_full() {
    let pair = Pair::new();
    let now = now();
    let third = pair.pki.issue(&name(3));
    let mut full = limits();
    full.admission.identities = 1;
    let book = || {
        let mut book = pair.book(Role::Node, Role::Node);
        book.add(&third.0, 3, Role::Node);
        book
    };
    let config = |own: &(Vec<u8>, Vec<u8>)| Config {
        tls: Tls::mutual(&credentials(&pair.pki.root, &own.0, &own.1), true).unwrap(),
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    let three = Node::<Mantle>::new(config(&third), Fixed::new(64 << 20, 64), book(), now).unwrap();
    let server = pair.node::<Mantle>(2, Role::Node, book(), full, 64 << 20, now);
    // Node 3 takes the server's one identity.
    let mut net = Net::new(now, three, server);
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    net.until(TURNS, |net| {
        let _ = events(&mut net.b);
        events(&mut net.a)
            .iter()
            .any(|event| matches!(event, Event::Connected { peer: 2, .. }))
    });
    let Net {
        now,
        a: three,
        b: server,
        ..
    } = net;
    // Node 1, another identity, from another address.
    let one = pair.node::<Mantle>(1, Role::Node, book(), limits(), 64 << 20, now);
    let mut net = Net::new(now, one, server);
    net.a_address = "127.0.0.1:5555".parse::<SocketAddr>().unwrap();
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    let mut asker = Asker::new();
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        let _ = events(&mut net.b);
        !asker.unreachable.is_empty() || !asker.closed.is_empty()
    });
    let told: Vec<_> = asker.unreachable.iter().chain(&asker.closed).collect();
    assert_eq!(told, vec![&(2, Some(Refusal::Identities))]);
    assert_eq!(net.b.stats().admission.identities, 1);
    drop(three);
}

/// An owner swaps its TLS again and again while the connection it dialed under the first stays up,
/// as a node renews its certificate and keeps the connections it holds: every swap is taken, since
/// the endpoint counts its configuration slots from what it may hold (each replaced configuration
/// still held has a handshake or a connection holding it), and the connection still carries an
/// exchange. The swaps outnumber twice the two sides × two generations an endpoint holds by
/// default.
#[test]
fn tls_swapped_again_and_again_while_a_dialed_connection_stays_up() {
    let pair = Pair::new();
    let now = now();
    let one = pair.node::<Mantle>(
        1,
        Role::Node,
        pair.book(Role::Node, Role::Node),
        limits(),
        64 << 20,
        now,
    );
    let two = pair.node::<Mantle>(
        2,
        Role::Node,
        pair.book(Role::Node, Role::Node),
        limits(),
        64 << 20,
        now,
    );
    let mut net = Net::new(now, one, two);
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    net.until(TURNS, |net| {
        let _ = events(&mut net.b);
        events(&mut net.a)
            .iter()
            .any(|event| matches!(event, Event::Connected { peer: 2, .. }))
    });
    for _ in 0..8 {
        net.a
            .set_tls(
                Tls::mutual(&credentials(&pair.pki.root, &pair.one.0, &pair.one.1), true).unwrap(),
            )
            .expect("a swap is taken while the connection dialed under an earlier one stays up");
    }
    let mut asker = Asker::new();
    let mut served = Server::new();
    asker
        .ask(
            &mut net.a,
            net.now,
            2,
            (Kind::Get, Class::Request),
            7,
            Some(4096),
            PERIOD,
        )
        .unwrap();
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        served.serve(&mut net.b);
        asker.finished()
    });
    assert!(asker.asked[0].done, "{:?}", asker.events);
}

#[test]
fn tls_set_while_running_is_what_the_next_handshake_meets_and_the_last_connection_stays() {
    let pair = Pair::new();
    let now = now();
    // The authority is succeeded: node 2 is reissued under it, and so is node 3.
    let successor = Pki::new();
    let renewed = successor.issue(&name(2));
    let third = successor.issue(&name(3));
    let server_book = || {
        let mut book = pair.book(Role::Node, Role::Node);
        book.add(&third.0, 3, Role::Node);
        book
    };
    let one = pair.node::<Mantle>(
        1,
        Role::Node,
        pair.book(Role::Node, Role::Node),
        limits(),
        64 << 20,
        now,
    );
    let server = pair.node::<Mantle>(2, Role::Node, server_book(), limits(), 64 << 20, now);
    let mut net = Net::new(now, one, server);
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    net.until(TURNS, |net| {
        let _ = events(&mut net.b);
        events(&mut net.a)
            .iter()
            .any(|event| matches!(event, Event::Connected { peer: 2, .. }))
    });
    // Node 2 now presents its renewed certificate and trusts the successor alone.
    net.b
        .set_tls(Tls::mutual(&credentials(&successor.root, &renewed.0, &renewed.1), true).unwrap())
        .unwrap();
    // The connection made under the last TLS still carries an exchange.
    let mut asker = Asker::new();
    let mut served = Server::new();
    asker
        .ask(
            &mut net.a,
            net.now,
            2,
            (Kind::Get, Class::Request),
            7,
            Some(4096),
            PERIOD,
        )
        .unwrap();
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        served.serve(&mut net.b);
        asker.finished()
    });
    assert!(asker.asked[0].done, "{:?}", asker.events);
    let Net {
        now,
        a: one,
        b: server,
        ..
    } = net;
    // Node 3, under the successor, meets the renewed certificate and is admitted.
    let mut three_book = Book::default();
    three_book.add(&renewed.0, 2, Role::Node);
    let config = Config {
        tls: Tls::mutual(&credentials(&successor.root, &third.0, &third.1), false).unwrap(),
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    let three = Node::<Mantle>::new(config, Fixed::new(64 << 20, 64), three_book, now).unwrap();
    let mut net = Net::new(now, three, server);
    net.a_address = "127.0.0.1:5556".parse::<SocketAddr>().unwrap();
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    let mut asker = Asker::new();
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        let _ = events(&mut net.b);
        !asker.connected.is_empty() || !asker.unreachable.is_empty()
    });
    assert_eq!(asker.connected, vec![2], "{:?}", asker.events);
    let Net {
        now,
        a: three,
        b: server,
        ..
    } = net;
    // A new node 1 under the authority node 2 no longer trusts is refused for its identity.
    let late = pair.node::<Mantle>(
        1,
        Role::Node,
        pair.book(Role::Node, Role::Node),
        limits(),
        64 << 20,
        now,
    );
    let mut net = Net::new(now, late, server);
    net.a_address = "127.0.0.1:5557".parse::<SocketAddr>().unwrap();
    let address = net.b_address;
    net.a.connect(now, 2, address).unwrap();
    let mut asker = Asker::new();
    net.until(TURNS, |net| {
        asker.drive(&mut net.a);
        let _ = events(&mut net.b);
        !asker.unreachable.is_empty() || !asker.closed.is_empty()
    });
    let told: Vec<_> = asker.unreachable.iter().chain(&asker.closed).collect();
    assert_eq!(told, vec![&(2, Some(Refusal::Identity))]);
    drop((one, three));
}

#[test]
fn a_tls_without_a_protocol_is_refused() {
    let pair = Pair::new();
    let mut tls =
        Tls::mutual(&credentials(&pair.pki.root, &pair.one.0, &pair.one.1), true).unwrap();
    tls.client.alpn_protocols.clear();
    let config = Config {
        tls,
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    let refused =
        Endpoint::<Mantle, _, _>::new(config, Fixed::new(1 << 20, 4), Book::default(), now());
    assert_eq!(refused.err(), Some(Refusal::Configuration));
    let mut tls =
        Tls::mutual(&credentials(&pair.pki.root, &pair.one.0, &pair.one.1), true).unwrap();
    if let Some(server) = tls.server.as_mut() {
        server.alpn_protocols.clear();
    }
    let config = Config {
        tls,
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    let refused =
        Endpoint::<Mantle, _, _>::new(config, Fixed::new(1 << 20, 4), Book::default(), now());
    assert_eq!(refused.err(), Some(Refusal::Configuration));
}

trait SeenBy {
    fn directory_seen(&self) -> Vec<Seen>;
}

impl SeenBy for Open {
    fn directory_seen(&self) -> Vec<Seen> {
        self.directory().seen.clone()
    }
}

/// The owner's congestion controller is what its connections run (`Config::congestion`): Copa keeps
/// no slow-start threshold and states the rate it paces at once it has a round trip; the default,
/// CUBIC, keeps a threshold and leaves pacing to the connection's rule.
#[test]
fn the_owners_congestion_controller_is_what_its_connections_run() {
    use hyper_transport::congestion::{Congestion, CopaConfig};
    let pair = Pair::new();
    let run = |congestion: Congestion| {
        let now = now();
        let config = |own: &(Vec<u8>, Vec<u8>), congestion: Congestion| Config {
            tls: Tls::mutual(&credentials(&pair.pki.root, &own.0, &own.1), true).unwrap(),
            role: Role::Node,
            limits: limits(),
            congestion,
        };
        let book = || pair.book(Role::Node, Role::Node);
        let a = Node::<Mantle>::new(
            config(&pair.one, congestion.clone()),
            Fixed::new(64 << 20, 64),
            book(),
            now,
        )
        .unwrap();
        let b = Node::<Mantle>::new(
            config(&pair.two, congestion),
            Fixed::new(64 << 20, 64),
            book(),
            now,
        )
        .unwrap();
        let mut net = Net::new(now, a, b);
        let address = net.b_address;
        net.a.connect(now, 2, address).unwrap();
        // A body that takes round trips, so the controller has samples.
        let mut asker = Asker::new();
        let mut served = Server::new();
        net.until(TURNS, |net| {
            let _ = events(&mut net.b);
            events(&mut net.a)
                .iter()
                .any(|event| matches!(event, Event::Connected { .. }))
        });
        asker
            .ask(
                &mut net.a,
                net.now,
                2,
                (Kind::Get, Class::Request),
                3,
                Some(256 << 10),
                PERIOD,
            )
            .unwrap();
        net.until(TURNS, |net| {
            asker.drive(&mut net.a);
            served.serve(&mut net.b);
            asker.finished()
        });
        net.a.path(2).unwrap()
    };
    let copa = run(Congestion::Copa(CopaConfig::default()));
    assert_eq!(copa.ssthresh, None, "{copa:?}");
    assert!(copa.pacing_rate.is_some_and(|rate| rate > 0), "{copa:?}");
    let cubic = run(Congestion::default());
    assert!(cubic.ssthresh.is_some(), "{cubic:?}");
    assert_eq!(cubic.pacing_rate, None, "{cubic:?}");
}
