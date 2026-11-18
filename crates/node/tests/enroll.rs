//! A written-off node's request for a new identity over the wire (docs/design/membership.md §5.1
//! step 3): `rejoin::ask` dials a member under its old certificate on the enrollment protocol
//! alone, and the member, a stub built from hyper-transport on a thread of its own, admits it as
//! enrolling by the ledger's entry, judges its request against the identity its handshake proved
//! and answers it once: the certificate the issuer signed, which a node's start takes as the
//! issuer's (`mantle_cert::node_issued`), or the refusal.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::net::SocketAddr;
use std::sync::mpsc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Config, Endpoint, Event, Fixed};
use mantle_cert::{IssuedBy, KeyPair, Serial, Validity};
use mantle_meta::record::{Grants, Ledgered, Rejoin};
use mantle_node::directory::{Admitted, Peers};
use mantle_node::enroll::{self, Issuer};
use mantle_node::rejoin::{self, AskError, Asking};
use mantle_protocol::bounds::{self, Stated};
use mantle_protocol::classes::{self, Class, Kind, NodeClasses, Role};
use mantle_protocol::enroll::{Answer, Granted, Member, Refusal, Request};

#[allow(dead_code)]
#[path = "support/net.rs"]
mod net;

const MEMBER: u64 = 1;
const OLD: u64 = 4;
const NEW: u64 = 9;
const AUTHORITY: &str = "mantle enroll test";

fn runtime() -> LocalRuntime {
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 2,
        interests_per_shard: interests_for(1).max(8),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// The cell's authority and its issuer, the member's identity and the written-off node's old one.
struct Cell {
    authority: Vec<u8>,
    issuer: (Vec<u8>, Vec<u8>),
    member: (Vec<u8>, Vec<u8>),
    old: (Vec<u8>, Vec<u8>),
    old_key: [u8; 32],
}

impl Cell {
    fn new() -> Self {
        let validity = Validity {
            not_before: now() - 3_600,
            not_after: now() + 86_400,
        };
        let key = KeyPair::generate().unwrap();
        let authority =
            mantle_cert::authority(&key, AUTHORITY, validity, Serial::draw().unwrap()).unwrap();
        let issuer_key = KeyPair::generate().unwrap();
        let issuer = mantle_cert::issuer(
            &key,
            AUTHORITY,
            issuer_key.public_key(),
            &format!("{AUTHORITY} issuer"),
            (validity, Serial::draw().unwrap()),
        )
        .unwrap();
        let grant = |id: u64, grant: u64| {
            let own = KeyPair::generate().unwrap();
            let certificate = mantle_cert::granted_node(
                &key,
                AUTHORITY,
                own.public_key(),
                (&mantle_protocol::node_name(id), grant),
                (validity, Serial::draw().unwrap()),
            )
            .unwrap();
            (certificate, own)
        };
        let (member, member_key) = grant(MEMBER, 1);
        let (old, old_key) = grant(OLD, 2);
        Self {
            authority,
            issuer: (issuer, issuer_key.pkcs8().to_vec()),
            member: (member, member_key.pkcs8().to_vec()),
            old_key: old_key.public_key().try_into().unwrap(),
            old: (old, old_key.pkcs8().to_vec()),
        }
    }

    fn credentials(&self, (certificate, key): &(Vec<u8>, Vec<u8>)) -> Credentials {
        Credentials {
            chain: vec![CertificateDer::from(certificate.clone())],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.clone())),
            roots: vec![CertificateDer::from(self.authority.clone())],
        }
    }
}

fn stated() -> Stated {
    let least = bounds::least_transport(&net::stated(0)).unwrap();
    net::stated(least)
}

type Stub = Driver<NodeClasses, Fixed, Peers>;

/// Runs the stub member: it serves the transport's protocol and the enrollment protocol, its
/// directory holding `OLD` written off and removed with its ledger entry, answers one request
/// as a member does, and returns the requests it took once the asker's connection closes.
fn member(
    credentials: Credentials,
    (old, issuer): (Vec<u8>, Issuer),
    bound: mpsc::Sender<SocketAddr>,
) -> usize {
    runtime()
        .block_on(async move {
            let stated = stated();
            let mut tls = Tls::mutual(&credentials, true).unwrap();
            if let Some(server) = tls.server.as_mut() {
                server.alpn_protocols.push(classes::ENROLL.to_vec());
            }
            let config = Config {
                tls,
                role: Role::Node,
                limits: bounds::limits(&stated).unwrap(),
                congestion: Default::default(),
            };
            let mut directory = Peers::new(Vec::new(), 0);
            directory.enrollments_at_once(1);
            directory.set_members(
                vec![Admitted {
                    id: OLD,
                    fingerprint: mantle_cert::fingerprint(&old),
                    admitted: false,
                }],
                (Grants::default(), 3),
                (
                    &[(
                        OLD,
                        Ledgered {
                            order: 1,
                            grant: 2,
                            fingerprint: mantle_cert::fingerprint(&old),
                            banned: false,
                            regranted: None,
                        },
                    )],
                    Rejoin::Automatic,
                ),
            );
            let endpoint = Endpoint::<NodeClasses, _, _>::new(
                config,
                Fixed::new(stated.transport_bytes, 1),
                directory,
                Instant::now(),
            )
            .unwrap();
            let mut driver: Stub = Driver::bind(
                endpoint,
                "127.0.0.1:0".parse().unwrap(),
                bounds::io(&stated),
            )
            .unwrap();
            let at = driver.local_addr().unwrap();
            bound.send(at).unwrap();
            let mut taken = 0;
            let mut asked = None;
            let mut bytes = Vec::new();
            let mut writing: Option<(hyper_transport::ExchangeId, Vec<u8>, usize)> = None;
            loop {
                match driver.event().await.unwrap() {
                    Event::Request {
                        exchange,
                        peer,
                        kind: Kind::Enroll,
                        ..
                    } => asked = Some((exchange, peer)),
                    Event::Closed { .. } => return taken,
                    _ => {}
                }
                if let Some((exchange, peer)) = asked {
                    let endpoint = driver.endpoint();
                    loop {
                        let mut piece = endpoint
                            .reserve(Class::Request, hyper_transport::RECEIVE_CHUNK as u64)
                            .unwrap();
                        let got = endpoint.read_body(exchange, &mut piece);
                        bytes.extend_from_slice(piece.bytes());
                        endpoint.release(piece);
                        if got.unwrap_or(0) == 0 {
                            break;
                        }
                    }
                    if endpoint.body_complete(exchange) {
                        asked = None;
                        taken += 1;
                        // Who asks is what the handshake proved: the request names only a key.
                        let Ok(Request::Regrant { key }) = Request::decode(&bytes) else {
                            panic!("a written-off node asks a regrant");
                        };
                        let proved = endpoint.directory().enrolling(peer).unwrap();
                        assert_eq!(proved.old, OLD);
                        let answer = match enroll::sign(
                            Some(&issuer),
                            (&proved, &key),
                            (NEW, 1),
                            (3, now()),
                        ) {
                            Ok(regrant) => Answer::Granted(Granted {
                                certificate: regrant.certificate,
                                issuer: issuer.certificate().to_vec(),
                                members: vec![Member {
                                    id: MEMBER,
                                    quic: at.to_string(),
                                    plane: at.to_string(),
                                }],
                            }),
                            Err(enroll::EnrollError::Refused(enroll::Refused::SameKey)) => {
                                Answer::Refused(Refusal::SameKey)
                            }
                            Err(other) => panic!("{other}"),
                        };
                        let body = answer.encode().unwrap();
                        endpoint
                            .reply(exchange, &[], Some(body.len() as u64))
                            .unwrap();
                        writing = Some((exchange, body, 0));
                    }
                }
                if let Some((exchange, body, written)) = writing.as_mut() {
                    let endpoint = driver.endpoint();
                    while *written < body.len() {
                        let took = endpoint.write_body(*exchange, &body[*written..]).unwrap();
                        if took == 0 {
                            break;
                        }
                        *written += took;
                    }
                    if *written == body.len() {
                        endpoint.end(*exchange);
                        writing = None;
                    }
                }
                driver.flush().unwrap();
            }
        })
        .unwrap()
}

/// Runs the stub member and `rejoin::ask` against it for `key`: what the asker came back with,
/// and the requests the member took.
fn ask(cell: &Cell, key: [u8; 32]) -> (Result<Granted, AskError>, usize) {
    let (bound, at) = mpsc::channel();
    let credentials = cell.credentials(&cell.member);
    let issuer = Issuer::new(
        cell.issuer.0.clone(),
        KeyPair::from_pkcs8(&cell.issuer.1).unwrap(),
    )
    .unwrap();
    let old = cell.old.0.clone();
    let stub = std::thread::spawn(move || member(credentials, (old, issuer), bound));
    let address = at.recv().unwrap();
    let messages = stated().messages;
    let asking = Asking {
        credentials: cell.credentials(&cell.old),
        members: vec![(MEMBER, address)],
        key,
        bind: "127.0.0.1:0".parse().unwrap(),
        messages,
        transport_bytes: stated().transport_bytes,
    };
    let asked = runtime().block_on(rejoin::ask(asking)).unwrap();
    (asked, stub.join().unwrap())
}

/// membership.md §5.1 step 3: the written-off node, under its old certificate on the enrollment
/// protocol alone, is answered once with a certificate the issuer signed for its new key, which
/// a node's start takes as the issuer's, the issuer's own and the member's places.
#[test]
fn a_written_off_node_asks_once_and_is_answered_a_certificate_the_issuer_signed() {
    let cell = Cell::new();
    let next = KeyPair::generate().unwrap();
    let (asked, taken) = ask(&cell, next.public_key().try_into().unwrap());
    let granted = asked.unwrap();
    assert_eq!(taken, 1, "one request a connection");
    assert_eq!(
        mantle_cert::node_issued(
            &granted.certificate,
            Some(&granted.issuer),
            &cell.authority,
            now()
        ),
        Ok(IssuedBy::Issuer)
    );
    let names = mantle_cert::node_names(&granted.certificate).unwrap();
    assert_eq!(names.name, mantle_protocol::node_name(NEW));
    assert_eq!(
        mantle_cert::public_key(&granted.certificate).unwrap(),
        next.public_key()
    );
    assert_eq!(granted.members.len(), 1);
    assert_eq!(granted.members[0].id, MEMBER);
}

/// The old key asked again is refused `SameKey`, typed, and the round ends on it.
#[test]
fn the_old_key_asked_again_is_refused_typed() {
    let cell = Cell::new();
    let (asked, taken) = ask(&cell, cell.old_key);
    assert!(
        matches!(asked, Err(AskError::Refused(Refusal::SameKey))),
        "{asked:?}"
    );
    assert_eq!(taken, 1);
}
