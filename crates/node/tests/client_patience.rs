//! The node's client gives each kind of work its own measured period (`mantle_node::client`): an
//! object's request asked with no deadline of a node that never answers it is given up once an
//! object's reply was measured, and a chunk's transfer its node says it works on is not cut by the
//! pace of the small requests measured before it. The node is a stub of mantle's own protocol,
//! built from hyper-transport on a thread of its own, its client on the test's.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::net::SocketAddr;
use std::pin::pin;
use std::sync::mpsc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use hyper_rt::combine::{Either, race2};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Config, Endpoint, Event, ExchangeId, Fixed, Refusal};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_chunk::ChunkKey;
use mantle_meta::wire::Entry;
use mantle_node::client::{Client, ClientError, ClientSettings, ObjectOutcome};
use mantle_node::directory::{Peer, Peers};
use mantle_node::node::ChunkOutcome;
use mantle_node::{chunk, command};
use mantle_protocol::bounds::{self, Stated};
use mantle_protocol::classes::{Class, Kind, NodeClasses, Role};
use mantle_protocol::object::{Op, Reply, Status};

#[allow(dead_code)]
#[path = "support/net.rs"]
mod net;

const NODE: u64 = 1;
const CLIENT: u64 = 7;
const AUTHORITY: &str = "mantle client patience test";
/// The bytes a chunk get is answered with: two of the transport's receive chunks.
const BYTES: usize = 2 * hyper_transport::RECEIVE_CHUNK;

/// The byte at `at` of a get's answer: its offset modulo a prime, so a byte moved, dropped or
/// repeated shows.
fn pattern(at: usize) -> u8 {
    u8::try_from(at % 251).unwrap()
}

/// What the stub node does with each request it is asked, in the order they come, once it has
/// read the request whole.
#[derive(Clone, Copy, Debug)]
enum Answer {
    /// At once: a command `NoRange`, an object's request `Done`, a chunk put `Stored`, a chunk
    /// get `Read` with [`BYTES`].
    Now,
    /// Never, and says nothing of it: a node that stopped.
    Never,
    /// As [`Answer::Now`], after two of its serve periods, saying each beat that it works on it, as
    /// a node does while its volume makes a chunk durable (`Endpoint::working`).
    Working,
}

/// A request being read: what the stub answers it with once it is whole.
#[derive(Clone, Copy)]
struct Reading {
    exchange: ExchangeId,
    class: Class,
    kind: Kind,
    answer: Answer,
    /// Whether a body follows its head.
    body: bool,
}

/// What the stub saw: the requests it was asked, those its client cut, and the notes of its work
/// it made.
#[derive(Debug, Default)]
struct Seen {
    asked: usize,
    cut: u32,
    notes: u64,
}

/// The node's certificate and key, the client's, and their authority, all DER.
struct Pki {
    authority: Vec<u8>,
    node: (Vec<u8>, Vec<u8>),
    client: (Vec<u8>, Vec<u8>),
}

impl Pki {
    fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let validity = Validity {
            not_before: now - 3_600,
            not_after: now + 86_400,
        };
        let key = KeyPair::generate().unwrap();
        let authority =
            mantle_cert::authority(&key, AUTHORITY, validity, Serial::draw().unwrap()).unwrap();
        let issue = |name: &str| {
            let own = KeyPair::generate().unwrap();
            let certificate = mantle_cert::node(
                &key,
                AUTHORITY,
                own.public_key(),
                name,
                (validity, Serial::draw().unwrap()),
            )
            .unwrap();
            (certificate, own.pkcs8().to_vec())
        };
        Self {
            node: issue(&mantle_protocol::node_name(NODE)),
            client: issue(&format!("client-{CLIENT}")),
            authority,
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

fn runtime(sockets: usize) -> LocalRuntime {
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 2,
        interests_per_shard: interests_for(1).max(2 * sockets),
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

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap()
}

/// What the stub node states of its transport.
fn node_stated() -> Stated {
    let least = bounds::least_transport(&net::stated(0)).unwrap();
    net::stated(least)
}

/// The client's settings: it knows the stub node, as a gateway, which also moves chunks.
fn client_settings(pki: &Pki) -> ClientSettings {
    let messages = node_stated().messages;
    let transport_bytes = bounds::least_transport(&Stated {
        peers: 1,
        clients: 0,
        pool: 0,
        ranges: 1,
        inflight: 0,
        lanes: 1,
        transfers: 0,
        asked: 1,
        transport_bytes: 0,
        messages,
        max_head: mantle_node::node::MAX_HEAD,
    })
    .unwrap();
    ClientSettings {
        id: CLIENT,
        bind: "127.0.0.1:0".parse().unwrap(),
        credentials: pki.credentials(&pki.client),
        nodes: vec![Peer {
            id: NODE,
            role: Role::Node,
            certificate: pki.node.0.clone(),
        }],
        role: Role::Gateway,
        messages,
        transport_bytes,
        seed: None,
    }
}

type Stub = Driver<NodeClasses, Fixed, Peers>;

/// A reply being written: its exchange, its body, and how much of it is written.
struct Writing {
    exchange: ExchangeId,
    body: Vec<u8>,
    written: usize,
}

/// Replies to `exchange`, a request of `kind`, at once: what is left to write of its body.
fn answer(driver: &mut Stub, exchange: ExchangeId, kind: Kind) -> Option<Writing> {
    let endpoint = driver.endpoint();
    let (head, body) = match kind {
        Kind::Command | Kind::Read => (
            command::reply_head(command::Status::NoRange, 0, 0).to_vec(),
            None,
        ),
        Kind::ChunkPut => (chunk::reply_head(chunk::Status::Stored).to_vec(), None),
        Kind::Object => {
            let (head, fields) = Reply::status(Status::Done).encode().unwrap();
            (head, Some(fields))
        }
        Kind::ChunkGet => (
            chunk::reply_head(chunk::Status::Read).to_vec(),
            Some((0..BYTES).map(pattern).collect()),
        ),
        other => panic!("the stub answers no {other:?}"),
    };
    let Some(body) = body else {
        endpoint.reply(exchange, &head, None).unwrap();
        endpoint.end(exchange);
        return None;
    };
    endpoint
        .reply(exchange, &head, Some(body.len() as u64))
        .unwrap();
    Some(Writing {
        exchange,
        body,
        written: 0,
    })
}

/// Writes what the reply's stream takes; whether it is written whole, and ended.
fn write(driver: &mut Stub, writing: &mut Writing) -> bool {
    let endpoint = driver.endpoint();
    while writing.written < writing.body.len() {
        let took = endpoint
            .write_body(writing.exchange, &writing.body[writing.written..])
            .unwrap();
        if took == 0 {
            return false;
        }
        writing.written += took;
    }
    endpoint.end(writing.exchange);
    true
}

/// Reads and lets go of what came of `exchange`'s request, of `class`.
fn drain(driver: &mut Stub, (exchange, class): (ExchangeId, Class)) {
    let endpoint = driver.endpoint();
    loop {
        let mut piece = endpoint
            .reserve(class, hyper_transport::RECEIVE_CHUNK as u64)
            .unwrap();
        let got = endpoint.read_body(exchange, &mut piece);
        endpoint.release(piece);
        match got {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Runs the stub node, answering the requests it is asked as `answers` says, each past them at
/// once, until its client's connection closes; tells `bound` where it listens.
fn stub(
    credentials: Credentials,
    client: Vec<u8>,
    answers: Vec<Answer>,
    bound: mpsc::Sender<SocketAddr>,
) -> Seen {
    runtime(4)
        .block_on(async move {
            let stated = node_stated();
            let config = Config {
                tls: Tls::mutual(&credentials, true).unwrap(),
                role: Role::Node,
                limits: bounds::limits(&stated).unwrap(),
                congestion: Default::default(),
            };
            let directory = Peers::new(
                vec![Peer {
                    id: CLIENT,
                    role: Role::Gateway,
                    certificate: client,
                }],
                0,
            );
            let endpoint = Endpoint::<NodeClasses, _, _>::new(
                config,
                Fixed::new(stated.transport_bytes, 1),
                directory,
                Instant::now(),
            )
            .unwrap();
            let mut driver = Driver::bind(
                endpoint,
                "127.0.0.1:0".parse().unwrap(),
                bounds::io(&stated),
            )
            .unwrap();
            bound.send(driver.local_addr().unwrap()).unwrap();
            let served = nanos(bounds::idle_timeout());
            let beat = served / 3;
            let mut seen = Seen::default();
            // The request last asked, its body read as it comes.
            let mut reading: Option<Reading> = None;
            let mut writing: Option<Writing> = None;
            // The request held while the stub works on it, its kind, and when its work ends.
            let mut working: Option<(ExchangeId, Kind, u64)> = None;
            loop {
                let pause = working.map(|(_, _, until)| {
                    until
                        .saturating_sub(hyper_rt::futures::now_ns())
                        .clamp(1, beat)
                });
                let event = match pause {
                    None => Some(driver.event().await.unwrap()),
                    Some(pause) => {
                        let event = pin!(driver.event());
                        let sleep = pin!(hyper_rt::futures::sleep(pause));
                        match race2(event, sleep).await {
                            Either::First(event) => Some(event.unwrap()),
                            Either::Second(slept) => {
                                slept.unwrap();
                                None
                            }
                        }
                    }
                };
                match event {
                    Some(Event::Request {
                        exchange,
                        kind,
                        class,
                        body,
                        ..
                    }) => {
                        let answer = answers.get(seen.asked).copied().unwrap_or(Answer::Now);
                        seen.asked += 1;
                        reading = Some(Reading {
                            exchange,
                            class,
                            kind,
                            answer,
                            body: body.is_some(),
                        });
                    }
                    Some(Event::Refused {
                        exchange,
                        by_peer: true,
                        ..
                    }) => {
                        seen.cut += 1;
                        reading = reading.filter(|r| r.exchange != exchange);
                    }
                    Some(Event::Closed { .. }) => {
                        seen.notes = driver.endpoint().stats().notes_made;
                        return seen;
                    }
                    _ => {}
                }
                // A request is answered once it is read whole, as a node answers one.
                if let Some(read) = reading {
                    drain(&mut driver, (read.exchange, read.class));
                    if !read.body || driver.endpoint().body_complete(read.exchange) {
                        reading = None;
                        match read.answer {
                            Answer::Now => writing = answer(&mut driver, read.exchange, read.kind),
                            Answer::Never => {}
                            Answer::Working => {
                                let until = hyper_rt::futures::now_ns() + 2 * served;
                                working = Some((read.exchange, read.kind, until));
                            }
                        }
                    }
                }
                if let Some((exchange, kind, until)) = working {
                    if hyper_rt::futures::now_ns() >= until {
                        working = None;
                        writing = answer(&mut driver, exchange, kind);
                    } else {
                        driver.endpoint().working(exchange);
                    }
                }
                if let Some(reply) = writing.as_mut()
                    && write(&mut driver, reply)
                {
                    writing = None;
                }
                driver.flush().unwrap();
            }
        })
        .unwrap()
}

/// Runs the stub node answering as `answers` says, and `asks` on the test's thread with a client
/// that knows it; what the stub saw, once the client is dropped and its connection closed.
fn with_stub<T: 'static>(
    answers: Vec<Answer>,
    asks: impl AsyncFnOnce(&mut Client) -> T + 'static,
) -> (T, Seen) {
    let pki = Pki::new();
    let (bound, at) = mpsc::channel();
    let (credentials, client_certificate) = (pki.credentials(&pki.node), pki.client.0.clone());
    let node = std::thread::spawn(move || stub(credentials, client_certificate, answers, bound));
    let address = at.recv().unwrap();
    let settings = client_settings(&pki);
    let asked = runtime(4)
        .block_on(async move {
            let mut client = Client::open(settings).await.unwrap();
            client.learn(NODE, address).unwrap();
            asks(&mut client).await
        })
        .unwrap();
    (asked, node.join().unwrap())
}

fn head_bucket() -> Op {
    Op::HeadBucket {
        bucket: "patience".into(),
    }
}

/// An object's request asked with no deadline of a node that never answers it, nor says it works
/// on it, is given up once an object's reply was measured: the client has a patience, and the
/// exchange is cut `Stalled` by its period, never sooner than the node's serve period, the least an
/// asker gives a node's work. Before each kind of work was measured, an object's reply measured
/// nothing: a client asking only objects had no patience, and an exchange asked with no deadline
/// was given all of it (the repair suite's puts through a stalled node, `u64::MAX`).
#[test]
fn an_object_asked_with_no_deadline_of_a_node_that_never_answers_is_cut_at_the_measured_period() {
    let ((cut, waited), seen) = with_stub(vec![Answer::Now, Answer::Never], async |client| {
        let first = client
            .object(NODE, &head_bucket(), &[], u64::MAX)
            .await
            .unwrap();
        assert!(
            matches!(&first, ObjectOutcome::Replied { reply, .. } if reply.status == Status::Done),
            "the first request answered {first:?}"
        );
        assert!(
            client.patience().is_some(),
            "an object's reply measured gives the client no patience"
        );
        let asked = hyper_rt::futures::now_ns();
        let cut = client.object(NODE, &head_bucket(), &[], u64::MAX).await;
        (cut, hyper_rt::futures::now_ns() - asked)
    });
    assert!(
        matches!(cut, Ok(ObjectOutcome::Lost(Refusal::Stalled))),
        "the unanswered request ended {cut:?}"
    );
    assert!(
        waited >= nanos(bounds::idle_timeout()),
        "cut after {waited} ns, sooner than the node's serve period"
    );
    assert_eq!(seen.asked, 2, "the stub was asked {seen:?}");
    assert_eq!(
        seen.cut, 1,
        "the client cut no request of the stub's: {seen:?}"
    );
}

/// A chunk put its node holds for two of its serve periods once it has the chunk whole, saying each
/// beat that it works on it as a node does while its volume makes a chunk durable, is answered,
/// though the client measured a command answered at once and a chunk get answered at once before
/// it: neither kind's quick reply cuts a transfer that moves. Measured as one, the command's reply
/// gave the put a period of a few round trips, and the node's second note, a beat after its first,
/// came after the client had cut it.
#[test]
fn a_chunk_its_node_works_on_past_every_reply_measured_is_not_cut() {
    let ((first, held, asks), seen) = with_stub(
        vec![Answer::Now, Answer::Now, Answer::Working],
        async |client| {
            let entry = Entry {
                at_ns: 0,
                commands: Vec::new(),
            };
            let refused = client.command(1, &entry, u64::MAX).await;
            assert!(
                matches!(refused, Err(ClientError::Status(command::Status::NoRange))),
                "the command answered {refused:?}"
            );
            let key = ChunkKey {
                block: 1,
                epoch: 0,
                index: 0,
            };
            let first = client
                .get_chunk(NODE, (1, key), (0, BYTES as u64), u64::MAX)
                .await
                .unwrap();
            let bytes: Vec<u8> = (0..BYTES).map(pattern).collect();
            let crc32c = mantle_crc::crc32c(&bytes);
            let held = client
                .put_chunk(NODE, (1, key), &bytes, crc32c, u64::MAX)
                .await
                .unwrap();
            (first, held, client.asks())
        },
    );
    match first {
        ChunkOutcome::Read(bytes) => {
            assert_eq!(bytes.len(), BYTES);
            assert!(bytes.iter().enumerate().all(|(at, b)| *b == pattern(at)));
        }
        other => panic!("the get answered at once ended {other:?}"),
    }
    assert!(
        matches!(held, ChunkOutcome::Stored),
        "the put its node worked on ended {held:?}"
    );
    assert_eq!(asks.lost, 0, "{asks:?}");
    assert_eq!(seen.asked, 3, "the stub was asked {seen:?}");
    assert_eq!(seen.cut, 0, "the client cut a request: {seen:?}");
    assert!(
        seen.notes >= 2,
        "the stub said it worked {} times",
        seen.notes
    );
}
