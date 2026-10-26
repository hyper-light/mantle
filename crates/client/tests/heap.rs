//! The client's heap while it puts and gets an object many windows long: counted by hyper-measure's
//! allocator on the client's thread alone, against the bound the client states
//! (`mantle_client::memory`), from before it opens to after it is dropped. A node of the native
//! protocol's own, built from `mantle_protocol` and hyper-transport on a thread of its own, answers
//! it: it holds a put's bytes to their trailer's CRC-64/NVME and keeps nothing of them, and answers a
//! get with bytes it makes as it sends them.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::large_enum_variant
)]

use std::net::SocketAddr;
use std::sync::mpsc::channel;
use std::time::{SystemTime, UNIX_EPOCH};

use hyper_measure::alloc;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{
    Authenticated, Config, Directory, Endpoint, Event, ExchangeId, Fixed, PeerId,
};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_client::{Client, Settings, Sink, Source, TRAILER};
use mantle_protocol::bounds;
use mantle_protocol::classes::{Class, NodeClasses, Role};
use mantle_protocol::object::{Head, Headers, Op, Reply, Status, Stored};
use mantle_s3::checksum::Hasher;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

const NODE: PeerId = 1;
const CLIENT: PeerId = 7;

/// The byte at `at` of the test's object: its offset's low byte, times a prime, so a byte moved,
/// dropped or repeated shows.
fn pattern(at: u64) -> u8 {
    (at.wrapping_mul(131) % 251) as u8
}

/// The object's bytes, made as they are read.
struct Made {
    at: u64,
    length: u64,
}

impl Source for Made {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let n = into.len().min((self.length - self.at) as usize);
        for byte in &mut into[..n] {
            *byte = pattern(self.at);
            self.at += 1;
        }
        Ok(n)
    }
}

/// A get's bytes, each checked against the pattern as it comes and kept nowhere.
struct Checked {
    at: u64,
}

impl Sink for Checked {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        for byte in bytes {
            if *byte != pattern(self.at) {
                return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
            }
            self.at += 1;
        }
        Ok(())
    }
}

/// The CRC-64/NVME of the object's first `length` bytes.
fn crc(length: u64) -> Vec<u8> {
    let mut hasher = Hasher::new(TRAILER).unwrap();
    let mut piece = vec![0u8; 1 << 16];
    let mut at = 0u64;
    while at < length {
        let n = piece.len().min((length - at) as usize);
        for byte in &mut piece[..n] {
            *byte = pattern(at);
            at += 1;
        }
        hasher.update(&piece[..n]).unwrap();
    }
    hasher.finish().unwrap().bytes
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
        page_bytes: usize::try_from(hyper_rt::machine::facts::Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

/// The cell's authority's certificate, and the node's and the client's credentials under it.
fn credentials() -> (Credentials, Credentials) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let validity = Validity {
        not_before: now - 3_600,
        not_after: now + 86_400,
    };
    let authority_key = KeyPair::generate().unwrap();
    let authority =
        mantle_cert::authority(&authority_key, "cell", validity, Serial::draw().unwrap()).unwrap();
    let issue = |certificate: Vec<u8>, key: &KeyPair| Credentials {
        chain: vec![CertificateDer::from(certificate)],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.pkcs8().to_vec())),
        roots: vec![CertificateDer::from(authority.clone())],
    };
    let node_key = KeyPair::generate().unwrap();
    let node = mantle_cert::node(
        &authority_key,
        "cell",
        node_key.public_key(),
        &mantle_protocol::node_name(NODE),
        (validity, Serial::draw().unwrap()),
    )
    .unwrap();
    let client_key = KeyPair::generate().unwrap();
    let client = mantle_cert::client(
        &authority_key,
        "cell",
        client_key.public_key(),
        (&mantle_protocol::client_name(CLIENT), "123456789012"),
        (validity, Serial::draw().unwrap()),
    )
    .unwrap();
    (issue(node, &node_key), issue(client, &client_key))
}

/// The stub node's directory: a client by its certificate's names, as a node admits one.
struct Clients;

impl Directory for Clients {
    type Role = Role;

    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Role)> {
        if authenticated.dialed.is_some() {
            return None;
        }
        let names = mantle_cert::client_names(authenticated.certificate?).ok()?;
        Some((mantle_protocol::client_id(names.name)?, Role::Client))
    }

    fn server_name(&self, _: PeerId) -> Option<&str> {
        None
    }
}

/// What the stub node is doing with the exchange it serves.
enum Serving {
    /// Reading a request: its head, its fields as they come, the bytes past them, and a put's
    /// checksum and trailer.
    Reading {
        exchange: ExchangeId,
        head: Head,
        fields: Vec<u8>,
        past: u64,
        hasher: Option<Hasher>,
        trailer: Vec<u8>,
    },
    /// Writing a reply: its fields, then `length` bytes of the pattern, `written` of them so far.
    Writing {
        exchange: ExchangeId,
        fields: Vec<u8>,
        length: u64,
        written: u64,
    },
}

/// Runs the stub node at `bound` until its client's connection closes, telling where it listens.
fn stub(
    credentials: Credentials,
    transport_bytes: u64,
    length: u64,
    bound: std::sync::mpsc::Sender<SocketAddr>,
) {
    let get_crc = crc(length);
    let stated = mantle_client::stated(1, transport_bytes);
    runtime(8)
        .block_on(async move {
            let config = Config {
                tls: Tls::mutual(&credentials, true).unwrap(),
                role: Role::Node,
                limits: bounds::limits(&stated).unwrap(),
                congestion: Default::default(),
            };
            let endpoint = Endpoint::<NodeClasses, _, _>::new(
                config,
                Fixed::new(transport_bytes, 1),
                Clients,
                std::time::Instant::now(),
            )
            .unwrap();
            let mut driver = Driver::bind(
                endpoint,
                "127.0.0.1:0".parse().unwrap(),
                bounds::io(&stated),
            )
            .unwrap();
            bound.send(driver.local_addr().unwrap()).unwrap();
            let mut serving: Option<Serving> = None;
            let mut piece = vec![0u8; mantle_client::PIECE];
            loop {
                let event = driver.event().await.unwrap();
                let endpoint = driver.endpoint();
                match event {
                    Event::Request { exchange, body, .. } => {
                        let head = Head::decode(endpoint.head(exchange).unwrap(), body).unwrap();
                        serving = Some(Serving::Reading {
                            exchange,
                            head,
                            fields: Vec::new(),
                            past: 0,
                            hasher: None,
                            trailer: Vec::new(),
                        });
                    }
                    Event::Closed { .. } => return,
                    _ => {}
                }
                // Read what came of a request, and once whole, answer it.
                if let Some(Serving::Reading {
                    exchange,
                    head,
                    fields,
                    past,
                    hasher,
                    trailer,
                }) = &mut serving
                {
                    loop {
                        let mut reservation = endpoint
                            .reserve(Class::Transfer, mantle_client::PIECE as u64)
                            .unwrap();
                        let got = endpoint.read_body(*exchange, &mut reservation).unwrap();
                        let mut bytes = reservation.bytes();
                        let want = (head.fields() as usize).saturating_sub(fields.len());
                        let front = want.min(bytes.len());
                        fields.extend_from_slice(&bytes[..front]);
                        bytes = &bytes[front..];
                        if !bytes.is_empty() {
                            let op = head.op(fields).unwrap();
                            let Op::Put { length, .. } = op else {
                                panic!("bytes past a get's fields");
                            };
                            let hasher =
                                hasher.get_or_insert_with(|| Hasher::new(TRAILER).unwrap());
                            let object = (length - *past).min(bytes.len() as u64) as usize;
                            hasher.update(&bytes[..object]).unwrap();
                            *past += object as u64;
                            trailer.extend_from_slice(&bytes[object..]);
                        }
                        endpoint.release(reservation);
                        if got == 0 {
                            break;
                        }
                    }
                    if !endpoint.body_complete(*exchange) {
                        continue;
                    }
                    let op = head.op(fields).unwrap();
                    let (reply, length) = match op {
                        Op::Put { length, .. } => {
                            let sum = hasher
                                .take()
                                .unwrap_or_else(|| Hasher::new(TRAILER).unwrap())
                                .finish()
                                .unwrap()
                                .bytes;
                            let reply = if sum == *trailer {
                                Reply {
                                    size: length,
                                    etag: "put".into(),
                                    checksum: Some(Stored {
                                        algorithm: TRAILER.code(),
                                        parts: 0,
                                        value: sum,
                                    }),
                                    ..Reply::status(Status::Done)
                                }
                            } else {
                                Reply::status(Status::BadDigest)
                            };
                            (reply, 0)
                        }
                        Op::Get { .. } => (
                            Reply {
                                size: length,
                                etag: "got".into(),
                                checksum: Some(Stored {
                                    algorithm: TRAILER.code(),
                                    parts: 0,
                                    value: get_crc.clone(),
                                }),
                                ..Reply::status(Status::Done)
                            },
                            length,
                        ),
                        other => panic!("the stub answers puts and gets: {other:?}"),
                    };
                    let (reply_head, reply_fields) = reply.encode().unwrap();
                    let exchange = *exchange;
                    endpoint
                        .reply(
                            exchange,
                            &reply_head,
                            Some(reply_fields.len() as u64 + length),
                        )
                        .unwrap();
                    serving = Some(Serving::Writing {
                        exchange,
                        fields: reply_fields,
                        length,
                        written: 0,
                    });
                }
                // Write what the reply's stream takes.
                if let Some(Serving::Writing {
                    exchange,
                    fields,
                    length,
                    written,
                }) = &mut serving
                {
                    let total = fields.len() as u64 + *length;
                    while *written < total {
                        let took = if (*written as usize) < fields.len() {
                            endpoint
                                .write_body(*exchange, &fields[*written as usize..])
                                .unwrap()
                        } else {
                            let at = *written - fields.len() as u64;
                            let n = piece.len().min((*length - at) as usize);
                            for (i, byte) in piece[..n].iter_mut().enumerate() {
                                *byte = pattern(at + i as u64);
                            }
                            endpoint.write_body(*exchange, &piece[..n]).unwrap()
                        };
                        if took == 0 {
                            break;
                        }
                        *written += took as u64;
                    }
                    if *written == total {
                        endpoint.end(*exchange);
                        serving = None;
                    }
                }
                driver.flush().unwrap();
            }
        })
        .unwrap();
}

/// The client's peak heap, from before it opens until it is dropped, while it puts an object of
/// `windows` of its windows from a source that makes it and gets it into a sink that keeps none of
/// it; and the bound it states.
fn peak_of(windows: u64) -> (u64, u64) {
    let (node, client) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let stated = mantle_client::stated(1, transport_bytes);
    let window = bounds::limits(&stated).unwrap().window_ceiling;
    let length = windows * window;
    let (tell, bound) = channel();
    let served = std::thread::spawn(move || stub(node, transport_bytes, length, tell));
    let address = bound.recv().unwrap();
    let sockets = mantle_client::sockets(1).unwrap();
    let (peak, memory) = runtime(sockets)
        .block_on(async move {
            alloc::begin();
            let mut client = Client::open(Settings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials: client,
                nodes: vec![(NODE, address)],
                transport_bytes,
            })
            .await
            .unwrap();
            let memory = client.memory();
            let put = Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: mantle_client::identity().unwrap(),
                length,
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: Some(TRAILER),
            };
            let made = client
                .put(&put, &mut Made { at: 0, length }, u64::MAX)
                .await
                .unwrap();
            assert_eq!(made.status, Status::Done);
            let get = Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
            };
            let mut checked = Checked { at: 0 };
            let got = client.get(&get, &mut checked, u64::MAX).await.unwrap();
            assert_eq!((got.status, checked.at), (Status::Done, length));
            drop(client);
            let counts = alloc::end();
            (counts.peak, memory)
        })
        .unwrap();
    served.join().unwrap();
    let peak = u64::try_from(peak).unwrap();
    eprintln!("an object of {length} bytes, {windows} windows: peak {peak} of the {memory} stated");
    (peak, memory)
}

/// A put and a get of an object many windows long hold the client within the memory it states,
/// and one sixteen times longer within the same.
#[test]
fn a_put_and_a_get_many_windows_long_hold_the_client_within_its_stated_memory() {
    for windows in [64, 1024] {
        let (peak, memory) = peak_of(windows);
        assert_eq!(peak.saturating_sub(memory), 0, "{peak} past {memory}");
    }
}

/// A put whose source ends before the length its op states is refused before its last byte, so
/// nothing commits, and the node is told the body was cut short.
#[test]
fn a_source_shorter_than_its_stated_length_cuts_the_put_short() {
    let (node, client) = credentials();
    let transport_bytes = mantle_client::least_transport(1).unwrap();
    let (tell, bound) = channel();
    let served = std::thread::spawn(move || stub(node, transport_bytes, 0, tell));
    let address = bound.recv().unwrap();
    let sockets = mantle_client::sockets(1).unwrap();
    runtime(sockets)
        .block_on(async move {
            let mut client = Client::open(Settings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials: client,
                nodes: vec![(NODE, address)],
                transport_bytes,
            })
            .await
            .unwrap();
            let put = Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: mantle_client::identity().unwrap(),
                length: 10,
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: Some(TRAILER),
            };
            let short: &[u8] = &[1, 2, 3];
            let refused = client.put(&put, &mut { short }, u64::MAX).await;
            assert!(
                matches!(
                    refused,
                    Err(mantle_client::ClientError::Length {
                        stated: 10,
                        read: 3
                    })
                ),
                "{refused:?}"
            );
        })
        .unwrap();
    served.join().unwrap();
}
