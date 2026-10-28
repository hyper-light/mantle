//! What the client's tests share: an object's bytes made as they are read and checked as they come,
//! a runtime, a cell's credentials, and a node of the native protocol's own, built from
//! `mantle_protocol` and hyper-transport, which answers puts, gets and bucket heads.

use std::net::SocketAddr;
use std::pin::pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hyper_rt::combine::{Either, race2};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{
    Authenticated, Config, Directory, Endpoint, Event, ExchangeId, Fixed, PeerId,
};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_client::{Sink, Source, TRAILER};
use mantle_protocol::bounds;
use mantle_protocol::classes::{Class, NodeClasses, Role};
use mantle_protocol::object::{Head, Op, Progress, Reply, Status, Stored};
use mantle_s3::checksum::Hasher;

pub(crate) const NODE: PeerId = 1;
pub(crate) const CLIENT: PeerId = 7;

/// The byte at `at` of the test's object: its offset times a prime, modulo another, so a byte
/// moved, dropped or repeated shows.
pub(crate) fn pattern(at: u64) -> u8 {
    (at.wrapping_mul(131) % 251) as u8
}

/// The object's bytes, made as they are read.
pub(crate) struct Made {
    pub(crate) at: u64,
    pub(crate) length: u64,
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
pub(crate) struct Checked {
    pub(crate) at: u64,
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
pub(crate) fn crc(length: u64) -> Vec<u8> {
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

pub(crate) fn runtime(sockets: usize) -> LocalRuntime {
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

/// The node's and the client's credentials under a cell's authority.
pub(crate) fn credentials() -> (Credentials, Credentials) {
    let (mut nodes, client) = credentials_of(&[NODE]);
    (nodes.remove(0), client)
}

/// As [`credentials`] for nodes `ids`, each a certificate of its own under one authority, and the
/// client's.
pub(crate) fn credentials_of(ids: &[PeerId]) -> (Vec<Credentials>, Credentials) {
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
    let nodes = ids
        .iter()
        .map(|&id| {
            let node_key = KeyPair::generate().unwrap();
            let node = mantle_cert::node(
                &authority_key,
                "cell",
                node_key.public_key(),
                &mantle_protocol::node_name(id),
                (validity, Serial::draw().unwrap()),
            )
            .unwrap();
            issue(node, &node_key)
        })
        .collect();
    let client_key = KeyPair::generate().unwrap();
    let client = mantle_cert::client(
        &authority_key,
        "cell",
        client_key.public_key(),
        (&mantle_protocol::client_name(CLIENT), "123456789012"),
        (validity, Serial::draw().unwrap()),
    )
    .unwrap();
    (nodes, issue(client, &client_key))
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

/// What the stub node does to a native upload's exchanges, each counted from 1 by its kind: the
/// Append it reads whole and never answers, as a node that died under it leaves it; the Append and
/// the Offset it holds unanswered until its client gives up on them; the Append it answers only
/// once half again as long has passed as its client waited on the one it gave up on, as a node
/// whose durable writes take longer than the client gave it answers the run sent again; and the
/// Get it holds unanswered, alive, as a node whose work for it never finishes.
#[derive(Clone, Copy, Default)]
pub(crate) struct Script {
    pub(crate) lose_append: Option<u32>,
    pub(crate) hold_append: Option<u32>,
    pub(crate) late_append: Option<u32>,
    pub(crate) hold_offset: Option<u32>,
    pub(crate) hold_get: Option<u32>,
}

/// The native upload's requests the stub took, by kind, and the exchanges its client cut.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Seen {
    pub(crate) opens: u32,
    pub(crate) appends: u32,
    pub(crate) offsets: u32,
    pub(crate) finishes: u32,
    pub(crate) gets: u32,
    pub(crate) cut: u32,
}

/// The stub's one native upload: its durable offset, its committed runs, and whether the last
/// one came; its runs are the least part long.
#[derive(Default)]
struct Runs {
    offset: u64,
    runs: u16,
    ended: bool,
}

impl Runs {
    fn progress(&self, checksum: Option<Vec<u8>>) -> Option<Box<Progress>> {
        Some(Box::new(Progress {
            offset: self.offset,
            runs: self.runs,
            run: mantle_s3::body::MIN_PART,
            ended: self.ended,
            checksum,
        }))
    }
}

/// What the stub node is doing with the exchange it serves.
enum Serving {
    /// Reading a request: its head, its fields as they come, the object's bytes past them, and a
    /// put's checksum and trailer.
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
    /// A request read whole and left unanswered.
    Holding { exchange: ExchangeId },
}

type Stub = Driver<NodeClasses, Fixed, Clients>;

/// The stub's next event, or none once `until`, the shard's time a pause ends, has come.
async fn next(driver: &mut Stub, until: Option<u64>) -> Option<Event<NodeClasses>> {
    let now = hyper_rt::futures::now_ns();
    match until {
        Some(until) if now < until => {
            let raced = {
                let event = pin!(driver.event());
                let sleep = pin!(hyper_rt::futures::sleep(until - now));
                race2(event, sleep).await
            };
            match raced {
                Either::First(event) => Some(event.unwrap()),
                Either::Second(slept) => {
                    slept.unwrap();
                    None
                }
            }
        }
        _ => Some(driver.event().await.unwrap()),
    }
}

/// Runs the stub node until its client's connection closes, telling `bound` where it listens: a
/// get's object `length` bytes long, and a put's body left unread for `pause`, where given, once
/// its first bytes came, as a node's driver waiting on durable writes leaves it.
pub(crate) fn stub(
    credentials: Credentials,
    transport_bytes: u64,
    (length, pause): (u64, Option<Duration>),
    bound: std::sync::mpsc::Sender<SocketAddr>,
) {
    scripted(
        credentials,
        transport_bytes,
        (length, pause),
        Script::default(),
        bound,
    );
}

/// As [`stub`], answering a native upload's Open, Appends, Offsets and Finish as `script` says,
/// its object the pattern's bytes: what it took of them, once its client's connection closes.
pub(crate) fn scripted(
    credentials: Credentials,
    transport_bytes: u64,
    (length, pause): (u64, Option<Duration>),
    script: Script,
    bound: std::sync::mpsc::Sender<SocketAddr>,
) -> Seen {
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
            let (mut runs, mut seen) = (Runs::default(), Seen::default());
            let mut piece = vec![0u8; mantle_client::PIECE];
            // The shard's time the pause ends, while it holds a put's body unread; once taken, a
            // pause is not taken again.
            let (mut held, mut paused) = (None::<u64>, false);
            // When the stub began holding an Append unanswered, how long its client waited on it
            // before cutting it, and when the late Append is answered.
            let (mut holding_since, mut waited, mut late) = (None::<u64>, None::<u64>, None::<u64>);
            loop {
                if let Some(event) = next(&mut driver, held).await {
                    match event {
                        Event::Refused {
                            exchange,
                            by_peer: true,
                            ..
                        } => {
                            seen.cut += 1;
                            if let Some(since) = holding_since.take() {
                                waited = Some(hyper_rt::futures::now_ns() - since);
                            }
                            let served = match &serving {
                                Some(
                                    Serving::Reading { exchange: e, .. }
                                    | Serving::Writing { exchange: e, .. }
                                    | Serving::Holding { exchange: e },
                                ) => *e == exchange,
                                None => false,
                            };
                            if served {
                                serving = None;
                            }
                        }
                        Event::Request { exchange, body, .. } => {
                            let head =
                                Head::decode(driver.endpoint().head(exchange).unwrap(), body)
                                    .unwrap();
                            serving = Some(Serving::Reading {
                                exchange,
                                head,
                                fields: Vec::new(),
                                past: 0,
                                hasher: None,
                                trailer: Vec::new(),
                            });
                        }
                        Event::Closed { .. } => return seen,
                        _ => {}
                    }
                }
                if held.is_some_and(|until| hyper_rt::futures::now_ns() < until) {
                    driver.flush().unwrap();
                    continue;
                }
                held = None;
                let endpoint = driver.endpoint();
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
                        let mut pause_now = false;
                        if !bytes.is_empty() {
                            let op = head.op(fields).unwrap();
                            let (length, from) = match op {
                                Op::Put { length, .. } => (length, None),
                                Op::Append { length, offset, .. } => (length, Some(offset)),
                                other => panic!("bytes past the fields of {other:?}"),
                            };
                            let hasher =
                                hasher.get_or_insert_with(|| Hasher::new(TRAILER).unwrap());
                            let object = (length - *past).min(bytes.len() as u64) as usize;
                            // A run's bytes are the object's from the run's offset.
                            if let Some(from) = from {
                                for (j, byte) in bytes[..object].iter().enumerate() {
                                    assert_eq!(*byte, pattern(from + *past + j as u64));
                                }
                            }
                            hasher.update(&bytes[..object]).unwrap();
                            *past += object as u64;
                            trailer.extend_from_slice(&bytes[object..]);
                            pause_now = pause.is_some() && !paused;
                        }
                        endpoint.release(reservation);
                        if pause_now && let Some(pause) = pause {
                            paused = true;
                            let pause = u64::try_from(pause.as_nanos()).unwrap();
                            held = Some(hyper_rt::futures::now_ns() + pause);
                            break;
                        }
                        if got == 0 {
                            break;
                        }
                    }
                    if held.is_some() || !endpoint.body_complete(*exchange) {
                        driver.flush().unwrap();
                        continue;
                    }
                    let op = head.op(fields).unwrap();
                    let exchange = *exchange;
                    if matches!(op, Op::Append { .. })
                        && script.late_append == Some(seen.appends + 1)
                        && late.is_none()
                    {
                        let waited = waited.expect("a late Append follows one its client cut");
                        let until = hyper_rt::futures::now_ns() + waited * 3 / 2;
                        late = Some(until);
                        held = Some(until);
                        driver.flush().unwrap();
                        continue;
                    }
                    let sum = hasher
                        .take()
                        .unwrap_or_else(|| Hasher::new(TRAILER).unwrap())
                        .finish()
                        .unwrap()
                        .bytes;
                    let answer = match op {
                        Op::Put { length, .. } => {
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
                            Some((reply, 0))
                        }
                        Op::Get { .. } => {
                            seen.gets += 1;
                            (script.hold_get != Some(seen.gets)).then(|| {
                                (
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
                                )
                            })
                        }
                        Op::HeadBucket { .. } => Some((Reply::status(Status::Done), 0)),
                        Op::Open { .. } => {
                            seen.opens += 1;
                            let reply = Reply {
                                upload: "upload".into(),
                                progress: runs.progress(None),
                                ..Reply::status(Status::Done)
                            };
                            Some((reply, 0))
                        }
                        Op::Append {
                            offset,
                            length,
                            last,
                            ..
                        } => {
                            seen.appends += 1;
                            if script.lose_append == Some(seen.appends) {
                                endpoint.end(exchange);
                                None
                            } else if script.hold_append == Some(seen.appends) {
                                holding_since = Some(hyper_rt::futures::now_ns());
                                None
                            } else if offset != runs.offset {
                                let reply = Reply {
                                    progress: runs.progress(None),
                                    ..Reply::status(Status::Offset)
                                };
                                Some((reply, 0))
                            } else if sum != *trailer {
                                Some((Reply::status(Status::BadDigest), 0))
                            } else {
                                runs.offset += length;
                                runs.runs += 1;
                                runs.ended = last;
                                let reply = Reply {
                                    progress: runs.progress(Some(sum)),
                                    ..Reply::status(Status::Done)
                                };
                                Some((reply, 0))
                            }
                        }
                        Op::Offset { .. } => {
                            seen.offsets += 1;
                            if script.hold_offset == Some(seen.offsets) {
                                None
                            } else {
                                let reply = Reply {
                                    progress: runs.progress(None),
                                    ..Reply::status(Status::Done)
                                };
                                Some((reply, 0))
                            }
                        }
                        Op::Finish {
                            length, checksum, ..
                        } => {
                            seen.finishes += 1;
                            let whole = runs.ended
                                && length == runs.offset
                                && checksum.bytes == crc(length);
                            let reply = if whole {
                                Reply {
                                    size: length,
                                    etag: "native".into(),
                                    checksum: Some(Stored {
                                        algorithm: checksum.algorithm.code(),
                                        parts: 0,
                                        value: checksum.bytes,
                                    }),
                                    ..Reply::status(Status::Done)
                                }
                            } else {
                                Reply::status(Status::BadDigest)
                            };
                            Some((reply, 0))
                        }
                        other => panic!("the stub does not answer {other:?}"),
                    };
                    // A request lost or held: nothing is written for it.
                    let Some((reply, length)) = answer else {
                        serving = Some(Serving::Holding { exchange });
                        driver.flush().unwrap();
                        continue;
                    };
                    let (reply_head, reply_fields) = reply.encode().unwrap();
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
        .unwrap()
}
