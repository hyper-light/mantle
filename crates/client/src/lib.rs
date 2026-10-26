//! A client of a mantle cell's objects over its native protocol (docs/design/gateway.md §6,
//! node.md §4.1): S3's bucket and object operations asked of a node over QUIC, each request and
//! reply an exchange of hyper-transport's of the object kind (`mantle_protocol::object`). It runs on
//! the current hyper-rt shard, and depends on none of the engine, the ranges or the chunk store.
//!
//! **Its memory does not grow with an object.** A put's body comes from a [`Source`] and a get's
//! bytes go to a [`Sink`], a piece at a time: the client holds the piece it read and has not yet
//! handed over, and its transport no more than its budget and what that budget does not charge, so
//! what it holds while it moves an object is bounded whatever the object's size ([`memory`]). It
//! polls its transport while it waits on a source or a sink, so a slow one stalls no connection.
//!
//! **Its connections.** It knows the cell's nodes by their ids and where they listen, and a node by
//! the certificate the cell's authority issued it for its name (`node-ID`), which the handshake
//! verifies; it presents its own, which the authority issued for the client role (node.md §3.6),
//! and takes no connection it did not dial. A certificate is neither compressed nor asked
//! compressed (RFC 8879 makes compression optional): one is a few hundred bytes, and brotli's
//! compression of one took 34 MB (`mantle_protocol::bounds::CERTIFICATE_COMPRESSION_BYTES`).
//!
//! **What it asks again.** Every mutation carries its operation's identity ([`identity`]), which
//! each attempt carries unchanged (gateway.md §2.1). A request is asked again, after a pause, only
//! when no node can have taken it: its connection refused at the node's admission, or its exchange
//! given no stream (`Refusal::Unsent`), none of its body taken by the transport; at most
//! [`ATTEMPTS`] times. An exchange that ended otherwise without a reply is the caller's:
//! [`ClientError::Lost`] says whether the request was sent whole, after which a mutation may have
//! been applied, and asking it again with its identity is answered from what it made.
//!
//! **What it checks.** The transport checks each body's CRC-32C on the way (node.md §3.2). A put
//! whose op names a trailer states its checksum after its bytes, computed as they are sent, which
//! the node checks before anything commits (`BadDigest` otherwise), and the checksum the node kept
//! is held to it. A get of a whole object whose kept checksum is a full-object value is held to it
//! as its bytes arrive; a composite value, of a multipart object's parts, is no checksum of the
//! bytes and is not checked, as botocore checks none (`httpchecksum.py`).

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::pin;
use std::time::Duration;

use hyper_rt::combine::{Either, Either3, race2, race3};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{Credentials, Tls};
use hyper_transport::{
    Authenticated, Config, Directory, Endpoint, Epoch, Event, ExchangeId, Fixed, PeerId, Progress,
    Refusal,
};
use mantle_protocol::bounds::{self, Stated};
use mantle_protocol::classes::{self, Bounds, Class, Kind, NodeClasses, Role};
use mantle_protocol::object::{self, ObjectError, Op, Reply, ReplyHead};
use mantle_s3::checksum::{Algorithm, Checksum, Hasher};

/// The piece a body is read and written in: the transport's own receive chunk.
pub const PIECE: usize = hyper_transport::RECEIVE_CHUNK;

/// The most times a request is asked when each is refused before any node took it: the AWS SDKs'
/// standard retry mode's default of three attempts (docs/research/73).
pub const ATTEMPTS: u32 = 3;

/// The checksum a put states in its trailer unless its caller names another: CRC-64/NVME, the one
/// S3 keeps for an object uploaded without one, and a full-object value across a multipart
/// upload's parts (docs/research/73).
pub const TRAILER: Algorithm = Algorithm::Crc64Nvme;

/// RFC 9002 §7.6.1's `kPersistentCongestionThreshold`: probe timeouts with no acknowledgement
/// after which a path is taken as failed.
const PERSISTENT_CONGESTION_THRESHOLD: u32 = 3;

/// What a client is given.
pub struct Settings {
    /// Its id, as its certificate names it (`client-ID`).
    pub id: PeerId,
    /// The address its endpoint binds.
    pub bind: SocketAddr,
    /// Its certificate, which the cell's authority issued for the client role, its key, and the
    /// authority's certificate, which a node's chains to.
    pub credentials: Credentials,
    /// The nodes it may ask: each one's id and where its QUIC endpoint listens.
    pub nodes: Vec<(PeerId, SocketAddr)>,
    /// The bytes its transport may hold at once: at least [`least_transport`].
    pub transport_bytes: u64,
}

/// Why an operation was not answered.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("the transport: {0:?}")]
    Transport(Refusal),
    #[error("the runtime: {0}")]
    Runtime(String),
    #[error("the client's bounds: {0}")]
    Bounds(String),
    #[error("an object's message: {0}")]
    Message(#[from] ObjectError),
    #[error("the client knows no node")]
    NoNodes,
    #[error("{0} is not an operation this call asks")]
    Operation(&'static str),
    #[error("the source: {0}")]
    Source(std::io::Error),
    #[error("the sink: {0}")]
    Sink(std::io::Error),
    #[error("the source held {read} bytes or more, not the {stated} stated")]
    Length { stated: u64, read: u64 },
    #[error("the node refused the connection: {0:?}")]
    Refused(Refusal),
    #[error("asked {ATTEMPTS} times, each refused before a node took it, the last {0:?}")]
    Untaken(Refusal),
    #[error(
        "the exchange ended unanswered ({refusal:?}); the request was {}",
        if *whole {
            "sent whole: a mutation may have been applied, and asking it again with its identity \
             is answered from what it made"
        } else {
            "cut short: nothing was applied"
        }
    )]
    Lost { refusal: Refusal, whole: bool },
    #[error("the node's reply does not hold: {0}")]
    Reply(&'static str),
    #[error("{0}'s bytes are not those its checksum names")]
    Corrupt(&'static str),
    #[error("the deadline passed before the operation was answered")]
    Deadline,
}

impl From<Refusal> for ClientError {
    fn from(r: Refusal) -> Self {
        Self::Transport(r)
    }
}

impl From<hyper_rt::RtError> for ClientError {
    fn from(e: hyper_rt::RtError) -> Self {
        Self::Runtime(format!("{e:?}"))
    }
}

/// Where a put's bytes come from.
pub trait Source {
    /// Reads the next bytes into `into`: how many, zero at the source's end. Its future may be
    /// dropped before it is ready, as the client polls its transport meanwhile: one dropped took
    /// nothing from the source.
    fn read(&mut self, into: &mut [u8]) -> impl Future<Output = std::io::Result<usize>>;
}

/// Where a get's bytes go.
pub trait Sink {
    /// Writes all of `bytes`. Its future may be dropped before it is ready, as the client polls its
    /// transport meanwhile: one dropped wrote none of them.
    fn write(&mut self, bytes: &[u8]) -> impl Future<Output = std::io::Result<()>>;
}

/// Bytes in memory, read from the front.
impl Source for &[u8] {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let n = into.len().min(self.len());
        let (now, rest) = self.split_at(n);
        if let Some(into) = into.get_mut(..n) {
            into.copy_from_slice(now);
        }
        *self = rest;
        Ok(n)
    }
}

/// Bytes kept in memory as they come.
impl Sink for Vec<u8> {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.try_reserve(bytes.len())
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        self.extend_from_slice(bytes);
        Ok(())
    }
}

/// No body: the source and the sink of an operation that has neither.
struct Nothing;

impl Source for Nothing {
    async fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

impl Sink for Nothing {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::InvalidData))
        }
    }
}

/// A new operation's identity: 128 bits from the operating system, which every attempt of a
/// mutation carries (gateway.md §2.1).
pub fn identity() -> Result<u128, ClientError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| ClientError::Runtime(e.to_string()))?;
    Ok(u128::from_le_bytes(bytes))
}

/// The classes' bounds a client states: an object's largest message in the transfer class, the
/// one it sends and receives, and nothing in the others.
fn client_bounds() -> Bounds {
    Bounds {
        replication: 0,
        request: 0,
        transfer: object::MAX_MESSAGE,
        bulk: 0,
    }
}

/// States the process's message bounds for a client. A process that stated them already, a
/// node's in a test that runs both, serves the client where its transfer class takes an object's
/// largest message.
fn state_bounds() -> Result<(), ClientError> {
    match classes::state_bounds(client_bounds()) {
        Ok(()) => Ok(()),
        Err(taken) if taken.stated.transfer >= object::MAX_MESSAGE => Ok(()),
        Err(taken) => Err(ClientError::Bounds(taken.to_string())),
    }
}

/// What a client of `nodes` nodes states of itself for its transport's bounds: one object
/// exchange out at a time, no range's appends, no chunks, `transport_bytes` held at once, and an
/// object's messages.
pub fn stated(nodes: usize, transport_bytes: u64) -> Stated {
    Stated {
        peers: nodes,
        ranges: 1,
        inflight: 0,
        transfers: 0,
        transport_bytes,
        messages: client_bounds(),
        max_head: object::MAX_HEAD,
    }
}

/// The least bytes a client of `nodes` nodes' transport may hold (`bounds::least_transport`).
pub fn least_transport(nodes: usize) -> Result<u64, ClientError> {
    bounds::least_transport(&stated(nodes, 0)).map_err(|e| ClientError::Bounds(e.to_string()))
}

/// The sockets a client of `nodes` nodes holds: its endpoint's, and one for each connection its
/// admission takes where the platform gives a connection a socket of its own (hyper-rt-transport's
/// driver); a runtime it runs on takes a read and a write interest for each.
pub fn sockets(nodes: usize) -> Result<usize, ClientError> {
    let limits = bounds::limits(&stated(nodes, least_transport(nodes)?))
        .map_err(|e| ClientError::Bounds(e.to_string()))?;
    limits
        .admission
        .connections
        .checked_add(1)
        .ok_or(ClientError::Bounds("the client's sockets".into()))
}

/// The most heap a client of `stated` holds from its opening while it puts or gets an object,
/// however long: its transport's budget and what the transport holds beside it
/// (`bounds::beside`), a request's head and longest fields, a reply's head, its longest fields
/// and what they decode to, and its piece of a body.
pub fn memory(stated: &Stated) -> Option<u64> {
    let count = |n: usize| u64::try_from(n).ok();
    let messages = count(object::HEAD_BYTES)?
        .checked_add(count(object::MAX_FIELDS)?)?
        .checked_add(count(object::REPLY_HEAD_BYTES)?)?
        .checked_add(count(object::MAX_REPLY_FIELDS)?.checked_mul(2)?)?
        .checked_add(count(PIECE)?)?;
    stated
        .transport_bytes
        .checked_add(bounds::beside(stated, false)?)?
        .checked_add(messages)
}

/// The cell's nodes as the client's directory holds them: each by the name its certificate
/// carries, which a dial checks.
struct Nodes {
    names: Vec<(PeerId, String)>,
}

impl Directory for Nodes {
    type Role = Role;

    /// The node dialed, its certificate verified for its name under the cell's authority at the
    /// handshake; none for a connection the client did not dial.
    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Role)> {
        let dialed = authenticated.dialed?;
        authenticated.certificate?;
        if self.names.iter().any(|(id, _)| *id == dialed) {
            Some((dialed, Role::Node))
        } else {
            None
        }
    }

    fn server_name(&self, peer: PeerId) -> Option<&str> {
        self.names
            .iter()
            .find(|(id, _)| *id == peer)
            .map(|(_, name)| name.as_str())
    }
}

/// A node as the client keeps it.
struct Known {
    id: PeerId,
    quic: SocketAddr,
    connected: bool,
    /// The epoch of the newest connection to it that came up.
    epoch: Epoch,
    /// Exchanges with it in a row cut by their period with no reply from it since: each doubles
    /// the next one's period (RFC 6298 §5.5), and its reply sets it back (§5).
    stalled: u32,
    /// Why its last connection or dial ended, where it said.
    refused: Option<Refusal>,
}

/// What one attempt came to.
enum Attempt {
    Replied(Box<Reply>),
    /// Refused before the node took any of it, `Refusal` why: asked again.
    Untaken(Refusal),
    /// The node's connection refused for a reason asking again does not change.
    Refused(Refusal),
    Lost {
        refusal: Refusal,
        whole: bool,
    },
}

/// A reply as it is read: its head and its body's length, the body's bytes read so far, the reply
/// its fields decode to, and the hasher a get's bytes are held to.
struct Reading {
    head: ReplyHead,
    body: u64,
    got: u64,
    /// The reply its fields decode to, once they are whole.
    decoded: Option<Reply>,
    hasher: Option<(Hasher, &'static str)>,
}

/// A put's body as the client sends it: its source, the bytes stated and those read, the piece
/// read and not yet handed over (`Client::piece[start..end]`), and its trailer's checksum.
struct Upload<'a, S> {
    source: &'a mut S,
    length: u64,
    read: u64,
    start: usize,
    end: usize,
    ended: bool,
    hasher: Option<Hasher>,
    /// The trailer's value once the source ended, and the bytes of it handed over.
    trailer: Option<(Checksum, usize)>,
}

/// A client of a cell's objects, on the current hyper-rt shard.
pub struct Client {
    id: PeerId,
    driver: Driver<NodeClasses, Fixed, Nodes>,
    nodes: Vec<Known>,
    /// The node asked next: its place in `nodes`.
    at: usize,
    /// The slowest exchange measured, from its opening to its reply, nanoseconds.
    slowest_ns: Option<u64>,
    /// Waits paused for so far: the draw each pause's jitter takes (`bounds::pause`).
    waits: u32,
    /// A body's piece, the source's or the reply's: `PIECE` bytes.
    piece: Vec<u8>,
    /// A reply's fields as they arrive.
    fields: Vec<u8>,
    memory: u64,
}

impl Client {
    /// Opens the client: binds its endpoint. Nodes are dialed as operations need them.
    pub async fn open(settings: Settings) -> Result<Self, ClientError> {
        if settings.nodes.is_empty() {
            return Err(ClientError::NoNodes);
        }
        state_bounds()?;
        let stated = stated(settings.nodes.len(), settings.transport_bytes);
        let limits = bounds::limits(&stated).map_err(|e| ClientError::Bounds(e.to_string()))?;
        let memory = memory(&stated).ok_or(ClientError::Bounds("the client's memory".into()))?;
        let mut tls = Tls::mutual(&settings.credentials, false)?;
        tls.client.cert_compressors.clear();
        tls.client.cert_decompressors.clear();
        let config = Config {
            tls,
            role: Role::Client,
            limits,
            congestion: Default::default(),
        };
        let budget = Fixed::new(settings.transport_bytes, stated.peers.max(1));
        let directory = Nodes {
            names: settings
                .nodes
                .iter()
                .map(|(id, _)| (*id, mantle_protocol::node_name(*id)))
                .collect(),
        };
        let endpoint = Endpoint::new(config, budget, directory, std::time::Instant::now())?;
        let driver = Driver::bind(endpoint, settings.bind, bounds::io(&stated))?;
        let mut piece = Vec::new();
        piece
            .try_reserve_exact(PIECE)
            .map_err(|_| ClientError::Bounds("the client's piece".into()))?;
        piece.resize(PIECE, 0);
        Ok(Self {
            id: settings.id,
            driver,
            nodes: settings
                .nodes
                .into_iter()
                .map(|(id, quic)| Known {
                    id,
                    quic,
                    connected: false,
                    epoch: 0,
                    stalled: 0,
                    refused: None,
                })
                .collect(),
            at: 0,
            slowest_ns: None,
            waits: 0,
            piece,
            fields: Vec::new(),
            memory,
        })
    }

    /// The most heap the client holds while it puts or gets an object ([`memory`]).
    pub fn memory(&self) -> u64 {
        self.memory
    }

    /// Asks `op`, an operation that sends no object's bytes and gets none back, by `deadline_ns`
    /// on the shard's clock: a bucket's, an object's facts or its removal, a listing, or a
    /// multipart upload's step that carries no part.
    pub async fn call(&mut self, op: &Op, deadline_ns: u64) -> Result<Reply, ClientError> {
        match op {
            Op::Put { .. } | Op::UploadPart { .. } => Err(ClientError::Operation("a put")),
            Op::Get { .. } => Err(ClientError::Operation("a get")),
            _ => {
                self.ask(op, None::<&mut Nothing>, None::<&mut Nothing>, deadline_ns)
                    .await
            }
        }
    }

    /// Puts `op`'s bytes, a PutObject's or an UploadPart's, `length` of them read from `source`,
    /// by `deadline_ns`; with a trailer named in `op`, its checksum computed as they are sent.
    pub async fn put<S: Source>(
        &mut self,
        op: &Op,
        source: &mut S,
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        match op {
            Op::Put { .. } | Op::UploadPart { .. } => {
                self.ask(op, Some(source), None::<&mut Nothing>, deadline_ns)
                    .await
            }
            _ => Err(ClientError::Operation("not a put")),
        }
    }

    /// Gets `op`'s bytes, a GetObject's, into `sink`, by `deadline_ns`.
    pub async fn get<K: Sink>(
        &mut self,
        op: &Op,
        sink: &mut K,
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        match op {
            Op::Get { .. } => {
                self.ask(op, None::<&mut Nothing>, Some(sink), deadline_ns)
                    .await
            }
            _ => Err(ClientError::Operation("not a get")),
        }
    }

    /// Asks `op` of the nodes in turn, by the module's rules.
    async fn ask<S: Source, K: Sink>(
        &mut self,
        op: &Op,
        source: Option<&mut S>,
        mut sink: Option<&mut K>,
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        let (head, fields) = op.encode()?;
        let body = op.body(&fields);
        let mut upload = match (op, source) {
            (
                Op::Put {
                    length, trailing, ..
                }
                | Op::UploadPart {
                    length, trailing, ..
                },
                Some(source),
            ) => Some(Upload {
                source,
                length: *length,
                read: 0,
                start: 0,
                end: 0,
                ended: false,
                hasher: trailing
                    .map(Hasher::new)
                    .transpose()
                    .map_err(|e| ClientError::Bounds(format!("{e:?}")))?,
                trailer: None,
            }),
            _ => None,
        };
        // RFC 6298 §5.5: the wait doubles from the measured probe timeout.
        let mut backoff = self.measured_pto();
        let mut last = Refusal::NotConnected;
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                let pause = bounds::pause(backoff, self.id, self.waits);
                self.waits = self.waits.wrapping_add(1);
                let wait = u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX);
                self.wait_until(
                    hyper_rt::futures::now_ns().saturating_add(wait),
                    deadline_ns,
                )
                .await?;
                backoff = bounds::backed_off(backoff);
            }
            let node = self.nodes.get(self.at).ok_or(ClientError::NoNodes)?.id;
            let asked = self
                .attempt(
                    node,
                    (&head, &fields, body),
                    (upload.as_mut(), sink.as_deref_mut()),
                    deadline_ns,
                )
                .await?;
            match asked {
                Attempt::Replied(reply) => {
                    let reply = *reply;
                    if let (Some(upload), Some(kept)) = (&upload, &reply.checksum)
                        && let Some((sent, _)) = &upload.trailer
                        && (kept.algorithm != sent.algorithm.code() || kept.value != sent.bytes)
                    {
                        return Err(ClientError::Corrupt("the put"));
                    }
                    return Ok(reply);
                }
                Attempt::Untaken(refusal) => {
                    last = refusal;
                    // The next node is asked: this one may be gone or full.
                    self.at = self
                        .at
                        .checked_add(1)
                        .filter(|at| *at < self.nodes.len())
                        .unwrap_or(0);
                }
                Attempt::Refused(refusal) => return Err(ClientError::Refused(refusal)),
                Attempt::Lost { refusal, whole } => {
                    return Err(ClientError::Lost { refusal, whole });
                }
            }
        }
        Err(ClientError::Untaken(last))
    }

    /// One attempt of a request whose head, fields and body's length are given, with a put's
    /// upload or a get's sink, of `node`.
    async fn attempt<S: Source, K: Sink>(
        &mut self,
        node: PeerId,
        (head, fields, body): (&[u8], &[u8], u64),
        (mut upload, mut sink): (Option<&mut Upload<'_, S>>, Option<&mut K>),
        deadline_ns: u64,
    ) -> Result<Attempt, ClientError> {
        if !self.reach(node, deadline_ns).await? {
            let refused = self
                .nodes
                .iter()
                .find(|n| n.id == node)
                .and_then(|n| n.refused)
                .unwrap_or(Refusal::NotConnected);
            return Ok(if untaken(refused) || refused == Refusal::NotConnected {
                Attempt::Untaken(refused)
            } else {
                Attempt::Refused(refused)
            });
        }
        let clock = self.driver.clock()?;
        let opened_ns = hyper_rt::futures::now_ns();
        let period = Progress::new(Duration::from_nanos(self.period_ns(
            node,
            deadline_ns,
            opened_ns,
        )))?;
        let exchange =
            match self
                .driver
                .endpoint()
                .open(clock, node, Kind::Object, head, Some(body), period)
            {
                Ok(exchange) => exchange,
                Err(Refusal::NotConnected) => {
                    self.lost(node);
                    return Ok(Attempt::Untaken(Refusal::NotConnected));
                }
                Err(refusal) => return Err(refusal.into()),
            };
        // The body's bytes the transport took: the fields', then the source's, then the
        // trailer's; and whether it stopped taking them, the exchange refused, its refusal queued.
        let mut sent = 0u64;
        let mut stopped = false;
        let mut reply: Option<Reading> = None;
        self.fields.clear();
        loop {
            if reply.is_none() && !stopped && sent < body {
                stopped = !self.send(exchange, (fields, &mut upload), &mut sent)?;
            }
            let wants_source = reply.is_none()
                && !stopped
                && sent >= fields_len(fields)
                && upload
                    .as_ref()
                    .is_some_and(|u| u.start == u.end && !u.ended);
            let event = if wants_source && let Some(upload) = upload.as_deref_mut() {
                // The source read raced against the transport, which keeps moving meanwhile.
                let mut piece = std::mem::take(&mut self.piece);
                let raced = {
                    let read = pin!(upload.source.read(&mut piece));
                    let event = pin!(self.driver.event());
                    let left = deadline_ns.saturating_sub(hyper_rt::futures::now_ns());
                    let sleep = pin!(hyper_rt::futures::sleep(left));
                    race3(read, event, sleep).await
                };
                self.piece = piece;
                match raced {
                    Either3::First(read) => {
                        let n = read.map_err(ClientError::Source)?;
                        took_from_source(upload, &self.piece, n)?;
                        continue;
                    }
                    Either3::Second(event) => self.keep(event?),
                    Either3::Third(slept) => {
                        slept?;
                        self.driver.endpoint().end(exchange);
                        return Err(ClientError::Deadline);
                    }
                }
            } else {
                let Some(event) = self.next_event(deadline_ns).await? else {
                    self.driver.endpoint().end(exchange);
                    return Err(ClientError::Deadline);
                };
                event
            };
            match event {
                Event::Writable { exchange: e } if e == exchange => {}
                Event::Reply { exchange: e, body } if e == exchange => {
                    let took = hyper_rt::futures::now_ns().saturating_sub(opened_ns);
                    self.slowest_ns = Some(self.slowest_ns.map_or(took, |s| s.max(took)));
                    if let Some(known) = self.nodes.iter_mut().find(|n| n.id == node) {
                        known.stalled = 0;
                    }
                    let Some(head) = self.driver.endpoint().head(exchange) else {
                        // Ended by the transport after the reply came: the refusal queued behind
                        // the reply ends it.
                        continue;
                    };
                    let head = ReplyHead::decode(head)?;
                    let fields = usize::try_from(head.fields())
                        .ok()
                        .filter(|f| *f <= object::MAX_REPLY_FIELDS)
                        .ok_or(ClientError::Reply("fields past their bound"))?;
                    self.fields
                        .try_reserve(fields)
                        .map_err(|_| ClientError::Reply("fields past the client's memory"))?;
                    let reading = reply.insert(Reading {
                        head,
                        body: body.unwrap_or(0),
                        got: 0,
                        decoded: None,
                        hasher: None,
                    });
                    if let Some(done) = self.read_reply(exchange, reading, &mut sink).await? {
                        return Ok(Attempt::Replied(Box::new(done)));
                    }
                }
                Event::BodyReady { exchange: e } if e == exchange => {
                    if let Some(reading) = reply.as_mut()
                        && let Some(done) = self.read_reply(exchange, reading, &mut sink).await?
                    {
                        return Ok(Attempt::Replied(Box::new(done)));
                    }
                }
                // Its connection closed: the transport queues why with the exchange's refusal,
                // which says whether the node took the request at all.
                Event::Refused {
                    exchange: e,
                    refusal,
                    ..
                } if e == exchange => {
                    let mut why = refusal;
                    if refusal == Refusal::Closed {
                        while let Some(event) = self.driver.endpoint().poll_event() {
                            if let Event::Closed { peer, refusal, .. }
                            | Event::Unreachable { peer, refusal } = self.keep(event)
                                && peer == node
                                && let Some(refusal) = refusal
                            {
                                why = refusal;
                            }
                        }
                    }
                    if refusal == Refusal::Stalled
                        && let Some(known) = self.nodes.iter_mut().find(|n| n.id == node)
                    {
                        known.stalled = known.stalled.saturating_add(1);
                    }
                    return Ok(ended(why, sent, (fields_len(fields), body)));
                }
                Event::Closed { peer, refusal, .. } | Event::Unreachable { peer, refusal }
                    if peer == node =>
                {
                    self.lost(peer);
                    let why = refusal.unwrap_or(Refusal::Closed);
                    return Ok(ended(why, sent, (fields_len(fields), body)));
                }
                _ => {}
            }
        }
    }

    /// Hands the transport what it takes of the request's body past `sent`, counting it there: the
    /// fields, the piece the upload holds, and, once the source ended, the trailer. False once the
    /// transport refused the exchange, whose refusal it queued.
    fn send<S: Source>(
        &mut self,
        exchange: ExchangeId,
        (fields, upload): (&[u8], &mut Option<&mut Upload<'_, S>>),
        sent: &mut u64,
    ) -> Result<bool, ClientError> {
        let endpoint = self.driver.endpoint();
        let mut at = usize::try_from(*sent).unwrap_or(usize::MAX);
        while let Some(rest) = fields.get(at..)
            && !rest.is_empty()
        {
            match write(endpoint, exchange, rest, sent)? {
                Some(0) => return Ok(true),
                Some(took) => at = at.saturating_add(took),
                None => return Ok(false),
            }
        }
        let Some(upload) = upload.as_deref_mut() else {
            return Ok(true);
        };
        while let Some(held) = self.piece.get(upload.start..upload.end)
            && !held.is_empty()
        {
            match write(endpoint, exchange, held, sent)? {
                Some(0) => return Ok(true),
                Some(took) => upload.start = upload.start.saturating_add(took),
                None => return Ok(false),
            }
        }
        if !upload.ended {
            return Ok(true);
        }
        if upload.read != upload.length {
            endpoint.end(exchange);
            return Err(ClientError::Length {
                stated: upload.length,
                read: upload.read,
            });
        }
        if upload.trailer.is_none()
            && let Some(hasher) = upload.hasher.take()
        {
            let checksum = hasher
                .finish()
                .map_err(|e| ClientError::Bounds(format!("{e:?}")))?;
            upload.trailer = Some((checksum, 0));
        }
        if let Some((checksum, done)) = &mut upload.trailer {
            while let Some(rest) = checksum.bytes.get(*done..)
                && !rest.is_empty()
            {
                match write(endpoint, exchange, rest, sent)? {
                    Some(0) => return Ok(true),
                    Some(took) => *done = done.saturating_add(took),
                    None => return Ok(false),
                }
            }
        }
        Ok(true)
    }

    /// Reads what the reply's body holds: its fields, decoded once whole, then a get's bytes into
    /// the sink, each piece written before the next is read. Once the body is whole and its
    /// checksum verified, the reply, a get's bytes held to the checksum kept.
    async fn read_reply<K: Sink>(
        &mut self,
        exchange: ExchangeId,
        reading: &mut Reading,
        sink: &mut Option<&mut K>,
    ) -> Result<Option<Reply>, ClientError> {
        let fields = usize::try_from(reading.head.fields()).unwrap_or(usize::MAX);
        loop {
            let mut piece = self
                .driver
                .endpoint()
                .reserve(Class::Transfer, u64::try_from(PIECE).unwrap_or(u64::MAX))?;
            let read = self.driver.endpoint().read_body(exchange, &mut piece);
            let poured = self
                .take_piece(exchange, reading, piece.bytes(), (fields, sink))
                .await;
            self.driver.endpoint().release(piece);
            poured?;
            if read? == 0 {
                break;
            }
        }
        if !self.driver.endpoint().body_complete(exchange) {
            return Ok(None);
        }
        self.driver.endpoint().end(exchange);
        if reading.got != reading.body {
            return Err(ClientError::Reply(
                "a body of another length than its head's",
            ));
        }
        let decoded = match reading.decoded.take() {
            Some(decoded) => decoded,
            None => reading.head.reply(&self.fields)?,
        };
        if let Some((hasher, what)) = reading.hasher.take()
            && let Some(kept) = &decoded.checksum
        {
            let sum = hasher
                .finish()
                .map_err(|e| ClientError::Bounds(format!("{e:?}")))?;
            if sum.bytes != kept.value {
                return Err(ClientError::Corrupt(what));
            }
        }
        Ok(Some(decoded))
    }

    /// Takes a piece of a reply's body: its front to the fields, which are decoded once whole, and
    /// the rest, a get's bytes, hashed and poured into the sink.
    async fn take_piece<K: Sink>(
        &mut self,
        exchange: ExchangeId,
        reading: &mut Reading,
        bytes: &[u8],
        (fields, sink): (usize, &mut Option<&mut K>),
    ) -> Result<(), ClientError> {
        let into_fields = fields.saturating_sub(self.fields.len()).min(bytes.len());
        let (front, rest) = bytes.split_at(into_fields);
        self.fields.extend_from_slice(front);
        reading.got = reading
            .got
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if reading.decoded.is_none() && self.fields.len() == fields {
            let decoded = reading.head.reply(&self.fields)?;
            // A get's bytes, which its sink takes, are held to the checksum kept.
            if sink.is_some() {
                reading.hasher = hasher_for(&decoded)?;
            }
            reading.decoded = Some(decoded);
        }
        if rest.is_empty() {
            return Ok(());
        }
        if let Some((hasher, _)) = reading.hasher.as_mut() {
            hasher
                .update(rest)
                .map_err(|e| ClientError::Bounds(format!("{e:?}")))?;
        }
        let Some(sink) = sink.as_deref_mut() else {
            self.driver.endpoint().end(exchange);
            return Err(ClientError::Reply("bytes after an answer that has none"));
        };
        // The sink's write raced against the transport, which keeps moving meanwhile; a write the
        // transport overtook took nothing and is made again.
        loop {
            let raced = {
                let write = pin!(sink.write(rest));
                let event = pin!(self.driver.event());
                race2(write, event).await
            };
            match raced {
                Either::First(wrote) => return wrote.map_err(ClientError::Sink),
                Either::Second(event) => {
                    self.keep(event?);
                }
            }
        }
    }

    /// Dials `node` if it has no connection and waits for it, as long as the client gives an
    /// exchange with it: whether it was reached.
    async fn reach(&mut self, node: PeerId, deadline_ns: u64) -> Result<bool, ClientError> {
        let Some(known) = self.nodes.iter().find(|n| n.id == node) else {
            return Ok(false);
        };
        if known.connected {
            return Ok(true);
        }
        let (quic, before) = (known.quic, known.epoch);
        let clock = self.driver.clock()?;
        if self.driver.endpoint().connect(clock, node, quic).is_err() {
            return Ok(false);
        }
        let now_ns = hyper_rt::futures::now_ns();
        let until_ns = now_ns.saturating_add(self.period_ns(node, deadline_ns, now_ns));
        loop {
            match self.next_event(until_ns).await? {
                Some(Event::Connected { peer, .. }) if peer == node => return Ok(true),
                Some(Event::Unreachable { peer, .. }) if peer == node => return Ok(false),
                Some(Event::Closed { peer, epoch, .. }) if peer == node && epoch > before => {
                    return Ok(false);
                }
                Some(_) => {}
                None if until_ns >= deadline_ns => return Err(ClientError::Deadline),
                None => return Ok(false),
            }
        }
    }

    /// The probe timeout of the slowest path the client has measured, the current retransmission
    /// timeout its waits double from (RFC 6298 §5.5); before any is measured, the one before any
    /// sample (RFC 9002 §6.2.2).
    fn measured_pto(&mut self) -> Duration {
        let mut most: Option<Duration> = None;
        for at in 0..self.nodes.len() {
            let path = self
                .nodes
                .get(at)
                .and_then(|n| self.driver.endpoint().path(n.id));
            if let Some(path) = path {
                let pto = bounds::measured_pto(&path);
                most = Some(most.map_or(pto, |m| m.max(pto)));
            }
        }
        most.unwrap_or_else(bounds::initial_pto)
    }

    /// The progress period an exchange with `node` is given (mantle-node's client's law): the
    /// time left before `deadline_ns` while nothing is measured, else the slowest exchange measured
    /// or the path's persistent-congestion duration (RFC 9002 §7.6.1), whichever is longer,
    /// doubled for each exchange with the node cut by its period since its last reply, within that
    /// time.
    fn period_ns(&mut self, node: PeerId, deadline_ns: u64, now_ns: u64) -> u64 {
        let left = deadline_ns.saturating_sub(now_ns).max(1);
        let Some(slowest) = self.slowest_ns else {
            return left;
        };
        let congested = self.driver.endpoint().path(node).map_or(0, |path| {
            u64::try_from(
                bounds::measured_pto(&path)
                    .saturating_mul(PERSISTENT_CONGESTION_THRESHOLD)
                    .as_nanos(),
            )
            .unwrap_or(u64::MAX)
        });
        let stalled = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .map_or(0, |n| n.stalled);
        let doubling = 1u64.checked_shl(stalled).unwrap_or(u64::MAX);
        slowest.max(congested).saturating_mul(doubling).min(left)
    }

    fn lost(&mut self, peer: PeerId) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == peer) {
            node.connected = false;
        }
    }

    /// The transport's next event, the connections it reports kept; `None` once `deadline_ns`
    /// has passed.
    async fn next_event(
        &mut self,
        deadline_ns: u64,
    ) -> Result<Option<Event<NodeClasses>>, ClientError> {
        if let Some(event) = self.driver.endpoint().poll_event() {
            return Ok(Some(self.keep(event)));
        }
        let left = deadline_ns.saturating_sub(hyper_rt::futures::now_ns());
        if left == 0 {
            return Ok(None);
        }
        let ended = {
            let event = pin!(self.driver.event());
            let sleep = pin!(hyper_rt::futures::sleep(left));
            race2(event, sleep).await
        };
        match ended {
            Either::First(event) => Ok(Some(self.keep(event?))),
            Either::Second(slept) => {
                slept?;
                Ok(None)
            }
        }
    }

    /// Keeps what a connection's event says of it.
    fn keep(&mut self, event: Event<NodeClasses>) -> Event<NodeClasses> {
        match &event {
            Event::Connected { peer, epoch, .. } => {
                if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *peer) {
                    node.connected = true;
                    node.epoch = *epoch;
                    node.refused = None;
                }
            }
            Event::Closed { peer, refusal, .. } | Event::Unreachable { peer, refusal } => {
                let peer = *peer;
                if let Some(node) = self.nodes.iter_mut().find(|n| n.id == peer) {
                    node.refused = *refusal;
                }
                self.lost(peer);
            }
            _ => {}
        }
        event
    }

    /// Waits until `until_ns`, the transport's events taken meanwhile; refused past
    /// `deadline_ns`.
    async fn wait_until(&mut self, until_ns: u64, deadline_ns: u64) -> Result<(), ClientError> {
        if until_ns >= deadline_ns {
            return Err(ClientError::Deadline);
        }
        while hyper_rt::futures::now_ns() < until_ns {
            if self.next_event(until_ns).await?.is_none() {
                break;
            }
        }
        Ok(())
    }
}

impl Drop for Client {
    /// The client's connections close as it goes, each node told by QUIC's CONNECTION_CLOSE, so a
    /// node frees what they held at once rather than after their idle timeout (mantle-node's
    /// client's reasons). Best effort: a clock the shard cannot read, or a send the OS refuses,
    /// leaves them to the idle timeout.
    fn drop(&mut self) {
        let Ok(clock) = self.driver.clock() else {
            return;
        };
        for at in 0..self.nodes.len() {
            if let Some(id) = self.nodes.get(at).map(|n| n.id) {
                self.driver.endpoint().disconnect(clock, id);
            }
        }
        let _ = self.driver.flush();
    }
}

/// Writes what `exchange` takes of `bytes`, counting it in `sent`: how many, or none once the
/// transport refused the exchange, which queued its refusal.
fn write(
    endpoint: &mut Endpoint<NodeClasses, Fixed, Nodes>,
    exchange: ExchangeId,
    bytes: &[u8],
    sent: &mut u64,
) -> Result<Option<usize>, ClientError> {
    match endpoint.write_body(exchange, bytes) {
        Ok(took) => {
            *sent = sent.saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
            Ok(Some(took))
        }
        // Past the body's stated length, or out of turn: the client's own error.
        Err(Refusal::Order) => Err(ClientError::Transport(Refusal::Order)),
        Err(_) => Ok(None),
    }
}

/// The hasher a get's bytes are held to: the kept checksum's algorithm where `reply` carries a
/// full-object value of the whole object's bytes; none for a range, a composite value or none
/// kept.
fn hasher_for(reply: &Reply) -> Result<Option<(Hasher, &'static str)>, ClientError> {
    let Some(kept) = reply.checksum.as_ref().filter(|c| c.parts == 0) else {
        return Ok(None);
    };
    if reply.range.is_some() {
        return Ok(None);
    }
    let algorithm =
        Algorithm::from_code(kept.algorithm).ok_or(ClientError::Reply("an unknown checksum"))?;
    let hasher = Hasher::new(algorithm).map_err(|e| ClientError::Bounds(format!("{e:?}")))?;
    Ok(Some((hasher, "the get")))
}

/// The bytes of a request's fields.
fn fields_len(fields: &[u8]) -> u64 {
    u64::try_from(fields.len()).unwrap_or(u64::MAX)
}

/// Keeps `n` bytes the source read into the client's piece: hashed for the trailer, held until
/// the transport takes them; refused past the length stated.
fn took_from_source<S>(
    upload: &mut Upload<'_, S>,
    piece: &[u8],
    n: usize,
) -> Result<(), ClientError> {
    if n == 0 {
        upload.ended = true;
        return Ok(());
    }
    let read = piece
        .get(..n)
        .ok_or_else(|| ClientError::Bounds("a read past the piece".into()))?;
    upload.read = upload
        .read
        .saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
    if upload.read > upload.length {
        return Err(ClientError::Length {
            stated: upload.length,
            read: upload.read,
        });
    }
    if let Some(hasher) = upload.hasher.as_mut() {
        hasher
            .update(read)
            .map_err(|e| ClientError::Bounds(format!("{e:?}")))?;
    }
    upload.start = 0;
    upload.end = n;
    Ok(())
}

/// What an exchange that ended unanswered with `refusal` came to, `sent` of its body's bytes taken
/// by the transport: asked again where no node can have taken it, none of the source's bytes
/// handed over; else lost, whole once every byte was.
fn ended(refusal: Refusal, sent: u64, (fields, body): (u64, u64)) -> Attempt {
    if untaken(refusal) && sent <= fields {
        Attempt::Untaken(refusal)
    } else {
        Attempt::Lost {
            refusal,
            whole: sent >= body,
        }
    }
}

/// Whether a request lost with `refusal` was never taken by its node: its connection refused at
/// the node's admission for the node's capacity, which comes before any stream on it is taken
/// (hyper-transport's `Endpoint`), a client past the node's bound among them
/// (`Refusal::Identities`), or its exchange given no stream (`Refusal::Unsent`).
fn untaken(refusal: Refusal) -> bool {
    matches!(
        refusal,
        Refusal::Pending
            | Refusal::Identities
            | Refusal::Connections
            | Refusal::Busy
            | Refusal::Events
            | Refusal::Unsent
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_is_asked_again_only_where_no_node_took_any_of_it() {
        for refusal in [
            Refusal::Pending,
            Refusal::Identities,
            Refusal::Connections,
            Refusal::Busy,
            Refusal::Events,
            Refusal::Unsent,
        ] {
            assert!(matches!(ended(refusal, 0, (10, 100)), Attempt::Untaken(_)));
            assert!(matches!(ended(refusal, 10, (10, 100)), Attempt::Untaken(_)));
            assert!(matches!(
                ended(refusal, 11, (10, 100)),
                Attempt::Lost { whole: false, .. }
            ));
        }
        for refusal in [Refusal::Closed, Refusal::Stalled, Refusal::Identity] {
            assert!(matches!(
                ended(refusal, 0, (10, 100)),
                Attempt::Lost { whole: false, .. }
            ));
            assert!(matches!(
                ended(refusal, 100, (10, 100)),
                Attempt::Lost { whole: true, .. }
            ));
        }
    }

    #[test]
    fn a_source_past_its_stated_length_is_refused() {
        let mut bytes: &[u8] = &[1, 2, 3];
        let mut upload = Upload {
            source: &mut bytes,
            length: 2,
            read: 0,
            start: 0,
            end: 0,
            ended: false,
            hasher: None,
            trailer: None,
        };
        let piece = [9u8; 4];
        took_from_source(&mut upload, &piece, 2).unwrap();
        assert_eq!((upload.start, upload.end, upload.read), (0, 2, 2));
        assert!(matches!(
            took_from_source(&mut upload, &piece, 1),
            Err(ClientError::Length { stated: 2, read: 3 })
        ));
        took_from_source(&mut upload, &piece, 0).unwrap();
        assert!(upload.ended);
    }
}
