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
//! each attempt carries unchanged (gateway.md §2.1). A request is asked again, of the next node,
//! after a pause, only when no node can have taken it: its node not reached, its connection refused
//! at the node's admission, or its exchange given no stream (`Refusal::Unsent`), none of its body
//! taken by the transport; at most [`ATTEMPTS`] times. A client whose certificate a node refused, or refused at admission once its
//! body's bytes were handed over, is told so ([`ClientError::Refused`]): nothing was applied. An
//! exchange that ended otherwise without a reply is the caller's: [`ClientError::Lost`] says
//! whether the request was sent whole, after which a mutation may have been applied, and asking it
//! again with its identity is answered from what it made.
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
use hyper_transport::tls::Tls;
use hyper_transport::{
    Authenticated, Config, Directory, Endpoint, Epoch, Event, ExchangeId, Fixed, Progress,
};
use mantle_protocol::bounds::{self, Stated};
use mantle_protocol::classes::{self, Bounds, Class, Kind, NodeClasses, Role};
use mantle_protocol::object::{self, Headers, ObjectError, Op, Reply, ReplyHead, Status};
use mantle_s3::body::MIN_PART;
use mantle_s3::checksum::{Algorithm, Checksum, Hasher};

/// What [`Settings`] and the client's answers name, so a caller needs none of the transport's own.
pub use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
pub use hyper_transport::{PeerId, Refusal};

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
    #[error("the node refused the client, and took nothing: {0:?}")]
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
    #[error("the upload's durable offset is {theirs}, not one the client's runs reach ({ours})")]
    Offset { ours: u64, theirs: u64 },
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

/// A source a native upload reads again from an offset, after an exchange that carried a run
/// ended unanswered: a file, say.
pub trait Rewind: Source {
    /// Positions the source to read from `offset` next.
    fn rewind(&mut self, offset: u64) -> impl Future<Output = std::io::Result<()>>;
}

/// What a native upload tells its caller as it goes (gateway.md §6): what a journal keeps.
pub trait Watch {
    /// The upload was opened, before any run is sent: its ID, which resuming it after a restart
    /// needs.
    fn opened(&mut self, upload: &str) -> impl Future<Output = std::io::Result<()>>;
    /// Where the upload stands: its progress once a run committed or once read on resuming.
    fn progressed(
        &mut self,
        progress: &object::Progress,
    ) -> impl Future<Output = std::io::Result<()>>;
}

/// A native upload's object (gateway.md §2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Native {
    pub bucket: String,
    pub key: String,
    /// The operation's identity, which its Open, every run and its Finish carry.
    pub id: u128,
    /// An upload to resume from the node's offset, as a journal kept it; `None` opens one.
    pub upload: Option<String>,
    /// Its full-object checksum's algorithm: a CRC, which combines from its runs'.
    pub checksum: Algorithm,
    /// The preconditions its object commits under.
    pub headers: Headers,
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

impl<'a, S> Upload<'a, S> {
    /// `length` bytes of `source`, with a trailer of `trailing`'s checksum where it names one.
    fn new(
        source: &'a mut S,
        length: u64,
        trailing: Option<Algorithm>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            source,
            length,
            read: 0,
            start: 0,
            end: 0,
            ended: false,
            hasher: trailing
                .map(Hasher::new)
                .transpose()
                .map_err(|e| ClientError::Bounds(format!("{e:?}")))?,
            trailer: None,
        })
    }

    /// The trailer's checksum, once every byte was read and it was computed.
    fn sent(&self) -> Option<&Checksum> {
        self.trailer.as_ref().map(|(checksum, _)| checksum)
    }
}

/// A client of a cell's objects, on the current hyper-rt shard.
pub struct Client {
    id: PeerId,
    driver: Driver<NodeClasses, Fixed, Nodes>,
    nodes: Vec<Known>,
    /// The node asked next: its place in `nodes`.
    at: usize,
    /// The slowest exchange measured, from its opening to its reply, or twice the longest a node
    /// held a request whole unanswered before the client cut the exchange, nanoseconds.
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

    /// Streams `length` bytes of `source` into `native`'s object as a native upload
    /// (gateway.md §2.1), resumable: runs of the length the node cuts, each its own exchange
    /// carrying its checksum in a trailer and answered with its progress once it committed. An
    /// exchange that ends unanswered is resumed from the node's durable offset, the source read
    /// again from there, so no committed run is sent again; at most [`ATTEMPTS`] times in a row
    /// without a run committing. The object commits at the Finish, held to the CRC of every byte,
    /// which the client computes as it reads them: resuming an upload a journal kept, the bytes
    /// below the node's offset are read and hashed and not sent, so a source changed since the
    /// first attempt ends in `BadDigest` and nothing commits. An object one run holds is put
    /// whole, which a run would be. The reply is the Finish's, or a refusal's.
    pub async fn upload<S: Rewind, W: Watch>(
        &mut self,
        native: &Native,
        (source, length): (&mut S, u64),
        watch: &mut W,
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        if native.upload.is_none() && length <= MIN_PART {
            return self.put_whole(native, source, length, deadline_ns).await;
        }
        let (upload, mut at) = match self
            .begin(native, (Some(length), None), watch, deadline_ns)
            .await?
        {
            Ok(begun) => begun,
            Err(refused) => return Ok(*refused),
        };
        let mut whole = Whole::new(native.checksum);
        source.rewind(0).await.map_err(ClientError::Source)?;
        self.hash_prefix(source, at.offset, &mut whole).await?;
        let mut failures = 0u32;
        while !at.ended {
            let run = length
                .checked_sub(at.offset)
                .ok_or(ClientError::Offset {
                    ours: length,
                    theirs: at.offset,
                })?
                .min(at.run);
            let last = at.offset.checked_add(run) == Some(length);
            source
                .rewind(at.offset)
                .await
                .map_err(ClientError::Source)?;
            let op = append(native, &upload, (at.offset, run, last));
            let mut bounded = Bounded {
                source: &mut *source,
                left: run,
            };
            let mut body = Upload::new(&mut bounded, run, Some(native.checksum))?;
            let asked = self
                .asked(&op, Some(&mut body), None::<&mut Nothing>, deadline_ns)
                .await;
            let sent = body.sent().cloned();
            match self
                .ran(native, &upload, &mut at, (run, sent), asked, deadline_ns)
                .await?
            {
                Ran::Committed(crc) => {
                    whole.add(crc, run)?;
                    failures = 0;
                    watch.progressed(&at).await.map_err(ClientError::Source)?;
                }
                Ran::Again(why) => failures = self.again(failures, why, deadline_ns).await?,
                Ran::Refused(reply) => return Ok(*reply),
            }
        }
        self.finish(native, &upload, (at.offset, whole), deadline_ns)
            .await
    }

    /// As [`upload`](Self::upload), from a `source` read once, standard input say, of `length`
    /// bytes where the caller knows them: the client holds each run, at most `buffer` bytes, until
    /// the run commits, and sends it again from there where an exchange ends unanswered. With its
    /// length known, a run is sent as it is read, each byte kept as it goes; without, a run is read
    /// whole first, with a piece past it to find the source's end, since a run states whether it is
    /// the last before its bytes. Resuming an upload a journal kept, the source is read from its
    /// start again and the bytes below the node's offset hashed and dropped.
    pub async fn upload_once<S: Source, W: Watch>(
        &mut self,
        native: &Native,
        (source, length): (&mut S, Option<u64>),
        (buffer, watch): (u64, &mut W),
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        let mut held = Held::new(buffer)?;
        if native.upload.is_none() && length.is_none_or(|l| l <= MIN_PART) {
            // An object one run holds is put whole: read up to the least run and a piece past
            // it, and if the source ends within, it is the object.
            let first = length.map_or(MIN_PART, |l| l.min(MIN_PART));
            held.fill(source, first, length.is_none()).await?;
            if held.ended || length.is_some_and(|l| l <= MIN_PART) {
                let bytes = held.bytes.len();
                let mut whole = &held.bytes[..];
                let length = u64::try_from(bytes).unwrap_or(u64::MAX);
                return self
                    .put_whole(native, &mut whole, length, deadline_ns)
                    .await;
            }
        }
        let (upload, mut at) = match self
            .begin(native, (length, Some(buffer)), watch, deadline_ns)
            .await?
        {
            Ok(begun) => begun,
            Err(refused) => return Ok(*refused),
        };
        let mut whole = Whole::new(native.checksum);
        if native.upload.is_some() {
            self.hash_prefix(source, at.offset, &mut whole).await?;
        }
        let mut failures = 0u32;
        while !at.ended {
            let left = length.map(|l| l.checked_sub(at.offset));
            let run = match left {
                Some(None) => {
                    return Err(ClientError::Offset {
                        ours: length.unwrap_or(0),
                        theirs: at.offset,
                    });
                }
                Some(Some(left)) => left.min(at.run),
                None => at.run,
            };
            let (taken, asked, sent) = match length {
                // Its length known, the run is sent as it is read, each byte kept as it goes
                // and given again first where it is sent again.
                Some(l) => {
                    let last = at.offset.checked_add(run) == Some(l);
                    let op = append(native, &upload, (at.offset, run, last));
                    let mut replay = Replay {
                        held: &mut held.bytes,
                        at: 0,
                        source: &mut *source,
                        run: usize::try_from(run)
                            .map_err(|_| ClientError::Bounds("a run past memory".into()))?,
                    };
                    let mut body = Upload::new(&mut replay, run, Some(native.checksum))?;
                    let asked = self
                        .asked(&op, Some(&mut body), None::<&mut Nothing>, deadline_ns)
                        .await;
                    (run, asked, body.sent().cloned())
                }
                // A run is the last where the source ends within it: a piece past it is read
                // ahead to know.
                None => {
                    held.fill(source, run, true).await?;
                    let taken = held.run(run)?;
                    let last = held.ended && held.ahead() == 0;
                    let op = append(native, &upload, (at.offset, taken, last));
                    let mut bytes = held.bytes.get(..held.len(taken)?).unwrap_or_default();
                    let mut body = Upload::new(&mut bytes, taken, Some(native.checksum))?;
                    let asked = self
                        .asked(&op, Some(&mut body), None::<&mut Nothing>, deadline_ns)
                        .await;
                    (taken, asked, body.sent().cloned())
                }
            };
            match self
                .ran(native, &upload, &mut at, (taken, sent), asked, deadline_ns)
                .await?
            {
                Ran::Committed(crc) => {
                    whole.add(crc, taken)?;
                    held.drop_run(taken)?;
                    failures = 0;
                    watch.progressed(&at).await.map_err(ClientError::Source)?;
                }
                Ran::Again(why) => failures = self.again(failures, why, deadline_ns).await?,
                Ran::Refused(reply) => return Ok(*reply),
            }
        }
        self.finish(native, &upload, (at.offset, whole), deadline_ns)
            .await
    }

    /// An object a single run would hold, put whole with its operation's identity and its
    /// checksum in a trailer: a native upload's object, the same bytes, ETag and checksum.
    async fn put_whole<S: Source>(
        &mut self,
        native: &Native,
        source: &mut S,
        length: u64,
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        let op = Op::Put {
            bucket: native.bucket.clone(),
            key: native.key.clone(),
            id: native.id,
            length,
            headers: native.headers.clone(),
            checksum: None,
            md5: None,
            trailing: Some(native.checksum),
        };
        self.put(&op, source, deadline_ns).await
    }

    /// A native upload opened, or the one `native` names resumed: its ID and progress, told to
    /// `watch`; or the refusal the node answered.
    async fn begin<W: Watch>(
        &mut self,
        native: &Native,
        (length, buffer): (Option<u64>, Option<u64>),
        watch: &mut W,
        deadline_ns: u64,
    ) -> Result<Result<(String, object::Progress), Box<Reply>>, ClientError> {
        let (upload, reply) = match &native.upload {
            Some(upload) => (
                upload.clone(),
                self.progress(native, upload, deadline_ns).await?,
            ),
            None => {
                let open = Op::Open {
                    bucket: native.bucket.clone(),
                    key: native.key.clone(),
                    id: native.id,
                    length,
                    buffer,
                    checksum: native.checksum,
                };
                let reply = self.call(&open, deadline_ns).await?;
                if reply.status == Status::Done {
                    watch
                        .opened(&reply.upload)
                        .await
                        .map_err(ClientError::Source)?;
                }
                (reply.upload.clone(), reply)
            }
        };
        if reply.status != Status::Done {
            return Ok(Err(Box::new(reply)));
        }
        let at = reply
            .progress
            .map(|p| *p)
            .ok_or(ClientError::Reply("a native upload's progress missing"))?;
        watch.progressed(&at).await.map_err(ClientError::Source)?;
        Ok(Ok((upload, at)))
    }

    /// A native upload's progress, as its Offset answers it. A read, it is asked again where its
    /// answer was lost, at most [`ATTEMPTS`] times in a row: a node whose range lost its leader
    /// with the node that died under a run holds the read until another leads, which may outlast
    /// the client's period, and answers the next ask.
    async fn progress(
        &mut self,
        native: &Native,
        upload: &str,
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        let op = Op::Offset {
            bucket: native.bucket.clone(),
            key: native.key.clone(),
            upload: upload.to_owned(),
        };
        let mut failures = 0u32;
        loop {
            match self.call(&op, deadline_ns).await {
                Err(error) if resumable(&error) => {
                    failures = self.again(failures, error, deadline_ns).await?;
                }
                asked => return asked,
            }
        }
    }

    /// Reads `len` bytes of `source` into `whole` and sends none: the committed runs' bytes, read
    /// again where an upload is resumed so its checksum is of the source as it is now.
    async fn hash_prefix<S: Source>(
        &mut self,
        source: &mut S,
        len: u64,
        whole: &mut Whole,
    ) -> Result<(), ClientError> {
        let mut hasher =
            Hasher::new(whole.algorithm).map_err(|e| ClientError::Bounds(format!("{e:?}")))?;
        let mut left = len;
        while left > 0 {
            let want = usize::try_from(left)
                .unwrap_or(usize::MAX)
                .min(self.piece.len());
            let piece = self
                .piece
                .get_mut(..want)
                .ok_or_else(|| ClientError::Bounds("a read past the piece".into()))?;
            let n = source.read(piece).await.map_err(ClientError::Source)?;
            if n == 0 {
                return Err(ClientError::Length {
                    stated: len,
                    read: len.saturating_sub(left),
                });
            }
            let read = piece
                .get(..n)
                .ok_or_else(|| ClientError::Bounds("a read past the piece".into()))?;
            hasher
                .update(read)
                .map_err(|e| ClientError::Bounds(format!("{e:?}")))?;
            left = left.saturating_sub(u64::try_from(n).unwrap_or(u64::MAX));
        }
        if len > 0 {
            let checksum = hasher
                .finish()
                .map_err(|e| ClientError::Bounds(format!("{e:?}")))?;
            whole.add(checksum, len)?;
        }
        Ok(())
    }

    /// What a run's exchange came to: committed, its checksum the trailer the client sent; to be
    /// sent again from the node's offset, where it ended unanswered and the node holds the offset
    /// the run began at; committed after all, the node's offset past it, its reply lost; or a
    /// refusal for the caller. `at` moves to the node's progress.
    async fn ran(
        &mut self,
        native: &Native,
        upload: &str,
        at: &mut object::Progress,
        (run, sent): (u64, Option<Checksum>),
        asked: Result<Reply, ClientError>,
        deadline_ns: u64,
    ) -> Result<Ran, ClientError> {
        let begun = at.offset;
        let end = begun.checked_add(run).ok_or(ClientError::Offset {
            ours: begun,
            theirs: begun,
        })?;
        let (theirs, why) = match asked {
            Ok(reply) if reply.status == Status::Done => (
                reply
                    .progress
                    .map(|p| *p)
                    .ok_or(ClientError::Reply("a run's reply without its progress"))?,
                None,
            ),
            // Another run committed at the offset first, or the upload ended: where it stands.
            Ok(reply) if reply.status == Status::Offset => (
                reply
                    .progress
                    .map(|p| *p)
                    .ok_or(ClientError::Reply("an offset refused without the upload's"))?,
                None,
            ),
            Ok(reply) => return Ok(Ran::Refused(Box::new(reply))),
            Err(error) if resumable(&error) => {
                let reply = self.progress(native, upload, deadline_ns).await?;
                if reply.status != Status::Done {
                    return Ok(Ran::Refused(Box::new(reply)));
                }
                let theirs = reply
                    .progress
                    .map(|p| *p)
                    .ok_or(ClientError::Reply("a native upload's progress missing"))?;
                (theirs, Some(error))
            }
            Err(error) => return Err(error),
        };
        if theirs.offset == begun && !theirs.ended {
            *at = theirs;
            let why = why.unwrap_or(ClientError::Offset {
                ours: begun,
                theirs: begun,
            });
            return Ok(Ran::Again(why));
        }
        // The run committed: the node holds what it checked against the trailer sent.
        let Some(crc) = sent.filter(|_| theirs.offset == end) else {
            return Err(ClientError::Offset {
                ours: begun,
                theirs: theirs.offset,
            });
        };
        *at = theirs;
        Ok(Ran::Committed(crc))
    }

    /// One more run that did not commit, for `why`: the pause before it is sent again, or, the
    /// attempts spent, `why`.
    async fn again(
        &mut self,
        failures: u32,
        why: ClientError,
        deadline_ns: u64,
    ) -> Result<u32, ClientError> {
        let failures = failures.saturating_add(1);
        if failures >= ATTEMPTS {
            return Err(why);
        }
        let mut backoff = self.measured_pto();
        for _ in 1..failures {
            backoff = bounds::backed_off(backoff);
        }
        let pause = bounds::pause(backoff, self.id, self.waits);
        self.waits = self.waits.wrapping_add(1);
        let wait = u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX);
        self.wait_until(
            hyper_rt::futures::now_ns().saturating_add(wait),
            deadline_ns,
        )
        .await?;
        Ok(failures)
    }

    /// The upload's Finish: its object, of `length` bytes held to `whole`, committed, or
    /// `BadDigest`. Asked again where its answer was lost: the node answers a retry from the
    /// version the first made, matched by its size and checksum.
    async fn finish(
        &mut self,
        native: &Native,
        upload: &str,
        (length, whole): (u64, Whole),
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        let checksum = whole.value()?;
        let op = Op::Finish {
            bucket: native.bucket.clone(),
            key: native.key.clone(),
            upload: upload.to_owned(),
            id: native.id,
            headers: native.headers.clone(),
            length,
            checksum: checksum.clone(),
        };
        let mut failures = 0u32;
        loop {
            match self.call(&op, deadline_ns).await {
                Ok(reply) => {
                    if reply.status == Status::Done
                        && reply.checksum.as_ref().is_some_and(|kept| {
                            kept.algorithm != checksum.algorithm.code()
                                || kept.value != checksum.bytes
                        })
                    {
                        return Err(ClientError::Corrupt("the upload"));
                    }
                    return Ok(reply);
                }
                Err(error) if resumable(&error) => {
                    failures = self.again(failures, error, deadline_ns).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Asks `op` of the nodes in turn, by the module's rules.
    async fn ask<S: Source, K: Sink>(
        &mut self,
        op: &Op,
        source: Option<&mut S>,
        sink: Option<&mut K>,
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        let mut upload = match (op, source) {
            (
                Op::Put {
                    length, trailing, ..
                }
                | Op::UploadPart {
                    length, trailing, ..
                }
                | Op::Append {
                    length, trailing, ..
                },
                Some(source),
            ) => Some(Upload::new(source, *length, *trailing)?),
            _ => None,
        };
        self.asked(op, upload.as_mut(), sink, deadline_ns).await
    }

    /// Asks `op` with its body's `upload`, which the caller keeps: a run's trailer is read from it
    /// once the exchange ends, answered or not.
    async fn asked<S: Source, K: Sink>(
        &mut self,
        op: &Op,
        mut upload: Option<&mut Upload<'_, S>>,
        mut sink: Option<&mut K>,
        deadline_ns: u64,
    ) -> Result<Reply, ClientError> {
        let (head, fields) = op.encode()?;
        let body = op.body(&fields);
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
                    (upload.as_deref_mut(), sink.as_deref_mut()),
                    deadline_ns,
                )
                .await?;
            match asked {
                Attempt::Replied(reply) => {
                    let reply = *reply;
                    if let (Some(upload), Some(kept)) = (upload.as_deref(), &reply.checksum)
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
                    // Asked again, it goes to the next node: one that took it and lost it may
                    // still be stuck on the work, as a founder just started again was on its
                    // first reads, while any other node can answer it, a mutation from what its
                    // identity made.
                    self.at = self
                        .at
                        .checked_add(1)
                        .filter(|at| *at < self.nodes.len())
                        .unwrap_or(0);
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
            // No connection came up, so no node took the request: another node is asked, as for
            // a node gone, unless the refusal is the client's own, its certificate or its role,
            // which every node of the cell refuses alike.
            return Ok(if matches!(refused, Refusal::Identity | Refusal::Kind) {
                Attempt::Refused(refused)
            } else {
                Attempt::Untaken(refused)
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
        // When the transport had the request whole: from then the node has it to answer.
        let mut whole_ns: Option<u64> = None;
        self.fields.clear();
        loop {
            if reply.is_none() && !stopped && sent < body {
                stopped = !self.send(exchange, (fields, &mut upload), &mut sent)?;
            }
            if whole_ns.is_none() && sent >= body {
                whole_ns = Some(hyper_rt::futures::now_ns());
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
                    by_peer,
                } if e == exchange => {
                    // Cut by its period with no answer begun, the request whole at the node: what
                    // the node does for it takes longer than the node was given, so the next
                    // exchange's period is at least twice that (RFC 6298 §5.5's backoff), kept
                    // with the slowest measured so that a quick answer to another request between
                    // the two, the offset a resumed upload asks first, does not undo it.
                    if refusal == Refusal::Stalled
                        && !by_peer
                        && reply.is_none()
                        && let Some(whole) = whole_ns
                    {
                        let went = hyper_rt::futures::now_ns()
                            .saturating_sub(whole)
                            .saturating_mul(2);
                        self.slowest_ns = Some(self.slowest_ns.map_or(went, |s| s.max(went)));
                    }
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

    /// The progress period an exchange with `node` is given: the longest of the slowest exchange
    /// measured, none before one is, the path's persistent-congestion duration (RFC 9002 §7.6.1)
    /// and the node's own serve period, doubled for each exchange with the node cut by its period
    /// since its last reply (mantle-node's client's law), within the time left before
    /// `deadline_ns`. A node judges an exchange stalled only once it moved nothing for as long as
    /// a connection may stay silent, the idle timeout (`bounds::limits`), and may pause an
    /// object's body that long while its driver waits on durable writes: a client cutting it
    /// sooner abandons an operation the node would finish. A node that died is found by its
    /// connection's idle timeout, but one alive that never answers keeps its connection, and with
    /// no period while nothing was measured a client waited on it for good: a read through a
    /// founder just started again waited two hours
    /// (docs/measurements/2026-10-28-transfer-scale.md).
    fn period_ns(&mut self, node: PeerId, deadline_ns: u64, now_ns: u64) -> u64 {
        let left = deadline_ns.saturating_sub(now_ns).max(1);
        let slowest = self.slowest_ns.unwrap_or(0);
        let served = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap_or(u64::MAX);
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
        slowest
            .max(congested)
            .max(served)
            .saturating_mul(doubling)
            .min(left)
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

/// What a run's exchange came to (`Client::ran`): sent again for the error that ended it.
enum Ran {
    Committed(Checksum),
    Again(ClientError),
    Refused(Box<Reply>),
}

/// Whether an operation ended so that asking the node's progress and going on may answer it: its
/// exchange lost, never taken, or refused at admission with bytes a run can give again.
fn resumable(error: &ClientError) -> bool {
    match error {
        ClientError::Lost { .. } | ClientError::Untaken(_) => true,
        ClientError::Refused(refusal) => untaken(*refusal),
        _ => false,
    }
}

/// A run's Append.
fn append(native: &Native, upload: &str, (offset, length, last): (u64, u64, bool)) -> Op {
    Op::Append {
        bucket: native.bucket.clone(),
        key: native.key.clone(),
        upload: upload.to_owned(),
        id: native.id,
        offset,
        length,
        last,
        trailing: Some(native.checksum),
    }
}

/// The full-object checksum of the bytes so far, combined from each run's as CRCs combine
/// (05 §3.4).
struct Whole {
    algorithm: Algorithm,
    value: Option<(Checksum, u64)>,
}

impl Whole {
    fn new(algorithm: Algorithm) -> Self {
        Self {
            algorithm,
            value: None,
        }
    }

    /// The next `len` bytes, whose checksum is `checksum`.
    fn add(&mut self, checksum: Checksum, len: u64) -> Result<(), ClientError> {
        self.value = Some(match self.value.take() {
            None => (checksum, len),
            Some((so_far, at)) => {
                let combined = mantle_s3::checksum::full_object(&[(so_far, at), (checksum, len)])
                    .ok_or(ClientError::Bounds(
                    "a checksum that does not combine".into(),
                ))?;
                let end = at
                    .checked_add(len)
                    .ok_or(ClientError::Bounds("an object past u64".into()))?;
                (combined, end)
            }
        });
        Ok(())
    }

    /// The object's checksum: an empty one's where it has no byte.
    fn value(self) -> Result<Checksum, ClientError> {
        match self.value {
            Some((checksum, _)) => Ok(checksum),
            None => Hasher::new(self.algorithm)
                .and_then(Hasher::finish)
                .map_err(|e| ClientError::Bounds(format!("{e:?}"))),
        }
    }
}

/// A run of a source read again: `left` more of its bytes, then the run's end, whatever the source
/// holds past it. The source is never asked for no bytes, which a source whose next bytes have not
/// come may wait on.
struct Bounded<'a, S> {
    source: &'a mut S,
    left: u64,
}

impl<S: Source> Source for Bounded<'_, S> {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let want = usize::try_from(self.left)
            .unwrap_or(usize::MAX)
            .min(into.len());
        let Some(into) = into.get_mut(..want).filter(|into| !into.is_empty()) else {
            return Ok(0);
        };
        let n = self.source.read(into).await?;
        self.left = self
            .left
            .saturating_sub(u64::try_from(n).unwrap_or(u64::MAX));
        Ok(n)
    }
}

/// A run of a source read once and of a known length, sent as it is read: the bytes `held`
/// already gives again first, from `at`, then the source's, each kept in `held` as it is read, to
/// the run's `run` bytes and no further.
struct Replay<'a, S> {
    held: &'a mut Vec<u8>,
    at: usize,
    source: &'a mut S,
    run: usize,
}

impl<S: Source> Source for Replay<'_, S> {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        if let Some(kept) = self.held.get(self.at..)
            && !kept.is_empty()
        {
            let n = kept.len().min(into.len());
            if let (Some(into), Some(kept)) = (into.get_mut(..n), kept.get(..n)) {
                into.copy_from_slice(kept);
            }
            self.at = self.at.saturating_add(n);
            return Ok(n);
        }
        // The run's end is the run's own: the source is not asked for no bytes, which a source
        // whose next bytes have not come waits on (hyper-rt's standard input does).
        let want = self.run.saturating_sub(self.held.len()).min(into.len());
        let Some(into) = into.get_mut(..want).filter(|into| !into.is_empty()) else {
            return Ok(0);
        };
        let n = self.source.read(into).await?;
        let read = into.get(..n).unwrap_or_default();
        self.held
            .try_reserve(n)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        self.held.extend_from_slice(read);
        self.at = self.held.len();
        Ok(n)
    }
}

/// A run of a source read once, held until it commits: its bytes, and those read past it.
struct Held {
    bytes: Vec<u8>,
    /// The source ended: no byte past `bytes` is to come.
    ended: bool,
    /// The run a fill read toward: the bytes of `bytes` past it were read ahead.
    run: usize,
}

impl Held {
    /// Room for a run of `buffer` bytes and a piece read ahead past it, reserved at once.
    fn new(buffer: u64) -> Result<Self, ClientError> {
        let room = usize::try_from(buffer)
            .ok()
            .and_then(|b| b.checked_add(PIECE))
            .ok_or(ClientError::Bounds(
                "a buffer past the address space".into(),
            ))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(room)
            .map_err(|_| ClientError::Bounds("the client's buffer".into()))?;
        Ok(Self {
            bytes,
            ended: false,
            run: 0,
        })
    }

    /// Reads `source` until a run of `run` bytes is held, and with `ahead` a piece past it, or
    /// it ends.
    async fn fill<S: Source>(
        &mut self,
        source: &mut S,
        run: u64,
        ahead: bool,
    ) -> Result<(), ClientError> {
        let run =
            usize::try_from(run).map_err(|_| ClientError::Bounds("a run past memory".into()))?;
        self.run = run;
        let want = if ahead { run.saturating_add(1) } else { run };
        while self.bytes.len() < want && !self.ended {
            let at = self.bytes.len();
            let room = self.bytes.capacity().saturating_sub(at).min(PIECE);
            if room == 0 {
                return Err(ClientError::Bounds("a run past the client's buffer".into()));
            }
            self.bytes.resize(at.saturating_add(room), 0);
            let read = match self.bytes.get_mut(at..) {
                Some(into) => source.read(into).await,
                None => Ok(0),
            };
            match read {
                Ok(n) => {
                    self.bytes.truncate(at.saturating_add(n));
                    self.ended = n == 0;
                }
                Err(e) => {
                    self.bytes.truncate(at);
                    return Err(ClientError::Source(e));
                }
            }
        }
        Ok(())
    }

    /// The bytes of the run held: the run asked, or fewer where the source ended within it.
    fn run(&self, run: u64) -> Result<u64, ClientError> {
        let held = u64::try_from(self.bytes.len()).unwrap_or(u64::MAX);
        Ok(held.min(run))
    }

    /// `taken` as an index into the held bytes.
    fn len(&self, taken: u64) -> Result<usize, ClientError> {
        usize::try_from(taken)
            .ok()
            .filter(|t| *t <= self.bytes.len())
            .ok_or(ClientError::Bounds("a run past what is held".into()))
    }

    /// Bytes held past the run: read ahead, the next run's first.
    fn ahead(&self) -> usize {
        self.bytes.len().saturating_sub(self.run)
    }

    /// The run of `taken` bytes committed: what was read past it moves to the front.
    fn drop_run(&mut self, taken: u64) -> Result<(), ClientError> {
        let taken = self.len(taken)?;
        self.bytes.drain(..taken);
        Ok(())
    }
}

/// What an exchange that ended unanswered with `refusal` came to, `sent` of its body's bytes taken
/// by the transport. Refused at the node's admission, or its certificate refused, the node took
/// nothing: TLS 1.3's client authenticates after the server's Finished (RFC 8446 §4.4), so a
/// connection the client took as up may be refused with its first request sent. Such a request is
/// asked again where none of the source's bytes were handed over, and else refused, as a once-read
/// source cannot give them again. Any other end is lost, whole once every byte was sent.
fn ended(refusal: Refusal, sent: u64, (fields, body): (u64, u64)) -> Attempt {
    if untaken(refusal) && sent <= fields {
        Attempt::Untaken(refusal)
    } else if untaken(refusal) || refusal == Refusal::Identity {
        Attempt::Refused(refusal)
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
            assert!(matches!(ended(refusal, 11, (10, 100)), Attempt::Refused(_)));
        }
        // A certificate refused: nothing taken, and asking again changes nothing.
        for sent in [0, 11, 100] {
            assert!(matches!(
                ended(Refusal::Identity, sent, (10, 100)),
                Attempt::Refused(Refusal::Identity)
            ));
        }
        for refusal in [Refusal::Closed, Refusal::Stalled] {
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
