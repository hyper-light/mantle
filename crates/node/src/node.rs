//! The node's owner (docs/design/node.md §2, §3): one shard's range replicas in hyper-durable's arena,
//! replicating over hyper-raft's transport, driven by one task on a hyper-rt shard. The owner is a
//! value with a turn ([`Node::turn`]): its task awaits turn after turn, and a test drives several
//! nodes from one task the same way, inspecting each between turns.
//!
//! **Transport.** QUIC carries what is stateful (hyper-transport through hyper-rt-transport's
//! driver): a range's Raft messages on one lane to each peer (node.md §3.2), and snapshots on the
//! bulk class (§3.1). Consensus control that carries no entries and fits a datagram goes on the
//! sealed plane instead, ahead of everything (§3.4), with the node-pair liveness stream's
//! heartbeats. The plane's keys come from the TLS session of the QUIC connection to the same peer
//! (its exporter), so the plane authenticates exactly whom the handshake did, and a peer with no
//! live connection gets no datagrams. Of each pair of nodes the one with the lower id dials, so a
//! pair keeps one connection and each end knows which end it is.
//!
//! **Elections** are by suspicion (hyper-raft timing step L-2): the node keeps one liveness stream
//! to each node it shares a range with (hyper-liveness, L-3), each heartbeat proving a durable
//! flush of its log, and gives each replica the stream's suspicion, trust and restarts
//! (hyper-durable's `Owner`). No range ticks.
//!
//! **Wakes.** A replica's log write is answered through the owner task's waker; the owner then
//! drives the replicas that have a write out, each taking its answer if it came. A waker naming its
//! replica's slot, which hyper-rt's wake bitmap could give, would wake only that one (open: it
//! belongs in hyper-rt, whose wakers it is).

use std::collections::{BTreeSet, VecDeque};
use std::future::poll_fn;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Poll, Waker};
use std::time::Duration;

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_datagram::{AdmitAll, EXPORTER_LABEL, ExporterSecret, Plane, SECRET_BYTES};
use hyper_durable::{Driven, Handle, Owner, Unbounded};
use hyper_liveness::{Change, Liveness};
use hyper_log::{Class as LogClass, Config as LogConfig, Log, LogError, LogStats, Pending, Update};
use hyper_raft::proto::{Message, MessageType};
use hyper_rt::combine::{Either, Either3, race2, race3};
use hyper_rt_transport::{Driver, PlaneSocket};
use hyper_timing::Exposure;
use hyper_transport::tls::{Credentials, Tls};
use hyper_transport::{
    Classes, Config, Endpoint, Event, ExchangeId, Fixed, PeerId, Progress, Refusal,
};
use mantle_chunk::ChunkKey;
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::wire::Answer;
use mantle_range::membership::{Next, Replacement};
use mantle_range::{Applied, Elections, GroupStore, LogStore, Range, RangeMachine, ReplicaError};

use crate::bounds::{self, Stated};
use crate::chunk;
use crate::classes::{self, Bounds, Kind, NodeClasses, Role};
use crate::command;
use crate::directory::{Peer, Peers};
use crate::frame;
use crate::layout::Layout;
use crate::read::Query;
use crate::storage::{Done, Origin, Storage};

/// A plane message's first byte: a range's Raft frame. A liveness heartbeat starts with its own,
/// `hyper_liveness::KIND`.
const PLANE_RAFT: u8 = 0x52;
/// The log's group for the stream's own writes (`Output::flush`): a node whose ranges write none
/// still proves its disk to its peers. The top of the group space, as group 0 holds hyper-log's own
/// key records; a range named either is refused at open.
const LIVENESS_GROUP: u128 = u128::MAX;
/// The label of the exporter both ends of a connection derive its plane epoch from (RFC 5705 §4:
/// a label beginning "EXPERIMENTAL" is for private use, unregistered).
const EPOCH_LABEL: &[u8] = b"EXPERIMENTAL mantle plane epoch";
/// The lane each peer's Raft frames travel on: the node runs its ranges on one shard.
const LANE: u32 = 0;

/// The log's id: the node's.
fn log_id(node: u64) -> u128 {
    u128::from(node)
}

/// What a node is given.
pub struct NodeSettings<E> {
    pub id: u64,
    pub dir: PathBuf,
    /// The QUIC endpoint's address, and the plane's.
    pub quic: SocketAddr,
    pub plane: SocketAddr,
    pub credentials: Credentials,
    pub peers: Vec<Peer>,
    /// Where peers listen, as far as the node knows at its start: QUIC's address, the plane's.
    pub addresses: Vec<(PeerId, SocketAddr, SocketAddr)>,
    /// The ranges it hosts, each a member named by the node's id.
    pub ranges: Vec<Hosted<E>>,
    pub log: LogConfig,
    /// The bytes the transport may hold at once.
    pub transport_bytes: u64,
    /// The seed of the QUIC endpoint's draws (`Endpoint::new_seeded`), so a simulation's run is its
    /// seed (node.md §9.1); `None` in a process, whose endpoint draws its own.
    pub seed: Option<[u8; 32]>,
    /// The node's storage role: the chunk volumes it keeps, opened before its runtime starts
    /// ([`Storage::open`]), or [`Storage::none`].
    pub storage: Storage,
    /// The largest chunk the cell's gateways write: with a chunk message's head, the transfer
    /// class's bound (node.md §3.1), the same in every node and client of a process, as every
    /// class's bound is.
    pub chunk_bytes: u64,
    /// The chunk puts and gets this node keeps out to its peers at once; one past them is
    /// answered `Busy` ([`Node::put_chunk`]).
    pub chunk_asks: usize,
}

/// The number a chunk put or get this node asked is answered under ([`Node::take_chunks`]).
pub type ChunkAsk = u64;

/// What became of a chunk put or get this node asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkOutcome {
    /// The put's chunk is durable on its volume.
    Stored,
    /// The get's bytes, verified by the volume.
    Read(Vec<u8>),
    /// The volume, or the node that holds it, refused it, and says why.
    Refused(chunk::Status),
    /// The exchange ended without an answer: its peer is unreachable, refused it (the
    /// transport's refusal: `Budget` when the peer holds as many chunks as its volumes queue), or
    /// its connection closed. A put may still have landed (node.md §5.2).
    Lost(Refusal),
}

/// A range the node hosts: its log group, the range, and its engine as it starts: a range's
/// founding state, or what the engine holds durably (`mantle_range::Stored`, opened at its last
/// checkpoint), from which the log is replayed.
#[derive(Debug, Clone)]
pub struct Hosted<E> {
    pub group: u128,
    pub range: Range,
    pub engine: E,
}

/// Why the node stopped.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("the node's records: {0}")]
    Layout(#[from] crate::layout::LayoutError),
    #[error("the node's log: {0}")]
    Log(String),
    #[error("a range: {0}")]
    Range(#[from] mantle_range::ReplicaError),
    #[error("the transport: {0:?}")]
    Transport(Refusal),
    #[error("the runtime: {0}")]
    Runtime(String),
    #[error("the plane: {0:?}")]
    Plane(hyper_datagram::Refusal),
    #[error("the liveness stream: {0:?}")]
    Liveness(hyper_liveness::Refusal),
    #[error("the node's bounds: {0}")]
    Bounds(String),
    #[error("turned by a task other than the one that opened it, whose waker its log answers")]
    Task,
    #[error("more ranges than the node's arena holds")]
    Full,
    #[error("a range's group {0:x} is the log's own or the liveness stream's")]
    Reserved(u128),
}

impl From<Refusal> for NodeError {
    fn from(r: Refusal) -> Self {
        Self::Transport(r)
    }
}

impl From<hyper_rt::RtError> for NodeError {
    fn from(e: hyper_rt::RtError) -> Self {
        Self::Runtime(format!("{e:?}"))
    }
}

/// What a refused reply on one of the node's exchanges comes to. A refusal of the exchange alone
/// (its asker gave up, its stream or connection closed, its progress or a budget ran out: the
/// transport ended it, and its `Refused` event follows or came) concerns that one asker, never the
/// node's ranges: the reply is dropped and the node goes on, and the client asks again. A reply out
/// of order or past its class's bounds is the node's own fault, and stops it.
fn exchange_ended(refusal: Refusal) -> Result<(), NodeError> {
    match refusal {
        Refusal::Order | Refusal::FrameBound => Err(NodeError::Transport(refusal)),
        _ => Ok(()),
    }
}

/// The node's log as its owner uses it: each range's group claimed as the store its replica writes
/// through, the liveness stream's writes on the log's own group (`LIVENESS_GROUP`), and what the
/// log measured. A node runs on hyper-log over its device's file ([`Node::open`]); a simulation
/// gives it hyper-log over a simulated device whose answers its run takes where it decides
/// (node.md §9.1).
pub trait NodeLog {
    /// The store a range's replica writes through.
    type Store: LogStore;
    /// A liveness write, until the log answers it.
    type Written: Written;
    /// `group`'s records, claimed for `range`'s replica.
    fn claim(&self, group: u128, range: &Range) -> Result<Self::Store, ReplicaError>;
    /// The stream's liveness write, an empty update on `group` of the latency class, its answer
    /// told through `waker`.
    fn write_liveness(&self, group: u128, waker: Waker) -> Result<Self::Written, LogError>;
    /// What the log measured from its opening (hyper-log's `Log::stats`).
    fn stats(&self, into: Option<Box<LogStats>>) -> Result<Box<LogStats>, LogError>;
}

/// A write the log answers once (hyper-log's `Pending::poll`).
pub trait Written {
    /// Its answer, once it came: given once.
    fn poll(&self) -> Option<Result<(), LogError>>;
}

impl Written for Pending {
    fn poll(&self) -> Option<Result<(), LogError>> {
        Pending::poll(self)
    }
}

impl<F: BlockFile + 'static> NodeLog for Log<F> {
    type Store = GroupStore<F>;
    type Written = Pending;

    fn claim(&self, group: u128, range: &Range) -> Result<GroupStore<F>, ReplicaError> {
        mantle_range::claim(self, group, range)
    }

    fn write_liveness(&self, group: u128, waker: Waker) -> Result<Pending, LogError> {
        self.submit_waking(group, LogClass::Latency, Update::default(), waker)
    }

    fn stats(&self, into: Option<Box<LogStats>>) -> Result<Box<LogStats>, LogError> {
        Log::stats(self, into)
    }
}

/// What the liveness stream asked during a call into it.
#[derive(Default)]
struct Asked {
    heartbeats: Vec<(PeerId, Vec<u8>)>,
    flush: bool,
    changes: Vec<Change>,
}

impl hyper_liveness::Output for Asked {
    fn heartbeat(&mut self, peer: PeerId, message: &[u8]) {
        self.heartbeats.push((peer, message.to_vec()));
    }
    fn flush(&mut self) {
        self.flush = true;
    }
    fn change(&mut self, change: Change) {
        self.changes.push(change);
    }
}

/// What a node shows of one of its ranges and of its liveness stream: what an operator, and a
/// test's waits, read of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// The range member's term, the leader it knows (0: none), its commit and what it applied.
    pub term: u64,
    pub leader: u64,
    pub commit: u64,
    pub applied: u64,
    /// Heartbeats the stream took from every peer.
    pub heartbeats: u64,
    /// Whether a margin judges every peer the stream keeps (`hyper_liveness::PairReport::judged`).
    pub judged: bool,
    /// Heartbeats taken from the peers not yet judged: what moves while the node measures its
    /// links, and stops moving if a peer it measures goes silent.
    pub measuring: u64,
    /// While some peer is not yet judged, the longest its heartbeats or this node's to it go
    /// between by the stream's schedule (`PairReport::interval`, `PairReport::sending`): how long
    /// the node's measuring may go without moving. `None` while every peer is judged, or none has
    /// a schedule yet.
    pub measuring_for: Option<Duration>,
    /// Whether the node waits on its own device: its stream wants a durable completion it lacks
    /// (`hyper_liveness::Liveness::own_proof`), and by its law judges no peer until one comes
    /// (timing.md §2.8, "A judge whose own proof stalls"). Nodes on one device wait on it
    /// together, and hold still together, as a stall of that device makes them.
    pub held: bool,
    /// How long the node may go without moving while its peers live, by its own law: the longest
    /// any peer's evidence may go unmoved before it is suspected (`PairReport::freshness`), and the
    /// wait of a judge with nothing durable since that point for a completion of its own, which its
    /// stream asks within its sending interval and a flush makes (hyper-liveness, timing.md §2.8:
    /// the longest interval it sends at, `PairReport::sending`, and its mean flush); then the
    /// range's expected election from a suspicion and one more round (hyper-raft's `Timing`).
    /// `None` until both are measured.
    pub patience: Option<Duration>,
    /// Snapshots this node sent that their member took, and snapshots it took, of every range.
    pub snapshots_sent: u64,
    pub snapshots_taken: u64,
    /// Whether the range's member waits for room in the log, its writes out, and the writes it
    /// made by what they were for.
    pub stalled: bool,
    pub in_flight: usize,
    pub writes: hyper_durable::Writes,
    /// Whether the member is fenced.
    pub fenced: bool,
    /// What the node's Raft sends did, and what it did with the frames that came to it.
    pub sends: Sends,
    pub received: Received,
    /// How long a link to a peer this node dials may go without moving, by its own law, should
    /// that peer fall silent (cut off, killed, stopped, its host stalled), which the node cannot
    /// tell from slow: a connection to a silent peer ends within the idle timeout, as a dial out
    /// does, and the next dial follows within the peer's backoff ([`bounds::backed_off`]), until
    /// one connects. `None` when the node dials no peer.
    pub reconnect: Option<Duration>,
    /// The reads the range's core holds, as its leader: waiting for its term's first commit,
    /// for a quorum to confirm it leads, or to be taken; and of them, those waiting for that
    /// quorum (hyper-raft's `Raft::reads_held`, `Raft::pending_read_count`).
    pub reads_held: usize,
    pub reads_confirming: usize,
    /// Clients' reads and commands this node holds open: reads waiting to be confirmed and
    /// applied through, and commands waiting for every answer.
    pub reading: usize,
    pub commanded: usize,
}

/// A peer's link as the node keeps it: whether the plane holds keys for its connection, the
/// dials made to it, the connections and dials to it that ended, and the last refusal one ended
/// with, as its peer or this side stated it (`None` when none was stated: a timeout, the path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Link {
    pub peer: PeerId,
    pub keyed: bool,
    pub dials: u64,
    pub ended: u64,
    pub refusal: Option<Refusal>,
}

/// What the node's sends of Raft messages did, all told.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Sends {
    /// Messages queued on the plane, frames given a lane, and snapshots' exchanges opened.
    pub plane: u64,
    pub lane: u64,
    pub snapshots: u64,
    /// Frames a full lane held for room.
    pub waited: u64,
    /// Messages dropped, as the network may drop them, for Raft to send again: no connection, a
    /// lane past its window, a refusal; and the latest refusal, with its frame's bytes and its
    /// message's type when it was a Raft message's.
    pub dropped: u64,
    pub refused: Option<(Refusal, usize, Option<MessageType>)>,
}

impl Sends {
    fn dropped(&mut self, refusal: Refusal, bytes: usize, kind: Option<MessageType>) {
        self.dropped = self.dropped.saturating_add(1);
        self.refused = Some((refusal, bytes, kind));
    }
}

/// What the node did with the Raft frames that came to it, all told: stepped into their range,
/// or dropped (damaged, misaddressed, for a range it does not host, from a sender not in the
/// range's configuration, or refused by a replica waiting for room).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Received {
    pub stepped: u64,
    pub dropped: u64,
}

/// A range as the node keeps it.
struct Kept {
    group: u128,
    /// The layer whose rows the range holds, which a read must ask of.
    layer: Layer,
    handle: Handle,
    max_entry_bytes: u64,
    /// The replica's latest wake, held in [`Node::deadlines`].
    wake: Option<u64>,
    /// A member lost for good the node replaces while it leads the range ([`Node::replace`]).
    replacing: Option<Replacement>,
}

/// A peer as the node keeps it.
struct Remote {
    id: PeerId,
    /// Where it listens, once known.
    quic: Option<SocketAddr>,
    plane: Option<SocketAddr>,
    /// Frames the lane had no room for, sent when it has (`Event::LaneRoom`), at most the lane's
    /// window; one past it is dropped, as the network may drop it, and Raft sends it again.
    waiting: VecDeque<Vec<u8>>,
    /// Whether the plane holds keys for its current connection.
    keyed: bool,
    /// When this node dials it again, for a peer it dials that has no connection.
    redial_at: Option<u64>,
    /// The redial's backoff ([`bounds::backed_off`]): from the probe timeout the last connection
    /// measured, or the initial one before any.
    backoff: Duration,
    /// Redials paused for so far: the draw each pause's jitter takes ([`bounds::pause`]).
    redials: u32,
    /// Dials made to it, connections and dials to it that ended, and the last refusal one ended
    /// with ([`Link`]).
    dials: u64,
    ended: u64,
    refusal: Option<Refusal>,
}

/// A chunk put or get this node asked of a peer, out on an exchange.
#[derive(Clone, Copy)]
struct ChunkOut {
    exchange: ExchangeId,
    asked: ChunkAsk,
    peer: PeerId,
    get: bool,
}

/// A body going out on an exchange, with how much of it the exchange has taken (`write_body`
/// takes what credit allows; `Event::Writable` says when more), and what follows once it is all
/// written.
struct Writing {
    exchange: ExchangeId,
    bytes: Vec<u8>,
    written: usize,
    then: Then,
}

/// What a body written whole waits for.
#[derive(Clone, Copy)]
enum Then {
    /// A snapshot of `group` to `to`: the member's reply, which says it arrived.
    Snapshot { group: u128, to: u64 },
    /// A command's reply: nothing; the exchange ends.
    Reply,
    /// A chunk's put this node asked: its peer's reply.
    Asked,
}

/// A body arriving on an exchange: from whom, its length, how much of it was read and what of it
/// is kept, and what it is.
struct Receiving {
    exchange: ExchangeId,
    peer: PeerId,
    body: u64,
    read: u64,
    bytes: Vec<u8>,
    what: Arriving,
}

/// What an arriving body is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Arriving {
    /// A range's snapshot, its frame whole.
    Snapshot,
    /// A command for the range of this group: an entry.
    Command(u128),
    /// A read of the range of this group: a query.
    Read(u128),
    /// A request this node answers without its body: the range of this group is led elsewhere,
    /// or the request's head is answered with this status. Its body is still read to its end,
    /// and dropped, before the answer goes, so the exchange ends whole: ending an exchange whose
    /// request half is unread resets that half (hyper-transport's `Endpoint::end`), and the asker
    /// hears the reset as the exchange refused, not the answer it carried (an early response,
    /// which RFC 9114 §4.1 allows a server only where its client keeps the complete response).
    Redirect(u128),
    Status(command::Status),
    /// A chunk's put for the volume in this slot, its body the chunk's bytes, which hold their
    /// length of the room the node takes chunks into.
    ChunkPut {
        slot: usize,
        key: ChunkKey,
        crc32c: u32,
    },
    /// A chunk's get from the volume in this slot; it carries no body.
    ChunkGet {
        slot: usize,
        key: ChunkKey,
        offset: u64,
        len: u64,
    },
    /// A chunk's request answered with this status once its body is read and dropped.
    ChunkStatus(chunk::Status),
    /// The bytes a peer read for this node's get, asked under this number.
    ChunkBytes(ChunkAsk),
}

impl Arriving {
    /// The class its body is read into.
    fn class(self) -> classes::Class {
        match self {
            Self::Snapshot => classes::Class::Bulk,
            Self::Command(_) | Self::Read(_) | Self::Redirect(_) | Self::Status(_) => {
                classes::Class::Request
            }
            Self::ChunkPut { .. }
            | Self::ChunkGet { .. }
            | Self::ChunkStatus(_)
            | Self::ChunkBytes(_) => classes::Class::Transfer,
        }
    }

    /// Whether its body is kept as it is read: what it is answered without is only read.
    fn keeps(self) -> bool {
        match self {
            Self::Snapshot
            | Self::Command(_)
            | Self::Read(_)
            | Self::ChunkPut { .. }
            | Self::ChunkBytes(_) => true,
            Self::Redirect(_) | Self::Status(_) | Self::ChunkGet { .. } | Self::ChunkStatus(_) => {
                false
            }
        }
    }
}

/// A read this node's member of `group` asked a quorum to confirm, as leader, waiting to be
/// served: its context's id (the shell gives it back with the read), and the query.
struct Reading {
    id: u64,
    exchange: ExchangeId,
    group: u128,
    query: Query,
}

/// A command this node's member of `group` proposed as leader, waiting for its entry to apply:
/// each command's session and serial, in the entry's order, with its answer once applied.
struct Commanded {
    exchange: ExchangeId,
    group: u128,
    commands: Vec<(u64, u64, Option<Answer>)>,
}

/// The piece a body is read in: the transport's own receive chunk.
const PIECE: u64 = hyper_transport::RECEIVE_CHUNK as u64;

/// One shard's ranges and the transport they replicate over, on the node's log.
pub struct Node<E: Engine, L: NodeLog = Log<DeviceFile>> {
    id: u64,
    log: L,
    owner: Owner<L::Store, RangeMachine<E>, Unbounded>,
    ranges: Vec<Kept>,
    deadlines: BTreeSet<(u64, usize)>,
    liveness: Liveness,
    asked: Asked,
    liveness_write: Option<(L::Written, u64)>,
    liveness_moved: bool,
    driver: Driver<NodeClasses, Fixed, Peers>,
    plane: Plane,
    plane_socket: PlaneSocket,
    remotes: Vec<Remote>,
    lane_window: usize,
    writing: Vec<Writing>,
    receiving: Vec<Receiving>,
    /// Commands proposed and not yet answered, and reads asked and not yet served: at most what
    /// the transport admits as exchanges.
    commanded: Vec<Commanded>,
    reading: Vec<Reading>,
    /// The id the next read's context carries.
    next_read: u64,
    /// Plane messages taken this turn: sender, the kernel's arrival stamp, the bytes.
    inbox: Vec<(PeerId, u64, Vec<u8>)>,
    events: Vec<Event<NodeClasses>>,
    out: hyper_durable::Output<Applied>,
    frame: Vec<u8>,
    applied: Vec<(u128, Applied)>,
    /// The replacements that ended while this node led, since the last call
    /// ([`Node::take_replaced`]).
    replaced: Vec<(u128, Replacement)>,
    snapshots_sent: u64,
    snapshots_taken: u64,
    /// Each range's timing before a measure, to tell which moved.
    timings: Vec<Option<hyper_raft::Timing>>,
    sends: Sends,
    received: Received,
    waker: Waker,
    storage: Storage,
    /// The storage's puts and gets answered, taken each turn (kept for its allocation).
    stored: Vec<Done>,
    /// The chunk puts and gets this node asked of its peers and not yet answered; at most
    /// `chunk_asks` of them.
    chunks_out: Vec<ChunkOut>,
    chunk_asks: usize,
    next_chunk: ChunkAsk,
    /// The chunk puts and gets this node asked, answered and not yet taken.
    chunks_done: Vec<(ChunkAsk, ChunkOutcome)>,
    chunk_bytes: u64,
}

/// The node's message bounds: a Raft message is at most the larger of a page of entries and one
/// entry of the range's largest, inside the core's fixed bytes, in a frame; a command is an entry
/// behind its head, and its reply no larger (`command::Status::TooLarge` otherwise); a snapshot's
/// data is at most what hyper-raft's wire states, a u32 of bytes.
pub fn message_bounds<E>(ranges: &[Hosted<E>], chunk_bytes: u64) -> Result<Bounds, NodeError> {
    let unfit = || NodeError::Bounds("a range's settings overflow a message's bound".into());
    let mut largest = 0u64;
    let mut entry = 0u64;
    for hosted in ranges {
        let s = &hosted.range.settings;
        let message = s
            .max_size_per_msg
            .max(s.max_entry_bytes)
            .checked_add(
                u64::try_from(hyper_raft::wire::MESSAGE_RECORD_FIXED_BYTES).map_err(|_| unfit())?,
            )
            .and_then(|m| m.checked_add(u64::try_from(frame::HEADER_BYTES).ok()?))
            .ok_or_else(unfit)?;
        largest = largest.max(message);
        entry = entry.max(s.max_entry_bytes);
    }
    let bulk = u64::from(u32::MAX).checked_add(largest).ok_or_else(unfit)?;
    let request = entry
        .checked_add(u64::try_from(command::HEAD_BYTES).map_err(|_| unfit())?)
        .ok_or_else(unfit)?;
    // A chunk's put carries the chunk behind its head; a get's reply, the bytes asked.
    let chunk_head = chunk::PUT_HEAD_BYTES
        .max(chunk::GET_HEAD_BYTES)
        .max(chunk::REPLY_HEAD_BYTES);
    let transfer = chunk_bytes
        .checked_add(u64::try_from(chunk_head).map_err(|_| unfit())?)
        .ok_or_else(unfit)?;
    Ok(Bounds {
        replication: largest,
        request,
        transfer,
        bulk,
    })
}

impl<E: Engine + 'static> Node<E> {
    /// Opens the node on the current hyper-rt shard on its log in its directory, as
    /// [`Node::open_on`] opens it on any.
    pub async fn open(settings: NodeSettings<E>) -> Result<Self, NodeError> {
        let log = open_log(&settings.dir, settings.id, settings.log)?;
        Self::open_on(settings, log).await
    }
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// Opens the node on the current hyper-rt shard, on `log`: raises its run, claims its ranges'
    /// groups and opens their replicas, binds its QUIC endpoint and its plane, and dials each peer
    /// whose id is above its own. The task that opens it is the one that turns it: its waker is the
    /// one the log answers through.
    pub async fn open_on(mut settings: NodeSettings<E>, log: L) -> Result<Self, NodeError> {
        let waker = poll_fn(|context| Poll::Ready(context.waker().clone())).await;
        let layout = Layout::new(&settings.dir);
        let run = layout.raise_run()?;
        let messages = message_bounds(&settings.ranges, settings.chunk_bytes)?;
        classes::state_bounds(messages).map_err(|e| NodeError::Bounds(e.to_string()))?;
        let inflight = settings
            .ranges
            .iter()
            .map(|h| h.range.settings.max_inflight_msgs)
            .max()
            .unwrap_or(0);
        let stated = Stated {
            peers: settings.peers.len(),
            ranges: settings.ranges.len(),
            inflight,
            transfers: settings
                .storage
                .transfers()
                .saturating_add(settings.chunk_asks),
            transport_bytes: settings.transport_bytes,
            messages,
        };
        let limits = bounds::limits(&stated).map_err(|e| NodeError::Bounds(e.to_string()))?;
        let lane_window = limits.lane_window;
        let io = bounds::io(&stated);
        let plane_limits = bounds::plane(&stated).map_err(|e| NodeError::Bounds(e.to_string()))?;

        let hosting = std::mem::take(&mut settings.ranges);
        let mut owner = Owner::new(vec![waker.clone(); hosting.len()]);
        let mut ranges = Vec::with_capacity(hosting.len());
        for hosted in hosting {
            if hosted.group == 0 || hosted.group == LIVENESS_GROUP {
                return Err(NodeError::Reserved(hosted.group));
            }
            let store = log.claim(hosted.group, &hosted.range)?;
            let shell = mantle_range::open_shell(
                settings.id,
                store,
                hosted.engine,
                &hosted.range,
                settings.id,
                Elections::Suspicion,
            )?;
            let handle = owner.insert(shell).map_err(|_| NodeError::Full)?;
            owner.schedule(handle);
            ranges.push(Kept {
                group: hosted.group,
                layer: hosted.range.layer,
                handle,
                max_entry_bytes: hosted.range.settings.max_entry_bytes,
                wake: None,
                replacing: None,
            });
        }
        let liveness = Liveness::new(hyper_liveness::Settings {
            local: settings.id,
            run,
            max_peers: settings.peers.len().max(1),
            history: Exposure::new(),
            resolution: Duration::from_nanos(hyper_rt::machine::clock::resolution_ns()),
        })
        .map_err(NodeError::Liveness)?;

        let tls = Tls::mutual(&settings.credentials, true)?;
        let config = Config {
            tls,
            role: Role::Node,
            limits,
            congestion: Default::default(),
        };
        let budget = Fixed::new(settings.transport_bytes, settings.peers.len().max(1));
        let directory = Peers::new(settings.peers.clone());
        let now = std::time::Instant::now();
        let endpoint = match settings.seed {
            Some(seed) => Endpoint::new_seeded(config, budget, directory, now, seed)?,
            None => Endpoint::new(config, budget, directory, now)?,
        };
        let driver = Driver::bind(endpoint, settings.quic, io)?;
        let plane_socket = PlaneSocket::bind(settings.plane, io)?;
        let plane = Plane::new(settings.id, plane_limits).map_err(NodeError::Plane)?;
        let remotes = settings
            .peers
            .iter()
            .filter(|p| p.role == Role::Node)
            .map(|p| Remote {
                id: p.id,
                quic: None,
                plane: None,
                waiting: VecDeque::new(),
                keyed: false,
                redial_at: None,
                backoff: bounds::initial_pto(),
                redials: 0,
                dials: 0,
                ended: 0,
                refusal: None,
            })
            .collect();
        let mut node = Self {
            id: settings.id,
            log,
            owner,
            ranges,
            deadlines: BTreeSet::new(),
            liveness,
            asked: Asked::default(),
            liveness_write: None,
            liveness_moved: true,
            driver,
            plane,
            plane_socket,
            remotes,
            lane_window,
            writing: Vec::new(),
            receiving: Vec::new(),
            commanded: Vec::new(),
            reading: Vec::new(),
            next_read: 0,
            inbox: Vec::new(),
            events: Vec::new(),
            out: hyper_durable::Output::default(),
            frame: Vec::new(),
            applied: Vec::new(),
            replaced: Vec::new(),
            snapshots_sent: 0,
            snapshots_taken: 0,
            timings: Vec::new(),
            sends: Sends::default(),
            received: Received::default(),
            waker,
            storage: std::mem::take(&mut settings.storage),
            stored: Vec::new(),
            chunks_out: Vec::new(),
            chunk_asks: settings.chunk_asks,
            next_chunk: 0,
            chunks_done: Vec::new(),
            chunk_bytes: settings.chunk_bytes,
        };
        for kept in &node.ranges {
            node.owner
                .pairs(kept.handle, &mut node.liveness, false)
                .map_err(NodeError::Liveness)?;
        }
        for (peer, quic, plane) in settings.addresses {
            node.learn(peer, quic, plane)?;
        }
        Ok(node)
    }

    /// Where `peer` listens: QUIC's address and the plane's. A peer that moved started again
    /// elsewhere, so its connection is to a process that is gone: it is closed now, not after its
    /// idle timeout, since the transport dials no peer it holds a connection to. A peer this node
    /// dials (its id is above this node's) is dialed now when it moved or has no connection; one
    /// that dials this node dials it again from where it started.
    pub fn learn(
        &mut self,
        peer: PeerId,
        quic: SocketAddr,
        plane: SocketAddr,
    ) -> Result<(), NodeError> {
        let dials = self.id < peer;
        let now = hyper_rt::futures::now_ns();
        let Some(remote) = self.remotes.iter_mut().find(|r| r.id == peer) else {
            return Ok(());
        };
        let moved = remote.quic.is_some_and(|known| known != quic);
        remote.quic = Some(quic);
        remote.plane = Some(plane);
        if dials && (moved || !remote.keyed) {
            remote.redial_at = Some(now);
            remote.backoff = bounds::initial_pto();
        }
        if moved {
            let clock = self.driver.clock()?;
            self.driver.endpoint().disconnect(clock, peer);
        }
        self.dial(now);
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// The QUIC endpoint's address and the plane's, as bound.
    pub fn addresses(&self) -> Result<(SocketAddr, SocketAddr), NodeError> {
        Ok((self.driver.local_addr()?, self.plane_socket.local_addr()?))
    }

    fn kept(&self, group: u128) -> Option<&Kept> {
        self.ranges.iter().find(|k| k.group == group)
    }

    /// Whether this node's member leads `group`.
    pub fn is_leader(&self, group: u128) -> bool {
        self.kept(group)
            .and_then(|k| self.owner.get(k.handle))
            .is_some_and(|r| r.is_leader())
    }

    /// The last entry this node's member of `group` applied.
    pub fn applied(&self, group: u128) -> u64 {
        self.kept(group)
            .and_then(|k| self.owner.get(k.handle))
            .map_or(0, |r| r.applied().index)
    }

    /// Proposes an entry of commands to `group`; only its leader takes it.
    pub fn propose(
        &mut self,
        group: u128,
        entry: &mantle_meta::wire::Entry,
    ) -> Result<(), NodeError> {
        let Some(kept) = self.ranges.iter().find(|k| k.group == group) else {
            return Err(NodeError::Bounds(format!("no range {group:x} here")));
        };
        let (handle, max) = (kept.handle, kept.max_entry_bytes);
        let shell = self
            .owner
            .get_mut(handle)
            .ok_or(NodeError::Bounds("a range's handle names nothing".into()))?;
        mantle_range::propose_entry(shell, entry, max)?;
        self.owner.schedule(handle);
        Ok(())
    }

    /// What the node shows of `group` and of its stream.
    pub fn status(&self, group: u128) -> Option<Status> {
        let replica = self.kept(group).and_then(|k| self.owner.get(k.handle))?;
        let (mut heartbeats, mut measuring) = (0u64, 0u64);
        let mut measuring_for: Option<Duration> = None;
        let mut judged = true;
        for report in self
            .remotes
            .iter()
            .filter_map(|r| self.liveness.report(r.id))
        {
            heartbeats = heartbeats.saturating_add(report.taken);
            judged &= report.judged;
            if !report.judged {
                measuring = measuring.saturating_add(report.taken);
                for gap in [report.interval, report.sending].into_iter().flatten() {
                    measuring_for = Some(measuring_for.map_or(gap, |m| m.max(gap)));
                }
            }
        }
        let patience = self.patience(group);
        Some(Status {
            term: replica.term(),
            leader: replica.leader(),
            commit: replica.logged_commit(),
            applied: replica.applied().index,
            heartbeats,
            judged,
            measuring,
            measuring_for,
            held: matches!(self.liveness.own_proof().waited, Some((_, None))),
            patience,
            snapshots_sent: self.snapshots_sent,
            snapshots_taken: self.snapshots_taken,
            stalled: replica.is_stalled(),
            in_flight: replica.in_flight(),
            writes: replica.writes(),
            fenced: replica.fenced().is_some(),
            sends: self.sends,
            received: self.received,
            reconnect: self
                .remotes
                .iter()
                .filter(|r| self.id < r.id)
                .map(|r| bounds::idle_timeout().saturating_add(r.backoff))
                .max(),
            reads_held: replica.core().raft.reads_held(),
            reads_confirming: replica.core().raft.pending_read_count(),
            reading: self.reading.iter().filter(|r| r.group == group).count(),
            commanded: self.commanded.iter().filter(|c| c.group == group).count(),
        })
    }

    /// How long `group`'s member may go without moving while its peers live, by the node's own law
    /// ([`Status::patience`]): the longest any peer's evidence may go unmoved before it is
    /// suspected, the judge's wait for a completion of its own, then the range's expected election
    /// and one more round. `None` until both are measured. Since hyper-liveness 2b13b15 a judge
    /// with nothing durable since a passed point asks no write of its own and judges on its next
    /// completion, its stream's within its sending interval. Left out, the patience a deaf leader
    /// stated was shorter than its step-down: on windows-11-arm leaders that heard no peer stopped
    /// leading up to 7.7 s after their last heartbeat, freshness about 5 s and sending about 2 s
    /// (CI run 39567831694), and the deaf leader's watch gave up at 5.3 s (run 39565012904).
    fn patience(&self, group: u128) -> Option<Duration> {
        let replica = self.kept(group).and_then(|k| self.owner.get(k.handle))?;
        let (mut evidence, mut sending) = (None::<Duration>, Duration::ZERO);
        for report in self
            .remotes
            .iter()
            .filter_map(|r| self.liveness.report(r.id))
        {
            if let Some(freshness) = report.freshness {
                evidence = Some(evidence.map_or(freshness, |e| e.max(freshness)));
            }
            if let Some(interval) = report.sending {
                sending = sending.max(interval);
            }
        }
        let judging = sending.saturating_add(self.liveness.flush_mean().unwrap_or_default());
        let timing = replica.core().raft.timing()?;
        Some(
            evidence?
                .saturating_add(judging)
                .saturating_add(timing.election)
                .saturating_add(timing.round),
        )
    }

    /// Stops the node as a power cut stops it, giving back what it held of its own: its log, and
    /// each range's group and engine as they stood, with whatever they had not made durable. Its
    /// connections and sockets go with it, unclosed, as a cut leaves them to its peers' timeouts.
    pub fn close(mut self) -> (L, Vec<(u128, E)>) {
        let mut engines = Vec::with_capacity(self.ranges.len());
        for kept in std::mem::take(&mut self.ranges) {
            if let Some(replica) = self.owner.remove(kept.handle) {
                engines.push((kept.group, replica.into_machine().into_engine()));
            }
        }
        (self.log, engines)
    }

    /// What the node's log measured from its opening (hyper-log `Log::stats`), asked of the log's
    /// owner, which answers as it comes: a flush that stalls shows as `flushing_since`. `into`, a
    /// box an earlier call gave back, is filled again.
    pub fn log_stats(&self, into: Option<Box<LogStats>>) -> Result<Box<LogStats>, NodeError> {
        self.log
            .stats(into)
            .map_err(|e| NodeError::Log(e.to_string()))
    }

    /// Each peer's link as the node keeps it.
    pub fn links(&self) -> impl Iterator<Item = Link> + '_ {
        self.remotes.iter().map(|r| Link {
            peer: r.id,
            keyed: r.keyed,
            dials: r.dials,
            ended: r.ended,
            refusal: r.refusal,
        })
    }

    /// What the node's ranges answered as they applied, since the last call: kept until taken, so
    /// its caller takes them at each turn, or drops them there.
    pub fn take_applied(&mut self) -> std::vec::Drain<'_, (u128, Applied)> {
        self.applied.drain(..)
    }

    /// Replaces `failed`, a member of `group`'s range lost for good, with `joining`, a member
    /// under an identity never used, hosted on a node that opened the range as its founders did
    /// (its founding configuration and rows), of which it is no member, so it campaigns for
    /// nothing until the replacement adds it (replica.md §6). While this node leads the range it proposes what
    /// `membership::Replacement` names next at each turn: `joining` as a learner, then, once it
    /// has caught up, one joint change that makes it a voter and removes `failed`. The
    /// replacement is stateless, so every node may be told it: one that leads later takes it up
    /// from the configuration it finds, and one already done ends at once. A replacement that
    /// ended is reported once by the node that led then ([`Self::take_replaced`]).
    pub fn replace(&mut self, group: u128, failed: u64, joining: u64) -> Result<(), NodeError> {
        let replacement = Replacement::new(failed, joining).ok_or(NodeError::Bounds(
            "a replacement takes a member under an identity never used, not zero".into(),
        ))?;
        let kept = self
            .ranges
            .iter_mut()
            .find(|k| k.group == group)
            .ok_or(NodeError::Bounds(format!("no range of group {group:x}")))?;
        kept.replacing = Some(replacement);
        self.owner.schedule(kept.handle);
        Ok(())
    }

    /// The replacements that ended while this node led, since the last call: each range's group,
    /// and the members swapped.
    pub fn take_replaced(&mut self) -> Vec<(u128, Replacement)> {
        std::mem::take(&mut self.replaced)
    }

    /// Proposes what each replacement names next, of the ranges this node leads: a change only
    /// while none is pending, since the core takes one at a time (a change it drops, with a
    /// leader stepping down, is proposed again by whichever node leads next).
    fn step_replacements(&mut self) {
        for kept in &mut self.ranges {
            let Some(replacement) = kept.replacing else {
                continue;
            };
            let Some(shell) = self.owner.get_mut(kept.handle) else {
                continue;
            };
            if !shell.is_leader() {
                continue;
            }
            let next = replacement.next(
                shell.configuration(),
                shell.caught_up(replacement.joining()),
                shell.configuration_known(),
            );
            match next {
                Next::Propose(change) => {
                    if !shell.core().raft.has_pending_conf() {
                        // A change the core refuses (its leader stepping down, its log full) is
                        // proposed again at a later turn.
                        let _ = shell.change(Vec::new(), &change);
                        self.owner.schedule(kept.handle);
                    }
                }
                Next::Wait => {}
                Next::Done => {
                    kept.replacing = None;
                    self.replaced.push((kept.group, replacement));
                }
            }
        }
    }

    /// One turn: waits until something is due, then takes everything that arrived, drives the
    /// replicas that have work, and sends what they gave.
    ///
    /// The stream is polled at a clock read before a drain of the plane's socket that emptied it,
    /// so every heartbeat the kernel stamped before that clock is fed first, and after what its
    /// write made durable (`Liveness::poll`; hyper-raft's E2E node orders it the same way).
    pub async fn turn(&mut self) -> Result<(), NodeError> {
        let own = poll_fn(|context| Poll::Ready(context.waker().will_wake(&self.waker))).await;
        if !own {
            return Err(NodeError::Task);
        }
        self.wait().await?;
        self.liveness_written()?;
        let now = hyper_rt::futures::now_ns();
        let emptied = self.drain_plane()?;
        self.take_inbox()?;
        if emptied {
            self.liveness.poll(now, &mut self.asked);
        }
        self.act_on_liveness()?;
        while let Some(event) = self.driver.endpoint().poll_event() {
            self.events.push(event);
        }
        for event in std::mem::take(&mut self.events) {
            self.on_event(event, now)?;
        }
        self.take_stored().await?;
        self.dial(now);
        if std::mem::take(&mut self.liveness_moved) {
            self.measure();
        }
        self.schedule_due(now);
        self.drive(now)?;
        self.step_replacements();
        self.send_plane();
        self.driver.flush()?;
        Ok(())
    }

    /// Each range's timing from what the stream measured, and each pair charged its ranges'
    /// election (hyper-durable's `Owner::measure`). A range whose timing moved is driven: its core
    /// draws a campaign's delay, or times a timer, only when driven, so a range given its first
    /// timing with nothing else to drive it never campaigned (its term stayed 0 on one CPU).
    fn measure(&mut self) {
        self.timings.clear();
        for kept in &self.ranges {
            let timing = self
                .owner
                .get(kept.handle)
                .and_then(|r| r.core().raft.timing());
            self.timings.push(timing);
        }
        self.owner.measure(&mut self.liveness);
        for (kept, was) in self.ranges.iter().zip(&self.timings) {
            let now = self
                .owner
                .get(kept.handle)
                .and_then(|r| r.core().raft.timing());
            if now != *was {
                self.owner.schedule(kept.handle);
            }
        }
    }

    /// Waits for the first of: the transport's next event, a datagram on the plane, the earliest
    /// deadline (a replica's, the stream's, a redial), or any other wake of the task (a log's
    /// answer). Returns at once when replicas are queued.
    ///
    /// A wait begun before the stream's wake and ended at or past it, whatever ended it, is
    /// reported to the stream as it ends: its lateness is the stream's granularity
    /// (`Liveness::on_wait`).
    async fn wait(&mut self) -> Result<(), NodeError> {
        if self.owner.has_work() {
            return Ok(());
        }
        let now = hyper_rt::futures::now_ns();
        let wake = self.liveness.wake();
        let due = [
            self.deadlines.first().map(|(at, _)| *at),
            wake,
            self.remotes.iter().filter_map(|r| r.redial_at).min(),
        ]
        .into_iter()
        .flatten()
        .min();
        // With nothing due the turn still comes at the idle timeout: every wait has a bound.
        let idle = u64::try_from(bounds::idle_timeout().as_nanos()).unwrap_or(u64::MAX);
        let sleep_ns = due.map_or(idle, |at| at.saturating_sub(now).min(idle));
        let Self {
            driver,
            plane,
            plane_socket,
            inbox,
            ..
        } = self;
        let mut woken = false;
        let any_wake = pin!(poll_fn(move |_| {
            if woken {
                Poll::Ready(())
            } else {
                woken = true;
                Poll::Pending
            }
        }));
        let receive = pin!(plane_socket.receive(plane, &AdmitAll, |arrival, opened| {
            if let Ok(opened) = opened {
                let sender = opened.sender;
                for message in opened.messages() {
                    inbox.push((sender, arrival.at_ns, message.to_vec()));
                }
            }
        }));
        let event = pin!(driver.event());
        let sleep = pin!(hyper_rt::futures::sleep(sleep_ns));
        let ended = race3(event, receive, race2(sleep, any_wake)).await;
        let woke = hyper_rt::futures::now_ns();
        if let Some(at) = wake
            && now < at
            && woke >= at
        {
            self.liveness.on_wait(at, woke);
            self.liveness_moved = true;
        }
        match ended {
            Either3::First(event) => self.events.push(event?),
            Either3::Second(received) => {
                received?;
            }
            Either3::Third(Either::First(slept)) => slept?,
            Either3::Third(Either::Second(())) => {}
        }
        Ok(())
    }

    /// What the stream's own write made durable, before the clock it is polled at. A write the
    /// log refused stops the node, as a fenced range does: a log whose device failed a write makes
    /// nothing durable again, so the node can prove no heartbeat and serve nothing; stopped, its
    /// process ends and is started again on what the device kept (node.md §6.2).
    fn liveness_written(&mut self) -> Result<(), NodeError> {
        if let Some((pending, started)) = &self.liveness_write
            && let Some(answer) = pending.poll()
        {
            let started = *started;
            self.liveness_write = None;
            answer.map_err(|e| NodeError::Log(e.to_string()))?;
            let durable = hyper_rt::futures::now_ns();
            self.liveness
                .on_durable(hyper_liveness::Write::Liveness, started, durable);
            self.liveness_moved = true;
        }
        Ok(())
    }

    /// Every datagram already on the plane's socket, opened into the inbox; whether a drain found
    /// the socket empty. A drain takes at most hyper-rt-transport's `TURNS` batches, so a second
    /// tells a socket emptied from one that still held datagrams; past that the stream waits for
    /// the next turn's drain.
    fn drain_plane(&mut self) -> Result<bool, NodeError> {
        let inbox = &mut self.inbox;
        for _ in 0..2 {
            let taken = self.plane_socket.receive_ready(
                &mut self.plane,
                &AdmitAll,
                |arrival, opened| {
                    if let Ok(opened) = opened {
                        let sender = opened.sender;
                        for message in opened.messages() {
                            inbox.push((sender, arrival.at_ns, message.to_vec()));
                        }
                    }
                },
            )?;
            if taken == 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The plane's messages: heartbeats to the stream, Raft frames to their ranges, each bound
    /// to the peer whose keys sealed it.
    fn take_inbox(&mut self) -> Result<(), NodeError> {
        for (sender, arrival, message) in std::mem::take(&mut self.inbox) {
            match message.split_first() {
                Some((&PLANE_RAFT, rest)) => self.step_frame(sender, rest)?,
                Some((first, _)) if *first == hyper_liveness::KIND => {
                    // A heartbeat the stream refuses (stale, malformed) is dropped, as the
                    // network may drop it.
                    let _ = self
                        .liveness
                        .on_heartbeat(sender, &message, arrival, &mut self.asked);
                    self.liveness_moved = true;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Steps a Raft frame from `sender` into its range, if this node hosts it and `sender` is the
    /// message's `from`: a message is bound to the peer the transport authenticated.
    fn step_frame(&mut self, sender: PeerId, bytes: &[u8]) -> Result<(), NodeError> {
        let stepped = self.step_checked(sender, bytes);
        let counter = if stepped {
            &mut self.received.stepped
        } else {
            &mut self.received.dropped
        };
        *counter = counter.saturating_add(1);
        Ok(())
    }

    /// Steps the frame if it may act: whether it was stepped.
    fn step_checked(&mut self, sender: PeerId, bytes: &[u8]) -> bool {
        let Ok((group, message)) = frame::decode(bytes) else {
            return false;
        };
        if message.from != sender || message.to != self.id {
            return false;
        }
        let Some(handle) = self.kept(group).map(|k| k.handle) else {
            return false;
        };
        // Only a member of the range's configuration may act on it (node.md §3.6); a member whose
        // configuration is still empty, one joining, learns it from its leader.
        let member = self.owner.get(handle).is_some_and(|r| {
            let c = r.configuration();
            let lists = [&c.voters, &c.learners, &c.voters_outgoing, &c.learners_next];
            lists.iter().all(|ids| ids.is_empty()) || lists.iter().any(|ids| ids.contains(&sender))
        });
        if !member {
            return false;
        }
        let Some(shell) = self.owner.get_mut(handle) else {
            return false;
        };
        // A replica waiting for room refuses a message, as the network may drop it.
        let stepped = shell.step(message).is_ok();
        self.owner.schedule(handle);
        stepped
    }

    /// Carries out what the stream asked: heartbeats onto the plane, a liveness write on the log,
    /// and its changes to the replicas.
    fn act_on_liveness(&mut self) -> Result<(), NodeError> {
        if std::mem::take(&mut self.asked.flush) && self.liveness_write.is_none() {
            let started = hyper_rt::futures::now_ns();
            let pending = self
                .log
                .write_liveness(LIVENESS_GROUP, self.waker.clone())
                .map_err(|e| NodeError::Log(e.to_string()))?;
            self.liveness_write = Some((pending, started));
        }
        for (peer, message) in std::mem::take(&mut self.asked.heartbeats) {
            // A peer with no keys yet loses the heartbeat, as the network may.
            let _ = self.plane.queue(peer, &message);
        }
        for change in std::mem::take(&mut self.asked.changes) {
            self.owner.believe(&change);
            self.liveness_moved = true;
        }
        Ok(())
    }

    fn on_event(&mut self, event: Event<NodeClasses>, now: u64) -> Result<(), NodeError> {
        match event {
            // A client's connection carries commands alone; only a node's keys the plane.
            Event::Connected {
                peer,
                epoch,
                role: Role::Node,
            } => self.key_plane(peer, epoch, now),
            Event::Connected { .. } => Ok(()),
            Event::Frame {
                peer, kind, frame, ..
            } => {
                // Stepped from the transport's own buffer, then given back.
                if kind == Kind::Raft {
                    self.step_frame(peer, frame.bytes())?;
                }
                self.driver.endpoint().release(frame);
                Ok(())
            }
            Event::LaneRoom { peer, .. } => {
                self.send_waiting(peer);
                Ok(())
            }
            Event::Request {
                exchange,
                peer,
                kind,
                body,
                ..
            } => {
                let what = match kind {
                    Kind::Snapshot => Arriving::Snapshot,
                    Kind::Command => self.take_request(exchange, Arriving::Command),
                    Kind::Read => self.take_request(exchange, Arriving::Read),
                    Kind::ChunkGet => self.take_chunk_get(exchange),
                    Kind::ChunkPut => match self.take_chunk_put(exchange, body.unwrap_or(0)) {
                        Some(what) => what,
                        // No room for its bytes now: refused before any is read, which its asker
                        // hears as this node's refusal, not its connection failing.
                        None => {
                            self.driver
                                .endpoint()
                                .refuse_exchange(exchange, Refusal::Budget);
                            return Ok(());
                        }
                    },
                    // A Raft message travels on a lane, never on an exchange.
                    Kind::Raft => {
                        self.driver
                            .endpoint()
                            .refuse_exchange(exchange, Refusal::Kind);
                        return Ok(());
                    }
                };
                let body = body.unwrap_or(0);
                let kept = if what.keeps() { body } else { 0 };
                self.receiving.push(Receiving {
                    exchange,
                    peer,
                    body,
                    read: 0,
                    bytes: Vec::with_capacity(usize::try_from(kept).unwrap_or(0)),
                    what,
                });
                self.read_exchange(exchange)
            }
            Event::BodyReady { exchange } => self.read_exchange(exchange),
            Event::Reply { exchange, body } => {
                if self.chunks_out.iter().any(|c| c.exchange == exchange) {
                    return self.chunk_replied(exchange, body.unwrap_or(0));
                }
                self.snapshot_done(exchange, true);
                self.driver.endpoint().end(exchange);
                Ok(())
            }
            Event::Refused {
                exchange, refusal, ..
            } => {
                self.snapshot_done(exchange, false);
                if let Some(at) = self.chunks_out.iter().position(|c| c.exchange == exchange) {
                    let out = self.chunks_out.swap_remove(at);
                    self.chunks_done
                        .push((out.asked, ChunkOutcome::Lost(refusal)));
                }
                self.give_back(exchange);
                self.receiving.retain(|r| r.exchange != exchange);
                self.writing.retain(|w| w.exchange != exchange);
                self.commanded.retain(|c| c.exchange != exchange);
                self.reading.retain(|r| r.exchange != exchange);
                Ok(())
            }
            Event::Writable { exchange } => {
                self.write_exchange(exchange);
                Ok(())
            }
            Event::Closed { peer, refusal, .. } | Event::Unreachable { peer, refusal } => {
                if let Some(remote) = self.remotes.iter_mut().find(|r| r.id == peer) {
                    remote.ended = remote.ended.saturating_add(1);
                    remote.refusal = refusal;
                }
                self.lost(peer, now);
                Ok(())
            }
        }
    }

    /// Keys the plane for the connection of `epoch` to `peer`: both ends derive the same secret
    /// and the same epoch number from the connection's TLS exporter, so a datagram one seals the
    /// other opens; each new connection replaces the last one's keys.
    fn key_plane(&mut self, peer: PeerId, _epoch: u64, _now: u64) -> Result<(), NodeError> {
        let mut secret = [0u8; SECRET_BYTES];
        let mut number = [0u8; 4];
        let endpoint = self.driver.endpoint();
        endpoint.export_keying_material(peer, EXPORTER_LABEL, b"", &mut secret)?;
        endpoint.export_keying_material(peer, EPOCH_LABEL, b"", &mut number)?;
        let max_datagram = endpoint
            .path(peer)
            .map(|path| usize::from(path.max_datagram));
        let role = if self.id < peer {
            hyper_datagram::Role::Initiator
        } else {
            hyper_datagram::Role::Acceptor
        };
        self.plane.remove_peer(peer);
        self.plane
            .install_epoch(
                peer,
                u32::from_le_bytes(number),
                &ExporterSecret::new(secret),
                role,
            )
            .map_err(NodeError::Plane)?;
        if let Some(max) = max_datagram {
            self.plane.set_path(peer, max).map_err(NodeError::Plane)?;
        }
        // The connection's handshake measured its path: a later redial backs off from its probe
        // timeout (RFC 6298 §5.5), not the one before any sample.
        let backoff = self
            .driver
            .endpoint()
            .path(peer)
            .map_or_else(bounds::initial_pto, |path| bounds::measured_pto(&path));
        if let Some(remote) = self.remotes.iter_mut().find(|r| r.id == peer) {
            remote.keyed = true;
            remote.redial_at = None;
            remote.backoff = backoff;
        }
        Ok(())
    }

    /// A peer's connection went: its plane keys with it, and a peer this node dials is dialed
    /// again after a pause of its backoff ([`Self::redial_later`]).
    fn lost(&mut self, peer: PeerId, now: u64) {
        let dials = self.id < peer;
        // A client's connection is its own to make again.
        let Some(remote) = self.remotes.iter_mut().find(|r| r.id == peer) else {
            return;
        };
        self.plane.remove_peer(peer);
        remote.keyed = false;
        remote.waiting.clear();
        if dials {
            Self::redial_later(self.id, remote, now);
        }
        for kept in &self.ranges {
            if let Some(shell) = self.owner.get_mut(kept.handle) {
                let _ = shell.report_unreachable(peer);
            }
        }
    }

    /// Dials each peer whose redial is due; a dial the endpoint refuses is tried again after the
    /// peer's backoff, as a lost connection is.
    fn dial(&mut self, now: u64) {
        let Ok(clock) = self.driver.clock() else {
            return;
        };
        for at in 0..self.remotes.len() {
            let Some(remote) = self.remotes.get_mut(at) else {
                continue;
            };
            if remote.redial_at.is_none_or(|due| due > now) {
                continue;
            }
            remote.redial_at = None;
            let (id, quic) = (remote.id, remote.quic);
            let Some(quic) = quic else {
                continue;
            };
            let dialed = self.driver.endpoint().connect(clock, id, quic);
            if let Some(remote) = self.remotes.get_mut(at) {
                remote.dials = remote.dials.saturating_add(1);
                if let Err(refusal) = dialed {
                    remote.ended = remote.ended.saturating_add(1);
                    remote.refusal = Some(refusal);
                    Self::redial_later(self.id, remote, now);
                }
            }
        }
    }

    /// Dials `remote` again after a pause of its backoff, and backs off further (node.md §5:
    /// doubled from the measured probe timeout, at most the idle timeout, with equal jitter).
    fn redial_later(local: u64, remote: &mut Remote, now: u64) {
        let pause = bounds::pause(remote.backoff, local, remote.redials);
        remote.redials = remote.redials.wrapping_add(1);
        let wait = u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX);
        remote.redial_at = Some(now.saturating_add(wait));
        remote.backoff = bounds::backed_off(remote.backoff);
    }

    /// Queues the replicas whose deadline has come, and those with a write out, which a log's
    /// answer may have woken.
    fn schedule_due(&mut self, now: u64) {
        while let Some(&(at, slot)) = self.deadlines.first() {
            if at > now {
                break;
            }
            self.deadlines.pop_first();
            if let Some(kept) = self.ranges.get_mut(slot) {
                kept.wake = None;
                self.owner.schedule(kept.handle);
            }
        }
        for kept in &self.ranges {
            if self
                .owner
                .get(kept.handle)
                .is_some_and(|r| r.in_flight() > 0)
            {
                self.owner.schedule(kept.handle);
            }
        }
    }

    /// A turn of the owner: each queued replica driven once, its messages sent, its flush shown
    /// to the stream, its next wake kept.
    fn drive(&mut self, now: u64) -> Result<(), NodeError> {
        let mut driven: Vec<(Handle, Driven)> = Vec::new();
        let answered_from = self.applied.len();
        let mut served: Vec<(u128, Vec<u8>)> = Vec::new();
        let mut fenced: Option<hyper_durable::ReplicaError> = None;
        let mut messages: Vec<(u128, Message)> = Vec::new();
        let groups: Vec<(Handle, u128)> = self.ranges.iter().map(|k| (k.handle, k.group)).collect();
        let applied = &mut self.applied;
        self.owner.turn(now, &mut self.out, |handle, outcome, out| {
            let group = groups
                .iter()
                .find(|(h, _)| *h == handle)
                .map_or(0, |(_, g)| *g);
            for message in out.messages.drain(..) {
                messages.push((group, message));
            }
            for answer in out.answers.drain(..) {
                applied.push((group, answer));
            }
            for (context, _) in out.reads.drain(..) {
                served.push((group, context));
            }
            match outcome {
                Ok(d) => driven.push((handle, d)),
                Err(e) => {
                    fenced.get_or_insert(e);
                }
            }
        });
        for (handle, Driven { wake, flushed, .. }) in driven {
            if let Some((started, durable)) = flushed {
                self.liveness
                    .on_durable(hyper_liveness::Write::Log, started, durable);
                self.liveness_moved = true;
            }
            if let Some(slot) = self.ranges.iter().position(|k| k.handle == handle)
                && let Some(kept) = self.ranges.get_mut(slot)
            {
                if let Some(old) = kept.wake.take() {
                    self.deadlines.remove(&(old, slot));
                }
                if let Some(at) = wake {
                    kept.wake = Some(at);
                    self.deadlines.insert((at, slot));
                }
            }
            self.owner
                .pairs(handle, &mut self.liveness, false)
                .map_err(NodeError::Liveness)?;
        }
        for (group, message) in messages {
            self.send(group, message, now)?;
        }
        // A fenced range stops the node for now: reopening it from its log or its peers, with the
        // others serving on, is the node's to build (node.md §6.2).
        if let Some(e) = fenced {
            return Err(NodeError::Range(e.into()));
        }
        if !served.is_empty() {
            self.serve_reads(served)?;
        }
        if !self.commanded.is_empty() || !self.reading.is_empty() {
            for (group, applied) in self.applied.get(answered_from..).unwrap_or_default() {
                answer_command(&mut self.commanded, *group, applied);
            }
            self.reply_commands()?;
        }
        self.compact_if_stalled(now)
    }

    /// A range whose write the log refused for room: every range compacts through what its engine
    /// made durable, freeing the log's room, and the stalled ones whose compaction had nothing to
    /// free make their writes again at their next drive (another range's compaction may have freed
    /// it). With the log out of room nothing older can be kept for a member that lags, which a
    /// snapshot then catches up; keeping entries while resending them costs less than a snapshot
    /// (node.md §2.6) needs the engine's size, which the model engine does not state.
    fn compact_if_stalled(&mut self, now: u64) -> Result<(), NodeError> {
        let any = self
            .ranges
            .iter()
            .any(|k| self.owner.get(k.handle).is_some_and(|r| r.is_stalled()));
        if !any {
            return Ok(());
        }
        for kept in &self.ranges {
            let Some(shell) = self.owner.get_mut(kept.handle) else {
                continue;
            };
            let stalled = shell.is_stalled();
            let compacted = shell
                .compact(0, now, &self.waker)
                .map_err(mantle_range::ReplicaError::from)?;
            if stalled && !compacted {
                shell.resume();
            }
            self.owner.schedule(kept.handle);
        }
        Ok(())
    }

    /// Sends a range's message by its class: control that fits a datagram on the plane, a
    /// snapshot on the bulk class, everything else on the peer's lane. The message is encoded
    /// once, behind the plane's first byte: the plane takes the whole, a lane the frame after it.
    fn send(&mut self, group: u128, message: Message, _now: u64) -> Result<(), NodeError> {
        let to = message.to;
        let Some(remote_at) = self.remotes.iter().position(|r| r.id == to) else {
            return Ok(());
        };
        let snapshot = message.snapshot.is_some() && message.msg_type == MessageType::MsgSnapshot;
        let control = message.entries.is_empty() && message.snapshot.is_none();
        self.frame.clear();
        self.frame.push(PLANE_RAFT);
        frame::encode(&mut self.frame, group, &message);
        if control && self.remotes.get(remote_at).is_some_and(|r| r.keyed) {
            let mut queued = self.plane.queue(to, &self.frame);
            // The datagram in the making is full: send it and queue this in the next.
            if queued == Err(hyper_datagram::Refusal::Busy) {
                self.send_plane();
                queued = self.plane.queue(to, &self.frame);
            }
            // Refused, or too large for the path's datagram: it takes the lane (node.md §3.4).
            if queued.is_ok() {
                self.sends.plane = self.sends.plane.saturating_add(1);
                return Ok(());
            }
        }
        if snapshot {
            return self.send_snapshot(group, to);
        }
        let lane_frame = self.frame.get(1..).unwrap_or_default();
        match self
            .driver
            .endpoint()
            .send_frame(to, LANE, Kind::Raft, lane_frame)
        {
            Ok(()) => self.sends.lane = self.sends.lane.saturating_add(1),
            Err(Refusal::LaneFull) => {
                match self.remotes.get_mut(remote_at) {
                    Some(remote) if remote.waiting.len() < self.lane_window => {
                        remote.waiting.push_back(lane_frame.to_vec());
                        self.sends.waited = self.sends.waited.saturating_add(1);
                    }
                    // Past the lane's window the frame is dropped, as the network may drop it.
                    _ => self.sends.dropped(
                        Refusal::LaneFull,
                        lane_frame.len(),
                        Some(message.msg_type),
                    ),
                }
            }
            // A peer with no connection: the message is lost, as on the network, and Raft sends
            // it again once the peer is reached.
            Err(refusal) => self
                .sends
                .dropped(refusal, lane_frame.len(), Some(message.msg_type)),
        }
        Ok(())
    }

    /// The frames the lane to `peer` had no room for, in order, while it has room.
    fn send_waiting(&mut self, peer: PeerId) {
        let Some(at) = self.remotes.iter().position(|r| r.id == peer) else {
            return;
        };
        while let Some(frame) = self.remotes.get_mut(at).and_then(|r| r.waiting.pop_front()) {
            match self
                .driver
                .endpoint()
                .send_frame(peer, LANE, Kind::Raft, &frame)
            {
                Ok(()) => self.sends.lane = self.sends.lane.saturating_add(1),
                Err(Refusal::LaneFull) => {
                    if let Some(remote) = self.remotes.get_mut(at) {
                        remote.waiting.push_front(frame);
                    }
                    break;
                }
                Err(refusal) => self.sends.dropped(refusal, frame.len(), None),
            }
        }
    }

    /// Opens a snapshot's exchange to `to`, its frame in the body, and writes what credit allows.
    fn send_snapshot(&mut self, group: u128, to: u64) -> Result<(), NodeError> {
        let frame = self.frame.get(1..).unwrap_or_default().to_vec();
        let body = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        let clock = self.driver.clock()?;
        let period = Progress::new(bounds::idle_timeout())?;
        match self
            .driver
            .endpoint()
            .open(clock, to, Kind::Snapshot, &[], Some(body), period)
        {
            Ok(exchange) => {
                self.sends.snapshots = self.sends.snapshots.saturating_add(1);
                self.writing.push(Writing {
                    exchange,
                    bytes: frame,
                    written: 0,
                    then: Then::Snapshot { group, to },
                });
                self.write_exchange(exchange);
            }
            Err(refusal) => {
                self.sends
                    .dropped(refusal, usize::try_from(body).unwrap_or(usize::MAX), None);
                self.report_snapshot(group, to, false);
            }
        }
        Ok(())
    }

    /// Writes what an exchange takes of what is left of its body; a reply written whole ends its
    /// exchange, and a snapshot waits for its member's reply.
    fn write_exchange(&mut self, exchange: ExchangeId) {
        let Some(at) = self.writing.iter().position(|w| w.exchange == exchange) else {
            return;
        };
        let endpoint = self.driver.endpoint();
        let mut failed = false;
        let mut whole = false;
        let mut then = None;
        if let Some(writing) = self.writing.get_mut(at) {
            while let Some(rest) = writing.bytes.get(writing.written..)
                && !rest.is_empty()
            {
                match endpoint.write_body(exchange, rest) {
                    Ok(0) => break,
                    Ok(took) => writing.written = writing.written.saturating_add(took),
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
            whole = writing.written >= writing.bytes.len();
            then = Some(writing.then);
        }
        match then {
            Some(Then::Snapshot { .. }) if failed => self.snapshot_done(exchange, false),
            Some(Then::Reply) if failed || whole => {
                self.writing.swap_remove(at);
                self.driver.endpoint().end(exchange);
            }
            // A put whose bytes could not all go is lost; one sent whole waits for its reply.
            Some(Then::Asked) if failed => {
                self.writing.swap_remove(at);
                if let Some(at) = self.chunks_out.iter().position(|c| c.exchange == exchange) {
                    let out = self.chunks_out.swap_remove(at);
                    self.chunks_done
                        .push((out.asked, ChunkOutcome::Lost(Refusal::Closed)));
                }
                self.driver.endpoint().end(exchange);
            }
            Some(Then::Asked) if whole => {
                self.writing.swap_remove(at);
            }
            _ => {}
        }
    }

    fn snapshot_done(&mut self, exchange: ExchangeId, arrived: bool) {
        let Some(at) = self.writing.iter().position(|w| w.exchange == exchange) else {
            return;
        };
        let Some(Then::Snapshot { group, to }) = self.writing.get(at).map(|w| w.then) else {
            return;
        };
        self.writing.swap_remove(at);
        if arrived {
            self.snapshots_sent = self.snapshots_sent.saturating_add(1);
        }
        self.report_snapshot(group, to, arrived);
    }

    fn report_snapshot(&mut self, group: u128, to: u64, arrived: bool) {
        if let Some(handle) = self.kept(group).map(|k| k.handle)
            && let Some(shell) = self.owner.get_mut(handle)
        {
            let _ = shell.report_snapshot(to, arrived);
            self.owner.schedule(handle);
        }
    }

    /// Reads what an exchange's body holds, a piece at a time from its class's budget; once it is
    /// whole, with its checksum verified, takes it: a snapshot is answered and its frame stepped,
    /// an entry is proposed, a read asked, and a request answered without its body is answered.
    fn read_exchange(&mut self, exchange: ExchangeId) -> Result<(), NodeError> {
        let Some(at) = self.receiving.iter().position(|r| r.exchange == exchange) else {
            return Ok(());
        };
        let endpoint = self.driver.endpoint();
        let Some(receiving) = self.receiving.get_mut(at) else {
            return Ok(());
        };
        while receiving.read < receiving.body {
            let Ok(mut into) = endpoint.reserve(receiving.what.class(), PIECE) else {
                return Ok(());
            };
            let got = endpoint.read_body(exchange, &mut into);
            let piece = into.bytes();
            receiving.read = receiving
                .read
                .saturating_add(u64::try_from(piece.len()).unwrap_or(u64::MAX));
            if receiving.what.keeps() {
                receiving.bytes.extend_from_slice(piece);
            }
            endpoint.release(into);
            match got {
                Ok(0) => return Ok(()),
                Ok(_) => {}
                Err(refusal) => {
                    let lost = self.receiving.swap_remove(at);
                    match lost.what {
                        Arriving::ChunkPut { .. } => self.storage.give_room(lost.body),
                        Arriving::ChunkBytes(asked) => {
                            self.chunks_done.push((asked, ChunkOutcome::Lost(refusal)));
                        }
                        _ => {}
                    }
                    return Ok(());
                }
            }
        }
        if !endpoint.body_complete(exchange) {
            // The body's checksum, after its last byte.
            let mut empty = hyper_transport::Reservation::default();
            let _ = endpoint.read_body(exchange, &mut empty);
            if !endpoint.body_complete(exchange) {
                return Ok(());
            }
        }
        let received = self.receiving.swap_remove(at);
        match received.what {
            Arriving::Snapshot => {
                // The snapshot arrived whole and verified: it is stepped whether or not its
                // sender's exchange is still there to hear that it was taken.
                match endpoint.reply(exchange, &[], Some(0)) {
                    Ok(()) => endpoint.end(exchange),
                    Err(refusal) => exchange_ended(refusal)?,
                }
                self.snapshots_taken = self.snapshots_taken.saturating_add(1);
                self.step_frame(received.peer, &received.bytes)
            }
            Arriving::Command(group) => self.propose_command(exchange, group, &received.bytes),
            Arriving::Read(group) => self.ask_read(exchange, group, &received.bytes),
            // Answered now its request was read whole: the leader as this node knows it now.
            Arriving::Redirect(group) => self.redirect(exchange, group),
            Arriving::Status(status) => self.answer_status(exchange, status, 0),
            Arriving::ChunkPut { slot, key, crc32c } => {
                let room = received.body;
                let put = self.storage.put(
                    slot,
                    key,
                    received.bytes,
                    crc32c,
                    &self.waker,
                    (Origin::Exchange(exchange), room),
                );
                match put {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        self.storage.give_room(room);
                        self.answer_chunk(exchange, chunk::Status::of_error(&error), None)
                    }
                }
            }
            Arriving::ChunkGet {
                slot,
                key,
                offset,
                len,
            } => match self
                .storage
                .get(slot, &key, offset, len, Origin::Exchange(exchange))
            {
                Ok(()) => Ok(()),
                Err(error) => self.answer_chunk(exchange, chunk::Status::of_error(&error), None),
            },
            Arriving::ChunkStatus(status) => self.answer_chunk(exchange, status, None),
            Arriving::ChunkBytes(asked) => {
                endpoint.end(exchange);
                self.chunks_done
                    .push((asked, ChunkOutcome::Read(received.bytes)));
                Ok(())
            }
        }
    }

    /// Asks a quorum to confirm the read a request's body holds, as the range's leader, and keeps
    /// it until the shell gives it back confirmed and applied through.
    fn ask_read(
        &mut self,
        exchange: ExchangeId,
        group: u128,
        body: &[u8],
    ) -> Result<(), NodeError> {
        let Ok(query) = Query::decode(body) else {
            return self.answer_status(exchange, command::Status::Malformed, 0);
        };
        let id = self.next_read;
        self.next_read = id.wrapping_add(1);
        let Some((handle, layer)) = self.kept(group).map(|k| (k.handle, k.layer)) else {
            return self.answer_status(exchange, command::Status::NoRange, 0);
        };
        if query.layer() != layer {
            return self.answer_status(exchange, command::Status::WrongLayer, self.id);
        }
        let asked = self
            .owner
            .get_mut(handle)
            .map(|shell| shell.read(id.to_le_bytes().to_vec()));
        match asked {
            Some(Ok(())) => {
                self.owner.schedule(handle);
                self.reading.push(Reading {
                    id,
                    exchange,
                    group,
                    query,
                });
                Ok(())
            }
            _ => {
                let leader = self.leader_of(group);
                if leader == self.id {
                    self.answer_status(exchange, command::Status::Refused, leader)
                } else {
                    self.redirect(exchange, group)
                }
            }
        }
    }

    /// Serves the reads the shell gave back, each confirmed and applied through: the query read
    /// from the range's engine now, and replied.
    fn serve_reads(&mut self, served: Vec<(u128, Vec<u8>)>) -> Result<(), NodeError> {
        for (group, context) in served {
            let Ok(id) = <[u8; 8]>::try_from(context.as_slice()).map(u64::from_le_bytes) else {
                continue;
            };
            let Some(at) = self
                .reading
                .iter()
                .position(|r| r.id == id && r.group == group)
            else {
                continue;
            };
            let reading = self.reading.swap_remove(at);
            let Some(handle) = self.kept(group).map(|k| k.handle) else {
                continue;
            };
            // An answer fills at most what a reply's body carries, past its head.
            let room = NodeClasses::frame_bound(classes::Class::Request)
                .saturating_sub(u64::try_from(command::REPLY_HEAD_BYTES).unwrap_or(u64::MAX));
            let room = usize::try_from(room).unwrap_or(usize::MAX);
            let read = self
                .owner
                .get_mut(handle)
                .map(|shell| reading.query.answer(shell.machine_mut().engine_mut(), room));
            let body = match read {
                Some(Ok(read)) => read.encode().ok(),
                _ => None,
            };
            let Some(body) = body else {
                // The engine failed the read: the range is fenced at its next write.
                self.answer_status(reading.exchange, command::Status::Refused, self.id)?;
                continue;
            };
            self.reply_body(reading.exchange, body)?;
        }
        Ok(())
    }

    /// Replies `body` answered, refused `TooLarge` past the class's bound, and writes it.
    fn reply_body(&mut self, exchange: ExchangeId, body: Vec<u8>) -> Result<(), NodeError> {
        let fits = u64::try_from(body.len().saturating_add(command::REPLY_HEAD_BYTES))
            .is_ok_and(|len| len <= NodeClasses::frame_bound(classes::Class::Request));
        if !fits {
            return self.answer_status(exchange, command::Status::TooLarge, self.id);
        }
        let len = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let head = command::reply_head(command::Status::Answered, self.id, 0);
        if let Err(refusal) = self.driver.endpoint().reply(exchange, &head, Some(len)) {
            return exchange_ended(refusal);
        }
        self.writing.push(Writing {
            exchange,
            bytes: body,
            written: 0,
            then: Then::Reply,
        });
        self.write_exchange(exchange);
        Ok(())
    }

    /// A command's or a read's head, checked before its body is read: what its body is read as,
    /// `led` of the group of a range this node leads, or the answer that says why not (no such
    /// range, or where the leader is), given once the body is read whole.
    fn take_request(&mut self, exchange: ExchangeId, led: fn(u128) -> Arriving) -> Arriving {
        let endpoint = self.driver.endpoint();
        let Ok(group) = command::group_of(endpoint.head(exchange).unwrap_or_default()) else {
            return Arriving::Status(command::Status::Malformed);
        };
        let Some(replica) = self.kept(group).and_then(|k| self.owner.get(k.handle)) else {
            return Arriving::Status(command::Status::NoRange);
        };
        if !replica.is_leader() {
            return Arriving::Redirect(group);
        }
        led(group)
    }

    /// Proposes the entry a command's body holds, as the range's leader, and keeps the command
    /// until its entry applies.
    fn propose_command(
        &mut self,
        exchange: ExchangeId,
        group: u128,
        body: &[u8],
    ) -> Result<(), NodeError> {
        let Ok(entry) = mantle_meta::wire::Entry::decode(body) else {
            return self.answer_status(exchange, command::Status::Malformed, 0);
        };
        match self.propose(group, &entry) {
            Ok(()) => {
                self.commanded.push(Commanded {
                    exchange,
                    group,
                    commands: entry
                        .commands
                        .iter()
                        .map(|c| (c.session, c.serial, None))
                        .collect(),
                });
                Ok(())
            }
            Err(NodeError::Range(mantle_range::ReplicaError::EntryTooLarge { .. })) => {
                self.answer_status(exchange, command::Status::Malformed, 0)
            }
            Err(_) => {
                let leader = self.leader_of(group);
                if leader == self.id {
                    self.answer_status(exchange, command::Status::Refused, leader)
                } else {
                    self.redirect(exchange, group)
                }
            }
        }
    }

    /// The leader this node's member of `group` knows, 0 for none.
    fn leader_of(&self, group: u128) -> u64 {
        self.kept(group)
            .and_then(|k| self.owner.get(k.handle))
            .map_or(0, |r| r.leader())
    }

    /// Replies a status alone and ends the exchange.
    /// A chunk put's head, checked before its body is read: what its body is read as, or the
    /// status it is answered with once its body is read and dropped (no such volume here, a head
    /// that does not decode). `None` when the node has no room for its bytes now: refused unread.
    fn take_chunk_put(&mut self, exchange: ExchangeId, body: u64) -> Option<Arriving> {
        let head = self.driver.endpoint().head(exchange).unwrap_or_default();
        let Ok(put) = chunk::Put::decode(head) else {
            return Some(Arriving::ChunkStatus(chunk::Status::Malformed));
        };
        let Some(slot) = self.storage.holds(put.volume) else {
            return Some(Arriving::ChunkStatus(chunk::Status::NoVolume));
        };
        if !self.storage.take_room(body) {
            return None;
        }
        Some(Arriving::ChunkPut {
            slot,
            key: put.key,
            crc32c: put.crc32c,
        })
    }

    /// A chunk get's head: the volume it is asked of, or why it is answered at once.
    fn take_chunk_get(&mut self, exchange: ExchangeId) -> Arriving {
        let head = self.driver.endpoint().head(exchange).unwrap_or_default();
        let Ok(get) = chunk::Get::decode(head) else {
            return Arriving::ChunkStatus(chunk::Status::Malformed);
        };
        let Some(slot) = self.storage.holds(get.volume) else {
            return Arriving::ChunkStatus(chunk::Status::NoVolume);
        };
        // Its reply carries the bytes asked, which the transfer class's bound must hold.
        if get.len > self.chunk_bytes {
            return Arriving::ChunkStatus(chunk::Status::TooLarge);
        }
        Arriving::ChunkGet {
            slot,
            key: get.key,
            offset: get.offset,
            len: get.len,
        }
    }

    /// Answers a chunk's request: its status, and a get's bytes behind it.
    fn answer_chunk(
        &mut self,
        exchange: ExchangeId,
        status: chunk::Status,
        body: Option<Vec<u8>>,
    ) -> Result<(), NodeError> {
        let head = chunk::reply_head(status);
        let endpoint = self.driver.endpoint();
        let Some(bytes) = body else {
            return match endpoint.reply(exchange, &head, None) {
                Ok(()) => {
                    endpoint.end(exchange);
                    Ok(())
                }
                Err(refusal) => exchange_ended(refusal),
            };
        };
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if let Err(refusal) = endpoint.reply(exchange, &head, Some(len)) {
            return exchange_ended(refusal);
        }
        self.writing.push(Writing {
            exchange,
            bytes,
            written: 0,
            then: Then::Reply,
        });
        self.write_exchange(exchange);
        Ok(())
    }

    /// Gives back the room a chunk put being read on `exchange` held, the exchange gone.
    fn give_back(&mut self, exchange: ExchangeId) {
        for receiving in self.receiving.iter().filter(|r| r.exchange == exchange) {
            if matches!(receiving.what, Arriving::ChunkPut { .. }) {
                self.storage.give_room(receiving.body);
            }
        }
    }

    /// The puts and gets the storage answered since the last turn: a peer's answered on its
    /// exchange, this node's own kept for [`Self::take_chunks`]. Registers this task to be woken
    /// when a get still out is answered.
    async fn take_stored(&mut self) -> Result<(), NodeError> {
        if self.storage.out() == 0 {
            return Ok(());
        }
        let mut stored = std::mem::take(&mut self.stored);
        poll_fn(|context| {
            self.storage.poll_done(context, &mut stored);
            Poll::Ready(())
        })
        .await;
        for done in stored.drain(..) {
            match done {
                Done::Put(Origin::Exchange(exchange), Ok(())) => {
                    self.answer_chunk(exchange, chunk::Status::Stored, None)?;
                }
                Done::Get(Origin::Exchange(exchange), Ok(bytes)) => {
                    self.answer_chunk(exchange, chunk::Status::Read, Some(bytes))?;
                }
                Done::Put(Origin::Exchange(exchange), Err(error))
                | Done::Get(Origin::Exchange(exchange), Err(error)) => {
                    self.answer_chunk(exchange, chunk::Status::of_error(&error), None)?;
                }
                Done::Put(Origin::Asked(asked), answered) => {
                    let outcome = match answered {
                        Ok(()) => ChunkOutcome::Stored,
                        Err(error) => ChunkOutcome::Refused(chunk::Status::of_error(&error)),
                    };
                    self.chunks_done.push((asked, outcome));
                }
                Done::Get(Origin::Asked(asked), answered) => {
                    let outcome = match answered {
                        Ok(bytes) => ChunkOutcome::Read(bytes),
                        Err(error) => ChunkOutcome::Refused(chunk::Status::of_error(&error)),
                    };
                    self.chunks_done.push((asked, outcome));
                }
            }
        }
        self.stored = stored;
        Ok(())
    }

    /// A peer's reply to a chunk put or get this node asked: a put's status, or a get's bytes,
    /// read through the transfer class's budget as a body is.
    fn chunk_replied(&mut self, exchange: ExchangeId, body: u64) -> Result<(), NodeError> {
        let Some(at) = self.chunks_out.iter().position(|c| c.exchange == exchange) else {
            return Ok(());
        };
        let ChunkOut {
            asked, peer, get, ..
        } = self.chunks_out.swap_remove(at);
        let endpoint = self.driver.endpoint();
        let status = chunk::read_reply_head(endpoint.head(exchange).unwrap_or_default())
            .unwrap_or(chunk::Status::Malformed);
        match status {
            chunk::Status::Read if get => {
                self.receiving.push(Receiving {
                    exchange,
                    peer,
                    body,
                    read: 0,
                    bytes: Vec::with_capacity(usize::try_from(body).unwrap_or(0)),
                    what: Arriving::ChunkBytes(asked),
                });
                self.read_exchange(exchange)
            }
            chunk::Status::Stored if !get => {
                endpoint.end(exchange);
                self.chunks_done.push((asked, ChunkOutcome::Stored));
                Ok(())
            }
            status => {
                endpoint.end(exchange);
                self.chunks_done
                    .push((asked, ChunkOutcome::Refused(status)));
                Ok(())
            }
        }
    }

    /// Puts `bytes`, `key`'s whole chunk with the CRC-32C its writer computed, on `volume`, which
    /// `at` holds: this node's own volume in-process (node.md §5.5), or a peer's on the transfer
    /// class. Its outcome comes from [`Self::take_chunks`] under the number returned, at once when
    /// it is refused before it goes (no such volume here, the node's asks all out).
    pub fn put_chunk(
        &mut self,
        at: PeerId,
        volume: u128,
        key: ChunkKey,
        bytes: Vec<u8>,
        crc32c: u32,
    ) -> Result<ChunkAsk, NodeError> {
        let asked = self.next_chunk;
        self.next_chunk = asked.wrapping_add(1);
        if at == self.id {
            let outcome = match self.storage.holds(volume) {
                None => Some(ChunkOutcome::Refused(chunk::Status::NoVolume)),
                Some(slot) => self
                    .storage
                    .put(
                        slot,
                        key,
                        bytes,
                        crc32c,
                        &self.waker,
                        (Origin::Asked(asked), 0),
                    )
                    .err()
                    .map(|e| ChunkOutcome::Refused(chunk::Status::of_error(&e))),
            };
            if let Some(outcome) = outcome {
                self.chunks_done.push((asked, outcome));
            }
            return Ok(asked);
        }
        if self.chunks_out.len() >= self.chunk_asks {
            self.chunks_done
                .push((asked, ChunkOutcome::Refused(chunk::Status::Busy)));
            return Ok(asked);
        }
        let head = chunk::Put {
            volume,
            key,
            crc32c,
        }
        .encode();
        let body = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let clock = self.driver.clock()?;
        let period = Progress::new(bounds::idle_timeout())?;
        match self
            .driver
            .endpoint()
            .open(clock, at, Kind::ChunkPut, &head, Some(body), period)
        {
            Ok(exchange) => {
                self.chunks_out.push(ChunkOut {
                    exchange,
                    asked,
                    peer: at,
                    get: false,
                });
                self.writing.push(Writing {
                    exchange,
                    bytes,
                    written: 0,
                    then: Then::Asked,
                });
                self.write_exchange(exchange);
            }
            Err(refusal) => self.chunks_done.push((asked, ChunkOutcome::Lost(refusal))),
        }
        Ok(asked)
    }

    /// Gets bytes `[offset, offset + len)` of `key`'s chunk from `volume`, which `at` holds, as
    /// [`Self::put_chunk`] puts one.
    pub fn get_chunk(
        &mut self,
        at: PeerId,
        volume: u128,
        key: ChunkKey,
        offset: u64,
        len: u64,
    ) -> Result<ChunkAsk, NodeError> {
        let asked = self.next_chunk;
        self.next_chunk = asked.wrapping_add(1);
        if at == self.id {
            let outcome = match self.storage.holds(volume) {
                None => Some(ChunkOutcome::Refused(chunk::Status::NoVolume)),
                Some(slot) => self
                    .storage
                    .get(slot, &key, offset, len, Origin::Asked(asked))
                    .err()
                    .map(|e| ChunkOutcome::Refused(chunk::Status::of_error(&e))),
            };
            if let Some(outcome) = outcome {
                self.chunks_done.push((asked, outcome));
            }
            return Ok(asked);
        }
        if self.chunks_out.len() >= self.chunk_asks {
            self.chunks_done
                .push((asked, ChunkOutcome::Refused(chunk::Status::Busy)));
            return Ok(asked);
        }
        let head = chunk::Get {
            volume,
            key,
            offset,
            len,
        }
        .encode();
        let clock = self.driver.clock()?;
        let period = Progress::new(bounds::idle_timeout())?;
        match self
            .driver
            .endpoint()
            .open(clock, at, Kind::ChunkGet, &head, None, period)
        {
            Ok(exchange) => self.chunks_out.push(ChunkOut {
                exchange,
                asked,
                peer: at,
                get: true,
            }),
            Err(refusal) => self.chunks_done.push((asked, ChunkOutcome::Lost(refusal))),
        }
        Ok(asked)
    }

    /// The chunk puts and gets this node asked that were answered since the last call, each
    /// under the number it was asked under.
    pub fn take_chunks(&mut self) -> std::vec::Drain<'_, (ChunkAsk, ChunkOutcome)> {
        self.chunks_done.drain(..)
    }

    /// The volumes the node holds: each one's id and its device directory.
    pub fn volumes(&self) -> impl Iterator<Item = (u128, &Path)> {
        self.storage.volumes()
    }

    fn answer_status(
        &mut self,
        exchange: ExchangeId,
        status: command::Status,
        leader: u64,
    ) -> Result<(), NodeError> {
        let endpoint = self.driver.endpoint();
        match endpoint.reply(exchange, &command::reply_head(status, leader, 0), None) {
            Ok(()) => {
                endpoint.end(exchange);
                Ok(())
            }
            Err(refusal) => exchange_ended(refusal),
        }
    }

    /// Replies that this node does not lead `group`'s range: the leader it knows, and how long
    /// until it acts on the leader's silence, by which the range has elected, so a client that
    /// learns of no leader asks again by then.
    fn redirect(&mut self, exchange: ExchangeId, group: u128) -> Result<(), NodeError> {
        let leader = self.leader_of(group);
        let patience_ns = self
            .patience(group)
            .map_or(0, |p| u64::try_from(p.as_nanos()).unwrap_or(u64::MAX));
        let endpoint = self.driver.endpoint();
        let head = command::reply_head(command::Status::Redirect, leader, patience_ns);
        match endpoint.reply(exchange, &head, None) {
            Ok(()) => {
                endpoint.end(exchange);
                Ok(())
            }
            Err(refusal) => exchange_ended(refusal),
        }
    }

    /// Replies to each command whose every answer came, and sends a command whose range this
    /// node no longer leads to the leader it knows: its entry may yet apply, and asking again
    /// returns the answers its sessions keep.
    fn reply_commands(&mut self) -> Result<(), NodeError> {
        let mut at = 0;
        while let Some(c) = self.commanded.get(at) {
            let answered = c.commands.iter().all(|(_, _, a)| a.is_some());
            let led = self
                .kept(c.group)
                .and_then(|k| self.owner.get(k.handle))
                .is_some_and(|r| r.is_leader());
            if !answered && led {
                at = at.saturating_add(1);
                continue;
            }
            let c = self.commanded.swap_remove(at);
            if !answered {
                self.redirect(c.exchange, c.group)?;
                continue;
            }
            let answers: Vec<Answer> = c.commands.into_iter().filter_map(|(_, _, a)| a).collect();
            let mut body = Vec::new();
            if command::encode_answers(&mut body, &answers).is_err() {
                self.answer_status(c.exchange, command::Status::TooLarge, self.id)?;
                continue;
            }
            self.reply_body(c.exchange, body)?;
        }
        // A read whose range this node no longer leads may never be confirmed here.
        let mut at = 0;
        while let Some(r) = self.reading.get(at) {
            let led = self
                .kept(r.group)
                .and_then(|k| self.owner.get(k.handle))
                .is_some_and(|r| r.is_leader());
            if led {
                at = at.saturating_add(1);
                continue;
            }
            let r = self.reading.swap_remove(at);
            self.redirect(r.exchange, r.group)?;
        }
        Ok(())
    }

    /// Seals what the plane queued and sends it, each peer's datagram to its plane's address.
    fn send_plane(&mut self) {
        let remotes = &self.remotes;
        self.plane_socket.flush(
            &mut self.plane,
            |peer| remotes.iter().find(|r| r.id == peer).and_then(|r| r.plane),
            |_, _| {},
        );
    }
}

/// An answer a range applied, given to the command waiting for it, if one is: the first whose
/// command of that session and serial has no answer yet.
fn answer_command(commanded: &mut [Commanded], group: u128, applied: &Applied) {
    let waiting = |(session, serial, answer): &(u64, u64, Option<Answer>)| {
        *session == applied.session && *serial == applied.serial && answer.is_none()
    };
    if let Some(slot) = commanded
        .iter_mut()
        .filter(|c| c.group == group)
        .find_map(|c| c.commands.iter_mut().find(|command| waiting(command)))
    {
        slot.2 = Some(applied.answer.clone());
    }
}

/// The node's log, on a file in its directory: created empty for a new node, which sizes it as
/// it writes.
fn open_log(dir: &Path, node: u64, config: LogConfig) -> Result<Log<DeviceFile>, NodeError> {
    let path = dir.join("log");
    let fresh = !path.exists();
    let file = DeviceFile::open(
        &path,
        fresh,
        CachingRequest::PreferDirect,
        Alignment::new(4096).map_err(|e| NodeError::Log(format!("{e:?}")))?,
    )
    .map_err(|e| NodeError::Log(e.to_string()))?;
    if fresh {
        Log::create(file, config, log_id(node)).map_err(|e| NodeError::Log(e.to_string()))
    } else {
        Log::open(file, config, log_id(node))
            .map(|(log, _)| log)
            .map_err(|e| NodeError::Log(e.to_string()))
    }
}
