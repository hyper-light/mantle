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

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_datagram::{AdmitAll, EXPORTER_LABEL, ExporterSecret, Plane, SECRET_BYTES};
use hyper_durable::{Driven, Handle, Owner, Unbounded};
use hyper_liveness::{Change, Liveness};
use hyper_log::{Class as LogClass, Config as LogConfig, Log, Pending, Update};
use hyper_raft::proto::{Message, MessageType};
use hyper_rt::combine::{Either, Either3, race2, race3};
use hyper_rt_transport::{Driver, PlaneSocket};
use hyper_timing::Exposure;
use hyper_transport::tls::{Credentials, Tls};
use hyper_transport::{Config, Endpoint, Event, ExchangeId, Fixed, PeerId, Progress, Refusal};
use mantle_meta::engine::Model;
use mantle_range::{Applied, Elections, GroupStore, Range, RangeMachine};

use crate::bounds::{self, Stated};
use crate::classes::{self, Bounds, Kind, NodeClasses, Role};
use crate::directory::{Peer, Peers};
use crate::frame;
use crate::layout::Layout;

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
pub struct NodeSettings {
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
    pub ranges: Vec<Hosted>,
    pub log: LogConfig,
    /// The bytes the transport may hold at once.
    pub transport_bytes: u64,
}

/// A range the node hosts: its log group, the range, and its engine as it starts: a range's
/// founding state, or what the node's engine holds durably.
#[derive(Debug, Clone)]
pub struct Hosted {
    pub group: u128,
    pub range: Range,
    pub engine: Model,
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

type Store = GroupStore<DeviceFile>;
type Machine = RangeMachine<Model>;

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
    /// How long the node may go without moving while its peers live, by its own law: the longest
    /// any peer's evidence may go unmoved before it is suspected (`PairReport::freshness`), then
    /// the range's expected election from a suspicion and one more round (hyper-raft's `Timing`);
    /// `None` until both are measured.
    pub patience: Option<Duration>,
}

/// A range as the node keeps it.
struct Kept {
    group: u128,
    handle: Handle,
    max_entry_bytes: u64,
    /// The replica's latest wake, held in [`Node::deadlines`].
    wake: Option<u64>,
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
    /// The redial's backoff: RFC 6298 §5.5's doubling, from the initial probe timeout.
    backoff: Duration,
}

/// A snapshot out on its exchange: to whom, for which range, and its frame with how much of it the
/// exchange has taken (`write_body` takes what credit allows; `Event::Writable` says when more).
struct Sending {
    exchange: ExchangeId,
    group: u128,
    to: u64,
    frame: Vec<u8>,
    written: usize,
}

/// A snapshot arriving on an exchange: from whom, the body's length, and what was read of it.
struct Receiving {
    exchange: ExchangeId,
    peer: PeerId,
    body: u64,
    bytes: Vec<u8>,
}

/// The piece a body is read in: the transport's own receive chunk.
const PIECE: u64 = hyper_transport::RECEIVE_CHUNK as u64;

/// One shard's ranges and the transport they replicate over.
pub struct Node {
    id: u64,
    log: Log<DeviceFile>,
    owner: Owner<Store, Machine, Unbounded>,
    ranges: Vec<Kept>,
    deadlines: BTreeSet<(u64, usize)>,
    liveness: Liveness,
    asked: Asked,
    liveness_write: Option<(Pending, u64)>,
    liveness_failed: bool,
    liveness_moved: bool,
    driver: Driver<NodeClasses, Fixed, Peers>,
    plane: Plane,
    plane_socket: PlaneSocket,
    remotes: Vec<Remote>,
    lane_window: usize,
    sending: Vec<Sending>,
    receiving: Vec<Receiving>,
    /// Plane messages taken this turn: sender, the kernel's arrival stamp, the bytes.
    inbox: Vec<(PeerId, u64, Vec<u8>)>,
    events: Vec<Event<NodeClasses>>,
    out: hyper_durable::Output<Applied>,
    frame: Vec<u8>,
    applied: Vec<(u128, Applied)>,
    waker: Waker,
}

/// The node's message bounds: a Raft message is at most the larger of a page of entries and one
/// entry of the range's largest, inside the core's fixed bytes, in a frame; a command is an entry;
/// a snapshot's data is at most what hyper-raft's wire states, a u32 of bytes.
fn message_bounds(ranges: &[Hosted]) -> Result<Bounds, NodeError> {
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
    Ok(Bounds {
        replication: largest,
        request: entry,
        bulk,
    })
}

impl Node {
    /// Opens the node on the current hyper-rt shard: raises its run, opens its log and its ranges,
    /// binds its QUIC endpoint and its plane, and dials each peer whose id is above its own. The
    /// task that opens it is the one that turns it: its waker is the one the log answers through.
    pub async fn open(settings: NodeSettings) -> Result<Self, NodeError> {
        let waker = poll_fn(|context| Poll::Ready(context.waker().clone())).await;
        let layout = Layout::new(&settings.dir);
        let run = layout.raise_run()?;
        let log = open_log(&settings.dir, settings.id, settings.log)?;
        let messages = message_bounds(&settings.ranges)?;
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
            transport_bytes: settings.transport_bytes,
            messages,
        };
        let limits = bounds::limits(&stated).map_err(|e| NodeError::Bounds(e.to_string()))?;
        let lane_window = limits.lane_window;
        let io = bounds::io(&stated);
        let plane_limits = bounds::plane(&stated).map_err(|e| NodeError::Bounds(e.to_string()))?;

        let mut owner = Owner::new(vec![waker.clone(); settings.ranges.len()]);
        let mut ranges = Vec::with_capacity(settings.ranges.len());
        for hosted in &settings.ranges {
            if hosted.group == 0 || hosted.group == LIVENESS_GROUP {
                return Err(NodeError::Reserved(hosted.group));
            }
            let store = mantle_range::claim(&log, hosted.group, &hosted.range)?;
            let shell = mantle_range::open_shell(
                settings.id,
                store,
                hosted.engine.clone(),
                &hosted.range,
                settings.id,
                Elections::Suspicion,
            )?;
            let handle = owner.insert(shell).map_err(|_| NodeError::Full)?;
            owner.schedule(handle);
            ranges.push(Kept {
                group: hosted.group,
                handle,
                max_entry_bytes: hosted.range.settings.max_entry_bytes,
                wake: None,
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
        let endpoint = Endpoint::new(config, budget, directory, std::time::Instant::now())?;
        let driver = Driver::bind(endpoint, settings.quic, io)?;
        let plane_socket = PlaneSocket::bind(settings.plane, io)?;
        let plane = Plane::new(settings.id, plane_limits).map_err(NodeError::Plane)?;
        let remotes = settings
            .peers
            .iter()
            .map(|p| Remote {
                id: p.id,
                quic: None,
                plane: None,
                waiting: VecDeque::new(),
                keyed: false,
                redial_at: None,
                backoff: bounds::initial_pto(),
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
            liveness_failed: false,
            liveness_moved: true,
            driver,
            plane,
            plane_socket,
            remotes,
            lane_window,
            sending: Vec::new(),
            receiving: Vec::new(),
            inbox: Vec::new(),
            events: Vec::new(),
            out: hyper_durable::Output::default(),
            frame: Vec::new(),
            applied: Vec::new(),
            waker,
        };
        for kept in &node.ranges {
            node.owner
                .pairs(kept.handle, &mut node.liveness, false)
                .map_err(NodeError::Liveness)?;
        }
        for (peer, quic, plane) in settings.addresses {
            node.learn(peer, quic, plane);
        }
        Ok(node)
    }

    /// Where `peer` listens: QUIC's address and the plane's. A peer this node dials (its id is
    /// above this node's) is dialed at the next turn.
    pub fn learn(&mut self, peer: PeerId, quic: SocketAddr, plane: SocketAddr) {
        let dials = self.id < peer;
        let now = hyper_rt::futures::now_ns();
        if let Some(remote) = self.remotes.iter_mut().find(|r| r.id == peer) {
            remote.quic = Some(quic);
            remote.plane = Some(plane);
            if dials && !remote.keyed {
                remote.redial_at = Some(now);
            }
        }
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
        let mut heartbeats = 0u64;
        let mut judged = true;
        let mut evidence: Option<Duration> = None;
        for report in self
            .remotes
            .iter()
            .filter_map(|r| self.liveness.report(r.id))
        {
            heartbeats = heartbeats.saturating_add(report.taken);
            judged &= report.judged;
            if let Some(fresh) = report.freshness {
                evidence = Some(evidence.map_or(fresh, |e| e.max(fresh)));
            }
        }
        let patience = evidence
            .zip(replica.core().raft.timing())
            .map(|(e, t)| e.saturating_add(t.election).saturating_add(t.round));
        Some(Status {
            term: replica.term(),
            leader: replica.leader(),
            commit: replica.logged_commit(),
            applied: replica.applied().index,
            heartbeats,
            judged,
            patience,
        })
    }

    /// What the node's ranges answered as they applied, since the last call.
    pub fn take_applied(&mut self) -> Vec<(u128, Applied)> {
        std::mem::take(&mut self.applied)
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
        self.liveness_written();
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
        self.dial(now);
        if std::mem::take(&mut self.liveness_moved) {
            self.owner.measure(&mut self.liveness);
        }
        self.schedule_due(now);
        self.drive(now)?;
        self.send_plane();
        self.driver.flush()?;
        Ok(())
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

    /// What the stream's own write made durable, before the clock it is polled at.
    fn liveness_written(&mut self) {
        if let Some((pending, started)) = &self.liveness_write
            && let Some(answer) = pending.poll()
        {
            let started = *started;
            self.liveness_write = None;
            match answer {
                Ok(()) => {
                    let durable = hyper_rt::futures::now_ns();
                    self.liveness
                        .on_durable(hyper_liveness::Write::Liveness, started, durable);
                    self.liveness_moved = true;
                }
                // The device failed: no heartbeat is proved again, so the peers suspect the node.
                Err(_) => self.liveness_failed = true,
            }
        }
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
        let Ok((group, message)) = frame::decode(bytes) else {
            return Ok(());
        };
        if message.from != sender || message.to != self.id {
            return Ok(());
        }
        let Some(handle) = self.kept(group).map(|k| k.handle) else {
            return Ok(());
        };
        if let Some(shell) = self.owner.get_mut(handle) {
            // A replica waiting for room refuses a message, as the network may drop it.
            let _ = shell.step(message);
            self.owner.schedule(handle);
        }
        Ok(())
    }

    /// Carries out what the stream asked: heartbeats onto the plane, a liveness write on the log,
    /// and its changes to the replicas.
    fn act_on_liveness(&mut self) -> Result<(), NodeError> {
        if std::mem::take(&mut self.asked.flush)
            && self.liveness_write.is_none()
            && !self.liveness_failed
        {
            let started = hyper_rt::futures::now_ns();
            match self.log.submit_waking(
                LIVENESS_GROUP,
                LogClass::Latency,
                Update::default(),
                self.waker.clone(),
            ) {
                Ok(pending) => self.liveness_write = Some((pending, started)),
                Err(_) => self.liveness_failed = true,
            }
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
            Event::Connected { peer, epoch, .. } => self.key_plane(peer, epoch, now),
            Event::Frame {
                peer, kind, frame, ..
            } => {
                if kind == Kind::Raft {
                    let bytes = frame.bytes().to_vec();
                    self.driver.endpoint().release(frame);
                    self.step_frame(peer, &bytes)?;
                } else {
                    self.driver.endpoint().release(frame);
                }
                Ok(())
            }
            Event::LaneRoom { peer, .. } => {
                self.send_waiting(peer);
                Ok(())
            }
            Event::Request {
                exchange,
                peer,
                kind: Kind::Snapshot,
                body,
                ..
            } => {
                let body = body.unwrap_or(0);
                self.receiving.push(Receiving {
                    exchange,
                    peer,
                    body,
                    bytes: Vec::with_capacity(usize::try_from(body).unwrap_or(0)),
                });
                self.read_snapshot(exchange)
            }
            Event::Request { exchange, .. } => {
                // Commands come with the request path (node.md §4); none is taken yet.
                self.driver
                    .endpoint()
                    .refuse_exchange(exchange, Refusal::Kind);
                Ok(())
            }
            Event::BodyReady { exchange } => self.read_snapshot(exchange),
            Event::Reply { exchange, .. } => {
                self.snapshot_done(exchange, true);
                self.driver.endpoint().end(exchange);
                Ok(())
            }
            Event::Refused { exchange, .. } => {
                self.snapshot_done(exchange, false);
                Ok(())
            }
            Event::Writable { exchange } => {
                self.write_snapshot(exchange);
                Ok(())
            }
            Event::Closed { peer, .. } | Event::Unreachable { peer, .. } => {
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
        if let Some(remote) = self.remotes.iter_mut().find(|r| r.id == peer) {
            remote.keyed = true;
            remote.redial_at = None;
            remote.backoff = bounds::initial_pto();
        }
        Ok(())
    }

    /// A peer's connection went: its plane keys with it, and a peer this node dials is dialed
    /// again after its backoff (RFC 6298 §5.5: doubled each time, from the initial probe timeout).
    fn lost(&mut self, peer: PeerId, now: u64) {
        self.plane.remove_peer(peer);
        let dials = self.id < peer;
        if let Some(remote) = self.remotes.iter_mut().find(|r| r.id == peer) {
            remote.keyed = false;
            remote.waiting.clear();
            if dials {
                let wait = u64::try_from(remote.backoff.as_nanos()).unwrap_or(u64::MAX);
                remote.redial_at = Some(now.saturating_add(wait));
                remote.backoff = remote.backoff.saturating_mul(2);
            }
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
            if self.driver.endpoint().connect(clock, id, quic).is_err()
                && let Some(remote) = self.remotes.get_mut(at)
            {
                let wait = u64::try_from(remote.backoff.as_nanos()).unwrap_or(u64::MAX);
                remote.redial_at = Some(now.saturating_add(wait));
                remote.backoff = remote.backoff.saturating_mul(2);
            }
        }
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
            if let Ok(d) = outcome {
                driven.push((handle, d));
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
        Ok(())
    }

    /// Sends a range's message by its class: control that fits a datagram on the plane, a
    /// snapshot on the bulk class, everything else on the peer's lane.
    fn send(&mut self, group: u128, message: Message, _now: u64) -> Result<(), NodeError> {
        let to = message.to;
        let Some(remote_at) = self.remotes.iter().position(|r| r.id == to) else {
            return Ok(());
        };
        let snapshot = message.snapshot.is_some() && message.msg_type == MessageType::MsgSnapshot;
        let control = message.entries.is_empty() && message.snapshot.is_none();
        self.frame.clear();
        frame::encode(&mut self.frame, group, &message);
        if control && self.remotes.get(remote_at).is_some_and(|r| r.keyed) {
            let mut datagram = Vec::with_capacity(self.frame.len().saturating_add(1));
            datagram.push(PLANE_RAFT);
            datagram.extend_from_slice(&self.frame);
            match self.plane.queue(to, &datagram) {
                Ok(()) => return Ok(()),
                // The datagram in the making is full: send it and queue this in the next.
                Err(hyper_datagram::Refusal::Busy) => {
                    self.send_plane();
                    if self.plane.queue(to, &datagram).is_ok() {
                        return Ok(());
                    }
                }
                // Too large for the path's datagram: it takes the lane, as node.md §3.4 says.
                Err(_) => {}
            }
        }
        if snapshot {
            return self.send_snapshot(group, to);
        }
        let frame = std::mem::take(&mut self.frame);
        match self
            .driver
            .endpoint()
            .send_frame(to, LANE, Kind::Raft, &frame)
        {
            Ok(()) => {}
            Err(Refusal::LaneFull) => {
                if let Some(remote) = self.remotes.get_mut(remote_at)
                    && remote.waiting.len() < self.lane_window
                {
                    remote.waiting.push_back(frame.clone());
                }
            }
            // A peer with no connection: the message is lost, as on the network, and Raft sends
            // it again once the peer is reached.
            Err(_) => {}
        }
        self.frame = frame;
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
                Ok(()) => {}
                Err(Refusal::LaneFull) => {
                    if let Some(remote) = self.remotes.get_mut(at) {
                        remote.waiting.push_front(frame);
                    }
                    break;
                }
                Err(_) => {}
            }
        }
    }

    /// Opens a snapshot's exchange to `to`, its frame in the body, and writes what credit allows.
    fn send_snapshot(&mut self, group: u128, to: u64) -> Result<(), NodeError> {
        let frame = self.frame.clone();
        let body = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        let clock = self.driver.clock()?;
        let period = Progress::new(bounds::idle_timeout())?;
        match self
            .driver
            .endpoint()
            .open(clock, to, Kind::Snapshot, &[], Some(body), period)
        {
            Ok(exchange) => {
                self.sending.push(Sending {
                    exchange,
                    group,
                    to,
                    frame,
                    written: 0,
                });
                self.write_snapshot(exchange);
            }
            Err(_) => self.report_snapshot(group, to, false),
        }
        Ok(())
    }

    /// Writes what a snapshot's exchange takes of what is left of its frame.
    fn write_snapshot(&mut self, exchange: ExchangeId) {
        let Some(at) = self.sending.iter().position(|s| s.exchange == exchange) else {
            return;
        };
        let endpoint = self.driver.endpoint();
        let mut failed = false;
        if let Some(sending) = self.sending.get_mut(at) {
            while let Some(rest) = sending.frame.get(sending.written..)
                && !rest.is_empty()
            {
                match endpoint.write_body(exchange, rest) {
                    Ok(0) => break,
                    Ok(took) => sending.written = sending.written.saturating_add(took),
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
        }
        if failed {
            self.snapshot_done(exchange, false);
        }
    }

    fn snapshot_done(&mut self, exchange: ExchangeId, arrived: bool) {
        if let Some(at) = self.sending.iter().position(|s| s.exchange == exchange) {
            let sent = self.sending.swap_remove(at);
            self.report_snapshot(sent.group, sent.to, arrived);
        }
        if !arrived {
            self.receiving.retain(|r| r.exchange != exchange);
        }
    }

    fn report_snapshot(&mut self, group: u128, to: u64, arrived: bool) {
        if let Some(handle) = self.kept(group).map(|k| k.handle)
            && let Some(shell) = self.owner.get_mut(handle)
        {
            let _ = shell.report_snapshot(to, arrived);
            self.owner.schedule(handle);
        }
    }

    /// Reads what a snapshot's exchange holds, a piece at a time from the budget; once it is whole,
    /// answers it, steps its frame and ends the exchange.
    fn read_snapshot(&mut self, exchange: ExchangeId) -> Result<(), NodeError> {
        let Some(at) = self.receiving.iter().position(|r| r.exchange == exchange) else {
            return Ok(());
        };
        let endpoint = self.driver.endpoint();
        let Some(receiving) = self.receiving.get_mut(at) else {
            return Ok(());
        };
        while u64::try_from(receiving.bytes.len()).unwrap_or(u64::MAX) < receiving.body {
            let Ok(mut into) = endpoint.reserve(classes::Class::Bulk, PIECE) else {
                return Ok(());
            };
            let got = endpoint.read_body(exchange, &mut into);
            receiving.bytes.extend_from_slice(into.bytes());
            endpoint.release(into);
            match got {
                Ok(0) => return Ok(()),
                Ok(_) => {}
                Err(_) => {
                    self.receiving.swap_remove(at);
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
        endpoint.reply(exchange, &[], Some(0))?;
        endpoint.end(exchange);
        self.step_frame(received.peer, &received.bytes)
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
