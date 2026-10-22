//! A client of a cell's ranges (docs/design/node.md §5.1): it asks a range's leader to apply an
//! entry (`crate::command`) or to read (`crate::read`) over the request class and waits for its
//! answer, following the leader a node names. One command is out at a time; the caller states each command's deadline, by which
//! it is answered or refused (node.md §5.4), and that deadline ends every wait and retry.
//!
//! A node that names no leader, its range electing, is asked again after RFC 6298 §5.5's
//! doubling backoff from QUIC's initial probe timeout (`crate::bounds::initial_pto`); a node the
//! client cannot reach, or whose exchange was refused, is passed over for the next it knows.
//! An object's request, asked of the node its caller names, is asked again of it after the same
//! pause when the node cannot have taken it: its connection refused at the node's admission, for
//! the identity's connections all in use (a client of the same identity that ended, its close
//! lost, holds them until they idle), or its exchange given no stream.
//!
//! **How long an exchange is given** (hyper-transport's progress period): by hyper-timing's law for
//! a round (`RoundBudget::derive`), a node nothing is known about is never cut off before the
//! caller's deadline; once the client has measured its exchanges, one is given the slowest it
//! measured, or the path's persistent-congestion duration if that is longer (RFC 9002 §7.6.1:
//! three probe timeouts with no acknowledgement, each with the peer's `max_ack_delay`, which an
//! exchange's period must exceed), never past the deadline. A node that died
//! holding a command's exchange is then left within that, not after the connection's idle
//! timeout. An exchange cut by its period is no measure (it never replied), so each one cut doubles
//! the period of the next exchange with that node until that node replies (RFC 6298 §5.5's
//! backed-off timer, which only a new measurement of the same path collapses, §5): a leader
//! slower than every exchange measured, its writes or its device under load, is asked again a
//! bounded number of times, not at the first measure's pace for ever, and a redirect from another
//! node, which measures nothing of the leader, does not set its period back. A command whose
//! exchange ended without its reply may have been applied: asking it again with the same sessions
//! and serials is answered from what the range's sessions keep, never applied twice (mantle_meta's
//! sessions, replica.md §1). A command no session holds, a registration, would apply again (the
//! Raft dissertation §6.3: a registration asked twice registers twice): its exchange is never cut
//! by a period, only by its connection's loss, as a live node answers every command, applied or,
//! once it no longer leads, with the leader it knows.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::pin::pin;
use std::time::Duration;

use hyper_rt::combine::{Either, race2};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{Credentials, Tls};
use hyper_transport::{
    Config, Endpoint, Epoch, Event, ExchangeId, Fixed, PeerId, Progress, Refusal,
};
use mantle_chunk::ChunkKey;
use mantle_meta::wire::{Answer, Entry};

use crate::bounds::{self, Stated};
use crate::chunk;
use crate::classes::{self, Bounds, Kind, NodeClasses, Role};
use crate::command::{self, CommandError, ReplyHead, Status};
use crate::directory::{Peer, Peers};
use crate::node::ChunkOutcome;
use crate::object::{self, Op};
use crate::read::{Query, Read, ReadError};

/// What a client is given.
pub struct ClientSettings {
    pub id: PeerId,
    /// The address its endpoint binds.
    pub bind: SocketAddr,
    pub credentials: Credentials,
    /// The nodes it may ask, by certificate.
    pub nodes: Vec<Peer>,
    /// What it is to the nodes, as their configurations name its certificate: a client of
    /// mantle's protocol, or a gateway, which also moves chunks (node.md §3.6).
    pub role: Role,
    /// The largest message of each class, as the cell's nodes state them (`classes::Bounds`).
    pub messages: Bounds,
    /// The bytes its transport may hold at once.
    pub transport_bytes: u64,
    /// The seed of its QUIC endpoint's draws (`Endpoint::new_seeded`), so a simulation's run is its
    /// seed (node.md §9.1); `None` elsewhere, the endpoint drawing its own.
    pub seed: Option<[u8; 32]>,
}

/// Why a command was not answered.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("the transport: {0:?}")]
    Transport(Refusal),
    #[error("the runtime: {0}")]
    Runtime(String),
    #[error("the client's bounds: {0}")]
    Bounds(String),
    #[error("a command's message: {0}")]
    Command(#[from] CommandError),
    #[error("the entry: {0}")]
    Entry(#[from] mantle_meta::record::RecordError),
    #[error("a read's message: {0}")]
    Read(#[from] ReadError),
    #[error("the range answered {0:?}")]
    Status(Status),
    #[error("the deadline passed before the command was answered")]
    Deadline,
    #[error("the client knows no node")]
    NoNodes,
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

/// What the client's asks of nodes came to, from its opening: each exchange it opened, by how
/// it ended. A cell whose nodes and paths are well loses none, and a node that does not lead
/// answers each ask with the leader it knows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Asks {
    /// Answered: a command's answers, or a read's.
    pub answered: u64,
    /// Answered with the leader the node knows, or with none known.
    pub redirected: u64,
    /// Refused by the leader for room, its uncommitted bound or its log full.
    pub refused: u64,
    /// Ended with no answer: cut by its period, refused by the transport, or its connection
    /// lost.
    pub lost: u64,
}

/// A node as the client keeps it.
struct Known {
    id: PeerId,
    quic: Option<SocketAddr>,
    connected: bool,
    /// The epoch of the newest connection to it that came up: the transport counts each peer's
    /// connections, so one dialed since has a newer epoch.
    epoch: Epoch,
    /// Exchanges with it in a row cut by their period with no reply from it since: each doubles
    /// the next one's period (RFC 6298 §5.5), and its reply, a new measurement of it, sets it back
    /// (§5).
    stalled: u32,
    /// Why its last connection or dial ended, where it said.
    refused: Option<Refusal>,
}

/// RFC 9002 §7.6.1's `kPersistentCongestionThreshold`: probe timeouts with no acknowledgement
/// after which a path is taken as failed.
const PERSISTENT_CONGESTION_THRESHOLD: u32 = 3;

/// The piece a reply's body is read in: the transport's own receive chunk.
const PIECE: u64 = hyper_transport::RECEIVE_CHUNK as u64;

/// A client of the cell's ranges, on the current hyper-rt shard.
pub struct Client {
    /// The client's own id, as the nodes' directories name it.
    id: u64,
    driver: Driver<NodeClasses, Fixed, Peers>,
    nodes: Vec<Known>,
    /// The leader each range was last found at, for the ranges this client asked.
    leaders: BTreeMap<u128, PeerId>,
    /// The slowest exchange measured, from its opening to its reply, nanoseconds.
    slowest_ns: Option<u64>,
    /// Waits paused for so far: the draw each pause's jitter takes (`bounds::pause`).
    waits: u32,
    asks: Asks,
    body: Vec<u8>,
}

impl Client {
    /// Opens the client: binds its endpoint. Nodes are dialed as commands need them.
    pub async fn open(settings: ClientSettings) -> Result<Self, ClientError> {
        classes::state_bounds(settings.messages).map_err(|e| ClientError::Bounds(e.to_string()))?;
        let peers = settings.nodes.len();
        let stated = Stated {
            peers,
            // A client keeps one command out, to one range at a time, and a gateway one chunk.
            ranges: 1,
            inflight: 0,
            transfers: usize::from(settings.role == Role::Gateway),
            transport_bytes: settings.transport_bytes,
            messages: settings.messages,
        };
        let limits = bounds::limits(&stated).map_err(|e| ClientError::Bounds(e.to_string()))?;
        let io = bounds::io(&stated);
        let tls = Tls::mutual(&settings.credentials, false)?;
        let config = Config {
            tls,
            role: settings.role,
            limits,
            congestion: Default::default(),
        };
        let budget = Fixed::new(settings.transport_bytes, peers.max(1));
        let nodes = settings
            .nodes
            .iter()
            .map(|n| Known {
                id: n.id,
                quic: None,
                connected: false,
                epoch: 0,
                stalled: 0,
                refused: None,
            })
            .collect();
        let (directory, now) = (Peers::new(settings.nodes), std::time::Instant::now());
        let endpoint = match settings.seed {
            Some(seed) => Endpoint::new_seeded(config, budget, directory, now, seed)?,
            None => Endpoint::new(config, budget, directory, now)?,
        };
        let driver = Driver::bind(endpoint, settings.bind, io)?;
        Ok(Self {
            id: settings.id,
            driver,
            nodes,
            leaders: BTreeMap::new(),
            slowest_ns: None,
            waits: 0,
            asks: Asks::default(),
            body: Vec::new(),
        })
    }

    /// Where node `id`'s QUIC endpoint listens.
    pub fn learn(&mut self, id: PeerId, quic: SocketAddr) -> Result<(), ClientError> {
        let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) else {
            return Ok(());
        };
        let moved = node.quic.is_some_and(|known| known != quic);
        node.quic = Some(quic);
        if moved {
            // The node started again elsewhere: its connection is to a process that is gone,
            // closed now so the next ask dials where it is, since the transport dials no peer it
            // holds a connection to.
            node.connected = false;
            let clock = self.driver.clock()?;
            self.driver.endpoint().disconnect(clock, id);
        }
        Ok(())
    }

    /// Asks `group`'s leader to apply `entry` and returns its answers, one for each command in
    /// the entry's order; refused once `deadline_ns`, on the shard's clock, has passed.
    pub async fn command(
        &mut self,
        group: u128,
        entry: &Entry,
        deadline_ns: u64,
    ) -> Result<Vec<Answer>, ClientError> {
        self.body.clear();
        self.body.extend_from_slice(&entry.encode()?);
        // A command no session holds (a registration) applies again if asked again: its exchange is
        // never cut by a period, only by its connection's loss (the module's rules).
        let guarded = entry.commands.iter().all(|c| c.session != 0);
        let reply = self
            .request(Kind::Command, group, (deadline_ns, guarded))
            .await?;
        Ok(command::decode_answers(&reply)?)
    }

    /// Reads `query` of `group`'s leader, confirmed by a quorum (crate::read): the answer sees
    /// every write committed before it was asked; refused once `deadline_ns` has passed.
    pub async fn read(
        &mut self,
        group: u128,
        query: &Query,
        deadline_ns: u64,
    ) -> Result<Read, ClientError> {
        self.body.clear();
        self.body.extend_from_slice(&query.encode());
        let reply = self.request(Kind::Read, group, (deadline_ns, true)).await?;
        Ok(Read::decode(query, &reply)?)
    }

    /// Asks the request of `kind` whose body the client holds of `group`'s leader, following the
    /// leaders the nodes name (the module's rules), by `deadline_ns`; `guarded` when asking it
    /// again cannot apply it twice. The answered reply's body.
    async fn request(
        &mut self,
        kind: Kind,
        group: u128,
        (deadline_ns, guarded): (u64, bool),
    ) -> Result<Vec<u8>, ClientError> {
        let mut target = self.leaders.get(&group).copied();
        // RFC 6298 §5.5: the wait doubles from the current retransmission timeout, the client's
        // measured probe timeout once a path is measured.
        let mut backoff = self.measured_pto();
        // Nodes asked since the last wait: once every node was tried and none answered or named a
        // leader, the client waits before it asks again: its backoff, and no longer than the
        // longest patience a node that named no leader stated, by when its range has elected.
        let mut tried = 0usize;
        let mut stated: Option<Duration> = None;
        // The nodes this command lost an exchange with: one named as the leader since is taken as
        // naming none, since the node naming it has not yet learned what this client found.
        let mut lost: Vec<PeerId> = Vec::new();
        loop {
            if tried >= self.nodes.len() {
                let delay = stated.map_or(backoff, |patience| patience.min(backoff));
                let pause = bounds::pause(delay, self.id, self.waits);
                self.waits = self.waits.wrapping_add(1);
                let wait = u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX);
                self.wait_until(
                    hyper_rt::futures::now_ns().saturating_add(wait),
                    deadline_ns,
                )
                .await?;
                backoff = bounds::backed_off(backoff);
                tried = 0;
                stated = None;
            }
            let node = match target {
                Some(node) => node,
                None => self.next_node(None).ok_or(ClientError::NoNodes)?,
            };
            tried = tried.saturating_add(1);
            if !self.reach(node, deadline_ns).await? {
                target = self.next_node(Some(node));
                continue;
            }
            let asked = self.ask(node, kind, group, (deadline_ns, guarded)).await?;
            let count = match asked {
                Asked::Answered(_) => &mut self.asks.answered,
                Asked::Leader(..) => &mut self.asks.redirected,
                Asked::Refused => &mut self.asks.refused,
                Asked::Lost => &mut self.asks.lost,
            };
            *count = count.saturating_add(1);
            match asked {
                Asked::Answered(reply) => {
                    self.leaders.insert(group, node);
                    return Ok(reply);
                }
                Asked::Leader(leader, _)
                    if leader != 0 && leader != node && !lost.contains(&leader) =>
                {
                    target = Some(leader);
                }
                // No node knows a leader yet: the next is asked, and once every node was, the
                // client waits.
                Asked::Leader(_, patience) => {
                    if !patience.is_zero() {
                        stated = Some(stated.map_or(patience, |s| s.max(patience)));
                    }
                    target = self.next_node(Some(node));
                }
                // The leader refused it for room, its uncommitted bound or its log full: nothing
                // was applied, and it is asked again of the same leader after the client's
                // pause, which that room takes to free.
                Asked::Refused => {
                    target = Some(node);
                    tried = self.nodes.len();
                }
                Asked::Lost => {
                    // At most every node the client knows, each once.
                    if !lost.contains(&node) {
                        lost.push(node);
                    }
                    target = self.next_node(Some(node));
                }
            }
        }
    }

    /// The node after `after` in the client's list, round the list; the first when `after` is
    /// none or unknown.
    fn next_node(&self, after: Option<PeerId>) -> Option<PeerId> {
        let at = after
            .and_then(|a| self.nodes.iter().position(|n| n.id == a))
            .map_or(0, |at| at.saturating_add(1));
        self.nodes
            .get(at)
            .or_else(|| self.nodes.first())
            .map(|n| n.id)
    }

    /// Dials `node` if it has no connection and waits for it, as long as the client gives an
    /// exchange with it (the module's law): whether it was reached. A dial that takes longer leaves
    /// the node unreached this round and goes on, and a later round finds it up: a dial to a node
    /// that is gone ends only at its handshake's idle timeout, which a request asked of the nodes
    /// that answer never waits out.
    async fn reach(&mut self, node: PeerId, deadline_ns: u64) -> Result<bool, ClientError> {
        let Some(known) = self.nodes.iter().find(|n| n.id == node) else {
            return Ok(false);
        };
        if known.connected {
            return Ok(true);
        }
        let Some(quic) = known.quic else {
            return Ok(false);
        };
        // The close of a connection no newer than this is of one before the dial: the one
        // `learn` closed, not the dial's.
        let before = known.epoch;
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

    /// Asks `node` once: its answers, the leader it names, or the exchange lost.
    async fn ask(
        &mut self,
        node: PeerId,
        kind: Kind,
        group: u128,
        (deadline_ns, guarded): (u64, bool),
    ) -> Result<Asked, ClientError> {
        let clock = self.driver.clock()?;
        let opened_ns = hyper_rt::futures::now_ns();
        let period_ns = if guarded {
            self.period_ns(node, deadline_ns, opened_ns)
        } else {
            deadline_ns.saturating_sub(opened_ns).max(1)
        };
        let period = Progress::new(Duration::from_nanos(period_ns))?;
        let len = u64::try_from(self.body.len()).unwrap_or(u64::MAX);
        let exchange = match self.driver.endpoint().open(
            clock,
            node,
            kind,
            &command::head(group),
            Some(len),
            period,
        ) {
            Ok(exchange) => exchange,
            Err(Refusal::NotConnected) => {
                self.lost(node);
                return Ok(Asked::Lost);
            }
            Err(refusal) => return Err(refusal.into()),
        };
        let mut written = 0usize;
        self.write(exchange, &mut written)?;
        let mut reply: Option<ReplyHead> = None;
        let mut bytes = Vec::new();
        loop {
            let Some(event) = self.next_event(deadline_ns).await? else {
                self.driver.endpoint().end(exchange);
                return Err(ClientError::Deadline);
            };
            match event {
                Event::Writable { exchange: e } if e == exchange => {
                    self.write(exchange, &mut written)?;
                }
                Event::Reply { exchange: e, .. } if e == exchange => {
                    let took = hyper_rt::futures::now_ns().saturating_sub(opened_ns);
                    self.slowest_ns = Some(self.slowest_ns.map_or(took, |s| s.max(took)));
                    if let Some(known) = self.nodes.iter_mut().find(|n| n.id == node) {
                        known.stalled = 0;
                    }
                    let head = self.driver.endpoint().head(exchange).unwrap_or_default();
                    let read = command::read_reply_head(head)?;
                    reply = Some(read);
                    if read.status != Status::Answered {
                        self.driver.endpoint().end(exchange);
                        return finish(read);
                    }
                    if let Some(done) =
                        self.read_reply(exchange, classes::Class::Request, &mut bytes)?
                    {
                        return Ok(Asked::Answered(done));
                    }
                }
                Event::BodyReady { exchange: e } if e == exchange && reply.is_some() => {
                    if let Some(done) =
                        self.read_reply(exchange, classes::Class::Request, &mut bytes)?
                    {
                        return Ok(Asked::Answered(done));
                    }
                }
                Event::Refused {
                    exchange: e,
                    refusal,
                    ..
                } if e == exchange => {
                    if refusal == Refusal::Stalled
                        && let Some(known) = self.nodes.iter_mut().find(|n| n.id == node)
                    {
                        known.stalled = known.stalled.saturating_add(1);
                    }
                    return Ok(Asked::Lost);
                }
                Event::Closed { peer, .. } | Event::Unreachable { peer, .. } => {
                    self.lost(peer);
                    if peer == node {
                        return Ok(Asked::Lost);
                    }
                }
                _ => {}
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

    /// What its asks came to so far.
    pub fn asks(&self) -> Asks {
        self.asks
    }

    /// The longest the client may wait now without asking a node anything, by the module's law,
    /// for a caller with no deadline of its own: an exchange with any node it knows, or a dial,
    /// given its period before the node is taken as lost, and a pause between rounds, at most the
    /// idle timeout (`bounds::backed_off`). None until an exchange is measured, before which an
    /// exchange is given all the time its caller left.
    pub fn patience(&mut self) -> Option<Duration> {
        self.slowest_ns?;
        let now = hyper_rt::futures::now_ns();
        let mut most = 0u64;
        for at in 0..self.nodes.len() {
            if let Some(id) = self.nodes.get(at).map(|n| n.id) {
                most = most.max(self.period_ns(id, u64::MAX, now));
            }
        }
        Some(Duration::from_nanos(most).max(bounds::idle_timeout()))
    }

    /// The progress period an exchange with `node` is given (the module's law): the time left
    /// before `deadline_ns` while nothing is measured, else the slowest exchange measured or the
    /// path's persistent-congestion duration, whichever is longer, doubled for each exchange with
    /// the node cut by its period since its last reply, within that time.
    fn period_ns(&mut self, node: PeerId, deadline_ns: u64, now_ns: u64) -> u64 {
        let left = deadline_ns.saturating_sub(now_ns).max(1);
        let Some(slowest) = self.slowest_ns else {
            return left;
        };
        // hyper-quic's probe timeout adds the delay its ACK frequency asks for, which may be
        // shorter than the peer's `max_ack_delay` that RFC 9002 §7.6.1 adds.
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

    /// Writes what the exchange takes of the command's body.
    fn write(&mut self, exchange: ExchangeId, written: &mut usize) -> Result<(), ClientError> {
        write_body(self.driver.endpoint(), exchange, &self.body, written)
    }

    /// Puts `bytes`, `key`'s whole chunk with the CRC-32C its writer computed, on `volume`, which
    /// `node` holds, as a gateway does (node.md §5.2): `Stored`, the status the node refused it
    /// with, or why the exchange was lost, after which the chunk may still have landed. One
    /// exchange at a time, as commands are; a client whose role is no gateway's is refused.
    pub async fn put_chunk(
        &mut self,
        node: PeerId,
        (volume, key): (u128, ChunkKey),
        bytes: &[u8],
        crc32c: u32,
        deadline_ns: u64,
    ) -> Result<ChunkOutcome, ClientError> {
        let head = chunk::Put {
            volume,
            key,
            crc32c,
        }
        .encode();
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let replied = self
            .transfer(node, Kind::ChunkPut, (&head, bytes, Some(len)), deadline_ns)
            .await?;
        Ok(match replied {
            Ok((chunk::Status::Stored, _)) => ChunkOutcome::Stored,
            Ok((status, _)) => ChunkOutcome::Refused(status),
            Err(refusal) => ChunkOutcome::Lost(refusal),
        })
    }

    /// Gets bytes `[offset, offset + len)` of `key`'s chunk from `volume`, which `node` holds:
    /// the bytes the volume verified, the status the node refused it with, or why the exchange
    /// was lost.
    pub async fn get_chunk(
        &mut self,
        node: PeerId,
        (volume, key): (u128, ChunkKey),
        (offset, len): (u64, u64),
        deadline_ns: u64,
    ) -> Result<ChunkOutcome, ClientError> {
        let head = chunk::Get {
            volume,
            key,
            offset,
            len,
        }
        .encode();
        let replied = self
            .transfer(node, Kind::ChunkGet, (&head, &[], None), deadline_ns)
            .await?;
        Ok(match replied {
            Ok((chunk::Status::Read, bytes)) => ChunkOutcome::Read(bytes),
            Ok((status, _)) => ChunkOutcome::Refused(status),
            Err(refusal) => ChunkOutcome::Lost(refusal),
        })
    }

    /// Asks `node` an object's request (`crate::object`): a bucket made, `data` put as the object's
    /// bytes, or a range got. The reply, with a get's bytes, or the refusal that ended the exchange
    /// unanswered, after which a put may still have committed (asking it again with the same
    /// identity is answered from what it made, gateway.md §2.1).
    ///
    /// A request `node` cannot have taken is asked again, after the client's pause, by
    /// `deadline_ns`: its connection refused at the node's admission, which takes a connection
    /// before any stream on it (hyper-transport), as a node does an identity whose connections are
    /// all in use, a client of the same identity that ended and whose close was lost holding them
    /// until they idle; or its exchange given no stream (`Refusal::Unsent`).
    pub async fn object(
        &mut self,
        node: PeerId,
        op: &Op,
        data: &[u8],
        deadline_ns: u64,
    ) -> Result<ObjectOutcome, ClientError> {
        // RFC 6298 §5.5, as `request` waits.
        let mut backoff = self.measured_pto();
        loop {
            match self.object_once(node, op, data, deadline_ns).await? {
                ObjectOutcome::Lost(refusal) if untaken(refusal) => {
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
                outcome => return Ok(outcome),
            }
        }
    }

    /// One object exchange with `node`, as [`Self::object`] asks it.
    async fn object_once(
        &mut self,
        node: PeerId,
        op: &Op,
        data: &[u8],
        deadline_ns: u64,
    ) -> Result<ObjectOutcome, ClientError> {
        let (head, names) = op
            .encode()
            .map_err(|e| ClientError::Bounds(e.to_string()))?;
        if !self.reach(node, deadline_ns).await? {
            let refused = self
                .nodes
                .iter()
                .find(|n| n.id == node)
                .and_then(|n| n.refused);
            return Ok(ObjectOutcome::Lost(
                refused.unwrap_or(Refusal::NotConnected),
            ));
        }
        let clock = self.driver.clock()?;
        let now_ns = hyper_rt::futures::now_ns();
        let period = Progress::new(Duration::from_nanos(self.period_ns(
            node,
            deadline_ns,
            now_ns,
        )))?;
        let exchange = match self.driver.endpoint().open(
            clock,
            node,
            Kind::Object,
            &head,
            Some(op.body()),
            period,
        ) {
            Ok(exchange) => exchange,
            Err(Refusal::NotConnected) => {
                self.lost(node);
                return Ok(ObjectOutcome::Lost(Refusal::NotConnected));
            }
            Err(refusal) => return Err(refusal.into()),
        };
        // The names, then the object's bytes, as the stream takes them.
        let mut written = 0usize;
        let write = |endpoint: &mut Endpoint<NodeClasses, Fixed, Peers>,
                     written: &mut usize|
         -> Result<(), ClientError> {
            for (from, part) in [(0usize, names.as_slice()), (names.len(), data)] {
                let end = from.saturating_add(part.len());
                if *written >= end {
                    continue;
                }
                let mut at = written.saturating_sub(from);
                write_body(endpoint, exchange, part, &mut at)?;
                *written = from.saturating_add(at);
                // The stream took no more for now: the rest goes once it is writable.
                if *written < end {
                    break;
                }
            }
            Ok(())
        };
        write(self.driver.endpoint(), &mut written)?;
        let mut replied: Option<object::ReplyHead> = None;
        let mut bytes = Vec::new();
        loop {
            let Some(event) = self.next_event(deadline_ns).await? else {
                self.driver.endpoint().end(exchange);
                return Err(ClientError::Deadline);
            };
            match event {
                Event::Writable { exchange: e } if e == exchange => {
                    write(self.driver.endpoint(), &mut written)?;
                }
                Event::Reply { exchange: e, .. } if e == exchange => {
                    let head = self.driver.endpoint().head(exchange).unwrap_or_default();
                    replied = Some(
                        object::ReplyHead::decode(head)
                            .map_err(|e| ClientError::Bounds(e.to_string()))?,
                    );
                    if let Some(done) =
                        self.read_reply(exchange, classes::Class::Transfer, &mut bytes)?
                    {
                        return reply_of(replied, done);
                    }
                }
                Event::BodyReady { exchange: e } if e == exchange && replied.is_some() => {
                    if let Some(done) =
                        self.read_reply(exchange, classes::Class::Transfer, &mut bytes)?
                    {
                        return reply_of(replied, done);
                    }
                }
                // Its connection closed: the transport queues why with the exchange's refusal
                // (`Event::Closed`), which says whether the node took the request at all.
                Event::Refused {
                    exchange: e,
                    refusal: Refusal::Closed,
                    ..
                } if e == exchange => {
                    while let Some(event) = self.driver.endpoint().poll_event() {
                        if let Event::Closed { peer, refusal, .. }
                        | Event::Unreachable { peer, refusal } = self.keep(event)
                            && peer == node
                        {
                            return Ok(ObjectOutcome::Lost(refusal.unwrap_or(Refusal::Closed)));
                        }
                    }
                    return Ok(ObjectOutcome::Lost(Refusal::Closed));
                }
                Event::Refused {
                    exchange: e,
                    refusal,
                    ..
                } if e == exchange => return Ok(ObjectOutcome::Lost(refusal)),
                Event::Closed { peer, refusal, .. } | Event::Unreachable { peer, refusal } => {
                    self.lost(peer);
                    if peer == node {
                        return Ok(ObjectOutcome::Lost(refusal.unwrap_or(Refusal::Closed)));
                    }
                }
                _ => {}
            }
        }
    }

    /// One chunk exchange with `node`: its reply's status and body, or the refusal that ended it
    /// unanswered (`NotConnected` for a node not reached).
    async fn transfer(
        &mut self,
        node: PeerId,
        kind: Kind,
        (head, body, len): (&[u8], &[u8], Option<u64>),
        deadline_ns: u64,
    ) -> Result<Result<(chunk::Status, Vec<u8>), Refusal>, ClientError> {
        if !self.reach(node, deadline_ns).await? {
            return Ok(Err(Refusal::NotConnected));
        }
        let clock = self.driver.clock()?;
        let now_ns = hyper_rt::futures::now_ns();
        let period = Progress::new(Duration::from_nanos(self.period_ns(
            node,
            deadline_ns,
            now_ns,
        )))?;
        let exchange = match self
            .driver
            .endpoint()
            .open(clock, node, kind, head, len, period)
        {
            Ok(exchange) => exchange,
            Err(Refusal::NotConnected) => {
                self.lost(node);
                return Ok(Err(Refusal::NotConnected));
            }
            Err(refusal) => return Err(refusal.into()),
        };
        let mut written = 0usize;
        write_body(self.driver.endpoint(), exchange, body, &mut written)?;
        let mut status = None;
        let mut bytes = Vec::new();
        loop {
            let Some(event) = self.next_event(deadline_ns).await? else {
                self.driver.endpoint().end(exchange);
                return Err(ClientError::Deadline);
            };
            match event {
                Event::Writable { exchange: e } if e == exchange => {
                    write_body(self.driver.endpoint(), exchange, body, &mut written)?;
                }
                Event::Reply { exchange: e, .. } if e == exchange => {
                    let head = self.driver.endpoint().head(exchange).unwrap_or_default();
                    let replied = chunk::read_reply_head(head).unwrap_or(chunk::Status::Malformed);
                    if replied != chunk::Status::Read {
                        self.driver.endpoint().end(exchange);
                        return Ok(Ok((replied, Vec::new())));
                    }
                    status = Some(replied);
                    if let Some(done) =
                        self.read_reply(exchange, classes::Class::Transfer, &mut bytes)?
                    {
                        return Ok(Ok((replied, done)));
                    }
                }
                Event::BodyReady { exchange: e } if e == exchange => {
                    if let Some(replied) = status
                        && let Some(done) =
                            self.read_reply(exchange, classes::Class::Transfer, &mut bytes)?
                    {
                        return Ok(Ok((replied, done)));
                    }
                }
                Event::Refused {
                    exchange: e,
                    refusal,
                    ..
                } if e == exchange => return Ok(Err(refusal)),
                Event::Closed { peer, .. } | Event::Unreachable { peer, .. } => {
                    self.lost(peer);
                    if peer == node {
                        return Ok(Err(Refusal::Closed));
                    }
                }
                _ => {}
            }
        }
    }

    /// Reads what the reply's body holds, a piece at a time from `class`'s budget; once it is
    /// whole and its checksum verified, the body.
    fn read_reply(
        &mut self,
        exchange: ExchangeId,
        class: classes::Class,
        into: &mut Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let endpoint = self.driver.endpoint();
        loop {
            let mut piece = endpoint.reserve(class, PIECE)?;
            let got = endpoint.read_body(exchange, &mut piece);
            into.extend_from_slice(piece.bytes());
            endpoint.release(piece);
            if got? == 0 {
                break;
            }
        }
        if !endpoint.body_complete(exchange) {
            return Ok(None);
        }
        endpoint.end(exchange);
        Ok(Some(std::mem::take(into)))
    }

    fn lost(&mut self, peer: PeerId) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == peer) {
            node.connected = false;
        }
        self.leaders.retain(|_, leader| *leader != peer);
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
    /// The client's connections close as it goes, each node told by QUIC's CONNECTION_CLOSE, so
    /// a node frees what they held at once rather than after their idle timeout. A node admits a
    /// bounded number of connections an identity, and replaces one only once it has been idle
    /// for the serve period: a client started again under the same identity while its old
    /// connections were held was refused (`Refusal::Busy`) until they were. A client whose
    /// process ends without dropping it leaves them to the idle timeout. Best effort: a clock
    /// the shard cannot read, or a send the OS refuses, leaves them so too.
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

/// What one ask of a node came to.
enum Asked {
    /// The answered reply's body.
    Answered(Vec<u8>),
    /// The node leads nothing for the range: the leader it knows, 0 for none, and how long
    /// until it acts on the leader's silence (zero while not yet measured).
    Leader(u64, Duration),
    /// The leader refused the request for room; nothing was applied.
    Refused,
    /// The exchange or its connection ended without a reply.
    Lost,
}

/// A reply's head that is not an answer: the leader it names, or the status as the error.
fn finish(head: ReplyHead) -> Result<Asked, ClientError> {
    match head.status {
        Status::Redirect => Ok(Asked::Leader(
            head.leader,
            Duration::from_nanos(head.patience_ns),
        )),
        Status::Refused => Ok(Asked::Refused),
        status => Err(ClientError::Status(status)),
    }
}

/// What became of an object's request ([`Client::object`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectOutcome {
    /// The node's reply, and a get's bytes.
    Replied {
        reply: object::Reply,
        bytes: Vec<u8>,
    },
    /// The exchange ended unanswered: its node is unreachable or refused it, or its connection
    /// closed.
    Lost(Refusal),
}

/// Whether a request lost with `refusal` was never taken by its node: its connection refused at
/// the node's admission for the node's capacity, which comes before any stream on it is taken
/// (hyper-transport's `Endpoint`), or its exchange given no stream (`Refusal::Unsent`). A
/// certificate refused (`Identity`) is no capacity's, and asking again changes nothing.
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

/// A reply whose body was read whole: its tags taken from the body's start, the rest a get's bytes.
fn reply_of(
    head: Option<object::ReplyHead>,
    mut body: Vec<u8>,
) -> Result<ObjectOutcome, ClientError> {
    let head = head.ok_or(ClientError::Bounds(
        "an object's body before its head".into(),
    ))?;
    let tags = usize::try_from(head.tags()).unwrap_or(usize::MAX);
    if tags > body.len() {
        return Err(ClientError::Bounds(
            "an object's reply shorter than its tags".into(),
        ));
    }
    let bytes = body.split_off(tags);
    let reply = head
        .reply(&body)
        .map_err(|e| ClientError::Bounds(e.to_string()))?;
    Ok(ObjectOutcome::Replied { reply, bytes })
}

/// Writes what `exchange` takes of `body` past `written`.
fn write_body(
    endpoint: &mut Endpoint<NodeClasses, Fixed, Peers>,
    exchange: ExchangeId,
    body: &[u8],
    written: &mut usize,
) -> Result<(), ClientError> {
    while let Some(rest) = body.get(*written..)
        && !rest.is_empty()
    {
        match endpoint.write_body(exchange, rest)? {
            0 => break,
            took => *written = written.saturating_add(took),
        }
    }
    Ok(())
}
