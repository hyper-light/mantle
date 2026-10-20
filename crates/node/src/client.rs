//! A client of a cell's ranges (docs/design/node.md §5.1): it asks a range's leader to apply an
//! entry (`crate::command`) or to read (`crate::read`) over the request class and waits for its
//! answer, following the leader a node names. One command is out at a time; the caller states each command's deadline, by which
//! it is answered or refused (node.md §5.4), and that deadline ends every wait and retry.
//!
//! A node that names no leader, its range electing, is asked again after RFC 6298 §5.5's
//! doubling backoff from QUIC's initial probe timeout (`crate::bounds::initial_pto`); a node the
//! client cannot reach, or whose exchange was refused, is passed over for the next it knows.
//!
//! **How long an exchange is given** (hyper-transport's progress period): by hyper-timing's law for
//! a round (`RoundBudget::derive`), a node nothing is known about is never cut off before the
//! caller's deadline; once the client has measured its exchanges, one is given the slowest it
//! measured, or the path's persistent-congestion duration if that is longer (RFC 9002 §7.6.1:
//! three probe timeouts with no acknowledgement, each with the peer's `max_ack_delay`, which an
//! exchange's period must exceed), never past the deadline. A node that died
//! holding a command's exchange is then left within that, not after the connection's idle
//! timeout. A
//! command whose exchange ended without its reply may have been applied: asking it again with
//! the same sessions and serials is answered from what the range's sessions keep, never applied
//! twice (mantle_meta's sessions, replica.md §1).

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
use mantle_meta::wire::{Answer, Entry};

use crate::bounds::{self, Stated};
use crate::classes::{self, Bounds, Kind, NodeClasses, Role};
use crate::command::{self, CommandError, ReplyHead, Status};
use crate::directory::{Peer, Peers};
use crate::read::{Query, Read, ReadError};

/// What a client is given.
pub struct ClientSettings {
    pub id: PeerId,
    /// The address its endpoint binds.
    pub bind: SocketAddr,
    pub credentials: Credentials,
    /// The nodes it may ask, by certificate.
    pub nodes: Vec<Peer>,
    /// The largest message of each class, as the cell's nodes state them (`classes::Bounds`).
    pub messages: Bounds,
    /// The bytes its transport may hold at once.
    pub transport_bytes: u64,
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

/// A node as the client keeps it.
struct Known {
    id: PeerId,
    quic: Option<SocketAddr>,
    connected: bool,
    /// The epoch of the newest connection to it that came up: the transport counts each peer's
    /// connections, so one dialed since has a newer epoch.
    epoch: Epoch,
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
    body: Vec<u8>,
}

impl Client {
    /// Opens the client: binds its endpoint. Nodes are dialed as commands need them.
    pub async fn open(settings: ClientSettings) -> Result<Self, ClientError> {
        classes::state_bounds(settings.messages).map_err(|e| ClientError::Bounds(e.to_string()))?;
        let peers = settings.nodes.len();
        let stated = Stated {
            peers,
            // A client keeps one command out, to one range at a time.
            ranges: 1,
            inflight: 0,
            transport_bytes: settings.transport_bytes,
            messages: settings.messages,
        };
        let limits = bounds::limits(&stated).map_err(|e| ClientError::Bounds(e.to_string()))?;
        let io = bounds::io(&stated);
        let tls = Tls::mutual(&settings.credentials, false)?;
        let config = Config {
            tls,
            role: Role::Client,
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
            })
            .collect();
        let endpoint = Endpoint::new(
            config,
            budget,
            Peers::new(settings.nodes),
            std::time::Instant::now(),
        )?;
        let driver = Driver::bind(endpoint, settings.bind, io)?;
        Ok(Self {
            id: settings.id,
            driver,
            nodes,
            leaders: BTreeMap::new(),
            slowest_ns: None,
            waits: 0,
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
        let reply = self.request(Kind::Command, group, deadline_ns).await?;
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
        let reply = self.request(Kind::Read, group, deadline_ns).await?;
        Ok(Read::decode(query, &reply)?)
    }

    /// Asks the request of `kind` whose body the client holds of `group`'s leader, following the
    /// leaders the nodes name (the module's rules): the answered reply's body.
    async fn request(
        &mut self,
        kind: Kind,
        group: u128,
        deadline_ns: u64,
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
            match self.ask(node, kind, group, deadline_ns).await? {
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
        deadline_ns: u64,
    ) -> Result<Asked, ClientError> {
        let clock = self.driver.clock()?;
        let opened_ns = hyper_rt::futures::now_ns();
        let period = Progress::new(Duration::from_nanos(self.period_ns(
            node,
            deadline_ns,
            opened_ns,
        )))?;
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
                    let head = self.driver.endpoint().head(exchange).unwrap_or_default();
                    let read = command::read_reply_head(head)?;
                    reply = Some(read);
                    if read.status != Status::Answered {
                        self.driver.endpoint().end(exchange);
                        return finish(read);
                    }
                    if let Some(done) = self.read_reply(exchange, &mut bytes)? {
                        return Ok(Asked::Answered(done));
                    }
                }
                Event::BodyReady { exchange: e } if e == exchange && reply.is_some() => {
                    if let Some(done) = self.read_reply(exchange, &mut bytes)? {
                        return Ok(Asked::Answered(done));
                    }
                }
                Event::Refused { exchange: e, .. } if e == exchange => {
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
    /// path's persistent-congestion duration, whichever is longer, within that time.
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
        slowest.max(congested).min(left)
    }

    /// Writes what the exchange takes of the command's body.
    fn write(&mut self, exchange: ExchangeId, written: &mut usize) -> Result<(), ClientError> {
        let endpoint = self.driver.endpoint();
        while let Some(rest) = self.body.get(*written..)
            && !rest.is_empty()
        {
            match endpoint.write_body(exchange, rest)? {
                0 => break,
                took => *written = written.saturating_add(took),
            }
        }
        Ok(())
    }

    /// Reads what the reply's body holds; once it is whole and its checksum verified, its answers.
    fn read_reply(
        &mut self,
        exchange: ExchangeId,
        into: &mut Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        let endpoint = self.driver.endpoint();
        loop {
            let mut piece = endpoint.reserve(classes::Class::Request, PIECE)?;
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
            Event::Closed { peer, .. } | Event::Unreachable { peer, .. } => {
                let peer = *peer;
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
        status => Err(ClientError::Status(status)),
    }
}
