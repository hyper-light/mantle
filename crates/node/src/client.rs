//! A client of a cell's ranges (docs/design/node.md §5.1): it asks a range's leader to apply an
//! entry over the request class (`crate::command`) and waits for its answers, following the leader
//! a node names. One command is out at a time; the caller states each command's deadline, by which
//! it is answered or refused (node.md §5.4), and that deadline ends every wait and retry.
//!
//! A node that names no leader, its range electing, is asked again after RFC 6298 §5.5's
//! doubling backoff from QUIC's initial probe timeout (`crate::bounds::initial_pto`); a node the
//! client cannot reach, or whose exchange was refused, is passed over for the next it knows. A
//! command whose exchange ended without its reply may have been applied: asking it again with
//! the same sessions and serials is answered from what the range's sessions keep, never applied
//! twice (mantle_meta's sessions, replica.md §1).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::pin::pin;

use hyper_rt::combine::{Either, race2};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{Credentials, Tls};
use hyper_transport::{Config, Endpoint, Event, ExchangeId, Fixed, PeerId, Progress, Refusal};
use mantle_meta::wire::{Answer, Entry};

use crate::bounds::{self, Stated};
use crate::classes::{self, Bounds, Kind, NodeClasses, Role};
use crate::command::{self, CommandError, Status};
use crate::directory::{Peer, Peers};

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
}

/// The piece a reply's body is read in: the transport's own receive chunk.
const PIECE: u64 = hyper_transport::RECEIVE_CHUNK as u64;

/// A client of the cell's ranges, on the current hyper-rt shard.
pub struct Client {
    driver: Driver<NodeClasses, Fixed, Peers>,
    nodes: Vec<Known>,
    /// The leader each range was last found at, for the ranges this client asked.
    leaders: BTreeMap<u128, PeerId>,
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
            driver,
            nodes,
            leaders: BTreeMap::new(),
            body: Vec::new(),
        })
    }

    /// Where node `id`'s QUIC endpoint listens.
    pub fn learn(&mut self, id: PeerId, quic: SocketAddr) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) {
            if node.quic.is_some_and(|known| known != quic) {
                node.connected = false;
            }
            node.quic = Some(quic);
        }
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
        let mut target = self.leaders.get(&group).copied();
        let mut backoff = bounds::initial_pto();
        // Nodes asked since the last wait: once every node was tried and none answered or named a
        // leader, the client waits its backoff before it asks again.
        let mut tried = 0usize;
        loop {
            if tried >= self.nodes.len() {
                let wait = u64::try_from(backoff.as_nanos()).unwrap_or(u64::MAX);
                self.wait_until(
                    hyper_rt::futures::now_ns().saturating_add(wait),
                    deadline_ns,
                )
                .await?;
                backoff = backoff.saturating_mul(2);
                tried = 0;
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
            match self.ask(node, group, deadline_ns).await? {
                Asked::Answers(answers) => {
                    self.leaders.insert(group, node);
                    return Ok(answers);
                }
                Asked::Leader(leader) if leader != 0 && leader != node => {
                    target = Some(leader);
                }
                // No node knows a leader yet: the next is asked, and once every node was, the
                // client waits its backoff.
                Asked::Leader(_) | Asked::Lost => target = self.next_node(Some(node)),
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

    /// Dials `node` if it has no connection and waits for it: whether it was reached.
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
        let clock = self.driver.clock()?;
        if self.driver.endpoint().connect(clock, node, quic).is_err() {
            return Ok(false);
        }
        loop {
            match self.next_event(deadline_ns).await? {
                Some(Event::Connected { peer, .. }) if peer == node => return Ok(true),
                Some(Event::Unreachable { peer, .. }) | Some(Event::Closed { peer, .. })
                    if peer == node =>
                {
                    return Ok(false);
                }
                Some(_) => {}
                None => return Err(ClientError::Deadline),
            }
        }
    }

    /// Asks `node` once: its answers, the leader it names, or the exchange lost.
    async fn ask(
        &mut self,
        node: PeerId,
        group: u128,
        deadline_ns: u64,
    ) -> Result<Asked, ClientError> {
        let clock = self.driver.clock()?;
        let period = Progress::new(bounds::idle_timeout())?;
        let len = u64::try_from(self.body.len()).unwrap_or(u64::MAX);
        let exchange = match self.driver.endpoint().open(
            clock,
            node,
            Kind::Command,
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
        let mut reply: Option<(Status, u64)> = None;
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
                    let head = self.driver.endpoint().head(exchange).unwrap_or_default();
                    let read = command::read_reply_head(head)?;
                    reply = Some(read);
                    if read.0 != Status::Answered {
                        self.driver.endpoint().end(exchange);
                        return finish(read);
                    }
                    if let Some(done) = self.read(exchange, &mut bytes)? {
                        return Ok(Asked::Answers(done));
                    }
                }
                Event::BodyReady { exchange: e } if e == exchange && reply.is_some() => {
                    if let Some(done) = self.read(exchange, &mut bytes)? {
                        return Ok(Asked::Answers(done));
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
    fn read(
        &mut self,
        exchange: ExchangeId,
        into: &mut Vec<u8>,
    ) -> Result<Option<Vec<Answer>>, ClientError> {
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
        Ok(Some(command::decode_answers(into)?))
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
            Event::Connected { peer, .. } => {
                if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *peer) {
                    node.connected = true;
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

/// What one ask of a node came to.
enum Asked {
    Answers(Vec<Answer>),
    /// The node leads nothing for the range: the leader it knows, 0 for none.
    Leader(u64),
    /// The exchange or its connection ended without a reply.
    Lost,
}

/// A reply's head that is not an answer: the leader it names, or the status as the error.
fn finish((status, leader): (Status, u64)) -> Result<Asked, ClientError> {
    match status {
        Status::Redirect => Ok(Asked::Leader(leader)),
        status => Err(ClientError::Status(status)),
    }
}
