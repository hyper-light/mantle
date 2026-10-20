//! Three nodes on real sockets on loopback, each a task on one hyper-rt shard as a node's process
//! runs its owner (docs/design/node.md §2): one Name range replicated over QUIC lanes and the
//! sealed datagram plane, elected by the suspicion of the node-pair liveness stream (hyper-raft
//! timing steps L-2, L-3). Nothing is told by hand: each node dials the nodes above it, keys the
//! plane from each connection's TLS exporter, measures its links from the stream's heartbeats and
//! its timer's lateness from its own waits, and the range elects from nothing once a quorum's paths
//! are measured.
//!
//! - The range elects, and a registration proposed at its leader is applied by every node at the
//!   same index with the same answer.
//! - The range writes more entries than its log group may retain, which only compaction frees;
//!   every node applies the last, and every registration any node applied, each applied at the
//!   same index with the same answer.
//! - A follower stopped while the range wrote past that room starts again on its log, a new run of
//!   its stream, and catches up by a snapshot on the bulk class; a write after is applied by all
//!   three at one index.
//! - A client's command, asked first of a node that does not lead, is redirected to the leader,
//!   answered there once applied, and applied by every node with the same answer.
//!
//! Each runs twice: the range's rows in the model engine, and in mantle's own (`Stored`, its
//! checkpoints the range's durable state and the log replayed past them).
//!
//! Each node's task reports its status (`Node::status`) and its answers over a channel. The test's
//! task waits on those reports and goes on while the nodes move: a term, a leader, a commit or an
//! applied index, an answer, a node opened or stopped, or, while some pair is not yet judged by a
//! margin, the heartbeats taken. It gives up once a quiet period passes with nothing moved: the
//! longest patience any node states by its own law (`Status::patience`), and never less than
//! RFC 6298's initial retransmission timeout, one second, before anything is measured. A node
//! whose log has a flush out extends the watch, since its range moves only as its writes become
//! durable, until the flush has been out longer than the longest any node's log has taken and the
//! quiet period besides (hyper-durable-e2e's rule, in one process).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, Waits};
use hyper_rt::combine::{Either, race2};
use hyper_rt::futures::{now_ns, sleep, spawn_detached};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::sync::{ChannelReceiver, Sender, channel};
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_cert::{KeyPair, Validity};
use mantle_engine::store::Config as StoreConfig;
use mantle_engine::trunk::TrunkConfig;
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model};
use mantle_meta::name;
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command as Op, Entry, Sessioned};
use mantle_node::classes::Role;
use mantle_node::client::{Client, ClientSettings};
use mantle_node::directory::{Peer, node_name};
use mantle_node::node::{Hosted, Node, NodeSettings, Status, message_bounds};
use mantle_range::{ConfState, Range, Settings, Stored};

/// The range every node hosts: the cell's first Name range.
const GROUP: u128 = 0x0072_616e_6765;
/// The nodes, each a founding voter of the range.
const NODES: u64 = 3;
/// The client's id: past every node's.
const CLIENT: u64 = NODES + 1;
/// RFC 6298 §2.1: the retransmission timeout before any round trip is measured, one second; the
/// least quiet period the test waits through.
const INITIAL_RTO: Duration = Duration::from_secs(1);
/// The authority's name.
const AUTHORITY: &str = "mantle test cell";
/// RFC 5280 §4.1.2.5: the notAfter of a certificate with no well-defined expiration,
/// 9999-12-31T23:59:59Z, in seconds since 1970.
const NO_EXPIRY: u64 = 253_402_300_799;

/// The range's settings and rules, as the range's own process test runs them
/// (`crates/range/tests/processes.rs`).
const SETTINGS: Settings = Settings {
    election_tick: 10,
    heartbeat_tick: 2,
    max_size_per_msg: 1 << 10,
    max_inflight_msgs: 2,
    max_uncommitted_size: 2 << 10,
    max_committed_size_per_ready: 1 << 20,
    max_entry_bytes: 1 << 10,
};

const RULES: Rules = Rules {
    lifetime_ns: u64::MAX / 2,
    max_sessions: 64,
    max_answers: 16,
    max_answer_bytes: usize::MAX,
    expiries_per_entry: 8,
};

fn range() -> Range {
    Range {
        layer: Layer::Name,
        rules: RULES,
        boot: ConfState {
            voters: (1..=NODES).collect(),
            ..ConfState::default()
        },
        settings: SETTINGS,
    }
}

/// The range as each node hosts it, its rows in `engine`.
fn hosted<E>(engine: E) -> Hosted<E> {
    Hosted {
        group: GROUP,
        range: range(),
        engine,
    }
}

/// The engine of a cell's first Name range, before any entry.
fn first_range<E: Engine>(mut engine: E) -> E {
    engine.install(0, name::first(1).unwrap()).unwrap();
    engine.persist().unwrap();
    engine
}

/// The engines a node keeps the range's rows in.
#[derive(Clone, Copy, Debug)]
enum Engines {
    Model,
    /// mantle's own, in a store file in the node's directory.
    Mantle,
}

/// The engine's own small test configuration (crates/engine/tests/pool_startup.rs).
const STORE: StoreConfig = StoreConfig {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};

/// The node's range engine in `dir`: opened at its last checkpoint when its store is there,
/// created with the range's founding rows when not.
fn stored(dir: &Path) -> Stored<DeviceFile> {
    let path = dir.join("range.store");
    let file = |create| {
        DeviceFile::open(
            &path,
            create,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap()
    };
    if path.exists() {
        Stored::open(file(false), STORE, STORE.page_size, TRUNK).unwrap()
    } else {
        first_range(Stored::create(file(true), STORE, STORE.page_size, TRUNK).unwrap())
    }
}

/// The range's log as its process test keeps it, with the node's measured waits and two groups:
/// the range's and the liveness stream's.
fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 16 * 4096,
        max_segments: 64,
        max_groups: 2,
        group_entries: 1 << 10,
        group_bytes: 4 << 10,
        group_cache: 1 << 12,
        queue_submissions: 16,
        waits: Waits::Measured,
    }
}

/// A registration, told from the others by its serial.
fn registration(serial: u64) -> Entry {
    Entry {
        at_ns: 0,
        commands: vec![Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Op::Register,
        }],
    }
}

/// Writes that pass a log group's room: one more entry than a group may retain, so the range
/// compacts at least once whatever an entry's bytes.
fn past_room() -> u64 {
    log_config().group_entries + 1
}

/// The shard the test runs on, sized by what it holds: its own task, a task a node (a node started
/// again takes the slot its stopped task left) and the client's; each node's wait racing its own
/// deadline and its driver's QUIC timer, and the client's and the test's racing one each; each
/// node waiting on its endpoint's socket, a socket for each connection (its peers' and the
/// client's) and its plane's, and the client on its endpoint's and one a node, to read and to
/// write. The watchdog's budget and the wheel's tick are hyper-rt-transport's test harness's
/// (`tests/exchanges.rs`); the stream measures the tick's lateness as part of its granularity.
fn runtime() -> RuntimeConfig {
    let nodes = usize::try_from(NODES).unwrap();
    let tasks = nodes + 2;
    let sockets = nodes * (nodes + 2) + (nodes + 1);
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: tasks,
        timers_per_shard: 2 * nodes + 2,
        interests_per_shard: interests_for(tasks).max(2 * sockets),
        ring_entries: tasks,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: tasks,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// The cell's authority, each node's certificate and key, and the client's.
struct Pki {
    authority: Vec<u8>,
    /// By node id: the certificate, DER, and the key, PKCS #8.
    nodes: BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
    client: (Vec<u8>, Vec<u8>),
}

impl Pki {
    fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let validity = Validity {
            not_before: now,
            not_after: NO_EXPIRY,
        };
        let key = KeyPair::generate().unwrap();
        let authority = mantle_cert::authority(&key, AUTHORITY, validity).unwrap();
        let issue = |name: &str| {
            let own = KeyPair::generate().unwrap();
            let certificate =
                mantle_cert::node(&key, AUTHORITY, own.public_key(), name, validity).unwrap();
            (certificate, own.pkcs8().to_vec())
        };
        let nodes = (1..=NODES).map(|id| (id, issue(&node_name(id)))).collect();
        let client = issue(&format!("client-{CLIENT}"));
        Self {
            authority,
            nodes,
            client,
        }
    }

    fn credentials(&self, (certificate, key): &(Vec<u8>, Vec<u8>)) -> Credentials {
        Credentials {
            chain: vec![CertificateDer::from(certificate.clone())],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.clone())),
            roots: vec![CertificateDer::from(self.authority.clone())],
        }
    }

    /// The nodes as peers, in `order`.
    fn node_peers(&self, order: &[u64]) -> Vec<Peer> {
        order
            .iter()
            .map(|id| Peer {
                id: *id,
                role: Role::Node,
                certificate: self.nodes[id].0.clone(),
            })
            .collect()
    }

    /// The client's settings: it asks the nodes in `order`.
    fn client_settings(&self, order: &[u64]) -> ClientSettings {
        let peers = self.node_peers(order);
        let transport_bytes =
            u64::try_from(peers.len() * 3 * hyper_transport::RECEIVE_CHUNK).unwrap();
        ClientSettings {
            id: CLIENT,
            bind: "127.0.0.1:0".parse().unwrap(),
            credentials: self.credentials(&self.client),
            nodes: peers,
            messages: message_bounds(&[hosted(())]).unwrap(),
            transport_bytes,
        }
    }

    /// Node `id`'s settings: its directory, loopback addresses the kernel picks, its credentials,
    /// its peers by certificate, and the range.
    fn settings<E>(&self, id: u64, dir: &Path, engine: E) -> NodeSettings<E> {
        let others: Vec<u64> = (1..=NODES).filter(|peer| *peer != id).collect();
        let mut peers = self.node_peers(&others);
        peers.push(Peer {
            id: CLIENT,
            role: Role::Client,
            certificate: self.client.0.clone(),
        });
        // Each peer's share holds a receive chunk, the largest datagram QUIC carries, for each of
        // the node's three classes.
        let transport_bytes =
            u64::try_from(peers.len() * 3 * hyper_transport::RECEIVE_CHUNK).unwrap();
        NodeSettings {
            id,
            dir: dir.to_path_buf(),
            quic: "127.0.0.1:0".parse().unwrap(),
            plane: "127.0.0.1:0".parse().unwrap(),
            credentials: self.credentials(&self.nodes[&id]),
            peers,
            addresses: Vec::new(),
            ranges: vec![hosted(engine)],
            log: log_config(),
            transport_bytes,
        }
    }
}

/// What a node's task tells the test.
#[derive(Debug)]
enum Report {
    /// Where the node listens, once open: QUIC's address and the plane's.
    Bound(u64, SocketAddr, SocketAddr),
    Status(u64, Status),
    /// A registration applied: its serial, its index, its answer.
    Applied(u64, u64, u64, Answer),
    /// What the node's log says of its flushes: how long the one out has been, if one is, and
    /// the longest an update has waited for one.
    Log(u64, Option<Duration>, Duration),
    /// The node is closed: its log, its sockets.
    Stopped(u64),
    Failed(u64, String),
    /// The client's command for registration `serial`: its answers, or why it had none.
    Answered(u64, Result<Vec<Answer>, String>),
}

/// What the test tells the client's task.
enum Ask {
    /// Where a node's QUIC endpoint listens.
    Learn(u64, SocketAddr),
    /// Registration `serial`, of the range.
    Command(u64),
}

/// The client's task: opens the client and asks what the test orders, one command at a time,
/// reporting each one's answers. Its commands have no deadline of their own: the test's watch
/// ends a command that does not come.
async fn client(settings: ClientSettings, mut asks: ChannelReceiver<Ask>, reports: Sender<Report>) {
    let mut client = match Client::open(settings).await {
        Ok(client) => client,
        Err(error) => {
            let _ = reports
                .send(Report::Failed(CLIENT, error.to_string()))
                .await;
            return;
        }
    };
    while let Ok(ask) = asks.recv().await {
        match ask {
            Ask::Learn(id, quic) => client.learn(id, quic),
            Ask::Command(serial) => {
                let answered = client
                    .command(GROUP, &registration(serial), u64::MAX)
                    .await
                    .map_err(|e| e.to_string());
                if reports
                    .send(Report::Answered(serial, answered))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

/// What the test tells a node's task.
enum Order {
    /// Where a peer listens.
    Learn(u64, SocketAddr, SocketAddr),
    Propose(Entry),
    Stop,
}

/// Orders a node's channel holds: where each peer listens, a proposal and a stop.
fn orders_bound() -> usize {
    usize::try_from(NODES).unwrap() + 1
}

/// A node's task: opens the node, as the task that turns a node opens it (`Node::open`), reports
/// where it listens, then turns it, each turn raced against the test's next order, reporting its
/// status when it changed and each registration's answer as it applied. Every report waits for
/// room in the channel: none is lost.
async fn run<E: Engine + 'static>(
    settings: NodeSettings<E>,
    mut orders: ChannelReceiver<Order>,
    reports: Sender<Report>,
) {
    let id = settings.id;
    let opened = Node::open(settings).await;
    let mut node = match opened.and_then(|node| node.addresses().map(|a| (node, a))) {
        Ok((node, (quic, plane))) => {
            if reports.send(Report::Bound(id, quic, plane)).await.is_err() {
                return;
            }
            node
        }
        Err(error) => {
            let _ = reports.send(Report::Failed(id, error.to_string())).await;
            return;
        }
    };
    let mut told: Option<Status> = None;
    let mut stats = None;
    loop {
        let ended = {
            let turn = pin!(node.turn());
            let order = pin!(orders.recv());
            race2(turn, order).await
        };
        match ended {
            Either::First(Ok(())) => {}
            Either::First(Err(error)) => {
                let _ = reports.send(Report::Failed(id, error.to_string())).await;
                return;
            }
            Either::Second(Ok(Order::Learn(peer, quic, plane))) => node.learn(peer, quic, plane),
            // A node that no longer leads refuses it, and the test proposes again at the leader.
            Either::Second(Ok(Order::Propose(entry))) => {
                let _ = node.propose(GROUP, &entry);
            }
            Either::Second(Ok(Order::Stop)) => {
                drop(node);
                let _ = reports.send(Report::Stopped(id)).await;
                return;
            }
            // The test is done.
            Either::Second(Err(_)) => return,
        }
        for (_, applied) in node.take_applied() {
            if applied.session == 0 {
                let report = Report::Applied(id, applied.serial, applied.index, applied.answer);
                if reports.send(report).await.is_err() {
                    return;
                }
            }
        }
        let status = node.status(GROUP);
        if status != told
            && let Some(now) = status
        {
            if reports.send(Report::Status(id, now)).await.is_err() {
                return;
            }
            told = status;
            match node.log_stats(stats.take()) {
                Ok(log) => {
                    let flushing = log
                        .flushing_since
                        .map(|since| log.at.saturating_duration_since(since));
                    let longest =
                        Duration::from_nanos(log.commit_wait.quantile(1_000_000).unwrap_or(0));
                    stats = Some(log);
                    if reports
                        .send(Report::Log(id, flushing, longest))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => {
                    let _ = reports.send(Report::Failed(id, error.to_string())).await;
                    return;
                }
            }
        }
    }
}

/// What the test has heard of the nodes.
#[derive(Debug, Default)]
struct Watch {
    bound: BTreeMap<u64, (SocketAddr, SocketAddr)>,
    /// The nodes open and not stopped.
    live: BTreeSet<u64>,
    status: BTreeMap<u64, Status>,
    /// Each node's registrations applied, by serial: the index and the answer.
    applied: BTreeMap<u64, BTreeMap<u64, (u64, Answer)>>,
    /// Each node's log: when it was heard, its flush out and for how long then, and the longest
    /// an update waited.
    log: BTreeMap<u64, (u64, Option<Duration>, Duration)>,
    /// The client's commands, by serial: their answers or why they had none.
    answered: BTreeMap<u64, Result<Vec<Answer>, String>>,
    /// When a node last moved, on the shard's clock.
    moved_at: u64,
}

impl Watch {
    /// Takes reports until `done` holds, and fails once a quiet period passes with nothing moved.
    async fn until<T>(
        &mut self,
        reports: &mut ChannelReceiver<Report>,
        done: impl Fn(&Self) -> Option<T>,
    ) -> T {
        self.moved_at = now_ns();
        loop {
            if let Some(found) = done(self) {
                return found;
            }
            let quiet = self.quiet();
            let quiet_ns = u64::try_from(quiet.as_nanos()).unwrap();
            let left = self
                .moved_at
                .saturating_add(quiet_ns)
                .max(self.flush_deadline(quiet_ns))
                .saturating_sub(now_ns());
            let ended = {
                let report = pin!(reports.recv());
                let wait = pin!(sleep(left));
                race2(report, wait).await
            };
            match ended {
                Either::First(Ok(report)) => self.take(report),
                Either::First(Err(error)) => panic!("every node's task ended: {error:?}; {self:?}"),
                Either::Second(Ok(())) => {
                    panic!("quiet for {quiet:?} with nothing moved: {self:?}")
                }
                Either::Second(Err(error)) => panic!("the test's timer: {error:?}"),
            }
        }
    }

    fn take(&mut self, report: Report) {
        match report {
            Report::Bound(id, quic, plane) => {
                self.bound.insert(id, (quic, plane));
                self.live.insert(id);
                self.moved_at = now_ns();
            }
            Report::Status(id, now) => {
                let moved = self.status.get(&id).is_none_or(|was| {
                    (was.term, was.leader, was.commit, was.applied)
                        != (now.term, now.leader, now.commit, now.applied)
                        || (!(was.judged && now.judged) && was.heartbeats != now.heartbeats)
                });
                if moved {
                    self.moved_at = now_ns();
                }
                self.status.insert(id, now);
            }
            Report::Applied(id, serial, index, answer) => {
                self.applied
                    .entry(id)
                    .or_default()
                    .insert(serial, (index, answer));
                self.moved_at = now_ns();
            }
            Report::Log(id, flushing, longest) => {
                self.log.insert(id, (now_ns(), flushing, longest));
            }
            Report::Stopped(id) => {
                self.live.remove(&id);
                self.status.remove(&id);
                self.log.remove(&id);
                self.moved_at = now_ns();
            }
            Report::Failed(id, error) => panic!("node {id} stopped: {error}; {self:?}"),
            Report::Answered(serial, answered) => {
                self.answered.insert(serial, answered);
                self.moved_at = now_ns();
            }
        }
    }

    /// When the watch ends for the flushes out: each live node's, as last heard, past the
    /// longest any node's log has taken and `quiet_ns` besides.
    fn flush_deadline(&self, quiet_ns: u64) -> u64 {
        let longest = self
            .log
            .values()
            .map(|(_, _, l)| *l)
            .max()
            .unwrap_or_default();
        let allowed = u64::try_from(longest.as_nanos())
            .unwrap()
            .saturating_add(quiet_ns);
        self.log
            .iter()
            .filter(|(id, _)| self.live.contains(id))
            .filter_map(|(_, (heard, flushing, _))| {
                let age = u64::try_from(flushing.as_ref()?.as_nanos()).unwrap();
                Some(heard.saturating_add(allowed.saturating_sub(age)))
            })
            .max()
            .unwrap_or(0)
    }

    /// The longest any node states it may go without moving, and never less than the initial
    /// retransmission timeout.
    fn quiet(&self) -> Duration {
        self.status
            .values()
            .filter_map(|s| s.patience)
            .fold(INITIAL_RTO, Duration::max)
    }

    /// The leader every live node names, in one term.
    fn agreed_leader(&self) -> Option<u64> {
        let mut named = self.live.iter().map(|id| self.status.get(id));
        let first = named.next()??;
        let agreed = first.leader != 0
            && self.live.contains(&first.leader)
            && named.all(|s| s.is_some_and(|s| s.leader == first.leader && s.term == first.term));
        if agreed { Some(first.leader) } else { None }
    }

    /// Registration `serial`'s index and answer, once every live node applied it the same.
    fn applied_everywhere(&self, serial: u64) -> Option<(u64, Answer)> {
        let mut each = self
            .live
            .iter()
            .map(|id| self.applied.get(id).and_then(|a| a.get(&serial)));
        let first = each.next()??;
        if each.all(|a| a == Some(first)) {
            Some(first.clone())
        } else {
            None
        }
    }
}

/// The cell under test: its nodes' tasks, the orders to each and the reports from all, and what
/// the test heard.
struct Cell {
    engines: Engines,
    pki: Pki,
    dirs: BTreeMap<u64, tempfile::TempDir>,
    orders: BTreeMap<u64, Sender<Order>>,
    asks: Option<Sender<Ask>>,
    tell: Sender<Report>,
    reports: ChannelReceiver<Report>,
    watch: Watch,
}

impl Cell {
    /// The cell's nodes started, each told where the others listen.
    async fn start(engines: Engines) -> Self {
        // A report from each node at a time.
        let (tell, reports) = channel(usize::try_from(NODES).unwrap()).unwrap();
        let mut cell = Cell {
            engines,
            pki: Pki::new(),
            dirs: (1..=NODES)
                .map(|id| (id, tempfile::tempdir().unwrap()))
                .collect(),
            orders: BTreeMap::new(),
            asks: None,
            tell,
            reports,
            watch: Watch::default(),
        };
        for id in 1..=NODES {
            cell.spawn(id);
        }
        cell.introduce(&(1..=NODES).collect::<Vec<_>>()).await;
        cell
    }

    /// Starts node `id`'s task on its directory.
    fn spawn(&mut self, id: u64) {
        let (order, taken) = channel(orders_bound()).unwrap();
        self.orders.insert(id, order);
        self.watch.bound.remove(&id);
        // The engine is opened on the shard that turns its node: here, the one shard.
        let dir = self.dirs[&id].path();
        let tell = self.tell.clone();
        match self.engines {
            Engines::Model => {
                let settings = self.pki.settings(id, dir, first_range(Model::default()));
                spawn_detached(run(settings, taken, tell)).unwrap();
            }
            Engines::Mantle => {
                // The test's one shard is the nodes' metadata shard: its replicas apply through
                // the engine's synchronous calls (node.md §1.2).
                mantle_engine::store::permit_blocking();
                let settings = self.pki.settings(id, dir, stored(dir));
                spawn_detached(run(settings, taken, tell)).unwrap();
            }
        };
    }

    fn order(&self, id: u64, order: Order) {
        self.orders[&id]
            .try_send(order)
            .map_err(|_| "a node's orders are full")
            .unwrap();
    }

    /// Waits until each of `ids` is open, then tells every live node where the others listen.
    async fn introduce(&mut self, ids: &[u64]) {
        self.watch
            .until(&mut self.reports, |w| {
                if ids.iter().all(|id| w.bound.contains_key(id)) {
                    Some(())
                } else {
                    None
                }
            })
            .await;
        for id in &self.watch.live {
            for (peer, (quic, plane)) in &self.watch.bound {
                if peer != id && self.watch.live.contains(peer) {
                    self.order(*id, Order::Learn(*peer, *quic, *plane));
                }
            }
        }
    }

    async fn leader(&mut self) -> u64 {
        self.watch
            .until(&mut self.reports, Watch::agreed_leader)
            .await
    }

    /// Proposes registration `serial` at the leader and waits until the leader applied it: its
    /// index and answer. A leader that steps down first is asked again where the leader is then,
    /// at most once for each node, since each step down is an election the cell moved through.
    async fn write(&mut self, serial: u64) -> (u64, Answer) {
        for _ in 0..NODES {
            let leader = self.leader().await;
            self.order(leader, Order::Propose(registration(serial)));
            let heard = self
                .watch
                .until(&mut self.reports, |w| {
                    if let Some(applied) = w.applied.get(&leader).and_then(|a| a.get(&serial)) {
                        return Some(Some(applied.clone()));
                    }
                    if w.status.get(&leader).is_some_and(|s| s.leader != leader) {
                        return Some(None);
                    }
                    None
                })
                .await;
            if let Some(applied) = heard {
                return applied;
            }
        }
        panic!(
            "registration {serial} not applied through {NODES} leaders: {:?}",
            self.watch
        );
    }

    async fn applied_everywhere(&mut self, serial: u64) -> (u64, Answer) {
        self.watch
            .until(&mut self.reports, |w| w.applied_everywhere(serial))
            .await
    }

    async fn stop(&mut self, id: u64) {
        self.order(id, Order::Stop);
        self.watch
            .until(&mut self.reports, |w| {
                if w.live.contains(&id) { None } else { Some(()) }
            })
            .await;
    }

    /// Starts node `id` again on its directory, at new addresses every live node is told.
    async fn restart(&mut self, id: u64) {
        self.spawn(id);
        self.introduce(&[id]).await;
    }

    /// Starts the client, asking the nodes in `order`, and tells it where each live node listens.
    fn start_client(&mut self, order: &[u64]) {
        // Where each node listens, and the commands, one at a time.
        let (asks, taken) = channel(usize::try_from(NODES).unwrap() + 1).unwrap();
        spawn_detached(client(
            self.pki.client_settings(order),
            taken,
            self.tell.clone(),
        ))
        .unwrap();
        for (id, (quic, _)) in &self.watch.bound {
            if self.watch.live.contains(id) {
                asks.try_send(Ask::Learn(*id, *quic))
                    .map_err(|_| "the client's asks are full")
                    .unwrap();
            }
        }
        self.asks = Some(asks);
    }

    /// The client's command for registration `serial`: its answers.
    async fn command(&mut self, serial: u64) -> Vec<Answer> {
        self.asks
            .as_ref()
            .unwrap()
            .try_send(Ask::Command(serial))
            .map_err(|_| "the client's asks are full")
            .unwrap();
        self.watch
            .until(&mut self.reports, |w| w.answered.get(&serial).cloned())
            .await
            .unwrap_or_else(|error| panic!("command {serial}: {error}"))
    }

    /// Stops every live node, so each closes its log before its directory goes.
    async fn close(mut self) {
        self.asks = None;
        let live: Vec<u64> = self.watch.live.iter().copied().collect();
        for id in live {
            self.stop(id).await;
        }
    }
}

/// Runs `test` as the root task of a shard sized for the cell.
fn on_a_shard(test: impl Future<Output = ()> + 'static) {
    LocalRuntime::new(&runtime())
        .unwrap()
        .block_on(test)
        .unwrap();
}

fn elect_and_apply(engines: Engines) {
    on_a_shard(async move {
        let mut cell = Cell::start(engines).await;
        let (index, answer) = cell.write(1).await;
        assert!(index > 0);
        assert!(
            matches!(answer, Answer::Registered { .. }),
            "the registration answered {answer:?}"
        );
        assert_eq!(cell.applied_everywhere(1).await, (index, answer));
        cell.close().await;
    });
}

fn write_past_room(engines: Engines) {
    on_a_shard(async move {
        let mut cell = Cell::start(engines).await;
        let writes = past_room();
        for serial in 1..=writes {
            cell.write(serial).await;
        }
        cell.applied_everywhere(writes).await;
        // A node that lagged past a compaction took a snapshot for what it missed and applied none
        // of it as entries; every registration a node applied, it applied as the others did.
        for serial in 1..=writes {
            let applied: Vec<&(u64, Answer)> = cell
                .watch
                .applied
                .values()
                .filter_map(|a| a.get(&serial))
                .collect();
            assert!(
                !applied.is_empty() && applied.iter().all(|a| *a == applied[0]),
                "registration {serial} applied as {applied:?}"
            );
        }
        cell.close().await;
    });
}

fn catch_up_by_snapshot(engines: Engines) {
    on_a_shard(async move {
        let mut cell = Cell::start(engines).await;
        let leader = cell.leader().await;
        let follower = (1..=NODES).rev().find(|id| *id != leader).unwrap();
        cell.stop(follower).await;
        let writes = past_room();
        let mut last = 0;
        for serial in 1..=writes {
            last = cell.write(serial).await.0;
        }
        cell.restart(follower).await;
        cell.watch
            .until(&mut cell.reports, |w| {
                w.status
                    .get(&follower)
                    .filter(|s| s.applied >= last)
                    .map(|s| s.snapshots_taken)
            })
            .await;
        let after = writes + 1;
        let (index, answer) = cell.write(after).await;
        assert_eq!(cell.applied_everywhere(after).await, (index, answer));
        let taken = cell.watch.status[&follower].snapshots_taken;
        assert!(taken > 0, "the follower caught up without a snapshot");
        cell.close().await;
    });
}

fn client_redirected(engines: Engines) {
    on_a_shard(async move {
        let mut cell = Cell::start(engines).await;
        let leader = cell.leader().await;
        // The client asks a node that does not lead first.
        let mut order: Vec<u64> = (1..=NODES).filter(|id| *id != leader).collect();
        order.push(leader);
        cell.start_client(&order);
        let answers = cell.command(1).await;
        assert!(
            matches!(answers.as_slice(), [Answer::Registered { .. }]),
            "the registration answered {answers:?}"
        );
        let (_, applied) = cell.applied_everywhere(1).await;
        assert_eq!(answers, vec![applied]);
        cell.close().await;
    });
}

#[test]
fn three_nodes_elect_by_suspicion_and_apply_a_registration_everywhere() {
    elect_and_apply(Engines::Model);
}

#[test]
fn three_nodes_elect_by_suspicion_and_apply_a_registration_everywhere_on_mantles_engine() {
    elect_and_apply(Engines::Mantle);
}

#[test]
fn a_range_writes_past_its_logs_room_and_every_node_applies_the_same() {
    write_past_room(Engines::Model);
}

#[test]
fn a_range_writes_past_its_logs_room_and_every_node_applies_the_same_on_mantles_engine() {
    write_past_room(Engines::Mantle);
}

#[test]
fn a_follower_stopped_while_the_range_compacted_catches_up_by_a_snapshot() {
    catch_up_by_snapshot(Engines::Model);
}

#[test]
fn a_follower_stopped_while_the_range_compacted_catches_up_by_a_snapshot_on_mantles_engine() {
    catch_up_by_snapshot(Engines::Mantle);
}

#[test]
fn a_clients_command_is_redirected_to_the_leader_and_applied_everywhere() {
    client_redirected(Engines::Model);
}

#[test]
fn a_clients_command_is_redirected_to_the_leader_and_applied_everywhere_on_mantles_engine() {
    client_redirected(Engines::Mantle);
}
