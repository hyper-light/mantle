//! Three nodes on real sockets on loopback, each on a thread of its own running a hyper-rt shard, as
//! a node's process runs its owner (docs/design/node.md §2): one Name range replicated over QUIC
//! lanes and the
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
//! - A leader cut off from its followers by the test's relay (`support/relay.rs`) loses the lead:
//!   the others elect and apply a write it does not have; joined again, it follows the new leader
//!   and applies the write as they did.
//! - Three clients, each its own identity and session, put and read two keys while the leader is
//!   stopped half way through, or cut off for a quarter of the history, each read by ReadIndex;
//!   each key's history is linearizable (Wing and Gong's search with Lowe's memo,
//!   `crates/range/tests/support/linear.rs`), and every live node ends where the others do.
//!
//! - A follower lost for good, its directory with it, is replaced by a node under an identity
//!   never used (replica.md §6), and an original node then stops, so the quorum counts the new
//!   one: the clients' history through both is linearizable, no acknowledged write lost.
//!
//! The cut and the linearizable histories run on cells of three voters and of five (node.md's
//! Phase C).
//!
//! Each runs twice: the range's rows in the model engine, and in mantle's own (`Stored`, its
//! checkpoints the range's durable state and the log replayed past them).
//!
//! Each node's task reports its status (`Node::status`) and its answers over a channel. The test's
//! thread waits on those reports and goes on while the nodes move: a term, a leader, a commit or an
//! applied index, an answer, a node opened or stopped, or the heartbeats taken from the peers not
//! yet judged by a margin (`Status::measuring`), which stop if a peer measured goes silent. It
//! gives up once a quiet period passes with nothing moved: the
//! longest patience any node states by its own law (`Status::patience`), or the longest until a
//! peer a node dials is connected again (`Status::reconnect`), or any client gives an exchange by
//! its own (`Client::patience`), and never less than RFC 6298's initial retransmission timeout, one
//! second, before anything is measured. A node
//! whose log has a flush out extends the watch, since its range moves only as its writes become
//! durable, until the flush has been out longer than the longest any node's log has taken and the
//! quiet period besides (hyper-durable-e2e's rule, in one process). A node's shard is its own, as
//! its process's would be, so an engine's flush on one node holds up none of the others, nor the
//! test.

#![allow(
    clippy::cast_possible_truncation,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, Waits};
use hyper_rt::combine::{Either, race2};
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
use mantle_meta::wire::{Answer, Entry};
use mantle_node::classes::Role;
use mantle_node::client::{Client, ClientSettings};
use mantle_node::directory::{Peer, node_name};
use mantle_node::node::{Hosted, Node, NodeSettings, Status, message_bounds};
use mantle_range::{ConfState, Range, Settings, Stored};

#[path = "support/history.rs"]
mod history;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
use history::{Part, Plan, Steer, Told, check_histories, registration, workload};
#[path = "support/relay.rs"]
mod relay;
use relay::Relay;

/// The range every node hosts: the cell's first Name range.
const GROUP: u128 = 0x0072_616e_6765;
/// The nodes of most cells, each a founding voter of the range.
const NODES: u64 = 3;
/// The most nodes a cell has: five, the larger of the cells node.md's Phase C asks histories of
/// (three voters and five).
const MOST_NODES: u64 = 5;
/// The first client's id, past every node's of the largest cell; the clients' ids follow it.
const CLIENT: u64 = MOST_NODES + 1;
/// The clients of the linearizable scenario, each its own identity and session: one for each node
/// of a cell of three.
const CLIENTS: u64 = NODES;
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

/// The range of a cell of `nodes`, each a founding voter.
fn range(nodes: u64) -> Range {
    Range {
        layer: Layer::Name,
        rules: RULES,
        boot: ConfState {
            voters: (1..=nodes).collect(),
            ..ConfState::default()
        },
        settings: SETTINGS,
    }
}

/// The range as each node of a cell of `nodes` hosts it, its rows in `engine`.
fn hosted<E>(nodes: u64, engine: E) -> Hosted<E> {
    Hosted {
        group: GROUP,
        range: range(nodes),
        engine,
    }
}

/// The engine of a cell's first Name range, before any entry.
fn first_range<E: Engine>(mut engine: E) -> E {
    engine.install(0, name::first(1).unwrap()).unwrap();
    engine.persist().unwrap();
    engine
}

/// How the nodes reach each other: directly, or through the test's relay, which partitions them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Network {
    Direct,
    Relayed,
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

/// The store file of a node's range engine in `dir`.
fn store_file(dir: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        &dir.join("range.store"),
        create,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .unwrap()
}

/// The node's range engine in `dir`: opened at its last checkpoint when its store is there,
/// created with the range's founding rows when not.
fn stored(dir: &Path) -> Stored<DeviceFile> {
    if dir.join("range.store").exists() {
        Stored::open(store_file(dir, false), STORE, STORE.page_size, TRUNK).unwrap()
    } else {
        first_range(Stored::create(store_file(dir, true), STORE, STORE.page_size, TRUNK).unwrap())
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

/// Writes that pass a log group's room: one more entry than a group may retain, so the range
/// compacts at least once whatever an entry's bytes.
fn past_room() -> u64 {
    log_config().group_entries + 1
}

/// A shard of its own, sized by what it holds: one task, its wait's timer and its driver's QUIC
/// timer, and `sockets` sockets, each read and written. The watchdog's budget and the wheel's tick
/// are hyper-rt-transport's test harness's (`tests/exchanges.rs`); a node's stream measures the
/// tick's lateness as part of its granularity.
fn shard(sockets: usize) -> LocalRuntime {
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
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

/// The sockets of a node of a cell of `nodes`: its endpoint's, one for each connection (its
/// peers' and each client's) and its plane's.
fn node_sockets(nodes: u64) -> usize {
    usize::try_from(nodes + CLIENTS).unwrap() + 1
}

/// The sockets of a client of `nodes` nodes: its endpoint's and one for each node.
fn client_sockets(nodes: usize) -> usize {
    nodes + 1
}

/// The cell's authority, each node's certificate and key, and the client's.
#[derive(Clone)]
struct Pki {
    authority: Vec<u8>,
    /// The range's founding voters: nodes 1 to `voters`. A node past them is a spare identity,
    /// one a replacement takes, which every directory holds from the start.
    voters: u64,
    /// By node id: the certificate, DER, and the key, PKCS #8.
    nodes: BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
    /// By client id: the certificate and the key.
    clients: BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
}

impl Pki {
    /// The authority, `nodes` nodes' certificates and keys, the first `voters` of them the range's
    /// founding voters, and the clients'.
    fn new(voters: u64, nodes: u64) -> Self {
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
        let nodes = (1..=nodes).map(|id| (id, issue(&node_name(id)))).collect();
        let clients = (CLIENT..CLIENT + CLIENTS)
            .map(|id| (id, issue(&format!("client-{id}"))))
            .collect();
        Self {
            authority,
            voters,
            nodes,
            clients,
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

    /// Client `id`'s settings: it asks the nodes in `order`.
    fn client_settings(&self, id: u64, order: &[u64]) -> ClientSettings {
        let peers = self.node_peers(order);
        let transport_bytes =
            u64::try_from(peers.len() * 3 * hyper_transport::RECEIVE_CHUNK).unwrap();
        ClientSettings {
            id,
            bind: "127.0.0.1:0".parse().unwrap(),
            credentials: self.credentials(&self.clients[&id]),
            nodes: peers,
            messages: message_bounds(&[hosted(u64::try_from(order.len()).unwrap(), ())]).unwrap(),
            transport_bytes,
        }
    }

    /// Node `id`'s settings: its directory, loopback addresses the kernel picks, its credentials,
    /// its peers by certificate, and the range.
    /// Node `id`'s settings, its rows in `engine`: its range names the founding voters, whether
    /// it is one or a spare that joins as a replacement adds it.
    fn settings<E>(&self, id: u64, dir: &Path, engine: E) -> NodeSettings<E> {
        let others: Vec<u64> = self
            .nodes
            .keys()
            .copied()
            .filter(|peer| *peer != id)
            .collect();
        let mut peers = self.node_peers(&others);
        peers.extend(self.clients.iter().map(|(id, (certificate, _))| Peer {
            id: *id,
            role: Role::Client,
            certificate: certificate.clone(),
        }));
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
            ranges: vec![hosted(self.voters, engine)],
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
    /// A node's task, or a client's, ended on an error.
    Failed(u64, String),
    /// The client's command for registration `serial`: its answers, or why it had none.
    Answered(u64, Result<Vec<Answer>, String>),
    /// A workload client's operations done, of all it does, and how long it now gives an exchange
    /// before it takes the node as lost (`Client::patience`).
    Progress(u64, u64, Option<Duration>),
    /// A replacement a node ended as the range's leader: the member lost for good and the one
    /// that took its place.
    Replaced(u64, u64),
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
async fn client(
    settings: ClientSettings,
    mut asks: ChannelReceiver<Ask>,
    reports: mpsc::Sender<Report>,
) {
    let mut client = match Client::open(settings).await {
        Ok(client) => client,
        Err(error) => {
            let _ = reports.send(Report::Failed(CLIENT, error.to_string()));
            return;
        }
    };
    while let Ok(ask) = asks.recv().await {
        match ask {
            Ask::Learn(id, quic) => {
                if let Err(error) = client.learn(id, quic) {
                    let _ = reports.send(Report::Failed(CLIENT, error.to_string()));
                    return;
                }
            }
            Ask::Command(serial) => {
                let answered = client
                    .command(GROUP, &registration(serial), u64::MAX)
                    .await
                    .map_err(|e| e.to_string());
                if reports.send(Report::Answered(serial, answered)).is_err() {
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
    /// Replace the first, a member lost for good, with the second (`Node::replace`).
    Replace(u64, u64),
    Stop,
}

/// Orders the channel of a node of a cell of `nodes` holds: where each peer listens, a proposal,
/// a replacement and a stop.
fn orders_bound(nodes: u64) -> usize {
    usize::try_from(nodes).unwrap() + 2
}

/// A node's task: opens the node, as the task that turns a node opens it (`Node::open`), reports
/// where it listens, then turns it, each turn raced against the test's next order, reporting its
/// status when it changed and each registration's answer as it applied.
async fn run<E: Engine + 'static>(
    settings: NodeSettings<E>,
    mut orders: ChannelReceiver<Order>,
    reports: mpsc::Sender<Report>,
) {
    let id = settings.id;
    let opened = Node::open(settings).await;
    let mut node = match opened.and_then(|node| node.addresses().map(|a| (node, a))) {
        Ok((node, (quic, plane))) => {
            if reports.send(Report::Bound(id, quic, plane)).is_err() {
                return;
            }
            node
        }
        Err(error) => {
            let _ = reports.send(Report::Failed(id, error.to_string()));
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
                let _ = reports.send(Report::Failed(id, error.to_string()));
                return;
            }
            Either::Second(Ok(Order::Learn(peer, quic, plane))) => {
                if let Err(error) = node.learn(peer, quic, plane) {
                    let _ = reports.send(Report::Failed(id, error.to_string()));
                    return;
                }
            }
            // A node that no longer leads refuses it, and the test proposes again at the leader.
            Either::Second(Ok(Order::Propose(entry))) => {
                let _ = node.propose(GROUP, &entry);
            }
            Either::Second(Ok(Order::Replace(failed, joining))) => {
                if let Err(error) = node.replace(GROUP, failed, joining) {
                    let _ = reports.send(Report::Failed(id, error.to_string()));
                    return;
                }
            }
            Either::Second(Ok(Order::Stop)) => {
                drop(node);
                let _ = reports.send(Report::Stopped(id));
                return;
            }
            // The test is done.
            Either::Second(Err(_)) => return,
        }
        for (_, replaced) in node.take_replaced() {
            let report = Report::Replaced(replaced.failed(), replaced.joining());
            if reports.send(report).is_err() {
                return;
            }
        }
        for (_, applied) in node.take_applied() {
            if applied.session == 0 {
                let report = Report::Applied(id, applied.serial, applied.index, applied.answer);
                if reports.send(report).is_err() {
                    return;
                }
            }
        }
        let status = node.status(GROUP);
        if status != told
            && let Some(now) = status
        {
            if reports.send(Report::Status(id, now)).is_err() {
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
                    if reports.send(Report::Log(id, flushing, longest)).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = reports.send(Report::Failed(id, error.to_string()));
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
    log: BTreeMap<u64, (Instant, Option<Duration>, Duration)>,
    /// The client's commands, by serial: their answers or why they had none.
    answered: BTreeMap<u64, Result<Vec<Answer>, String>>,
    /// Each workload client's operations done.
    progress: BTreeMap<u64, u64>,
    /// How long each workload client gives an exchange before it takes the node as lost.
    clients: BTreeMap<u64, Duration>,
    /// The replacements a leader ended: the member lost for good and the one in its place.
    replaced: BTreeSet<(u64, u64)>,
    /// When a node last moved.
    moved_at: Option<Instant>,
}

impl Watch {
    /// Takes reports until `done` holds, and fails once a quiet period passes with nothing moved.
    fn until<T>(
        &mut self,
        reports: &mpsc::Receiver<Report>,
        done: impl Fn(&Self) -> Option<T>,
    ) -> T {
        self.moved_at = Some(Instant::now());
        loop {
            if let Some(found) = done(self) {
                return found;
            }
            let quiet = self.quiet();
            let moved = self.moved_at.unwrap_or_else(Instant::now);
            let until = (moved + quiet).max(self.flush_deadline(quiet));
            // Checked before each report: reports that keep coming with nothing moved in them
            // (a heartbeat's count, a log's stats) end the wait all the same.
            let Some(left) = until.checked_duration_since(Instant::now()) else {
                panic!("quiet for {quiet:?} with nothing moved: {self:?}")
            };
            match reports.recv_timeout(left) {
                Ok(report) => self.take(report),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    panic!("quiet for {quiet:?} with nothing moved: {self:?}")
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("every node's task ended: {self:?}")
                }
            }
        }
    }

    fn take(&mut self, report: Report) {
        match report {
            Report::Bound(id, quic, plane) => {
                self.bound.insert(id, (quic, plane));
                self.live.insert(id);
                self.moved_at = Some(Instant::now());
            }
            Report::Status(id, now) => {
                let moved = self.status.get(&id).is_none_or(|was| {
                    (was.term, was.leader, was.commit, was.applied)
                        != (now.term, now.leader, now.commit, now.applied)
                        || was.measuring != now.measuring
                });
                if moved {
                    self.moved_at = Some(Instant::now());
                }
                self.status.insert(id, now);
            }
            Report::Applied(id, serial, index, answer) => {
                self.applied
                    .entry(id)
                    .or_default()
                    .insert(serial, (index, answer));
                self.moved_at = Some(Instant::now());
            }
            Report::Log(id, flushing, longest) => {
                self.log.insert(id, (Instant::now(), flushing, longest));
            }
            Report::Stopped(id) => {
                self.live.remove(&id);
                self.status.remove(&id);
                self.log.remove(&id);
                self.moved_at = Some(Instant::now());
            }
            Report::Failed(id, error) => {
                let what = if id >= CLIENT { "client" } else { "node" };
                panic!("{what} {id} failed: {error}; {self:?}")
            }
            Report::Answered(serial, answered) => {
                self.answered.insert(serial, answered);
                self.moved_at = Some(Instant::now());
            }
            Report::Replaced(failed, joining) => {
                self.replaced.insert((failed, joining));
                self.moved_at = Some(Instant::now());
            }
            Report::Progress(client, done, patience) => {
                self.progress.insert(client, done);
                if let Some(patience) = patience {
                    self.clients.insert(client, patience);
                }
                self.moved_at = Some(Instant::now());
            }
        }
    }

    /// When the watch ends for the flushes out: each live node's, as last heard, past the
    /// longest any node's log has taken and `quiet` besides.
    fn flush_deadline(&self, quiet: Duration) -> Instant {
        let longest = self
            .log
            .values()
            .map(|(_, _, l)| *l)
            .max()
            .unwrap_or_default();
        let allowed = longest + quiet;
        self.log
            .iter()
            .filter(|(id, _)| self.live.contains(id))
            .filter_map(|(_, (heard, flushing, _))| {
                Some(*heard + allowed.saturating_sub(*flushing.as_ref()?))
            })
            .max()
            .unwrap_or_else(Instant::now)
    }

    /// The longest any node states it may go without moving or until a peer it dials is
    /// connected again, or any client gives an exchange before it asks another node, and never
    /// less than the initial retransmission timeout.
    fn quiet(&self) -> Duration {
        self.status
            .values()
            .flat_map(|s| [s.patience, s.reconnect, s.measuring_for])
            .flatten()
            .chain(self.clients.values().copied())
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

/// The cell under test: each node's thread and the orders to it, the client's, the reports from
/// all, and what the test heard.
struct Cell {
    /// The cell's node identities, 1 to `nodes`: the range's founding voters, then any spares
    /// (`Pki::voters`).
    nodes: u64,
    engines: Engines,
    pki: Pki,
    dirs: BTreeMap<u64, tempfile::TempDir>,
    orders: BTreeMap<u64, Sender<Order>>,
    threads: BTreeMap<u64, JoinHandle<()>>,
    asks: Option<Sender<Ask>>,
    client: Option<JoinHandle<()>>,
    tell: mpsc::Sender<Report>,
    reports: mpsc::Receiver<Report>,
    watch: Watch,
    relay: Option<Relay>,
    /// Where the running workload's clients are told of a node that starts.
    learns: Vec<mpsc::Sender<(u64, SocketAddr)>>,
}

impl Drop for Cell {
    /// The orders go, so each task ends at its next turn, and every thread is joined.
    fn drop(&mut self) {
        self.asks = None;
        self.orders.clear();
        for (_, thread) in std::mem::take(&mut self.threads) {
            let _ = thread.join();
        }
        if let Some(thread) = self.client.take() {
            let _ = thread.join();
        }
    }
}

impl Cell {
    /// A cell of three nodes started, each told where the others listen, directly.
    fn start(engines: Engines) -> Self {
        Self::start_on(engines, Network::Direct, NODES)
    }

    /// A cell of `nodes` nodes started on `network`, each told where the others listen.
    fn start_on(engines: Engines, network: Network, nodes: u64) -> Self {
        Self::start_spared(engines, network, nodes, 0)
    }

    /// A cell of `voters` nodes started on `network`, each told where the others listen, and
    /// `spares` identities past them that every directory holds and no node runs yet.
    fn start_spared(engines: Engines, network: Network, voters: u64, spares: u64) -> Self {
        let nodes = voters + spares;
        let (tell, reports) = mpsc::channel();
        let mut cell = Cell {
            nodes,
            engines,
            pki: Pki::new(voters, nodes),
            dirs: (1..=nodes)
                .map(|id| (id, tempfile::tempdir().unwrap()))
                .collect(),
            orders: BTreeMap::new(),
            threads: BTreeMap::new(),
            asks: None,
            client: None,
            tell,
            reports,
            watch: Watch::default(),
            relay: (network == Network::Relayed)
                .then(|| Relay::start(&(1..=nodes).collect::<Vec<_>>())),
            learns: Vec::new(),
        };
        for id in 1..=voters {
            cell.spawn(id);
        }
        cell.introduce(&(1..=voters).collect::<Vec<_>>());
        cell
    }

    /// Starts node `id` on a thread of its own, its shard, on its directory: the engine is opened
    /// on the shard that turns its node. A spare starts as the founding voters did, with their
    /// configuration and rows, and is no member of the range until a replacement adds it.
    fn spawn(&mut self, id: u64) {
        let (order, taken) = channel(orders_bound(self.nodes)).unwrap();
        self.orders.insert(id, order);
        self.watch.bound.remove(&id);
        let dir: PathBuf = self.dirs[&id].path().to_path_buf();
        let (pki, tell, engines) = (self.pki.clone(), self.tell.clone(), self.engines);
        let sockets = node_sockets(self.nodes);
        let thread = std::thread::spawn(move || {
            // A panic on the node's thread is told as its failure, so a wait on it ends.
            let failed = tell.clone();
            let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let mut rt = shard(sockets);
                match engines {
                    Engines::Model => {
                        let settings = pki.settings(id, &dir, first_range(Model::default()));
                        rt.block_on(run(settings, taken, tell)).unwrap();
                    }
                    Engines::Mantle => {
                        // The node's shard is its metadata shard: its replicas apply through the
                        // engine's synchronous calls (node.md §1.2).
                        mantle_engine::store::permit_blocking();
                        let settings = pki.settings(id, &dir, stored(&dir));
                        rt.block_on(run(settings, taken, tell)).unwrap();
                    }
                }
            }));
            if ran.is_err() {
                let _ = failed.send(Report::Failed(id, "its thread panicked".into()));
            }
        });
        // A node started again: its last thread ended with its task.
        if let Some(last) = self.threads.insert(id, thread) {
            last.join().unwrap();
        }
    }

    fn order(&self, id: u64, order: Order) {
        self.orders[&id]
            .try_send(order)
            .map_err(|_| "a node's orders are full")
            .unwrap();
    }

    /// Waits until each of `ids` is open, then tells every live node where the others listen: where
    /// they do, or on a relayed cell, at the relay, which forwards to where they do. A node's
    /// opening (its runtime, its log's and store's files, its keys) is the operating system's and
    /// the device's work, with no law of the node's to bound it, so nothing but the node's own
    /// word ends the wait: it listens (`Report::Bound`) or it failed.
    fn introduce(&mut self, ids: &[u64]) {
        while !ids.iter().all(|id| self.watch.bound.contains_key(id)) {
            match self.reports.recv() {
                Ok(report) => self.watch.take(report),
                Err(_) => panic!("every node's task ended: {:?}", self.watch),
            }
        }
        if let Some(relay) = &self.relay {
            for id in ids {
                let (quic, plane) = self.watch.bound[id];
                relay.listen(*id, quic, plane);
            }
        }
        for id in &self.watch.live {
            for (peer, bound) in &self.watch.bound {
                if peer != id && self.watch.live.contains(peer) {
                    let (quic, plane) = self
                        .relay
                        .as_ref()
                        .map_or(*bound, |relay| relay.front(*id, *peer));
                    self.order(*id, Order::Learn(*peer, quic, plane));
                }
            }
        }
    }

    /// Cuts node `id` from every other node, both ways, or joins it to them again.
    fn isolate(&self, id: u64, cut: bool) {
        let relay = self.relay.as_ref().unwrap();
        for peer in (1..=self.nodes).filter(|peer| *peer != id) {
            relay.join(id, peer, !cut);
        }
    }

    /// Waits until the workload's clients, `ops` operations each, have done `quarters` quarters
    /// of them in all.
    fn done_by(&mut self, ops: u64, quarters: u64) {
        let done = ops * CLIENTS * quarters / 4;
        self.watch.until(&self.reports, |w| {
            if w.progress.values().sum::<u64>() >= done {
                Some(())
            } else {
                None
            }
        });
    }

    /// Waits until every live node has applied as far as the others.
    fn converged(&mut self) -> u64 {
        self.watch.until(&self.reports, |w| {
            let mut applied = w.live.iter().map(|id| w.status.get(id).map(|s| s.applied));
            let first = applied.next()??;
            if applied.all(|a| a == Some(first)) {
                Some(first)
            } else {
                None
            }
        })
    }

    fn leader(&mut self) -> u64 {
        self.watch.until(&self.reports, Watch::agreed_leader)
    }

    /// Proposes registration `serial` at the leader and waits until the leader applied it: its
    /// index and answer. A leader that steps down first is asked again where the leader is then,
    /// at most once for each node, since each step down is an election the cell moved through.
    fn write(&mut self, serial: u64) -> (u64, Answer) {
        for _ in 0..self.nodes {
            let leader = self.leader();
            self.order(leader, Order::Propose(registration(serial)));
            let heard = self.watch.until(&self.reports, |w| {
                if let Some(applied) = w.applied.get(&leader).and_then(|a| a.get(&serial)) {
                    return Some(Some(applied.clone()));
                }
                if w.status.get(&leader).is_some_and(|s| s.leader != leader) {
                    return Some(None);
                }
                None
            });
            if let Some(applied) = heard {
                return applied;
            }
        }
        panic!(
            "registration {serial} not applied through {} leaders: {:?}",
            self.nodes, self.watch
        );
    }

    fn applied_everywhere(&mut self, serial: u64) -> (u64, Answer) {
        self.watch
            .until(&self.reports, |w| w.applied_everywhere(serial))
    }

    fn stop(&mut self, id: u64) {
        self.order(id, Order::Stop);
        self.watch.until(&self.reports, |w| {
            if w.live.contains(&id) { None } else { Some(()) }
        });
    }

    /// Starts node `id` again on its directory, at new addresses every live node is told.
    fn restart(&mut self, id: u64) {
        self.spawn(id);
        self.introduce(&[id]);
    }

    /// Starts the client on a thread of its own, asking the nodes in `order`, and tells it where
    /// each live node listens.
    fn start_client(&mut self, order: &[u64]) {
        // Where each node listens, and the commands, one at a time.
        let (asks, taken) = channel(usize::try_from(self.nodes).unwrap() + 1).unwrap();
        let (settings, tell) = (self.pki.client_settings(CLIENT, order), self.tell.clone());
        let sockets = client_sockets(order.len());
        self.client = Some(std::thread::spawn(move || {
            shard(sockets)
                .block_on(client(settings, taken, tell))
                .unwrap();
        }));
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
    fn command(&mut self, serial: u64) -> Vec<Answer> {
        self.asks
            .as_ref()
            .unwrap()
            .try_send(Ask::Command(serial))
            .map_err(|_| "the client's asks are full")
            .unwrap();
        self.watch
            .until(&self.reports, |w| w.answered.get(&serial).cloned())
            .unwrap_or_else(|error| panic!("command {serial}: {error}"))
    }

    /// Where each live node's QUIC endpoint listens.
    fn bound_quic(&self) -> Vec<(u64, SocketAddr)> {
        self.watch
            .bound
            .iter()
            .filter(|(id, _)| self.watch.live.contains(id))
            .map(|(id, (quic, _))| (*id, *quic))
            .collect()
    }

    /// Opens the bucket's gate, which every put needs, through a client of its own: its session
    /// registered, the gate its first command.
    fn open_bucket(&mut self) {
        let order: Vec<u64> = (1..=self.nodes).collect();
        let (settings, bound) = (self.pki.client_settings(CLIENT, &order), self.bound_quic());
        let tell = self.tell.clone();
        let thread = std::thread::spawn(move || {
            shard(client_sockets(order.len()))
                .block_on(async move {
                    let mut client = Client::open(settings).await.unwrap();
                    for (node, quic) in bound {
                        client.learn(node, quic).unwrap();
                    }
                    let answered = history::open_bucket(&mut client, GROUP, CLIENT).await;
                    let _ = tell.send(Report::Answered(u64::MAX, answered));
                })
                .unwrap();
        });
        let answers = self
            .watch
            .until(&self.reports, |w| w.answered.get(&u64::MAX).cloned())
            .unwrap_or_else(|why| panic!("the gate's opening: {why}"));
        thread.join().unwrap();
        assert_eq!(answers, vec![Answer::Name(name::Outcome::GateMoved)]);
    }

    /// Starts the workload clients, as `plan` says, each knowing every node identity and told
    /// where the live nodes listen; the senders whose drop stops them.
    fn start_workload(&mut self, plan: Plan) -> (Vec<JoinHandle<Part>>, Vec<mpsc::Sender<()>>) {
        let order: Vec<u64> = (1..=self.nodes).collect();
        let mut threads = Vec::new();
        let mut stops = Vec::new();
        self.learns.clear();
        for id in CLIENT..CLIENT + CLIENTS {
            let (stop, stopping) = mpsc::channel();
            let (learn, learning) = mpsc::channel();
            let settings = self.pki.client_settings(id, &order);
            let (bound, tell) = (self.bound_quic(), self.tell.clone());
            let steer = Steer {
                learn: learning,
                stop: stopping,
            };
            let told = move |told: Told| {
                let _ = tell.send(match told {
                    Told::Progress(id, done, patience) => Report::Progress(id, done, patience),
                    Told::Failed(id, why) => Report::Failed(id, why),
                });
            };
            threads.push(std::thread::spawn(move || {
                workload(settings, bound, (plan, steer), shard, told)
            }));
            stops.push(stop);
            self.learns.push(learn);
        }
        (threads, stops)
    }

    /// Tells the running workload's clients where node `id` listens.
    fn tell_workload(&self, id: u64) {
        let (quic, _) = self.watch.bound[&id];
        for learn in &self.learns {
            // A client done with its operations has dropped its end.
            let _ = learn.send((id, quic));
        }
    }

    /// Stops node `id` for good: its process and its directory gone, as a node whose device
    /// failed. Its identity never runs again.
    fn lose(&mut self, id: u64) {
        self.stop(id);
        self.dirs.remove(&id);
    }

    /// Stops every live node, so each closes its log before its directory goes.
    fn close(mut self) {
        self.asks = None;
        let live: Vec<u64> = self.watch.live.iter().copied().collect();
        for id in live {
            self.stop(id);
        }
    }
}

fn elect_and_apply(engines: Engines) {
    {
        let mut cell = Cell::start(engines);
        let (index, answer) = cell.write(1);
        assert!(index > 0);
        assert!(
            matches!(answer, Answer::Registered { .. }),
            "the registration answered {answer:?}"
        );
        assert_eq!(cell.applied_everywhere(1), (index, answer));
        cell.close();
    }
}

fn write_past_room(engines: Engines) {
    {
        let mut cell = Cell::start(engines);
        let writes = past_room();
        for serial in 1..=writes {
            cell.write(serial);
        }
        cell.applied_everywhere(writes);
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
        cell.close();
    }
}

fn catch_up_by_snapshot(engines: Engines) {
    {
        let mut cell = Cell::start(engines);
        let leader = cell.leader();
        let follower = (1..=cell.nodes).rev().find(|id| *id != leader).unwrap();
        cell.stop(follower);
        let writes = past_room();
        let mut last = 0;
        for serial in 1..=writes {
            last = cell.write(serial).0;
        }
        cell.restart(follower);
        cell.watch.until(&cell.reports, |w| {
            w.status
                .get(&follower)
                .filter(|s| s.applied >= last)
                .map(|s| s.snapshots_taken)
        });
        let after = writes + 1;
        let (index, answer) = cell.write(after);
        assert_eq!(cell.applied_everywhere(after), (index, answer));
        let taken = cell.watch.status[&follower].snapshots_taken;
        assert!(taken > 0, "the follower caught up without a snapshot");
        cell.close();
    }
}

fn client_redirected(engines: Engines) {
    {
        let mut cell = Cell::start(engines);
        let leader = cell.leader();
        // The client asks a node that does not lead first.
        let mut order: Vec<u64> = (1..=cell.nodes).filter(|id| *id != leader).collect();
        order.push(leader);
        cell.start_client(&order);
        let answers = cell.command(1);
        assert!(
            matches!(answers.as_slice(), [Answer::Registered { .. }]),
            "the registration answered {answers:?}"
        );
        let (_, applied) = cell.applied_everywhere(1);
        assert_eq!(answers, vec![applied]);
        cell.close();
    }
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

/// What befalls the leader half way through the linearizable history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// It stops.
    Stop,
    /// It is cut off from the other nodes, and joined to them again three quarters through.
    Cut,
}

fn leader_cut_off(engines: Engines, nodes: u64) {
    let mut cell = Cell::start_on(engines, Network::Relayed, nodes);
    let leader = cell.leader();
    cell.write(1);
    cell.applied_everywhere(1);
    cell.isolate(leader, true);
    // The other nodes elect one of themselves, in a later term than the one cut off leads.
    let led = cell.watch.status[&leader].term;
    let others: Vec<u64> = (1..=cell.nodes).filter(|id| *id != leader).collect();
    let elected = cell.watch.until(&cell.reports, |w| {
        let named: Vec<&Status> = others.iter().filter_map(|id| w.status.get(id)).collect();
        let first = named.first()?;
        let agreed = named.len() == others.len()
            && others.contains(&first.leader)
            && first.term > led
            && named
                .iter()
                .all(|s| (s.leader, s.term) == (first.leader, first.term));
        if agreed { Some(first.leader) } else { None }
    });
    // A write the new leader applies, which the node cut off cannot have.
    cell.order(elected, Order::Propose(registration(2)));
    let applied = cell.watch.until(&cell.reports, |w| {
        let mut each = others
            .iter()
            .map(|id| w.applied.get(id).and_then(|a| a.get(&2)));
        let first = each.next()??;
        if each.all(|a| a == Some(first)) {
            Some(first.clone())
        } else {
            None
        }
    });
    assert!(
        !cell.watch.applied[&leader].contains_key(&2),
        "the node cut off applied a write its peers made without it"
    );
    // Joined again, it follows the new leader and applies the write as the others did.
    cell.isolate(leader, false);
    assert_eq!(cell.applied_everywhere(2), applied);
    assert_eq!(cell.leader(), elected);
    cell.close();
}

#[test]
fn a_leader_cut_off_from_its_followers_loses_the_lead_and_follows_once_joined_again() {
    leader_cut_off(Engines::Model, NODES);
}

#[test]
fn a_leader_cut_off_from_its_followers_loses_the_lead_and_follows_once_joined_again_on_mantles_engine()
 {
    leader_cut_off(Engines::Mantle, NODES);
}

#[test]
fn a_leader_cut_off_from_four_followers_loses_the_lead_and_follows_once_joined_again() {
    leader_cut_off(Engines::Model, MOST_NODES);
}

#[test]
fn a_leader_cut_off_from_four_followers_loses_the_lead_and_follows_once_joined_again_on_mantles_engine()
 {
    leader_cut_off(Engines::Mantle, MOST_NODES);
}

fn linearizable(engines: Engines, fault: Fault, nodes: u64) {
    let network = match fault {
        Fault::Stop => Network::Direct,
        Fault::Cut => Network::Relayed,
    };
    let mut cell = Cell::start_on(engines, network, nodes);
    let leader = cell.leader();
    cell.open_bucket();
    // Puts enough to pass the log group's room in all, so the history crosses compactions.
    let ops = workload_ops();
    let (threads, stops) = cell.start_workload(Plan {
        group: GROUP,
        ops,
        phase: 0,
        start: Instant::now(),
    });
    // Half the history before the fault, and for a cut, a quarter cut off and a quarter after.
    cell.done_by(ops, 2);
    match fault {
        Fault::Stop => cell.stop(leader),
        Fault::Cut => {
            cell.isolate(leader, true);
            cell.done_by(ops, 3);
            cell.isolate(leader, false);
        }
    }
    cell.done_by(ops, 4);
    drop(stops);
    check_histories(threads.into_iter().flat_map(|t| t.join().unwrap()));
    // Every live node, the one cut off among them, ends where the others do.
    cell.converged();
    cell.close();
}

/// Each workload client's operations: puts enough to pass the log group's room in all, so the
/// history crosses compactions.
fn workload_ops() -> u64 {
    (2 * past_room()).div_ceil(CLIENTS)
}

/// A follower lost for good, its directory with it, replaced by a node under an identity never
/// used: added as a learner, caught up, and swapped in by one joint change (replica.md §6). Then
/// an original node stops, so the range's quorum counts the new one. The history of both phases,
/// one through the loss and the replacement and one after the stop, is linearizable: no write
/// acknowledged before is lost, since a read after sees it.
fn replaced_for_good(engines: Engines) {
    let mut cell = Cell::start_spared(engines, Network::Direct, NODES, 1);
    let joining = NODES + 1;
    let leader = cell.leader();
    cell.open_bucket();
    let ops = workload_ops();
    let start = Instant::now();
    let mut history = Vec::new();
    let (threads, stops) = cell.start_workload(Plan {
        group: GROUP,
        ops,
        phase: 0,
        start,
    });
    cell.done_by(ops, 1);
    let lost = (1..=NODES).find(|id| *id != leader).unwrap();
    cell.lose(lost);
    cell.spawn(joining);
    cell.introduce(&[joining]);
    cell.tell_workload(joining);
    // Every live node is told, the spare among them: whichever leads, the spare too once it
    // votes, takes the replacement up from the configuration it finds.
    let told: Vec<u64> = cell.watch.live.iter().copied().collect();
    for id in told {
        cell.order(id, Order::Replace(lost, joining));
    }
    cell.watch.until(&cell.reports, |w| {
        if w.replaced.contains(&(lost, joining)) {
            Some(())
        } else {
            None
        }
    });
    cell.done_by(ops, 4);
    drop(stops);
    for thread in threads {
        history.extend(thread.join().unwrap());
    }
    // An original voter stops: the two left of the three are a quorum only with the new one.
    let original = (1..=NODES)
        .find(|id| *id != lost && cell.watch.live.contains(id))
        .unwrap();
    cell.stop(original);
    cell.watch.progress.clear();
    let (threads, stops) = cell.start_workload(Plan {
        group: GROUP,
        ops,
        phase: 1,
        start,
    });
    cell.done_by(ops, 4);
    drop(stops);
    for thread in threads {
        history.extend(thread.join().unwrap());
    }
    check_histories(history.into_iter());
    cell.converged();
    cell.close();
}

#[test]
fn a_follower_lost_for_good_is_replaced_and_no_acknowledged_write_is_lost() {
    replaced_for_good(Engines::Model);
}

#[test]
fn a_follower_lost_for_good_is_replaced_and_no_acknowledged_write_is_lost_on_mantles_engine() {
    replaced_for_good(Engines::Mantle);
}

#[test]
fn puts_and_reads_through_a_leader_stop_are_linearizable() {
    linearizable(Engines::Model, Fault::Stop, NODES);
}

#[test]
fn puts_and_reads_through_a_leader_stop_are_linearizable_on_mantles_engine() {
    linearizable(Engines::Mantle, Fault::Stop, NODES);
}

#[test]
fn puts_and_reads_through_a_leader_cut_off_are_linearizable() {
    linearizable(Engines::Model, Fault::Cut, NODES);
}

#[test]
fn puts_and_reads_through_a_leader_cut_off_are_linearizable_on_mantles_engine() {
    linearizable(Engines::Mantle, Fault::Cut, NODES);
}

#[test]
fn puts_and_reads_through_a_leader_stop_of_five_voters_are_linearizable() {
    linearizable(Engines::Model, Fault::Stop, MOST_NODES);
}

#[test]
fn puts_and_reads_through_a_leader_stop_of_five_voters_are_linearizable_on_mantles_engine() {
    linearizable(Engines::Mantle, Fault::Stop, MOST_NODES);
}

#[test]
fn puts_and_reads_through_a_leader_cut_off_from_five_voters_are_linearizable() {
    linearizable(Engines::Model, Fault::Cut, MOST_NODES);
}

#[test]
fn puts_and_reads_through_a_leader_cut_off_from_five_voters_are_linearizable_on_mantles_engine() {
    linearizable(Engines::Mantle, Fault::Cut, MOST_NODES);
}
