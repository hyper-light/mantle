//! Three nodes on real sockets on loopback, each a task on one hyper-rt shard as a node's process
//! runs its owner (docs/design/node.md §2): one Name range replicated over QUIC lanes and the
//! sealed datagram plane, elected by the suspicion of the node-pair liveness stream (hyper-raft
//! timing steps L-2, L-3). Nothing is told by hand: each node dials the nodes above it, keys the
//! plane from each connection's TLS exporter, measures its links from the stream's heartbeats and
//! its timer's lateness from its own waits, and the range elects from nothing once a quorum's paths
//! are measured. Asserted: a leader every node agrees on, and a registration proposed there applied
//! by every node at the same index with the same answer.
//!
//! Each node's task reports its status (`Node::status`) and its answers over a channel. The test's
//! task waits on those reports and goes on while the nodes move: a term, a leader, a commit or an
//! applied index, or, while some pair is not yet judged by a margin, the heartbeats taken. It gives
//! up once a quiet period passes with nothing moved: the longest patience any node states by its
//! own law (`Status::patience`), and never less than RFC 6298's initial retransmission timeout,
//! one second, before anything is measured (hyper-durable-e2e's rule, in one process).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hyper_log::{Config as LogConfig, Waits};
use hyper_rt::combine::{Either, race2};
use hyper_rt::futures::{now_ns, sleep, spawn_detached};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::sync::{ChannelReceiver, Sender, SyncError, channel};
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_cert::{KeyPair, Validity};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model};
use mantle_meta::name;
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command as Op, Entry, Sessioned};
use mantle_node::classes::Role;
use mantle_node::directory::{Peer, node_name};
use mantle_node::node::{Hosted, Node, NodeSettings, Status};
use mantle_range::{ConfState, Range, Settings};

/// The range every node hosts: the cell's first Name range.
const GROUP: u128 = 0x0072_616e_6765;
/// The nodes, each a founding voter of the range.
const NODES: u64 = 3;
/// RFC 6298 §2.1: the retransmission timeout before any round trip is measured, one second; the
/// least quiet period the test waits through.
const INITIAL_RTO: Duration = Duration::from_secs(1);
/// The registration's serial, by which each node's answer to it is told from others.
const NONCE: u64 = 7;
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

/// The engine of a cell's first Name range, before any entry.
fn first_range() -> Model {
    let mut m = Model::default();
    m.install(0, name::first(1).unwrap()).unwrap();
    m.persist().unwrap();
    m
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

fn registration() -> Entry {
    Entry {
        at_ns: 0,
        commands: vec![Sessioned {
            session: 0,
            serial: NONCE,
            unanswered: 0,
            command: Op::Register,
        }],
    }
}

/// The shard the test runs on, sized by what it holds: its own task and a task a node, each
/// node's wait racing its own deadline and its driver's QUIC timer, and each node waiting on its
/// endpoint's socket, a socket a peer and its plane's, to read and to write. The watchdog's budget
/// and the wheel's tick are hyper-rt-transport's test harness's (`tests/exchanges.rs`); the stream
/// measures the tick's lateness as part of its granularity.
fn runtime() -> RuntimeConfig {
    let nodes = usize::try_from(NODES).unwrap();
    let tasks = nodes + 1;
    let sockets = nodes + 1;
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: tasks,
        timers_per_shard: 2 * nodes + 1,
        interests_per_shard: interests_for(tasks).max(2 * sockets * nodes),
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

/// The cell's authority and each node's certificate and key.
struct Pki {
    authority: Vec<u8>,
    /// By node id: the certificate, DER, and the key, PKCS #8.
    nodes: BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
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
        let nodes = (1..=NODES)
            .map(|id| {
                let own = KeyPair::generate().unwrap();
                let certificate =
                    mantle_cert::node(&key, AUTHORITY, own.public_key(), &node_name(id), validity)
                        .unwrap();
                (id, (certificate, own.pkcs8().to_vec()))
            })
            .collect();
        Self { authority, nodes }
    }

    /// Node `id`'s settings: its directory, loopback addresses the kernel picks, its credentials,
    /// its peers by certificate, and the range.
    fn settings(&self, id: u64, dir: &Path) -> NodeSettings {
        let (certificate, key) = &self.nodes[&id];
        let peers: Vec<Peer> = self
            .nodes
            .iter()
            .filter(|(peer, _)| **peer != id)
            .map(|(peer, (certificate, _))| Peer {
                id: *peer,
                role: Role::Node,
                certificate: certificate.clone(),
            })
            .collect();
        // Each peer's share holds a receive chunk, the largest datagram QUIC carries, for each of
        // the node's three classes.
        let transport_bytes =
            u64::try_from(peers.len() * 3 * hyper_transport::RECEIVE_CHUNK).unwrap();
        NodeSettings {
            id,
            dir: dir.to_path_buf(),
            quic: "127.0.0.1:0".parse().unwrap(),
            plane: "127.0.0.1:0".parse().unwrap(),
            credentials: Credentials {
                chain: vec![CertificateDer::from(certificate.clone())],
                key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.clone())),
                roots: vec![CertificateDer::from(self.authority.clone())],
            },
            peers,
            addresses: Vec::new(),
            ranges: vec![Hosted {
                group: GROUP,
                range: range(),
                engine: first_range(),
            }],
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
    /// The registration applied, at its index, with its answer.
    Applied(u64, u64, Answer),
    Failed(u64, String),
}

/// What the test tells a node's task.
enum Order {
    /// Where a peer listens.
    Learn(u64, SocketAddr, SocketAddr),
    Propose(Entry),
}

/// A node's task: opens the node, as the task that turns a node opens it (`Node::open`), reports
/// where it listens, then turns it, each turn raced against the test's next order, reporting its
/// status when it changed and the registration's answer when it applied. A report the channel has
/// no room for is sent after the next turn.
async fn run(settings: NodeSettings, mut orders: ChannelReceiver<Order>, reports: Sender<Report>) {
    let id = settings.id;
    let opened = Node::open(settings).await;
    let mut node = match opened.and_then(|node| node.addresses().map(|a| (node, a))) {
        Ok((node, (quic, plane))) => {
            if reports.try_send(Report::Bound(id, quic, plane)).is_err() {
                return;
            }
            node
        }
        Err(error) => {
            let _ = reports.try_send(Report::Failed(id, error.to_string()));
            return;
        }
    };
    let mut told: Option<Status> = None;
    let mut unsent: VecDeque<Report> = VecDeque::new();
    loop {
        let ended = {
            let turn = pin!(node.turn());
            let order = pin!(orders.recv());
            race2(turn, order).await
        };
        match ended {
            Either::First(Ok(())) => {}
            Either::First(Err(error)) => {
                let _ = reports.try_send(Report::Failed(id, error.to_string()));
                return;
            }
            Either::Second(Ok(Order::Learn(peer, quic, plane))) => node.learn(peer, quic, plane),
            // A node that no longer leads refuses it, and the test proposes again at the leader.
            Either::Second(Ok(Order::Propose(entry))) => {
                let _ = node.propose(GROUP, &entry);
            }
            // The test is done.
            Either::Second(Err(_)) => return,
        }
        for (_, applied) in node.take_applied() {
            if applied.session == 0 && applied.serial == NONCE {
                unsent.push_back(Report::Applied(id, applied.index, applied.answer));
            }
        }
        while let Some(report) = unsent.pop_front() {
            match reports.try_send(report) {
                Ok(()) => {}
                Err(SyncError::Full(report)) => {
                    unsent.push_front(report);
                    break;
                }
                Err(_) => return,
            }
        }
        let status = node.status(GROUP);
        if status != told
            && let Some(now) = status
            && reports.try_send(Report::Status(id, now)).is_ok()
        {
            told = status;
        }
    }
}

/// What the test has heard of the nodes.
#[derive(Debug, Default)]
struct Watch {
    bound: BTreeMap<u64, (SocketAddr, SocketAddr)>,
    status: BTreeMap<u64, Status>,
    answers: BTreeMap<u64, (u64, Answer)>,
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
            Report::Applied(id, index, answer) => {
                self.answers.insert(id, (index, answer));
                self.moved_at = now_ns();
            }
            Report::Failed(id, error) => panic!("node {id} stopped: {error}"),
        }
    }

    /// The longest any node states it may go without moving, and never less than the initial
    /// retransmission timeout.
    fn quiet(&self) -> Duration {
        self.status
            .values()
            .filter_map(|s| s.patience)
            .fold(INITIAL_RTO, Duration::max)
    }

    /// Where every node listens, once each is open.
    fn all_bound(&self) -> Option<Vec<(u64, SocketAddr, SocketAddr)>> {
        if self.bound.len() == usize::try_from(NODES).unwrap() {
            Some(
                self.bound
                    .iter()
                    .map(|(id, (q, p))| (*id, *q, *p))
                    .collect(),
            )
        } else {
            None
        }
    }

    /// The leader every node names, in one term.
    fn agreed_leader(&self) -> Option<u64> {
        let first = self.status.values().next()?;
        let agreed = self.status.len() == usize::try_from(NODES).unwrap()
            && first.leader != 0
            && self
                .status
                .values()
                .all(|s| s.leader == first.leader && s.term == first.term);
        if agreed { Some(first.leader) } else { None }
    }

    /// The registration's index and answer, once every node applied it.
    fn applied_everywhere(&self) -> Option<(u64, Answer)> {
        let first = self.answers.values().next()?;
        let all = self.answers.len() == usize::try_from(NODES).unwrap()
            && self.answers.values().all(|a| a == first);
        if all { Some(first.clone()) } else { None }
    }
}

#[test]
fn three_nodes_elect_by_suspicion_and_apply_a_registration_everywhere() {
    let pki = Pki::new();
    let dirs: Vec<tempfile::TempDir> = (0..NODES).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut rt = LocalRuntime::new(&runtime()).unwrap();
    let (leader, (index, answer)) = rt
        .block_on(async move {
            let nodes = usize::try_from(NODES).unwrap();
            let (tell, mut reports) = channel(nodes * 2).unwrap();
            let mut orders = BTreeMap::new();
            for (id, dir) in (1..=NODES).zip(&dirs) {
                // Each node is told where its peers listen and, at the leader, one proposal.
                let (order, taken) = channel(nodes).unwrap();
                orders.insert(id, order);
                spawn_detached(run(pki.settings(id, dir.path()), taken, tell.clone())).unwrap();
            }
            drop(tell);
            let mut watch = Watch::default();
            let bound = watch.until(&mut reports, Watch::all_bound).await;
            for (id, order) in &orders {
                for &(peer, quic, plane) in bound.iter().filter(|(peer, ..)| peer != id) {
                    order
                        .try_send(Order::Learn(peer, quic, plane))
                        .map_err(|_| "a node's orders are full")
                        .unwrap();
                }
            }
            let leader = watch.until(&mut reports, Watch::agreed_leader).await;
            orders[&leader]
                .try_send(Order::Propose(registration()))
                .map_err(|_| "the leader's orders are full")
                .unwrap();
            let applied = watch.until(&mut reports, Watch::applied_everywhere).await;
            drop(dirs);
            (leader, applied)
        })
        .unwrap();
    assert!((1..=NODES).contains(&leader));
    assert!(index > 0);
    assert!(
        matches!(answer, Answer::Registered { .. }),
        "the registration answered {answer:?}"
    );
}
