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
//!   the others elect and apply a write it does not have, and the cut holds until each node that
//!   dials across it has seen its connection close and then two redials end unanswered, each end
//!   told to it (a dial that fails as the last one did is still the owner's to hear of, or it never
//!   dials again); joined again, it follows the new leader and applies the write as they did.
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
//! applied index, an answer, a node opened or stopped, a peer's link (a dial, or one ended:
//! `Node::links`), or the heartbeats taken from the peers not yet judged by a margin
//! (`Status::measuring`), which stop if a peer measured goes silent. It
//! gives up once a quiet period passes with nothing moved: the
//! longest patience any node states by its own law (`Status::patience`), or the longest a link to
//! a peer a node dials may go without moving should that peer fall silent (`Status::reconnect`:
//! a cut's first movement is its connections' idle timeout), or an ask the node itself waits on
//! paused (`Status::asking`), or any client gives an exchange by
//! its own (`Client::patience`), and never less than RFC 6298's initial retransmission timeout, one
//! second, before anything is measured. A node
//! whose log has a flush out extends the watch, since its range moves only as its writes become
//! durable, until the flush has been out longer than the longest any node's log has taken and the
//! quiet period besides (hyper-durable-e2e's rule, in one process). A cell no majority of whose
//! live nodes is free of its own device holds still for its devices (`Node::held`: a node
//! judges no peer while its own proof is out, timing.md §2.8), and the watch does not count that
//! time, each such stretch up to the longest any node's log has taken and the quiet period
//! besides, as `crates/mantle/tests/node.rs`'s does. A node's shard is its own, as
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
use hyper_transport::Refusal;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_chunk::{ChunkKey, Config as VolumeConfig, Limits as VolumeLimits, Reads};
use mantle_engine::store::Config as StoreConfig;
use mantle_engine::trunk::TrunkConfig;
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model};
use mantle_meta::name;
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command, Entry};
use mantle_node::ask::{CommandOutcome, ReadOutcome, Wait};
use mantle_node::chunk::Status as ChunkStatus;
use mantle_node::client::{Client, ClientSettings};
use mantle_node::directory::Peer;
use mantle_node::node::{
    ChunkAsk, ChunkOutcome, Hosted, Link, Node, NodeSettings, Status, Wall, message_bounds,
};
use mantle_node::read::{Query, Read};
use mantle_node::storage::{Storage, VolumeSettings};
use mantle_protocol::classes::Role;
use mantle_protocol::node_name;
use mantle_range::{ConfState, Range, Settings, Stored};

#[allow(dead_code)]
#[path = "support/history.rs"]
mod history;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
use history::{
    Clock, Part, Plan, Steer, Told, check_histories, open_gate, put, registration, workload,
};
#[allow(dead_code)]
#[path = "support/relay.rs"]
mod relay;
use relay::Relay;

/// The range every node hosts: the cell's first Name range.
const GROUP: u128 = 0x0072_616e_6765;
/// The largest chunk the cell's gateways write, the transfer class's bound with its head:
/// `mantle_gateway::layout::CHUNK`, Tectonic's 8 MiB. Every node and client of the process states
/// the same bounds.
const CHUNK_BYTES: u64 = 8 << 20;
/// The chunk puts and gets a node keeps out to its peers at once, and the most chunk orders the
/// test keeps out to one node.
const CHUNK_ASKS: usize = 4;
/// The commands a node asks of ranges itself and holds at once, and the most command orders the
/// test keeps out to one node.
const COMMAND_ASKS: usize = 8;
/// The reads a node asks of ranges itself and holds at once, and the most read orders the test
/// keeps out to one node.
const READ_ASKS: usize = 8;
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
    // Set by `rules`, for the store the cell's mantle engines run with.
    max_key_bytes: 0,
    max_value_bytes: 0,
};

/// The range's rules: a row's bounds the most of an equal key and value that the store and its
/// memtable of a page hold (`mantle_range::holds_rows`), so either engine takes the range alike.
fn rules() -> Rules {
    let bound = |x| Rules {
        max_key_bytes: x,
        max_value_bytes: x,
        ..RULES
    };
    let held = (0..=STORE.page_size)
        .rev()
        .find(|&x| mantle_range::holds_rows(STORE.page_size, STORE.page_size, &bound(x)))
        .unwrap();
    bound(held)
}

/// The range of a cell of `nodes`, each a founding voter.
fn range(nodes: u64) -> Range {
    Range {
        layer: Layer::Name,
        rules: rules(),
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
        // The least that holds a ready of the range's settings, so writes pass it.
        group_bytes: mantle_range::ready_held(&SETTINGS).unwrap().1,
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
        let drawn = || Serial::draw().unwrap();
        let authority = mantle_cert::authority(&key, AUTHORITY, validity, drawn()).unwrap();
        let issue = |name: &str| {
            let own = KeyPair::generate().unwrap();
            let certificate =
                mantle_cert::node(&key, AUTHORITY, own.public_key(), name, (validity, drawn()))
                    .unwrap();
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
            role: Role::Gateway,
            messages: message_bounds(
                &[hosted(u64::try_from(order.len()).unwrap(), ())],
                CHUNK_BYTES,
            )
            .unwrap(),
            transport_bytes,
            seed: None,
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
            role: Role::Gateway,
            certificate: certificate.clone(),
        }));
        // Each peer's share holds a receive chunk, the largest datagram QUIC carries, for each of
        // the node's four classes.
        let transport_bytes =
            u64::try_from(peers.len() * 4 * hyper_transport::RECEIVE_CHUNK).unwrap();
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
            seed: None,
            storage: Storage::none(),
            chunk_bytes: CHUNK_BYTES,
            chunk_asks: CHUNK_ASKS,
            command_asks: COMMAND_ASKS,
            read_asks: READ_ASKS,
            wall: Wall::System,
            gateway: None,
            // These nodes enlist nothing, so the cell keeps no history of them: every start's
            // detectors begin from what they watch.
            history: mantle_meta::record::History::default(),
        }
    }
}

/// What a node's task tells the test.
#[derive(Debug)]
enum Report {
    /// Where the node listens, once open: QUIC's address and the plane's.
    Bound(u64, SocketAddr, SocketAddr),
    Status(u64, Box<Status>),
    /// Whether the node waits on its own device, as it changes (`Node::held`).
    Held(u64, bool),
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
    /// A node's link to a peer as it changed: dialed, keyed, or a connection or dial ended.
    Link(u64, Link),
    /// The volumes a node holds, once open.
    Volumes(u64, Vec<u128>),
    /// A chunk's put or get answered, under the test's tag.
    Chunk(u64, ChunkOutcome),
    /// A command a node asked itself answered, under the test's tag.
    Command(u64, CommandOutcome),
    /// A read a node asked itself answered, under the test's tag.
    Read(u64, ReadOutcome),
    /// The current version of a key, as the client read it: its order, or none.
    Current(String, Option<u64>),
}

/// What the test tells the client's task.
enum Ask {
    /// Where a node's QUIC endpoint listens.
    Learn(u64, SocketAddr),
    /// Registration `serial`, of the range.
    Command(u64),
    /// The current version of a key of bucket `b`.
    Current(String),
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
                    .command(GROUP, &registration(CLIENT, 1, serial), u64::MAX)
                    .await
                    .map_err(|e| e.to_string());
                if reports.send(Report::Answered(serial, answered)).is_err() {
                    return;
                }
            }
            Ask::Current(key) => {
                let query = Query::Current {
                    bucket: "b".into(),
                    key: key.clone(),
                };
                let order = match client.read(GROUP, &query, u64::MAX).await {
                    Ok(Read::Current(current)) => current.map(|(order, _)| order),
                    other => {
                        let _ = reports.send(Report::Failed(CLIENT, format!("{other:?}")));
                        return;
                    }
                };
                if reports.send(Report::Current(key, order)).is_err() {
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
    /// A chunk's put or get for the node to ask, under the test's tag.
    Chunk(u64, Chunked),
    /// A command for the node to ask the range itself, under the test's tag.
    Command(u64, Command),
    /// A read for the node to ask the range itself, under the test's tag.
    Read(u64, Query),
    Stop,
}

/// A chunk's put or get a node asks (`Node::put_chunk`, `Node::get_chunk`): of the volume, which
/// the node `at` holds, itself or a peer.
#[derive(Debug)]
enum Chunked {
    Put {
        at: u64,
        volume: u128,
        key: ChunkKey,
        bytes: Vec<u8>,
        crc32c: u32,
    },
    Get {
        at: u64,
        volume: u128,
        key: ChunkKey,
        offset: u64,
        len: u64,
    },
}

/// Orders the channel of a node of a cell of `nodes` holds: where each peer listens, a proposal,
/// a replacement and a stop, and the chunks and commands the test keeps out to it.
fn orders_bound(nodes: u64) -> usize {
    usize::try_from(nodes).unwrap() + 2 + CHUNK_ASKS + COMMAND_ASKS
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
            let volumes = node.volumes().map(|(volume, _)| volume).collect();
            if reports.send(Report::Volumes(id, volumes)).is_err() {
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
    let mut told_held: Option<bool> = None;
    let mut stats = None;
    // The test's tag for each chunk asked, and each command, by the number the node asked it under.
    let mut tags: BTreeMap<ChunkAsk, u64> = BTreeMap::new();
    let mut command_tags: BTreeMap<u64, u64> = BTreeMap::new();
    let mut read_tags: BTreeMap<u64, u64> = BTreeMap::new();
    // Each peer's link as last reported, a slot a peer.
    let mut links: Vec<Link> = Vec::new();
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
            Either::Second(Ok(Order::Propose(mut entry))) => {
                let _ = node.propose(GROUP, &mut entry);
            }
            Either::Second(Ok(Order::Replace(failed, joining))) => {
                if let Err(error) = node.replace(GROUP, failed, joining) {
                    let _ = reports.send(Report::Failed(id, error.to_string()));
                    return;
                }
            }
            Either::Second(Ok(Order::Chunk(tag, chunked))) => {
                let asked = match chunked {
                    Chunked::Put {
                        at,
                        volume,
                        key,
                        bytes,
                        crc32c,
                    } => node.put_chunk(at, volume, key, bytes, crc32c),
                    Chunked::Get {
                        at,
                        volume,
                        key,
                        offset,
                        len,
                    } => node.get_chunk(at, volume, key, offset, len),
                };
                match asked {
                    Ok(asked) => {
                        tags.insert(asked, tag);
                    }
                    Err(error) => {
                        let _ = reports.send(Report::Failed(id, error.to_string()));
                        return;
                    }
                }
            }
            Either::Second(Ok(Order::Command(tag, command))) => {
                match node.command(GROUP, command, u64::MAX, Wait::Request) {
                    Ok(asked) => {
                        command_tags.insert(asked, tag);
                    }
                    Err(error) => {
                        let _ = reports.send(Report::Failed(id, error.to_string()));
                        return;
                    }
                }
            }
            Either::Second(Ok(Order::Read(tag, query))) => {
                match node.read(GROUP, query, u64::MAX, Wait::Request) {
                    Ok(asked) => {
                        read_tags.insert(asked, tag);
                    }
                    Err(error) => {
                        let _ = reports.send(Report::Failed(id, error.to_string()));
                        return;
                    }
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
        let mut answered = Vec::new();
        tags.retain(|asked, tag| match node.take_chunk(*asked) {
            Some(outcome) => {
                answered.push(Report::Chunk(*tag, outcome));
                false
            }
            None => true,
        });
        command_tags.retain(|asked, tag| match node.take_command(*asked) {
            Some(outcome) => {
                answered.push(Report::Command(*tag, outcome));
                false
            }
            None => true,
        });
        read_tags.retain(|asked, tag| match node.take_read(*asked) {
            Some(outcome) => {
                answered.push(Report::Read(*tag, outcome));
                false
            }
            None => true,
        });
        for report in answered {
            if reports.send(report).is_err() {
                return;
            }
        }
        for (at, now) in node.links().enumerate() {
            if links.get(at) == Some(&now) {
                continue;
            }
            if reports.send(Report::Link(id, now)).is_err() {
                return;
            }
            match links.get_mut(at) {
                Some(was) => *was = now,
                None => links.push(now),
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
        let held = node.held();
        if told_held != Some(held) {
            if reports.send(Report::Held(id, held)).is_err() {
                return;
            }
            told_held = Some(held);
        }
        let status = node.status(GROUP);
        if status != told
            && let Some(now) = status
        {
            if reports.send(Report::Status(id, Box::new(now))).is_err() {
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
    /// The volumes each node said it holds when it opened.
    volumes: BTreeMap<u64, Vec<u128>>,
    /// The chunk puts and gets answered, by the test's tag.
    chunks: BTreeMap<u64, ChunkOutcome>,
    /// The commands nodes asked themselves, answered, by the test's tag, and the current versions
    /// the client read, by key.
    commands: BTreeMap<u64, CommandOutcome>,
    currents: BTreeMap<String, Option<u64>>,
    /// The reads nodes asked themselves, answered, by the test's tag.
    reads: BTreeMap<u64, ReadOutcome>,
    /// The nodes open and not stopped.
    live: BTreeSet<u64>,
    status: BTreeMap<u64, Status>,
    /// The live nodes whose last word was that they wait on their own device.
    held: BTreeSet<u64>,
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
    /// Each live node's link to each peer, as it last reported it, by (node, peer).
    links: BTreeMap<(u64, u64), Link>,
    /// When a node last moved.
    moved_at: Option<Instant>,
    /// Since the last movement, how long the cell held still for its devices, each stretch at
    /// most `Watch::held_bound`, and when the stretch under way began, if one is.
    held_for: Duration,
    held_since: Option<Instant>,
}

impl Watch {
    /// Takes reports until `done` holds, and fails once a quiet period passes with nothing moved.
    fn until<T>(
        &mut self,
        reports: &mpsc::Receiver<Report>,
        done: impl Fn(&Self) -> Option<T>,
    ) -> T {
        self.moved();
        loop {
            if let Some(found) = done(self) {
                return found;
            }
            let quiet = self.quiet();
            let until = self.deadline(quiet).max(self.flush_deadline(quiet));
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
                self.moved();
            }
            Report::Status(id, now) => {
                let moved = self.status.get(&id).is_none_or(|was| {
                    (was.term, was.leader, was.commit, was.applied)
                        != (now.term, now.leader, now.commit, now.applied)
                        || was.measuring != now.measuring
                        || was.asking != now.asking
                });
                self.status.insert(id, *now);
                // A status whose change moved nothing (its patience, say) moves nothing.
                if moved {
                    self.moved();
                }
            }
            Report::Held(id, held) => {
                // Whether the node waits on its device begins or ends a stretch held still.
                if held {
                    self.held.insert(id);
                } else {
                    self.held.remove(&id);
                }
                self.hold();
            }
            Report::Applied(id, serial, index, answer) => {
                self.applied
                    .entry(id)
                    .or_default()
                    .insert(serial, (index, answer));
                self.moved();
            }
            Report::Log(id, flushing, longest) => {
                self.log.insert(id, (Instant::now(), flushing, longest));
            }
            Report::Stopped(id) => {
                self.live.remove(&id);
                self.status.remove(&id);
                self.held.remove(&id);
                self.log.remove(&id);
                self.links.retain(|(of, _), _| *of != id);
                self.moved();
            }
            Report::Failed(id, error) => {
                let what = if id >= CLIENT { "client" } else { "node" };
                panic!("{what} {id} failed: {error}; {self:?}")
            }
            Report::Answered(serial, answered) => {
                self.answered.insert(serial, answered);
                self.moved();
            }
            Report::Replaced(failed, joining) => {
                self.replaced.insert((failed, joining));
                self.moved();
            }
            Report::Link(id, link) => {
                self.links.insert((id, link.peer), link);
                self.moved();
            }
            Report::Volumes(id, volumes) => {
                self.volumes.insert(id, volumes);
                self.moved();
            }
            Report::Chunk(tag, outcome) => {
                self.chunks.insert(tag, outcome);
                self.moved();
            }
            Report::Command(tag, outcome) => {
                self.commands.insert(tag, outcome);
                self.moved();
            }
            Report::Read(tag, outcome) => {
                self.reads.insert(tag, outcome);
                self.moved();
            }
            Report::Current(key, order) => {
                self.currents.insert(key, order);
                self.moved();
            }
            Report::Progress(client, done, patience) => {
                self.progress.insert(client, done);
                if let Some(patience) = patience {
                    self.clients.insert(client, patience);
                }
                self.moved();
            }
        }
    }

    /// How long one stretch of the cell holding still for its devices goes uncounted: the longest
    /// any node's log has taken and the quiet period besides.
    fn held_bound(&self, quiet: Duration) -> Duration {
        let longest = self
            .log
            .values()
            .map(|(_, _, longest)| *longest)
            .max()
            .unwrap_or_default();
        longest + quiet
    }

    /// Whether no majority of the live nodes is free of its own device: the range's quorum then
    /// waits on the devices of the nodes that wait on theirs.
    fn held_by_devices(&self) -> bool {
        let held = self.live.iter().filter(|id| self.held.contains(id)).count();
        held > 0 && 2 * held >= self.live.len()
    }

    /// Begins or ends a stretch of the cell holding still for its devices, as it now stands.
    fn hold(&mut self) {
        let now = Instant::now();
        match (self.held_since, self.held_by_devices()) {
            (None, true) => self.held_since = Some(now),
            (Some(since), false) => {
                let bound = self.held_bound(self.quiet());
                self.held_for += now.saturating_duration_since(since).min(bound);
                self.held_since = None;
            }
            (None, false) | (Some(_), true) => {}
        }
    }

    /// Something moved: the watch begins again.
    fn moved(&mut self) {
        let now = Instant::now();
        self.moved_at = Some(now);
        self.held_for = Duration::ZERO;
        self.held_since = if self.held_by_devices() {
            Some(now)
        } else {
            None
        };
    }

    /// When the watch ends: `quiet` after the last movement, past the stretches the cell held still
    /// for its devices, the one under way counted to its bound.
    fn deadline(&self, quiet: Duration) -> Instant {
        let holding = if self.held_since.is_some() {
            self.held_bound(quiet)
        } else {
            Duration::ZERO
        };
        self.moved_at.unwrap_or_else(Instant::now) + quiet + self.held_for + holding
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

    /// The longest any node states it may go without moving or a link to a peer it dials may,
    /// should that peer fall silent, or any client gives an exchange before it asks another node,
    /// and never less than the initial retransmission timeout.
    fn quiet(&self) -> Duration {
        self.status
            .values()
            .flat_map(|s| [s.patience, s.reconnect, s.measuring_for, s.asking])
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
    /// The volume each node keeps, in its directory's `device`, in a cell whose nodes store
    /// chunks.
    volume: Option<VolumeSettings>,
    /// The next tag a chunk order is asked under.
    next_tag: u64,
    /// The nodes told a chunk bound other than the cell's, by id.
    chunk_bytes: BTreeMap<u64, u64>,
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
        Self::start_spared(engines, network, nodes, 0, None)
    }

    /// A cell of `voters` nodes started on `network`, each told where the others listen, and
    /// `spares` identities past them that every directory holds and no node runs yet.
    fn start_spared(
        engines: Engines,
        network: Network,
        voters: u64,
        spares: u64,
        volume: Option<VolumeSettings>,
    ) -> Self {
        Self::start_bounded(engines, network, voters, spares, volume, BTreeMap::new())
    }

    /// `start_spared`, the nodes `chunk_bytes` names told those chunk bounds.
    fn start_bounded(
        engines: Engines,
        network: Network,
        voters: u64,
        spares: u64,
        volume: Option<VolumeSettings>,
        chunk_bytes: BTreeMap<u64, u64>,
    ) -> Self {
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
            volume,
            next_tag: 0,
            chunk_bytes,
        };
        for id in 1..=voters {
            cell.spawn(id);
        }
        cell.introduce(&(1..=voters).collect::<Vec<_>>());
        cell
    }

    /// A cell of three nodes on `engines`, each keeping one volume as `volume` states it, in its
    /// directory.
    fn start_stored(engines: Engines, volume: VolumeSettings) -> Self {
        Self::start_spared(engines, Network::Direct, NODES, 0, Some(volume))
    }

    /// The volume node `id` holds, once it says so.
    fn volume_of(&mut self, id: u64) -> u128 {
        self.watch.until(&self.reports, |w| {
            w.volumes.get(&id).and_then(|v| v.first().copied())
        })
    }

    /// Orders node `from` to ask a chunk's put or get, and returns the tag its answer comes under.
    fn ask_chunk(&mut self, from: u64, chunked: Chunked) -> u64 {
        let tag = self.next_tag;
        self.next_tag += 1;
        self.order(from, Order::Chunk(tag, chunked));
        tag
    }

    /// Waits until node `from` holds a connection to `to`: its link to it keyed.
    fn connected(&mut self, from: u64, to: u64) {
        self.watch.until(&self.reports, |w| {
            w.links
                .get(&(from, to))
                .filter(|link| link.keyed)
                .map(|_| ())
        });
    }

    /// Waits until node `from` holds a connection to `to` that it dialed after its dial count to `to`
    /// passed `dials`: one to `to`'s new process, and not the one it held to its former process,
    /// whose last report says keyed until `from` learns `to` moved and closes it.
    fn connected_anew(&mut self, from: u64, to: u64, dials: u64) {
        self.watch.until(&self.reports, |w| {
            w.links
                .get(&(from, to))
                .filter(|link| link.keyed && link.dials > dials)
                .map(|_| ())
        });
    }

    /// Waits until node `from` holds a keyed connection to `to` and its detectors trust `to`: what
    /// a chunk read of `to` needs, one of a peer they suspect being lost at once
    /// (`Node::end_chunk_gets`).
    fn trusted(&mut self, from: u64, to: u64) {
        self.watch.until(&self.reports, |w| {
            w.links
                .get(&(from, to))
                .filter(|link| link.keyed && !link.suspect)
                .map(|_| ())
        });
    }

    /// What became of the chunk's put or get asked under `tag`, once it is answered.
    fn chunk(&mut self, tag: u64) -> ChunkOutcome {
        self.watch
            .until(&self.reports, |w| w.chunks.get(&tag).cloned())
    }

    /// Node `from` puts `bytes` as `key`'s chunk on `volume`, which `at` holds, and waits for what
    /// became of it.
    fn put_chunk(
        &mut self,
        from: u64,
        at: u64,
        volume: u128,
        key: ChunkKey,
        bytes: &[u8],
    ) -> ChunkOutcome {
        let tag = self.ask_chunk(
            from,
            Chunked::Put {
                at,
                volume,
                key,
                bytes: bytes.to_vec(),
                crc32c: mantle_crc::crc32c(bytes),
            },
        );
        self.chunk(tag)
    }

    /// Node `from` gets bytes `[offset, offset + len)` of `key`'s chunk from `volume`, which `at`
    /// holds, and waits for what became of it.
    fn get_chunk(
        &mut self,
        from: u64,
        at: u64,
        volume: u128,
        key: ChunkKey,
        offset: u64,
        len: u64,
    ) -> ChunkOutcome {
        let tag = self.ask_chunk(
            from,
            Chunked::Get {
                at,
                volume,
                key,
                offset,
                len,
            },
        );
        self.chunk(tag)
    }

    /// Node `from`'s read of `key` on `at`'s `volume`, as `get_chunk`, once `from` trusts `at`,
    /// and asked again each time it is lost to a suspicion: on a loaded host the detectors may
    /// suspect a live peer, a mistake they allow (hyper-liveness's `PairReport::allowance`), and a
    /// read of a suspected peer is lost at once (`Node::end_chunk_gets`). A loss counts as a
    /// suspicion's only once `from`'s link to `at` reports one more suspicion than when the read
    /// was asked; lost for any other cause, it stands.
    fn read_back(
        &mut self,
        from: u64,
        at: u64,
        volume: u128,
        key: ChunkKey,
        offset: u64,
        len: u64,
    ) -> ChunkOutcome {
        if from == at {
            return self.get_chunk(from, at, volume, key, offset, len);
        }
        loop {
            self.trusted(from, at);
            let before = self.watch.links.get(&(from, at)).map_or(0, |l| l.suspected);
            let outcome = self.get_chunk(from, at, volume, key, offset, len);
            if outcome != ChunkOutcome::Lost(Refusal::Closed) {
                return outcome;
            }
            self.watch.until(&self.reports, |w| {
                w.links
                    .get(&(from, at))
                    .filter(|link| link.suspected > before)
                    .map(|_| ())
            });
        }
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
        let volume = self.volume.clone().map(|template| VolumeSettings {
            dir: dir.join("device"),
            ..template
        });
        let chunk_bytes = self.chunk_bytes.get(&id).copied().unwrap_or(CHUNK_BYTES);
        let thread = std::thread::spawn(move || {
            // A panic on the node's thread is told as its failure, so a wait on it ends.
            let failed = tell.clone();
            let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                // Its volume opens before its runtime runs: a volume's recovery waits on its
                // device, which no shard may.
                let storage = match volume {
                    Some(volume) => Storage::open(&[volume]).unwrap(),
                    None => Storage::none(),
                };
                let mut rt = shard(sockets);
                match engines {
                    Engines::Model => {
                        let mut settings = pki.settings(id, &dir, first_range(Model::default()));
                        settings.storage = storage;
                        settings.chunk_bytes = chunk_bytes;
                        rt.block_on(run(settings, taken, tell)).unwrap();
                    }
                    Engines::Mantle => {
                        // The node's shard is its metadata shard: its replicas apply through the
                        // engine's synchronous calls (node.md §1.2).
                        mantle_engine::store::permit_blocking();
                        let mut settings = pki.settings(id, &dir, stored(&dir));
                        settings.storage = storage;
                        settings.chunk_bytes = chunk_bytes;
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

    /// Waits until a margin judges every peer of every live node (`Status::judged`): a failover
    /// from here is one the nodes' detectors judge, which a node's own steps wait out
    /// (node.md §5.4).
    fn judging(&mut self) {
        self.watch.until(&self.reports, |w| {
            let all = w
                .live
                .iter()
                .all(|id| w.status.get(id).is_some_and(|s| s.judged));
            if all { Some(()) } else { None }
        });
    }

    /// Proposes registration `serial` at the leader and waits until the leader applied it: its
    /// index and answer. A leader that steps down first is asked again where the leader is then,
    /// at most once for each node, since each step down is an election the cell moved through.
    fn write(&mut self, serial: u64) -> (u64, Answer) {
        for _ in 0..self.nodes {
            let leader = self.leader();
            self.order(leader, Order::Propose(registration(serial, 1, serial)));
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

    /// Node `from` asks `command` of the range itself (`Node::command`), under the tag returned.
    fn ask_command(&mut self, from: u64, command: Command) -> u64 {
        let tag = self.next_tag;
        self.next_tag += 1;
        self.order(from, Order::Command(tag, command));
        tag
    }

    /// Node `from` reads `query` of the range itself (`Node::read`), under the tag returned.
    fn ask_read(&mut self, from: u64, query: Query) -> u64 {
        let tag = self.next_tag;
        self.next_tag += 1;
        self.order(from, Order::Read(tag, query));
        tag
    }

    /// What the read asked under `tag` read, once it is answered.
    fn read_of(&mut self, tag: u64) -> ReadOutcome {
        self.watch
            .until(&self.reports, |w| w.reads.get(&tag).cloned())
    }

    /// What became of the command asked under `tag`, once it is answered.
    fn commanded(&mut self, tag: u64) -> CommandOutcome {
        self.watch
            .until(&self.reports, |w| w.commands.get(&tag).cloned())
    }

    /// The current version of `key`, as the client reads it: its order, or none.
    fn current(&mut self, key: &str) -> Option<u64> {
        self.watch.currents.remove(key);
        self.asks
            .as_ref()
            .unwrap()
            .try_send(Ask::Current(key.to_owned()))
            .map_err(|_| "the client's asks are full")
            .unwrap();
        self.watch
            .until(&self.reports, |w| w.currents.get(key).copied())
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
            // Until it measures an exchange, a client's patience is the idle timeout, the least
            // `Client::patience` states once it has (RFC 9000 §10.1).
            self.watch
                .clients
                .insert(id, mantle_protocol::bounds::idle_timeout());
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

/// A node's own commands (`Node::command`) and reads (`Node::read`): the gate asked by the leader
/// of its own member, puts asked by a follower of the leader over QUIC, and more puts with the
/// leader stopped while they are out, which go to the next leader with their serials the same.
/// Each put is answered with the version it made, which the client, the follower and the node that
/// leads then (in-process) each read as its key's current version: a put applied twice would have
/// made a newer one.
fn own_commands(engines: Engines) {
    {
        let mut cell = Cell::start(engines);
        let leader = cell.leader();
        let follower = (1..=cell.nodes).find(|id| *id != leader).unwrap();
        let gate = cell.ask_command(leader, open_gate());
        assert_eq!(
            cell.commanded(gate),
            CommandOutcome::Answered(Answer::Name(name::Outcome::GateMoved))
        );
        let half = COMMAND_ASKS as u64 / 2;
        let put_by = |cell: &mut Cell, n: u64| {
            let key = format!("k{n}");
            let tag = cell.ask_command(follower, put(&key, &format!("e{n}"), u128::from(n) + 1));
            (key, tag)
        };
        let first: Vec<_> = (0..half).map(|n| put_by(&mut cell, n)).collect();
        let mut answered: Vec<(String, CommandOutcome)> = first
            .into_iter()
            .map(|(key, tag)| (key, cell.commanded(tag)))
            .collect();
        // The failover these puts wait out is one the nodes' detectors judge.
        cell.judging();
        let second: Vec<_> = (half..2 * half).map(|n| put_by(&mut cell, n)).collect();
        cell.stop(leader);
        answered.extend(
            second
                .into_iter()
                .map(|(key, tag)| (key, cell.commanded(tag))),
        );
        let running: Vec<u64> = (1..=cell.nodes).filter(|id| *id != leader).collect();
        cell.start_client(&running);
        // Each key's current version, read by the client, by the follower that put it, and by the
        // node that leads now, in-process: the version its put was answered.
        let now_leading = cell.leader();
        for (key, outcome) in answered {
            let CommandOutcome::Answered(Answer::Name(name::Outcome::Put { version })) = outcome
            else {
                panic!("the put of {key} answered {outcome:?}");
            };
            let current = cell.current(&key).expect("the key has a version");
            assert_eq!(mantle_meta::key::version_id(current), version, "{key}");
            for reader in [follower, now_leading] {
                let query = Query::Current {
                    bucket: "b".into(),
                    key: key.clone(),
                };
                let tag = cell.ask_read(reader, query);
                let ReadOutcome::Read(read) = cell.read_of(tag) else {
                    panic!("node {reader}'s read of {key} was not answered");
                };
                let Read::Current(Some((order, _))) = *read else {
                    panic!("node {reader} read no version of {key}: {read:?}");
                };
                assert_eq!(order, current, "node {reader}'s read of {key}");
            }
        }
        cell.close();
    }
}

/// A node whose range lost its quorum ends a read and a command of its own, both asked with no
/// deadline, once its detectors leave its member no quorum for the range's patience (node.md
/// §5.4): each answered as past its deadline, where both waited without end. The survivor
/// followed, and named no leader that answers; or it led, and leads on with no quorum its
/// detectors trust (hyper-raft hands over rather than stepping down), committing nothing and
/// confirming no read.
fn leaderless_steps_end(engines: Engines, survivor_led: bool) {
    let mut cell = Cell::start(engines);
    cell.judging();
    let leader = cell.leader();
    let survivor = if survivor_led {
        leader
    } else {
        (1..=cell.nodes).find(|id| *id != leader).unwrap()
    };
    for id in (1..=cell.nodes).filter(|id| *id != survivor) {
        cell.stop(id);
    }
    let query = Query::Current {
        bucket: "b".into(),
        key: "k".into(),
    };
    let read = cell.ask_read(survivor, query);
    let command = cell.ask_command(survivor, put("k", "e", 1));
    let outcome = cell.read_of(read);
    assert!(
        matches!(outcome, ReadOutcome::Deadline),
        "the read came to {outcome:?}"
    );
    assert_eq!(cell.commanded(command), CommandOutcome::Deadline);
    cell.close();
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
fn a_nodes_own_commands_reach_the_leader_wherever_it_is_and_each_applies_once() {
    own_commands(Engines::Model);
}

#[test]
fn a_node_whose_range_lost_its_quorum_ends_its_steps_after_the_ranges_patience() {
    leaderless_steps_end(Engines::Model, false);
}

#[test]
fn a_leader_left_no_quorum_ends_its_own_steps_after_the_ranges_patience() {
    leaderless_steps_end(Engines::Model, true);
}

#[test]
fn a_nodes_own_commands_reach_the_leader_wherever_it_is_on_mantles_engine() {
    own_commands(Engines::Mantle);
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

/// The ends a dialer's link to a node cut off counts before the cut is healed: its connection's
/// close, then two redials unanswered, the second ending as the first did.
const ENDS_ACROSS_CUT: u64 = 3;

fn leader_cut_off(engines: Engines, nodes: u64) {
    let mut cell = Cell::start_on(engines, Network::Relayed, nodes);
    let leader = cell.leader();
    cell.write(1);
    cell.applied_everywhere(1);
    // Each pair across the cut, as the lower node dials it, and the ends its link had counted.
    let across: Vec<((u64, u64), u64)> = (1..=cell.nodes)
        .filter(|id| *id != leader)
        .map(|peer| {
            let pair = (leader.min(peer), leader.max(peer));
            (pair, cell.watch.links.get(&pair).map_or(0, |l| l.ended))
        })
        .collect();
    let others: Vec<u64> = (1..=cell.nodes).filter(|id| *id != leader).collect();
    // What the nodes the cut leaves joined have suspected of one another, all told: each node's
    // detectors judge each peer apart.
    let joined_suspicions = |w: &Watch| -> u64 {
        others
            .iter()
            .flat_map(|of| others.iter().map(move |to| (*of, *to)))
            .filter_map(|pair| w.links.get(&pair))
            .map(|link| link.suspected)
            .sum()
    };
    let suspected_at_cut = joined_suspicions(&cell.watch);
    cell.isolate(leader, true);
    // The other nodes elect one of themselves, in a later term than the one cut off leads.
    let led = cell.watch.status[&leader].term;
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
    cell.order(elected, Order::Propose(registration(2, 1, 2)));
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
    // The cut holds until every dialer across it was told its connection closed and two redials
    // ended unanswered: a dial that ends as the last one did must still be told its owner, or
    // the owner never dials again and the node cut off is never joined.
    cell.watch.until(&cell.reports, |w| {
        let told = across.iter().all(|(pair, ended)| {
            w.links
                .get(pair)
                .is_some_and(|l| l.ended >= ended + ENDS_ACROSS_CUT)
        });
        if told { Some(()) } else { None }
    });
    // Joined again, it follows the leader the others follow and applies the write as they did.
    cell.isolate(leader, false);
    assert_eq!(cell.applied_everywhere(2), applied);
    let follows = cell.leader();
    // A node cut off and joined again moves no leader its peers trust (pre-vote and check-quorum,
    // Ongaro's dissertation §9.6): the leader the others elected leads still unless their own
    // detectors, which the cut never touched, came to suspect one another, as a host that stalls
    // makes them, for only that ends its lead. It steps down for want of a quorum it trusts, a
    // hand-over is ordered only at a step-down, and an election wins only votes that no longer
    // trust it. Each node's report of a suspicion comes before its status moves, and the leader
    // the nodes agree on is read from their statuses, so the count read here holds every
    // suspicion behind a change.
    let suspected = joined_suspicions(&cell.watch) - suspected_at_cut;
    if suspected == 0 {
        assert_eq!(
            follows, elected,
            "the leader moved though no node the cut left joined suspected another"
        );
    }
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
        clock: Clock::Host(Instant::now()),
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
    let mut cell = Cell::start_spared(engines, Network::Direct, NODES, 1, None);
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
        clock: Clock::Host(start),
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
        clock: Clock::Host(start),
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

/// The volume each node of a chunk test keeps: 64 MiB of 4 MiB segments, so a chunk of up to a
/// segment's payload; 4 KiB checksum blocks; a device not measured, read one at a time at an
/// issuer of one transfer (`Reads::default`, `issuer::depth`).
fn test_volume() -> VolumeSettings {
    VolumeSettings {
        dir: PathBuf::new(),
        size: 64 << 20,
        config: VolumeConfig {
            segment_size: 4 << 20,
            checksum_shift: 12,
            max_fragments: 4096,
            compact: true,
            scrub_period: None,
            limits: VolumeLimits {
                batch_requests: 64,
                batch_bytes: 4 << 20,
                fragments_per_chunk: 64,
            },
            prewrite: false,
            reads: Reads::default(),
        },
        depth: 1,
    }
}

/// Chunk `n`'s key.
fn chunk_key(n: u64) -> ChunkKey {
    ChunkKey {
        block: u128::from(n).wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835),
        epoch: 1,
        index: u16::try_from(n % 9).unwrap(),
    }
}

/// Deterministic bytes, different for each seed.
fn chunk_data(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_le_bytes()[0]
        })
        .collect()
}

#[test]
fn chunks_put_on_each_nodes_volume_read_back_whole_and_in_part_from_any_node() {
    let mut cell = Cell::start_stored(Engines::Model, test_volume());
    let volumes: Vec<u128> = (1..=NODES).map(|id| cell.volume_of(id)).collect();
    // Nodes 1 and 2 ask every node's volume.
    for from in [1, 2] {
        for to in (1..=NODES).filter(|to| *to != from) {
            cell.connected(from, to);
        }
    }
    // Node 1 puts chunks on every node's volume: its own in-process, its peers' over the transfer
    // class. The sizes cross the checksum block and a large one spans many.
    let mut kept = Vec::new();
    let mut n = 0u64;
    for at in 1..=NODES {
        let volume = volumes[usize::try_from(at - 1).unwrap()];
        for size in [1usize, 4096, 4097, 65_537, 1 << 20] {
            n += 1;
            let (key, bytes) = (chunk_key(n), chunk_data(n, size));
            assert_eq!(
                cell.put_chunk(1, at, volume, key, &bytes),
                ChunkOutcome::Stored
            );
            kept.push((at, volume, key, bytes));
        }
    }
    // Each reads back whole and in part, from node 1 and node 2: their own volume in-process, a
    // peer's over the transfer class, every byte verified by the volume that holds it.
    for (at, volume, key, bytes) in &kept {
        let len = u64::try_from(bytes.len()).unwrap();
        for from in [1, 2] {
            assert_eq!(
                cell.read_back(from, *at, *volume, *key, 0, len),
                ChunkOutcome::Read(bytes.clone())
            );
            if len > 2 {
                let (offset, part) = (len / 3, len / 3);
                let range =
                    usize::try_from(offset).unwrap()..usize::try_from(offset + part).unwrap();
                assert_eq!(
                    cell.read_back(from, *at, *volume, *key, offset, part),
                    ChunkOutcome::Read(bytes[range].to_vec())
                );
            }
        }
    }
    cell.close();
}

#[test]
fn a_chunk_refused_is_answered_why_by_the_node_that_refused_it() {
    // Node 2 is told a chunk bound below what its volume holds, past this test's chunks: a chunk
    // past it its volume would store, so only the bound refuses it.
    let bound = 16_384u64;
    let mut cell = Cell::start_bounded(
        Engines::Model,
        Network::Direct,
        NODES,
        0,
        Some(test_volume()),
        BTreeMap::from([(2, bound)]),
    );
    let (own, peer) = (cell.volume_of(1), cell.volume_of(2));
    cell.connected(1, 2);
    let bytes = chunk_data(7, 10_000);
    let crc = mantle_crc::crc32c(&bytes);
    for (at, volume) in [(1, own), (2, peer)] {
        // Changed on the way: the volume checks the CRC-32C its sender computed.
        let tag = cell.ask_chunk(
            1,
            Chunked::Put {
                at,
                volume,
                key: chunk_key(at),
                bytes: bytes.clone(),
                crc32c: crc ^ 1,
            },
        );
        assert_eq!(
            cell.chunk(tag),
            ChunkOutcome::Refused(ChunkStatus::Checksum)
        );
        // A volume the node does not hold.
        assert_eq!(
            cell.put_chunk(1, at, volume ^ 1, chunk_key(at), &bytes),
            ChunkOutcome::Refused(ChunkStatus::NoVolume)
        );
        // A chunk the volume does not hold, and bytes past one's end.
        assert_eq!(
            cell.read_back(1, at, volume, chunk_key(at), 0, 1),
            ChunkOutcome::Refused(ChunkStatus::NotFound)
        );
        assert_eq!(
            cell.put_chunk(1, at, volume, chunk_key(at), &bytes),
            ChunkOutcome::Stored
        );
        assert_eq!(
            cell.read_back(1, at, volume, chunk_key(at), 9_999, 2),
            ChunkOutcome::Refused(ChunkStatus::Range)
        );
        // The same bytes again are answered as stored; other bytes under the key never replace
        // what a write stored (node.md §5.2).
        assert_eq!(
            cell.put_chunk(1, at, volume, chunk_key(at), &bytes),
            ChunkOutcome::Stored
        );
        assert_eq!(
            cell.put_chunk(1, at, volume, chunk_key(at), &chunk_data(8, 10_000)),
            ChunkOutcome::Refused(ChunkStatus::Exists)
        );
        assert_eq!(
            cell.read_back(1, at, volume, chunk_key(at), 0, 10_000),
            ChunkOutcome::Read(bytes.clone())
        );
    }
    // More bytes than the transfer class carries in a reply.
    assert_eq!(
        cell.read_back(1, 2, peer, chunk_key(2), 0, CHUNK_BYTES + 1),
        ChunkOutcome::Refused(ChunkStatus::TooLarge)
    );
    // A chunk past the largest a node's cell writes: refused by its sender before it goes, and
    // by a peer whatever its sender's bound, before any room or buffer is taken for its body.
    let past = chunk_data(9, usize::try_from(bound).unwrap() + 1);
    assert_eq!(
        cell.put_chunk(2, 1, own, chunk_key(3), &past),
        ChunkOutcome::Refused(ChunkStatus::TooLarge)
    );
    assert_eq!(
        cell.put_chunk(1, 2, peer, chunk_key(3), &past),
        ChunkOutcome::Refused(ChunkStatus::TooLarge)
    );
    cell.close();
}

#[test]
fn chunks_a_peer_stored_read_back_after_it_stops_and_starts_again() {
    let mut cell = Cell::start_stored(Engines::Model, test_volume());
    let volume = cell.volume_of(2);
    cell.connected(1, 2);
    let chunks: Vec<(ChunkKey, Vec<u8>)> = (1..=6u64)
        .map(|n| {
            (
                chunk_key(n),
                chunk_data(n, 30_000 * usize::try_from(n).unwrap()),
            )
        })
        .collect();
    for (key, bytes) in &chunks {
        assert_eq!(
            cell.put_chunk(1, 2, volume, *key, bytes),
            ChunkOutcome::Stored
        );
    }
    cell.stop(2);
    // Its volume is closed: what node 1 asks of it is lost, not answered.
    assert!(matches!(
        cell.get_chunk(1, 2, volume, chunks[0].0, 0, 1),
        ChunkOutcome::Lost(_)
    ));
    let dials = cell.watch.links.get(&(1, 2)).map_or(0, |link| link.dials);
    cell.restart(2);
    assert_eq!(cell.volume_of(2), volume, "the same volume, recovered");
    cell.connected_anew(1, 2, dials);
    for (key, bytes) in &chunks {
        let len = u64::try_from(bytes.len()).unwrap();
        assert_eq!(
            cell.read_back(1, 2, volume, *key, 0, len),
            ChunkOutcome::Read(bytes.clone())
        );
    }
    cell.close();
}
