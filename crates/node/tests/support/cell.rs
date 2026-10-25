//! The simulated cell the node's simulation tests run (docs/design/node.md §9.1): three nodes or
//! five and their clients, each on a shard of its own of one hyper-rt simulation runtime, which
//! runs every shard on one thread on a virtual clock. The nodes' QUIC endpoints, their sealed
//! datagram planes and the clients' endpoints are sockets on the runtime's fabric; each node's log
//! is hyper-log on hyper-block's simulated device, every write's answer taken as it is asked (as
//! the range simulation's members are, `crates/range/tests/sim.rs`), and its range's rows are in
//! the model engine. The nodes are the production owner (`Node`), elected by suspicion over their
//! liveness stream, replicating on QUIC lanes with consensus control on the plane; the clients are
//! the production client, redirected and retrying as they are. A run is its seed: every key,
//! certificate serial and endpoint's draws derive from it.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::mpsc;
use std::task::Waker;
use std::time::Duration;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Crash, Fault as DeviceFault, SimFile};
use hyper_log::{Class as LogClass, Config as LogConfig, Log, LogError, LogStats, Update, Waits};
use hyper_rt::combine::{Either, race2};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{RuntimeConfig, interests_for};
use hyper_rt::sim::{PPM, SimLoss, SimPath, SimRuntime};
use hyper_rt::sync::{ChannelReceiver, Sender, channel};
use hyper_sim::rng::{Seeded, stream_seed};
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model};
use mantle_meta::name;
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Entry};
use mantle_node::classes::Role;
use mantle_node::client::{Asks, Client, ClientSettings};
use mantle_node::directory::{Peer, node_name};
use mantle_node::node::{
    Hosted, Node, NodeLog, NodeSettings, Status, Wall, Written, message_bounds,
};
use mantle_node::storage::Storage;
use mantle_range::{ConfState, Range, ReplicaError, Settings};

use crate::history::{Clock, Part, Plan, Steer, Told, open_bucket, work};
use crate::store::Synchronous;

/// The range every node hosts: the cell's first Name range.
pub const GROUP: u128 = 0x0072_616e_6765;
/// The largest chunk the cell's gateways write: `mantle_gateway::layout::CHUNK`. Every node and
/// client of the process states the same bounds.
const CHUNK_BYTES: u64 = 8 << 20;
/// The nodes of a cell on an odd seed: three, and on an even one five, the cells node.md's
/// Phase C asks histories of.
pub const NODES: u64 = 3;
const MOST_NODES: u64 = 5;
/// The first client's id, past every node's of the largest cell; the clients' ids follow it.
pub const CLIENT: u64 = MOST_NODES + 1;
/// The clients, each its own identity and session: one for each node of a cell of three.
pub const CLIENTS: u64 = NODES;
/// The authority's name.
const AUTHORITY: &str = "mantle simulated cell";
/// RFC 5280 §4.1.2.5: the notAfter of a certificate with no well-defined expiration,
/// 9999-12-31T23:59:59Z, in seconds since 1970.
const NO_EXPIRY: u64 = 253_402_300_799;

/// The range's settings and rules, as the cluster test runs them (`tests/cluster.rs`).
pub const SETTINGS: Settings = Settings {
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
    // The model engine holds a row of any length.
    max_key_bytes: usize::MAX,
    max_value_bytes: usize::MAX,
};

/// The datagrams lost while a run's faults last, when its draw loses any: 2% of each, independently,
/// the range simulation's rate (`crates/range/tests/sim.rs`, a message dropped one time in fifty while
/// its faults run).
const LOSS_PPM: u32 = PPM / 50;

/// The named profiles of focal's path model (focal 27 §3.1 P7), as hyper-sim names them
/// (`hyper_sim::net::Path::LAN`, `REGIONAL`, `GEOGRAPHIC`): one switch apart, 0.2 ms ± 0.1 ms one
/// way; one continent, 80 ms ± 20 ms; across the planet on a poor route, 500 ms ± 100 ms.
pub const PROFILES: [(&str, u64, u64); 3] = [
    ("lan", 200_000, 100_000),
    ("regional", 80_000_000, 20_000_000),
    ("geographic", 500_000_000, 100_000_000),
];

/// The range of a cell of `nodes`, each a founding voter, under `settings`.
fn range(nodes: u64, settings: Settings) -> Range {
    Range {
        layer: Layer::Name,
        rules: RULES,
        boot: ConfState {
            voters: (1..=nodes).collect(),
            ..ConfState::default()
        },
        settings,
    }
}

/// The engine of a cell's first Name range, before any entry.
fn first_range(mut engine: Model) -> Model {
    engine.install(0, name::first(1).unwrap()).unwrap();
    engine.persist().unwrap();
    engine
}

/// The node's log, as the range simulation's members keep theirs: each member's submissions one at
/// a time, so the writer never waits for more (`Waits::Never`); two groups, the range's and the
/// liveness stream's.
pub fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 16 * 4096,
        max_segments: 64,
        max_groups: 2,
        group_entries: 1 << 10,
        // The least that holds a ready of the range's settings, so writes pass it.
        group_bytes: mantle_range::ready_held(&SETTINGS).unwrap().1,
        group_cache: 1 << 12,
        queue_submissions: 16,
        waits: Waits::Never,
    }
}

/// The log's id: the node's.
fn log_id(node: u64) -> u128 {
    u128::from(node)
}

/// A node's log in the simulation: hyper-log on its simulated device, every write's answer taken as
/// it is asked (a liveness write's once the clock has passed it), so a run never hangs on when the
/// log's threads answered and a seed runs the same every time; and the faults the test arms on the device, each armed at the log's next call,
/// between its own writes (hyper-log's `Log::with_file`), at the same point in every run.
struct SimLog {
    log: Log<SimFile>,
    faults: mpsc::Receiver<DeviceFault>,
}

impl SimLog {
    /// Arms on the device what the test asked.
    fn arm(&self) {
        while let Ok(fault) = self.faults.try_recv() {
            self.log
                .with_file(move |file| file.inject(fault))
                .unwrap()
                .unwrap();
        }
    }
}

/// A liveness write, made durable as it was given, and its answer taken once the shard's clock has
/// passed that moment, as a device's completion comes after its submission. Taken at the very
/// instant it was asked, a turn took it with no time passed, and the stream, which asks its next
/// proof as a heartbeat it sends needs one, asked again at that same instant, again and again.
struct Answered {
    answer: Cell<Option<Result<(), LogError>>>,
    given_ns: u64,
}

impl Written for Answered {
    fn poll(&self) -> Option<Result<(), LogError>> {
        if hyper_rt::futures::now_ns() > self.given_ns {
            self.answer.take()
        } else {
            None
        }
    }
}

impl NodeLog for SimLog {
    type Store = Synchronous<SimFile>;
    type Written = Answered;

    fn claim(&self, group: u128, range: &Range) -> Result<Synchronous<SimFile>, ReplicaError> {
        self.arm();
        Ok(Synchronous::new(
            mantle_range::claim(&self.log, group, range)?,
            None,
        ))
    }

    /// Waited for here, under a waker no task holds: the node takes the answer at its next poll,
    /// and no thread of the log's wakes a simulated shard.
    fn write_liveness(&self, group: u128, _waker: Waker) -> Result<Answered, LogError> {
        self.arm();
        let pending = self.log.submit_waking(
            group,
            LogClass::Latency,
            Update::default(),
            Waker::noop().clone(),
        )?;
        Ok(Answered {
            answer: Cell::new(Some(pending.wait())),
            given_ns: hyper_rt::futures::now_ns(),
        })
    }

    fn stats(&self, into: Option<Box<LogStats>>) -> Result<Box<LogStats>, LogError> {
        self.arm();
        self.log.stats(into)
    }
}

/// The cell's authority, each node's certificate and key, and each client's.
struct Pki {
    authority: Vec<u8>,
    nodes: BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
    clients: BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
}

impl Pki {
    /// The cell's certificates in run `seed`: every key and serial derived from it, each valid from
    /// the epoch with no expiry, so a run's handshakes carry the same bytes, which compress to the
    /// same sizes (RFC 8879), and a seed replays.
    fn new(nodes: u64, seed: u64) -> Self {
        let validity = Validity {
            not_before: 0,
            not_after: NO_EXPIRY,
        };
        // The authority's: id 0, no endpoint's.
        let key = KeyPair::from_seed(&derived(seed, 0, Derived::Key)).unwrap();
        let serial =
            |id| Serial::from_octets(derived(seed, id, Derived::Serial)[..16].try_into().unwrap());
        let authority = mantle_cert::authority(&key, AUTHORITY, validity, serial(0)).unwrap();
        let issue = |id: u64, name: &str| {
            let own = KeyPair::from_seed(&derived(seed, id, Derived::Key)).unwrap();
            let certificate = mantle_cert::node(
                &key,
                AUTHORITY,
                own.public_key(),
                name,
                (validity, serial(id)),
            )
            .unwrap();
            (certificate, own.pkcs8().to_vec())
        };
        Self {
            authority,
            nodes: (1..=nodes)
                .map(|id| (id, issue(id, &node_name(id))))
                .collect(),
            clients: (CLIENT..CLIENT + CLIENTS)
                .map(|id| (id, issue(id, &format!("client-{id}"))))
                .collect(),
        }
    }

    fn credentials(&self, (certificate, key): &(Vec<u8>, Vec<u8>)) -> Credentials {
        Credentials {
            chain: vec![CertificateDer::from(certificate.clone())],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.clone())),
            roots: vec![CertificateDer::from(self.authority.clone())],
        }
    }

    fn node_peers(&self, ids: impl Iterator<Item = u64>) -> Vec<Peer> {
        ids.map(|id| Peer {
            id,
            role: Role::Node,
            certificate: self.nodes[&id].0.clone(),
        })
        .collect()
    }

    /// Node `id`'s peers: the other nodes and every client.
    fn peers(&self, id: u64) -> Vec<Peer> {
        let mut peers = self.node_peers(self.nodes.keys().copied().filter(|p| *p != id));
        peers.extend(self.clients.iter().map(|(client, (certificate, _))| Peer {
            id: *client,
            role: Role::Client,
            certificate: certificate.clone(),
        }));
        peers
    }

    fn client_settings(&self, id: u64, seed: u64, settings: Settings) -> ClientSettings {
        let nodes = self.node_peers(self.nodes.keys().copied());
        let count = u64::try_from(nodes.len()).unwrap();
        ClientSettings {
            id,
            bind: "127.0.0.1:0".parse().unwrap(),
            credentials: self.credentials(&self.clients[&id]),
            transport_bytes: u64::try_from(nodes.len() * 3 * hyper_transport::RECEIVE_CHUNK)
                .unwrap(),
            nodes,
            role: Role::Client,
            messages: message_bounds(
                &[Hosted {
                    group: GROUP,
                    range: range(count, settings),
                    engine: (),
                }],
                CHUNK_BYTES,
            )
            .unwrap(),
            seed: Some(derived(seed, id, Derived::Endpoint)),
        }
    }
}

/// What a run's seed gives an endpoint: its key, its certificate's serial, its QUIC endpoint's
/// draws (`Endpoint::new_seeded`). Each its own, as a host's are.
#[derive(Clone, Copy)]
enum Derived {
    Key = 1,
    Serial = 2,
    Endpoint = 3,
}

/// The 32 octets run `seed` gives endpoint `id` for `what`: four draws of its stream (hyper-sim's
/// named streams, `docs/sim.md` §3.1).
fn derived(seed: u64, id: u64, what: Derived) -> [u8; 32] {
    let mut stream = Seeded::new(stream_seed(seed, "derived", &[id, what as u64]));
    let mut bytes = [0u8; 32];
    for word in bytes.as_chunks_mut::<8>().0 {
        *word = stream.next_u64().to_le_bytes();
    }
    bytes
}

/// What the test tells a node's task.
enum Order {
    /// Where a peer listens: QUIC's address and the plane's.
    Learn(u64, SocketAddr, SocketAddr),
    /// Lose power: give back the log and the engines as they stand.
    Crash,
}

/// What a task tells the test.
enum Report {
    /// Where a node listens, once open.
    Bound(u64, SocketAddr, SocketAddr),
    Status(u64, Status),
    /// A node's power cut: its log and its range's engine, as they stood.
    Crashed(u64, SimLog, Vec<(u128, Model)>),
    /// A node stopped on an error, as its process would, giving back what it held.
    Stopped(u64, String, SimLog, Vec<(u128, Model)>),
    /// A node's task, or a client's, ended on an error before it held anything.
    Failed(u64, String),
    /// The gate's opening: its answers, or why there were none.
    Opened(Result<Vec<Answer>, String>),
    /// A test client's answer, or why there was none, and what its asks came to so far.
    Asked(Result<Vec<Answer>, String>, Asks),
    Told(Told),
    /// A workload client's part of the history, once it ended.
    Part(u64, Part),
}

/// A node's task: opens the node on its log, reports where it listens, then turns it, each turn
/// raced against the test's next order, reporting its status as it changes.
async fn run_node(
    settings: NodeSettings<Model>,
    log: SimLog,
    mut orders: ChannelReceiver<Order>,
    reports: mpsc::Sender<Report>,
) {
    let id = settings.id;
    let opened = Node::open_on(settings, log).await;
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
    loop {
        let ended = {
            let turn = pin!(node.turn());
            let order = pin!(orders.recv());
            race2(turn, order).await
        };
        match ended {
            Either::First(Ok(())) => {}
            Either::First(Err(error)) => {
                let (log, engines) = node.close();
                let _ = reports.send(Report::Stopped(id, error.to_string(), log, engines));
                return;
            }
            Either::Second(Ok(Order::Learn(peer, quic, plane))) => {
                if let Err(error) = node.learn(peer, quic, plane) {
                    let _ = reports.send(Report::Failed(id, error.to_string()));
                    return;
                }
            }
            Either::Second(Ok(Order::Crash)) => {
                let (log, engines) = node.close();
                let _ = reports.send(Report::Crashed(id, log, engines));
                return;
            }
            // The test is done.
            Either::Second(Err(_)) => return,
        }
        let status = node.status(GROUP);
        if status != told
            && let Some(now) = status
        {
            if reports.send(Report::Status(id, now)).is_err() {
                return;
            }
            told = status;
        }
    }
}

/// A node of the cell as the test keeps it.
pub struct Member {
    shard: hyper_rt::shard::ShardId,
    dir: tempfile::TempDir,
    orders: Option<Sender<Order>>,
    bound: Option<(SocketAddr, SocketAddr)>,
    /// While it is down: its device and its engines, as its power cut left them.
    down: Option<(SimFile, Vec<(u128, Model)>)>,
    pub status: Option<Status>,
    /// The device faults the test arms, while it runs.
    faults: Option<mpsc::Sender<DeviceFault>>,
    /// A fault armed on its device: it may stop on it.
    armed: bool,
}

/// The cell, its runtime and what the test has heard.
pub struct Sim {
    rt: SimRuntime,
    seed: u64,
    pub nodes: u64,
    pki: Pki,
    pub members: BTreeMap<u64, Member>,
    /// The clients' shards, by id.
    client_shards: BTreeMap<u64, hyper_rt::shard::ShardId>,
    reports: mpsc::Receiver<Report>,
    tell: mpsc::Sender<Report>,
    /// The range's settings, which every node and client of the cell runs, and its nodes' logs'.
    settings: Settings,
    log: LogConfig,
    /// The profile's path, and its one-way delay: the finest step anything between nodes moves in.
    path: SimPath,
    step_ns: u64,
    /// The longest a datagram may take on any of the cell's paths now: a round trip of it is the
    /// least the cell may go without moving.
    longest_ns: u64,
    progress: BTreeMap<u64, u64>,
    patience: BTreeMap<u64, Duration>,
    pub parts: BTreeMap<u64, Part>,
    opened: Option<Result<Vec<Answer>, String>>,
    pub asked: Option<(Result<Vec<Answer>, String>, Asks)>,
    learns: Vec<mpsc::Sender<(u64, SocketAddr)>>,
    /// When something last moved, on the virtual clock.
    moved_at: u64,
    /// The node starts so far, each drawing its endpoint's seed afresh.
    starts: u64,
}

impl Sim {
    pub fn new(seed: u64) -> Self {
        Self::with(seed, SETTINGS, log_config())
    }

    /// The cell of `seed`, its range under `settings` and its nodes' logs under `log`.
    pub fn with(seed: u64, settings: Settings, log: LogConfig) -> Self {
        let nodes = if seed % 2 == 1 { NODES } else { MOST_NODES };
        let shards = usize::try_from(nodes + CLIENTS + 1).unwrap();
        let mut rt = SimRuntime::new(
            &RuntimeConfig {
                shards: u16::try_from(shards).unwrap(),
                tasks_per_shard: 1,
                timers_per_shard: 2,
                interests_per_shard: interests_for(1).max(2 * 2 * shards),
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
            },
            seed,
        )
        .unwrap();
        let (name, one_way, jitter) = PROFILES[usize::try_from(seed / 2).unwrap() % PROFILES.len()];
        let path = SimPath::in_order(one_way, jitter);
        rt.set_path(path);
        eprintln!("seed {seed}: {nodes} nodes, {name} paths");
        let ids = rt.shard_ids();
        let pki = Pki::new(nodes, seed);
        let (tell, reports) = mpsc::channel();
        let mut members = BTreeMap::new();
        for id in 1..=nodes {
            members.insert(
                id,
                Member {
                    shard: ids[usize::try_from(id - 1).unwrap()],
                    dir: tempfile::tempdir().unwrap(),
                    orders: None,
                    bound: None,
                    down: None,
                    status: None,
                    faults: None,
                    armed: false,
                },
            );
        }
        let client_shards = (CLIENT..CLIENT + CLIENTS + 1)
            .zip(ids[usize::try_from(nodes).unwrap()..].iter().copied())
            .collect();
        let mut cell = Self {
            rt,
            seed,
            nodes,
            pki,
            members,
            client_shards,
            reports,
            tell,
            settings,
            log,
            path,
            step_ns: one_way,
            longest_ns: one_way + jitter,
            progress: BTreeMap::new(),
            patience: BTreeMap::new(),
            parts: BTreeMap::new(),
            opened: None,
            asked: None,
            learns: Vec::new(),
            moved_at: 0,
            starts: 0,
        };
        for id in 1..=nodes {
            let file = SimFile::new(
                Alignment::new(4096).unwrap(),
                Alignment::new(512).unwrap(),
                seed ^ (id << 32),
            )
            .unwrap();
            let log = Log::create(file, cell.log, log_id(id)).unwrap();
            cell.spawn(id, log, vec![(GROUP, first_range(Model::default()))]);
        }
        cell.until("every node listens", |c| {
            if c.members.values().all(|m| m.bound.is_some()) {
                Some(())
            } else {
                None
            }
        });
        cell.introduce();
        cell
    }

    /// Starts node `id`'s task on its shard, on `log` with `engines`.
    fn spawn(&mut self, id: u64, log: Log<SimFile>, engines: Vec<(u128, Model)>) {
        let (orders, taken) = channel::<Order>(usize::try_from(self.nodes).unwrap() + 2).unwrap();
        let (faults, armed) = mpsc::channel();
        let log = SimLog { log, faults: armed };
        let member = self.members.get_mut(&id).unwrap();
        member.orders = Some(orders);
        member.faults = Some(faults);
        member.armed = false;
        member.bound = None;
        let addresses = Vec::new();
        let settings = NodeSettings {
            id,
            dir: member.dir.path().to_path_buf(),
            quic: "127.0.0.1:0".parse().unwrap(),
            plane: "127.0.0.1:0".parse().unwrap(),
            credentials: self.pki.credentials(&self.pki.nodes[&id]),
            peers: self.pki.peers(id),
            addresses,
            ranges: engines
                .into_iter()
                .map(|(group, engine)| Hosted {
                    group,
                    range: range(self.nodes, self.settings),
                    engine,
                })
                .collect(),
            log: self.log,
            transport_bytes: u64::try_from(
                self.pki.peers(id).len() * 4 * hyper_transport::RECEIVE_CHUNK,
            )
            .unwrap(),
            // A node started again draws afresh, as a process started again would.
            seed: Some(derived(
                self.seed ^ (self.starts << 32),
                id,
                Derived::Endpoint,
            )),
            // The simulated cell's nodes keep ranges only, and ask nothing of them themselves.
            storage: Storage::none(),
            chunk_bytes: CHUNK_BYTES,
            chunk_asks: 0,
            command_asks: 0,
            read_asks: 0,
            // The run's clock from the Unix epoch: only differences of the times the ranges stamp
            // matter to the commands the simulation runs, and a seed replays them.
            wall: Wall::From(0),
            gateway: None,
            // The simulated cell keeps no history (its nodes enlist nothing): every start's
            // detectors begin from what they watch.
            history: mantle_meta::record::History::default(),
        };
        self.starts += 1;
        let shard = member.shard;
        let tell = self.tell.clone();
        self.rt
            .spawn_on(shard, run_node(settings, log, taken, tell))
            .unwrap();
    }

    /// Tells each running node where every other listens, and every client where each node does.
    fn introduce(&mut self) {
        let bound: Vec<(u64, SocketAddr, SocketAddr)> = self
            .members
            .iter()
            .filter_map(|(id, m)| m.bound.map(|(q, p)| (*id, q, p)))
            .collect();
        for (id, member) in &self.members {
            let Some(orders) = &member.orders else {
                continue;
            };
            for (peer, quic, plane) in bound.iter().filter(|(p, ..)| p != id) {
                orders
                    .try_send(Order::Learn(*peer, *quic, *plane))
                    .map_err(|_| "a node's orders are full")
                    .unwrap();
            }
        }
        for learn in &self.learns {
            for (id, quic, _) in &bound {
                let _ = learn.send((*id, *quic));
            }
        }
    }

    /// The ports each node's sockets have now.
    fn ports(&self, id: u64) -> Vec<u16> {
        self.members[&id]
            .bound
            .map(|(q, p)| vec![q.port(), p.port()])
            .unwrap_or_default()
    }

    /// Cuts node `id` off from every other node, both ways, or joins it to them again.
    pub fn isolate(&mut self, id: u64, cut: bool) {
        let path = if cut {
            self.path.with_loss(SimLoss::random(PPM))
        } else {
            self.path
        };
        let own = self.ports(id);
        for peer in (1..=self.nodes).filter(|p| *p != id) {
            for theirs in self.ports(peer) {
                for ours in &own {
                    self.rt.set_pair_path(*ours, theirs, path);
                    self.rt.set_pair_path(theirs, *ours, path);
                }
            }
        }
    }

    /// Loses every datagram the other nodes' planes send node `id`'s, or none: their liveness
    /// streams and the consensus control their planes carry reach it no more, while its own reach
    /// them and every QUIC path stays as it was.
    pub fn deafen_plane(&mut self, id: u64, deaf: bool) {
        let path = if deaf {
            self.path.with_loss(SimLoss::random(PPM))
        } else {
            self.path
        };
        let Some(&own) = self.ports(id).get(1) else {
            return;
        };
        for peer in (1..=self.nodes).filter(|p| *p != id) {
            if let Some(&theirs) = self.ports(peer).get(1) {
                self.rt.set_pair_path(theirs, own, path);
            }
        }
    }

    /// Cuts node `id`'s power: its task gives back its log and engines, its device keeps what it
    /// made durable as the seed says, its engine what it persisted.
    pub fn crash(&mut self, id: u64) {
        let orders = self.members.get_mut(&id).unwrap().orders.take().unwrap();
        orders
            .try_send(Order::Crash)
            .map_err(|_| "a node's orders are full")
            .unwrap();
        drop(orders);
        let (file, mut engines) = self.until("the node's power is cut", |c| {
            c.members.get_mut(&id).unwrap().down.take()
        });
        for (_, engine) in &mut engines {
            engine.crash();
        }
        file.crash(Crash::Random).unwrap();
        let member = self.members.get_mut(&id).unwrap();
        member.bound = None;
        member.status = None;
        member.down = Some((file, engines));
    }

    /// Arms `fault` on node `id`'s device, and runs until the node stops on it, as its process
    /// would (a range it hosts fenced by its log's failed write, node.md §6.2).
    pub fn fail_device(&mut self, id: u64, fault: DeviceFault) {
        let member = self.members.get_mut(&id).unwrap();
        member.armed = true;
        member.faults.as_ref().unwrap().send(fault).unwrap();
        self.until("the node stops on its device's failure", |c| {
            c.members[&id].down.as_ref().map(|_| ())
        });
    }

    /// Loses `LOSS_PPM` of every datagram, or none.
    pub fn lossy(&mut self, lossy: bool) {
        let path = if lossy {
            self.path.with_loss(SimLoss::random(LOSS_PPM))
        } else {
            self.path
        };
        self.rt.set_path(path);
    }

    /// Sets every path to or from a client to `path` from now on; the nodes' paths among
    /// themselves stay the profile's.
    pub fn slow_clients(&mut self, path: SimPath) {
        for id in 1..=self.nodes {
            for peer in (1..=self.nodes).filter(|p| *p != id) {
                for ours in self.ports(id) {
                    for theirs in self.ports(peer) {
                        self.rt.set_pair_path(ours, theirs, self.path);
                    }
                }
            }
        }
        self.rt.set_path(path);
        self.longest_ns = self.longest_ns.max(path.one_way_ns() + path.jitter_ns());
    }

    /// Starts node `id` again on what its device and engines kept.
    pub fn restart(&mut self, id: u64) {
        let (file, engines) = self.members.get_mut(&id).unwrap().down.take().unwrap();
        file.clear_faults().unwrap();
        let (log, _) = Log::open(file, self.log, log_id(id)).unwrap();
        self.spawn(id, log, engines);
        self.until("the node listens again", |c| {
            c.members[&id].bound.map(|_| ())
        });
        self.introduce();
    }

    /// Takes what the tasks reported.
    fn take(&mut self) {
        while let Ok(report) = self.reports.try_recv() {
            // What moves the cell: everything but a status whose only change is a count of
            // heartbeats, which a cell that has stopped still takes (as `tests/cluster.rs` judges).
            let moved = match &report {
                Report::Status(id, now) => self.members[id].status.is_none_or(|was| {
                    (was.term, was.leader, was.commit, was.applied, was.measuring)
                        != (now.term, now.leader, now.commit, now.applied, now.measuring)
                }),
                _ => true,
            };
            if moved {
                self.moved_at = self.rt.now_ns();
            }
            match report {
                Report::Bound(id, quic, plane) => {
                    self.members.get_mut(&id).unwrap().bound = Some((quic, plane));
                }
                Report::Status(id, status) => {
                    self.members.get_mut(&id).unwrap().status = Some(status);
                }
                Report::Crashed(id, log, engines) => {
                    let file = log.log.close().unwrap();
                    self.members.get_mut(&id).unwrap().down = Some((file, engines));
                }
                // A node stops on its device's failure only when the test armed one there.
                Report::Stopped(id, why, log, engines) => {
                    let member = self.members.get_mut(&id).unwrap();
                    assert!(member.armed, "seed {}: {id} stopped: {why}", self.seed);
                    member.orders = None;
                    member.faults = None;
                    member.bound = None;
                    member.status = None;
                    member.down = Some((log.log.close().unwrap(), engines));
                }
                Report::Failed(id, why) => panic!("seed {}: {id} failed: {why}", self.seed),
                Report::Opened(answers) => self.opened = Some(answers),
                Report::Asked(answered, asks) => self.asked = Some((answered, asks)),
                Report::Told(Told::Progress(id, done, patience)) => {
                    self.progress.insert(id, done);
                    if let Some(patience) = patience {
                        self.patience.insert(id, patience);
                    }
                }
                Report::Told(Told::Failed(id, why)) => {
                    panic!("seed {}: client {id} failed: {why}", self.seed)
                }
                Report::Part(id, part) => {
                    self.parts.insert(id, part);
                }
            }
        }
    }

    /// The quiet period: the longest any node states it may go without moving, or a link to a
    /// peer it dials may should that peer fall silent, or any client gives an exchange; never
    /// less than a round trip of the longest path, nor than one step.
    fn quiet(&self) -> u64 {
        self.members
            .values()
            .filter_map(|m| m.status.as_ref())
            .flat_map(|s| [s.patience, s.reconnect, s.measuring_for])
            .flatten()
            .chain(self.patience.values().copied())
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .fold(
                self.step_ns.max(1).max(self.longest_ns.saturating_mul(2)),
                u64::max,
            )
    }

    /// Runs the cell a step at a time until `done` holds; fails once a quiet period passes with
    /// nothing reported.
    pub fn until<T>(&mut self, what: &str, mut done: impl FnMut(&mut Self) -> Option<T>) -> T {
        self.moved_at = self.rt.now_ns();
        loop {
            self.take();
            if let Some(found) = done(self) {
                return found;
            }
            let now = self.rt.now_ns();
            if now.saturating_sub(self.moved_at) > self.quiet() {
                panic!(
                    "seed {}: {what}: quiet for {} ns at {now} ns; statuses {:?}, progress {:?}",
                    self.seed,
                    now - self.moved_at,
                    self.members
                        .iter()
                        .map(|(id, m)| (*id, m.status))
                        .collect::<Vec<_>>(),
                    self.progress
                );
            }
            self.rt.run_until(now.saturating_add(self.step_ns.max(1)));
        }
    }

    /// The leader every running node names, in one term.
    pub fn leader(&mut self) -> u64 {
        self.until("the nodes agree on a leader", |c| {
            let named: Vec<(u64, u64)> = c
                .members
                .values()
                .filter(|m| m.orders.is_some())
                .map(|m| m.status.as_ref().map(|s| (s.term, s.leader)))
                .collect::<Option<_>>()?;
            let first = *named.first()?;
            let agreed = first.1 != 0
                && c.members.get(&first.1).is_some_and(|m| m.orders.is_some())
                && named.iter().all(|n| *n == first);
            if agreed { Some(first.1) } else { None }
        })
    }

    /// Runs until every running node judges every peer its stream keeps by a margin.
    pub fn judged(&mut self) {
        self.until("every node judges its links", |c| {
            let judged = c
                .members
                .values()
                .filter(|m| m.orders.is_some())
                .all(|m| m.status.is_some_and(|s| s.judged));
            if judged { Some(()) } else { None }
        });
    }

    /// Every node's applied index, once all are equal.
    pub fn converged(&mut self) -> u64 {
        self.until("every node applies as far as the others", |c| {
            let mut applied = c
                .members
                .values()
                .map(|m| m.status.as_ref().map(|s| s.applied));
            let first = applied.next()??;
            if first > 0 && applied.all(|a| a == Some(first)) {
                Some(first)
            } else {
                None
            }
        })
    }

    /// Opens the bucket's gate through a client of its own.
    pub fn open_bucket(&mut self) {
        let id = CLIENT + CLIENTS;
        let settings = self.pki.client_settings(CLIENT, self.seed, self.settings);
        let bound = self.bound_quic();
        let tell = self.tell.clone();
        let shard = self.client_shards[&id];
        self.rt
            .spawn_on(shard, async move {
                let mut client = match Client::open(settings).await {
                    Ok(client) => client,
                    Err(error) => {
                        let _ = tell.send(Report::Opened(Err(error.to_string())));
                        return;
                    }
                };
                for (node, quic) in bound {
                    client.learn(node, quic).unwrap();
                }
                let answered = open_bucket(&mut client, GROUP, id).await;
                let _ = tell.send(Report::Opened(answered));
            })
            .unwrap();
        let answers = self
            .until("the gate opens", |c| c.opened.take())
            .unwrap_or_else(|why| panic!("seed {}: the gate's opening: {why}", self.seed));
        assert_eq!(answers, vec![Answer::Name(name::Outcome::GateMoved)]);
    }

    /// Starts a client of the test's own, `id` on its shard, that knows the nodes in `order` and
    /// asks each command the test sends it of the range of the group sent with it, telling the
    /// test its answer and what its asks came to.
    pub fn asking(&mut self, id: u64, order: &[u64]) -> Sender<(u128, Entry)> {
        let mut settings = self.pki.client_settings(id, self.seed, self.settings);
        settings
            .nodes
            .sort_by_key(|peer| order.iter().position(|o| *o == peer.id));
        let bound = self.bound_quic();
        let (asks, mut taking) = channel::<(u128, Entry)>(1).unwrap();
        let tell = self.tell.clone();
        let shard = self.client_shards[&id];
        self.rt
            .spawn_on(shard, async move {
                let mut client = match Client::open(settings).await {
                    Ok(client) => client,
                    Err(error) => {
                        let _ = tell.send(Report::Failed(id, error.to_string()));
                        return;
                    }
                };
                for (node, quic) in bound {
                    client.learn(node, quic).unwrap();
                }
                while let Ok((group, entry)) = taking.recv().await {
                    let answered = client
                        .command(group, &entry, u64::MAX)
                        .await
                        .map_err(|e| e.to_string());
                    let _ = tell.send(Report::Asked(answered, client.asks()));
                }
            })
            .unwrap();
        asks
    }

    /// Has the client `asking` ask `entry` of `group`'s range: its answer and what its asks came to.
    pub fn ask(
        &mut self,
        asking: &Sender<(u128, Entry)>,
        group: u128,
        entry: Entry,
    ) -> (Result<Vec<Answer>, String>, Asks) {
        asking
            .try_send((group, entry))
            .map_err(|_| "the client's asks are full")
            .unwrap();
        self.until("the client's answer", |c| c.asked.take())
    }

    fn bound_quic(&self) -> Vec<(u64, SocketAddr)> {
        self.members
            .iter()
            .filter_map(|(id, m)| Some((*id, m.bound?.0)))
            .collect()
    }

    /// Starts the workload clients, each on its shard.
    pub fn start_workload(&mut self, ops: u64) -> Vec<mpsc::Sender<()>> {
        let mut stops = Vec::new();
        for id in CLIENT..CLIENT + CLIENTS {
            let (stop, stopping) = mpsc::channel();
            let (learn, learning) = mpsc::channel();
            self.learns.push(learn);
            stops.push(stop);
            let settings = self.pki.client_settings(id, self.seed, self.settings);
            let bound = self.bound_quic();
            let tell = self.tell.clone();
            let plan = Plan {
                group: GROUP,
                ops,
                phase: 0,
                clock: Clock::Shard,
            };
            let steer = Steer {
                learn: learning,
                stop: stopping,
            };
            let shard = self.client_shards[&id];
            self.rt
                .spawn_on(shard, async move {
                    let told = |told: Told| {
                        let _ = tell.send(Report::Told(told));
                    };
                    let part = work(settings, bound, (plan, steer), told).await;
                    let _ = tell.send(Report::Part(id, part));
                })
                .unwrap();
        }
        stops
    }

    /// Runs until every client did `quarters` quarters of its `ops` operations.
    pub fn done_by(&mut self, ops: u64, quarters: u64) {
        let target = ops * quarters / 4;
        self.until("the clients' operations", |c| {
            let done = c.progress.len() == usize::try_from(CLIENTS).unwrap()
                && c.progress.values().all(|d| *d >= target);
            if done { Some(()) } else { None }
        });
    }
}
