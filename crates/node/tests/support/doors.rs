//! A cell whose nodes host objects and serve both doors (docs/design/node.md §4–§5), on the cluster
//! test's real sockets and threads: each node hosts a range of every layer (§1.5) in mantle's own
//! engine, keeps a chunk volume on a real file, enlists itself and its volume in the root range as
//! a node's process does (`process::serve`), and serves the native protocol on its QUIC endpoint
//! and S3 on a listener bound on loopback (`listener::Listener`), as `mantle node` runs with
//! `root_key`, `gateway.` and `s3.` keys. Each door has its own client: mantle-client, the native
//! protocol's production client, its retries and patience its own; and the S3 listener's test
//! client (`crates/mantle/tests/support/s3.rs`), each request SigV4-signed by `mantle_s3::sigv4`
//! and asked again after `503 SlowDown` as botocore's standard mode asks, mantle-client's own rule
//! (`ATTEMPTS`, `SLOWED_BASE`, `SLOWED_MOST`). Both act for one account, so either door reads
//! what the other wrote.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::pin;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_rt::combine::{Either3, race3};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{Runtime, RuntimeConfig};
use hyper_rt::sync::{ChannelReceiver, Sender, channel};
use mantle_chunk::{Config as VolumeConfig, Limits as VolumeLimits, Reads};
use mantle_ec::durability::Rates;
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::root;
use mantle_meta::wire::{Answer, Command};
use mantle_node::ask::{CommandOutcome, Wait};
use mantle_node::derive::{Charges, charges, shard_shape};
use mantle_node::listener::{Listener, Settings as ListenerSettings};
use mantle_node::node::failures::Failures;
use mantle_node::node::refusals::Refusals;
use mantle_node::node::{GatewaySettings, Hosted, Node, NodeSettings};
use mantle_node::process::{AccessKey, Enlisting, NativeShards, RangeShards};
use mantle_node::storage::{Storage, VolumeSettings};
use mantle_protocol::object::{Headers, Op, Status};
use mantle_range::{Range, Stored};
use mantle_s3::seal::WrappingKey;
use tempfile::TempDir;

use super::s3;
use super::{
    CLIENT, CLIENTS, GROUP, NODES, Pki, STORE, TRUNK, first_range, on_first_shard, range, shard,
};

/// The groups of the cell's root, Bucket, File and Block ranges, beside the Name range's
/// ([`GROUP`]), as `crates/mantle/tests/support/cell.rs` names them.
const ROOT: u128 = 0x726f_6f74;
const BUCKETS: u128 = 0x6275_636b_6574;
const FILES: u128 = 0x6669_6c65;
const BLOCKS: u128 = 0x62_6c6f_636b;
/// A range of each layer, in the cell's order of ranges (`node::lead`'s places).
const LAYERED: [(u128, Layer); 5] = [
    (ROOT, Layer::Root),
    (BUCKETS, Layer::Bucket),
    (GROUP, Layer::Name),
    (FILES, Layer::File),
    (BLOCKS, Layer::Block),
];

/// The native clients' ids, one for each node of a cell of three, past every node's and every
/// pinned client's: each certificate the authority issued for the client role, pinned nowhere
/// (node.md §3.6), all acting for [`ACCOUNT`].
pub const OBJECT_CLIENT: u64 = CLIENT + CLIENTS;
pub const OBJECT_CLIENTS: u64 = NODES;
/// The account both doors' clients act for: twelve digits.
pub const ACCOUNT: &str = "000000000042";
/// The S3 endpoint's region and access key, AWS's own example key pair (the AWS General
/// Reference's "Understanding and getting your AWS credentials"), acting for [`ACCOUNT`].
const REGION: &str = "us-east-1";
const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

/// What each node's gateway is given, as `crates/mantle/tests/support/cell.rs`'s `GATEWAY_STATED`
/// states it for a node of a cell that hosts objects: two blocks going down or being read at once,
/// four operations at once, a block held 30 s for its file's write, S3's 10,000 buckets an owner
/// (research/05 §10.4) and 64 rows a coordinator's read; a native client admitted for each of the
/// cell's nodes ([`OBJECT_CLIENTS`]); and the
/// durability a block's scheme meets: Backblaze's 2025 drive AFR, 1.36% a drive-year, per hour;
/// repair in a day, the slow end research/15 cites ([Cha12]); S3's eleven nines a year.
const WINDOW: usize = 2;
const OBJECTS: usize = 4;
const HANDOVER_NS: u64 = 30_000_000_000;
const CHUNK_RATE: f64 = 0.000_001_552_511_415_525_114_2;
const REPAIR_RATE: f64 = 0.041_666_666_666_666_664;
const TARGET: f64 = 0.000_000_000_01;

/// The chunk asks a node that hosts objects keeps out at once, as
/// `crates/mantle/tests/support/cell.rs` states them: a put's window of blocks going down, each
/// copied to every node. Its commands and reads are the cluster test's, the same as that file's.
const OBJECT_CHUNK_ASKS: usize = 12;

/// The longest any wait of this cell goes with nothing heard: a client's whole patience for one
/// operation, as `crates/mantle/tests/support/cell.rs`'s `wait_patience` reckons it: at most
/// `ATTEMPTS` attempts, each its dial, its dial again and its exchange's stock patience, and a
/// slowed pause between them. A node's start, election and enlistment are each a step the cell
/// takes within it or never; a client's operation is answered or given up within it.
pub fn patience() -> Duration {
    let waits_per_attempt = 3;
    (mantle_protocol::bounds::STOCK_PATIENCE * waits_per_attempt + mantle_client::SLOWED_MOST)
        * mantle_client::ATTEMPTS
}

/// What a door node's task tells the test.
#[derive(Debug)]
pub enum Said {
    /// Where the node's QUIC endpoint and plane listen, once open.
    Bound(u64, SocketAddr, SocketAddr),
    /// Where its S3 listener listens, once bound.
    S3(u64, SocketAddr),
    /// The root range took its enlistment.
    Enlisted(u64),
    /// Each range's leader as the node knows it, in [`LAYERED`]'s order, and the hand-overs it
    /// began to spread the leads (`Node::lead_moves`), as either changes.
    Leads(u64, Vec<u64>, u64),
    Failed(u64, String),
}

/// What the test tells a door node's task.
enum Told {
    /// Where a peer listens.
    Learn(u64, SocketAddr, SocketAddr),
    /// Ask the enlistment held ([`Enlist::Held`]).
    Enlist,
}

/// When a node asks the root range to enlist it: as it starts, as a node's process does once it
/// listens (`process::serve`); or held until the test says, as a node whose start is slow is late,
/// so the cell's first operations come before any volume is in its table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enlist {
    AtOnce,
    Held,
}

/// An object operation through a door.
#[derive(Clone)]
pub enum ObjectOp {
    CreateBucket(String),
    Put(String, String, Vec<u8>),
    Get(String, String),
}

/// An operation as a failure names it: a put by its bytes' length, not its bytes.
impl std::fmt::Debug for ObjectOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateBucket(bucket) => write!(f, "CreateBucket {bucket}"),
            Self::Put(bucket, key, bytes) => write!(f, "Put {bucket}/{key} of {}", bytes.len()),
            Self::Get(bucket, key) => write!(f, "Get {bucket}/{key}"),
        }
    }
}

/// What a door answered an operation: its bytes where it was done (a get's object, else none),
/// else what it was answered or why it was not.
pub struct Answered {
    pub door: &'static str,
    pub op: ObjectOp,
    pub answer: Result<Vec<u8>, String>,
}

/// The cell's nodes, each on a thread of its own, and what they said.
pub struct DoorCell {
    pki: Pki,
    _dirs: Vec<TempDir>,
    orders: BTreeMap<u64, Sender<Told>>,
    said: mpsc::Receiver<Said>,
    threads: Vec<JoinHandle<()>>,
    pub quic: BTreeMap<u64, SocketAddr>,
    planes: BTreeMap<u64, SocketAddr>,
    pub s3: BTreeMap<u64, SocketAddr>,
    pub enlisted: BTreeSet<u64>,
    /// Each node's view of the ranges' leaders, and the hand-overs it began.
    pub leads: BTreeMap<u64, (Vec<u64>, u64)>,
}

impl Drop for DoorCell {
    /// The orders go, so each node's task ends at its next turn, and every thread is joined.
    fn drop(&mut self) {
        self.orders.clear();
        for thread in std::mem::take(&mut self.threads) {
            let _ = thread.join();
        }
    }
}

/// The volume of a node that hosts objects, as `crates/mantle/tests/support/cell.rs`'s
/// `OBJECT_VOLUME_STATED`: 256 MiB of 16 MiB segments, so a block's whole chunk
/// (`mantle_gateway::layout::CHUNK`, Tectonic's 8 MiB) fits a segment's payload, written in batches
/// of a chunk; a device not measured, read one at a time.
fn object_volume(dir: &Path) -> VolumeSettings {
    VolumeSettings {
        dir: dir.join("device"),
        size: 256 << 20,
        config: VolumeConfig {
            segment_size: 16 << 20,
            checksum_shift: 12,
            max_fragments: 4096,
            compact: true,
            scrub_period: None,
            limits: VolumeLimits {
                batch_requests: 64,
                batch_bytes: 8 << 20,
                fragments_per_chunk: 64,
            },
            prewrite: false,
            reads: Reads::default(),
        },
        depth: 1,
    }
}

/// Range `group`'s engine in `dir`, mantle's own, holding `layer`'s rows: a Name range founded with
/// its first rows, as a node's process founds one (`process::engine`).
fn layer_engine(dir: &Path, group: u128, layer: Layer) -> Stored<DeviceFile> {
    let file = DeviceFile::open(
        &dir.join(format!("range-{group:x}.store")),
        true,
        CachingRequest::Buffered,
        Alignment::new(STORE.page_size).unwrap(),
    )
    .unwrap();
    let mut stored = Stored::create(file, STORE, STORE.page_size, TRUNK).unwrap();
    if layer == Layer::Name {
        return first_range(stored);
    }
    stored.persist().unwrap();
    stored
}

/// How each node enlists: in the root range, its own failure domain, the table holding every node
/// of the cell and a volume of each.
fn enlisting(id: u64) -> Enlisting {
    Enlisting {
        root: ROOT,
        domain: format!("domain-{id}"),
        max_nodes: u32::try_from(NODES).unwrap(),
        max_volumes: u32::try_from(NODES).unwrap(),
    }
}

/// The gateway host's settings ([`WINDOW`] and the rest), its root key 32 bytes of the test's
/// own, each operation charged at its own size under the schemes a cell of [`NODES`] domains'
/// blocks may take (`derive::charges`), the operations' bytes four of the most one holds.
fn gateway(enlist: &Enlisting) -> GatewaySettings {
    let rates = Rates {
        chunk: CHUNK_RATE,
        domains: None,
        bursts: Vec::new(),
        repair: REPAIR_RATE,
    };
    let schemes = mantle_ec::durability::candidates();
    let most = Charges::new(WINDOW, &schemes).unwrap().most();
    GatewaySettings {
        keys: vec![(1, WrappingKey::new(&[0x5a; 32]))],
        handover_ns: HANDOVER_NS,
        window: WINDOW,
        objects: OBJECTS,
        objects_bytes: u64::try_from(OBJECTS).unwrap() * most,
        cache_bytes: 0,
        cache_floor: 0,
        charges: charges(WINDOW, Some(enlist), &rates, TARGET).unwrap(),
        clients: usize::try_from(OBJECT_CLIENTS).unwrap(),
        bucket_quota: 10_000,
        scan_rows: 64,
        rates,
        target: TARGET,
        refusals: Refusals::default(),
        failures: Failures::default(),
    }
}

/// The access key both the listener and the S3 client hold.
fn access_key() -> AccessKey {
    AccessKey {
        id: ACCESS_KEY_ID.into(),
        account: ACCOUNT.into(),
        secret: SECRET_KEY.into(),
    }
}

/// A door node's runtime: one shard, sized as a node's process sizes its first for an S3 listener
/// of [`OBJECTS`] connections and `peers` connections of the transport (`derive::shard_shape`),
/// the rest as the cluster test's shards.
fn door_runtime(peers: usize) -> Runtime {
    let connections = u64::try_from(OBJECTS).unwrap();
    let shape = shard_shape(
        Some(connections),
        peers,
        (RangeShards::First, NativeShards::First),
    )
    .unwrap();
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: shape.tasks,
        timers_per_shard: shape.timers,
        interests_per_shard: shape.interests,
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

/// A door node's task: opens the node, binds its S3 listener on its door, asks its enlistment of
/// the root range as a node's process does (`process::serve`), then turns the node, each turn raced
/// against the test's next order and the listener's accepts, saying where it listens, its
/// enlistment, and the ranges' leads as they move.
async fn run_door(
    mut settings: NodeSettings<Stored<DeviceFile>>,
    (enlistment, when): (root::Enlist, Enlist),
    mut orders: ChannelReceiver<Told>,
    said: mpsc::Sender<Said>,
) {
    let id = settings.id;
    let failed = |why: String| {
        let _ = said.send(Said::Failed(id, why));
    };
    settings.transport_bytes =
        mantle_protocol::bounds::least_transport(&settings.stated().unwrap()).unwrap();
    let mut node = match Node::open(settings).await {
        Ok(node) => node,
        Err(e) => return failed(e.to_string()),
    };
    let (quic, plane) = match node.addresses() {
        Ok(at) => at,
        Err(e) => return failed(e.to_string()),
    };
    if said.send(Said::Bound(id, quic, plane)).is_err() {
        return;
    }
    // Its door holds a request for each connection the listener serves, as a process's does
    // (`process::listen`).
    let connections = u64::try_from(OBJECTS).unwrap();
    let listener = node
        .open_door(OBJECTS)
        .map_err(|e| e.to_string())
        .and_then(|door| {
            Listener::bind(
                ListenerSettings {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    region: REGION.into(),
                    domains: Vec::new(),
                    access: vec![access_key()],
                    tls: None,
                    node: id,
                    connections,
                },
                door,
            )
            .map_err(|e| e.to_string())
        });
    let listener = match listener {
        Ok(listener) => listener,
        Err(why) => return failed(why),
    };
    match listener.local_addr() {
        Ok(at) => {
            if said.send(Said::S3(id, at)).is_err() {
                return;
            }
        }
        Err(e) => return failed(e.to_string()),
    }
    // Its enlistment, asked once, until the root range answers it: the range's laws, not a
    // deadline of the node's, decide when (`process::serve`).
    let mut held = Some(enlistment);
    let mut enlisting = None;
    let ask = |node: &mut Node<Stored<DeviceFile>>, held: &mut Option<root::Enlist>| {
        held.take().map(|enlist| {
            let command = Command::Root(root::Command::Enlist(enlist));
            node.command(ROOT, command, u64::MAX, Wait::Answered)
        })
    };
    if when == Enlist::AtOnce {
        match ask(&mut node, &mut held) {
            Some(Ok(asked)) => enlisting = Some(asked),
            Some(Err(e)) => return failed(e.to_string()),
            None => {}
        }
    }
    let mut accepting = pin!(listener.serve());
    let mut told: Option<(Vec<u64>, u64)> = None;
    loop {
        let ended = {
            let turn = pin!(node.turn());
            let order = pin!(orders.recv());
            race3(turn, order, accepting.as_mut()).await
        };
        match ended {
            Either3::First(Ok(())) => {}
            Either3::First(Err(e)) => return failed(e.to_string()),
            Either3::Second(Ok(Told::Learn(peer, quic, plane))) => {
                if let Err(e) = node.learn(peer, quic, plane) {
                    return failed(e.to_string());
                }
            }
            Either3::Second(Ok(Told::Enlist)) => match ask(&mut node, &mut held) {
                Some(Ok(asked)) => enlisting = Some(asked),
                Some(Err(e)) => return failed(e.to_string()),
                None => {}
            },
            // The test is done.
            Either3::Second(Err(_)) => return,
            Either3::Third(ended) => return failed(format!("the S3 listener ended: {ended}")),
        }
        // Nothing here proposed: what the ranges answered went to its asker's exchange.
        node.take_applied().for_each(drop);
        if let Some(asked) = enlisting
            && let Some(outcome) = node.take_command(asked)
        {
            enlisting = None;
            match outcome {
                CommandOutcome::Answered(Answer::Root(root::Outcome::Enlisted)) => {
                    if said.send(Said::Enlisted(id)).is_err() {
                        return;
                    }
                }
                other => return failed(format!("its enlistment was answered {other:?}")),
            }
        }
        let leaders: Vec<u64> = LAYERED
            .iter()
            .map(|(group, _)| node.status(*group).map_or(0, |s| s.leader))
            .collect();
        let now = (leaders, node.lead_moves().0);
        if told.as_ref() != Some(&now) {
            if said.send(Said::Leads(id, now.0.clone(), now.1)).is_err() {
                return;
            }
            told = Some(now);
        }
    }
}

impl DoorCell {
    /// A cell of [`NODES`] nodes that host objects started, each told where the others listen, and
    /// each node's S3 listener bound; each enlists as `enlist` says.
    pub fn start(pki: &Pki, enlist: Enlist) -> Self {
        let (tell, said) = mpsc::channel();
        let mut cell = Self {
            pki: pki.clone(),
            _dirs: Vec::new(),
            orders: BTreeMap::new(),
            said,
            threads: Vec::new(),
            quic: BTreeMap::new(),
            planes: BTreeMap::new(),
            s3: BTreeMap::new(),
            enlisted: BTreeSet::new(),
            leads: BTreeMap::new(),
        };
        for id in 1..=NODES {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_path_buf();
            cell._dirs.push(dir);
            let (order, taken) = channel(usize::try_from(NODES).unwrap()).unwrap();
            cell.orders.insert(id, order);
            let (pki, tell) = (cell.pki.clone(), tell.clone());
            cell.threads.push(std::thread::spawn(move || {
                let failed = tell.clone();
                let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let volume = object_volume(&path);
                    std::fs::create_dir_all(&volume.dir).unwrap();
                    // Its volume opens before its runtime runs: a volume's recovery waits on its
                    // device, which no shard may.
                    let storage = Storage::open(&[volume]).unwrap();
                    let when = enlist;
                    let enlist = enlisting(id);
                    let enlistment = root::Enlist {
                        node: id,
                        domain: enlist.domain.clone(),
                        volumes: storage.enlisting(),
                        max_nodes: enlist.max_nodes,
                        max_volumes: enlist.max_volumes,
                        at_ns: 0,
                    };
                    let peers = usize::try_from(NODES - 1 + CLIENTS + OBJECT_CLIENTS).unwrap();
                    let rt = door_runtime(peers);
                    let make = move || {
                        // The node's shard is its metadata shard: its replicas apply through the
                        // engine's synchronous calls (node.md §1.2).
                        mantle_engine::store::permit_blocking();
                        let members = usize::try_from(enlist.max_nodes).unwrap();
                        let ranges = LAYERED
                            .iter()
                            .map(|&(group, layer)| Hosted {
                                group,
                                range: Range {
                                    layer,
                                    members,
                                    ..range(pki.voters)
                                },
                                engine: layer_engine(&path, group, layer),
                            })
                            .collect();
                        let mut settings = pki.settings_hosting(id, &path, ranges);
                        settings.storage = storage;
                        // A log group for each range, and the liveness stream's own.
                        settings.log.max_groups = LAYERED.len() + 1;
                        settings.chunk_asks = OBJECT_CHUNK_ASKS;
                        settings.gateway = Some(gateway(&enlist));
                        // A node that enlists states the cell's bound on its nodes, as a process
                        // does (`process::serve`).
                        settings.max_nodes = enlist.max_nodes;
                        run_door(settings, (enlistment, when), taken, tell)
                    };
                    on_first_shard(rt, make);
                }));
                if ran.is_err() {
                    let _ = failed.send(Said::Failed(id, "its thread panicked".into()));
                }
            }));
        }
        cell.until("every node bound", |c| {
            let bound = c.quic.len() == c.orders.len() && c.s3.len() == c.orders.len();
            if bound { Some(()) } else { None }
        });
        cell
    }

    /// Tells every node where the others listen.
    fn introduce(&self) {
        for (id, order) in &self.orders {
            for (peer, quic) in &self.quic {
                if peer != id {
                    order
                        .try_send(Told::Learn(*peer, *quic, self.planes[peer]))
                        .map_err(|_| "a node's orders are full")
                        .unwrap();
                }
            }
        }
    }

    /// Takes what a node said; once every node is bound, tells each where the others listen.
    fn take(&mut self, said: Said) {
        match said {
            Said::Bound(id, quic, plane) => {
                self.quic.insert(id, quic);
                self.planes.insert(id, plane);
                if self.quic.len() == self.orders.len() {
                    self.introduce();
                }
            }
            Said::S3(id, at) => {
                self.s3.insert(id, at);
            }
            Said::Enlisted(id) => {
                self.enlisted.insert(id);
            }
            Said::Leads(id, leaders, moves) => {
                self.leads.insert(id, (leaders, moves));
            }
            Said::Failed(id, why) => panic!("node {id} failed: {why}"),
        }
    }

    /// Waits on what the nodes say until `done` holds, each saying taken as it comes; gives up once
    /// [`patience`] passes with nothing said. Every node is told where the others listen once all
    /// are bound.
    pub fn until<T>(&mut self, what: &str, mut done: impl FnMut(&Self) -> Option<T>) -> T {
        loop {
            if let Some(t) = done(self) {
                return t;
            }
            match self.said.recv_timeout(patience()) {
                Ok(said) => self.take(said),
                Err(e) => panic!("{what}: nothing said in {:?} ({e})", patience()),
            }
        }
    }

    /// Tells node `id` to ask its enlistment, held until now ([`Enlist::Held`]).
    pub fn enlist(&self, id: u64) {
        self.orders[&id]
            .try_send(Told::Enlist)
            .map_err(|_| "a node's orders are full")
            .unwrap();
    }

    /// Takes what the nodes said so far, without waiting.
    pub fn heard(&mut self) {
        while let Ok(said) = self.said.try_recv() {
            self.take(said);
        }
    }

    /// The hand-overs every node began, in all.
    pub fn lead_moves(&self) -> u64 {
        self.leads.values().map(|(_, moves)| moves).sum()
    }

    /// Asks `ops` in order through node `first`'s native door, the client asking the others after
    /// it as it moves on, on a thread and a runtime of its own: mantle-client as its command line
    /// asks, under native client `client`'s certificate. Each answer is sent on `told` as it comes,
    /// and `go_on` is asked before each operation: a false ends the thread.
    pub fn native(
        &self,
        (first, client): (u64, u64),
        ops: impl Iterator<Item = ObjectOp> + Send + 'static,
        go_on: impl Fn() -> bool + Send + 'static,
        told: mpsc::Sender<Answered>,
    ) -> JoinHandle<()> {
        let mut nodes: Vec<(u64, SocketAddr)> =
            self.quic.iter().map(|(id, at)| (*id, *at)).collect();
        nodes.sort_by_key(|(id, _)| *id != first);
        let credentials = self.pki.credentials(&self.pki.objects[&client]);
        std::thread::spawn(move || {
            let sockets = mantle_client::sockets(nodes.len()).unwrap();
            let transport_bytes = mantle_client::least_transport(nodes.len()).unwrap();
            shard(sockets)
                .block_on(async move {
                    let settings = mantle_client::Settings {
                        id: client,
                        bind: "127.0.0.1:0".parse().unwrap(),
                        credentials,
                        nodes,
                        names: Vec::new(),
                        transport_bytes,
                    };
                    let mut ops = ops;
                    let mut native = match mantle_client::Client::open(settings).await {
                        Ok(native) => native,
                        Err(e) => {
                            if let Some(op) = ops.next() {
                                let answer = Err(format!("the client did not open: {e}"));
                                let _ = told.send(Answered {
                                    door: "native",
                                    op,
                                    answer,
                                });
                            }
                            return;
                        }
                    };
                    for op in ops {
                        if !go_on() {
                            return;
                        }
                        let answer = native_op(&mut native, &op).await;
                        let answered = Answered {
                            door: "native",
                            op,
                            answer,
                        };
                        if told.send(answered).is_err() {
                            return;
                        }
                    }
                })
                .unwrap();
        })
    }

    /// Asks `ops` in order through node `at`'s S3 listener on a thread of its own, as
    /// [`DoorCell::native`] asks through the native door.
    pub fn s3(
        &self,
        at: u64,
        ops: impl Iterator<Item = ObjectOp> + Send + 'static,
        go_on: impl Fn() -> bool + Send + 'static,
        told: mpsc::Sender<Answered>,
    ) -> JoinHandle<()> {
        let at = self.s3[&at];
        std::thread::spawn(move || {
            let key = s3::Key {
                id: ACCESS_KEY_ID.into(),
                secret: SECRET_KEY.into(),
            };
            let mut client = s3::Client::new(at, key, REGION);
            // The client blocks on its socket; its pauses between attempts are a runtime timer's,
            // on the thread's own shard.
            shard(0)
                .block_on(async move {
                    for op in ops {
                        if !go_on() {
                            return;
                        }
                        let answer = s3_op(&mut client, &op).await;
                        if told
                            .send(Answered {
                                door: "s3",
                                op,
                                answer,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                })
                .unwrap();
        })
    }
}

/// A put's bytes held whole, given from their first again where the client asks them again after
/// `SlowDown` (`mantle_client::Source::again`).
struct Again<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl mantle_client::Source for Again<'_> {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let rest = self.bytes.get(self.at..).unwrap_or_default();
        let n = rest.len().min(into.len());
        into[..n].copy_from_slice(&rest[..n]);
        self.at += n;
        Ok(n)
    }

    async fn again(&mut self) -> std::io::Result<bool> {
        self.at = 0;
        Ok(true)
    }
}

/// `op` asked through the native door: done, a get with its bytes; else its status or error.
async fn native_op(client: &mut mantle_client::Client, op: &ObjectOp) -> Result<Vec<u8>, String> {
    let replied = match op {
        ObjectOp::CreateBucket(bucket) => {
            let op = Op::Create {
                bucket: bucket.clone(),
                id: mantle_client::identity().map_err(|e| e.to_string())?,
            };
            client
                .call(&op, u64::MAX)
                .await
                .map(|reply| (reply, Vec::new()))
        }
        ObjectOp::Put(bucket, key, bytes) => {
            let op = Op::Put {
                bucket: bucket.clone(),
                key: key.clone(),
                id: mantle_client::identity().map_err(|e| e.to_string())?,
                length: u64::try_from(bytes.len()).unwrap(),
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: Some(mantle_client::TRAILER),
                metadata: Vec::new(),
            };
            let mut source = Again { bytes, at: 0 };
            client
                .put(&op, &mut source, u64::MAX)
                .await
                .map(|reply| (reply, Vec::new()))
        }
        ObjectOp::Get(bucket, key) => {
            let op = Op::Get {
                bucket: bucket.clone(),
                key: key.clone(),
                headers: Headers::default(),
                version: None,
            };
            let mut sink = Vec::new();
            client
                .get(&op, &mut sink, u64::MAX)
                .await
                .map(|reply| (reply, sink))
        }
    };
    match replied {
        Ok((reply, bytes)) if reply.status == Status::Done => Ok(bytes),
        Ok((reply, _)) => Err(format!("{:?}", reply.status)),
        Err(e) => Err(e.to_string()),
    }
}

/// `op` asked through an S3 listener, asked again after `503 SlowDown` as botocore's standard
/// retry mode asks, mantle-client's own rule: at most `ATTEMPTS` attempts, the `n`-th pause the
/// most of its full jitter, `min(2^(n-1) SLOWED_BASE, SLOWED_MOST)`. Done (`200`), a get with
/// its bytes; else the status and the error's code.
async fn s3_op(client: &mut s3::Client, op: &ObjectOp) -> Result<Vec<u8>, String> {
    let (method, target, body) = match op {
        ObjectOp::CreateBucket(bucket) => ("PUT", format!("/{bucket}"), &[][..]),
        ObjectOp::Put(bucket, key, bytes) => ("PUT", format!("/{bucket}/{key}"), bytes.as_slice()),
        ObjectOp::Get(bucket, key) => ("GET", format!("/{bucket}/{key}"), &[][..]),
    };
    let mut attempt = 1;
    loop {
        // The test client unwinds where its connection fails: told as the answer, and the next
        // operation asked on a connection of its own.
        let sent = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            client.send(method, &target, &[], body)
        }));
        let Ok(response) = sent else {
            client.drop_connection();
            return Err("the S3 client's connection failed".into());
        };
        if response.status == 200 {
            return Ok(response.body);
        }
        if response.status != 503 || attempt == mantle_client::ATTEMPTS {
            let code = response.element("Code").unwrap_or_default();
            return Err(format!("{} {code}", response.status));
        }
        let pause =
            (mantle_client::SLOWED_BASE * 2u32.pow(attempt - 1)).min(mantle_client::SLOWED_MOST);
        hyper_rt::futures::sleep(u64::try_from(pause.as_nanos()).unwrap())
            .await
            .map_err(|e| format!("the pause: {e:?}"))?;
        attempt += 1;
    }
}
