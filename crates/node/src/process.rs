//! A node as a process (`mantle node`, docs/design/node.md §1): its configuration, stated in a
//! file, and its run on a runtime of one shard, the node's metadata shard (§1.2), whose replicas
//! apply through the engine's synchronous calls.
//!
//! **The file** states every number the node is run with, one `key = value` a line (`#` begins a
//! comment): nothing is assumed for the operator, and a key it does not know is refused at its
//! line ([`KEYS`], [`REPEATED`]). A `peer` line, repeated, names a peer's id, its
//! role (`node` or `gateway`) and its certificate's file, and may name where it listens,
//! QUIC's endpoint and the plane's, `peer = ID ROLE CERTIFICATE QUIC PLANE`, each `HOST:PORT` with
//! the host an address (an IPv6 one in brackets) or a name, resolved as the node starts
//! ([`resolve`]); a peer named without them is told where it listens on standard input, as one
//! that moved is. Certificates and the key are
//! DER (the key PKCS #8). A `volume` line, repeated, names a chunk volume of the node's storage
//! role (node.md §1.1): `volume = DIR SIZE DEPTH READ_BYTES READ_GAP PREWRITE`, its device
//! directory, its bytes, and what its device measured (`mantle disk probe --measure`); the layout
//! every volume shares is the `volume.` keys. `chunk_bytes` is the largest chunk the cell's
//! gateways write, `chunk_asks` the chunk puts and gets the node keeps out to its peers, and
//! `command_asks` and `read_asks` the commands and reads it asks of ranges itself and holds at
//! once. A node with volumes
//! states its failure domain (`domain`, at most 255 bytes) and the cell's bounds on its volume
//! table (`root.max_nodes`, `root.max_volumes`), and hosts the root range: once it listens it
//! enlists itself and its volumes there (node.md §6.6), says `enlisted` once the range took them,
//! and stops, naming the range's answer, on a refusal. A node that hosts objects for clients and
//! gateways (node.md §4–§5) names its root key's generations, each `root_key = GENERATION FILE`, 32
//! bytes only its owner may read (encryption.md §2), and states its `gateway.` and `durability.`
//! keys: how long a block is held for its file's write, the blocks an object holds going down or
//! being read, the operations it runs at once, the clients of the native protocol it admits at
//! once, each by the certificate the cell's authority issued it for the client role and never by a
//! `peer` line (node.md §3.6), the buckets an owner may have, the rows a bucket's
//! coordinator passes in a read, and the chunk loss and repair rates, per hour, each event that
//! strikes chunks across domains at once, `durability.burst = RATE FRACTION` (its rate per hour and
//! the share of the nodes it destroys; none where no line names one), and the annual loss
//! probability a block's scheme is chosen to meet.
//!
//! **`mantle serve`** ([`serve_stated`]) reads a file of its operator's statements alone
//! ([`STATED_KEYS`], [`STATED_REPEATED`]), measures what it needs of its host and devices, derives
//! every other value (`crate::derive::derive`), says each on standard output, `basis KEY = VALUE
//! (KIND: WHY)`, before anything opens, and runs as `mantle node` does.
//!
//! **Its start** runs node.md §1.7's steps in order, each owning what it starts: the configuration;
//! the identity (the node's key and certificate, the authority's and each peer's, the root keys);
//! the node directory's `LOCK` and each device directory's, held while the process runs; for
//! `mantle serve`, the devices measured and the budgets derived; the storage (the volumes, the log,
//! each range's store and replica); the network (the peers' addresses resolved, the runtime, the
//! QUIC endpoint and the plane bound). A failure ends the start naming its step ([`Step`],
//! [`ProcessError::Start`]), and the process exits non-zero.
//!
//! **Its talk** is lines. On standard output: `bound QUIC PLANE` once the node listens, and
//! `volume ID` for each volume it holds, its 128-bit id in hex; then, for each range it hosts,
//! `status GROUP TERM LEADER COMMIT APPLIED PATIENCE_NS RECONNECT_NS MEASURING_NS ASKING_NS HELD`,
//! the group in hex, each time one of the term, leader, commit or applied index
//! moves, its redials to a peer back off further or start over, or the heartbeats it takes from
//! peers not yet judged move (`Status::measuring`): how long the node states it may go
//! without moving
//! while its peers live (`Status::patience`, 0 until measured), how long a link to a peer it dials
//! may go without moving should that peer fall silent (`Status::reconnect`, 0 when it dials none);
//! while
//! some peer is not yet judged, the longest its stream's heartbeats go between
//! (`Status::measuring_for`, else 0); while a command or read the node asks of the range itself
//! waits paused, the longest the pause may last, its backoff (`Status::asking`, else 0), said each
//! time it changes; and 1 while the node waits on its own device, said each time that changes
//! (`Status::held`, else 0). After each status, `log FLUSHING_NS LONGEST_NS`: how
//! long the log's flush has been out (0 when none is) and the longest an update has waited for
//! its commit (hyper-log's `LogStats`), since the node moves only as its writes become durable;
//! and
//! `replaced GROUP FAILED JOINING` once, from the node that led the range then, when a
//! replacement ended; and
//! `link PEER KEYED DIALS ENDED SUSPECTED REFUSAL` when a peer's link changes: whether the plane
//! holds keys for its connection, the dials made to it, the connections and dials to it that
//! ended, the times the node's detectors came to suspect it, and the last refusal one ended with
//! (`Node::links`); and `moved BYTES SERVE_NS` as its transfers move:
//! the bytes of exchanges' bodies it has read and written (`Node::moved`), said at most every half
//! of the transport's serve period `SERVE_NS`, the longest an exchange it carries may go without
//! moving before the transport refuses it; and, in an orderly stop, `stopping` as it begins,
//! `handed GROUP LEADER` for each range whose lead it handed over, and `stopped` as it ends. A
//! node with an S3 endpoint says `s3-bound ADDR CONNECTIONS` once its listener is bound: where it
//! listens and the connections it holds at once (node.md §4.1). On standard input:
//! `learn ID QUIC PLANE`, where a peer listens, `replace FAILED JOINING`, a member lost for good
//! and the spare that takes its place in every range the node hosts (`Node::replace`), `stop`,
//! which asks the orderly stop as SIGTERM does, and `chunks`, answered `chunks RECEIVED WRITTEN
//! HELD UNKEPT FRAGMENTS BUDGET FREE`: the bytes of chunks its peers put on it over QUIC, of those
//! it put on its own volumes, of those its volumes hold and of those of the first two no volume
//! kept; the fragments its volumes' indexes hold and the most they may; and the bytes of its
//! volumes' free segments (`Node::chunk_bytes`). A line it cannot read ends the process
//! with the line named. Standard input closing ends `mantle node`, its
//! harness's child, at once, without the orderly stop's steps; `mantle serve` runs on, as under a
//! service manager, whose input is empty.
//!
//! The ranges it hosts are the `range` lines', each `range = GROUP LAYER`: a group of the voters
//! the file names, holding the rows of one layer (`root`, the cell's volume table; `bucket`,
//! `name`, `file` or `block`), at most one of each, since a cell's first ranges each span their layer's whole key space (node.md
//! §1.5) and splitting them is the range directory's. Each keeps its rows in an engine store of
//! its own in the node's directory, `range-GROUP.store`: founded on first start, opened at its
//! last checkpoint after, the log replayed past it. A node whose id the voters do not name is a
//! spare: it starts as the founders did and is no member of the ranges until a replacement adds
//! it.

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::time::Duration;

use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, LogStats, Waits};
use hyper_rt::combine::{Either3, race3};
use hyper_rt::machine::calibration::{Calibration, Policy};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::signal::Signal;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_ec::durability::{Burst, Rates};
use mantle_engine::store::{Config as StoreConfig, Shape, Store};
use mantle_engine::trunk::TrunkConfig;
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::name;
use mantle_meta::record::History;
use mantle_meta::root::{self, Enlist, Lived, MAX_DOMAIN};
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command};
use mantle_range::{ConfState, Range, Settings, Stored};
use mantle_s3::seal::WrappingKey;

use crate::ask::CommandOutcome;
use crate::derive::{Device, Formatted, Measured, Stated, VolumeFacts};
use crate::directory::Peer;
use crate::history;
use crate::layout::{Layout, LayoutError, Stopped};
use crate::node::{
    GatewaySettings, Hosted, Link, Node, NodeError, NodeSettings, Status, Step, Wall,
};
use crate::secret::{self, SecretError};
use crate::storage::{Locks, NodeLock, Storage, StorageError, VolumeSettings};
use mantle_protocol::classes::Role;

/// Why the process stopped.
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    /// A file the configuration is, or names, that could not be read.
    #[error("{}: {}", .0.display(), .1)]
    Read(PathBuf, String),
    #[error("the configuration, line {0}: {1}")]
    Line(usize, String),
    #[error("the configuration states no {0}")]
    Missing(&'static str),
    #[error("the configuration's {0}: {1}")]
    Value(&'static str, String),
    #[error("the runtime: {0}")]
    Runtime(String),
    #[error("the engine: {0}")]
    Engine(String),
    #[error("the node: {0}")]
    Node(#[from] NodeError),
    #[error("the node's storage: {0}")]
    Storage(#[from] StorageError),
    #[error("standard input, line {0:?}: {1}")]
    Input(String, &'static str),
    #[error("the root range refused the node's enlistment: {0}")]
    Enlist(String),
    #[error("the root key {0}: {1}")]
    Key(PathBuf, String),
    /// A key file that could not be read, or that others than its owner may read.
    #[error("{}: {}", .0.display(), .1)]
    Secret(PathBuf, SecretError),
    /// A certificate the cell's authority did not issue, or one outside its validity.
    #[error("{}: {}", .0.display(), .1)]
    Certificate(PathBuf, mantle_cert::CertError),
    #[error("the settings: {0}")]
    Derive(#[from] crate::derive::DeriveError),
    #[error("the device under {0}: {1}")]
    Device(PathBuf, String),
    #[error("standard output: {0}")]
    Say(String),
    #[error("peer {0}'s address {1}: {2}")]
    Resolve(u64, String, String),
    #[error("the start's {0} step: {1}")]
    Start(Step, Box<ProcessError>),
    #[error("stopped at once: {0}")]
    Stopped(&'static str),
    #[error("the node's history: {0}")]
    History(String),
    #[error("the S3 listener: {0}")]
    Listener(String),
}

/// The signals that stop the node in order (node.md §1.7): SIGTERM, which a service manager or a
/// container's runtime sends, and SIGINT.
#[cfg(unix)]
const STOP_SIGNALS: &[Signal] = &[Signal::Terminate, Signal::Interrupt];
/// The signals that stop the node in order (node.md §1.7): Ctrl-C and Ctrl-Break. A console's
/// close, a logoff and a shutdown end the process as a crash would: hyper-rt's handler returns as
/// it delivers them, and Windows then ends the process (node.md §1.7).
#[cfg(windows)]
const STOP_SIGNALS: &[Signal] = &[Signal::Interrupt, Signal::Break];

/// The orderly stop as the process is asked it (node.md §1.7), by a signal or a `stop` line: the
/// first request begins it, to end by its bound; a second ends the process at once.
struct Stop {
    bound_ns: u64,
    /// When the stop ends at the latest, once begun.
    by: Option<u64>,
}

impl Stop {
    fn new(bound: Duration) -> Self {
        Self {
            bound_ns: u64::try_from(bound.as_nanos()).unwrap_or(u64::MAX),
            by: None,
        }
    }

    /// A request at `now`: the stop begins, or, begun already, the process ends at once, for the
    /// reason said.
    fn ask(&mut self, now: u64) -> Result<(), &'static str> {
        if self.by.is_some() {
            return Err("a second request to stop came as the node stopped");
        }
        self.by = Some(now.saturating_add(self.bound_ns));
        Ok(())
    }
}

/// `error` as a failure of the start's `step` (node.md §1.7).
fn step<T: Into<ProcessError>>(at: Step) -> impl FnOnce(T) -> ProcessError {
    move |error| ProcessError::Start(at, Box::new(error.into()))
}

/// A node's failure to open as the process says it: one in a step of its start as that step's.
fn opened(error: NodeError) -> ProcessError {
    match error {
        NodeError::Start(at, inner) => {
            ProcessError::Start(at, Box::new(ProcessError::Node(*inner)))
        }
        other => ProcessError::Node(other),
    }
}

/// What the node is and whom it trusts, read at its start's first step (node.md §1.7): its
/// certificate and key, the cell authority's certificate, each peer's certificate and the root
/// key's generations.
struct Identity {
    credentials: Credentials,
    peers: Vec<Peer>,
    keys: Vec<(u32, WrappingKey)>,
}

/// Reads the identity the configuration names: the node's `certificate` and `key`, the cell's
/// `authority`, each of `peers`' certificates and each of `keys`' generations. The node's
/// certificate and each peer's are refused unless the authority issued them and they are valid by
/// this machine's clock (`mantle_cert::issued_by`, RFC 5280 §6.1.3 (a)), so a directory holding
/// another cell's identity, or a peer pinned from another cell, refuses the start rather than
/// every handshake.
fn identity(
    (certificate, key, authority): (&Path, &Path, &Path),
    peers: &[PeerLine],
    keys: &[(u32, PathBuf)],
) -> Result<Identity, ProcessError> {
    let authority = read_file(authority)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| ProcessError::Runtime(e.to_string()))?
        .as_secs();
    let issued = |path: &Path| -> Result<Vec<u8>, ProcessError> {
        let der = read_file(path)?;
        mantle_cert::issued_by(&der, &authority, now)
            .map_err(|e| ProcessError::Certificate(path.to_path_buf(), e))?;
        Ok(der)
    };
    let chain = vec![CertificateDer::from(issued(certificate)?)];
    let mut known = Vec::with_capacity(peers.len());
    for peer in peers {
        known.push(Peer {
            id: peer.id,
            role: peer.role,
            certificate: issued(&peer.certificate)?,
        });
    }
    let mut root = Vec::with_capacity(keys.len());
    for (generation, path) in keys {
        root.push((*generation, root_key(path)?));
    }
    Ok(Identity {
        credentials: Credentials {
            chain,
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(read_secret(key)?)),
            roots: vec![CertificateDer::from(authority)],
        },
        peers: known,
        keys: root,
    })
}

/// A root key's generation from its file: 32 bytes only its owner may read (encryption.md §2).
fn root_key(path: &Path) -> Result<WrappingKey, ProcessError> {
    let key: [u8; 32] = read_secret(path)?
        .as_slice()
        .try_into()
        .map_err(|_| ProcessError::Key(path.to_path_buf(), "is not 32 bytes".to_owned()))?;
    Ok(WrappingKey::new(&key))
}

/// The key file at `path`, read only where no one but its owner may read it (`crate::secret`).
fn read_secret(path: &Path) -> Result<Vec<u8>, ProcessError> {
    secret::read(path).map_err(|e| ProcessError::Secret(path.to_path_buf(), e))
}

/// Where a peer listens, as its `peer` line names it: a host, an IP address or a name, and a port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    /// `HOST:PORT`, an IPv6 address in brackets (RFC 3986 §3.2.2's form), which is how an address
    /// with a port is written; a name holds neither a colon nor a bracket.
    pub fn parse(text: &str) -> Option<Self> {
        if let Ok(address) = text.parse::<SocketAddr>() {
            return Some(Self {
                host: address.ip().to_string(),
                port: address.port(),
            });
        }
        let (host, port) = text.rsplit_once(':')?;
        if host.is_empty() || host.contains([':', '[', ']']) {
            return None;
        }
        Some(Self {
            host: host.to_owned(),
            port: port.parse().ok()?,
        })
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// A peer as its `peer` line names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerLine {
    pub id: u64,
    pub role: Role,
    pub certificate: PathBuf,
    /// Where it listens, QUIC's endpoint and the plane's, when the line names them.
    pub at: Option<(Endpoint, Endpoint)>,
}

/// What the file states.
pub struct ProcessConfig {
    pub id: u64,
    pub dir: PathBuf,
    pub quic: SocketAddr,
    pub plane: SocketAddr,
    pub authority: PathBuf,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub peers: Vec<PeerLine>,
    /// The ranges the node hosts (`range` lines): each one's group and layer, every one founded
    /// by `voters`.
    pub ranges: Vec<(u128, Layer)>,
    pub voters: Vec<u64>,
    pub settings: Settings,
    pub rules: Rules,
    pub log: LogConfig,
    pub store: StoreConfig,
    pub memtable_bytes: usize,
    pub trunk: TrunkConfig,
    pub transport_bytes: u64,
    /// Each probe of the machine's calibration runs within this (hyper-rt `Calibration::measure`).
    pub probe_budget: Duration,
    /// The orderly stop's bound, from its first signal (node.md §1.7): past it the process ends as a
    /// crash would.
    pub stop: Duration,
    /// The chunk volumes the node keeps (`volume` lines), none for a node with no storage role.
    pub volumes: Vec<VolumeSettings>,
    /// The largest chunk the cell's gateways write, and the chunk puts and gets the node keeps out
    /// to its peers at once (`NodeSettings::chunk_bytes`, `chunk_asks`).
    pub chunk_bytes: u64,
    pub chunk_asks: usize,
    /// The commands the node asks of ranges itself and holds at once
    /// (`NodeSettings::command_asks`).
    pub command_asks: usize,
    /// The reads the node asks of ranges itself and holds at once (`NodeSettings::read_asks`).
    pub read_asks: usize,
    /// How a node with volumes enlists them: none for a node without.
    pub enlist: Option<Enlisting>,
    /// The gateway host's, for a node that hosts objects: none for one that refuses them.
    pub gateway: Option<GatewayConfig>,
}

/// What a node that hosts objects states (`GatewaySettings`), its root keys by their files.
pub struct GatewayConfig {
    pub keys: Vec<(u32, PathBuf)>,
    pub handover_ns: u64,
    pub window: usize,
    pub objects: usize,
    pub clients: usize,
    pub bucket_quota: u32,
    pub scan_rows: u32,
    pub chunk_rate: f64,
    pub repair_rate: f64,
    /// Events that strike chunks across domains at once (`durability.burst` lines).
    pub bursts: Vec<Burst>,
    pub target: f64,
}

/// How a node enlists itself and its volumes in the cell's volume table (node.md §6.6): the root
/// range it hosts, its failure domain, and the cell's bounds on the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enlisting {
    pub root: u128,
    pub domain: String,
    pub max_nodes: u32,
    pub max_volumes: u32,
}

/// The S3 endpoint a node's file states (node.md §4; `mantle init`): where it listens, the region
/// it serves, its certificate and key, and each of the cell's access keys, by its id and the file
/// that holds its account and secret (`access_key = ID FILE`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Stated {
    pub listen: SocketAddr,
    pub region: String,
    /// The DNS names its clients reach it by (`s3.name`), lowercase.
    pub names: Vec<String>,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub access: Vec<(String, PathBuf)>,
}

/// The S3 endpoint's keys, read at the start's identity step: its certificate and key, and each
/// access key's account and secret, read only from a file no one but its owner may read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Keys {
    pub listen: SocketAddr,
    pub region: String,
    pub names: Vec<String>,
    pub certificate: Vec<u8>,
    pub key: Vec<u8>,
    pub access: Vec<AccessKey>,
}

/// An access key: its id, the account it acts for (twelve digits), and its secret.
#[derive(Clone, PartialEq, Eq)]
pub struct AccessKey {
    pub id: String,
    pub account: String,
    pub secret: String,
}

impl std::fmt::Debug for AccessKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessKey")
            .field("id", &self.id)
            .field("account", &self.account)
            .finish_non_exhaustive()
    }
}

/// The keys `mantle node`'s file states once each (node.md §1.4's table): a key it does not know
/// is refused.
pub const KEYS: &[&str] = &[
    "id",
    "dir",
    "quic",
    "plane",
    "authority",
    "certificate",
    "key",
    "voters",
    "range.election_tick",
    "range.heartbeat_tick",
    "range.max_size_per_msg",
    "range.max_inflight_msgs",
    "range.max_uncommitted_size",
    "range.max_committed_size_per_ready",
    "range.max_entry_bytes",
    "rules.lifetime_ns",
    "rules.max_sessions",
    "rules.max_answers",
    "rules.max_answer_bytes",
    "rules.expiries_per_entry",
    "rules.max_key_bytes",
    "rules.max_value_bytes",
    "log.segment_bytes",
    "log.max_segments",
    "log.max_groups",
    "log.group_entries",
    "log.group_bytes",
    "log.group_cache",
    "log.queue_submissions",
    "log.waits",
    "engine.page_size",
    "engine.extent_pages",
    "engine.max_extents",
    "engine.memtable_bytes",
    "engine.fanout",
    "engine.leaf_entries",
    "transport_bytes",
    "runtime.probe_budget_ms",
    "stop_ms",
    "chunk_bytes",
    "chunk_asks",
    "command_asks",
    "read_asks",
    "domain",
    "root.max_nodes",
    "root.max_volumes",
    "volume.segment_bytes",
    "volume.checksum_shift",
    "volume.max_fragments",
    "volume.compact",
    "volume.scrub_period_s",
    "volume.batch_requests",
    "volume.batch_bytes",
    "volume.fragments_per_chunk",
    "gateway.handover_ns",
    "gateway.window",
    "gateway.objects",
    "gateway.clients",
    "gateway.bucket_quota",
    "gateway.scan_rows",
    "durability.chunk_rate",
    "durability.repair_rate",
    "durability.target",
];

/// The lines `mantle node`'s file repeats: its peers, ranges and volumes, its root key's
/// generations and the durability model's bursts.
pub const REPEATED: &[&str] = &["peer", "range", "volume", "root_key", "durability.burst"];

/// The keys `mantle serve`'s file states once each, its operator's alone (node.md §1.4's first
/// column): every other value is measured or derived, and a key of those is refused. `memory`
/// is the most the node may hold, and `durability.target` a block's annual chance of loss at most,
/// each where stated.
pub const STATED_KEYS: &[&str] = &[
    "id",
    "dir",
    "quic",
    "plane",
    "authority",
    "certificate",
    "key",
    "voters",
    "domain",
    "root.max_nodes",
    "root.max_volumes",
    "memory",
    "durability.target",
    "stop_ms",
    "s3",
    "s3.region",
    "s3.certificate",
    "s3.key",
];

/// The lines `mantle serve`'s file repeats: its peers and ranges, its volumes, each `volume = DIR
/// SIZE`, its root key's generations, and the cell's access keys, each `access_key = ID FILE`.
pub const STATED_REPEATED: &[&str] = &[
    "peer",
    "range",
    "volume",
    "root_key",
    "access_key",
    "s3.name",
];

/// The file's values by key, each with the line it was on, and its repeated lines by their key; and
/// the directory the file is in, which a relative path in it names a place from, so a node's
/// directory, its configuration and identity in it, moves to its host whole (`mantle init`).
struct Lines {
    values: BTreeMap<String, (usize, String)>,
    repeated: BTreeMap<&'static str, Vec<(usize, String)>>,
    base: PathBuf,
}

impl Lines {
    /// `text`'s lines, each `key = value` (`#` begins a comment): a key of `known` once, one of
    /// `repeated` any number of times, and any other refused at its line.
    fn parse(text: &str, known: &[&str], repeated: &[&'static str]) -> Result<Self, ProcessError> {
        let mut values = BTreeMap::new();
        let mut lines: BTreeMap<&'static str, Vec<(usize, String)>> = BTreeMap::new();
        for (at, line) in text.lines().enumerate() {
            let number = at.saturating_add(1);
            let line = line.split('#').next().unwrap_or_default().trim();
            if line.is_empty() {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(ProcessError::Line(number, "not key = value".into()));
            };
            let (key, value) = (key.trim(), value.trim().to_owned());
            if let Some(&kind) = repeated.iter().find(|kind| **kind == key) {
                lines.entry(kind).or_default().push((number, value));
            } else if !known.contains(&key) {
                return Err(ProcessError::Line(
                    number,
                    format!("{key} is not a key this configuration states"),
                ));
            } else if values.insert(key.to_owned(), (number, value)).is_some() {
                return Err(ProcessError::Line(number, format!("{key} stated twice")));
            }
        }
        Ok(Self {
            values,
            repeated: lines,
            base: PathBuf::new(),
        })
    }

    /// These lines read from the file at `path`: a relative path in them names a place from its
    /// directory.
    fn of_file(mut self, path: &Path) -> Self {
        self.base = path.parent().map(Path::to_path_buf).unwrap_or_default();
        self
    }

    /// The place `text` names: from the file's directory where it is relative.
    fn path(&self, text: &str) -> PathBuf {
        self.base.join(text)
    }

    /// The repeated lines of `key`, each with the line it was on.
    fn all(&self, key: &str) -> &[(usize, String)] {
        self.repeated.get(key).map_or(&[], Vec::as_slice)
    }

    fn text(&self, key: &'static str) -> Result<&str, ProcessError> {
        self.values
            .get(key)
            .map(|(_, v)| v.as_str())
            .ok_or(ProcessError::Missing(key))
    }

    fn get<T: std::str::FromStr>(&self, key: &'static str) -> Result<T, ProcessError>
    where
        T::Err: std::fmt::Display,
    {
        self.text(key)?
            .parse()
            .map_err(|e: T::Err| ProcessError::Value(key, e.to_string()))
    }

    /// `key`'s value where stated.
    fn maybe<T: std::str::FromStr>(&self, key: &'static str) -> Result<Option<T>, ProcessError>
    where
        T::Err: std::fmt::Display,
    {
        match self.values.contains_key(key) {
            true => self.get(key).map(Some),
            false => Ok(None),
        }
    }
}

/// The `peer` lines, each `peer = ID ROLE CERTIFICATE`, or with where the peer listens,
/// `peer = ID ROLE CERTIFICATE QUIC PLANE`.
fn peers(lines: &Lines) -> Result<Vec<PeerLine>, ProcessError> {
    let mut peers = Vec::with_capacity(lines.all("peer").len());
    for (number, value) in lines.all("peer") {
        let form = || {
            ProcessError::Line(
                *number,
                "peer = ID ROLE CERTIFICATE, or peer = ID ROLE CERTIFICATE QUIC PLANE".into(),
            )
        };
        let mut fields = value.split_whitespace();
        let (Some(id), Some(role), Some(certificate)) =
            (fields.next(), fields.next(), fields.next())
        else {
            return Err(form());
        };
        let endpoint = |text: &str| {
            Endpoint::parse(text).ok_or_else(|| {
                ProcessError::Line(
                    *number,
                    format!("{text}: HOST:PORT, an IPv6 address in brackets"),
                )
            })
        };
        let at = match (fields.next(), fields.next(), fields.next()) {
            (None, None, None) => None,
            (Some(quic), Some(plane), None) => Some((endpoint(quic)?, endpoint(plane)?)),
            _ => return Err(form()),
        };
        let id = id
            .parse()
            .map_err(|_| ProcessError::Line(*number, "a peer's id".into()))?;
        let role = match role {
            "node" => Role::Node,
            "gateway" => Role::Gateway,
            "client" => {
                return Err(ProcessError::Line(
                    *number,
                    "a client is admitted by the certificate the cell's authority issued it, \
                     never pinned (node.md §3.6)"
                        .into(),
                ));
            }
            _ => {
                return Err(ProcessError::Line(
                    *number,
                    "a role: node or gateway".into(),
                ));
            }
        };
        peers.push(PeerLine {
            id,
            role,
            certificate: lines.path(certificate),
            at,
        });
    }
    Ok(peers)
}

/// Where each peer whose line names it listens, resolved now (node.md §1.7 step 5) through the
/// platform's resolver, as hyper-rt's `dns::resolve` does (std's `ToSocketAddrs`: getaddrinfo on
/// Unix, GetAddrInfoW on Windows): the first of a name's addresses in the family of the node's own
/// socket, the only family that socket sends to. It runs before the node's runtime starts, so a
/// lookup's wait holds no shard.
pub fn resolve(config: &ProcessConfig) -> Result<Vec<(u64, SocketAddr, SocketAddr)>, ProcessError> {
    let mut addresses = Vec::with_capacity(config.peers.len());
    for peer in &config.peers {
        if let Some((quic, plane)) = &peer.at {
            addresses.push((
                peer.id,
                lookup(peer.id, quic, config.quic)?,
                lookup(peer.id, plane, config.plane)?,
            ));
        }
    }
    Ok(addresses)
}

/// The first of `at`'s addresses in `own`'s family.
fn lookup(peer: u64, at: &Endpoint, own: SocketAddr) -> Result<SocketAddr, ProcessError> {
    let refused = |why: String| ProcessError::Resolve(peer, at.to_string(), why);
    (at.host.as_str(), at.port)
        .to_socket_addrs()
        .map_err(|e| refused(e.to_string()))?
        .find(|address| address.is_ipv4() == own.is_ipv4())
        .ok_or_else(|| {
            let family = if own.is_ipv4() { "IPv4" } else { "IPv6" };
            refused(format!("no {family} address, the family the node binds"))
        })
}

/// The `voters` key: ids, comma-separated.
fn voters(lines: &Lines) -> Result<Vec<u64>, ProcessError> {
    lines
        .text("voters")?
        .split(',')
        .map(|v| v.trim().parse())
        .collect::<Result<Vec<u64>, _>>()
        .map_err(|e| ProcessError::Value("voters", e.to_string()))
}

/// The `root_key` lines, each `root_key = GENERATION FILE`, generations in increasing order, the
/// last the current one.
fn root_keys(lines: &Lines) -> Result<Vec<(u32, PathBuf)>, ProcessError> {
    let mut keys: Vec<(u32, PathBuf)> = Vec::with_capacity(lines.all("root_key").len());
    for (number, value) in lines.all("root_key") {
        let bad = || ProcessError::Line(*number, "root_key = GENERATION FILE".into());
        let fields: Vec<&str> = value.split_whitespace().collect();
        let [generation, file] = fields.as_slice() else {
            return Err(bad());
        };
        let generation: u32 = generation.parse().map_err(|_| bad())?;
        if keys.last().is_some_and(|(last, _)| *last >= generation) {
            return Err(ProcessError::Line(
                *number,
                "root key generations in increasing order".into(),
            ));
        }
        keys.push((generation, lines.path(file)));
    }
    Ok(keys)
}

impl ProcessConfig {
    /// The configuration the file at `path` states.
    pub fn read(path: &Path) -> Result<Self, ProcessError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ProcessError::Read(path.to_path_buf(), e.to_string()))?;
        let lines = Lines::parse(&text, KEYS, REPEATED)?.of_file(path);
        let peers = peers(&lines)?;
        let voters = voters(&lines)?;
        let waits = match lines.text("log.waits")? {
            "measured" => Waits::Measured,
            "never" => Waits::Never,
            other => return Err(ProcessError::Value("log.waits", other.to_owned())),
        };
        let config = Self {
            id: lines.get("id")?,
            dir: lines.path(lines.text("dir")?),
            quic: lines.get("quic")?,
            plane: lines.get("plane")?,
            authority: lines.path(lines.text("authority")?),
            certificate: lines.path(lines.text("certificate")?),
            key: lines.path(lines.text("key")?),
            peers,
            ranges: ranges(&lines)?,
            voters,
            settings: Settings {
                election_tick: lines.get("range.election_tick")?,
                heartbeat_tick: lines.get("range.heartbeat_tick")?,
                max_size_per_msg: lines.get("range.max_size_per_msg")?,
                max_inflight_msgs: lines.get("range.max_inflight_msgs")?,
                max_uncommitted_size: lines.get("range.max_uncommitted_size")?,
                max_committed_size_per_ready: lines.get("range.max_committed_size_per_ready")?,
                max_entry_bytes: lines.get("range.max_entry_bytes")?,
            },
            rules: Rules {
                lifetime_ns: lines.get("rules.lifetime_ns")?,
                max_sessions: lines.get("rules.max_sessions")?,
                max_answers: lines.get("rules.max_answers")?,
                max_answer_bytes: lines.get("rules.max_answer_bytes")?,
                expiries_per_entry: lines.get("rules.expiries_per_entry")?,
                max_key_bytes: lines.get("rules.max_key_bytes")?,
                max_value_bytes: lines.get("rules.max_value_bytes")?,
            },
            log: LogConfig {
                segment_bytes: lines.get("log.segment_bytes")?,
                max_segments: lines.get("log.max_segments")?,
                max_groups: lines.get("log.max_groups")?,
                group_entries: lines.get("log.group_entries")?,
                group_bytes: lines.get("log.group_bytes")?,
                group_cache: lines.get("log.group_cache")?,
                queue_submissions: lines.get("log.queue_submissions")?,
                waits,
            },
            store: StoreConfig {
                page_size: lines.get("engine.page_size")?,
                extent_pages: lines.get("engine.extent_pages")?,
                max_extents: lines.get("engine.max_extents")?,
            },
            memtable_bytes: lines.get("engine.memtable_bytes")?,
            trunk: TrunkConfig {
                fanout: lines.get("engine.fanout")?,
                leaf_entries: lines.get("engine.leaf_entries")?,
            },
            transport_bytes: lines.get("transport_bytes")?,
            probe_budget: Duration::from_millis(lines.get("runtime.probe_budget_ms")?),
            stop: Duration::from_millis(lines.get("stop_ms")?),
            volumes: volumes(&lines)?,
            chunk_bytes: lines.get("chunk_bytes")?,
            chunk_asks: lines.get("chunk_asks")?,
            command_asks: lines.get("command_asks")?,
            read_asks: lines.get("read_asks")?,
            enlist: enlisting(&lines)?,
            gateway: gateway(&lines)?,
        };
        if !config.rules.admit_sessions() {
            return Err(ProcessError::Value(
                "rules.max_key_bytes",
                "with rules.max_value_bytes, admits no row a session writes of itself".into(),
            ));
        }
        // An engine that took a row past its page would fence at its next checkpoint, on every
        // member at once, and again at each restart (`mantle_range::holds_rows`).
        if !mantle_range::holds_rows(config.store.page_size, config.memtable_bytes, &config.rules) {
            return Err(ProcessError::Value(
                "engine.page_size",
                format!(
                    "a page of {} bytes and a memtable of {} hold no row of a {}-byte key and a \
                     {}-byte value (rules.max_key_bytes, rules.max_value_bytes)",
                    config.store.page_size,
                    config.memtable_bytes,
                    config.rules.max_key_bytes,
                    config.rules.max_value_bytes
                ),
            ));
        }
        Ok(config)
    }
}

/// The ranges the `range` lines state, each `range = GROUP LAYER`: its group in hex, as the log
/// names it, and the layer whose rows it holds, at most one range of each layer.
fn ranges(lines: &Lines) -> Result<Vec<(u128, Layer)>, ProcessError> {
    if lines.all("range").is_empty() {
        return Err(ProcessError::Missing("range"));
    }
    let mut ranges: Vec<(u128, Layer)> = Vec::with_capacity(lines.all("range").len());
    for (number, value) in lines.all("range") {
        let bad = |what: &str| ProcessError::Line(*number, what.into());
        let fields: Vec<&str> = value.split_whitespace().collect();
        let [group, layer] = fields.as_slice() else {
            return Err(bad("range = GROUP LAYER"));
        };
        let group = u128::from_str_radix(group.trim_start_matches("0x"), 16)
            .map_err(|_| bad("a range's group, in hex"))?;
        let layer = match *layer {
            "bucket" => Layer::Bucket,
            "name" => Layer::Name,
            "file" => Layer::File,
            "block" => Layer::Block,
            "root" => Layer::Root,
            _ => return Err(bad("a layer: root, bucket, name, file or block")),
        };
        if ranges.iter().any(|(g, _)| *g == group) {
            return Err(bad("a group another range has"));
        }
        if ranges.iter().any(|(_, l)| *l == layer) {
            return Err(bad(
                "a second range of a layer, whose first spans every key",
            ));
        }
        ranges.push((group, layer));
    }
    Ok(ranges)
}

/// What a node with `root_key` lines states of its gateway host, each `root_key = GENERATION FILE`
/// a generation of the root key, the last the current one; none for a node without them.
fn gateway(lines: &Lines) -> Result<Option<GatewayConfig>, ProcessError> {
    let keys = root_keys(lines)?;
    if keys.is_empty() {
        return Ok(None);
    }
    let mut bursts = Vec::with_capacity(lines.all("durability.burst").len());
    for (number, value) in lines.all("durability.burst") {
        let bad = || {
            ProcessError::Line(
                *number,
                "durability.burst = RATE FRACTION: events an hour, and the share of the nodes \
                 each destroys"
                    .into(),
            )
        };
        let fields: Vec<&str> = value.split_whitespace().collect();
        let [rate, fraction] = fields.as_slice() else {
            return Err(bad());
        };
        let rate: f64 = rate.parse().map_err(|_| bad())?;
        let fraction: f64 = fraction.parse().map_err(|_| bad())?;
        if !(rate.is_finite() && rate >= 0.0 && (0.0..=1.0).contains(&fraction)) {
            return Err(bad());
        }
        bursts.push(Burst { rate, fraction });
    }
    Ok(Some(GatewayConfig {
        keys,
        handover_ns: lines.get("gateway.handover_ns")?,
        window: lines.get("gateway.window")?,
        objects: lines.get("gateway.objects")?,
        clients: lines.get("gateway.clients")?,
        bucket_quota: lines.get("gateway.bucket_quota")?,
        scan_rows: lines.get("gateway.scan_rows")?,
        chunk_rate: lines.get("durability.chunk_rate")?,
        repair_rate: lines.get("durability.repair_rate")?,
        bursts,
        target: lines.get("durability.target")?,
    }))
}

/// The gateway host's settings, its root keys read from their files: each 32 bytes that only its
/// owner may read, as `mantle init` makes them (encryption.md §2).
fn gateway_settings(config: GatewayConfig, keys: Vec<(u32, WrappingKey)>) -> GatewaySettings {
    GatewaySettings {
        keys,
        handover_ns: config.handover_ns,
        window: config.window,
        objects: config.objects,
        clients: config.clients,
        bucket_quota: config.bucket_quota,
        scan_rows: config.scan_rows,
        rates: Rates {
            chunk: config.chunk_rate,
            domains: None,
            bursts: config.bursts,
            repair: config.repair_rate,
        },
        target: config.target,
    }
}

/// How a node with `volume` lines enlists them: the root range its `range` lines name, its
/// `domain`, and the table's `root.` bounds, each required of it; none for a node without volumes.
fn enlisting(lines: &Lines) -> Result<Option<Enlisting>, ProcessError> {
    if lines.all("volume").is_empty() {
        return Ok(None);
    }
    let root = ranges(lines)?
        .into_iter()
        .find(|(_, layer)| *layer == Layer::Root)
        .map(|(group, _)| group)
        .ok_or(ProcessError::Missing("range = GROUP root, for the volumes"))?;
    let domain = lines.text("domain")?.to_owned();
    if domain.is_empty() || domain.len() > MAX_DOMAIN {
        return Err(ProcessError::Value(
            "domain",
            format!("{} bytes, where 1 to {MAX_DOMAIN} are", domain.len()),
        ));
    }
    Ok(Some(Enlisting {
        root,
        domain,
        max_nodes: lines.get("root.max_nodes")?,
        max_volumes: lines.get("root.max_volumes")?,
    }))
}

/// The volumes the `volume` lines state, each `volume = DIR SIZE DEPTH READ_BYTES READ_GAP
/// PREWRITE`: its device directory, its bytes, and what its device measured (mantle disk probe
/// --measure): the depth its reads and its issuer keep in flight, the bytes reads buffer at once,
/// the payload a read goes past rather than ask twice, and whether it is written once at layout
/// (1) or not (0). The layout every volume shares is stated once, in the `volume.` keys.
fn volumes(lines: &Lines) -> Result<Vec<VolumeSettings>, ProcessError> {
    if lines.all("volume").is_empty() {
        return Ok(Vec::new());
    }
    let scrub_s: u64 = lines.get("volume.scrub_period_s")?;
    let shared = mantle_chunk::Config {
        segment_size: lines.get("volume.segment_bytes")?,
        checksum_shift: lines.get("volume.checksum_shift")?,
        max_fragments: lines.get("volume.max_fragments")?,
        compact: lines.get::<u8>("volume.compact")? == 1,
        scrub_period: (scrub_s > 0).then(|| Duration::from_secs(scrub_s)),
        limits: mantle_chunk::Limits {
            batch_requests: lines.get("volume.batch_requests")?,
            batch_bytes: lines.get("volume.batch_bytes")?,
            fragments_per_chunk: lines.get("volume.fragments_per_chunk")?,
        },
        prewrite: false,
        reads: mantle_chunk::Reads::default(),
    };
    let mut volumes = Vec::with_capacity(lines.all("volume").len());
    for (number, value) in lines.all("volume") {
        let bad = || {
            ProcessError::Line(
                *number,
                "volume = DIR SIZE DEPTH READ_BYTES READ_GAP PREWRITE".into(),
            )
        };
        let fields: Vec<&str> = value.split_whitespace().collect();
        let [dir, size, depth, bytes, gap, prewrite] = fields.as_slice() else {
            return Err(bad());
        };
        let depth: usize = depth.parse().map_err(|_| bad())?;
        let prewrite = match *prewrite {
            "0" => false,
            "1" => true,
            _ => return Err(bad()),
        };
        volumes.push(VolumeSettings {
            dir: lines.path(dir),
            size: size.parse().map_err(|_| bad())?,
            config: mantle_chunk::Config {
                prewrite,
                reads: mantle_chunk::Reads::measured(
                    depth,
                    bytes.parse().map_err(|_| bad())?,
                    gap.parse().map_err(|_| bad())?,
                ),
                ..shared
            },
            depth,
        });
    }
    Ok(volumes)
}

/// Runs the node the file at `config` states until it stops in order or standard input closes.
pub fn run(config: &Path) -> Result<(), ProcessError> {
    // Before any thread allocates: a freed large block's memory goes back to the OS, which a range's
    // budget following the machine reads as room (docs/research/65 §6).
    mantle_engine::port::mmap::return_freed_blocks();
    let config = ProcessConfig::read(config).map_err(step(Step::Configuration))?;
    let keys = config
        .gateway
        .as_ref()
        .map_or(&[][..], |g| g.keys.as_slice());
    let identity = identity(
        (&config.certificate, &config.key, &config.authority),
        &config.peers,
        keys,
    )
    .map_err(step(Step::Identity))?;
    let node = Locks::node(&config.dir).map_err(step(Step::Locks))?;
    let locks =
        Locks::take(config.volumes.iter().map(|v| v.dir.as_path())).map_err(step(Step::Locks))?;
    start(config, (node, locks), identity, None, true)
}

/// Runs the node whose operator's statements the file at `config` holds (`mantle serve`, node.md
/// §1.4): each volume's device directory locked, then each device's profile kept or measured, the
/// node directory's device and the host's memory, cores and page measured; every other value
/// derived from those (`derive::derive`) and said on standard output, a `basis` line each, before
/// anything opens; then the node, as `mantle node` runs it, until it stops in order.
pub fn serve_stated(config: &Path) -> Result<(), ProcessError> {
    mantle_engine::port::mmap::return_freed_blocks();
    let stated = read_stated(config).map_err(step(Step::Configuration))?;
    let identity = identity(
        (&stated.certificate, &stated.key, &stated.authority),
        &stated.peers,
        &stated.keys,
    )
    .map_err(step(Step::Identity))?;
    let s3 = stated
        .s3
        .as_ref()
        .map(s3_keys)
        .transpose()
        .map_err(step(Step::Identity))?;
    let node = Locks::node(&stated.dir).map_err(step(Step::Locks))?;
    let locks = Locks::take(stated.volumes.iter().map(|(dir, _)| dir.as_path()))
        .map_err(step(Step::Locks))?;
    let measured = measure(&stated).map_err(step(Step::Devices))?;
    let dir = stated.dir.clone();
    let align = measured.meta.align;
    let mut stores = |group: u128, page: usize| -> Result<Option<Shape>, String> {
        let path = store_path(&dir, group);
        if !path.try_exists().map_err(|e| e.to_string())? {
            return Ok(None);
        }
        let file = DeviceFile::open(&path, false, CachingRequest::Buffered, align)
            .map_err(|e| e.to_string())?;
        Store::shape(&file, page)
            .map(Some)
            .map_err(|e| e.to_string())
    };
    let derived =
        crate::derive::derive(stated, &measured, &mut stores).map_err(step(Step::Budgets))?;
    let mut out = std::io::stdout().lock();
    for basis in &derived.bases {
        writeln!(out, "basis {basis}").map_err(|e| ProcessError::Say(e.to_string()))?;
    }
    // The S3 endpoint as its keys were read: where it listens, its region and its keys' ids.
    if let Some(s3) = &s3 {
        let ids: Vec<&str> = s3.access.iter().map(|k| k.id.as_str()).collect();
        writeln!(out, "s3 {} {} {}", s3.listen, s3.region, ids.join(","))
            .map_err(|e| ProcessError::Say(e.to_string()))?;
    }
    out.flush().map_err(|e| ProcessError::Say(e.to_string()))?;
    // The runtime's writer says the rest: this thread holds no lock on standard output for it.
    drop(out);
    start(derived.config, (node, locks), identity, s3, false)
}

/// How the node's thread ended, as [`start`] hears it.
enum Ended {
    /// The node ran to its end, in order or failing, its teardown done: its result.
    Ran(Result<(), ProcessError>),
    /// A stop that ends the process at once, as a crash would: a second request to stop, or the
    /// stop's bound passing (node.md §1.7), told before the node's teardown begins.
    AtOnce(&'static str),
}

/// The node of `config`, its directory and its volumes' devices under `held`'s locks, as
/// `identity` says it is, with the S3 listener its `s3` keys state: the start's steps after the
/// locks (node.md §1.7), each failure naming its step, then the node until it stops in order at a
/// signal, or, if it `ends_with_input`, until standard input closes. The directory's lock is held
/// until the node ends.
///
/// The node runs on a thread of its own, and this one waits for it: told of a stop that ends the
/// process at once, it returns without waiting for the node's teardown, and the process ends as a
/// crash would, its logs and volumes finishing no device work they began, every write they
/// acknowledged already durable. Run on this thread, the node finished its in-flight writes and
/// flushes as it was dropped, one device's wait after another: a second request to stop took more
/// than a second on a loaded runner (CI's `a_second_request_to_stop_ends_the_process_at_once`).
fn start(
    config: ProcessConfig,
    held: (NodeLock, Locks),
    identity: Identity,
    s3: Option<S3Keys>,
    ends_with_input: bool,
) -> Result<(), ProcessError> {
    // Room for both: a stop at once, and the node's end after its teardown, which no one hears.
    let config_id = config.id;
    let (tell, heard) = sync_channel::<Ended>(2);
    let at_once = tell.clone();
    let thread = std::thread::Builder::new()
        .name("node".into())
        .spawn(move || {
            let ran = run_node(config, held, identity, (s3, ends_with_input), at_once);
            let _ = tell.send(Ended::Ran(ran));
        })
        .map_err(|e| ProcessError::Runtime(format!("the node's thread: {e}")))?;
    let diag_id = config_id;
    match heard.recv() {
        Ok(Ended::Ran(ran)) => {
            crate::diag::say(diag_id, &format!("heard-ran ok={}", ran.is_ok()));
            thread
                .join()
                .map_err(|_| ProcessError::Runtime("the node's thread unwound".into()))?;
            crate::diag::say(diag_id, "joined");
            ran
        }
        Ok(Ended::AtOnce(why)) => {
            crate::diag::say(diag_id, &format!("heard-at-once {why}"));
            Err(ProcessError::Stopped(why))
        }
        Err(_) => Err(ProcessError::Runtime(
            "the node's thread ended without saying how".into(),
        )),
    }
}

/// [`start`]'s node, on the node's own thread: `at_once` hears a stop that ends the process at
/// once before the node's teardown begins.
fn run_node(
    mut config: ProcessConfig,
    held: (NodeLock, Locks),
    identity: Identity,
    (s3, ends_with_input): (Option<S3Keys>, bool),
    at_once: SyncSender<Ended>,
) -> Result<(), ProcessError> {
    let (node, locks) = held;
    // The volumes open before the node's runtime starts: an issuer's start and a volume's
    // recovery wait on their device, which no shard may (node.md §1.3).
    let storage = Storage::open_locked(&config.volumes, locks).map_err(step(Step::Storage))?;
    let Identity {
        credentials,
        peers,
        keys,
    } = identity;
    let gateway = config.gateway.take().map(|g| gateway_settings(g, keys));
    let addresses = resolve(&config).map_err(step(Step::Network))?;
    let runtime = runtime(&config, s3.is_some()).map_err(step(Step::Network))?;
    let mut rt = LocalRuntime::new(&runtime)
        .map_err(|e| step(Step::Network)(ProcessError::Runtime(format!("{e:?}"))))?;
    let ran = rt
        .block_on(serve(
            config,
            storage,
            gateway,
            (credentials, peers),
            (addresses, s3),
            (ends_with_input, at_once),
        ))
        .map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
    drop(node);
    ran
}

/// The S3 listener `keys` state, its door open on `node`: where it listens, its region, keys and
/// TLS (node.md §4.1). It binds the address the keys state over TCP, a stated port that port, as an
/// https authority serves TCP and QUIC on one (research/71); a stated port of 0 takes a TCP port
/// of its own, the UDP port QUIC was given saying nothing of the TCP port of the same number.
/// Plain HTTP on a loopback address, TLS with the endpoint's certificate elsewhere. Its door holds
/// a request for each connection the listener admits, so it refuses none for room: each waits
/// there until the host has room for it (node.md §4.5). A node that hosts no objects has no
/// operation to hand it.
fn listen<E: Engine + 'static>(
    node: &mut Node<E>,
    keys: S3Keys,
    (id, hosts): (u64, bool),
) -> Result<crate::listener::Listener, ProcessError> {
    let refused = |e: String| ProcessError::Listener(e);
    if !hosts {
        return Err(refused(
            "an S3 endpoint on a node that hosts no objects".into(),
        ));
    }
    let waiting = usize::try_from(crate::listener::CONNECTIONS)
        .map_err(|_| refused("the listener's connections".into()))?;
    let listen = keys.listen;
    let tls = if listen.ip().is_loopback() {
        None
    } else {
        Some((keys.certificate, keys.key))
    };
    let door = node.open_door(waiting)?;
    crate::listener::Listener::bind(
        crate::listener::Settings {
            listen,
            region: keys.region,
            domains: keys.names,
            access: keys.access,
            tls,
            node: id,
        },
        door,
    )
    .map_err(|e| refused(e.to_string()))
}

/// What the file at `path` states for `mantle serve`: only [`STATED_KEYS`] and
/// [`STATED_REPEATED`], each volume `volume = DIR SIZE`.
pub fn read_stated(path: &Path) -> Result<Stated, ProcessError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| ProcessError::Read(path.to_path_buf(), e.to_string()))?;
    let lines = Lines::parse(&text, STATED_KEYS, STATED_REPEATED)?.of_file(path);
    let mut volumes = Vec::with_capacity(lines.all("volume").len());
    for (number, value) in lines.all("volume") {
        let bad = || ProcessError::Line(*number, "volume = DIR SIZE".into());
        // The size after the last space, so a directory with a space in it reads.
        let (dir, size) = value.rsplit_once(char::is_whitespace).ok_or_else(bad)?;
        let dir = dir.trim();
        if dir.is_empty() {
            return Err(bad());
        }
        volumes.push((lines.path(dir), size.parse().map_err(|_| bad())?));
    }
    let target = lines.maybe::<f64>("durability.target")?;
    if target.is_some_and(|t| !(t.is_finite() && t > 0.0 && t < 1.0)) {
        return Err(ProcessError::Value(
            "durability.target",
            "a probability between 0 and 1".into(),
        ));
    }
    Ok(Stated {
        id: lines.get("id")?,
        dir: lines.path(lines.text("dir")?),
        quic: lines.get("quic")?,
        plane: lines.get("plane")?,
        authority: lines.path(lines.text("authority")?),
        certificate: lines.path(lines.text("certificate")?),
        key: lines.path(lines.text("key")?),
        peers: peers(&lines)?,
        ranges: ranges(&lines)?,
        voters: voters(&lines)?,
        volumes,
        enlist: enlisting(&lines)?,
        keys: root_keys(&lines)?,
        ceiling: lines.maybe("memory")?,
        stop: lines.maybe("stop_ms")?.map(Duration::from_millis),
        target,
        s3: s3_stated(&lines)?,
    })
}

/// The shortest and longest access key ids IAM takes, word characters each (IAM API Reference,
/// AccessKey: "Minimum length of 16. Maximum length of 128. Pattern: [\w]+").
const ACCESS_KEY_ID_LEAST: usize = 16;
const ACCESS_KEY_ID_MOST: usize = 128;

/// Whether `id` is an access key's id IAM takes: 16 to 128 ASCII word characters.
fn access_key_id(id: &str) -> bool {
    (ACCESS_KEY_ID_LEAST..=ACCESS_KEY_ID_MOST).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The S3 endpoint the file states (node.md §4): none where it states none of it, and where it
/// states any of it, all of it: `s3`, `s3.region`, `s3.certificate`, `s3.key`, and one
/// `access_key = ID FILE` line at least, with an `s3.name = NAME` line for each DNS name its
/// clients reach it by. A region is lowercase letters, digits and hyphens, S3's region codes'
/// alphabet, and holds no `/`, which SigV4's credential scope separates by; a name is a DNS name a
/// certificate may hold (`mantle_cert::check_dns_name`).
fn s3_stated(lines: &Lines) -> Result<Option<S3Stated>, ProcessError> {
    let keys = ["s3", "s3.region", "s3.certificate", "s3.key"];
    if !keys.iter().any(|key| lines.values.contains_key(*key))
        && lines.all("access_key").is_empty()
        && lines.all("s3.name").is_empty()
    {
        return Ok(None);
    }
    let region = lines.text("s3.region")?;
    if region.is_empty()
        || !region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(ProcessError::Value(
            "s3.region",
            "lowercase letters, digits and hyphens".into(),
        ));
    }
    let mut access = Vec::with_capacity(lines.all("access_key").len());
    for (number, value) in lines.all("access_key") {
        let bad = || ProcessError::Line(*number, "access_key = ID FILE".into());
        let (id, file) = value.split_once(char::is_whitespace).ok_or_else(bad)?;
        let file = file.trim();
        if !access_key_id(id) || file.is_empty() {
            return Err(bad());
        }
        access.push((id.to_owned(), lines.path(file)));
    }
    if access.is_empty() {
        return Err(ProcessError::Missing("access_key"));
    }
    let mut names = Vec::with_capacity(lines.all("s3.name").len());
    for (number, name) in lines.all("s3.name") {
        let name = name.trim().to_ascii_lowercase();
        mantle_cert::check_dns_name(&name)
            .map_err(|e| ProcessError::Line(*number, format!("s3.name = NAME: {e}")))?;
        names.push(name);
    }
    Ok(Some(S3Stated {
        listen: lines.get("s3")?,
        region: region.to_owned(),
        names,
        certificate: lines.path(lines.text("s3.certificate")?),
        key: lines.path(lines.text("s3.key")?),
        access,
    }))
}

/// The S3 endpoint's keys `s3` names, read at the start's identity step (node.md §1.7): its
/// certificate; its key; and each access key, from a file holding `account = ACCOUNT` and
/// `secret = SECRET`, its account twelve digits (`mantle_s3::account::valid_id`). Every key is read
/// only from a file no one but its owner may read, as the node's own and the root key's are.
pub fn s3_keys(s3: &S3Stated) -> Result<S3Keys, ProcessError> {
    let mut access = Vec::with_capacity(s3.access.len());
    for (id, path) in &s3.access {
        let bad = |why: &str| ProcessError::Key(path.clone(), why.to_owned());
        let text = String::from_utf8(read_secret(path)?).map_err(|_| bad("is not UTF-8"))?;
        let (mut account, mut secret) = (None, None);
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| bad("holds a line that is not key = value"))?;
            let slot = match key.trim() {
                "account" => &mut account,
                "secret" => &mut secret,
                _ => return Err(bad("states a key other than account and secret")),
            };
            if slot.replace(value.trim().to_owned()).is_some() {
                return Err(bad("states a key twice"));
            }
        }
        let account = account
            .filter(|a| mantle_s3::account::valid_id(a))
            .ok_or_else(|| bad("states no account of twelve digits"))?;
        let secret = secret
            .filter(|s| !s.is_empty() && !s.contains(char::is_whitespace))
            .ok_or_else(|| bad("states no secret"))?;
        access.push(AccessKey {
            id: id.clone(),
            account,
            secret,
        });
    }
    Ok(S3Keys {
        listen: s3.listen,
        region: s3.region.clone(),
        names: s3.names.clone(),
        certificate: read_file(&s3.certificate)?,
        key: read_secret(&s3.key)?,
        access,
    })
}

/// What `mantle serve` measures (node.md §1.7 step 2), each volume's device directory already
/// locked: the host's memory, cores and page; the node directory's device; and each volume's
/// device, its profile kept or measured now (`mantle_disk::profile`), and its volume's layout once
/// laid out (`mantle_chunk::formatted`).
fn measure(stated: &Stated) -> Result<Measured, ProcessError> {
    let facts = hyper_rt::machine::facts::Facts::query();
    std::fs::create_dir_all(&stated.dir)
        .map_err(|e| ProcessError::Device(stated.dir.clone(), e.to_string()))?;
    let meta = device(&mantle_disk::probe::identify(&stated.dir));
    let mut volumes = Vec::with_capacity(stated.volumes.len());
    for (dir, _) in &stated.volumes {
        let failed = |e: &dyn std::fmt::Display| ProcessError::Device(dir.clone(), e.to_string());
        let identity = mantle_disk::probe::identify(dir);
        let align = identity.transfer_alignment();
        let mantle = crate::storage::mantle_dir(dir);
        let plan = mantle_disk::calibrate::Plan::standard(align, identity.queue_depth);
        let (profile, measured_now) =
            mantle_disk::profile::load_or_measure(dir, &mantle, &identity, &plan)
                .map_err(|e| failed(&e))?;
        let path = crate::storage::volume_path(dir);
        let formatted = if path.try_exists().map_err(|e| failed(&e))? {
            let file = DeviceFile::open(&path, false, CachingRequest::PreferDirect, align)
                .map_err(|e| failed(&e))?;
            let (geometry, checksum_shift) =
                mantle_chunk::formatted(&file).map_err(|e| failed(&e))?;
            Some(Formatted {
                geometry,
                checksum_shift,
            })
        } else {
            None
        };
        volumes.push(VolumeFacts {
            device: device(&identity),
            profile,
            measured_now,
            formatted,
        });
    }
    Ok(Measured {
        memory: facts.memory,
        cores: facts.cores.len(),
        page: facts.page.base,
        meta,
        volumes,
    })
}

/// A device as `identity` describes it: its file system's bytes, or those available to the
/// process where the OS gives no total.
fn device(identity: &mantle_disk::identity::Identity) -> Device {
    Device {
        name: identity.device.clone(),
        align: identity.transfer_alignment(),
        queue: identity.queue_depth,
        bytes: identity
            .file_system
            .total_bytes
            .or(identity.file_system.available_bytes),
    }
}

/// Range `group`'s engine store in the node's directory `dir`.
fn store_path(dir: &Path, group: u128) -> PathBuf {
    dir.join(format!("range-{group:032x}.store"))
}

/// The machine's calibration record's file in the node's directory (node.md §1.6).
const CALIBRATION: &str = "calibration";

/// The longest calibration record: hyper-rt's layout (`hyper_rt::machine::record`) of 137 bytes
/// beside its key, at most `MAX_KEY` long: magic 8, version 2, key length 2, measured at 8, the
/// wake's six u64s, three u32s and four bytes, the system call's five u64s, two u32s and a byte,
/// and its CRC-32C 4.
const CALIBRATION_BYTES: usize = 137 + hyper_rt::machine::record::MAX_KEY;

/// This machine's calibration (hyper-rt `Calibration::load_or_measure`): the record kept in the node
/// directory `dir` while the machine's key still matches it (its identity line and power source,
/// whatever its age: node.md §1.4's `runtime.probe_budget_ms`), else measured now, each probe within
/// `budget`, and kept in its place. A record that does not read is measured again and replaced.
pub fn calibration(dir: &Path, budget: Duration) -> Result<Calibration, ProcessError> {
    std::fs::create_dir_all(dir)
        .map_err(|e| ProcessError::Device(dir.to_path_buf(), e.to_string()))?;
    let path = dir.join(CALIBRATION);
    let record = hyper_block::record::read(&path, CALIBRATION_BYTES)
        .ok()
        .flatten();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| ProcessError::Runtime(e.to_string()))?
        .as_secs();
    let (calibration, fresh) =
        Calibration::load_or_measure(record.as_deref(), now, u64::MAX, budget, 0)
            .map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
    if let Some(fresh) = fresh {
        hyper_block::record::write(&path, &fresh)
            .map_err(|e| ProcessError::Device(path.clone(), e.to_string()))?;
    }
    Ok(calibration)
}

/// One shard, calibrated on this machine (hyper-rt `RuntimeConfig::from_calibration`), sized by
/// what the node holds: its one task, its wait and its driver's QUIC timer, and a socket for its
/// endpoint, each connection and its plane, each read and written; with an S3 listener, a task for
/// each connection it holds at once, and a socket for the listener and each of those connections.
fn runtime(config: &ProcessConfig, s3: bool) -> Result<RuntimeConfig, ProcessError> {
    let calibration = calibration(&config.dir, config.probe_budget)?;
    let constants = calibration
        .constants(&Policy {
            reserved_cores: 0,
            lateness_tolerance_ns: None,
            latency_objective_ns: None,
        })
        .map_err(|unmet| ProcessError::Runtime(format!("{unmet:?}")))?;
    let connections = if s3 {
        usize::try_from(crate::listener::CONNECTIONS)
            .map_err(|_| ProcessError::Runtime("the S3 listener's connections".into()))?
    } else {
        0
    };
    let tasks = connections
        .checked_add(1)
        .ok_or_else(|| ProcessError::Runtime("the node's tasks".into()))?;
    let mut runtime = RuntimeConfig::from_calibration(&calibration, &constants, tasks, 2);
    runtime.shards = 1;
    runtime.pin = false;
    runtime.cores.clear();
    let listening = if s3 { connections.saturating_add(1) } else { 0 };
    let sockets = config
        .peers
        .len()
        .saturating_add(2)
        .saturating_add(listening);
    runtime.interests_per_shard = runtime.interests_per_shard.max(sockets.saturating_mul(2));
    Ok(runtime)
}

fn read_file(path: &Path) -> Result<Vec<u8>, ProcessError> {
    std::fs::read(path).map_err(|e| ProcessError::Read(path.to_path_buf(), e.to_string()))
}

/// Range `group`'s engine in the node's directory, holding `layer`'s rows: opened at its last
/// checkpoint when its store is there, created when not, a Name range founded with its first rows
/// (its descriptor, spanning every key).
fn engine(
    config: &ProcessConfig,
    group: u128,
    layer: Layer,
) -> Result<Stored<DeviceFile>, ProcessError> {
    let path = store_path(&config.dir, group);
    let fresh = !path.exists();
    // The file transfers at its device's alignment, which the engine's pages are a multiple of
    // (`Store::check`): a page derived to hold a row is a multiple of the device's unit, not a
    // power of two (`mantle_engine::branch::least_page`).
    let align = mantle_disk::probe::identify(&config.dir).transfer_alignment();
    let file = DeviceFile::open(&path, fresh, CachingRequest::Buffered, align)
        .map_err(|e| ProcessError::Engine(e.to_string()))?;
    let engine = |e: mantle_engine::Error| ProcessError::Engine(e.to_string());
    if !fresh {
        return Stored::open(file, config.store, config.memtable_bytes, config.trunk)
            .map_err(engine);
    }
    let mut stored =
        Stored::create(file, config.store, config.memtable_bytes, config.trunk).map_err(engine)?;
    let founded = |e: mantle_meta::engine::EngineError| ProcessError::Engine(e.to_string());
    if layer == Layer::Name {
        let first = name::first(1).map_err(|e| ProcessError::Engine(e.to_string()))?;
        stored.install(0, first).map_err(founded)?;
    }
    stored.persist().map_err(founded)?;
    Ok(stored)
}

/// The node, its talk on standard input and output, and its S3 listener where `s3` states one.
async fn serve(
    config: ProcessConfig,
    storage: Storage,
    gateway: Option<GatewaySettings>,
    (credentials, peers): (Credentials, Vec<Peer>),
    (addresses, s3): (Vec<(u64, SocketAddr, SocketAddr)>, Option<S3Keys>),
    (ends_with_input, at_once): (bool, SyncSender<Ended>),
) -> Result<(), ProcessError> {
    // This process's one shard runs the node's replicas, which apply through the engine's
    // synchronous calls (node.md §1.2).
    mantle_engine::store::permit_blocking();
    let volumes = storage.enlisting();
    // The node's history (node.md §2.4, item 7): its last run and last orderly stop as its
    // directory keeps them, read before the node raises its run, and what the root range's
    // replica here holds of every node's.
    let layout = Layout::new(&config.dir);
    let kept = |e: LayoutError| step(Step::Storage)(ProcessError::History(e.to_string()));
    let (last_run, last_stop) = (layout.run().map_err(kept)?, layout.stopped().map_err(kept)?);
    let mut held = (History::default(), History::default());
    let mut ranges = Vec::with_capacity(config.ranges.len());
    for (group, layer) in &config.ranges {
        let mut engine = engine(&config, *group, *layer).map_err(step(Step::Storage))?;
        if *layer == Layer::Root {
            held = cell_history(&mut engine, config.id).map_err(step(Step::Storage))?;
        }
        ranges.push(Hosted {
            group: *group,
            range: Range {
                layer: *layer,
                rules: config.rules,
                boot: ConfState {
                    voters: config.voters.clone(),
                    ..ConfState::default()
                },
                settings: config.settings,
            },
            engine,
        });
    }
    let (cell, row) = held;
    // Every start reads a stop record: one the directory has not kept yet is kept now, before the
    // run is raised, naming the runs its counts begin from.
    let last_stop = match last_stop {
        Some(stopped) => stopped,
        None => {
            let first = history::first_stop(last_run, row);
            layout.keep_stopped(first).map_err(kept)?;
            first
        }
    };
    let own = history::at_start(row, last_run, Some(last_stop));
    let history = history::cell_with(cell, row, own);
    // Whether the node hosts objects, which an S3 listener hands its operations to.
    let hosts = gateway.is_some();
    let settings = NodeSettings {
        id: config.id,
        dir: config.dir.clone(),
        quic: config.quic,
        plane: config.plane,
        credentials,
        peers,
        addresses,
        ranges,
        log: config.log,
        transport_bytes: config.transport_bytes,
        seed: None,
        storage,
        chunk_bytes: config.chunk_bytes,
        chunk_asks: config.chunk_asks,
        command_asks: config.command_asks,
        read_asks: config.read_asks,
        wall: Wall::System,
        gateway,
        history,
    };
    let mut node = Node::open(settings).await.map_err(opened)?;
    let run = layout
        .run()
        .map_err(kept)?
        .ok_or_else(|| step(Step::Storage)(ProcessError::History("no run kept".into())))?;
    let started_ns = hyper_rt::futures::now_ns();
    // A buffer each way, a page each (hyper-rt `stdio`: a page is the natural chunk): the talk is
    // a line now and then, each far shorter.
    let talk = |e: String| step(Step::Network)(ProcessError::Runtime(e));
    let chunk = usize::try_from(hyper_rt::machine::facts::Facts::query().page.base)
        .map_err(|e| talk(e.to_string()))?;
    let mut input = hyper_rt::stdio::stdin(1, chunk).map_err(|e| talk(format!("{e:?}")))?;
    let mut output = hyper_rt::stdio::stdout(1, chunk).map_err(|e| talk(format!("{e:?}")))?;
    let (quic, plane) = node.addresses()?;
    say(&mut output, &format!("bound {quic} {plane}\n")).await?;
    let listener = match s3 {
        Some(keys) => {
            let listener =
                listen(&mut node, keys, (config.id, hosts)).map_err(step(Step::Network))?;
            let at = listener
                .local_addr()
                .map_err(|e| step(Step::Network)(ProcessError::Listener(e.to_string())))?;
            let line = format!("s3-bound {at} {}\n", crate::listener::CONNECTIONS);
            say(&mut output, &line).await?;
            Some(listener)
        }
        None => None,
    };
    let accepting = async {
        match &listener {
            Some(listener) => listener.serve().await,
            None => std::future::pending().await,
        }
    };
    let mut accepting = pin!(accepting);
    // What the node's failure detectors start from, and this node's part of it.
    let line = format!(
        "history {} {} {} {}\n",
        history.exposure_ns, history.crashes, own.exposure_ns, own.crashes
    );
    say(&mut output, &line).await?;
    let held: Vec<u128> = node.volumes().map(|(id, _)| id).collect();
    for volume in held {
        say(&mut output, &format!("volume {volume:032x}\n")).await?;
    }
    // Its volumes enlisted by one command, asked until the root range answers it: the range's
    // laws, not a deadline of the node's, decide when.
    // The node's statements of its history to the root range, once enlisted (node.md §2.4, item 7).
    let mut living = Living::default();
    let mut enlisting = match &config.enlist {
        Some(e) => {
            let enlist = Command::Root(root::Command::Enlist(Enlist {
                node: config.id,
                domain: e.domain.clone(),
                volumes,
                max_nodes: e.max_nodes,
                max_volumes: e.max_volumes,
            }));
            Some(node.command(e.root, enlist, u64::MAX)?)
        }
        None => None,
    };
    let mut read = vec![0u8; chunk];
    let mut pending = Vec::new();
    // Each range's status as last said, a range a slot in the configuration's order.
    let mut told: Vec<Option<Status>> = vec![None; config.ranges.len()];
    // The log's measures, filled again at each status said (hyper-log's `Log::stats`).
    let mut stats: Option<Box<LogStats>> = None;
    // Each peer's link as last said, a peer a slot.
    let mut links: Vec<Link> = Vec::with_capacity(config.peers.len());
    // The bytes its transfers moved as last said, and when, none before the first: the first
    // movement said at once, and the next once they move again, at most every half of the
    // transport's serve period, the longest an exchange it carries may go without moving before
    // the transport refuses it, so while a transfer lives the next is said within one serve period
    // of the last. (The runtime's clock starts at zero, so a first saying stood for one at zero
    // held back every movement of the runtime's first half period, a cell of one's first objects
    // among them, and a watcher saw none.)
    let serve = mantle_protocol::bounds::idle_timeout();
    let serve_ns = u64::try_from(serve.as_nanos()).unwrap_or(u64::MAX);
    let mut said_moved: Option<(u64, u64)> = None;
    // Whether the node waits on its own device, as last said, none before the first saying.
    let mut said_held: Option<bool> = None;
    let signals = hyper_rt::signal::subscribe(STOP_SIGNALS).map_err(|e| talk(format!("{e:?}")))?;
    let mut stop = Stop::new(config.stop);
    // Whether standard input may say more: a node that does not end with it runs on once it closes,
    // as `mantle serve` does under a service manager, whose input is empty.
    let mut input_open = true;
    // Scratch (diag/roll-stop-bound): the loop's iterations while stopping, and the slowest.
    let mut diag_turns: u64 = 0;
    let mut diag_slowest: (u64, u64, &'static str) = (0, 0, "");
    let mut diag_after: u64 = hyper_rt::futures::now_ns();
    let mut diag_progress: u64 = 0;
    loop {
        let diag_before = hyper_rt::futures::now_ns();
        let ended = {
            let turn = pin!(node.turn());
            let line = pin!(async {
                if input_open {
                    input.read(&mut read).await
                } else {
                    std::future::pending().await
                }
            });
            let signal = pin!(signals.recv());
            let bound = pin!(async {
                match stop.by {
                    Some(at) => {
                        let left = at.saturating_sub(hyper_rt::futures::now_ns());
                        hyper_rt::futures::sleep(left).await
                    }
                    None => std::future::pending().await,
                }
            });
            race3(turn, line, race3(signal, bound, accepting.as_mut())).await
        };
        if stop.by.is_some() {
            let woke = hyper_rt::futures::now_ns();
            let waited = woke.saturating_sub(diag_before) / 1_000_000;
            let between = if diag_turns == 0 {
                0
            } else {
                diag_before.saturating_sub(diag_after) / 1_000_000
            };
            let what = match &ended {
                Either3::First(_) => "turn",
                Either3::Second(_) => "line",
                Either3::Third(Either3::First(_)) => "signal",
                Either3::Third(Either3::Second(_)) => "bound",
                Either3::Third(Either3::Third(_)) => "listener",
            };
            diag_turns = diag_turns.saturating_add(1);
            if waited > diag_slowest.0 {
                diag_slowest = (waited, diag_turns, what);
            }
            if waited >= 250 || between >= 100 {
                crate::diag::say(
                    config.id,
                    &format!(
                        "slow-loop iteration={diag_turns} waited_ms={waited} by={what} \
                         body_before_ms={between}"
                    ),
                );
            }
            diag_after = woke;
            if woke.saturating_sub(diag_progress) >= 2_000_000_000 {
                if diag_progress != 0 {
                    let state = node.stop_diag();
                    crate::diag::say(config.id, &format!("progress {state}"));
                }
                diag_progress = woke;
            }
        }
        match ended {
            Either3::First(turned) => turned?,
            Either3::Second(Ok(0)) if ends_with_input => return Ok(()),
            Either3::Second(Ok(0)) => input_open = false,
            Either3::Second(Ok(n)) => {
                pending.extend_from_slice(read.get(..n).unwrap_or_default());
                while let Some(end) = pending.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = pending.drain(..=end).collect();
                    if line.trim_ascii() == b"stop" {
                        crate::diag::say(
                            config.id,
                            &format!("asked by=line pid={}", std::process::id()),
                        );
                        stop_asked(&mut stop, &mut node, (&mut output, &at_once)).await?;
                    } else if line.trim_ascii() == b"chunks" {
                        let (received, written, held, unkept) = node.chunk_bytes()?;
                        let (fragments, budget) = held.fragments;
                        let line = format!(
                            "chunks {received} {written} {} {unkept} {fragments} {budget} {}\n",
                            held.bytes, held.free
                        );
                        say(&mut output, &line).await?;
                    } else {
                        take_line(&mut node, &config.ranges, &line)?;
                    }
                }
                // A line is never longer than the buffer it is read into.
                if pending.len() > chunk {
                    let text = String::from_utf8_lossy(&pending).into_owned();
                    return Err(ProcessError::Input(text, "a line longer than a page"));
                }
            }
            Either3::Second(Err(e)) => return Err(ProcessError::Runtime(format!("{e:?}"))),
            Either3::Third(Either3::First(signal)) => {
                signal.map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
                crate::diag::say(
                    config.id,
                    &format!("asked by=signal pid={}", std::process::id()),
                );
                stop_asked(&mut stop, &mut node, (&mut output, &at_once)).await?;
            }
            Either3::Third(Either3::Second(slept)) => {
                slept.map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
                let why = "the stop's bound passed";
                let state = node.stop_diag();
                crate::diag::say(
                    config.id,
                    &format!(
                        "BOUND-PASSED loop_iterations={diag_turns} slowest_wait={diag_slowest:?} \
                         {state}"
                    ),
                );
                let _ = at_once.try_send(Ended::AtOnce(why));
                return Err(ProcessError::Stopped(why));
            }
            Either3::Third(Either3::Third(ended)) => {
                return Err(ProcessError::Listener(ended.to_string()));
            }
        }
        // The node keeps what its ranges answered as they applied for a caller that proposed to
        // them itself (`Node::take_applied`); this process proposes nothing itself, and a client's
        // answers reach it through its exchange, so they are dropped each turn, never kept.
        node.take_applied().for_each(drop);
        // Its own command's outcome only: the gateway host takes those of the commands it asks.
        if let Some(asked) = enlisting
            && let Some(outcome) = node.take_command(asked)
        {
            enlisting = None;
            match outcome {
                CommandOutcome::Answered(Answer::Root(root::Outcome::Enlisted)) => {
                    say(&mut output, "enlisted\n").await?;
                    if let Some(e) = &config.enlist {
                        living.state(&mut node, e.root, own, started_ns)?;
                    }
                }
                other => return Err(ProcessError::Enlist(format!("{other:?}"))),
            }
        }
        if let Some(e) = &config.enlist {
            living.take(&mut node, e.root, own, started_ns)?;
        }
        for (group, replaced) in node.take_replaced() {
            let line = format!(
                "replaced {group:x} {} {}\n",
                replaced.failed(),
                replaced.joining()
            );
            say(&mut output, &line).await?;
        }
        for (at, now) in node.links().enumerate() {
            if links.get(at) == Some(&now) {
                continue;
            }
            let refusal = now
                .refusal
                .map_or_else(|| "none".into(), |r| format!("{r:?}"));
            let line = format!(
                "link {} {} {} {} {} {refusal}\n",
                now.peer,
                if now.keyed { "keyed" } else { "unkeyed" },
                now.dials,
                now.ended,
                now.suspected
            );
            say(&mut output, &line).await?;
            match links.get_mut(at) {
                Some(was) => *was = now,
                None => links.push(now),
            }
        }
        let moved = node.moved();
        let now = hyper_rt::futures::now_ns();
        let due = match said_moved {
            None => moved > 0,
            Some((said, at)) => moved != said && now.saturating_sub(at) >= serve_ns / 2,
        };
        if due {
            say(&mut output, &format!("moved {moved} {serve_ns}\n")).await?;
            said_moved = Some((moved, now));
        }
        // Whether the node waits on its own device, said as it changes, so a watcher knows it holds
        // still for its device and not by choice: a fact of the node, said once, where a status
        // for each range repeated it, and a log line after, at each of the liveness stream's
        // flushes (12 lines a flush on a node of five ranges; 120 to 560 a second from each
        // node while a peer of a cell of three was stopped, on this Mac).
        let held = node.held();
        if said_held != Some(held) {
            say(&mut output, &format!("held {}\n", u8::from(held))).await?;
            said_held = Some(held);
        }
        let mut said = false;
        for ((group, _), was) in config.ranges.iter().zip(told.iter_mut()) {
            let Some(now) = node.status(*group) else {
                continue;
            };
            // While some pair is not yet judged, the node is measuring its links, which moves only
            // the heartbeats it takes from those pairs: said too, so a watcher sees it move before
            // it elects, and sees it stop if a peer it measures goes silent. The reconnect bound
            // is said each time it changes, as a redial's backoff doubles, so a watcher never
            // holds one shorter than the node's own.
            let moved = was.is_none_or(|was| {
                (was.term, was.leader, was.commit, was.applied, was.reconnect)
                    != (now.term, now.leader, now.commit, now.applied, now.reconnect)
                    || was.measuring != now.measuring
                    || was.asking != now.asking
            });
            if !moved {
                continue;
            }
            let patience = now.patience.map_or(0, |p| p.as_nanos());
            let reconnect = now.reconnect.map_or(0, |r| r.as_nanos());
            let measuring = now.measuring_for.map_or(0, |m| m.as_nanos());
            let asking = now.asking.map_or(0, |a| a.as_nanos());
            let line = format!(
                "status {group:x} {} {} {} {} {patience} {reconnect} {measuring} {asking}\n",
                now.term, now.leader, now.commit, now.applied
            );
            say(&mut output, &line).await?;
            *was = Some(now);
            said = true;
        }
        // The log the ranges share, once after the statuses that moved.
        if said {
            let log = node.log_stats(stats.take())?;
            let flushing = log.flushing_since.map_or(0, |since| {
                log.at.saturating_duration_since(since).as_nanos()
            });
            let longest = log.commit_wait.quantile(1_000_000).unwrap_or(0);
            say(&mut output, &format!("log {flushing} {longest}\n")).await?;
            stats = Some(log);
        }
        // The orderly stop: each range whose lead it begins to hand over, and to whom (again, after
        // its core gave a transfer up); each it handed over, to the member that then led it; and
        // its end, after which the node's log and volumes close as it is dropped.
        for (group, to) in node.take_handing() {
            say(&mut output, &format!("handing {group:x} {to}\n")).await?;
        }
        for (group, leader) in node.take_handed() {
            say(&mut output, &format!("handed {group:x} {leader}\n")).await?;
        }
        if node.stopped() {
            say(&mut output, "stopped\n").await?;
            crate::diag::say(
                config.id,
                &format!(
                    "stopped-said loop_iterations={diag_turns} slowest_wait={diag_slowest:?} \
                     left_ms={}",
                    stop.by
                        .unwrap_or(0)
                        .saturating_sub(hyper_rt::futures::now_ns())
                        / 1_000_000
                ),
            );
            // Nothing the node holds is owed to the cell: its log and volumes close, and then the
            // stop is kept, with the node's time run, so its next start counts no crash.
            let ran_ns = hyper_rt::futures::now_ns().saturating_sub(started_ns);
            let diag_id = config.id;
            let dropping = hyper_rt::futures::now_ns();
            drop(node);
            crate::diag::say(
                diag_id,
                &format!(
                    "node-dropped took_ms={}",
                    hyper_rt::futures::now_ns().saturating_sub(dropping) / 1_000_000
                ),
            );
            let stopped = Stopped {
                run,
                exposure_ns: own.exposure_ns.saturating_add(ran_ns),
                stops: last_stop.stops.saturating_add(1),
            };
            layout
                .keep_stopped(stopped)
                .map_err(|e| ProcessError::History(e.to_string()))?;
            crate::diag::say(diag_id, "stop-kept");
            return Ok(());
        }
    }
}

/// What the root range's replica holds of the cell's history, and of node `id`'s own: the sum over
/// every node the table holds, and its row (node.md §2.4, item 7).
fn cell_history<E: Engine>(engine: &mut E, id: u64) -> Result<(History, History), ProcessError> {
    let meta = |e: mantle_meta::error::MetaError| ProcessError::History(e.to_string());
    let nodes = root::census(engine).map_err(meta)?.nodes;
    let nodes = usize::try_from(nodes).map_err(|e| ProcessError::History(e.to_string()))?;
    Ok((
        root::history(engine, nodes).map_err(meta)?,
        root::history_of(engine, id).map_err(meta)?,
    ))
}

/// The node's statements of its history to the root range (node.md §2.4, item 7): its totals once
/// it has enlisted, and again each time its run's time has doubled since the last statement was
/// answered, so a crash loses at most half of a run's time and a run of `t` states about
/// `log2(t / first)` times, the first statement's own wait the unit; one statement out at a time.
#[derive(Default)]
struct Living {
    out: Option<u64>,
    /// When the next statement is due, in the run's time; none before the first is answered.
    next_ns: Option<u64>,
}

impl Living {
    /// States the node's totals now: what it began this run with, and the run's time so far.
    fn state<E: Engine + 'static>(
        &mut self,
        node: &mut Node<E>,
        root: u128,
        own: History,
        started_ns: u64,
    ) -> Result<(), ProcessError> {
        if self.out.is_some() {
            return Ok(());
        }
        let ran_ns = hyper_rt::futures::now_ns().saturating_sub(started_ns);
        let lived = Command::Root(root::Command::Lived(Lived {
            node: node.id(),
            exposure_ns: own.exposure_ns.saturating_add(ran_ns),
            crashes: own.crashes,
        }));
        self.out = Some(node.command(root, lived, u64::MAX)?);
        Ok(())
    }

    /// Takes the statement's answer, and states again once the run's time has doubled since.
    fn take<E: Engine + 'static>(
        &mut self,
        node: &mut Node<E>,
        root: u128,
        own: History,
        started_ns: u64,
    ) -> Result<(), ProcessError> {
        let ran_ns = hyper_rt::futures::now_ns().saturating_sub(started_ns);
        if let Some(asked) = self.out
            && let Some(outcome) = node.take_command(asked)
        {
            self.out = None;
            match outcome {
                CommandOutcome::Answered(Answer::Root(root::Outcome::Kept)) => {
                    self.next_ns = Some(ran_ns.saturating_mul(2));
                }
                other => return Err(ProcessError::History(format!("{other:?}"))),
            }
        }
        if self.next_ns.is_some_and(|next| ran_ns >= next) {
            self.state(node, root, own, started_ns)?;
        }
        Ok(())
    }
}

/// Takes a line of the node's input: `learn ID QUIC PLANE` or `replace FAILED JOINING`, a
/// replacement in each of `ranges`. (`stop`, the orderly stop a signal asks, and `chunks`, whose
/// answer the node says, the loop takes itself.)
fn take_line<E: Engine + 'static>(
    node: &mut Node<E>,
    ranges: &[(u128, Layer)],
    line: &[u8],
) -> Result<(), ProcessError> {
    let text = String::from_utf8_lossy(line).trim().to_owned();
    let mut fields = text.split_whitespace();
    let words = (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    );
    let id = |word: &str, what| {
        word.parse::<u64>()
            .map_err(|_| ProcessError::Input(text.clone(), what))
    };
    let address = |word: &str, what| {
        word.parse::<SocketAddr>()
            .map_err(|_| ProcessError::Input(text.clone(), what))
    };
    match words {
        (Some("learn"), Some(peer), Some(quic), Some(plane), None) => {
            let peer = id(peer, "a peer's id")?;
            let quic = address(quic, "a QUIC address")?;
            let plane = address(plane, "a plane address")?;
            node.learn(peer, quic, plane)?;
        }
        (Some("replace"), Some(failed), Some(joining), None, None) => {
            let failed = id(failed, "a member's id")?;
            let joining = id(joining, "a spare's id")?;
            for (group, _) in ranges {
                node.replace(*group, failed, joining)?;
            }
        }
        _ => {
            return Err(ProcessError::Input(
                text,
                "learn ID QUIC PLANE, or replace FAILED JOINING",
            ));
        }
    }
    Ok(())
}

/// A request to stop, by a signal or a line: the node's orderly stop begins, said; begun already,
/// the process ends at once.
/// A request to stop: the first begins the orderly stop, and one as it runs ends the process at
/// once, told to `at_once` before the node's teardown begins.
async fn stop_asked<E: Engine + 'static>(
    stop: &mut Stop,
    node: &mut Node<E>,
    (output, at_once): (&mut hyper_rt::stdio::Stdout, &SyncSender<Ended>),
) -> Result<(), ProcessError> {
    if let Err(why) = stop.ask(hyper_rt::futures::now_ns()) {
        let _ = at_once.try_send(Ended::AtOnce(why));
        return Err(ProcessError::Stopped(why));
    }
    node.stop();
    let said = say(output, "stopping\n").await;
    crate::diag::say(node.id(), "stopping-said");
    said
}

async fn say(output: &mut hyper_rt::stdio::Stdout, line: &str) -> Result<(), ProcessError> {
    output
        .write_all(line.as_bytes())
        .await
        .map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
    output
        .flush()
        .await
        .map_err(|e| ProcessError::Runtime(format!("{e:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_is_a_key_and_its_value_and_a_key_is_stated_once() {
        let parse = |text| Lines::parse(text, KEYS, REPEATED);
        let lines =
            parse("# a node\nid = 3\npeer = 2 node a.der\n\npeer = 9 client b.der # x\n").unwrap();
        assert_eq!(lines.get::<u64>("id").unwrap(), 3);
        assert_eq!(lines.all("peer").len(), 2);
        assert!(matches!(
            parse("id = 1\nid = 2\n"),
            Err(ProcessError::Line(2, _))
        ));
        assert!(matches!(parse("id 1\n"), Err(ProcessError::Line(1, _))));
        // A key the node does not know is refused at its line, a derived one in a stated-only
        // file too.
        assert!(matches!(
            parse("id = 1\nidd = 2\n"),
            Err(ProcessError::Line(2, _))
        ));
        assert!(matches!(
            Lines::parse(
                "id = 1\nengine.page_size = 4096\n",
                STATED_KEYS,
                STATED_REPEATED
            ),
            Err(ProcessError::Line(2, _))
        ));
        assert!(matches!(
            lines.get::<u64>("dir"),
            Err(ProcessError::Missing("dir"))
        ));
    }

    /// The first request to stop begins the stop, to end by its bound; a second ends the process at
    /// once.
    #[test]
    fn a_second_request_to_stop_ends_the_process_at_once() {
        let mut stop = Stop::new(Duration::from_secs(10));
        stop.ask(5).unwrap();
        assert_eq!(stop.by, Some(5 + 10_000_000_000));
        assert_eq!(
            stop.ask(6),
            Err("a second request to stop came as the node stopped")
        );
        assert_eq!(stop.by, Some(5 + 10_000_000_000));
        let mut unbounded = Stop::new(Duration::MAX);
        unbounded.ask(u64::MAX - 1).unwrap();
        assert_eq!(unbounded.by, Some(u64::MAX));
    }

    /// A peer line may name where the peer listens, by address or by name; a malformed endpoint,
    /// one of the two missing, a field past them, or a client, which no node pins, is refused at
    /// its line.
    #[test]
    fn a_peer_line_names_where_it_listens_by_address_or_name() {
        let lines = Lines::parse(
            "peer = 2 node a.der 10.0.0.2:7000 10.0.0.2:7001\n\
             peer = 3 node b.der node-3.example:7000 [::1]:7001\n\
             peer = 9 gateway c.der\n",
            KEYS,
            REPEATED,
        )
        .unwrap();
        let read = peers(&lines).unwrap();
        let said = |peer: &PeerLine| {
            peer.at
                .as_ref()
                .map(|(quic, plane)| (quic.to_string(), plane.to_string()))
        };
        assert_eq!(
            said(&read[0]),
            Some(("10.0.0.2:7000".into(), "10.0.0.2:7001".into()))
        );
        assert_eq!(
            said(&read[1]),
            Some(("node-3.example:7000".into(), "[::1]:7001".into()))
        );
        assert_eq!(read[2].at, None);
        assert_eq!(read[2].role, Role::Gateway);
        for bad in [
            "peer = 9 client c.der\n",
            "peer = 2 node a.der 10.0.0.2:7000\n",
            "peer = 2 node a.der host 10.0.0.2:7001\n",
            "peer = 2 node a.der ::1:7000 [::1]:7001\n",
            "peer = 2 node a.der h:70000 h:7001\n",
            "peer = 2 node a.der h:7000 h:7001 h:7002\n",
        ] {
            let lines = Lines::parse(bad, KEYS, REPEATED).unwrap();
            assert!(
                matches!(peers(&lines), Err(ProcessError::Line(1, _))),
                "{bad}"
            );
        }
    }

    /// A peer resolves to an address of the family the node binds, since a socket of one family
    /// sends to no other: `localhost` to the IPv4 loopback for an IPv4 node (RFC 6761 §6.3), and an
    /// address of the other family is refused naming the peer.
    #[test]
    fn a_peer_resolves_to_the_family_the_node_binds() {
        let at = |text: &str| Endpoint::parse(text).unwrap();
        let v4: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let v6: SocketAddr = "[::1]:7000".parse().unwrap();
        let address = |text: &str| text.parse::<SocketAddr>().unwrap();
        assert_eq!(
            lookup(2, &at("localhost:7100"), v4).unwrap(),
            address("127.0.0.1:7100")
        );
        assert_eq!(
            lookup(2, &at("[::1]:7100"), v6).unwrap(),
            address("[::1]:7100")
        );
        assert!(matches!(
            lookup(2, &at("[::1]:7100"), v4),
            Err(ProcessError::Resolve(2, _, _))
        ));
        assert!(matches!(
            lookup(3, &at("127.0.0.1:7100"), v6),
            Err(ProcessError::Resolve(3, _, _))
        ));
    }

    /// The machine's calibration is kept in the node's directory and taken again while the
    /// machine's key matches it; the bound on its record holds hyper-rt's longest.
    #[test]
    fn the_calibration_is_kept_in_the_node_directory_and_taken_again() {
        let dir = tempfile::tempdir().unwrap();
        let budget = Duration::from_millis(40);
        let measured = calibration(dir.path(), budget).unwrap();
        let record = hyper_block::record::read(&dir.path().join(CALIBRATION), CALIBRATION_BYTES)
            .unwrap()
            .unwrap();
        assert!(record.len() <= CALIBRATION_BYTES);
        // Taken again: the same measurements, the facts queried afresh.
        let again = calibration(dir.path(), budget).unwrap();
        assert_eq!(
            (again.wake, again.syscall),
            (measured.wake, measured.syscall)
        );
        // A record of the longest key is within the bound.
        let mut longest = measured.clone();
        longest.facts.identity.cpu = "c".repeat(hyper_rt::machine::record::MAX_KEY);
        if let Some(encoded) = longest.encode(1) {
            assert!(encoded.len() <= CALIBRATION_BYTES);
        }
    }

    /// A stated volume's directory may hold a space, its size after the last; a relative one is
    /// from the configuration's directory.
    #[test]
    fn a_stated_volumes_directory_may_hold_a_space() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config");
        // Absolute on this platform: on Windows a rooted path with no drive is the config's drive's.
        let disks = if cfg!(windows) {
            "C:/my disks/one"
        } else {
            "/my disks/one"
        };
        std::fs::write(
            &config,
            format!(
                "id = 1\ndir = .\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\nauthority = a.der\n\
                 certificate = n.der\nkey = n.key\nvoters = 1\nrange = 1 root\n\
                 volume = {disks} 1073741824\nvolume = data 4096\ndomain = a\n\
                 root.max_nodes = 1\nroot.max_volumes = 2\n"
            ),
        )
        .unwrap();
        let stated = read_stated(&config).unwrap();
        assert_eq!(
            stated.volumes,
            vec![
                (PathBuf::from(disks), 1 << 30),
                (dir.path().join("data"), 4096)
            ]
        );
        for bad in ["volume = 1024", "volume = /d", "volume = /d big"] {
            let text = std::fs::read_to_string(&config)
                .unwrap()
                .replace("volume = data 4096", bad);
            std::fs::write(&config, text).unwrap();
            assert!(
                matches!(read_stated(&config), Err(ProcessError::Line(..))),
                "{bad}"
            );
            std::fs::write(
                &config,
                std::fs::read_to_string(&config)
                    .unwrap()
                    .replace(bad, "volume = data 4096"),
            )
            .unwrap();
        }
    }

    #[test]
    fn ranges_are_a_group_and_a_layer_each_and_one_of_each_layer() {
        let ranges = |text: &str| super::ranges(&Lines::parse(text, KEYS, REPEATED).unwrap());
        assert_eq!(
            ranges("range = 0x72 name\nrange = 9 block\n").unwrap(),
            vec![(0x72, Layer::Name), (9, Layer::Block)]
        );
        assert!(matches!(
            ranges("id = 1\n"),
            Err(ProcessError::Missing("range"))
        ));
        // A layer no range holds, a group stated twice, a second range of a layer whose first
        // spans every key, and a line of the wrong shape: each refused at its line.
        for (text, line) in [
            ("range = 1 trunk\n", 1),
            ("range = 1 root\nrange = 2 root\n", 2),
            ("range = 1 name\nrange = 1 file\n", 2),
            ("range = 1 file\nrange = 2 file\n", 2),
            ("range = 1\n", 1),
            ("range = 1 name name\n", 1),
            ("range = zz name\n", 1),
        ] {
            assert!(
                matches!(ranges(text), Err(ProcessError::Line(n, _)) if n == line),
                "{text:?}"
            );
        }
    }
}
