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
//! `status GROUP TERM LEADER COMMIT APPLIED PATIENCE_NS RECONNECT_NS MEASURING_NS ASKING_NS HEARD`,
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
//! time it changes; and the heartbeats taken from the peers not yet judged (`Status::measuring`,
//! 0 once every peer is), so a watcher comparing a range's lines sees them move as the node
//! measures its links. On a line of its own, `held 1` while the node waits on its own device and
//! `held 0` once it no longer does, said each time that changes. After each status,
//! `log FLUSHING_NS LONGEST_NS`: how
//! long the log's flush has been out (0 when none is) and the longest an update has waited for
//! its commit (hyper-log's `LogStats`), since the node moves only as its writes become durable;
//! `room COMPACTIONS WAITS` as either moves: the node's rounds of compaction, each asked when the
//! log refused a write for room, the only time the node compacts (`Status::compactions`, node.md
//! §6.2), and the liveness stream's writes the log refused for room (`Status::liveness_waits`); and
//! `replaced GROUP FAILED JOINING` once, from the node that led the range then, when a
//! replacement ended; and
//! `link PEER KEYED DIALS ENDED SUSPECTED SUSPECT REFUSAL` when a peer's link changes: whether the
//! plane holds keys for its connection, the dials made to it, the connections and dials to it that
//! ended, the times the node's detectors came to suspect it and whether they suspect it now
//! (`suspect` or `trusted`), and the last refusal one ended with (`Node::links`); and
//! `moved BYTES SERVE_NS` as its transfers move:
//! the bytes of exchanges' bodies it has read and written (`Node::moved`), said at most every half
//! of the transport's serve period `SERVE_NS`, the longest an exchange it carries may go without
//! moving before the transport refuses it; and, in an orderly stop, `stopping` as it begins,
//! `handed GROUP LEADER` for each range whose lead it handed over, and `stopped` as it ends. A
//! node with an S3 endpoint says `s3-bound ADDR CONNECTIONS`, the connections it serves, once its listener is bound: where it
//! listens and the connections it holds at once (node.md §4.1). On standard input:
//! `learn ID QUIC PLANE`, where a peer listens, `replace FAILED JOINING`, a member lost for good
//! and the spare that takes its place in every range the node hosts (`Node::replace`), `stop`,
//! which asks the orderly stop as SIGTERM does, and `chunks`, answered `chunks RECEIVED WRITTEN
//! HELD UNKEPT FRAGMENTS BUDGET FREE`: the bytes of chunks its peers put on it over QUIC, of those
//! it put on its own volumes, of those its volumes hold and of those of the first two no volume
//! kept; the fragments its volumes' indexes hold and the most they may; and the bytes of its
//! volumes' free segments (`Node::chunk_bytes`); and `counts`, answered with the binary's account
//! of its own process ([`answer_counts`]: `uncounted` where the binary gives none), then the
//! write path's counts (`write_path_counts`): the log's frames, their updates and its flushes,
//! the entries the node proposed and their commands, and its updates' commit waits since it was
//! last asked; then the table of what held its shard (`slow_poll_counts`): each spawn site and
//! label whose polls held the shard past twice its quantum, with those polls since the shard
//! began, how long they held it in all and at most, and whose time it was. A line it cannot read
//! ends the process with the line named. Standard input closing ends `mantle node`, its
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
use std::fmt::Write as _;
use std::future::Future;
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::{Pin, pin};
use std::sync::OnceLock;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::time::Duration;

use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, LogStats, Waits};
use hyper_rt::SlotHolder;
use hyper_rt::combine::{Either, Either3, race2, race3};
use hyper_rt::machine::calibration::{Calibration, Policy};
use hyper_rt::runtime::{Runtime, RuntimeConfig};
use hyper_rt::signal::Signal;
use hyper_timing::Histogram;
use hyper_transport::Refusal;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_ec::durability::{Burst, Rates};
use mantle_engine::store::{Config as StoreConfig, Shape, Store};
use mantle_engine::trunk::TrunkConfig;
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::name;
use mantle_meta::record::History;
use mantle_meta::record::MemberState;
use mantle_meta::root::{self, Enlist, Join, Lived, MAX_DOMAIN};
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command};
use mantle_range::{ConfState, Range, Settings, Stored};
use mantle_s3::seal::WrappingKey;

use crate::ask::{CommandAsk, CommandOutcome, Wait};
use crate::derive::{Derived, Device, Formatted, Measured, Stated, VolumeBytes, VolumeFacts};
use crate::directory::Peer;
use crate::history;
use crate::layout::{Layout, LayoutError, Live, Stopped};
use crate::names::{Answered, Names};
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
    /// The grant the node's certificate names, if it was granted, and that certificate's
    /// fingerprint: what its join states (docs/design/membership.md §2, §3).
    grant: Option<u64>,
    fingerprint: [u8; mantle_meta::record::FINGERPRINT],
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
    let own = issued(certificate)?;
    let grant = mantle_cert::node_names(&own)
        .map_err(|e| ProcessError::Certificate(certificate.to_path_buf(), e))?
        .grant;
    let fingerprint = mantle_cert::fingerprint(&own);
    let chain = vec![CertificateDer::from(own)];
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
        grant,
        fingerprint,
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

/// A seed as its `seed` line names it (docs/design/membership.md §3 step 2): a member a granted
/// node dials first, known by its name under the authority rather than a pin, and where it listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedLine {
    pub id: u64,
    /// Where it listens, QUIC's endpoint and the plane's, when the line names them: a member bound
    /// at a port of its own choosing is told on standard input, as a peer that moved is.
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
    /// The members a granted node dials first (`seed` lines), none for a founder.
    pub seeds: Vec<SeedLine>,
    /// The host its peers dial it at on its QUIC and plane ports where it differs from the address
    /// it binds (`reach`), which its join states (membership.md §3).
    pub reach: Option<String>,
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
    /// The shards the node places its ranges over (`range_shards`, node.md §2.9).
    pub range_shards: RangeShards,
}

/// The shards a node places its ranges over (`range_shards`, node.md §2.9): a mode its operator
/// states, `first` where unstated, so a ladder measures both without a change to the code.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RangeShards {
    /// Every range on the node's first shard, beside its transport and its native door: ladder
    /// r28 (lean-doors `54fda898`) served 2,779 PUTs a second best, where r40 (`ec7519e1`),
    /// placed over the shards, served 641.
    #[default]
    First,
    /// Each range on the shard past the first with the fewest of the node's ranges
    /// (`node::spread`), each driven by a task on its shard.
    Spread,
}

impl std::str::FromStr for RangeShards {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "first" => Ok(Self::First),
            "spread" => Ok(Self::Spread),
            other => Err(format!("{other} is not first or spread")),
        }
    }
}

impl std::fmt::Display for RangeShards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::First => "first",
            Self::Spread => "spread",
        })
    }
}

/// What a node that hosts objects states (`GatewaySettings`), its root keys by their files.
pub struct GatewayConfig {
    pub keys: Vec<(u32, PathBuf)>,
    pub handover_ns: u64,
    pub window: usize,
    pub objects: usize,
    pub objects_bytes: u64,
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
    "range_shards",
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
    "gateway.objects_bytes",
    "gateway.clients",
    "gateway.bucket_quota",
    "gateway.scan_rows",
    "durability.chunk_rate",
    "durability.repair_rate",
    "durability.target",
];

/// The lines `mantle node`'s file repeats: its peers, ranges and volumes, its root key's
/// generations and the durability model's bursts.
pub const REPEATED: &[&str] = &[
    "peer",
    "seed",
    "range",
    "volume",
    "root_key",
    "durability.burst",
];

/// The keys `mantle serve`'s file states once each, its operator's alone (node.md §1.4's first
/// column): every other value is measured or derived, and a key of those is refused. `memory`
/// is the most the node may hold, `durability.target` a block's annual chance of loss at most, and
/// `durability.repair_rate` the cell's repairs an hour (docs/design/repair.md §2), each where
/// stated.
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
    "reach",
    "root.max_nodes",
    "root.max_volumes",
    "memory",
    "durability.target",
    "durability.repair_rate",
    "stop_ms",
    "range_shards",
    "s3",
    "s3.region",
    "s3.certificate",
    "s3.key",
];

/// The lines `mantle serve`'s file repeats: its peers and ranges, its volumes, each `volume = DIR
/// SIZE`, its root key's generations, and the cell's access keys, each `access_key = ID FILE`.
pub const STATED_REPEATED: &[&str] = &[
    "peer",
    "seed",
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

/// The `seed` lines, each `seed = ID QUIC PLANE`, or `seed = ID` for one told where it listens on
/// standard input: a member a granted node dials first, by its id and where it listens, each
/// `HOST:PORT` (docs/design/membership.md §3 step 2).
fn seeds(lines: &Lines) -> Result<Vec<SeedLine>, ProcessError> {
    let mut seeds = Vec::with_capacity(lines.all("seed").len());
    for (number, value) in lines.all("seed") {
        let form = || ProcessError::Line(*number, "seed = ID QUIC PLANE, or seed = ID".into());
        let fields: Vec<&str> = value.split_whitespace().collect();
        let (id, at) = match fields.as_slice() {
            [id] => (id, None),
            [id, quic, plane] => (id, Some((quic, plane))),
            _ => return Err(form()),
        };
        let endpoint = |text: &str| {
            Endpoint::parse(text).ok_or_else(|| {
                ProcessError::Line(
                    *number,
                    format!("{text}: HOST:PORT, an IPv6 address in brackets"),
                )
            })
        };
        let at = match at {
            Some((quic, plane)) => Some((endpoint(quic)?, endpoint(plane)?)),
            None => None,
        };
        seeds.push(SeedLine {
            id: id.parse().map_err(|_| form())?,
            at,
        });
    }
    Ok(seeds)
}

/// The `reach` key: a host its peers dial it at, an IP address or a DNS name, where stated.
fn reach(lines: &Lines) -> Result<Option<String>, ProcessError> {
    let Some(host) = lines.maybe::<String>("reach")? else {
        return Ok(None);
    };
    if host.parse::<std::net::IpAddr>().is_err() && mantle_cert::check_dns_name(&host).is_err() {
        return Err(ProcessError::Value(
            "reach",
            format!("{host:?} is neither an IP address nor a DNS name"),
        ));
    }
    Ok(Some(host))
}

/// The peers' addresses as the node starts (node.md §1.7 step 5), and the peers its lines name by a
/// host name, which the node looks up again as it runs (`crate::names`).
pub struct Resolved {
    /// Where each peer listens whose line names an address, or a name that resolved now.
    pub addresses: Vec<(u64, SocketAddr, SocketAddr)>,
    /// Each peer named by a host name: where its line says it listens and what its name gave now,
    /// none where it did not resolve.
    pub named: Vec<crate::names::Start>,
    /// Why each name that did not resolve now did not, its peer and its endpoint, for the node to say.
    pub unresolved: Vec<(u64, String, String)>,
}

/// Where each peer whose line names it listens, resolved now (node.md §1.7 step 5) through the
/// platform's resolver, as hyper-rt's `dns::resolve` does (std's `ToSocketAddrs`: getaddrinfo on
/// Unix, GetAddrInfoW on Windows): the first of a name's addresses in the family of the node's own
/// socket, the only family that socket sends to. It runs before the node's runtime starts, so a
/// lookup's wait holds no shard. An address of the other family stops the start, naming the peer;
/// a name that does not resolve does not, since a name stands for whatever address its holder has
/// now, and a Kubernetes pod not yet made again has none (research/78): the node looks it up again
/// as it runs.
pub fn resolve(config: &ProcessConfig) -> Result<Resolved, ProcessError> {
    let mut resolved = Resolved {
        addresses: Vec::with_capacity(config.peers.len()),
        named: Vec::new(),
        unresolved: Vec::new(),
    };
    let named = config
        .peers
        .iter()
        .filter_map(|peer| peer.at.as_ref().map(|at| (peer.id, at)))
        .chain(
            config
                .seeds
                .iter()
                .filter_map(|seed| seed.at.as_ref().map(|at| (seed.id, at))),
        );
    for (id, (quic, plane)) in named {
        let is_address = |at: &Endpoint| at.host.parse::<std::net::IpAddr>().is_ok();
        if is_address(quic) && is_address(plane) {
            resolved.addresses.push((
                id,
                lookup(id, quic, config.quic)?,
                lookup(id, plane, config.plane)?,
            ));
            continue;
        }
        let found = lookup(id, quic, config.quic)
            .and_then(|q| lookup(id, plane, config.plane).map(|p| (q, p)));
        match found {
            Ok((q, p)) => {
                resolved.addresses.push((id, q, p));
                resolved.named.push(crate::names::Start {
                    id,
                    at: (quic.clone(), plane.clone()),
                    known: Some((q, p)),
                });
            }
            Err(e) => {
                resolved.named.push(crate::names::Start {
                    id,
                    at: (quic.clone(), plane.clone()),
                    known: None,
                });
                let (at, why) = match e {
                    ProcessError::Resolve(_, at, why) => (at, why),
                    other => (quic.to_string(), other.to_string()),
                };
                resolved.unresolved.push((id, at, why));
            }
        }
    }
    Ok(resolved)
}

/// A named peer's lookup in flight ([`look_up_peer`]): its id and what its name gave.
type Looking = Pin<Box<dyn Future<Output = (u64, Result<(SocketAddr, SocketAddr), String>)>>>;

/// The first of `at`'s addresses in `own`'s family, asked of hyper-rt's resolver on the blocking
/// pool, off the node's shards: how a running node looks up a named peer again (`crate::names`).
async fn look_up(at: &Endpoint, own: SocketAddr) -> Result<SocketAddr, String> {
    // Every address the platform's resolver gives, as [`lookup`] takes them.
    let found = hyper_rt::dns::resolve(&at.host, at.port, usize::MAX)
        .await
        .map_err(|e| format!("{e:?}"))?;
    found
        .into_iter()
        .find(|address| address.is_ipv4() == own.is_ipv4())
        .ok_or_else(|| {
            let family = if own.is_ipv4() { "IPv4" } else { "IPv6" };
            format!("no {family} address, the family the node binds")
        })
}

/// A named peer's lookup, `wait` nanoseconds from now: its QUIC endpoint's address and its plane's,
/// one lookup where both name one host.
async fn look_up_peer(
    id: u64,
    (quic, plane): (Endpoint, Endpoint),
    own: (SocketAddr, SocketAddr),
    wait: u64,
) -> (u64, Result<(SocketAddr, SocketAddr), String>) {
    if wait > 0
        && let Err(e) = hyper_rt::futures::sleep(wait).await
    {
        return (id, Err(format!("{e:?}")));
    }
    let q = match look_up(&quic, own.0).await {
        Ok(q) => q,
        Err(e) => return (id, Err(e)),
    };
    let p = if plane.host == quic.host && own.0.is_ipv4() == own.1.is_ipv4() {
        Ok(SocketAddr::new(q.ip(), plane.port))
    } else {
        look_up(&plane, own.1).await
    };
    (id, p.map(|p| (q, p)))
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
            seeds: seeds(&lines)?,
            reach: None,
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
            range_shards: lines.maybe("range_shards")?.unwrap_or_default(),
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
        objects_bytes: lines.get("gateway.objects_bytes")?,
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
/// owner may read, as `mantle init` makes them (encryption.md §2); what each operation holds at
/// its own size, at the window its file states, under the schemes its blocks may take in a cell
/// its `enlist` names the most domains of (`derive::charges`), as the node's floor reckoned them.
fn gateway_settings(
    config: GatewayConfig,
    keys: Vec<(u32, WrappingKey)>,
    enlist: Option<&Enlisting>,
) -> Result<GatewaySettings, ProcessError> {
    let rates = Rates {
        chunk: config.chunk_rate,
        domains: None,
        bursts: config.bursts,
        repair: config.repair_rate,
    };
    let charges = crate::derive::charges(config.window, enlist, &rates, config.target)
        .map_err(|e| ProcessError::Value("gateway.window", e.to_string()))?;
    Ok(GatewaySettings {
        keys,
        handover_ns: config.handover_ns,
        window: config.window,
        objects: config.objects,
        objects_bytes: config.objects_bytes,
        charges,
        clients: config.clients,
        bucket_quota: config.bucket_quota,
        scan_rows: config.scan_rows,
        rates,
        target: config.target,
    })
}

/// How a node with `volume` lines, or one that states the cell's bound on its nodes, enlists or
/// joins (docs/design/membership.md §3): the root range its `range` lines name, its `domain`, and
/// the table's `root.` bounds, each required of it; none for a node with neither.
fn enlisting(lines: &Lines) -> Result<Option<Enlisting>, ProcessError> {
    if lines.all("volume").is_empty() && lines.maybe::<u32>("root.max_nodes")?.is_none() {
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
    start(
        config,
        (node, locks),
        identity,
        None,
        Asked {
            ends_with_input: true,
            leave: false,
        },
    )
}

/// What a node's `counts` answer begins with: the binary's account of its own process (its
/// allocator's counts, its faults and switches), which only the binary can give, the global
/// allocator being the binary's choice (`mantle bench doors`). Set once, before the node runs.
static COUNTS: OnceLock<fn() -> String> = OnceLock::new();

/// Sets the binary's account that `counts` answers first ([`COUNTS`]), the line's words after
/// `counts`, the write path's counts said after them (`write_path_counts`): false when it was set
/// already. A node it was never set for says `uncounted` in its place.
pub fn answer_counts(with: fn() -> String) -> bool {
    COUNTS.set(with).is_ok()
}

/// The write path's counts, written after the binary's account in a `counts` answer, each a name
/// and a value: the log's frames, the updates they carried and its flushes (hyper-log's
/// `LogStats`), and the entries the node proposed as its ranges' leader and the commands they
/// carried (`Node::proposed`), each since the node opened; then its updates' commit waits since
/// the node was last asked, how many, and their median, 99th percentile and longest in
/// nanoseconds, each the most its histogram's bucket holds (`-` where none ended). `asked` is the
/// log's commit-wait histogram (`waited`) as it stood at the last answer, set to now's: a copy in
/// the room it has, so an answer grows nothing.
fn write_path_counts(
    said: &mut String,
    (frames, updates, flushes): (u64, u64, u64),
    (entries, commands): (u64, u64),
    waited: &Histogram,
    asked: &mut Histogram,
) -> std::fmt::Result {
    let waits = waited.count().saturating_sub(asked.count());
    write!(
        said,
        " frames {frames} updates {updates} flushes {flushes} entries {entries} commands {commands} \
         waits {waits}"
    )?;
    let quantiles = [
        ("wait_p50_ns", since(waited, asked, 500_000)),
        ("wait_p99_ns", since(waited, asked, 990_000)),
        ("wait_max_ns", since(waited, asked, 1_000_000)),
    ];
    for (name, ns) in quantiles {
        match ns {
            Some(ns) => write!(said, " {name} {ns}")?,
            None => write!(said, " {name} -")?,
        }
    }
    asked.clone_from(waited);
    Ok(())
}

/// The table of what held the node's shard, written after the write path's counts in a `counts`
/// answer, as names and values (hyper-rt `slow`): `slow_entries`, the entries that follow, then
/// for each `slow_site`, the file, line and column its tasks were spawned at, `slow_label`, the
/// label they had as their polls ended (each a word, [`word`]; `-` for none), `slow_polls`, the
/// polls that held the shard past twice its quantum, `slow_held_ns` and `slow_longest_ns`, how
/// long they held it in all and at most, and `slow_task` and `slow_host`, those of them that were
/// the task's time and the host's. Every count is since the shard began, so a reader takes two
/// answers' difference; the entries are at most the table's ([`hyper_rt::slow::ENTRIES`] and its
/// one unkeyed entry), so an answer grows no further.
pub fn slow_poll_counts(said: &mut String, polls: &[hyper_rt::slow::SlowPoll]) -> std::fmt::Result {
    write!(said, " slow_entries {}", polls.len())?;
    for poll in polls {
        said.push_str(" slow_site ");
        match poll.site {
            Some(site) => {
                word(said, site.file())?;
                write!(said, ":{}:{}", site.line(), site.column())?;
            }
            None => said.push('-'),
        }
        said.push_str(" slow_label ");
        match poll.label {
            Some(label) => word(said, label.name)?,
            None => said.push('-'),
        }
        write!(
            said,
            " slow_polls {} slow_held_ns {} slow_longest_ns {} slow_task {} slow_host {}",
            poll.polls, poll.held_ns, poll.longest_ns, poll.task_polls, poll.host_polls
        )?;
    }
    Ok(())
}

/// `text` as one word of a line its reader splits at whitespace: each whitespace character, and
/// each `%`, written as `%` and its octets' two hex digits, as a URI percent-encodes an octet (RFC
/// 3986 §2.1), so a path with a space in it stays one word.
fn word(said: &mut String, text: &str) -> std::fmt::Result {
    for c in text.chars() {
        if c.is_whitespace() || c == '%' {
            for octet in c.to_string().bytes() {
                write!(said, "%{octet:02X}")?;
            }
        } else {
            said.push(c);
        }
    }
    Ok(())
}

/// The `q_ppm` quantile of the values `now` counted since it stood as `then`, bucket by bucket, as
/// `Histogram::quantile` reads one histogram (by the nearest rank, the most the bucket holding it
/// holds): `None` when nothing was counted since. `then` is an earlier state of the one histogram,
/// so no bucket of it is above `now`'s.
fn since(now: &Histogram, then: &Histogram, q_ppm: u32) -> Option<u64> {
    let count = now.count().checked_sub(then.count()).filter(|n| *n > 0)?;
    let rank = u128::from(count)
        .saturating_mul(u128::from(q_ppm))
        .div_ceil(1_000_000)
        .clamp(1, u128::from(count));
    let mut seen = 0u128;
    let buckets = now.buckets().iter().zip(then.buckets().iter());
    for (bucket, (held, was)) in buckets.enumerate() {
        seen = seen.saturating_add(u128::from(held.saturating_sub(*was)));
        if seen >= rank {
            let (_, past) = Histogram::bounds(bucket);
            return Some(if bucket == Histogram::BUCKETS.saturating_sub(1) {
                past
            } else {
                past.saturating_sub(1)
            });
        }
    }
    None
}

/// Runs the node whose operator's statements the file at `config` holds (`mantle serve`, node.md
/// §1.4): each volume's device directory locked, then each device's profile kept or measured, the
/// node directory's device and the host's memory, cores and page measured; every other value
/// derived from those (`derive::derive`) and said on standard output, a `basis` line each, before
/// anything opens; then the node, as `mantle node` runs it, until it stops in order.
pub fn serve_stated(config: &Path, leave: bool) -> Result<(), ProcessError> {
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
    let (derived, node, locks) = said_derived(stated)?;
    let mut out = std::io::stdout().lock();
    // The S3 endpoint as its keys were read: where it listens, its region and its keys' ids.
    if let Some(s3) = &s3 {
        let ids: Vec<&str> = s3.access.iter().map(|k| k.id.as_str()).collect();
        writeln!(out, "s3 {} {} {}", s3.listen, s3.region, ids.join(","))
            .map_err(|e| ProcessError::Say(e.to_string()))?;
    }
    out.flush().map_err(|e| ProcessError::Say(e.to_string()))?;
    // The runtime's writer says the rest: this thread holds no lock on standard output for it.
    drop(out);
    start(
        derived.config,
        (node, locks),
        identity,
        s3,
        Asked {
            ends_with_input: false,
            leave,
        },
    )
}

/// What the process is asked of the node as it starts: whether it ends when its standard input
/// closes (`mantle node`, its harness's child), and whether it leaves the cell in order and then
/// ends (`mantle serve --leave`, docs/design/membership.md §4).
#[derive(Clone, Copy)]
struct Asked {
    ends_with_input: bool,
    leave: bool,
}

/// What `mantle serve` derives from the statements the file at `config` holds, said as it says
/// them, a `basis` line each, without serving (`mantle derive`): the node directory and each
/// volume's device directory locked, each device's profile kept or measured and kept as a start
/// keeps it, the rest measured, and every other value derived (`derive::derive`). Among the bases
/// is `memory.need`, what an orchestrator gives the node's pod (docs/design/kubernetes.md §3), so
/// an operator derives it on the machine the node will run on before it is deployed.
pub fn derive_stated(config: &Path) -> Result<(), ProcessError> {
    let stated = read_stated(config).map_err(step(Step::Configuration))?;
    let (_derived, _node, _locks) = said_derived(stated)?;
    Ok(())
}

/// The start's steps from the locks to the budgets (node.md §1.7) for `stated`: its directory's
/// and its volumes' locks, the measurements, the derivation, each basis said on standard output.
fn said_derived(stated: Stated) -> Result<(Derived, NodeLock, Locks), ProcessError> {
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
    out.flush().map_err(|e| ProcessError::Say(e.to_string()))?;
    Ok((derived, node, locks))
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
    asked: Asked,
) -> Result<(), ProcessError> {
    // Room for both: a stop at once, and the node's end after its teardown, which no one hears.
    let (tell, heard) = sync_channel::<Ended>(2);
    let at_once = tell.clone();
    let thread = std::thread::Builder::new()
        .name("node".into())
        .spawn(move || {
            let ran = run_node(config, held, identity, (s3, asked), at_once);
            let _ = tell.send(Ended::Ran(ran));
        })
        .map_err(|e| ProcessError::Runtime(format!("the node's thread: {e}")))?;
    match heard.recv() {
        Ok(Ended::Ran(ran)) => {
            thread
                .join()
                .map_err(|_| ProcessError::Runtime("the node's thread unwound".into()))?;
            ran
        }
        Ok(Ended::AtOnce(why)) => Err(ProcessError::Stopped(why)),
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
    (s3, asked): (Option<S3Keys>, Asked),
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
        grant,
        fingerprint,
    } = identity;
    let gateway = config
        .gateway
        .take()
        .map(|g| gateway_settings(g, keys, config.enlist.as_ref()))
        .transpose()?;
    let resolved = resolve(&config).map_err(step(Step::Network))?;
    // A named peer is looked up again as the node runs, on hyper-rt's blocking pool, off its shards:
    // one lookup at a time (`crate::names`), so one place and no line; a granted node's members
    // name the hosts they are reached at in the root range's rows, any of which may be a name.
    if !resolved.named.is_empty() || grant.is_some() {
        hyper_rt::blocking::start(hyper_rt::blocking::Config {
            workers: 1,
            shares: vec![hyper_rt::blocking::Share::new(hyper_rt::dns::SHARE, 1)],
        })
        .map_err(|e| step(Step::Network)(ProcessError::Runtime(format!("{e:?}"))))?;
    }
    // The S3 listener serves as many connections as the node runs operations (`listener::Settings`).
    let listening = s3.is_some().then(|| {
        gateway
            .as_ref()
            .map_or(0, |g| u64::try_from(g.objects).unwrap_or(u64::MAX))
    });
    let runtime = runtime(&config, listening).map_err(step(Step::Network))?;
    let rt = Runtime::start(&runtime)
        .map_err(|e| step(Step::Network)(ProcessError::Runtime(format!("{e:?}"))))?;
    // The shards past the first, which the S3 listener hands its connections to, each named by
    // the registration holding it: a lane submitted to it reaches this runtime's shard and never
    // a later holder of its slot. A shard already gone takes none.
    let others: Vec<SlotHolder> = rt
        .shard_ids()
        .split_first()
        .map_or(&[][..], |(_, others)| others)
        .iter()
        .filter_map(|shard| rt.holder_of(*shard))
        .collect();
    let ran = run_on_first_shard(rt, move || {
        serve(
            config,
            storage,
            gateway,
            (credentials, peers, (grant, fingerprint)),
            (resolved, s3, others),
            (asked, at_once),
        )
    })?;
    drop(node);
    ran
}

/// Runs the future `make` makes on `rt`'s first shard until it ends, then shuts `rt` down: what the
/// future left running on its shards is cancelled and joined before this returns, as
/// `LocalRuntime::block_on` does with what its root leaves. `make` crosses to the shard's thread
/// and makes the future there, in a launcher that spawns it on that shard and ends, as hyper-rt's
/// `tcp::serve` makes a connection's handler on the shard that serves it: the future need not be
/// `Send`. Its output comes back on a channel of one slot, the one value sent on it: the output, or
/// the launcher's refusal to spawn it. A task that ends without either, unwound or its shard
/// lost, drops its sender, which ends the wait. The task is Critical (`hyper_rt::coop::Class`): it
/// runs the node's consensus, log and transport, so woken it goes ahead of the S3 connections the
/// shard serves beside it, which stay Normal, the class every other spawn takes, the shares of the
/// shard's time staying equal (hyper-raft docs/runtime.md §3.15).
fn run_on_first_shard<M, F, T>(rt: Runtime, make: M) -> Result<T, ProcessError>
where
    M: FnOnce() -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let (tell, heard) = sync_channel(1);
    let launch = async move {
        let refused = tell.clone();
        let made = make();
        let task = async move {
            let _ = tell.try_send(Ok(made.await));
        };
        if let Err(e) = hyper_rt::futures::spawn_detached_as(hyper_rt::coop::Class::Critical, task)
        {
            let _ = refused.try_send(Err(e));
        }
    };
    let ran = rt
        .shard_ids()
        .first()
        .ok_or_else(|| "a runtime of no shard".to_string())
        .and_then(|first| rt.spawn_on(*first, launch).map_err(|e| format!("{e:?}")))
        .and_then(|()| match heard.recv() {
            Ok(sent) => sent.map_err(|e| format!("{e:?}")),
            Err(_) => Err("the node's task ended without its output".to_string()),
        });
    // Every shard stops whether or not the task ran; a shard's own failure is why the task ended
    // without its output, or a failure of its own after it.
    let ended = rt
        .shutdown()
        .map_err(|e| ProcessError::Runtime(format!("{e:?}")));
    ended.and(ran.map_err(ProcessError::Runtime))
}

/// The S3 listener `keys` state, its door open on `node`: where it listens, its region, keys and
/// TLS (node.md §4.1). It binds the address the keys state over TCP, a stated port that port, as an
/// https authority serves TCP and QUIC on one (research/84); a stated port of 0 takes a TCP port
/// of its own, the UDP port QUIC was given saying nothing of the TCP port of the same number.
/// Plain HTTP on a loopback address, TLS with the endpoint's certificate elsewhere. Its door holds
/// a request for each connection the listener admits, so it refuses none for room: each waits
/// there until the host has room for it (node.md §4.5). A node that hosts no objects has no
/// operation to hand it. It hands its connections to the shards `others` names
/// (`Listener::spread`).
fn listen<E: Engine + 'static>(
    node: &mut Node<E>,
    keys: S3Keys,
    (id, hosts, connections): (u64, bool, u64),
    others: &[SlotHolder],
) -> Result<crate::listener::Listener, ProcessError> {
    let refused = |e: String| ProcessError::Listener(e);
    if !hosts {
        return Err(refused(
            "an S3 endpoint on a node that hosts no objects".into(),
        ));
    }
    let waiting =
        usize::try_from(connections).map_err(|_| refused("the listener's connections".into()))?;
    let listen = keys.listen;
    let tls = if listen.ip().is_loopback() {
        None
    } else {
        Some((keys.certificate, keys.key))
    };
    let door = node.open_door(waiting)?;
    let mut listener = crate::listener::Listener::bind(
        crate::listener::Settings {
            listen,
            region: keys.region,
            domains: keys.names,
            access: keys.access,
            tls,
            node: id,
            connections,
        },
        door,
    )
    .map_err(|e| refused(e.to_string()))?;
    listener
        .spread(others)
        .map_err(|e| refused(e.to_string()))?;
    Ok(listener)
}

/// What the file at `path` states for `mantle serve`: only [`STATED_KEYS`] and
/// [`STATED_REPEATED`], each volume `volume = DIR SIZE`.
pub fn read_stated(path: &Path) -> Result<Stated, ProcessError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| ProcessError::Read(path.to_path_buf(), e.to_string()))?;
    let lines = Lines::parse(&text, STATED_KEYS, STATED_REPEATED)?.of_file(path);
    let mut volumes = Vec::with_capacity(lines.all("volume").len());
    for (number, value) in lines.all("volume") {
        let (dir, bytes) = volume_line(value).ok_or_else(|| {
            ProcessError::Line(
                *number,
                "volume = DIR SIZE, or volume = DIR spare KEEP".into(),
            )
        })?;
        volumes.push((lines.path(dir), bytes));
    }
    let target = lines.maybe::<f64>("durability.target")?;
    if target.is_some_and(|t| !(t.is_finite() && t > 0.0 && t < 1.0)) {
        return Err(ProcessError::Value(
            "durability.target",
            "a probability between 0 and 1".into(),
        ));
    }
    let repair = lines.maybe::<f64>("durability.repair_rate")?;
    if repair.is_some_and(|r| !(r.is_finite() && r > 0.0)) {
        return Err(ProcessError::Value(
            "durability.repair_rate",
            "repairs an hour, more than none".into(),
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
        seeds: seeds(&lines)?,
        reach: reach(&lines)?,
        ranges: ranges(&lines)?,
        voters: voters(&lines)?,
        volumes,
        enlist: enlisting(&lines)?,
        keys: root_keys(&lines)?,
        ceiling: lines.maybe("memory")?,
        stop: lines.maybe("stop_ms")?.map(Duration::from_millis),
        target,
        repair,
        s3: s3_stated(&lines)?,
        range_shards: lines.maybe("range_shards")?,
    })
}

/// A `volume` line's directory and bytes (docs/design/membership.md §3.1): `DIR SIZE`; or `DIR spare
/// KEEP`, a volume of what its file system spares less the `KEEP` bytes the machine's other work
/// keeps there, `DIR spare` where it keeps none. Its words are read from the end, so a directory
/// with a space in it reads; one whose last word is `spare` is stated with a `/` after it.
fn volume_line(value: &str) -> Option<(&str, VolumeBytes)> {
    let (rest, last) = value.rsplit_once(char::is_whitespace)?;
    let (dir, bytes) = if last == "spare" {
        (rest, VolumeBytes::Spare { keep: 0 })
    } else {
        let last = last.parse().ok()?;
        match rest.trim_end().rsplit_once(char::is_whitespace) {
            Some((dir, "spare")) => (dir, VolumeBytes::Spare { keep: last }),
            _ => (rest, VolumeBytes::Stated(last)),
        }
    };
    let dir = dir.trim();
    if dir.is_empty() {
        None
    } else {
        Some((dir, bytes))
    }
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
    // Each shard's control entries, as the runtime derives them (`runtime`), from the host's
    // calibration, kept in the node's directory once measured.
    let calibration = calibration(
        &stated.dir,
        crate::derive::probe_budget().map_err(|e| ProcessError::Runtime(e.to_string()))?,
    )?;
    let ring =
        RuntimeConfig::from_calibration(&calibration, &constants(&calibration)?, 1, 1).ring_entries;
    Ok(Measured {
        memory: facts.memory,
        cores: shards(&facts),
        ring,
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
        available: identity.file_system.available_bytes,
    }
}

/// Range `group`'s engine store in the node's directory `dir`.
fn store_path(dir: &Path, group: u128) -> PathBuf {
    dir.join(format!("range-{group:032x}.store"))
}

/// The machine's calibration record's file in the node's directory (node.md §1.6).
pub const CALIBRATION: &str = "calibration";

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

/// The timers the node's task holds at once, one for each sleep pending: its turn's wait, its
/// transport driver's QUIC timer, and, once a stop is asked, the stop's bound. A sleep that finds
/// every timer taken waits for one to free (hyper-rt's `Counters::timer_waits`), and the release
/// wakes the task, which ends the turn's wait at once (`Node::wait`: any wake ends it). Counted
/// two, a stopping node turned without pause until it ended: 8,834 turns and 8,798 timer waits in
/// a stop of 346 ms on macOS (diag/roll-stop-bound), a CPU spent for the whole stop. Each of the
/// S3 listener's connection tasks holds one more while it waits on its client
/// (`listener::tasks`).
pub(crate) const TIMERS: usize = 3;

/// The node's shards: one for each CPU the process runs at once, its cores capped by the whole CPUs
/// its CPU budget buys (hyper-rt `Facts::cpus_at_once`: its affinity and its cgroup's quota), and
/// one where the budget buys less than a whole CPU, the least that runs the node.
fn shards(facts: &hyper_rt::machine::facts::Facts) -> usize {
    facts.cpus_at_once().max(1)
}

/// The node's shards ([`shards`]), calibrated on this machine (hyper-rt
/// `RuntimeConfig::from_calibration`). The first runs the node's task, its ranges, log, transport
/// and doors; with an S3 listener, the rest each run the connections it hands them
/// (`listener::Listener::spread`). hyper-rt sizes every shard alike, so each is sized for the
/// first's role, the largest: the node's one task, the launcher that spawns it there
/// ([`run_on_first_shard`]), the timers the task holds at once ([`TIMERS`]), and a socket for its
/// endpoint, each connection and its plane, each read and written; with an S3 listener, a task and
/// a timer for each connection it holds at once, served or answered refused (`listener::tasks`),
/// as each falls back to the first shard where the shard it was handed to refuses it, and a socket
/// for the listener and each of those connections. Another shard holds at most its taker of
/// handed connections and each connection.
fn runtime(config: &ProcessConfig, listening: Option<u64>) -> Result<RuntimeConfig, ProcessError> {
    let calibration = calibration(&config.dir, config.probe_budget)?;
    let constants = constants(&calibration)?;
    // The shape the node's floor counts each shard at (`derive::runtime_bytes`).
    let shape = crate::derive::shard_shape(listening, config.peers.len())
        .ok_or_else(|| ProcessError::Runtime("the node's shards' tasks".into()))?;
    let mut runtime =
        RuntimeConfig::from_calibration(&calibration, &constants, shape.tasks, shape.timers);
    runtime.shards = u16::try_from(shards(&calibration.facts))
        .map_err(|_| ProcessError::Runtime("the node's shards".into()))?;
    runtime.pin = false;
    runtime.cores.clear();
    runtime.interests_per_shard = shape.interests;
    Ok(runtime)
}

/// The runtime's constants from `calibration`, no core reserved and no objective of the node's own.
fn constants(
    calibration: &Calibration,
) -> Result<hyper_rt::machine::calibration::Constants, ProcessError> {
    calibration
        .constants(&Policy {
            reserved_cores: 0,
            lateness_tolerance_ns: None,
            latency_objective_ns: None,
        })
        .map_err(|unmet| ProcessError::Runtime(format!("{unmet:?}")))
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

/// The node, its talk on standard input and output, and its S3 listener where `s3` states one,
/// which hands its connections to the shards `others` names.
async fn serve(
    config: ProcessConfig,
    storage: Storage,
    gateway: Option<GatewaySettings>,
    (credentials, peers, (grant, fingerprint)): (
        Credentials,
        Vec<Peer>,
        (Option<u64>, [u8; mantle_meta::record::FINGERPRINT]),
    ),
    (resolved, s3, others): (Resolved, Option<S3Keys>, Vec<SlotHolder>),
    (asked, at_once): (Asked, SyncSender<Ended>),
) -> Result<(), ProcessError> {
    let Asked {
        ends_with_input,
        leave,
    } = asked;
    let Resolved {
        addresses,
        named,
        unresolved,
    } = resolved;
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
    // A node of a cell that states its bound on nodes keeps peers, pairs and keys for as many, and
    // every range it hosts may name as many members (docs/design/membership.md §9); a granted one
    // joins the cell under its grant (§3).
    let max_nodes = config.enlist.as_ref().map_or(0, |e| e.max_nodes);
    let members = usize::try_from(max_nodes).map_err(|e| {
        step(Step::Configuration)(ProcessError::Value("root.max_nodes", e.to_string()))
    })?;
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
                members,
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
    // The S3 listener serves as many connections as the node runs operations (`listener::Settings`).
    let connections = gateway
        .as_ref()
        .map_or(0, |g| u64::try_from(g.objects).unwrap_or(u64::MAX));
    // The shards the node places its ranges over, as its operator states (`range_shards`, node.md
    // §2.9): its first alone, or the runtime's shards that take tasks, its first and each past it a
    // holder names. Placed over every shard, ladder r40 (lean-doors ec7519e1,
    // adas-mac-studio.local-2, 1 B, native door) served PUTs at 641/s best, at 128 clients, and left
    // 86 unanswered at 256, where r28 (54fda898, ranges on the first shard) served 2,779/s at 1,024;
    // GETs fell from 8,551/s to 2,190/s.
    let shards = match config.range_shards {
        RangeShards::First => NonZeroUsize::MIN,
        RangeShards::Spread => NonZeroUsize::MIN.checked_add(others.len()).ok_or_else(|| {
            step(Step::Network)(ProcessError::Runtime("the node's shards".into()))
        })?,
    };
    let settings = NodeSettings {
        id: config.id,
        dir: config.dir.clone(),
        quic: config.quic,
        plane: config.plane,
        credentials,
        peers,
        addresses,
        ranges,
        shards,
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
        max_nodes,
        seeds: config.seeds.iter().map(|s| s.id).collect(),
    };
    let mut node = Node::open(settings).await.map_err(opened)?;
    // Each range placed past the first shard moves to its shard before the node's first turn
    // (node.md §2.9); a refusal stops the start, typed, as nothing would drive those ranges.
    node.spread(&others)
        .map_err(|e| step(Step::Network)(ProcessError::Node(e)))?;
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
    // The peers named by a host name, looked up again as the node runs (`crate::names`): each name
    // that did not resolve as the node started said, and asked again after a pause.
    let bound_at = (quic, plane);
    let mut names = Names::new(config.id, named, hyper_rt::futures::now_ns());
    for (peer, at, why) in &unresolved {
        say(&mut output, &format!("unresolved {peer} {at} {why}\n")).await?;
    }
    let mut looking: Option<Looking> = None;
    let listener = match s3 {
        Some(keys) => {
            let listener = listen(&mut node, keys, (config.id, hosts, connections), &others)
                .map_err(step(Step::Network))?;
            let at = listener
                .local_addr()
                .map_err(|e| step(Step::Network)(ProcessError::Listener(e.to_string())))?;
            let line = format!("s3-bound {at} {connections}\n");
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
    // Whether the root range enlisted the node in this run, and the readiness lock a supervisor's
    // check asks for (`mantle status`), held from then until the process ends.
    let (mut enlisted, mut live): (bool, Option<Live>) = (false, None);
    // A granted node of a cell whose members come and go joins under its grant, stating where its
    // peers dial it (docs/design/membership.md §3); any other enlists. The entry that carries
    // either gives it its time.
    let mut enlisting = match &config.enlist {
        Some(e) => {
            let command = match grant.filter(|_| max_nodes > 0) {
                Some(grant) => Command::Root(root::Command::Join(Join {
                    node: config.id,
                    grant,
                    fingerprint,
                    quic: reached_at(config.reach.as_deref(), quic).map_err(step(Step::Network))?,
                    plane: reached_at(config.reach.as_deref(), plane)
                        .map_err(step(Step::Network))?,
                    domain: e.domain.clone(),
                    volumes,
                    max_nodes: e.max_nodes,
                    max_volumes: e.max_volumes,
                    at_ns: 0,
                })),
                None => Command::Root(root::Command::Enlist(Enlist {
                    node: config.id,
                    domain: e.domain.clone(),
                    volumes,
                    max_nodes: e.max_nodes,
                    max_volumes: e.max_volumes,
                    at_ns: 0,
                })),
            };
            Some(node.command(e.root, command, u64::MAX, Wait::Answered)?)
        }
        None => None,
    };
    // The node's leave (membership.md §4): asked at its start or on its standard input, answered,
    // and done once the root range removed it; and an operator's statements that another node is
    // gone, each out until answered.
    let mut departure = Departure::Staying;
    // A member stated gone is asked of the root range once the node's commands have room; each
    // id is held once, and at most as many as the cell holds nodes.
    let mut stated_gone: Vec<(Option<CommandAsk>, u64)> = Vec::new();
    // Each member's standing as last said, and each range's configuration, a range a slot in the
    // configuration's order.
    let mut said_standing: Vec<(u64, MemberState, u64)> = Vec::new();
    let mut said_config: Vec<Option<(Vec<u64>, Vec<u64>)>> = vec![None; config.ranges.len()];
    let mut read = vec![0u8; chunk];
    let mut pending = Vec::new();
    // Each range's status as last said, a range a slot in the configuration's order.
    let mut told: Vec<Option<Status>> = vec![None; config.ranges.len()];
    // The log's measures, filled again at each status said and each `counts` answered (hyper-log's
    // `Log::stats`), and its commit-wait histogram as the last `counts` answer read it: one of
    // fixed room, copied over at each answer (`write_path_counts`).
    let mut stats: Option<Box<LogStats>> = None;
    let mut asked = Box::new(Histogram::new());
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
    // The log's refusals for room as last said: the node's rounds of compaction and the liveness
    // stream's writes it refused for room, none before the first saying.
    let mut said_room: Option<(u64, u64)> = None;
    // The cell's repair on this node as last said, none before the first saying.
    let mut said_repair: Option<crate::node::RepairStatus> = None;
    // The longest poll of the node's shard as last said, 0 before the first saying.
    let mut said_poll = 0u64;
    // Every shard of the node's runtime, its own first, none where the runtime registers none for
    // it; and the stall said last of each, a shard's stalls said at most once each half serve
    // period (`STALL_LINES_A_PERIOD`).
    let holders: Vec<Option<SlotHolder>> = std::iter::once(
        hyper_rt::registry::current_shard().and_then(hyper_rt::registry::holder_of),
    )
    .chain(others.iter().copied().map(Some))
    .collect();
    let mut said_stalls = vec![SaidStall::default(); holders.len()];
    let stall_gap_ns = serve_ns
        .checked_div(STALL_LINES_A_PERIOD)
        .unwrap_or(serve_ns);
    let signals = hyper_rt::signal::subscribe(STOP_SIGNALS).map_err(|e| talk(format!("{e:?}")))?;
    let mut stop = Stop::new(config.stop);
    // Whether standard input may say more: a node that does not end with it runs on once it closes,
    // as `mantle serve` does under a service manager, whose input is empty.
    let mut input_open = true;
    // The lines a turn says of the node (its links, ranges, log, movement and stop), written as
    // they are made into this one buffer and said together at the turn's end: one write and one
    // flush a turn that says anything, where each line was a `String` and a flush of its own. Its
    // storage is kept across the loop's turns, emptied each time it is said, so a turn regrows
    // nothing: no pool, the loop's one buffer. Bound: one turn's lines, a line for each link,
    // each range and each shard's stall at most, and one each for the replaced, the movement, the
    // held state, the longest poll, the log and the handovers, every line a few numbers long or,
    // a stall's, its task's spawn site and label.
    let mut lines = String::new();
    let wrote = |e: std::fmt::Error| ProcessError::Runtime(e.to_string());
    loop {
        if looking.is_none()
            && let Some((peer, at, wait)) = names.next(hyper_rt::futures::now_ns())
        {
            looking = Some(Box::pin(look_up_peer(peer, at, bound_at, wait)));
        }
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
            let looked = pin!(async {
                match looking.as_mut() {
                    Some(lookup) => lookup.await,
                    None => std::future::pending().await,
                }
            });
            race3(
                turn,
                line,
                race3(signal, bound, race2(accepting.as_mut(), looked)),
            )
            .await
        };
        match ended {
            Either3::First(turned) => turned?,
            Either3::Second(Ok(0)) if ends_with_input => return Ok(()),
            Either3::Second(Ok(0)) => input_open = false,
            Either3::Second(Ok(n)) => {
                pending.extend_from_slice(read.get(..n).unwrap_or_default());
                while let Some(end) = pending.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = pending.drain(..=end).collect();
                    if line.trim_ascii() == b"stop" {
                        stop_asked(&mut stop, &mut node, (&mut output, &at_once)).await?;
                    } else if line.trim_ascii() == b"chunks" {
                        let (received, written, held, unkept) = node.chunk_bytes()?;
                        let (fragments, budget) = held.fragments;
                        let line = format!(
                            "chunks {received} {written} {} {unkept} {fragments} {budget} {}\n",
                            held.bytes, held.free
                        );
                        say(&mut output, &line).await?;
                    } else if line.trim_ascii() == b"counts" {
                        let log = node.log_stats(stats.take())?;
                        let own = COUNTS
                            .get()
                            .map_or_else(|| "uncounted".to_owned(), |counts| counts());
                        let mut line = format!("counts {own}");
                        write_path_counts(
                            &mut line,
                            (log.frames, log.updates, log.flushes),
                            node.proposed(),
                            &log.commit_wait,
                            &mut asked,
                        )
                        .map_err(wrote)?;
                        // The table of the shard this task runs on, none where the runtime
                        // registers no table for it.
                        let slow = hyper_rt::registry::current_shard()
                            .and_then(hyper_rt::registry::holder_of)
                            .map(hyper_rt::slow::slow_polls)
                            .unwrap_or_default();
                        slow_poll_counts(&mut line, &slow).map_err(wrote)?;
                        let episodes: Vec<Option<u64>> = holders
                            .iter()
                            .map(|holder| {
                                holder
                                    .and_then(hyper_rt::watch::stalls)
                                    .map(|stalls| stalls.episodes)
                            })
                            .collect();
                        stall_counts(&mut line, &episodes).map_err(wrote)?;
                        line.push('\n');
                        stats = Some(log);
                        say(&mut output, &line).await?;
                    } else if line.trim_ascii() == b"leave" {
                        departure.ask(&mut node, config.enlist.as_ref(), enlisted)?;
                    } else if let Some(id) = gone_line(&line)? {
                        let most = config.enlist.as_ref().map(|e| e.max_nodes).ok_or(
                            ProcessError::Input(
                                format!("gone {id}"),
                                "a node that hosts no root range states no member gone",
                            ),
                        )?;
                        if stated_gone.iter().any(|(_, held)| *held == id) {
                            // Stated already: one statement out for each member.
                        } else if u64::try_from(stated_gone.len()).unwrap_or(u64::MAX)
                            >= u64::from(most)
                        {
                            say(
                                &mut output,
                                &format!(
                                    "gone {id} refused: as many stated gone as the cell's nodes\n"
                                ),
                            )
                            .await?;
                        } else {
                            stated_gone.push((None, id));
                        }
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
                stop_asked(&mut stop, &mut node, (&mut output, &at_once)).await?;
            }
            Either3::Third(Either3::Second(slept)) => {
                slept.map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
                // Past its bound with only its connections' closing periods left, the stop is
                // done (`Node::stop_at_bound`), and ends below as one done in time does.
                if !node.stop_at_bound() {
                    let why = "the stop's bound passed";
                    let _ = at_once.try_send(Ended::AtOnce(why));
                    return Err(ProcessError::Stopped(why));
                }
            }
            Either3::Third(Either3::Third(Either::First(ended))) => {
                return Err(ProcessError::Listener(ended.to_string()));
            }
            Either3::Third(Either3::Third(Either::Second((peer, found)))) => {
                looking = None;
                match names.answered(peer, found, hyper_rt::futures::now_ns()) {
                    Answered::Moved(quic, plane) => {
                        node.learn(peer, quic, plane)?;
                        say(&mut output, &format!("resolved {peer} {quic} {plane}\n")).await?;
                    }
                    Answered::Same => {}
                    Answered::Unresolved(why) => {
                        let at = config
                            .peers
                            .iter()
                            .find(|p| p.id == peer)
                            .and_then(|p| p.at.as_ref())
                            .map_or_else(String::new, |(quic, _)| quic.to_string());
                        say(&mut output, &format!("unresolved {peer} {at} {why}\n")).await?;
                    }
                }
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
            let resumed = matches!(
                outcome,
                CommandOutcome::Answered(Answer::Root(root::Outcome::Leaving))
            );
            match outcome {
                CommandOutcome::Answered(Answer::Root(
                    root::Outcome::Enlisted | root::Outcome::Joined | root::Outcome::Leaving,
                )) => {
                    // Ready before it says so: a check asked once `enlisted` is said finds the
                    // lock held.
                    enlisted = true;
                    live = layout.take_live().map_err(kept)?;
                    say(&mut output, "enlisted\n").await?;
                    if let Some(e) = &config.enlist {
                        living.state(&mut node, e.root, own, started_ns)?;
                    }
                }
                other => return Err(ProcessError::Enlist(format!("{other:?}"))),
            }
            // A node that was leaving when it stopped leaves on; one started to leave asks it now.
            if resumed {
                departure = Departure::Leaving;
                say(&mut output, "leaving\n").await?;
            } else if leave {
                departure.ask(&mut node, config.enlist.as_ref(), enlisted)?;
            }
        }
        if let Some(said) = departure.step(&mut node)? {
            say(&mut output, said).await?;
        }
        for at in (0..stated_gone.len()).rev() {
            let Some(&(ask, id)) = stated_gone.get(at) else {
                continue;
            };
            match ask {
                Some(ask) => {
                    if let Some(outcome) = node.take_command(ask) {
                        stated_gone.swap_remove(at);
                        say(&mut output, &format!("gone {id} {outcome:?}\n")).await?;
                    }
                }
                None => {
                    if let (Some(e), true) = (&config.enlist, node.command_room()) {
                        let gone = Command::Root(root::Command::Gone {
                            node: id,
                            // The entry that carries it gives it its time.
                            at_ns: 0,
                        });
                        let asked = node.command(e.root, gone, u64::MAX, Wait::Answered)?;
                        if let Some(held) = stated_gone.get_mut(at) {
                            held.0 = Some(asked);
                        }
                    }
                }
            }
        }
        // The members whose reach names a host: looked up as a peer's name is; those the cell no
        // longer admits, and that no line names, looked up no more.
        if let Some(cell) = node.cell() {
            let configured = |id: u64| {
                config.peers.iter().any(|p| p.id == id) || config.seeds.iter().any(|s| s.id == id)
            };
            names.retain(|id| {
                configured(id)
                    || cell.member(id).is_some_and(|row| {
                        matches!(row.state, MemberState::Serving | MemberState::Leaving)
                    })
            });
        }
        for (id, quic, plane) in node.take_reached() {
            match (Endpoint::parse(&quic), Endpoint::parse(&plane)) {
                (Some(quic), Some(plane)) => {
                    names.add(id, (quic, plane), hyper_rt::futures::now_ns());
                }
                _ => {
                    say(
                        &mut output,
                        &format!("unresolved {id} {quic} not HOST:PORT\n"),
                    )
                    .await?
                }
            }
        }
        // A readiness check holds the lock shared for the moment it looks: a take it refused is
        // asked again at the next turn.
        if enlisted && live.is_none() {
            live = layout.take_live().map_err(kept)?;
        }
        if let Some(e) = &config.enlist {
            living.take(&mut node, e.root, own, started_ns)?;
        }
        for (group, replaced) in node.take_replaced() {
            writeln!(
                lines,
                "replaced {group:x} {} {}",
                replaced.failed(),
                replaced.joining()
            )
            .map_err(wrote)?;
        }
        let repair = node.repair_status();
        if said_repair != Some(repair) {
            repair_line(&mut lines, &repair).map_err(wrote)?;
            said_repair = Some(repair);
        }
        standing_lines(&mut lines, &node, &mut said_standing).map_err(wrote)?;
        for ((group, _), was) in config.ranges.iter().zip(said_config.iter_mut()) {
            let Some(conf) = node.configuration(*group) else {
                continue;
            };
            let now = (conf.voters.clone(), conf.learners.clone());
            if was.as_ref() != Some(&now) {
                writeln!(lines, "config {group:x} {} {}", ids(&now.0), ids(&now.1))
                    .map_err(wrote)?;
                *was = Some(now);
            }
        }
        for (at, now) in node.links().enumerate() {
            if links.get(at) == Some(&now) {
                continue;
            }
            // A link to a named peer that ended: its name is looked up again (`crate::names`), as
            // the pod behind it may have come back at another address.
            let link_ended = links
                .get(at)
                .map_or(now.ended > 0, |was| now.ended > was.ended);
            if link_ended && names.names(now.peer) {
                names.ended(now.peer, hyper_rt::futures::now_ns());
            }
            link_line(&mut lines, &now).map_err(wrote)?;
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
            writeln!(lines, "moved {moved} {serve_ns}").map_err(wrote)?;
            said_moved = Some((moved, now));
        }
        // Whether the node waits on its own device, said as it changes, so a watcher knows it holds
        // still for its device and not by choice: a fact of the node, said once, where a status
        // for each range repeated it, and a log line after, at each of the liveness stream's
        // flushes (12 lines a flush on a node of five ranges; 120 to 560 a second from each
        // node while a peer of a cell of three was stopped, on this Mac).
        let held = node.held();
        if said_held != Some(held) {
            writeln!(lines, "held {}", u8::from(held)).map_err(wrote)?;
            said_held = Some(held);
        }
        // The longest poll the node's shard has run, said as it grows: a poll holds the one thread
        // every exchange the node carries waits on, so a poll past the transport's idle timeout
        // (`mantle_protocol::bounds::idle_timeout`) loses each exchange, and a watcher that sees
        // one lost tells from this line whether a poll held the node that long.
        let longest_ns = hyper_rt::futures::shard_counters()
            .ok_or_else(|| ProcessError::Runtime("the node's loop runs on no shard".into()))?
            .longest_step_ns;
        said_poll = longest_poll_line(&mut lines, said_poll, longest_ns).map_err(wrote)?;
        // Each shard's latest stall as the watchdog reported it, while the poll held the shard:
        // the node's own is read once its poll ends and the loop turns again, another's as it
        // happens, so a watcher that sees an exchange lost tells which shard was held, by which
        // task, and whose the time was.
        let now_ns = hyper_rt::futures::now_ns();
        for (shard, (holder, said)) in holders.iter().zip(said_stalls.iter_mut()).enumerate() {
            let report = holder.and_then(hyper_rt::watch::report);
            stall_line(
                &mut lines,
                (shard, said),
                report.as_ref(),
                (now_ns, stall_gap_ns),
            )
            .map_err(wrote)?;
        }
        let mut said = false;
        let mut room = (0u64, 0u64);
        for ((group, _), was) in config.ranges.iter().zip(told.iter_mut()) {
            let Some(now) = node.status(*group) else {
                continue;
            };
            room = (now.compactions, now.liveness_waits);
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
            status_line(&mut lines, *group, &now).map_err(wrote)?;
            *was = Some(now);
            said = true;
        }
        // The log's refusals for room, said as they move: the node compacts its ranges only when
        // the log refuses a write for room (node.md §6.2), so a watcher sees the log fill and the
        // node go on.
        if said_room != Some(room) {
            writeln!(lines, "room {} {}", room.0, room.1).map_err(wrote)?;
            said_room = Some(room);
        }
        // The log the ranges share, once after the statuses that moved.
        if said {
            // What the turn wrote is said before a failure ends the process, as each line was.
            let log = match node.log_stats(stats.take()) {
                Ok(log) => log,
                Err(e) => {
                    say(&mut output, &lines).await?;
                    return Err(e.into());
                }
            };
            let flushing = log.flushing_since.map_or(0, |since| {
                log.at.saturating_duration_since(since).as_nanos()
            });
            let longest = log.commit_wait.quantile(1_000_000).unwrap_or(0);
            writeln!(lines, "log {flushing} {longest}").map_err(wrote)?;
            stats = Some(log);
        }
        // The orderly stop: each range whose lead it begins to hand over, and to whom (again, after
        // its core gave a transfer up); each it handed over, to the member that then led it; and
        // its end, after which the node's log and volumes close as it is dropped.
        for (group, to) in node.take_handing() {
            writeln!(lines, "handing {group:x} {to}").map_err(wrote)?;
        }
        for (group, leader) in node.take_handed() {
            writeln!(lines, "handed {group:x} {leader}").map_err(wrote)?;
        }
        if !lines.is_empty() {
            say(&mut output, &lines).await?;
            lines.clear();
        }
        if node.stopped() {
            // Said with the sleeps of the node's loop that found every timer taken, none where
            // the shard holds one for each wait the loop holds at once ([`TIMERS`]).
            let waits = hyper_rt::futures::shard_counters()
                .ok_or_else(|| ProcessError::Runtime("the node's loop runs on no shard".into()))?
                .timer_waits;
            say(&mut output, &format!("stopped {waits}\n")).await?;
            // Nothing the node holds is owed to the cell: its log and volumes close, and then the
            // stop is kept, with the node's time run, so its next start counts no crash.
            let ran_ns = hyper_rt::futures::now_ns().saturating_sub(started_ns);
            drop(node);
            let stopped = Stopped {
                run,
                exposure_ns: own.exposure_ns.saturating_add(ran_ns),
                stops: last_stop.stops.saturating_add(1),
            };
            layout
                .keep_stopped(stopped)
                .map_err(|e| ProcessError::History(e.to_string()))?;
            return Ok(());
        }
    }
}

/// Where a node's leave stands (docs/design/membership.md §4).
enum Departure {
    Staying,
    /// Asked of its root range (the group) while the node's commands had no room: asked once
    /// they have.
    Wanting(u128),
    /// Asked of the root range, not yet answered.
    Asking(CommandAsk),
    /// The root range holds it leaving: it waits until its volumes are retired and it is removed.
    Leaving,
    /// Removed, as its own replica of the root range applied it: it said `left`, and serves on,
    /// admitted as the root range's learner, so the root range's leader hears that its storage
    /// holds the commit that removed it, which the leader waits for before it drops the learner
    /// (membership.md §4 step 5); a leader elected meanwhile hears it from it again. A peer's
    /// refusal of its identity says the cell has dropped it, the leader having heard: each
    /// peer's link endings as counted at its removal, so a refusal from before does not count.
    Removed(Vec<(u64, u64)>),
    /// Removed and dropped: its orderly stop runs, and the process ends with it.
    Left,
    /// A node with no standing whose volumes are leaving: repair drains them while it serves.
    Draining,
}

impl Departure {
    /// Asks the node's leave of the root range of `enlist`, once the node is enlisted: asked
    /// before that, at the enlistment's answer.
    fn ask<E: Engine + 'static>(
        &mut self,
        node: &mut Node<E>,
        enlist: Option<&Enlisting>,
        enlisted: bool,
    ) -> Result<(), ProcessError> {
        let Some(e) = enlist else {
            return Err(ProcessError::Input(
                "leave".into(),
                "a node that hosts no root range is a member of no cell",
            ));
        };
        if !enlisted || !matches!(self, Self::Staying) {
            return Ok(());
        }
        *self = Self::Wanting(e.root);
        self.send(node)
    }

    /// Asks the root range the leave it wants, once the node's commands have room
    /// (`Node::command_room`): before then it stays wanted.
    fn send<E: Engine + 'static>(&mut self, node: &mut Node<E>) -> Result<(), ProcessError> {
        let Self::Wanting(root) = *self else {
            return Ok(());
        };
        if !node.command_room() {
            return Ok(());
        }
        let leave = Command::Root(root::Command::Leave {
            node: node.id(),
            // The entry that carries it gives it its time.
            at_ns: 0,
        });
        *self = Self::Asking(node.command(root, leave, u64::MAX, Wait::Answered)?);
        Ok(())
    }

    /// A turn's step: the leave's answer taken, and the node's removal seen in its own replica of
    /// the root range, which begins its orderly stop. What to say, if anything.
    fn step<E: Engine + 'static>(
        &mut self,
        node: &mut Node<E>,
    ) -> Result<Option<&'static str>, ProcessError> {
        match self {
            Self::Wanting(_) => {
                self.send(node)?;
                Ok(None)
            }
            Self::Asking(ask) => match node.take_command(*ask) {
                Some(CommandOutcome::Answered(Answer::Root(root::Outcome::Leaving))) => {
                    *self = Self::Leaving;
                    Ok(Some("leaving\n"))
                }
                // A node enlisted with no standing, a cell made before grants: its volumes are
                // leaving and repair drains them, and it serves on, no removal to wait for.
                Some(CommandOutcome::Answered(Answer::Root(root::Outcome::Kept))) => {
                    *self = Self::Draining;
                    Ok(Some("leaving\n"))
                }
                Some(other) => Err(ProcessError::Enlist(format!("the leave: {other:?}"))),
                None => Ok(None),
            },
            Self::Leaving => {
                let removed = node
                    .cell()
                    .and_then(|cell| cell.member(node.id()))
                    .is_none_or(|row| row.state == MemberState::Removed);
                if removed {
                    *self = Self::Removed(node.links().map(|l| (l.peer, l.ended)).collect());
                    Ok(Some("left\n"))
                } else {
                    Ok(None)
                }
            }
            Self::Removed(ended) => {
                let refused = node.links().any(|l| {
                    l.refusal == Some(Refusal::Identity)
                        && ended
                            .iter()
                            .find(|(peer, _)| *peer == l.peer)
                            .is_none_or(|(_, then)| l.ended > *then)
                });
                if refused {
                    *self = Self::Left;
                    node.stop();
                    Ok(Some("stopping\n"))
                } else {
                    Ok(None)
                }
            }
            Self::Staying | Self::Left | Self::Draining => Ok(None),
        }
    }
}

/// Where a node's peers dial its endpoint bound at `bound`, `HOST:PORT`: its stated reach on the
/// bound port, else the bound address where its peers can dial it (membership.md §3 step 2).
fn reached_at(reach: Option<&str>, bound: SocketAddr) -> Result<String, ProcessError> {
    match reach {
        Some(host) if host.contains(':') => Ok(format!("[{host}]:{}", bound.port())),
        Some(host) => Ok(format!("{host}:{}", bound.port())),
        None if !bound.ip().is_unspecified() => Ok(bound.to_string()),
        None => Err(ProcessError::Value(
            "reach",
            format!("the node binds {bound}, every address, and states no host its peers dial"),
        )),
    }
}

/// The member a `gone ID` line names, an operator's statement that it is gone for good
/// (membership.md §5); none for another line.
fn gone_line(line: &[u8]) -> Result<Option<u64>, ProcessError> {
    let text = String::from_utf8_lossy(line).trim().to_owned();
    let mut words = text.split_whitespace();
    match (words.next(), words.next(), words.next()) {
        (Some("gone"), Some(id), None) => id
            .parse()
            .map(Some)
            .map_err(|_| ProcessError::Input(text.clone(), "gone ID: a member's id")),
        _ => Ok(None),
    }
}

/// Each member's `standing ID STATE ABSENT_NS` line, written onto `lines`, for each whose state or
/// recorded absence moved since `said`, as this node's member of the root range holds it.
fn standing_lines<E: Engine + 'static>(
    lines: &mut String,
    node: &Node<E>,
    said: &mut Vec<(u64, MemberState, u64)>,
) -> std::fmt::Result {
    let Some(cell) = node.cell() else {
        return Ok(());
    };
    // Away since when, as the cell's presence keeps it (membership.md §8): 0 while it is seen.
    let away = |id: u64| {
        cell.presence(id)
            .filter(|p| !p.present)
            .map_or(0, |p| p.since_ns)
    };
    for (id, row) in cell.members() {
        let now = (*id, row.state, away(*id));
        if said.contains(&now) {
            continue;
        }
        let state = match row.state {
            MemberState::Serving => "serving",
            MemberState::Leaving => "leaving",
            MemberState::Gone => "gone",
            MemberState::Removed => "removed",
        };
        writeln!(lines, "standing {id} {state} {}", now.2)?;
    }
    // Members delisted: said once, as no state.
    for (id, ..) in said.iter() {
        if cell.member(*id).is_none() {
            writeln!(lines, "standing {id} delisted 0")?;
        }
    }
    said.clear();
    said.extend(
        cell.members()
            .iter()
            .map(|(id, row)| (*id, row.state, away(*id))),
    );
    Ok(())
}

/// Ids, comma-separated, `-` for none.
fn ids(members: &[u64]) -> String {
    if members.is_empty() {
        return "-".to_owned();
    }
    let ids: Vec<String> = members.iter().map(u64::to_string).collect();
    ids.join(",")
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
/// `log2(t / first)` times, the first statement's own wait the unit; one statement out at a time,
/// asked only while the node's commands have room.
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
        // The node's commands at the most it keeps (`command_room`): the statement stays due,
        // from now, and a later turn asks it, as the node's other commands wait for room.
        if !node.command_room() {
            self.next_ns = Some(self.next_ns.map_or(ran_ns, |next| next.min(ran_ns)));
            return Ok(());
        }
        let lived = Command::Root(root::Command::Lived(Lived {
            node: node.id(),
            exposure_ns: own.exposure_ns.saturating_add(ran_ns),
            crashes: own.crashes,
        }));
        self.out = Some(node.command(root, lived, u64::MAX, Wait::Answered)?);
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
/// replacement in each of `ranges`. (`stop`, the orderly stop a signal asks, and `chunks` and
/// `counts`, whose answers the node says, the loop takes itself.)
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
    say(output, "stopping\n").await
}

/// A peer's `link` line, written onto `lines`.
fn link_line(lines: &mut String, now: &Link) -> std::fmt::Result {
    write!(
        lines,
        "link {} {} {} {} {} {} ",
        now.peer,
        if now.keyed { "keyed" } else { "unkeyed" },
        now.dials,
        now.ended,
        now.suspected,
        if now.suspect { "suspect" } else { "trusted" }
    )?;
    match now.refusal {
        Some(refusal) => writeln!(lines, "{refusal:?}"),
        None => writeln!(lines, "none"),
    }
}

/// Range `group`'s `status` line, written onto `lines`.
fn status_line(lines: &mut String, group: u128, now: &Status) -> std::fmt::Result {
    let patience = now.patience.map_or(0, |p| p.as_nanos());
    let reconnect = now.reconnect.map_or(0, |r| r.as_nanos());
    let measuring = now.measuring_for.map_or(0, |m| m.as_nanos());
    let asking = now.asking.map_or(0, |a| a.as_nanos());
    writeln!(
        lines,
        "status {group:x} {} {} {} {} {patience} {reconnect} {measuring} {asking} {}",
        now.term, now.leader, now.commit, now.applied, now.measuring
    )
}

/// `longest-poll NS`, written onto `lines` where `longest_ns`, the longest poll the node's shard
/// has run (hyper-rt's `Counters::longest_step_ns`: one task's poll by the wall clock, since the
/// process began), is past the one last `said`: the longest said now.
fn longest_poll_line(
    lines: &mut String,
    said: u64,
    longest_ns: u64,
) -> Result<u64, std::fmt::Error> {
    if longest_ns <= said {
        return Ok(said);
    }
    writeln!(lines, "longest-poll {longest_ns}")?;
    Ok(longest_ns)
}

/// The parts of the transport's serve period (`mantle_protocol::bounds::idle_timeout`) in each of
/// which the node says one stall of a shard at most ([`stall_line`]): two. The watchdog reports a
/// stall within twice its limit of the poll's start (hyper-rt `watch`), so it reports every stall
/// as long as the serve period, the one that loses each exchange its shard carries, only where its
/// limit is at most half the period; such a stall is then its shard's latest report for at least
/// the period's other half, as the shard runs no other poll while it lasts. A shard said at most
/// once each half period has each of those said, and at most two lines each period whatever it
/// reports (docs/design/constants.md).
const STALL_LINES_A_PERIOD: u64 = 2;

/// What the node said last of one of its shards' stalls: the number of the watchdog's report it
/// said, none before the first, and when, on the node's shard's clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SaidStall {
    number: u64,
    at_ns: Option<u64>,
}

/// `stall SHARD SITE LABEL HELD_NS CPU_NS WHOSE MISSED`, written onto `lines` where `report`, the
/// latest stall hyper-rt's watchdog reported on the node's shard `shard` (its first the node's
/// own), is newer than the one `said`, and `gap_ns` has passed since that was said, at `now_ns`:
/// the shard; the spawn site of the task whose poll held it, `FILE:LINE`; its label; how long the
/// poll had held the shard when reported and the CPU its thread ran meanwhile, in nanoseconds;
/// whose the time was (`Whose::as_str`); and the reports since the one said that this one
/// replaced unsaid. A site, label or CPU time the report lacks is `-`; a site's or label's
/// whitespace and `%` are percent-encoded, so each stays one word.
fn stall_line(
    lines: &mut String,
    (shard, said): (usize, &mut SaidStall),
    report: Option<&hyper_rt::watch::StallReport>,
    (now_ns, gap_ns): (u64, u64),
) -> std::fmt::Result {
    let Some(report) = report.filter(|report| report.number > said.number) else {
        return Ok(());
    };
    if said
        .at_ns
        .is_some_and(|at| now_ns.saturating_sub(at) < gap_ns)
    {
        return Ok(());
    }
    write!(lines, "stall {shard} ")?;
    match report.site {
        Some(site) => {
            word(lines, site.file())?;
            write!(lines, ":{}", site.line())?;
        }
        None => lines.push('-'),
    }
    lines.push(' ');
    match report.label {
        Some(label) => word(lines, label.name)?,
        None => lines.push('-'),
    }
    write!(lines, " {} ", report.held_ns)?;
    match report.cpu_ns {
        Some(cpu_ns) => write!(lines, "{cpu_ns}")?,
        None => lines.push('-'),
    }
    let missed = report.number.saturating_sub(said.number).saturating_sub(1);
    writeln!(lines, " {} {missed}", report.whose.as_str())?;
    *said = SaidStall {
        number: report.number,
        at_ns: Some(now_ns),
    };
    Ok(())
}

/// The stalls hyper-rt's watchdog counted on each of the node's shards (`watch::stalls`), its own
/// first, as the `counts` answer says them, names and values: `stall_shards`, then each shard's
/// `stall_episodes`, the polls seen held past its limit since the shard began, `-` for a shard
/// gone. Bounded by the runtime's shards.
pub fn stall_counts(said: &mut String, episodes: &[Option<u64>]) -> std::fmt::Result {
    write!(said, " stall_shards {}", episodes.len())?;
    for shard in episodes {
        match shard {
            Some(episodes) => write!(said, " stall_episodes {episodes}")?,
            None => said.push_str(" stall_episodes -"),
        }
    }
    Ok(())
}

/// The cell's repair as this node runs it (docs/design/repair.md §5): `repair LEADING SETTLED
/// ROOT_APPLIED BLOCK_APPLIED REBUILT TIMING COMMANDS UNPLACED UNREADABLE CORRUPT WAITING SHORT LOST
/// TAKERS RUNNING`, each flag 0 or 1, said each time it changes.
fn repair_line(lines: &mut String, r: &crate::node::RepairStatus) -> Result<(), std::fmt::Error> {
    writeln!(
        lines,
        "repair {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
        u8::from(r.leading),
        u8::from(r.settled),
        r.root_applied,
        r.block_applied,
        r.rebuilt,
        r.timing,
        r.commands,
        r.unplaced,
        r.unreadable,
        r.corrupt,
        r.waiting,
        r.short,
        r.lost,
        r.takers,
        r.running,
    )
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

    /// A `counts` answer's write path says the log's and the proposals' counts as given, then the
    /// commit waits that ended since the last answer alone: their count, and the quantiles a
    /// histogram holding only them gives, where the log's histogram from its opening, a slow wait
    /// in every three, gives another; an answer with none since says dashes. Each answer sets where
    /// the next counts from, in the one histogram's room.
    #[test]
    fn a_counts_answer_says_the_commit_waits_since_the_last_answer() {
        let mut waited = Histogram::new();
        let mut first = Histogram::new();
        for _ in 0..50 {
            waited.record(5_000_000);
            first.record(5_000_000);
        }
        let mut asked = Box::new(Histogram::new());
        let quantiles =
            |h: &Histogram| [500_000, 990_000, 1_000_000].map(|ppm| h.quantile(ppm).unwrap());
        let mut said = String::new();
        write_path_counts(&mut said, (3, 9, 6), (2, 5), &waited, &mut asked).unwrap();
        let [p50, p99, max] = quantiles(&first);
        assert_eq!(
            said,
            format!(
                " frames 3 updates 9 flushes 6 entries 2 commands 5 waits 50 wait_p50_ns {p50} \
                 wait_p99_ns {p99} wait_max_ns {max}"
            )
        );
        assert_eq!(
            p50, 5_242_879,
            "the most 5 ms's bucket holds, an eighth of a doubling"
        );

        let mut second = Histogram::new();
        for us in 1..=100u64 {
            waited.record(us * 1_000);
            second.record(us * 1_000);
        }
        let mut said = String::new();
        write_path_counts(&mut said, (4, 12, 8), (3, 9), &waited, &mut asked).unwrap();
        let [p50, p99, max] = quantiles(&second);
        assert_eq!(
            said,
            format!(
                " frames 4 updates 12 flushes 8 entries 3 commands 9 waits 100 wait_p50_ns {p50} \
                 wait_p99_ns {p99} wait_max_ns {max}"
            )
        );
        assert_eq!((p50, p99), (53_247, 106_495));
        assert_ne!(
            quantiles(&waited),
            quantiles(&second),
            "the opening's waits are not this answer's"
        );

        let mut said = String::new();
        write_path_counts(&mut said, (4, 12, 8), (3, 9), &waited, &mut asked).unwrap();
        assert_eq!(
            said,
            " frames 4 updates 12 flushes 8 entries 3 commands 9 waits 0 wait_p50_ns - wait_p99_ns - \
             wait_max_ns -"
        );
        assert_eq!(*asked, waited);
    }

    /// A `repair` line says each count in its place, the domains its last pass found taking chunks
    /// among them: the fact a test of a joining node waits on, which the root range's commit the
    /// test last heard can predate (repair.rs's domain spread onto a joiner, CI run 43992405910).
    #[test]
    fn a_repair_line_says_the_domains_its_last_pass_found_taking_chunks() {
        let status = crate::node::RepairStatus {
            leading: true,
            settled: true,
            root_applied: 27,
            block_applied: 17,
            rebuilt: 6,
            timing: 1,
            commands: 2,
            unplaced: 3,
            unreadable: 4,
            corrupt: 5,
            waiting: 7,
            short: 8,
            lost: 9,
            takers: 3,
            running: 10,
            ended_ns: 11,
        };
        let mut lines = String::new();
        repair_line(&mut lines, &status).unwrap();
        assert_eq!(lines, "repair 1 1 27 17 6 1 2 3 4 5 7 8 9 3 10\n");
    }

    /// A turn's link and status lines, written into the buffer the serve loop keeps: byte for byte
    /// the lines each said from a `String` of its own, and once the buffer has grown nothing is
    /// asked of the allocator, where each line was a `String` and a flush's oneshot (research/71,
    /// step 5).
    #[test]
    fn a_turns_lines_are_written_into_the_kept_buffer_as_each_was_said() {
        let link = Link {
            peer: 3,
            keyed: true,
            dials: 4,
            ended: 1,
            suspected: 2,
            suspect: false,
            refusal: Some(hyper_transport::Refusal::Closed),
        };
        let quiet = Link {
            keyed: false,
            suspect: true,
            refusal: None,
            ..link
        };
        let status = Status {
            term: 7,
            leader: 2,
            commit: 40,
            applied: 39,
            heartbeats: 0,
            judged: true,
            measuring: 0,
            measuring_for: Some(Duration::from_millis(250)),
            patience: Some(Duration::from_millis(1_500)),
            snapshots_sent: 0,
            snapshots_taken: 0,
            stalled: false,
            liveness_waits: 0,
            compactions: 0,
            in_flight: 0,
            writes: hyper_durable::Writes::default(),
            fenced: false,
            uncertain: false,
            sends: crate::node::Sends::default(),
            received: crate::node::Received::default(),
            reconnect: None,
            asking: Some(Duration::from_nanos(9)),
            reads_held: 0,
            reads_confirming: 0,
            reading: 0,
            commanded: 0,
        };
        let turn = |lines: &mut String| {
            lines.clear();
            link_line(lines, &link).unwrap();
            link_line(lines, &quiet).unwrap();
            status_line(lines, 0x2a, &status).unwrap();
        };
        let mut lines = String::new();
        turn(&mut lines);
        assert_eq!(
            lines,
            "link 3 keyed 4 1 2 trusted Closed\n\
             link 3 unkeyed 4 1 2 suspect none\n\
             status 2a 7 2 40 39 1500000000 0 250000000 9 0\n"
        );
        hyper_measure::alloc::begin();
        turn(&mut lines);
        let counts = hyper_measure::alloc::end();
        assert_eq!(
            (counts.allocations, counts.reallocations, counts.bytes),
            (0, 0, 0)
        );
    }

    /// A status said because the heartbeats taken from peers not yet judged moved, and nothing else
    /// did, says what moved: a watcher that compares a range's lines, as one must to pass over the
    /// status a process started again says first, sees the node measure its links. Before, both
    /// lines read alike while a peer's interval stayed: a node started again on a loaded host took
    /// 28 heartbeats from each peer over 23 s, saying a status at each, and the cell's watch, finding
    /// each the same as the last, judged it still for its quiet period (CI windows-x86_64, run
    /// 43231201498; jupiter, 7 of 86 runs under load).
    #[test]
    fn a_status_said_for_heartbeats_taken_while_measuring_says_them() {
        let status = |measuring| Status {
            term: 2,
            leader: 3,
            commit: 8,
            applied: 9,
            heartbeats: measuring,
            judged: false,
            measuring,
            measuring_for: Some(Duration::from_nanos(822_549_396)),
            patience: None,
            snapshots_sent: 0,
            snapshots_taken: 0,
            stalled: false,
            liveness_waits: 0,
            compactions: 0,
            in_flight: 0,
            writes: hyper_durable::Writes::default(),
            fenced: false,
            uncertain: false,
            sends: crate::node::Sends::default(),
            received: crate::node::Received::default(),
            reconnect: Some(Duration::from_nanos(4_003_992_800)),
            asking: None,
            reads_held: 0,
            reads_confirming: 0,
            reading: 0,
            commanded: 0,
        };
        let (mut before, mut after) = (String::new(), String::new());
        status_line(&mut before, 0x2a, &status(54)).unwrap();
        status_line(&mut after, 0x2a, &status(56)).unwrap();
        assert_ne!(before, after, "two statuses of a node measuring read alike");
    }

    /// The longest poll is said as it grows and only then: a turn whose longest is no longer than
    /// the one said says nothing, and one past it says it, `longest-poll NS`, which is then the one
    /// said. The longest a process starts from, none, says nothing.
    #[test]
    fn the_longest_poll_is_said_as_it_grows() {
        let mut lines = String::new();
        assert_eq!(longest_poll_line(&mut lines, 0, 0).unwrap(), 0);
        assert_eq!(longest_poll_line(&mut lines, 0, 4_200).unwrap(), 4_200);
        assert_eq!(longest_poll_line(&mut lines, 4_200, 4_200).unwrap(), 4_200);
        assert_eq!(
            longest_poll_line(&mut lines, 4_200, 3_072_000_001).unwrap(),
            3_072_000_001
        );
        assert_eq!(lines, "longest-poll 4200\nlongest-poll 3072000001\n");
    }

    /// A report of the watchdog's, numbered `number`, on a task spawned at `site` and labelled
    /// `label`, held 3.072 s and a nanosecond, past the transport's idle timeout.
    fn stalled(
        number: u64,
        (site, label): (
            Option<&'static std::panic::Location<'static>>,
            Option<&'static hyper_rt::slow::Label>,
        ),
        (cpu_ns, whose): (Option<u64>, hyper_rt::watch::Whose),
    ) -> hyper_rt::watch::StallReport {
        hyper_rt::watch::StallReport {
            number,
            task: hyper_rt::TaskId(hyper_rt::mem::Encoded::from_word(0)),
            site,
            label,
            held_ns: 3_072_000_001,
            cpu_ns,
            whose,
        }
    }

    /// A stall is said once, as its report is new, with its shard, its task's spawn site
    /// `FILE:LINE`, its label, its hold and CPU time in nanoseconds, whose the time was and the
    /// reports it replaced unsaid; what the report lacks is `-`, and a site's or label's whitespace
    /// is percent-encoded so each stays one word. A report already said, or none, says nothing.
    #[test]
    fn a_stall_is_said_once_with_its_task_hold_and_whose() {
        static LABEL: hyper_rt::slow::Label = hyper_rt::slow::Label::new("Put Object");
        let site = std::panic::Location::caller();
        let at = format!("{}:{}", site.file(), site.line());
        let mut lines = String::new();
        let mut said = SaidStall::default();
        stall_line(&mut lines, (3, &mut said), None, (0, 1)).unwrap();
        assert_eq!(lines, "");
        let task = stalled(
            1,
            (Some(site), Some(&LABEL)),
            (Some(2_000_000), hyper_rt::watch::Whose::Task),
        );
        stall_line(&mut lines, (3, &mut said), Some(&task), (10, 1)).unwrap();
        assert_eq!(
            lines,
            format!("stall 3 {at} Put%20Object 3072000001 2000000 task 0\n")
        );
        // Said already: nothing, however long after.
        stall_line(&mut lines, (3, &mut said), Some(&task), (u64::MAX, 1)).unwrap();
        assert_eq!(lines.lines().count(), 1);
        // The fourth report, the second and third replaced unsaid; no site, label or CPU time.
        lines.clear();
        let host = stalled(4, (None, None), (None, hyper_rt::watch::Whose::Host));
        stall_line(&mut lines, (0, &mut said), Some(&host), (20, 1)).unwrap();
        assert_eq!(lines, "stall 0 - - 3072000001 - host 2\n");
        assert_eq!(
            said,
            SaidStall {
                number: 4,
                at_ns: Some(20)
            }
        );
    }

    /// However many stalls a shard reports, the node says at most one line of them each half
    /// serve period: a watchdog reporting a stall at every turn of a node turning every
    /// millisecond for a minute has its shard said at most once a gap, the first at once, and
    /// each line's missed count says the reports it skipped, so the lines and the skipped sum to
    /// every report but those after the last line.
    #[test]
    fn stall_lines_keep_to_their_rate_under_many_stalls() {
        let serve_ns = u64::try_from(mantle_protocol::bounds::idle_timeout().as_nanos()).unwrap();
        let gap_ns = serve_ns / STALL_LINES_A_PERIOD;
        let span_ns = 60_000_000_000u64;
        let turn_ns = 1_000_000u64;
        let mut lines = String::new();
        let mut said = SaidStall::default();
        let mut number = 0;
        let mut now_ns = 0;
        while now_ns < span_ns {
            number += 1;
            let report = stalled(number, (None, None), (None, hyper_rt::watch::Whose::Task));
            stall_line(&mut lines, (1, &mut said), Some(&report), (now_ns, gap_ns)).unwrap();
            now_ns += turn_ns;
        }
        let said_lines = lines.lines().count() as u64;
        assert!(said_lines > 1);
        assert!(
            said_lines <= span_ns / gap_ns + 1,
            "{said_lines} lines in {span_ns} ns at one each {gap_ns} ns"
        );
        let missed: u64 = lines
            .lines()
            .map(|line| line.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
            .sum();
        assert_eq!(said_lines + missed, said.number);
        assert!(number - said.number <= gap_ns / turn_ns);
    }

    /// The `counts` answer's stalls are each shard's, its count or `-` for a shard gone, after
    /// how many shards it says.
    #[test]
    fn the_stall_counts_are_said_a_shard_each() {
        let mut said = "counts uncounted".to_owned();
        stall_counts(&mut said, &[Some(0), Some(7), None]).unwrap();
        assert_eq!(
            said,
            "counts uncounted stall_shards 3 stall_episodes 0 stall_episodes 7 stall_episodes -"
        );
    }

    /// The table of what held the shard is said entry by entry, each field a name and one word,
    /// the unkeyed entry's site and label `-`, and a site's or a label's whitespace and `%`
    /// percent-encoded so each stays one word.
    #[test]
    fn the_slow_poll_table_is_said_as_names_and_words() {
        static OP: hyper_rt::slow::Label = hyper_rt::slow::Label::new("Get Object%");
        let site = std::panic::Location::caller();
        let polls = [
            hyper_rt::slow::SlowPoll {
                site: Some(site),
                label: Some(&OP),
                polls: 3,
                held_ns: 900,
                longest_ns: 500,
                task_polls: 2,
                host_polls: 1,
            },
            hyper_rt::slow::SlowPoll {
                site: None,
                label: None,
                polls: 1,
                held_ns: 7,
                longest_ns: 7,
                task_polls: 0,
                host_polls: 0,
            },
        ];
        let mut said = String::new();
        slow_poll_counts(&mut said, &polls).unwrap();
        let expected = format!(
            " slow_entries 2 slow_site {}:{}:{} slow_label Get%20Object%25 slow_polls 3 \
             slow_held_ns 900 slow_longest_ns 500 slow_task 2 slow_host 1 slow_site - slow_label - \
             slow_polls 1 slow_held_ns 7 slow_longest_ns 7 slow_task 0 slow_host 0",
            site.file(),
            site.line(),
            site.column()
        );
        assert_eq!(said, expected);
        assert_eq!(said.split_whitespace().count() % 2, 0, "names and values");
        let mut none = String::new();
        slow_poll_counts(&mut none, &[]).unwrap();
        assert_eq!(none, " slow_entries 0");
    }

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

    /// `range_shards` is a mode its operator states, `first` or `spread`, unstated where the file
    /// names it not, and any other word refused naming the key (node.md §2.9).
    #[test]
    fn the_shards_a_nodes_ranges_take_are_first_or_spread() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config");
        let base = "id = 1\ndir = .\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\nauthority = a.der\n\
                    certificate = n.der\nkey = n.key\nvoters = 1\nrange = 1 root\n";
        std::fs::write(&config, base).unwrap();
        assert_eq!(read_stated(&config).unwrap().range_shards, None);
        for (word, shards) in [
            ("first", RangeShards::First),
            ("spread", RangeShards::Spread),
        ] {
            std::fs::write(&config, format!("{base}range_shards = {word}\n")).unwrap();
            assert_eq!(read_stated(&config).unwrap().range_shards, Some(shards));
            assert_eq!(shards.to_string(), word);
        }
        std::fs::write(&config, format!("{base}range_shards = every\n")).unwrap();
        assert!(matches!(
            read_stated(&config),
            Err(ProcessError::Value("range_shards", _))
        ));
        assert_eq!(RangeShards::default(), RangeShards::First);
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
                 volume = {disks} 1073741824\nvolume = data 4096\nvolume = {disks} spare\n\
                 volume = spare data spare 2048\nvolume = my spare/ 512\ndomain = a\n\
                 root.max_nodes = 1\nroot.max_volumes = 2\n"
            ),
        )
        .unwrap();
        let stated = read_stated(&config).unwrap();
        assert_eq!(
            stated.volumes,
            vec![
                (PathBuf::from(disks), VolumeBytes::Stated(1 << 30)),
                (dir.path().join("data"), VolumeBytes::Stated(4096)),
                (PathBuf::from(disks), VolumeBytes::Spare { keep: 0 }),
                (
                    dir.path().join("spare data"),
                    VolumeBytes::Spare { keep: 2048 }
                ),
                (dir.path().join("my spare"), VolumeBytes::Stated(512)),
            ]
        );
        for bad in [
            "volume = 1024",
            "volume = /d",
            "volume = /d big",
            "volume = spare",
            "volume = /d spare big",
        ] {
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

    /// A runtime of `shards` shards, each with `tasks` task slots, as the listener's tests build one.
    fn started(shards: u16, tasks: usize) -> Runtime {
        Runtime::start(&RuntimeConfig {
            shards,
            tasks_per_shard: tasks,
            timers_per_shard: 1,
            interests_per_shard: hyper_rt::runtime::interests_for(tasks),
            ring_entries: 1,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            batch: 1,
            pin: false,
            cores: Vec::new(),
            page_bytes: usize::try_from(hyper_rt::machine::facts::Facts::query().page.base)
                .unwrap(),
            spin_ns: 0,
            wake_tracking: None,
            latency_objective_ns: None,
        })
        .unwrap()
    }

    /// Sets its flag as it is dropped.
    struct Dropped(std::sync::Arc<std::sync::atomic::AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// The node's task is spawned Critical, so its consensus, log and transport go ahead of the
    /// connections its shard serves; what it spawns, those connections among them, stays Normal.
    #[test]
    fn the_nodes_task_is_spawned_critical_and_what_it_spawns_normal() {
        use hyper_rt::coop::Class;
        let ran = run_on_first_shard(started(1, 3), || async {
            let (tell, heard) = hyper_rt::sync::oneshot().unwrap();
            hyper_rt::futures::spawn_detached(async move {
                let _ = tell.send(hyper_rt::coop::class());
            })
            .unwrap();
            let spawned = heard.await.ok().flatten();
            (hyper_rt::coop::class(), spawned)
        })
        .unwrap();
        assert_eq!(ran, (Some(Class::Critical), Some(Class::Normal)));
    }

    /// The node's task runs on its runtime's first shard, made there, so it need not be `Send`; and
    /// what it leaves running is cancelled and joined before its output is handed on, the runtime
    /// shut down: nothing it spawned outlives the call.
    #[test]
    fn the_node_runs_on_its_runtimes_first_shard_and_nothing_it_leaves_outlives_the_runtime() {
        // The launcher, the node's task and the task it leaves running.
        let rt = started(2, 3);
        let first = rt.shard_ids()[0];
        let left = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = Dropped(std::sync::Arc::clone(&left));
        let ran = run_on_first_shard(rt, move || async move {
            hyper_rt::futures::spawn_detached(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            })
            .unwrap();
            // Held across an await: the task is not `Send`.
            let here = std::rc::Rc::new(hyper_rt::futures::shard_id());
            hyper_rt::futures::yield_now().await;
            *here
        })
        .unwrap();
        assert_eq!(ran, Some(first));
        assert!(left.load(std::sync::atomic::Ordering::Acquire));
    }

    /// A shard of one task slot refuses the node's task, the launcher holding the slot as it
    /// spawns it, and the refusal is the call's typed failure: the node's runtime counts a slot for
    /// each.
    #[test]
    fn a_shard_with_no_slot_beside_the_launchers_refuses_the_node_typed() {
        let ran = run_on_first_shard(started(1, 1), || async {});
        assert!(
            matches!(&ran, Err(ProcessError::Runtime(e)) if e.contains("TooManyTasks")),
            "{ran:?}"
        );
    }

    /// A node's task that unwinds ends the wait for its output, which would otherwise never end,
    /// with a typed failure.
    #[test]
    fn a_node_task_that_unwinds_ends_the_wait_typed() {
        let ran: Result<(), ProcessError> = run_on_first_shard(started(1, 2), || async {
            panic!("the node's task unwinds");
        });
        assert!(
            matches!(&ran, Err(ProcessError::Runtime(e)) if e.contains("without its output")),
            "{ran:?}"
        );
    }
}
