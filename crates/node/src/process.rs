//! A node as a process (`mantle node`, docs/design/node.md §1): its configuration, stated in a
//! file, and its run on a runtime of one shard, the node's metadata shard (§1.2), whose replicas
//! apply through the engine's synchronous calls.
//!
//! **The file** states every number the node is run with, one `key = value` a line (`#` begins a
//! comment): nothing is assumed for the operator, and a key it does not know is refused at its
//! line ([`KEYS`], [`REPEATED`]). A `peer` line, repeated, names a peer's id, its
//! role (`node`, `gateway` or `client`) and its certificate's file. Certificates and the key are
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
//! being read, the operations it runs at once, the buckets an owner may have, the rows a bucket's
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
//! **Its talk** is lines. On standard output: `bound QUIC PLANE` once the node listens, and
//! `volume ID` for each volume it holds, its 128-bit id in hex; then, for each range it hosts,
//! `status GROUP TERM LEADER COMMIT APPLIED PATIENCE_NS RECONNECT_NS MEASURING_NS HELD`, the group
//! in hex, each time one of the term, leader, commit or applied index
//! moves, its redials to a peer back off further or start over, or the heartbeats it takes from
//! peers not yet judged move (`Status::measuring`): how long the node states it may go
//! without moving
//! while its peers live (`Status::patience`, 0 until measured), how long a link to a peer it dials
//! may go without moving should that peer fall silent (`Status::reconnect`, 0 when it dials none);
//! while
//! some peer is not yet judged, the longest its stream's heartbeats go between
//! (`Status::measuring_for`, else 0); and 1 while the node waits on its own device, said each time
//! that changes (`Status::held`, else 0). After each status, `log FLUSHING_NS LONGEST_NS`: how
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
//! moving before the transport refuses it. On
//! standard input: `learn ID QUIC PLANE`, where a peer listens, and `replace FAILED JOINING`, a
//! member lost for good and the spare that takes its place in every range the node hosts
//! (`Node::replace`). A line it cannot read ends the process with the line named; standard input
//! closing ends it as a stop.
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
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::time::Duration;

use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, LogStats, Waits};
use hyper_rt::combine::{Either, race2};
use hyper_rt::machine::calibration::{Calibration, Policy};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_ec::durability::{Burst, Rates};
use mantle_engine::store::{Config as StoreConfig, Shape, Store};
use mantle_engine::trunk::TrunkConfig;
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::name;
use mantle_meta::root::{self, Enlist, MAX_DOMAIN};
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command};
use mantle_range::{ConfState, Range, Settings, Stored};
use mantle_s3::seal::WrappingKey;

use crate::ask::CommandOutcome;
use crate::classes::Role;
use crate::derive::{Device, Formatted, Measured, Stated, VolumeFacts};
use crate::directory::Peer;
use crate::node::{GatewaySettings, Hosted, Link, Node, NodeError, NodeSettings, Status, Wall};
use crate::storage::{Locks, Storage, StorageError, VolumeSettings};

/// Why the process stopped.
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("the configuration {0}: {1}")]
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
    #[error("the settings: {0}")]
    Derive(#[from] crate::derive::DeriveError),
    #[error("the device under {0}: {1}")]
    Device(PathBuf, String),
    #[error("standard output: {0}")]
    Say(String),
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
    pub peers: Vec<(u64, Role, PathBuf)>,
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
];

/// The lines `mantle serve`'s file repeats: its peers and ranges, its volumes, each `volume = DIR
/// SIZE`, and its root key's generations.
pub const STATED_REPEATED: &[&str] = &["peer", "range", "volume", "root_key"];

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

/// The `peer` lines, each `peer = ID ROLE CERTIFICATE`.
fn peers(lines: &Lines) -> Result<Vec<(u64, Role, PathBuf)>, ProcessError> {
    let mut peers = Vec::with_capacity(lines.all("peer").len());
    for (number, value) in lines.all("peer") {
        let mut fields = value.split_whitespace();
        let (Some(id), Some(role), Some(certificate), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            return Err(ProcessError::Line(
                *number,
                "peer = ID ROLE CERTIFICATE".into(),
            ));
        };
        let id = id
            .parse()
            .map_err(|_| ProcessError::Line(*number, "a peer's id".into()))?;
        let role = match role {
            "node" => Role::Node,
            "gateway" => Role::Gateway,
            "client" => Role::Client,
            _ => {
                return Err(ProcessError::Line(
                    *number,
                    "a role: node, gateway or client".into(),
                ));
            }
        };
        peers.push((id, role, lines.path(certificate)));
    }
    Ok(peers)
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
fn gateway_settings(config: GatewayConfig) -> Result<GatewaySettings, ProcessError> {
    let mut keys = Vec::with_capacity(config.keys.len());
    for (generation, path) in &config.keys {
        let refused = |why: &str| ProcessError::Key(path.clone(), why.to_owned());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(path)
                .map_err(|e| refused(&e.to_string()))?
                .permissions()
                .mode();
            if mode & 0o077 != 0 {
                return Err(refused("is readable by others than its owner"));
            }
        }
        let bytes = std::fs::read(path).map_err(|e| refused(&e.to_string()))?;
        let key: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| refused("is not 32 bytes"))?;
        keys.push((*generation, WrappingKey::new(&key)));
    }
    Ok(GatewaySettings {
        keys,
        handover_ns: config.handover_ns,
        window: config.window,
        objects: config.objects,
        bucket_quota: config.bucket_quota,
        scan_rows: config.scan_rows,
        rates: Rates {
            chunk: config.chunk_rate,
            domains: None,
            bursts: config.bursts,
            repair: config.repair_rate,
        },
        target: config.target,
    })
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

/// Runs the node the file at `config` states until standard input closes.
pub fn run(config: &Path) -> Result<(), ProcessError> {
    // Before any thread allocates: a freed large block's memory goes back to the OS, which a range's
    // budget following the machine reads as room (docs/research/65 §6).
    mantle_engine::port::mmap::return_freed_blocks();
    start(ProcessConfig::read(config)?, Locks::default())
}

/// Runs the node whose operator's statements the file at `config` holds (`mantle serve`, node.md
/// §1.4): each volume's device directory locked, then each device's profile kept or measured, the
/// node directory's device and the host's memory, cores and page measured; every other value
/// derived from those (`derive::derive`) and said on standard output, a `basis` line each, before
/// anything opens; then the node, as `mantle node` runs it, until standard input closes.
pub fn serve_stated(config: &Path) -> Result<(), ProcessError> {
    mantle_engine::port::mmap::return_freed_blocks();
    let stated = read_stated(config)?;
    let locks = Locks::take(stated.volumes.iter().map(|(dir, _)| dir.as_path()))?;
    let measured = measure(&stated)?;
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
    let derived = crate::derive::derive(stated, &measured, &mut stores)?;
    let mut out = std::io::stdout().lock();
    for basis in &derived.bases {
        writeln!(out, "basis {basis}").map_err(|e| ProcessError::Say(e.to_string()))?;
    }
    out.flush().map_err(|e| ProcessError::Say(e.to_string()))?;
    // The runtime's writer says the rest: this thread holds no lock on standard output for it.
    drop(out);
    start(derived.config, locks)
}

/// The node of `config`, its volumes under `locks` (any not held taken now), until standard input
/// closes.
fn start(mut config: ProcessConfig, locks: Locks) -> Result<(), ProcessError> {
    // The volumes open before the node's runtime starts: an issuer's start and a volume's
    // recovery wait on their device, which no shard may (node.md §1.3).
    let storage = Storage::open_locked(&config.volumes, locks)?;
    let gateway = config.gateway.take().map(gateway_settings).transpose()?;
    let runtime = runtime(&config)?;
    let mut rt =
        LocalRuntime::new(&runtime).map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
    rt.block_on(serve(config, storage, gateway))
        .map_err(|e| ProcessError::Runtime(format!("{e:?}")))?
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
        target,
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
/// endpoint, each connection and its plane, each read and written.
fn runtime(config: &ProcessConfig) -> Result<RuntimeConfig, ProcessError> {
    let calibration = calibration(&config.dir, config.probe_budget)?;
    let constants = calibration
        .constants(&Policy {
            reserved_cores: 0,
            lateness_tolerance_ns: None,
            latency_objective_ns: None,
        })
        .map_err(|unmet| ProcessError::Runtime(format!("{unmet:?}")))?;
    let mut runtime = RuntimeConfig::from_calibration(&calibration, &constants, 1, 2);
    runtime.shards = 1;
    runtime.pin = false;
    runtime.cores.clear();
    let sockets = config.peers.len().saturating_add(2);
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

/// The node, its talk on standard input and output.
async fn serve(
    config: ProcessConfig,
    storage: Storage,
    gateway: Option<GatewaySettings>,
) -> Result<(), ProcessError> {
    // This process's one shard runs the node's replicas, which apply through the engine's
    // synchronous calls (node.md §1.2).
    mantle_engine::store::permit_blocking();
    let volumes = storage.enlisting();
    let mut ranges = Vec::with_capacity(config.ranges.len());
    for (group, layer) in &config.ranges {
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
            engine: engine(&config, *group, *layer)?,
        });
    }
    let mut peers = Vec::with_capacity(config.peers.len());
    for (id, role, certificate) in &config.peers {
        peers.push(Peer {
            id: *id,
            role: *role,
            certificate: read_file(certificate)?,
        });
    }
    let settings = NodeSettings {
        id: config.id,
        dir: config.dir.clone(),
        quic: config.quic,
        plane: config.plane,
        credentials: Credentials {
            chain: vec![CertificateDer::from(read_file(&config.certificate)?)],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(read_file(&config.key)?)),
            roots: vec![CertificateDer::from(read_file(&config.authority)?)],
        },
        peers,
        addresses: Vec::new(),
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
    };
    let mut node = Node::open(settings).await?;
    // A buffer each way, a page each (hyper-rt `stdio`: a page is the natural chunk): the talk is
    // a line now and then, each far shorter.
    let chunk = usize::try_from(hyper_rt::machine::facts::Facts::query().page.base)
        .map_err(|e| ProcessError::Runtime(e.to_string()))?;
    let mut input =
        hyper_rt::stdio::stdin(1, chunk).map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
    let mut output =
        hyper_rt::stdio::stdout(1, chunk).map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
    let (quic, plane) = node.addresses()?;
    say(&mut output, &format!("bound {quic} {plane}\n")).await?;
    let held: Vec<u128> = node.volumes().map(|(id, _)| id).collect();
    for volume in held {
        say(&mut output, &format!("volume {volume:032x}\n")).await?;
    }
    // Its volumes enlisted by one command, asked until the root range answers it: the range's
    // laws, not a deadline of the node's, decide when.
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
    // The bytes its transfers moved as last said, and when: said again once they move, at most
    // every half of the transport's serve period, the longest an exchange it carries may go
    // without moving before the transport refuses it, so while a transfer lives the next is said
    // within one serve period of the last.
    let serve = crate::bounds::idle_timeout();
    let serve_ns = u64::try_from(serve.as_nanos()).unwrap_or(u64::MAX);
    let mut said_moved = (0u64, 0u64);
    loop {
        let ended = {
            let turn = pin!(node.turn());
            let line = pin!(input.read(&mut read));
            race2(turn, line).await
        };
        match ended {
            Either::First(turned) => turned?,
            Either::Second(Ok(0)) => return Ok(()),
            Either::Second(Ok(n)) => {
                pending.extend_from_slice(read.get(..n).unwrap_or_default());
                while let Some(end) = pending.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = pending.drain(..=end).collect();
                    take_line(&mut node, &config.ranges, &line)?;
                }
                // A line is never longer than the buffer it is read into.
                if pending.len() > chunk {
                    let text = String::from_utf8_lossy(&pending).into_owned();
                    return Err(ProcessError::Input(text, "a line longer than a page"));
                }
            }
            Either::Second(Err(e)) => return Err(ProcessError::Runtime(format!("{e:?}"))),
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
                }
                other => return Err(ProcessError::Enlist(format!("{other:?}"))),
            }
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
        if moved != said_moved.0 && now.saturating_sub(said_moved.1) >= serve_ns / 2 {
            say(&mut output, &format!("moved {moved} {serve_ns}\n")).await?;
            said_moved = (moved, now);
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
            // holds one shorter than the node's own; and whether the node waits on its own device,
            // so a watcher knows it holds still for its device and not by choice.
            let moved = was.is_none_or(|was| {
                (was.term, was.leader, was.commit, was.applied, was.reconnect)
                    != (now.term, now.leader, now.commit, now.applied, now.reconnect)
                    || was.measuring != now.measuring
                    || was.held != now.held
            });
            if !moved {
                continue;
            }
            let patience = now.patience.map_or(0, |p| p.as_nanos());
            let reconnect = now.reconnect.map_or(0, |r| r.as_nanos());
            let measuring = now.measuring_for.map_or(0, |m| m.as_nanos());
            let line = format!(
                "status {group:x} {} {} {} {} {patience} {reconnect} {measuring} {}\n",
                now.term,
                now.leader,
                now.commit,
                now.applied,
                u8::from(now.held)
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
    }
}

/// Takes a line of the node's input: `learn ID QUIC PLANE` or `replace FAILED JOINING`, a
/// replacement in each of `ranges`.
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
        std::fs::write(
            &config,
            "id = 1\ndir = .\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\nauthority = a.der\n\
             certificate = n.der\nkey = n.key\nvoters = 1\nrange = 1 root\n\
             volume = /my disks/one 1073741824\nvolume = data 4096\ndomain = a\n\
             root.max_nodes = 1\nroot.max_volumes = 2\n",
        )
        .unwrap();
        let stated = read_stated(&config).unwrap();
        assert_eq!(
            stated.volumes,
            vec![
                (PathBuf::from("/my disks/one"), 1 << 30),
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
