//! A node as a process (`mantle node`, docs/design/node.md §1): its configuration, stated in a
//! file, and its run on a runtime of one shard, the node's metadata shard (§1.2), whose replicas
//! apply through the engine's synchronous calls.
//!
//! **The file** states every number the node is run with, one `key = value` a line (`#` begins a
//! comment): nothing is assumed for the operator. A `peer` line, repeated, names a peer's id, its
//! role (`node` or `client`) and its certificate's file. Certificates and the key are DER (the
//! key PKCS #8).
//!
//! **Its talk** is lines. On standard output: `bound QUIC PLANE` once the node listens, then
//! `status TERM LEADER COMMIT APPLIED PATIENCE_NS RECONNECT_NS` each time one of the first four
//! moves, a peer it dials goes or comes back, or the heartbeats it takes from peers not yet judged
//! move (`Status::measuring`): how long the node states it may go without moving
//! while its peers live (`Status::patience`, 0 until measured), and while a peer it dials is not
//! connected, how long until it is again by the node's law (`Status::reconnect`, else 0); and
//! `replaced FAILED JOINING` once, from the node that led then, when a replacement ended. On
//! standard input: `learn ID QUIC PLANE`, where a peer listens, and `replace FAILED JOINING`, a
//! member of the range lost for good and the spare that takes its place (`Node::replace`). A line
//! it cannot read ends the process with the line named; standard input closing ends it as a stop.
//!
//! The range it hosts is the cell's first Name range of the voters the file names, its rows in
//! mantle's engine in the node's directory: founded on first start, opened at its last
//! checkpoint after, the log replayed past it. A node whose id the voters do not name is a spare:
//! it starts as the founders did and is no member of the range until a replacement adds it.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::time::Duration;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, Waits};
use hyper_rt::combine::{Either, race2};
use hyper_rt::machine::calibration::{Calibration, Policy};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_engine::store::Config as StoreConfig;
use mantle_engine::trunk::TrunkConfig;
use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_meta::name;
use mantle_meta::session::Rules;
use mantle_range::{ConfState, Range, Settings, Stored};

use crate::classes::Role;
use crate::directory::Peer;
use crate::node::{Hosted, Node, NodeError, NodeSettings, Status};

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
    #[error("standard input, line {0:?}: {1}")]
    Input(String, &'static str),
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
    pub group: u128,
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
}

/// The file's values by key, each with the line it was on; `peer` lines kept apart.
struct Lines {
    values: BTreeMap<String, (usize, String)>,
    peers: Vec<(usize, String)>,
}

impl Lines {
    fn parse(text: &str) -> Result<Self, ProcessError> {
        let mut values = BTreeMap::new();
        let mut peers = Vec::new();
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
            if key == "peer" {
                peers.push((number, value));
            } else if values.insert(key.to_owned(), (number, value)).is_some() {
                return Err(ProcessError::Line(number, format!("{key} stated twice")));
            }
        }
        Ok(Self { values, peers })
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
}

impl ProcessConfig {
    /// The configuration the file at `path` states.
    pub fn read(path: &Path) -> Result<Self, ProcessError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ProcessError::Read(path.to_path_buf(), e.to_string()))?;
        let lines = Lines::parse(&text)?;
        let mut peers = Vec::new();
        for (number, value) in &lines.peers {
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
                "client" => Role::Client,
                _ => return Err(ProcessError::Line(*number, "a role: node or client".into())),
            };
            peers.push((id, role, PathBuf::from(certificate)));
        }
        let group = lines.text("group")?;
        let group = u128::from_str_radix(group.trim_start_matches("0x"), 16)
            .map_err(|e| ProcessError::Value("group", e.to_string()))?;
        let voters = lines
            .text("voters")?
            .split(',')
            .map(|v| v.trim().parse())
            .collect::<Result<Vec<u64>, _>>()
            .map_err(|e| ProcessError::Value("voters", e.to_string()))?;
        let waits = match lines.text("log.waits")? {
            "measured" => Waits::Measured,
            "never" => Waits::Never,
            other => return Err(ProcessError::Value("log.waits", other.to_owned())),
        };
        Ok(Self {
            id: lines.get("id")?,
            dir: PathBuf::from(lines.text("dir")?),
            quic: lines.get("quic")?,
            plane: lines.get("plane")?,
            authority: PathBuf::from(lines.text("authority")?),
            certificate: PathBuf::from(lines.text("certificate")?),
            key: PathBuf::from(lines.text("key")?),
            peers,
            group,
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
        })
    }
}

/// Runs the node the file at `config` states until standard input closes.
pub fn run(config: &Path) -> Result<(), ProcessError> {
    let config = ProcessConfig::read(config)?;
    let runtime = runtime(&config)?;
    let mut rt =
        LocalRuntime::new(&runtime).map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
    rt.block_on(serve(config))
        .map_err(|e| ProcessError::Runtime(format!("{e:?}")))?
}

/// One shard, calibrated on this machine (hyper-rt `RuntimeConfig::from_calibration`), sized by
/// what the node holds: its one task, its wait and its driver's QUIC timer, and a socket for its
/// endpoint, each connection and its plane, each read and written.
fn runtime(config: &ProcessConfig) -> Result<RuntimeConfig, ProcessError> {
    let calibration = Calibration::measure(config.probe_budget, 0)
        .map_err(|e| ProcessError::Runtime(format!("{e:?}")))?;
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

/// The node's range engine in its directory: opened at its last checkpoint when its store is
/// there, created and founded with the range's first rows when not.
fn engine(config: &ProcessConfig) -> Result<Stored<DeviceFile>, ProcessError> {
    let path = config.dir.join("range.store");
    let fresh = !path.exists();
    let align = Alignment::new(config.store.page_size)
        .map_err(|e| ProcessError::Engine(format!("{e:?}")))?;
    let file = DeviceFile::open(&path, fresh, CachingRequest::Buffered, align)
        .map_err(|e| ProcessError::Engine(e.to_string()))?;
    let engine = |e: mantle_engine::Error| ProcessError::Engine(e.to_string());
    if !fresh {
        return Stored::open(file, config.store, config.memtable_bytes, config.trunk)
            .map_err(engine);
    }
    let mut stored =
        Stored::create(file, config.store, config.memtable_bytes, config.trunk).map_err(engine)?;
    let first = name::first(1).map_err(|e| ProcessError::Engine(e.to_string()))?;
    let founded = |e: mantle_meta::engine::EngineError| ProcessError::Engine(e.to_string());
    stored.install(0, first).map_err(founded)?;
    stored.persist().map_err(founded)?;
    Ok(stored)
}

/// The node, its talk on standard input and output.
async fn serve(config: ProcessConfig) -> Result<(), ProcessError> {
    // This process's one shard runs the node's replicas, which apply through the engine's
    // synchronous calls (node.md §1.2).
    mantle_engine::store::permit_blocking();
    let engine = engine(&config)?;
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
        ranges: vec![Hosted {
            group: config.group,
            range: Range {
                layer: Layer::Name,
                rules: config.rules,
                boot: ConfState {
                    voters: config.voters.clone(),
                    ..ConfState::default()
                },
                settings: config.settings,
            },
            engine,
        }],
        log: config.log,
        transport_bytes: config.transport_bytes,
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
    let mut read = vec![0u8; chunk];
    let mut pending = Vec::new();
    let mut told: Option<Status> = None;
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
                    take_line(&mut node, config.group, &line)?;
                }
                // A line is never longer than the buffer it is read into.
                if pending.len() > chunk {
                    let text = String::from_utf8_lossy(&pending).into_owned();
                    return Err(ProcessError::Input(text, "a line longer than a page"));
                }
            }
            Either::Second(Err(e)) => return Err(ProcessError::Runtime(format!("{e:?}"))),
        }
        for (_, replaced) in node.take_replaced() {
            let line = format!("replaced {} {}\n", replaced.failed(), replaced.joining());
            say(&mut output, &line).await?;
        }
        if let Some(now) = node.status(config.group) {
            // While some pair is not yet judged, the node is measuring its links, which moves only
            // the heartbeats it takes from those pairs: said too, so a watcher sees it move before
            // it elects, and sees it stop if a peer it measures goes silent.
            let moved = told.is_none_or(|was| {
                (
                    was.term,
                    was.leader,
                    was.commit,
                    was.applied,
                    was.reconnect.is_some(),
                ) != (
                    now.term,
                    now.leader,
                    now.commit,
                    now.applied,
                    now.reconnect.is_some(),
                ) || was.measuring != now.measuring
            });
            if moved {
                let patience = now.patience.map_or(0, |p| p.as_nanos());
                let reconnect = now.reconnect.map_or(0, |r| r.as_nanos());
                let line = format!(
                    "status {} {} {} {} {patience} {reconnect}\n",
                    now.term, now.leader, now.commit, now.applied
                );
                say(&mut output, &line).await?;
                told = Some(now);
            }
        }
    }
}

/// Takes a line of the node's input: `learn ID QUIC PLANE` or `replace FAILED JOINING`, a
/// replacement of `group`'s range.
fn take_line<E: Engine + 'static>(
    node: &mut Node<E>,
    group: u128,
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
            node.replace(group, failed, joining)?;
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
        let lines =
            Lines::parse("# a node\nid = 3\npeer = 2 node a.der\n\npeer = 9 client b.der # x\n")
                .unwrap();
        assert_eq!(lines.get::<u64>("id").unwrap(), 3);
        assert_eq!(lines.peers.len(), 2);
        assert!(matches!(
            Lines::parse("id = 1\nid = 2\n"),
            Err(ProcessError::Line(2, _))
        ));
        assert!(matches!(
            Lines::parse("id 1\n"),
            Err(ProcessError::Line(1, _))
        ));
        assert!(matches!(
            lines.get::<u64>("dir"),
            Err(ProcessError::Missing("dir"))
        ));
    }
}
