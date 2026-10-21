//! Real processes: a cell of `mantle node`s on loopback, three voters or five, each its own
//! process with its own directory, log file and engine store, replicating the cell's first Name
//! range over QUIC and the sealed datagram plane.
//!
//! - A client writes registrations to the range's leader; the leader's process is killed
//!   (SIGKILL) part way and started again on its directory, a new run of its liveness stream; the
//!   survivors elect, the client follows, and the node started again catches up. Asserted: every
//!   write the client was answered was answered once, and every node ends at the same applied
//!   index past every answered write.
//! - Three clients, each its own identity and session, put and read two keys while the leader is
//!   killed with SIGKILL half way and started again three quarters through, or cut off from the
//!   other nodes for a quarter by the test's relay (`crates/node/tests/support/relay.rs`), or, on
//!   three voters, has its plane deafened by the relay for a quarter (it hears no peer's plane
//!   while they hear its); each key's history is linearizable
//!   (`crates/node/tests/support/history.rs`), and every node ends where the others do. On cells
//!   of three voters and of five.
//! - A follower's engine store is damaged at rest while it is down, where both its superblocks
//!   lie: started again, it refuses the store with a typed corruption error rather than serve,
//!   and a spare under an identity never used replaces it (`replace` on the nodes' input, the
//!   leader's `replaced` line); then as many original voters are killed as leave a bare quorum,
//!   the spare in it. The clients' history through both is linearizable. On three voters and five.
//!
//! The test waits on facts the processes report (`bound`, `status` lines) and the clients'
//! progress, and gives up once a quiet period passes with nothing moved: the longest any node
//! states it may go without moving (`Status::patience`) or a link to a peer it dials may, should
//! that peer fall silent (`Status::reconnect`), or any client gives an exchange
//! (`Client::patience`), never less than RFC 6298's initial retransmission timeout, one second (as
//! `crates/node/tests/cluster.rs` does). A node whose log has a flush out extends the watch, since
//! its range moves only as its writes become durable, until the flush has been out longer than the
//! longest any node's log has taken and the quiet period besides (its `log` line;
//! hyper-durable-e2e's rule, as the cluster's). A cell no majority of whose running nodes is free
//! of its own device holds still for its devices, as its law has it (`Status::held`: a node judges
//! no peer while its own proof is out, timing.md §2.8, and a range's quorum waits on its members'
//! devices), and the watch does not count that time, each such stretch up to the same bound, so a
//! cell held by a fault of its own still fails. On windows-2025 the runner's one disk held every
//! node's writes 5 to 7.5 s at once (CI run 39544711299): the nodes held still, and the watch took
//! the cell for a stopped one. A process's lines are taken only while that process runs: a node
//! started again is a new process, and a line its killed one left is dropped.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_meta::wire::Answer;
use mantle_node::classes::{Bounds, Role};
use mantle_node::client::{Client, ClientSettings};
use mantle_node::directory::{Peer, node_name};

#[allow(dead_code)]
#[path = "../../node/tests/support/history.rs"]
mod history;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
use history::{Clock, Part, Plan, Steer, Told, check_histories, registration, workload};
#[path = "../../node/tests/support/relay.rs"]
mod relay;
use relay::Relay;

const NODE: &str = env!("CARGO_BIN_EXE_mantle");
/// The range's group.
const GROUP: u128 = 0x0072_616e_6765;
/// The nodes of most cells, each a founding voter of the range.
const NODES: u64 = 3;
/// The most nodes a cell has: five, the larger of the cells node.md's Phase C asks histories of.
const MOST_NODES: u64 = 5;
/// The writer client's id, past every node's of the largest cell; the workload's clients follow.
const CLIENT: u64 = MOST_NODES + 1;
/// The workload's clients, each its own identity and session.
const CLIENTS: u64 = 3;
/// RFC 6298 §2.1: the retransmission timeout before any round trip is measured, one second.
const INITIAL_RTO: Duration = Duration::from_secs(1);
/// RFC 5280 §4.1.2.5: the notAfter of a certificate with no well-defined expiration.
const NO_EXPIRY: u64 = 253_402_300_799;
/// Writes before the leader is killed and after: enough to pass the range's log group's room
/// (`log.group_bytes` below) on each side of the kill.
const WRITES: u64 = 200;
/// The entries a log group retains (the configuration's `log.group_entries`).
const GROUP_ENTRIES: u64 = 1024;
/// The engine's page (the configuration's `engine.page_size`), the engine's own small test
/// configuration's (crates/engine/tests/pool_startup.rs).
const STORE_PAGE: usize = 4096;

/// Every number the nodes run with, as the range's process test and the cluster test state them:
/// the range's settings and rules, the log's and the engine's sizes (the engine's own small test
/// configuration, crates/engine/tests/pool_startup.rs), and the calibration's probe budget the
/// engine's bench gives each probe.
const STATED: &str = "\
range.election_tick = 10
range.heartbeat_tick = 2
range.max_size_per_msg = 1024
range.max_inflight_msgs = 2
range.max_uncommitted_size = 2048
range.max_committed_size_per_ready = 1048576
range.max_entry_bytes = 1024
rules.lifetime_ns = 9223372036854775807
rules.max_sessions = 64
rules.max_answers = 16
rules.max_answer_bytes = 18446744073709551615
rules.expiries_per_entry = 8
log.segment_bytes = 65536
log.max_segments = 64
log.max_groups = 2
log.group_bytes = 4096
log.group_cache = 4096
log.queue_submissions = 16
log.waits = measured
engine.extent_pages = 4
engine.max_extents = 256
engine.memtable_bytes = 4096
engine.fanout = 3
engine.leaf_entries = 96
runtime.probe_budget_ms = 40
chunk_bytes = 8388608
chunk_asks = 0
";

/// The largest chunk the cell's gateways write (`chunk_bytes` above): `mantle_gateway::layout::CHUNK`,
/// Tectonic's 8 MiB.
const CHUNK_BYTES: u64 = 8 << 20;

/// The message bounds the nodes state for these settings (`mantle_node::node::message_bounds`):
/// the client states the same.
fn bounds() -> Bounds {
    use mantle_meta::apply::Layer;
    use mantle_meta::session::Rules;
    use mantle_node::node::{Hosted, message_bounds};
    use mantle_range::{ConfState, Range, Settings};
    message_bounds(
        &[Hosted {
            group: GROUP,
            range: Range {
                layer: Layer::Name,
                rules: Rules {
                    lifetime_ns: u64::MAX / 2,
                    max_sessions: 64,
                    max_answers: 16,
                    max_answer_bytes: usize::MAX,
                    expiries_per_entry: 8,
                },
                boot: ConfState::default(),
                settings: Settings {
                    election_tick: 10,
                    heartbeat_tick: 2,
                    max_size_per_msg: 1024,
                    max_inflight_msgs: 2,
                    max_uncommitted_size: 2048,
                    max_committed_size_per_ready: 1 << 20,
                    max_entry_bytes: 1024,
                },
            },
            engine: (),
        }],
        CHUNK_BYTES,
    )
    .unwrap()
}

/// A node's report.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Line {
    Bound(SocketAddr, SocketAddr),
    /// A peer's link as the node keeps it: the peer, and the rest of its line as said.
    Link(u64, String),
    /// A replacement the node ended as the range's leader: the member lost for good and the one
    /// in its place.
    Replaced(u64, u64),
    Status {
        term: u64,
        leader: u64,
        commit: u64,
        applied: u64,
        patience: Duration,
        /// While a peer it dials is not connected, how long until it is again (else zero).
        reconnect: Duration,
        /// While a peer is not yet judged, the longest its stream's heartbeats go between.
        measuring: Duration,
        /// Whether the node waits on its own device (`Status::held`).
        held: bool,
    },
    /// The node's log as it said with its last status: how long its flush had been out, if one
    /// was, and the longest an update has waited for its commit.
    Log(Option<Duration>, Duration),
}

fn parse(line: &str) -> Option<Line> {
    let fields: Vec<&str> = line.split_whitespace().collect();
    match fields.as_slice() {
        ["bound", quic, plane] => Some(Line::Bound(quic.parse().ok()?, plane.parse().ok()?)),
        ["replaced", failed, joining] => {
            Some(Line::Replaced(failed.parse().ok()?, joining.parse().ok()?))
        }
        ["link", peer, rest @ ..] => Some(Line::Link(peer.parse().ok()?, rest.join(" "))),
        ["log", flushing, longest] => {
            let flushing = Duration::from_nanos(flushing.parse().ok()?);
            let flushing = if flushing.is_zero() {
                None
            } else {
                Some(flushing)
            };
            Some(Line::Log(
                flushing,
                Duration::from_nanos(longest.parse().ok()?),
            ))
        }
        [
            "status",
            term,
            leader,
            commit,
            applied,
            patience,
            reconnect,
            measuring,
            held,
        ] => Some(Line::Status {
            term: term.parse().ok()?,
            leader: leader.parse().ok()?,
            commit: commit.parse().ok()?,
            applied: applied.parse().ok()?,
            patience: Duration::from_nanos(patience.parse().ok()?),
            reconnect: Duration::from_nanos(reconnect.parse().ok()?),
            measuring: Duration::from_nanos(measuring.parse().ok()?),
            held: match *held {
                "0" => false,
                "1" => true,
                _ => return None,
            },
        }),
        _ => None,
    }
}

/// What of a status is movement: all of it but whether the node waits on its device.
fn movement(line: &Line) -> Option<(u64, u64, u64, u64, Duration, Duration)> {
    match line {
        Line::Status {
            term,
            leader,
            commit,
            applied,
            reconnect,
            measuring,
            ..
        } => Some((*term, *leader, *commit, *applied, *reconnect, *measuring)),
        Line::Bound(..) | Line::Replaced(..) | Line::Link(..) | Line::Log(..) => None,
    }
}

/// What the test hears: a process's line, by the node and the process's generation, or what a
/// workload client tells.
enum Heard {
    Line(u64, u64, Line),
    /// A process's output ended, by the node and the process's generation: the process ended.
    Ended(u64, u64),
    Told(Told),
}

/// The files of the cell's authority, each node's certificate and key, and the clients'.
struct Pki {
    /// The range's founding voters: nodes 1 to `voters`. A node past them is a spare.
    voters: u64,
    authority: PathBuf,
    authority_der: Vec<u8>,
    /// By node id: the certificate's file and the key's, and the certificate.
    nodes: BTreeMap<u64, (PathBuf, PathBuf, Vec<u8>)>,
    /// By client id: the certificate's file, the certificate and the key.
    clients: BTreeMap<u64, (PathBuf, Vec<u8>, Vec<u8>)>,
}

impl Pki {
    /// The authority, `nodes` nodes' certificates and keys, the first `voters` of them the
    /// range's founding voters, and the clients', written in `dir`.
    fn new(dir: &Path, voters: u64, nodes: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let validity = Validity {
            not_before: now,
            not_after: NO_EXPIRY,
        };
        let key = KeyPair::generate().unwrap();
        let authority_der =
            mantle_cert::authority(&key, "mantle test cell", validity, Serial::draw().unwrap())
                .unwrap();
        let authority = dir.join("authority.der");
        std::fs::write(&authority, &authority_der).unwrap();
        let issue = |name: &str| {
            let own = KeyPair::generate().unwrap();
            let issued = (validity, Serial::draw().unwrap());
            let certificate =
                mantle_cert::node(&key, "mantle test cell", own.public_key(), name, issued)
                    .unwrap();
            (certificate, own.pkcs8().to_vec())
        };
        let mut issued = BTreeMap::new();
        for id in 1..=nodes {
            let (certificate, pkcs8) = issue(&node_name(id));
            let (c, k) = (
                dir.join(format!("node-{id}.der")),
                dir.join(format!("node-{id}.key")),
            );
            std::fs::write(&c, &certificate).unwrap();
            std::fs::write(&k, &pkcs8).unwrap();
            issued.insert(id, (c, k, certificate));
        }
        let mut clients = BTreeMap::new();
        for id in CLIENT..=CLIENT + CLIENTS {
            let (certificate, pkcs8) = issue(&format!("client-{id}"));
            let c = dir.join(format!("client-{id}.der"));
            std::fs::write(&c, &certificate).unwrap();
            clients.insert(id, (c, certificate, pkcs8));
        }
        Self {
            voters,
            authority,
            authority_der,
            nodes: issued,
            clients,
        }
    }

    /// Node `id`'s configuration file in `dir`.
    fn config(&self, id: u64, dir: &Path) -> PathBuf {
        let (certificate, key, _) = &self.nodes[&id];
        let voters: Vec<String> = (1..=self.voters).map(|v| v.to_string()).collect();
        // Each peer's share holds a receive chunk, the largest datagram QUIC carries, for each of
        // the node's four classes.
        let peers = self.nodes.len() - 1 + self.clients.len();
        let mut text = format!(
            "id = {id}\ndir = {}\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\nauthority = {}\n\
             certificate = {}\nkey = {}\ngroup = {GROUP:x}\nvoters = {}\n\
             transport_bytes = {}\nlog.group_entries = {GROUP_ENTRIES}\n\
             engine.page_size = {STORE_PAGE}\n{STATED}",
            dir.display(),
            self.authority.display(),
            certificate.display(),
            key.display(),
            voters.join(","),
            peers * 4 * hyper_transport::RECEIVE_CHUNK,
        );
        for (peer, (c, _, _)) in self.nodes.iter().filter(|(p, _)| **p != id) {
            text.push_str(&format!("peer = {peer} node {}\n", c.display()));
        }
        for (client, (c, _, _)) in &self.clients {
            text.push_str(&format!("peer = {client} client {}\n", c.display()));
        }
        let path = dir.join("node.conf");
        std::fs::write(&path, text).unwrap();
        path
    }

    /// Client `id`'s settings: it knows every node.
    fn client_settings(&self, id: u64) -> ClientSettings {
        let (_, certificate, key) = &self.clients[&id];
        ClientSettings {
            id,
            bind: "127.0.0.1:0".parse().unwrap(),
            credentials: Credentials {
                chain: vec![CertificateDer::from(certificate.clone())],
                key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.clone())),
                roots: vec![CertificateDer::from(self.authority_der.clone())],
            },
            nodes: self
                .nodes
                .iter()
                .map(|(id, (_, _, certificate))| Peer {
                    id: *id,
                    role: Role::Node,
                    certificate: certificate.clone(),
                })
                .collect(),
            messages: bounds(),
            transport_bytes: u64::try_from(self.nodes.len() * 3 * hyper_transport::RECEIVE_CHUNK)
                .unwrap(),
            seed: None,
        }
    }
}

/// A node's process, its input, where it said it listens, and its generation: the count of
/// processes the cell started before it.
struct Running {
    child: Child,
    input: ChildStdin,
    bound: Option<(SocketAddr, SocketAddr)>,
    generation: u64,
}

/// How the nodes reach each other: directly, or through the test's relay, which partitions them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Network {
    Direct,
    Relayed,
}

/// The cell: each node's directory and process, what the test heard of them and of the clients,
/// and the relay of a partitioned cell.
struct Cell {
    /// The cell's node identities, 1 to `nodes`: the founding voters, then any spares.
    nodes: u64,
    pki: Pki,
    dirs: BTreeMap<u64, PathBuf>,
    running: BTreeMap<u64, Running>,
    generations: u64,
    tell: Sender<Heard>,
    heard: Receiver<Heard>,
    status: BTreeMap<u64, Line>,
    /// Each workload client's operations done, and how long it gives an exchange.
    progress: BTreeMap<u64, u64>,
    clients: BTreeMap<u64, Duration>,
    /// The replacements a leader ended: the member lost for good and the one in its place.
    replaced: BTreeSet<(u64, u64)>,
    /// Each node's link to each peer as it last said, by node and peer.
    links: BTreeMap<(u64, u64), String>,
    /// Each node's log as it last said, and when that was heard: its flush out, if one was, and the
    /// longest an update has waited for its commit.
    log: BTreeMap<u64, (Instant, Option<Duration>, Duration)>,
    relay: Option<Relay>,
    /// Where the running workload's clients are told of a node started again.
    learns: Vec<Sender<(u64, SocketAddr)>>,
    moved_at: Instant,
    /// Since the last movement, how long the cell held still for its devices, each stretch at
    /// most `Cell::held_bound`, and when the stretch under way began, if one is.
    held_for: Duration,
    held_since: Option<Instant>,
    _root: tempfile::TempDir,
}

impl Drop for Cell {
    fn drop(&mut self) {
        for running in self.running.values_mut() {
            let _ = running.child.kill();
            let _ = running.child.wait();
        }
    }
}

impl Cell {
    /// A cell of `nodes` nodes started on `network`, each told where the others listen.
    fn new(nodes: u64, network: Network) -> Self {
        Self::spared(nodes, 0, network)
    }

    /// A cell of `voters` nodes started on `network`, each told where the others listen, and
    /// `spares` identities past them that every node knows and no process runs yet.
    fn spared(voters: u64, spares: u64, network: Network) -> Self {
        let nodes = voters + spares;
        let root = tempfile::tempdir().unwrap();
        let pki = Pki::new(root.path(), voters, nodes);
        let dirs = (1..=nodes)
            .map(|id| {
                let dir = root.path().join(format!("n{id}"));
                std::fs::create_dir(&dir).unwrap();
                (id, dir)
            })
            .collect();
        let (tell, heard) = channel();
        let mut cell = Self {
            nodes,
            pki,
            dirs,
            running: BTreeMap::new(),
            generations: 0,
            tell,
            heard,
            status: BTreeMap::new(),
            progress: BTreeMap::new(),
            clients: BTreeMap::new(),
            replaced: BTreeSet::new(),
            links: BTreeMap::new(),
            log: BTreeMap::new(),
            relay: (network == Network::Relayed)
                .then(|| Relay::start(&(1..=nodes).collect::<Vec<_>>())),
            learns: Vec::new(),
            moved_at: Instant::now(),
            held_for: Duration::ZERO,
            held_since: None,
            _root: root,
        };
        // One at a time, each listening before the next starts (`Cell::start`).
        for id in 1..=voters {
            cell.start(id);
        }
        cell.introduce();
        cell
    }

    /// Starts node `id`'s process on its directory, a thread forwarding its lines, and waits until
    /// it listens.
    fn start(&mut self, id: u64) {
        let config = self.pki.config(id, &self.dirs[&id]);
        let mut child = Command::new(NODE)
            .arg("node")
            .arg(&config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let (tell, generation) = (self.tell.clone(), self.generations);
        self.generations += 1;
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let Ok(line) = line else { break };
                if let Some(parsed) = parse(&line)
                    && tell.send(Heard::Line(id, generation, parsed)).is_err()
                {
                    return;
                }
            }
            let _ = tell.send(Heard::Ended(id, generation));
        });
        self.running.insert(
            id,
            Running {
                child,
                input,
                bound: None,
                generation,
            },
        );
        self.listening(id);
    }

    /// Waits until node `id`'s process listens, or fails once it ends first. A process's start
    /// (its runtime's calibration, its log's and store's files, its keys) is the operating
    /// system's and the device's work, with no law of the node's to bound it, so nothing but the
    /// process's own word ends the wait: its `bound` line, or its output's end.
    fn listening(&mut self, id: u64) {
        while self.running[&id].bound.is_none() {
            match self.heard.recv() {
                Ok(heard) => self.take(heard),
                Err(_) => panic!("node {id}'s output is gone"),
            }
        }
        self.moved();
    }

    /// Starts node `id`'s process on a directory whose store is damaged, and waits for its end:
    /// it must refuse the store, a typed corruption error, before it listens. What it said on
    /// standard error.
    fn start_damaged(&mut self, id: u64) -> String {
        let config = self.pki.config(id, &self.dirs[&id]);
        let mut child = Command::new(NODE)
            .arg("node")
            .arg(&config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // It listens only once its store opened: a `bound` line is a node serving from a store
        // it should have refused. Its output ends as it does.
        for line in BufReader::new(child.stdout.take().unwrap()).lines() {
            let line = line.unwrap();
            assert!(
                !matches!(parse(&line), Some(Line::Bound(..))),
                "node {id} listens on a damaged store"
            );
        }
        let status = child.wait().unwrap();
        let mut said = String::new();
        std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut said).unwrap();
        assert!(!status.success(), "node {id} ended well on a damaged store");
        said
    }

    /// Overwrites the first two pages of node `id`'s engine store, where both its superblocks
    /// lie, with bytes it never wrote: a device that returned other bytes than it acknowledged.
    fn damage_store(&self, id: u64) {
        let path = self.dirs[&id].join("range.store");
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.write_all(&[0xA5; 2 * STORE_PAGE]).unwrap();
        file.sync_all().unwrap();
    }

    /// Tells every running node to replace `failed`, a member lost for good, with `joining`.
    fn replace(&mut self, failed: u64, joining: u64) {
        for running in self.running.values_mut() {
            writeln!(running.input, "replace {failed} {joining}").unwrap();
            running.input.flush().unwrap();
        }
    }

    /// Kills node `id`'s process, as a power cut takes it.
    fn kill(&mut self, id: u64) {
        let mut running = self.running.remove(&id).unwrap();
        running.child.kill().unwrap();
        running.child.wait().unwrap();
        self.status.remove(&id);
        self.log.remove(&id);
        self.hold();
    }

    /// The quiet period: the longest any node states it may go without moving or a link to a peer
    /// it dials may, should that peer fall silent, or any client gives an exchange, never less than
    /// the initial RTO.
    fn quiet(&self) -> Duration {
        self.status
            .values()
            .filter_map(|l| match l {
                Line::Status {
                    patience,
                    reconnect,
                    measuring,
                    ..
                } => Some((*patience).max(*reconnect).max(*measuring)),
                Line::Bound(..) | Line::Replaced(..) | Line::Link(..) | Line::Log(..) => None,
            })
            .chain(self.clients.values().copied())
            .fold(INITIAL_RTO, Duration::max)
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

    /// Whether no majority of the running nodes is free of its own device: the range's quorum then
    /// waits on the devices of the nodes that wait on theirs.
    fn held_by_devices(&self) -> bool {
        let held = self
            .running
            .keys()
            .filter(|id| matches!(self.status.get(id), Some(Line::Status { held: true, .. })))
            .count();
        held > 0 && 2 * held >= self.running.len()
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
        self.moved_at = Instant::now();
        self.held_for = Duration::ZERO;
        self.held_since = if self.held_by_devices() {
            Some(self.moved_at)
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
        self.moved_at + quiet + self.held_for + holding
    }

    /// When the watch ends for the flushes out: each running node's, as last said, past the
    /// longest any node's log has taken and `quiet` besides, as the cluster's watch has it.
    fn flush_deadline(&self, quiet: Duration) -> Instant {
        let allowed = self.held_bound(quiet);
        self.log
            .iter()
            .filter(|(id, _)| self.running.contains_key(id))
            .filter_map(|(_, (heard, flushing, _))| {
                Some(*heard + allowed.saturating_sub(*flushing.as_ref()?))
            })
            .max()
            .unwrap_or_else(Instant::now)
    }

    /// Takes what it hears until `done` holds; fails once a quiet period passes with nothing.
    fn until<T>(&mut self, mut done: impl FnMut(&mut Self) -> Option<T>) -> T {
        self.moved();
        loop {
            if let Some(found) = done(self) {
                return found;
            }
            let quiet = self.quiet();
            let left = self
                .deadline(quiet)
                .max(self.flush_deadline(quiet))
                .saturating_duration_since(Instant::now());
            match self.heard.recv_timeout(left) {
                Ok(heard) => self.take(heard),
                Err(_) => panic!(
                    "quiet for {quiet:?} with nothing moved, besides {:?} held still for the \
                     devices: {:?}, progress {:?}, links {:?}, log {:?}",
                    self.held_for, self.status, self.progress, self.links, self.log
                ),
            }
        }
    }

    /// Takes what was heard and not yet taken, without waiting.
    fn take_heard(&mut self) {
        loop {
            match self.heard.try_recv() {
                Ok(heard) => self.take(heard),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }

    fn take(&mut self, heard: Heard) {
        match heard {
            Heard::Line(id, generation, line) => {
                // A line its killed process left: the node runs another process now, or none.
                let Some(running) = self
                    .running
                    .get_mut(&id)
                    .filter(|r| r.generation == generation)
                else {
                    return;
                };
                match line {
                    Line::Bound(quic, plane) => running.bound = Some((quic, plane)),
                    Line::Replaced(failed, joining) => {
                        self.replaced.insert((failed, joining));
                    }
                    Line::Link(peer, link) => {
                        self.links.insert((id, peer), link);
                    }
                    Line::Status { .. } => {
                        // A status that says only whether the node waits on its device begins
                        // or ends a stretch held still, and moves nothing.
                        let moved = self.status.get(&id).and_then(movement) != movement(&line);
                        self.status.insert(id, line);
                        if !moved {
                            self.hold();
                            return;
                        }
                    }
                    Line::Log(flushing, longest) => {
                        self.log.insert(id, (Instant::now(), flushing, longest));
                        return;
                    }
                }
            }
            // A process ended that the cell did not kill: it failed.
            Heard::Ended(id, generation) => {
                if self
                    .running
                    .get(&id)
                    .is_some_and(|r| r.generation == generation)
                {
                    panic!("node {id} ended: {:?}", self.status);
                }
            }
            Heard::Told(Told::Progress(client, done, patience)) => {
                self.progress.insert(client, done);
                if let Some(patience) = patience {
                    self.clients.insert(client, patience);
                }
            }
            Heard::Told(Told::Failed(client, why)) => panic!("client {client} failed: {why}"),
        }
        self.moved();
    }

    /// Tells each running node, each listening (`Cell::start`), where the others do: where they
    /// do, or on a relayed cell, at the relay, which forwards to where they do.
    fn introduce(&mut self) {
        let bound: Vec<(u64, SocketAddr, SocketAddr)> = self
            .running
            .iter()
            .map(|(id, r)| (*id, r.bound.unwrap().0, r.bound.unwrap().1))
            .collect();
        if let Some(relay) = &self.relay {
            for (id, quic, plane) in &bound {
                relay.listen(*id, *quic, *plane);
            }
        }
        for (id, running) in &mut self.running {
            for (peer, quic, plane) in bound.iter().filter(|(p, ..)| p != id) {
                let (quic, plane) = self
                    .relay
                    .as_ref()
                    .map_or((*quic, *plane), |relay| relay.front(*id, *peer));
                writeln!(running.input, "learn {peer} {quic} {plane}").unwrap();
            }
            running.input.flush().unwrap();
        }
    }

    /// Drops every datagram the other nodes' planes send node `id`'s, or none.
    fn deafen_plane(&self, id: u64, deaf: bool) {
        self.relay.as_ref().unwrap().deafen_plane(id, deaf);
    }

    /// Cuts node `id` from every other node, both ways, or joins it to them again.
    fn isolate(&self, id: u64, cut: bool) {
        let relay = self.relay.as_ref().unwrap();
        for peer in (1..=self.nodes).filter(|peer| *peer != id) {
            relay.join(id, peer, !cut);
        }
    }

    /// The leader every running node names, in one term.
    fn leader(&mut self) -> u64 {
        self.until(|c| {
            let named: Vec<(u64, u64)> = c
                .running
                .keys()
                .filter_map(|id| match c.status.get(id) {
                    Some(Line::Status { term, leader, .. }) => Some((*term, *leader)),
                    _ => None,
                })
                .collect();
            let first = *named.first()?;
            let agreed = named.len() == c.running.len()
                && first.1 != 0
                && c.running.contains_key(&first.1)
                && named.iter().all(|n| *n == first);
            if agreed { Some(first.1) } else { None }
        })
    }

    /// What node `id` applied, as it last said; 0 before it said.
    fn applied(&self, id: u64) -> u64 {
        match self.status.get(&id) {
            Some(Line::Status { applied, .. }) => *applied,
            _ => 0,
        }
    }

    /// Waits until every running node has applied as far as the others. A node's commit as its
    /// log holds it may trail what it applied by the commit that moved alone, which rides the
    /// group's next write (hyper-durable), so applied is the measure.
    fn converged(&mut self) -> u64 {
        self.until(|c| {
            let mut ends = c.running.keys().map(|id| c.applied(*id));
            let first = ends.next()?;
            if first > 0 && ends.all(|a| a == first) {
                Some(first)
            } else {
                None
            }
        })
    }

    /// Where each running node's QUIC endpoint listens.
    fn bound_quic(&self) -> Vec<(u64, SocketAddr)> {
        self.running
            .iter()
            .filter_map(|(id, r)| Some((*id, r.bound?.0)))
            .collect()
    }

    /// Opens the bucket's gate, which every put needs, through the writer client.
    fn open_bucket(&mut self) {
        let (settings, bound) = (self.pki.client_settings(CLIENT), self.bound_quic());
        let opened = std::thread::spawn(move || {
            client_runtime(settings.nodes.len() + 1)
                .block_on(async move {
                    let mut client = Client::open(settings).await.unwrap();
                    for (node, quic) in bound {
                        client.learn(node, quic).unwrap();
                    }
                    history::open_bucket(&mut client, GROUP, CLIENT).await
                })
                .unwrap()
        })
        .join()
        .unwrap()
        .unwrap_or_else(|why| panic!("the gate's opening: {why}"));
        assert_eq!(
            opened,
            vec![Answer::Name(mantle_meta::name::Outcome::GateMoved)]
        );
        self.take_heard();
    }

    /// Starts the workload's clients as `plan` says, each knowing every node and told where the
    /// running ones listen; the senders whose drop stops them.
    fn start_workload(&mut self, plan: Plan) -> (Vec<JoinHandle<Part>>, Vec<Sender<()>>) {
        let mut threads = Vec::new();
        let mut stops = Vec::new();
        self.learns.clear();
        for id in CLIENT + 1..=CLIENT + CLIENTS {
            let (stop, stopping) = channel();
            let (learn, learning) = channel();
            let (settings, bound) = (self.pki.client_settings(id), self.bound_quic());
            let tell = self.tell.clone();
            let told = move |told: Told| {
                let _ = tell.send(Heard::Told(told));
            };
            let steer = Steer {
                learn: learning,
                stop: stopping,
            };
            threads.push(std::thread::spawn(move || {
                workload(settings, bound, (plan, steer), client_runtime, told)
            }));
            stops.push(stop);
            self.learns.push(learn);
        }
        (threads, stops)
    }

    /// Tells the running workload's clients where node `id` listens.
    fn tell_workload(&self, id: u64) {
        let quic = self.running[&id].bound.unwrap().0;
        for learn in &self.learns {
            // A client done with its operations has dropped its end.
            let _ = learn.send((id, quic));
        }
    }

    /// Waits until the workload's clients, `ops` operations each, have done `quarters` quarters
    /// of them in all.
    fn done_by(&mut self, ops: u64, quarters: u64) {
        let done = ops * CLIENTS * quarters / 4;
        self.until(|c| {
            if c.progress.values().sum::<u64>() >= done {
                Some(())
            } else {
                None
            }
        });
    }
}

/// A client's shard: its one task, its wait's timer and its driver's QUIC timer, and `sockets`
/// sockets, each read and written. The watchdog's budget and the wheel's tick are
/// hyper-rt-transport's test harness's (`tests/exchanges.rs`).
fn client_runtime(sockets: usize) -> LocalRuntime {
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
        page_bytes: usize::try_from(hyper_rt::machine::facts::Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

/// The writer: registrations 1 to `writes` through a client of its own, each with no deadline of
/// its own, as the client's commands have none (the cell's laws decide, and the test's watch ends
/// one never answered), each answer kept and each told as progress.
fn writer(
    settings: ClientSettings,
    bound: Vec<(u64, SocketAddr)>,
    writes: u64,
    tell: Sender<Heard>,
) -> BTreeMap<u64, Vec<Answer>> {
    let id = settings.id;
    client_runtime(settings.nodes.len() + 1)
        .block_on(async move {
            let mut client = Client::open(settings).await.unwrap();
            for (node, quic) in bound {
                client.learn(node, quic).unwrap();
            }
            let mut answered = BTreeMap::new();
            for serial in 1..=writes {
                match client.command(GROUP, &registration(serial), u64::MAX).await {
                    Ok(answers) => {
                        answered.insert(serial, answers);
                    }
                    Err(error) => {
                        let why = format!("registration {serial}: {error}");
                        let _ = tell.send(Heard::Told(Told::Failed(id, why)));
                        return answered;
                    }
                }
                let _ = tell.send(Heard::Told(Told::Progress(id, serial, client.patience())));
            }
            answered
        })
        .unwrap()
}

#[test]
fn a_leader_killed_mid_write_loses_no_answered_write_and_catches_up_when_started_again() {
    let mut cell = Cell::new(NODES, Network::Direct);
    let leader = cell.leader();
    let (settings, bound) = (cell.pki.client_settings(CLIENT), cell.bound_quic());
    let (writes, tell) = (2 * WRITES, cell.tell.clone());
    let thread = std::thread::spawn(move || writer(settings, bound, writes, tell));
    let written = |done: u64| {
        move |c: &mut Cell| {
            if c.progress.get(&CLIENT).is_some_and(|d| *d >= done) {
                Some(())
            } else {
                None
            }
        }
    };
    // Killed part way: the writes after go through the survivors.
    cell.until(written(WRITES));
    cell.kill(leader);
    cell.until(written(writes));
    let answered = thread.join().unwrap();
    cell.start(leader);
    cell.introduce();
    // What the survivors applied, which the node started again must apply.
    let last = (1..=NODES)
        .filter(|id| *id != leader)
        .map(|id| cell.applied(id))
        .max()
        .unwrap();
    cell.until(|c| {
        if c.applied(leader) >= last {
            Some(())
        } else {
            None
        }
    });
    // Every write was answered once, a registration's answer each.
    assert_eq!(answered.len() as u64, writes);
    for (serial, answers) in &answered {
        assert!(
            matches!(
                answers.as_slice(),
                [Answer::Registered { .. } | Answer::SessionsFull { .. }]
            ),
            "registration {serial} answered {answers:?}"
        );
    }
    cell.converged();
}

/// What befalls the leader half way through the linearizable history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// Its process is killed (SIGKILL), and started again three quarters through.
    Kill,
    /// It is cut off from the other nodes, and joined to them again three quarters through.
    Cut,
    /// Its plane hears the other nodes no more, while they hear it and every QUIC path passes, and
    /// hears them again three quarters through.
    Deaf,
}

fn linearizable(nodes: u64, fault: Fault) {
    let network = match fault {
        Fault::Kill => Network::Direct,
        Fault::Cut | Fault::Deaf => Network::Relayed,
    };
    let mut cell = Cell::new(nodes, network);
    let leader = cell.leader();
    cell.open_bucket();
    // Puts enough to pass the log group's room in all, so the history crosses compactions.
    let ops = (2 * (GROUP_ENTRIES + 1)).div_ceil(CLIENTS);
    let (threads, stops) = cell.start_workload(Plan {
        group: GROUP,
        ops,
        phase: 0,
        clock: Clock::Host(Instant::now()),
    });
    cell.done_by(ops, 2);
    match fault {
        Fault::Kill => {
            cell.kill(leader);
            cell.done_by(ops, 3);
            cell.start(leader);
            cell.introduce();
            cell.tell_workload(leader);
        }
        Fault::Cut => {
            cell.isolate(leader, true);
            cell.done_by(ops, 3);
            cell.isolate(leader, false);
        }
        Fault::Deaf => {
            cell.deafen_plane(leader, true);
            cell.done_by(ops, 3);
            cell.deafen_plane(leader, false);
        }
    }
    cell.done_by(ops, 4);
    drop(stops);
    check_histories(threads.into_iter().flat_map(|t| t.join().unwrap()));
    // Every node, the one killed or cut off among them, ends where the others do.
    cell.converged();
}

#[test]
fn puts_and_reads_through_a_leader_killed_and_started_again_are_linearizable() {
    linearizable(NODES, Fault::Kill);
}

#[test]
fn puts_and_reads_through_a_leader_cut_off_are_linearizable() {
    linearizable(NODES, Fault::Cut);
}

/// The leader's plane goes deaf: it suspects its followers, which hear it and trust it, and steps
/// down; the others elect one of themselves, which it follows by its lanes.
#[test]
fn puts_and_reads_through_a_leader_whose_plane_goes_deaf_are_linearizable() {
    linearizable(NODES, Fault::Deaf);
}

#[test]
fn puts_and_reads_through_a_leader_of_five_voters_killed_and_started_again_are_linearizable() {
    linearizable(MOST_NODES, Fault::Kill);
}

#[test]
fn puts_and_reads_through_a_leader_cut_off_from_five_voters_are_linearizable() {
    linearizable(MOST_NODES, Fault::Cut);
}

/// A follower's device fails at rest: while it is down its engine's store is overwritten where
/// both its superblocks lie. Started again, the node refuses the store with a typed corruption
/// error rather than serve from it (CLAUDE.md §6). It is lost for good, and a spare under an
/// identity never used replaces it (replica.md §6); then as many original voters are killed as
/// leave the range a bare quorum, so every quorum counts the spare. The history through both
/// phases is linearizable: no acknowledged write is lost.
fn damaged_and_replaced(voters: u64) {
    let mut cell = Cell::spared(voters, 1, Network::Direct);
    let spare = voters + 1;
    let leader = cell.leader();
    cell.open_bucket();
    let ops = (2 * (GROUP_ENTRIES + 1)).div_ceil(CLIENTS);
    let start = Instant::now();
    let mut history = Vec::new();
    let (threads, stops) = cell.start_workload(Plan {
        group: GROUP,
        ops,
        phase: 0,
        clock: Clock::Host(start),
    });
    cell.done_by(ops, 1);
    let damaged = (1..=voters).find(|id| *id != leader).unwrap();
    cell.kill(damaged);
    cell.damage_store(damaged);
    let said = cell.start_damaged(damaged);
    assert!(said.contains("corruption"), "node {damaged} said: {said}");
    cell.start(spare);
    cell.introduce();
    cell.tell_workload(spare);
    cell.replace(damaged, spare);
    cell.until(|c| {
        if c.replaced.contains(&(damaged, spare)) {
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
    // Of the voters now, a bare quorum is left, the spare in it.
    let originals: Vec<u64> = (1..=voters).filter(|id| *id != damaged).collect();
    for id in originals.iter().take(usize::try_from(voters / 2).unwrap()) {
        cell.kill(*id);
    }
    cell.progress.clear();
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
}

#[test]
fn a_node_whose_store_is_damaged_refuses_it_and_is_replaced_with_no_acknowledged_write_lost() {
    damaged_and_replaced(NODES);
}

#[test]
fn a_node_of_five_voters_whose_store_is_damaged_is_replaced_with_no_acknowledged_write_lost() {
    damaged_and_replaced(MOST_NODES);
}
