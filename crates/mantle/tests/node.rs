//! Real processes: three `mantle node`s on loopback, each its own process with its own directory,
//! log file and engine store, replicating the cell's first Name range over QUIC and the sealed
//! datagram plane. A client in the test writes registrations to the range's leader; the leader's
//! process is killed (SIGKILL) part way and started again on its directory, a new run of its
//! liveness stream; the survivors elect, the client follows, and the node started again catches
//! up. Asserted: every write the client was answered was answered once, each at its own index,
//! every node ends at the same commit and applied index, and the node killed applies past every
//! answered write.
//!
//! The test waits on facts the processes report (`bound`, `status` lines) and the client's
//! answers, and gives up once a quiet period passes with nothing moved: the longest patience any
//! node states (`Status::patience`), never less than RFC 6298's initial retransmission timeout,
//! one second (as `crates/node/tests/cluster.rs` does). A client's command is given that period
//! and an ask's round besides.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_cert::{KeyPair, Validity};
use mantle_meta::wire::{Answer, Command as Op, Entry, Sessioned};
use mantle_node::classes::{Bounds, Role};
use mantle_node::client::{Client, ClientSettings};
use mantle_node::directory::{Peer, node_name};

const NODE: &str = env!("CARGO_BIN_EXE_mantle");
/// The range's group.
const GROUP: u128 = 0x0072_616e_6765;
const NODES: u64 = 3;
const CLIENT: u64 = NODES + 1;
/// RFC 6298 §2.1: the retransmission timeout before any round trip is measured, one second.
const INITIAL_RTO: Duration = Duration::from_secs(1);
/// RFC 5280 §4.1.2.5: the notAfter of a certificate with no well-defined expiration.
const NO_EXPIRY: u64 = 253_402_300_799;
/// Writes before the leader is killed and after: enough to pass the range's log group's room
/// (`log.group_bytes` below) on each side of the kill.
const WRITES: u64 = 200;

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
log.group_entries = 1024
log.group_bytes = 4096
log.group_cache = 4096
log.queue_submissions = 16
log.waits = measured
engine.page_size = 4096
engine.extent_pages = 4
engine.max_extents = 256
engine.memtable_bytes = 4096
engine.fanout = 3
engine.leaf_entries = 96
runtime.probe_budget_ms = 40
";

/// The message bounds the nodes state for these settings (`mantle_node::node::message_bounds`):
/// the client states the same.
fn bounds() -> Bounds {
    use mantle_meta::apply::Layer;
    use mantle_meta::session::Rules;
    use mantle_node::node::{Hosted, message_bounds};
    use mantle_range::{ConfState, Range, Settings};
    message_bounds(&[Hosted {
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
    }])
    .unwrap()
}

/// A node's report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Line {
    Bound(SocketAddr, SocketAddr),
    Status {
        term: u64,
        leader: u64,
        commit: u64,
        applied: u64,
        patience: Duration,
        /// While a peer it dials is not connected, how long until it is again (else zero).
        reconnect: Duration,
    },
}

fn parse(line: &str) -> Option<Line> {
    let fields: Vec<&str> = line.split_whitespace().collect();
    match fields.as_slice() {
        ["bound", quic, plane] => Some(Line::Bound(quic.parse().ok()?, plane.parse().ok()?)),
        ["status", term, leader, commit, applied, patience, reconnect] => Some(Line::Status {
            term: term.parse().ok()?,
            leader: leader.parse().ok()?,
            commit: commit.parse().ok()?,
            applied: applied.parse().ok()?,
            patience: Duration::from_nanos(patience.parse().ok()?),
            reconnect: Duration::from_nanos(reconnect.parse().ok()?),
        }),
        _ => None,
    }
}

/// The files of the cell's authority and each node's and the client's certificate and key.
struct Pki {
    authority: PathBuf,
    nodes: BTreeMap<u64, (PathBuf, PathBuf)>,
    client: (Vec<u8>, Vec<u8>),
    client_certificate: PathBuf,
    authority_der: Vec<u8>,
    node_ders: BTreeMap<u64, Vec<u8>>,
}

impl Pki {
    fn new(dir: &Path) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let validity = Validity {
            not_before: now,
            not_after: NO_EXPIRY,
        };
        let key = KeyPair::generate().unwrap();
        let authority_der = mantle_cert::authority(&key, "mantle test cell", validity).unwrap();
        let authority = dir.join("authority.der");
        std::fs::write(&authority, &authority_der).unwrap();
        let issue = |name: &str| {
            let own = KeyPair::generate().unwrap();
            let certificate =
                mantle_cert::node(&key, "mantle test cell", own.public_key(), name, validity)
                    .unwrap();
            (certificate, own.pkcs8().to_vec())
        };
        let mut nodes = BTreeMap::new();
        let mut node_ders = BTreeMap::new();
        for id in 1..=NODES {
            let (certificate, pkcs8) = issue(&node_name(id));
            let (c, k) = (
                dir.join(format!("node-{id}.der")),
                dir.join(format!("node-{id}.key")),
            );
            std::fs::write(&c, &certificate).unwrap();
            std::fs::write(&k, &pkcs8).unwrap();
            nodes.insert(id, (c, k));
            node_ders.insert(id, certificate);
        }
        let client = issue(&format!("client-{CLIENT}"));
        let client_certificate = dir.join("client.der");
        std::fs::write(&client_certificate, &client.0).unwrap();
        Self {
            authority,
            nodes,
            client,
            client_certificate,
            authority_der,
            node_ders,
        }
    }

    /// Node `id`'s configuration file in `dir`.
    fn config(&self, id: u64, dir: &Path) -> PathBuf {
        let (certificate, key) = &self.nodes[&id];
        let mut text = format!(
            "id = {id}\ndir = {}\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\nauthority = {}\n\
             certificate = {}\nkey = {}\ngroup = {GROUP:x}\nvoters = 1,2,3\n\
             transport_bytes = {}\n{STATED}",
            dir.display(),
            self.authority.display(),
            certificate.display(),
            key.display(),
            // Each peer's share holds a receive chunk, the largest datagram QUIC carries, for
            // each of the node's three classes.
            usize::try_from(NODES).unwrap() * 3 * hyper_transport::RECEIVE_CHUNK,
        );
        for (peer, (c, _)) in self.nodes.iter().filter(|(p, _)| **p != id) {
            text.push_str(&format!("peer = {peer} node {}\n", c.display()));
        }
        text.push_str(&format!(
            "peer = {CLIENT} client {}\n",
            self.client_certificate.display()
        ));
        let path = dir.join("node.conf");
        std::fs::write(&path, text).unwrap();
        path
    }
}

/// A node's process, its input, and where it said it listens.
struct Running {
    child: Child,
    input: ChildStdin,
    bound: Option<(SocketAddr, SocketAddr)>,
}

/// The cell: each node's directory and process, and every node's lines as they come.
struct Cell {
    pki: Pki,
    dirs: BTreeMap<u64, PathBuf>,
    running: BTreeMap<u64, Running>,
    tell: Sender<(u64, Line)>,
    lines: Receiver<(u64, Line)>,
    status: BTreeMap<u64, Line>,
    moved_at: Instant,
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
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let pki = Pki::new(root.path());
        let dirs = (1..=NODES)
            .map(|id| {
                let dir = root.path().join(format!("n{id}"));
                std::fs::create_dir(&dir).unwrap();
                (id, dir)
            })
            .collect();
        let (tell, lines) = channel();
        let mut cell = Self {
            pki,
            dirs,
            running: BTreeMap::new(),
            tell,
            lines,
            status: BTreeMap::new(),
            moved_at: Instant::now(),
            _root: root,
        };
        for id in 1..=NODES {
            cell.start(id);
        }
        cell.introduce();
        cell
    }

    /// Starts node `id`'s process on its directory; a thread forwards its lines, one a process.
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
        let tell = self.tell.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let Ok(line) = line else { return };
                if let Some(parsed) = parse(&line)
                    && tell.send((id, parsed)).is_err()
                {
                    return;
                }
            }
        });
        self.running.insert(
            id,
            Running {
                child,
                input,
                bound: None,
            },
        );
    }

    /// Kills node `id`'s process, as a power cut takes it.
    fn kill(&mut self, id: u64) {
        let mut running = self.running.remove(&id).unwrap();
        running.child.kill().unwrap();
        running.child.wait().unwrap();
        self.status.remove(&id);
    }

    /// The quiet period: the longest patience any node states, or time until a peer it dials is
    /// connected again, never less than the initial RTO.
    fn quiet(&self) -> Duration {
        self.status
            .values()
            .filter_map(|l| match l {
                Line::Status {
                    patience,
                    reconnect,
                    ..
                } => Some((*patience).max(*reconnect)),
                Line::Bound(..) => None,
            })
            .fold(INITIAL_RTO, Duration::max)
    }

    /// Takes lines until `done` holds; fails once a quiet period passes with none.
    fn until<T>(&mut self, mut done: impl FnMut(&mut Self) -> Option<T>) -> T {
        self.moved_at = Instant::now();
        loop {
            if let Some(found) = done(self) {
                return found;
            }
            let left = (self.moved_at + self.quiet()).saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok((id, line)) => self.take(id, line),
                Err(_) => panic!(
                    "quiet for {:?} with nothing moved: {:?}",
                    self.quiet(),
                    self.status
                ),
            }
        }
    }

    fn take(&mut self, id: u64, line: Line) {
        // A line from a process killed since is the old process's.
        let Some(running) = self.running.get_mut(&id) else {
            return;
        };
        match line {
            Line::Bound(quic, plane) => running.bound = Some((quic, plane)),
            Line::Status { .. } => {
                self.status.insert(id, line);
            }
        }
        self.moved_at = Instant::now();
    }

    /// Waits until every running node listens, then tells each where the others do.
    fn introduce(&mut self) {
        self.until(|c| {
            if c.running.values().all(|r| r.bound.is_some()) {
                Some(())
            } else {
                None
            }
        });
        let bound: Vec<(u64, SocketAddr, SocketAddr)> = self
            .running
            .iter()
            .map(|(id, r)| (*id, r.bound.unwrap().0, r.bound.unwrap().1))
            .collect();
        for (id, running) in &mut self.running {
            for (peer, quic, plane) in bound.iter().filter(|(p, ..)| p != id) {
                writeln!(running.input, "learn {peer} {quic} {plane}").unwrap();
            }
            running.input.flush().unwrap();
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

    fn applied(&self, id: u64) -> (u64, u64) {
        match self.status.get(&id) {
            Some(Line::Status {
                commit, applied, ..
            }) => (*commit, *applied),
            _ => (0, 0),
        }
    }
}

/// The client's shard: its one task, its wait's timer, and its endpoint's socket and one a node,
/// read and written. The watchdog's budget and the wheel's tick are hyper-rt-transport's test
/// harness's (`tests/exchanges.rs`).
fn client_runtime() -> LocalRuntime {
    let sockets = usize::try_from(NODES).unwrap() + 1;
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

fn registration(serial: u64) -> Entry {
    Entry {
        at_ns: 0,
        commands: vec![Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Op::Register,
        }],
    }
}

/// Asks registration `serial` of the range through `client`, given the quiet period and an ask's
/// round besides; takes what the nodes said meanwhile.
async fn ask(cell: &mut Cell, client: &mut Client, serial: u64) -> Vec<Answer> {
    let within = cell.quiet() + INITIAL_RTO;
    let deadline = hyper_rt::futures::now_ns() + u64::try_from(within.as_nanos()).unwrap();
    let answers = client
        .command(GROUP, &registration(serial), deadline)
        .await
        .unwrap_or_else(|e| panic!("registration {serial}: {e}; {:?}", cell.status));
    while let Ok((id, line)) = cell.lines.try_recv() {
        cell.take(id, line);
    }
    answers
}

#[test]
fn a_leader_killed_mid_write_loses_no_answered_write_and_catches_up_when_started_again() {
    // The client's shard runs the whole test: between its commands the test reads the nodes'
    // lines, waiting on its own thread, with no command of the client's out.
    client_runtime()
        .block_on(async move {
            let mut cell = Cell::new();
            let leader = cell.leader();
            let pki = &cell.pki;
            let settings = ClientSettings {
                id: CLIENT,
                bind: "127.0.0.1:0".parse().unwrap(),
                credentials: Credentials {
                    chain: vec![CertificateDer::from(pki.client.0.clone())],
                    key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pki.client.1.clone())),
                    roots: vec![CertificateDer::from(pki.authority_der.clone())],
                },
                nodes: (1..=NODES)
                    .map(|id| Peer {
                        id,
                        role: Role::Node,
                        certificate: pki.node_ders[&id].clone(),
                    })
                    .collect(),
                messages: bounds(),
                transport_bytes: u64::try_from(
                    usize::try_from(NODES).unwrap() * 3 * hyper_transport::RECEIVE_CHUNK,
                )
                .unwrap(),
            };
            let mut client = Client::open(settings).await.unwrap();
            for (id, running) in &cell.running {
                client.learn(*id, running.bound.unwrap().0).unwrap();
            }
            let mut answered: BTreeMap<u64, Vec<Answer>> = BTreeMap::new();
            for serial in 1..=WRITES {
                answered.insert(serial, ask(&mut cell, &mut client, serial).await);
            }
            cell.kill(leader);
            for serial in WRITES + 1..=2 * WRITES {
                answered.insert(serial, ask(&mut cell, &mut client, serial).await);
            }
            cell.start(leader);
            cell.introduce();
            client
                .learn(leader, cell.running[&leader].bound.unwrap().0)
                .unwrap();
            // What the survivors applied, which the node started again must apply. A node's commit
            // as its log holds it may trail what it applied by the commit that moved alone, which
            // rides the group's next write (hyper-durable), so applied is the measure.
            let last = (1..=NODES)
                .filter(|id| *id != leader)
                .map(|id| cell.applied(id).1)
                .max()
                .unwrap();
            cell.until(|c| {
                if c.applied(leader).1 >= last {
                    Some(())
                } else {
                    None
                }
            });
            // Every write was answered once, a registration's answer each.
            assert_eq!(answered.len() as u64, 2 * WRITES);
            for (serial, answers) in &answered {
                assert!(
                    matches!(
                        answers.as_slice(),
                        [Answer::Registered { .. } | Answer::SessionsFull { .. }]
                    ),
                    "registration {serial} answered {answers:?}"
                );
            }
            // Every node ends where the others do.
            cell.until(|c| {
                let ends: Vec<u64> = (1..=NODES).map(|id| c.applied(id).1).collect();
                if ends.windows(2).all(|w| w[0] == w[1]) {
                    Some(())
                } else {
                    None
                }
            });
        })
        .unwrap();
}
