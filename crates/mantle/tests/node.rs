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
//! - A cell hosting a range of each layer, the root range and the Bucket, Name, File and Block
//!   ranges, on every node, voters one to a node: each range elects and answers a session, a Name
//!   command is applied by the Name range and answered `WrongLayer` by the others, and the root
//!   range reads its volume table and refuses a read of another layer's rows; the node leading the most ranges is
//!   killed (SIGKILL), each range serves on, and the node started again catches up on each.
//! - A cell of every layer whose nodes keep volumes: each node enlists itself and its volume in the
//!   root range as it starts, the table a client reads names each volume's node and domain, and
//!   placement over it offers a volume of each node first for every block; started again, a node
//!   enlists again and the table holds what it held; started in another domain, a node is refused
//!   and ends saying why, and enlists again in its own.
//! - A cell of every layer whose nodes keep volumes and host objects: a bucket made, objects of a
//!   byte to 20 MiB put through each node in turn and read back, whole and in part, through
//!   another, each block's chunks on distinct nodes; with one node killed (SIGKILL) every object
//!   reads back through the others, and through it once it is started again.
//! - A storage cell, each node keeping a chunk volume: a gateway, a client in the gateway's role,
//!   puts chunks round the nodes over QUIC, one of each length on each, and one node is killed
//!   (SIGKILL) part way. Every put to a node running is stored, the killed node's until the kill
//!   and lost after; with it down, its peers' chunks read back whole and a put to it is lost; started
//!   again on its directory, it holds the same volume, every chunk acknowledged reads back whole, a
//!   lost put landed whole or not at all and is stored when retried, and each refusal is typed.
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
use hyper_transport::Refusal;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer};
use mantle_cert::{KeyPair, Serial, Validity};
use mantle_chunk::ChunkKey;
use mantle_meta::root::Placed;
use mantle_meta::wire::{Answer, Entry};
use mantle_node::chunk::Status;
use mantle_node::classes::{Bounds, Role};
use mantle_node::client::{Client, ClientSettings};
use mantle_node::client::{ClientError, ObjectOutcome};
use mantle_node::command::Status as CommandStatus;
use mantle_node::directory::{Peer, node_name};
use mantle_node::node::ChunkOutcome;
use mantle_node::object::{Op, Status as ObjectStatus};
use mantle_node::place;
use mantle_node::read::{Query, Read};

#[allow(dead_code)]
#[path = "../../node/tests/support/history.rs"]
mod history;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
use history::{
    Clock, Part, Plan, Steer, Told, check_histories, open_gate, registration, sessioned, workload,
};
#[path = "../../node/tests/support/relay.rs"]
mod relay;
use relay::Relay;

const NODE: &str = env!("CARGO_BIN_EXE_mantle");
/// The Name range's group, the range most tests write through.
const GROUP: u128 = 0x0072_616e_6765;
/// The groups of the cell's root, Bucket, File and Block ranges, where a cell hosts every layer.
const ROOT: u128 = 0x726f_6f74;
const BUCKETS: u128 = 0x6275_636b_6574;
const FILES: u128 = 0x6669_6c65;
const BLOCKS: u128 = 0x62_6c6f_636b;
/// The Name range alone, each group with its layer as a `range` line names it.
const NAME_RANGE: &[(u128, &str)] = &[(GROUP, "name")];
/// The Name range and the root range, where a storage cell's nodes enlist their volumes.
const STORAGE_RANGES: &[(u128, &str)] = &[(ROOT, "root"), (GROUP, "name")];
/// A range of each layer, as a cell of node.md §1.5 hosts them.
const EVERY_LAYER: &[(u128, &str)] = &[
    (ROOT, "root"),
    (BUCKETS, "bucket"),
    (GROUP, "name"),
    (FILES, "file"),
    (BLOCKS, "block"),
];
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
/// The bytes a log group retains (the configuration's `log.group_bytes`): the least that holds one
/// ready of the stated range settings (`mantle_range::ready_held`), so the writes pass it: 3,072
/// bytes in up to 237 entries of the least a range proposes, each with its 5-byte shell.
const GROUP_BYTES: u64 = 3_072 + 237 * 5;
/// The engine's page (the configuration's `engine.page_size`), the engine's own small test
/// configuration's (crates/engine/tests/pool_startup.rs).
const STORE_PAGE: usize = 4096;
/// The engine's memtable (the configuration's `engine.memtable_bytes`): a page's.
const MEMTABLE: usize = STORE_PAGE;

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
log.group_cache = 4096
log.queue_submissions = 16
log.waits = measured
engine.extent_pages = 4
engine.max_extents = 256
engine.fanout = 3
engine.leaf_entries = 96
runtime.probe_budget_ms = 40
chunk_bytes = 8388608
";

/// The largest chunk the cell's gateways write (`chunk_bytes` above): `mantle_gateway::layout::CHUNK`,
/// Tectonic's 8 MiB.
const CHUNK_BYTES: u64 = 8 << 20;
/// The gateway's identity: a client's certificate the nodes of a storage cell name a gateway, which
/// moves chunks (node.md §3.6).
const GATEWAY: u64 = CLIENT + CLIENTS + 1;
/// The bytes of the volume each node of a storage cell keeps, in its directory's `device`, read as a
/// device not measured is (`mantle_chunk::Reads::default`: one read at a time, nothing read past
/// what is asked) and written with no layout pass; its layout, every volume's, is
/// `VOLUME_STATED`: 4 MiB segments, so a chunk of up to a segment's payload, and 4 KiB checksum
/// blocks, chunk-store's own test volume's (crates/chunk/tests/common/mod.rs).
const VOLUME_BYTES: u64 = 64 << 20;
const VOLUME_STATED: &str = "\
volume.segment_bytes = 4194304
volume.checksum_shift = 12
volume.max_fragments = 4096
volume.compact = 1
volume.scrub_period_s = 0
volume.batch_requests = 64
volume.batch_bytes = 4194304
volume.fragments_per_chunk = 64
";

/// The volume of a node that hosts objects: 256 MiB of 16 MiB segments, so a block's whole chunk
/// (`mantle_gateway::layout::CHUNK`, Tectonic's 8 MiB) fits a segment's payload, written in batches
/// of a chunk; the rest as `VOLUME_STATED`.
const OBJECT_VOLUME_BYTES: u64 = 256 << 20;
const OBJECT_VOLUME_STATED: &str = "\
volume.segment_bytes = 16777216
volume.checksum_shift = 12
volume.max_fragments = 4096
volume.compact = 1
volume.scrub_period_s = 0
volume.batch_requests = 64
volume.batch_bytes = 8388608
volume.fragments_per_chunk = 64
";

/// What a node that hosts objects states (`mantle_node::process`'s gateway keys): a block held 30 s
/// for its file's write, two blocks going down or being read at once, four operations at once,
/// S3's 10,000 buckets an owner (research/05 §10.4), 64 rows a coordinator's read; and the
/// durability a block's scheme meets: Backblaze's 2025 drive AFR, 1.36% a drive-year, per hour;
/// repair in a day, the slow end research/15 cites ([Cha12]); S3's eleven nines a year
/// (research/15, "S3's target").
const GATEWAY_STATED: &str = "\
gateway.handover_ns = 30000000000
gateway.window = 2
gateway.objects = 4
gateway.bucket_quota = 10000
gateway.scan_rows = 64
durability.chunk_rate = 0.0000015525114155251142
durability.repair_rate = 0.041666666666666664
durability.target = 0.00000000001
";

/// The cell's root key file in `dir`: 32 bytes only its owner may read.
fn root_key(dir: &Path) -> PathBuf {
    let path = dir.join("root.key");
    std::fs::write(&path, [0x5a; 32]).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path
}

/// The message bounds the nodes state for these settings (`mantle_node::node::message_bounds`):
/// the client states the same.
/// A row's bounds (`rules.max_key_bytes`, `rules.max_value_bytes`): the most of an equal key and
/// value that the engine's page and memtable hold (`mantle_range::holds_rows`).
fn held_row() -> usize {
    use mantle_meta::session::Rules;
    let bound = |x| Rules {
        lifetime_ns: u64::MAX,
        max_sessions: 1,
        max_answers: 1,
        max_answer_bytes: 0,
        expiries_per_entry: 1,
        max_key_bytes: x,
        max_value_bytes: x,
    };
    (0..=STORE_PAGE)
        .rev()
        .find(|&x| mantle_range::holds_rows(STORE_PAGE, MEMTABLE, &bound(x)))
        .unwrap()
}

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
                    max_key_bytes: held_row(),
                    max_value_bytes: held_row(),
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
    /// A replacement the node ended as a range's leader: the range's group, the member lost for
    /// good and the one in its place.
    Replaced(u128, u64, u64),
    /// One of the node's ranges, by its group.
    Status {
        group: u128,
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
    /// A volume the node holds.
    Volume(u128),
    /// The root range took the node's enlistment of itself and its volumes.
    Enlisted,
    /// The bytes the node's transfers moved, said as they move, and its transport's serve period:
    /// the longest an exchange it carries may go without moving before the transport refuses it.
    Moved(u64, Duration),
}

fn parse(line: &str) -> Option<Line> {
    let fields: Vec<&str> = line.split_whitespace().collect();
    match fields.as_slice() {
        ["bound", quic, plane] => Some(Line::Bound(quic.parse().ok()?, plane.parse().ok()?)),
        ["replaced", group, failed, joining] => Some(Line::Replaced(
            u128::from_str_radix(group, 16).ok()?,
            failed.parse().ok()?,
            joining.parse().ok()?,
        )),
        ["link", peer, rest @ ..] => Some(Line::Link(peer.parse().ok()?, rest.join(" "))),
        ["volume", id] => Some(Line::Volume(u128::from_str_radix(id, 16).ok()?)),
        ["enlisted"] => Some(Line::Enlisted),
        ["moved", bytes, serve] => Some(Line::Moved(
            bytes.parse().ok()?,
            Duration::from_nanos(serve.parse().ok()?),
        )),
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
            group,
            term,
            leader,
            commit,
            applied,
            patience,
            reconnect,
            measuring,
            held,
        ] => Some(Line::Status {
            group: u128::from_str_radix(group, 16).ok()?,
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
        Line::Bound(..)
        | Line::Replaced(..)
        | Line::Link(..)
        | Line::Log(..)
        | Line::Volume(..)
        | Line::Enlisted
        | Line::Moved(..) => None,
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
    /// The gateway's certificate's file, its certificate and its key.
    gateway: (PathBuf, Vec<u8>, Vec<u8>),
    /// The cell's root key's first generation, which every node that hosts objects holds
    /// (encryption.md §2): only its owner may read it, as `mantle init` makes it.
    root_key: PathBuf,
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
        let (certificate, pkcs8) = issue(&format!("gateway-{GATEWAY}"));
        let c = dir.join(format!("gateway-{GATEWAY}.der"));
        std::fs::write(&c, &certificate).unwrap();
        Self {
            voters,
            authority,
            authority_der,
            nodes: issued,
            clients,
            gateway: (c, certificate, pkcs8),
            root_key: root_key(dir),
        }
    }

    /// Node `id`'s configuration file in `dir`, hosting `ranges`: on a storage cell, with its
    /// volume and the gateway.
    fn config(&self, id: u64, dir: &Path, shape: (bool, &[(u128, &str)], &str, bool)) -> PathBuf {
        let (stored, ranges, domain, gateway) = shape;
        let (certificate, key, _) = &self.nodes[&id];
        let voters: Vec<String> = (1..=self.voters).map(|v| v.to_string()).collect();
        // Each peer's share holds a receive chunk, the largest datagram QUIC carries, for each of
        // the node's four classes.
        let peers = self.nodes.len() - 1 + self.clients.len() + usize::from(stored);
        let held = held_row();
        let mut text = format!(
            "id = {id}\ndir = {}\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\nauthority = {}\n\
             certificate = {}\nkey = {}\nvoters = {}\n\
             transport_bytes = {}\nlog.group_entries = {GROUP_ENTRIES}\nlog.group_bytes = {GROUP_BYTES}\n\
             engine.page_size = {STORE_PAGE}\nengine.memtable_bytes = {MEMTABLE}\n\
             rules.max_key_bytes = {held}\nrules.max_value_bytes = {held}\n{STATED}",
            dir.display(),
            self.authority.display(),
            certificate.display(),
            key.display(),
            voters.join(","),
            peers * 4 * hyper_transport::RECEIVE_CHUNK,
        );
        for (group, layer) in ranges {
            text.push_str(&format!("range = {group:x} {layer}\n"));
        }
        // What the node asks itself: a gateway's chunks for a put's window of blocks going down,
        // each copied to every node, and its commands and reads; else its one enlistment.
        if gateway {
            text.push_str(&format!(
                "chunk_asks = 12\ncommand_asks = 8\nread_asks = 8\nroot_key = 1 {}\n\
                 {GATEWAY_STATED}",
                self.root_key.display()
            ));
        } else {
            text.push_str("chunk_asks = 0\ncommand_asks = 1\nread_asks = 0\n");
        }
        // Each range's log group, and the liveness stream's own (`mantle_node::node`'s
        // `LIVENESS_GROUP`).
        text.push_str(&format!("log.max_groups = {}\n", ranges.len() + 1));
        for (peer, (c, _, _)) in self.nodes.iter().filter(|(p, _)| **p != id) {
            text.push_str(&format!("peer = {peer} node {}\n", c.display()));
        }
        for (client, (c, _, _)) in &self.clients {
            text.push_str(&format!("peer = {client} client {}\n", c.display()));
        }
        if stored {
            // Its volume, enlisted in the root range in its failure domain; the table holds every
            // node of the cell and a volume of each. A node that hosts objects keeps a volume whose
            // segments hold a block's whole chunk.
            let (bytes, layout) = if gateway {
                (OBJECT_VOLUME_BYTES, OBJECT_VOLUME_STATED)
            } else {
                (VOLUME_BYTES, VOLUME_STATED)
            };
            text.push_str(&format!(
                "peer = {GATEWAY} gateway {}\nvolume = {} {bytes} 1 0 0 0\n{layout}\
                 domain = {domain}\nroot.max_nodes = {nodes}\nroot.max_volumes = {nodes}\n",
                self.gateway.0.display(),
                dir.join("device").display(),
                nodes = self.nodes.len(),
            ));
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
            role: Role::Client,
            messages: bounds(),
            transport_bytes: u64::try_from(self.nodes.len() * 3 * hyper_transport::RECEIVE_CHUNK)
                .unwrap(),
            seed: None,
        }
    }

    /// The gateway's settings: it knows every node, and moves chunks.
    fn gateway_settings(&self) -> ClientSettings {
        let (_, certificate, key) = &self.gateway;
        ClientSettings {
            id: GATEWAY,
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
            role: Role::Gateway,
            messages: bounds(),
            // A share for the request class's replies and the transfer class's, each node's.
            transport_bytes: u64::try_from(self.nodes.len() * 4 * hyper_transport::RECEIVE_CHUNK)
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
    /// Each workload client's operations done, and how long it gives an exchange.
    progress: BTreeMap<u64, u64>,
    clients: BTreeMap<u64, Duration>,
    /// Each node's ranges' statuses as last said, by node and group.
    status: BTreeMap<(u64, u128), Line>,
    /// The replacements a leader ended: the range's group, the member lost for good and the one in
    /// its place.
    replaced: BTreeSet<(u128, u64, u64)>,
    /// Each node's link to each peer as it last said, by node and peer.
    links: BTreeMap<(u64, u64), String>,
    /// Each node's log as it last said, and when that was heard: its flush out, if one was, and the
    /// longest an update has waited for its commit.
    log: BTreeMap<u64, (Instant, Option<Duration>, Duration)>,
    /// The bytes each node's transfers moved as it last said, and its transport's serve period: a
    /// transfer that lives moves within one, so the quiet period is never shorter.
    transfers: BTreeMap<u64, (u64, Duration)>,
    relay: Option<Relay>,
    /// Where the running workload's clients are told of a node started again.
    learns: Vec<Sender<(u64, SocketAddr)>>,
    moved_at: Instant,
    /// Since the last movement, how long the cell held still for its devices, each stretch at
    /// most `Cell::held_bound`, and when the stretch under way began, if one is.
    held_for: Duration,
    held_since: Option<Instant>,
    /// Whether each node keeps a volume, the volume each running node said it holds, and the
    /// nodes whose running process said the root range took its enlistment.
    stored: bool,
    volumes: BTreeMap<u64, u128>,
    enlisted: BTreeSet<u64>,
    /// Each node's failure domain, where a test states another than its own name's.
    domains: BTreeMap<u64, String>,
    /// The node whose process the test started to be refused, and that process once it ended.
    refusing: Option<u64>,
    refused: Option<Running>,
    /// The ranges every node hosts, and whether each hosts objects.
    ranges: &'static [(u128, &'static str)],
    gateway: bool,
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

/// What a cell is: its founding voters, the spares past them that every node knows and no process
/// runs yet, its network, whether each node keeps a volume, and the ranges every node hosts.
struct Shape {
    voters: u64,
    spares: u64,
    network: Network,
    stored: bool,
    ranges: &'static [(u128, &'static str)],
    /// Whether each node hosts objects (a gateway host).
    gateway: bool,
}

impl Cell {
    /// A cell of `nodes` nodes started on `network`, hosting the Name range, each told where the
    /// others listen.
    fn new(nodes: u64, network: Network) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network,
            stored: false,
            ranges: NAME_RANGE,
            gateway: false,
        })
    }

    /// A cell of `nodes` nodes, each keeping a volume, each told where the others listen.
    fn stored(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: true,
            ranges: STORAGE_RANGES,
            gateway: false,
        })
    }

    /// A cell of `nodes` nodes hosting a range of each layer, each keeping a volume.
    fn layered_stored(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: true,
            ranges: EVERY_LAYER,
            gateway: false,
        })
    }

    /// A cell of `nodes` nodes hosting a range of each layer, each keeping a volume and hosting
    /// objects.
    fn gateways(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: true,
            ranges: EVERY_LAYER,
            gateway: true,
        })
    }

    /// A cell of `nodes` nodes hosting a range of each layer, each told where the others listen.
    fn layered(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: false,
            ranges: EVERY_LAYER,
            gateway: false,
        })
    }

    /// A cell of `shape`'s voters, each told where the others listen.
    fn shaped(shape: Shape) -> Self {
        let Shape {
            voters,
            spares,
            network,
            stored,
            ranges,
            gateway,
        } = shape;
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
            transfers: BTreeMap::new(),
            relay: (network == Network::Relayed)
                .then(|| Relay::start(&(1..=nodes).collect::<Vec<_>>())),
            learns: Vec::new(),
            moved_at: Instant::now(),
            held_for: Duration::ZERO,
            held_since: None,
            stored,
            volumes: BTreeMap::new(),
            enlisted: BTreeSet::new(),
            domains: BTreeMap::new(),
            refusing: None,
            refused: None,
            ranges,
            gateway,
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
        self.spawn_process(id, Stdio::inherit());
    }

    /// Starts node `id`'s process on its directory, its standard error to `stderr`, a thread
    /// forwarding its lines, and waits until it listens.
    fn spawn_process(&mut self, id: u64, stderr: Stdio) {
        let config = self.config_of(id);
        let mut child = Command::new(NODE)
            .arg("node")
            .arg(&config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        self.volumes.remove(&id);
        self.enlisted.remove(&id);
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
        let config = self.config_of(id);
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

    /// Node `id`'s configuration file, written for its process: its domain the test's, if it set
    /// one, else its name's.
    fn config_of(&self, id: u64) -> PathBuf {
        let domain = self
            .domains
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("n{id}"));
        self.pki.config(
            id,
            &self.dirs[&id],
            (self.stored, self.ranges, &domain, self.gateway),
        )
    }

    /// Starts node `id`'s process, which must listen and then end on a failure of its own: what
    /// it said on standard error. The cell learns where it listens, and the watch waits for its end
    /// as for any fact the cell reports.
    fn start_refused(&mut self, id: u64) -> String {
        self.refusing = Some(id);
        self.spawn_process(id, Stdio::piped());
        self.introduce();
        let mut running = self.until(|c| c.refused.take());
        self.refusing = None;
        let status = running.child.wait().unwrap();
        let mut said = String::new();
        if let Some(mut stderr) = running.child.stderr.take() {
            std::io::Read::read_to_string(&mut stderr, &mut said).unwrap();
        }
        assert!(!status.success(), "node {id} ended well: {said}");
        said
    }

    /// Overwrites the first two pages of node `id`'s engine store, where both its superblocks
    /// lie, with bytes it never wrote: a device that returned other bytes than it acknowledged.
    fn damage_store(&self, id: u64) {
        let path = self.dirs[&id].join(format!("range-{GROUP:032x}.store"));
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
        eprintln!(
            "diag-test killed node {id} pid {} wall_ms={}",
            running.child.id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        self.status.retain(|(node, _), _| *node != id);
        self.log.remove(&id);
        self.transfers.remove(&id);
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
                Line::Bound(..)
                | Line::Replaced(..)
                | Line::Link(..)
                | Line::Log(..)
                | Line::Volume(..)
                | Line::Enlisted
                | Line::Moved(..) => None,
            })
            .chain(self.clients.values().copied())
            .chain(self.transfers.values().map(|(_, serve)| *serve))
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
            .filter(|id| {
                self.status
                    .range((**id, 0)..=(**id, u128::MAX))
                    .any(|(_, line)| matches!(line, Line::Status { held: true, .. }))
            })
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
                    Line::Replaced(group, failed, joining) => {
                        self.replaced.insert((group, failed, joining));
                    }
                    Line::Link(peer, link) => {
                        self.links.insert((id, peer), link);
                    }
                    Line::Status { group, .. } => {
                        // A status that says only whether the node waits on its device begins
                        // or ends a stretch held still, and moves nothing.
                        let key = (id, group);
                        let moved = self.status.get(&key).and_then(movement) != movement(&line);
                        self.status.insert(key, line);
                        if !moved {
                            self.hold();
                            return;
                        }
                    }
                    Line::Log(flushing, longest) => {
                        self.log.insert(id, (Instant::now(), flushing, longest));
                        return;
                    }
                    Line::Volume(volume) => {
                        self.volumes.insert(id, volume);
                    }
                    Line::Enlisted => {
                        self.enlisted.insert(id);
                    }
                    // Bytes moving are the cell moving, though no range moves with them: a block's
                    // chunks crossing to the nodes that store them commit nothing until all land.
                    Line::Moved(bytes, serve) => {
                        let was = self.transfers.insert(id, (bytes, serve));
                        if was.is_some_and(|(before, _)| before == bytes) {
                            return;
                        }
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
                    // A process the test starts to be refused ends as the test expects.
                    if self.refusing != Some(id) {
                        panic!("node {id} ended: {:?}", self.status);
                    }
                    self.refused = self.running.remove(&id);
                    self.status.retain(|(node, _), _| *node != id);
                    self.log.remove(&id);
                    self.transfers.remove(&id);
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

    /// The leader of `group`'s range every running node names, in one term.
    fn leader(&mut self, group: u128) -> u64 {
        self.until(|c| {
            let named: Vec<(u64, u64)> = c
                .running
                .keys()
                .filter_map(|id| match c.status.get(&(*id, group)) {
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

    /// What node `id` applied of `group`'s range, as it last said; 0 before it said.
    fn applied(&self, id: u64, group: u128) -> u64 {
        match self.status.get(&(id, group)) {
            Some(Line::Status { applied, .. }) => *applied,
            _ => 0,
        }
    }

    /// Waits until every running node has applied as far as the others in `group`'s range. A
    /// node's commit as its log holds it may trail what it applied by the commit that moved alone,
    /// which rides the group's next write (hyper-durable), so applied is the measure.
    fn converged(&mut self, group: u128) -> u64 {
        self.until(|c| {
            let mut ends = c.running.keys().map(|id| c.applied(*id, group));
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

    /// The volume node `id`'s running process holds, once it has said so.
    fn volume(&mut self, id: u64) -> u128 {
        self.until(|c| c.volumes.get(&id).copied())
    }

    /// Starts the gateway on `ops`, a client of its own knowing every node and told where the
    /// running ones listen.
    fn start_gateway(&mut self, ops: Vec<Chunked>) -> JoinHandle<Vec<ChunkOutcome>> {
        self.progress.remove(&GATEWAY);
        self.clients.remove(&GATEWAY);
        let (settings, bound) = (self.pki.gateway_settings(), self.bound_quic());
        let tell = self.tell.clone();
        std::thread::spawn(move || gateway(settings, bound, ops, tell))
    }

    /// Waits until the gateway has done `done` of its operations.
    fn gateway_done(&mut self, done: u64) {
        self.until(|c| {
            if c.progress.get(&GATEWAY).is_some_and(|d| *d >= done) {
                Some(())
            } else {
                None
            }
        });
    }

    /// The outcomes of `ops`, which the gateway does in turn.
    fn through_gateway(&mut self, ops: Vec<Chunked>) -> Vec<ChunkOutcome> {
        let done = ops.len() as u64;
        let thread = self.start_gateway(ops);
        self.gateway_done(done);
        thread.join().unwrap()
    }

    /// What `ops` came to, each an object's request asked of a node with its data, which the
    /// writer client asks in turn, knowing every node and told where the running ones listen.
    fn objects(&mut self, ops: Vec<(u64, Op, Vec<u8>)>) -> Vec<ObjectOutcome> {
        self.progress.remove(&CLIENT);
        self.clients.remove(&CLIENT);
        let done = ops.len() as u64;
        let (settings, bound) = (self.pki.client_settings(CLIENT), self.bound_quic());
        let tell = self.tell.clone();
        let thread = std::thread::spawn(move || objects(settings, bound, ops, tell));
        self.until(|c| {
            if c.progress.get(&CLIENT).is_some_and(|d| *d >= done) {
                Some(())
            } else {
                None
            }
        });
        thread.join().unwrap()
    }

    /// The root range's volume table, read a page at a time through the writer client.
    fn table(&mut self) -> Vec<Placed> {
        let mut table = Vec::new();
        let mut after = None;
        loop {
            let read = self.read(vec![(ROOT, Query::Volumes { after })]);
            let [Ok(Read::Volumes { volumes, more })] = read.as_slice() else {
                panic!("the volume table read {read:?}");
            };
            after = volumes.last().map(|p| p.volume);
            table.extend(volumes.iter().cloned());
            if !more {
                return table;
            }
        }
    }

    /// Waits until node `id`'s running process says the root range took its enlistment.
    fn enlisted(&mut self, id: u64) {
        self.until(|c| {
            if c.enlisted.contains(&id) {
                Some(())
            } else {
                None
            }
        });
    }

    /// What `reads` read, each a query of a range's group, which the writer client asks in turn,
    /// knowing every node and told where the running ones listen.
    fn read(&mut self, reads: Vec<(u128, Query)>) -> Vec<Result<Read, ClientError>> {
        self.progress.remove(&CLIENT);
        self.clients.remove(&CLIENT);
        let done = reads.len() as u64;
        let (settings, bound) = (self.pki.client_settings(CLIENT), self.bound_quic());
        let tell = self.tell.clone();
        let thread = std::thread::spawn(move || reader(settings, bound, reads, tell));
        self.until(|c| {
            if c.progress.get(&CLIENT).is_some_and(|d| *d >= done) {
                Some(())
            } else {
                None
            }
        });
        thread.join().unwrap()
    }

    /// The answers to `asks`, each an entry to a range's group, which the writer client asks in
    /// turn, knowing every node and told where the running ones listen.
    fn asked(&mut self, asks: Vec<(u128, Entry)>) -> Vec<Vec<Answer>> {
        self.progress.remove(&CLIENT);
        self.clients.remove(&CLIENT);
        let done = asks.len() as u64;
        let (settings, bound) = (self.pki.client_settings(CLIENT), self.bound_quic());
        let tell = self.tell.clone();
        let thread = std::thread::spawn(move || asker(settings, bound, asks, tell));
        self.until(|c| {
            if c.progress.get(&CLIENT).is_some_and(|d| *d >= done) {
                Some(())
            } else {
                None
            }
        });
        thread.join().unwrap()
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
                match client
                    .command(GROUP, &registration(id, 1, serial), u64::MAX)
                    .await
                {
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

/// The asker: `asks` in turn through a client of its own, each an entry to a range's group with no
/// deadline of its own, as the writer's commands have none, each answer kept and each told as
/// progress.
fn asker(
    settings: ClientSettings,
    bound: Vec<(u64, SocketAddr)>,
    asks: Vec<(u128, Entry)>,
    tell: Sender<Heard>,
) -> Vec<Vec<Answer>> {
    let id = settings.id;
    client_runtime(settings.nodes.len() + 1)
        .block_on(async move {
            let mut client = Client::open(settings).await.unwrap();
            for (node, quic) in bound {
                client.learn(node, quic).unwrap();
            }
            let mut answered = Vec::with_capacity(asks.len());
            for (done, (group, entry)) in (1..).zip(asks) {
                match client.command(group, &entry, u64::MAX).await {
                    Ok(answers) => answered.push(answers),
                    Err(error) => {
                        let why = format!("command {done} to range {group:x}: {error}");
                        let _ = tell.send(Heard::Told(Told::Failed(id, why)));
                        return answered;
                    }
                }
                let _ = tell.send(Heard::Told(Told::Progress(id, done, client.patience())));
            }
            answered
        })
        .unwrap()
}

/// The object client: `ops` in turn through a client of its own, each an object's request asked of
/// a node with no deadline of its own, each outcome kept and each told as progress.
fn objects(
    settings: ClientSettings,
    bound: Vec<(u64, SocketAddr)>,
    ops: Vec<(u64, Op, Vec<u8>)>,
    tell: Sender<Heard>,
) -> Vec<ObjectOutcome> {
    let id = settings.id;
    client_runtime(settings.nodes.len() + 1)
        .block_on(async move {
            let mut client = Client::open(settings).await.unwrap();
            for (node, quic) in bound {
                client.learn(node, quic).unwrap();
            }
            let mut outcomes = Vec::with_capacity(ops.len());
            for (done, (node, op, data)) in (1..).zip(ops) {
                match client.object(node, &op, &data, u64::MAX).await {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(error) => {
                        let why = format!("object operation {done}: {error}");
                        let _ = tell.send(Heard::Told(Told::Failed(id, why)));
                        return outcomes;
                    }
                }
                let _ = tell.send(Heard::Told(Told::Progress(id, done, client.patience())));
            }
            outcomes
        })
        .unwrap()
}

/// The reader: `reads` in turn through a client of its own, each a query of a range's group with
/// no deadline of its own, each answer or refusal kept and each told as progress.
fn reader(
    settings: ClientSettings,
    bound: Vec<(u64, SocketAddr)>,
    reads: Vec<(u128, Query)>,
    tell: Sender<Heard>,
) -> Vec<Result<Read, ClientError>> {
    let id = settings.id;
    client_runtime(settings.nodes.len() + 1)
        .block_on(async move {
            let mut client = Client::open(settings).await.unwrap();
            for (node, quic) in bound {
                client.learn(node, quic).unwrap();
            }
            let mut read = Vec::with_capacity(reads.len());
            for (done, (group, query)) in (1..).zip(reads) {
                read.push(client.read(group, &query, u64::MAX).await);
                let _ = tell.send(Heard::Told(Told::Progress(id, done, client.patience())));
            }
            read
        })
        .unwrap()
}

/// A chunk operation of the gateway's: a put of bytes, with the CRC-32C it states for them, on a
/// volume a node holds, or a get of a range of a chunk's bytes from one.
enum Chunked {
    Put(u64, u128, ChunkKey, Vec<u8>, u32),
    Get(u64, u128, ChunkKey, (u64, u64)),
}

impl Chunked {
    /// A put of `bytes` with their own CRC-32C.
    fn put(node: u64, volume: u128, key: ChunkKey, bytes: Vec<u8>) -> Self {
        let crc32c = mantle_crc::crc32c(&bytes);
        Self::Put(node, volume, key, bytes, crc32c)
    }
}

/// The gateway: `ops` in turn through a client of its own in the gateway's role, each with no
/// deadline of its own, as the writer's commands have none (an exchange with a node gone ends at
/// its connection's idle timeout, a dial to one at its handshake's, and the test's watch ends one
/// never answered), each outcome kept and each told as progress.
fn gateway(
    settings: ClientSettings,
    bound: Vec<(u64, SocketAddr)>,
    ops: Vec<Chunked>,
    tell: Sender<Heard>,
) -> Vec<ChunkOutcome> {
    client_runtime(settings.nodes.len() + 1)
        .block_on(async move {
            let mut client = Client::open(settings).await.unwrap();
            for (node, quic) in bound {
                client.learn(node, quic).unwrap();
            }
            let mut outcomes = Vec::with_capacity(ops.len());
            for (done, op) in (1..).zip(ops) {
                let outcome = match op {
                    Chunked::Put(node, volume, key, bytes, crc32c) => {
                        client
                            .put_chunk(node, (volume, key), &bytes, crc32c, u64::MAX)
                            .await
                    }
                    Chunked::Get(node, volume, key, range) => {
                        client.get_chunk(node, (volume, key), range, u64::MAX).await
                    }
                };
                match outcome {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(error) => {
                        let why = format!("chunk operation {done}: {error}");
                        let _ = tell.send(Heard::Told(Told::Failed(GATEWAY, why)));
                        return outcomes;
                    }
                }
                let _ = tell.send(Heard::Told(Told::Progress(
                    GATEWAY,
                    done,
                    client.patience(),
                )));
            }
            outcomes
        })
        .unwrap()
}

/// The lengths of the chunks the gateway puts, one of each on each node: a byte; either side of a
/// volume's 4 KiB checksum block; past a datagram's largest payload, the transport's receive chunk
/// (`hyper_transport::RECEIVE_CHUNK`); and up to a mebibyte.
const CHUNK_LENS: [usize; 6] = [1, 4095, 4097, 65_528, 300_001, 1 << 20];

/// Fails at the first outcome that is not the one wanted, naming the operation.
fn outcomes_are(got: &[ChunkOutcome], want: &[ChunkOutcome]) {
    assert_eq!(got.len(), want.len());
    for (n, (got, want)) in got.iter().zip(want).enumerate() {
        assert!(
            got == want,
            "operation {n}: {} where {} was wanted",
            brief(got),
            brief(want)
        );
    }
}

/// An outcome, its bytes by their length and CRC-32C.
fn brief(outcome: &ChunkOutcome) -> String {
    match outcome {
        ChunkOutcome::Read(bytes) => format!(
            "{} bytes read, CRC-32C {:08x}",
            bytes.len(),
            mantle_crc::crc32c(bytes)
        ),
        other => format!("{other:?}"),
    }
}

/// Chunk `n`'s key: a block of its own, the first chunk of the block's first epoch.
fn chunk_key(n: u64) -> ChunkKey {
    ChunkKey {
        block: 0x6368_756e_6b00_0000_0000_0000_0000_0000 | u128::from(n),
        epoch: 1,
        index: 0,
    }
}

/// Chunk `n`'s bytes: `len` of them, each a function of `n` and its place, so a chunk read back
/// from another's bytes, or shifted, differs.
fn chunk_bytes(n: u64, len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| {
            i.wrapping_mul(0x9E37_79B9)
                .wrapping_add(n * 131)
                .to_le_bytes()[0]
        })
        .collect()
}

/// The stated log group's bytes are the least that holds one ready of the stated range settings.
#[test]
fn the_stated_log_group_holds_one_ready_of_the_stated_range() {
    let settings = mantle_range::Settings {
        election_tick: 10,
        heartbeat_tick: 2,
        max_size_per_msg: 1024,
        max_inflight_msgs: 2,
        max_uncommitted_size: 2048,
        max_committed_size_per_ready: 1_048_576,
        max_entry_bytes: 1024,
    };
    let (entries, bytes) = mantle_range::ready_held(&settings).unwrap();
    assert_eq!(bytes, GROUP_BYTES);
    assert!(entries <= GROUP_ENTRIES);
}

#[test]
fn a_leader_killed_mid_write_loses_no_answered_write_and_catches_up_when_started_again() {
    let mut cell = Cell::new(NODES, Network::Direct);
    let leader = cell.leader(GROUP);
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
        .map(|id| cell.applied(id, GROUP))
        .max()
        .unwrap();
    cell.until(|c| {
        if c.applied(leader, GROUP) >= last {
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
    cell.converged(GROUP);
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
    let leader = cell.leader(GROUP);
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
    cell.converged(GROUP);
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
    let mut cell = Cell::shaped(Shape {
        voters,
        spares: 1,
        network: Network::Direct,
        stored: false,
        ranges: NAME_RANGE,
        gateway: false,
    });
    let spare = voters + 1;
    let leader = cell.leader(GROUP);
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
        if c.replaced.contains(&(GROUP, damaged, spare)) {
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
    cell.converged(GROUP);
}

#[test]
fn a_node_whose_store_is_damaged_refuses_it_and_is_replaced_with_no_acknowledged_write_lost() {
    damaged_and_replaced(NODES);
}

#[test]
fn a_node_of_five_voters_whose_store_is_damaged_is_replaced_with_no_acknowledged_write_lost() {
    damaged_and_replaced(MOST_NODES);
}

#[test]
fn chunks_a_node_acknowledged_read_back_whole_after_it_is_killed_mid_put_and_started_again() {
    const KILLED: u64 = 2;
    let mut cell = Cell::stored(NODES);
    let volumes: BTreeMap<u64, u128> = (1..=NODES).map(|id| (id, cell.volume(id))).collect();
    // A round puts one chunk on each node, the rounds in the lengths' order.
    let puts: Vec<(u64, ChunkKey, Vec<u8>)> = (0..CHUNK_LENS.len() as u64 * NODES)
        .map(|n| {
            let node = 1 + n % NODES;
            let len = CHUNK_LENS[usize::try_from(n / NODES).unwrap()];
            (node, chunk_key(n), chunk_bytes(n, len))
        })
        .collect();
    let put = |(node, key, bytes): &(u64, ChunkKey, Vec<u8>)| {
        Chunked::put(*node, volumes[node], *key, bytes.clone())
    };
    let whole = |(node, key, bytes): &(u64, ChunkKey, Vec<u8>)| {
        Chunked::Get(*node, volumes[node], *key, (0, bytes.len() as u64))
    };
    let thread = cell.start_gateway(puts.iter().map(put).collect());
    // Killed once the gateway has done half the rounds and the next round's first put: the killed
    // node's put of that round is out, or about to go, or done.
    let asked = puts.len() as u64 / 2 + 1;
    cell.gateway_done(asked);
    cell.kill(KILLED);
    cell.gateway_done(puts.len() as u64);
    let outcomes = thread.join().unwrap();
    // Every put to a node running is stored; the killed node's are stored until it is killed, the
    // ones done before the kill was asked among them, and lost after.
    let mut lost = Vec::new();
    for (n, ((node, ..), outcome)) in puts.iter().zip(&outcomes).enumerate() {
        match outcome {
            ChunkOutcome::Stored if *node != KILLED || lost.is_empty() => {}
            ChunkOutcome::Lost(_) if *node == KILLED && n as u64 >= asked => lost.push(n),
            other => panic!("put {n} to node {node}: {}", brief(other)),
        }
    }
    // Down, the killed node is not reached; its peers' chunks read back whole from each.
    let running: Vec<_> = puts.iter().filter(|(node, ..)| *node != KILLED).collect();
    let mut ops: Vec<Chunked> = running.iter().copied().map(whole).collect();
    let mut want: Vec<ChunkOutcome> = running
        .iter()
        .map(|(_, _, bytes)| ChunkOutcome::Read(bytes.clone()))
        .collect();
    ops.push(put(&(KILLED, chunk_key(1000), chunk_bytes(1000, 5000))));
    want.push(ChunkOutcome::Lost(Refusal::NotConnected));
    outcomes_are(&cell.through_gateway(ops), &want);
    // Started again on its directory, it holds the same volume; every chunk acknowledged reads
    // back whole, and a lost put landed whole or not at all.
    cell.start(KILLED);
    cell.introduce();
    assert_eq!(cell.volume(KILLED), volumes[&KILLED]);
    let got = cell.through_gateway(puts.iter().map(whole).collect());
    for (n, ((_, _, bytes), outcome)) in puts.iter().zip(&got).enumerate() {
        let read = *outcome == ChunkOutcome::Read(bytes.clone());
        let missing = *outcome == ChunkOutcome::Refused(Status::NotFound) && lost.contains(&n);
        assert!(read || missing, "get {n}: {}", brief(outcome));
    }
    // Each lost put retried is stored, landed or not: a retry's bytes are the first's
    // (chunk-store's audit B05). Then every chunk reads back whole, and in part across its
    // checksum blocks; and each refusal is the volume's or the node's own, typed.
    let mut ops: Vec<Chunked> = lost.iter().map(|n| put(&puts[*n])).collect();
    let mut want = vec![ChunkOutcome::Stored; lost.len()];
    ops.extend(puts.iter().map(whole));
    want.extend(
        puts.iter()
            .map(|(_, _, bytes)| ChunkOutcome::Read(bytes.clone())),
    );
    for (node, key, bytes) in puts.iter().filter(|(_, _, b)| b.len() == 1 << 20) {
        ops.push(Chunked::Get(*node, volumes[node], *key, (4095, 300_001)));
        want.push(ChunkOutcome::Read(bytes[4095..4095 + 300_001].to_vec()));
    }
    let (node, key, bytes) = &puts[0];
    let changed = mantle_crc::crc32c(bytes) ^ 1;
    ops.push(Chunked::Put(
        *node,
        volumes[node],
        chunk_key(1001),
        bytes.clone(),
        changed,
    ));
    want.push(ChunkOutcome::Refused(Status::Checksum));
    ops.push(Chunked::Get(*node, volumes[node], chunk_key(1001), (0, 1)));
    want.push(ChunkOutcome::Refused(Status::NotFound));
    ops.push(Chunked::Get(
        *node,
        volumes[node],
        *key,
        (bytes.len() as u64, 1),
    ));
    want.push(ChunkOutcome::Refused(Status::Range));
    ops.push(put(&(*node, chunk_key(1002), bytes.clone())));
    want.push(ChunkOutcome::Stored);
    ops.push(Chunked::put(
        *node,
        volumes[&KILLED],
        chunk_key(1003),
        bytes.clone(),
    ));
    want.push(ChunkOutcome::Refused(Status::NoVolume));
    outcomes_are(&cell.through_gateway(ops), &want);
}

#[test]
fn a_cell_hosts_a_range_of_each_layer_one_voter_a_node_and_each_serves_through_a_kill() {
    let mut cell = Cell::layered(NODES);
    for (group, _) in EVERY_LAYER {
        cell.leader(*group);
    }
    // A session with each range, and a Name command in each: the Name range applies it, every other
    // answers that it holds another layer's rows.
    let sessions = |answered: &[Vec<Answer>]| -> Vec<u64> {
        answered
            .iter()
            .map(|answers| match answers.as_slice() {
                [Answer::Registered { session }] => *session,
                other => panic!("a registration answered {other:?}"),
            })
            .collect()
    };
    let registered = cell.asked(
        (1..)
            .zip(EVERY_LAYER)
            .map(|(n, (group, _))| (*group, registration(CLIENT, 1, n)))
            .collect(),
    );
    let opened = cell.asked(
        EVERY_LAYER
            .iter()
            .zip(sessions(&registered))
            .map(|((group, _), session)| (*group, sessioned(session, 1, open_gate())))
            .collect(),
    );
    for ((group, layer), answers) in EVERY_LAYER.iter().zip(&opened) {
        let want = if *group == GROUP {
            Answer::Name(mantle_meta::name::Outcome::GateMoved)
        } else {
            Answer::WrongLayer
        };
        assert_eq!(answers, &vec![want], "the {layer} range");
    }
    // The root range's volume table, empty in a cell no node of which keeps a volume, and a read
    // of the Name layer's rows asked of it, refused.
    let read = cell.read(vec![
        (ROOT, Query::Volumes { after: None }),
        (
            ROOT,
            Query::Current {
                bucket: "b".into(),
                key: "k".into(),
            },
        ),
    ]);
    assert!(
        matches!(
            read.as_slice(),
            [
                Ok(Read::Volumes { volumes, more: false }),
                Err(ClientError::Status(CommandStatus::WrongLayer)),
            ] if volumes.is_empty()
        ),
        "{read:?}"
    );
    // The node leading the most ranges killed: each range has a quorum of its voters, one to a
    // node, and serves on; started again, the node catches up on each.
    let mut leaders: BTreeMap<u64, usize> = BTreeMap::new();
    for (group, _) in EVERY_LAYER {
        *leaders.entry(cell.leader(*group)).or_default() += 1;
    }
    let killed = leaders
        .iter()
        .max_by_key(|(_, led)| **led)
        .map(|(id, _)| *id)
        .unwrap();
    cell.kill(killed);
    let again = cell.asked(
        (10..)
            .zip(EVERY_LAYER)
            .map(|(n, (group, _))| (*group, registration(CLIENT, 1, n)))
            .collect(),
    );
    sessions(&again);
    cell.start(killed);
    cell.introduce();
    for (group, _) in EVERY_LAYER {
        cell.converged(*group);
    }
}

#[test]
fn each_node_enlists_its_volume_and_placement_offers_one_to_a_domain() {
    let mut cell = Cell::layered_stored(NODES);
    for id in 1..=NODES {
        cell.enlisted(id);
    }
    let volumes: BTreeMap<u64, u128> = (1..=NODES).map(|id| (id, cell.volume(id))).collect();
    let table = cell.table();
    assert_eq!(table.len(), volumes.len(), "{table:?}");
    for placed in &table {
        assert_eq!(volumes[&placed.node], placed.volume);
        assert_eq!(placed.domain, format!("n{}", placed.node));
        assert!(placed.bytes > 0);
    }
    // Every block is offered a volume of each node, each its own domain, before any other.
    let node_of: BTreeMap<u128, u64> = table.iter().map(|p| (p.volume, p.node)).collect();
    for n in 0..1_000u128 {
        let block = n.wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C834);
        let order = place::order(block, &table);
        let first: BTreeSet<u64> = order[..volumes.len()].iter().map(|v| node_of[v]).collect();
        assert_eq!(first.len(), volumes.len(), "block {block:x}: {order:?}");
    }
    // Started again, a node enlists again, and the table holds what it held.
    cell.kill(2);
    cell.start(2);
    cell.introduce();
    cell.enlisted(2);
    assert_eq!(cell.table(), table);
    // Started in another domain, a node is refused, and ends naming the range's answer; in its
    // own domain again, it enlists.
    cell.kill(3);
    cell.domains.insert(3, "elsewhere".into());
    let said = cell.start_refused(3);
    assert!(
        said.contains("refused the node's enlistment") && said.contains("Conflict"),
        "{said}"
    );
    cell.domains.remove(&3);
    cell.start(3);
    cell.introduce();
    cell.enlisted(3);
    assert_eq!(cell.table(), table);
}

/// Object `n`'s bytes: `len` of them, each a function of `n` and its place.
fn object_bytes(n: u64, len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| {
            i.wrapping_mul(0x2545_F491)
                .wrapping_add(n * 7)
                .to_le_bytes()[1]
        })
        .collect()
}

/// The outcome's reply, which must be done, and its bytes.
fn done(outcome: &ObjectOutcome, what: &str) -> Vec<u8> {
    match outcome {
        ObjectOutcome::Replied { reply, bytes } if reply.status == ObjectStatus::Done => {
            bytes.clone()
        }
        other => panic!("{what}: {other:?}"),
    }
}

#[test]
fn objects_round_trip_through_any_node_and_read_back_after_one_is_killed() {
    let mut cell = Cell::gateways(NODES);
    for id in 1..=NODES {
        cell.enlisted(id);
    }
    let made = cell.objects(vec![(1, Op::Create { bucket: "b".into() }, Vec::new())]);
    done(&made[0], "the bucket");
    // Lengths either side of a sealed segment's 64 KiB and across blocks of 8 MiB chunks.
    let lens = [0usize, 1, 65_535, 65_537, 3 << 20, 20 << 20];
    let objects: Vec<(String, Vec<u8>)> = (0u64..)
        .zip(lens)
        .map(|(n, len)| (format!("o{n}"), object_bytes(n, len)))
        .collect();
    let puts = objects
        .iter()
        .enumerate()
        .map(|(n, (key, data))| {
            let op = Op::Put {
                bucket: "b".into(),
                key: key.clone(),
                id: n as u128 + 1,
                length: data.len() as u64,
            };
            (1 + n as u64 % NODES, op, data.clone())
        })
        .collect();
    for (outcome, (key, _)) in cell.objects(puts).iter().zip(&objects) {
        done(outcome, &format!("the put of {key}"));
    }
    // Each read back whole through another node than its put's, and its middle third.
    let reads = |through: &dyn Fn(usize) -> u64| -> Vec<(u64, Op, Vec<u8>)> {
        let mut ops = Vec::new();
        for (n, (key, data)) in objects.iter().enumerate() {
            let get = |offset: u64, len: u64| Op::Get {
                bucket: "b".into(),
                key: key.clone(),
                offset,
                len,
            };
            ops.push((through(n), get(0, u64::MAX), Vec::new()));
            let third = data.len() as u64 / 3;
            if third > 0 {
                ops.push((through(n), get(third, third), Vec::new()));
            }
        }
        ops
    };
    let check = |got: Vec<ObjectOutcome>, how: &str| {
        let mut got = got.iter();
        for (key, data) in &objects {
            assert!(
                done(got.next().unwrap(), &format!("{key} {how}")) == *data,
                "{key} {how}: other bytes"
            );
            let third = data.len() / 3;
            if third > 0 {
                let part = done(got.next().unwrap(), &format!("{key}'s third {how}"));
                assert!(part == data[third..2 * third], "{key}'s third {how}");
            }
        }
    };
    let got = cell.objects(reads(&|n| 1 + (n as u64 + 1) % NODES));
    check(got, "through another node");
    // Each block's chunks on distinct nodes.
    let node_of: BTreeMap<u128, u64> = cell.table().iter().map(|p| (p.volume, p.node)).collect();
    for (key, data) in &objects {
        let current = cell.read(vec![(
            GROUP,
            Query::Current {
                bucket: "b".into(),
                key: key.clone(),
            },
        )]);
        let [Ok(Read::Current(Some((_, version))))] = current.as_slice() else {
            panic!("{key}: {current:?}");
        };
        let Some(file) = version.file else {
            assert!(data.is_empty(), "{key} has bytes and no file");
            continue;
        };
        let extents = cell.read(vec![(
            FILES,
            Query::Extents {
                file,
                offset: 0,
                max: 16,
            },
        )]);
        let [Ok(Read::Extents(extents))] = extents.as_slice() else {
            panic!("{key}'s extents: {extents:?}");
        };
        for (_, extent) in extents {
            let mantle_meta::record::Target::Block(block) = extent.target else {
                panic!("{key}: an extent of a file");
            };
            let read = cell.read(vec![(BLOCKS, Query::Block { block })]);
            let [Ok(Read::Block(Some((_, places))))] = read.as_slice() else {
                panic!("{key}'s block: {read:?}");
            };
            let nodes: BTreeSet<u64> = places.iter().map(|p| node_of[&p.volume]).collect();
            assert_eq!(
                nodes.len(),
                places.len(),
                "{key}'s block {block:x}: {places:?}"
            );
        }
    }
    // Each node in turn killed: every object reads back through the other two; started again,
    // it serves them too.
    for killed in 1..=NODES {
        cell.kill(killed);
        let others: Vec<u64> = (1..=NODES).filter(|id| *id != killed).collect();
        let got = cell.objects(reads(&|n| others[n % others.len()]));
        check(got, &format!("with node {killed} killed"));
        cell.start(killed);
        cell.introduce();
        cell.enlisted(killed);
        let got = cell.objects(reads(&|_| killed));
        check(got, &format!("through node {killed} started again"));
    }
}
