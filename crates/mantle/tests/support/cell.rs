//! The harness `crates/mantle/tests/node.rs` and `crates/mantle/tests/serve.rs` share: a cell of
//! real `mantle node` or `mantle serve` processes on loopback, its authority and identities, the
//! lines its processes say, the watch that waits on them (node.rs's module comment), and the
//! clients that drive it. Each test binary is a process of its own, whose message bounds it states
//! once (`mantle_protocol::classes::state_bounds`): a cell of `mantle serve` processes, whose bounds
//! its nodes derive, runs in a binary apart from the cells of stated settings.

pub(crate) use std::collections::{BTreeMap, BTreeSet};
pub(crate) use std::io::{BufRead, BufReader, Write};
pub(crate) use std::net::SocketAddr;
pub(crate) use std::path::{Path, PathBuf};
pub(crate) use std::process::{Child, ChildStdin, Command, Stdio};
pub(crate) use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
pub(crate) use std::thread::JoinHandle;
pub(crate) use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) use crate::history::{
    Clock, Part, Plan, Steer, Told, check_histories, open_gate, registration, sessioned, workload,
};
pub(crate) use crate::relay::Relay;
pub(crate) use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
pub(crate) use hyper_transport::Refusal;
pub(crate) use hyper_transport::tls::{
    CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer,
};
pub(crate) use mantle_cert::{KeyPair, Serial, Validity};
pub(crate) use mantle_chunk::ChunkKey;
pub(crate) use mantle_meta::root::Placed;
pub(crate) use mantle_meta::wire::{Answer, Entry};
pub(crate) use mantle_node::chunk::Status;
pub(crate) use mantle_node::client::{Client, ClientSettings};
pub(crate) use mantle_node::client::{ClientError, ObjectOutcome};
pub(crate) use mantle_node::command::Status as CommandStatus;
pub(crate) use mantle_node::directory::Peer;
pub(crate) use mantle_node::node::ChunkOutcome;
pub(crate) use mantle_node::place;
pub(crate) use mantle_node::read::{Query, Read};
pub(crate) use mantle_protocol::classes::{Bounds, Role};
pub(crate) use mantle_protocol::object::{
    Headers, List, Listing, Op, Reply, Status as ObjectStatus,
};
pub(crate) use mantle_protocol::{client_name, node_name};

pub(crate) const NODE: &str = env!("CARGO_BIN_EXE_mantle");
/// The Name range's group, the range most tests write through.
pub(crate) const GROUP: u128 = 0x0072_616e_6765;
/// The groups of the cell's root, Bucket, File and Block ranges, where a cell hosts every layer.
pub(crate) const ROOT: u128 = 0x726f_6f74;
pub(crate) const BUCKETS: u128 = 0x6275_636b_6574;
pub(crate) const FILES: u128 = 0x6669_6c65;
pub(crate) const BLOCKS: u128 = 0x62_6c6f_636b;
/// The Name range alone, each group with its layer as a `range` line names it.
pub(crate) const NAME_RANGE: &[(u128, &str)] = &[(GROUP, "name")];
/// The Name range and the root range, where a storage cell's nodes enlist their volumes.
pub(crate) const STORAGE_RANGES: &[(u128, &str)] = &[(ROOT, "root"), (GROUP, "name")];
/// A range of each layer, as a cell of node.md §1.5 hosts them.
pub(crate) const EVERY_LAYER: &[(u128, &str)] = &[
    (ROOT, "root"),
    (BUCKETS, "bucket"),
    (GROUP, "name"),
    (FILES, "file"),
    (BLOCKS, "block"),
];
/// The nodes of most cells, each a founding voter of the range.
pub(crate) const NODES: u64 = 3;
/// The most nodes a cell has: five, the larger of the cells node.md's Phase C asks histories of.
pub(crate) const MOST_NODES: u64 = 5;
/// The writer client's id, past every node's of the largest cell; the workload's clients follow.
/// Each asks the ranges for itself, a gateway's work, and is pinned as one (node.md §3.6).
pub(crate) const CLIENT: u64 = MOST_NODES + 1;
/// The workload's clients, each its own identity and session.
pub(crate) const CLIENTS: u64 = 3;
/// RFC 6298 §2.1: the retransmission timeout before any round trip is measured, one second.
pub(crate) const INITIAL_RTO: Duration = Duration::from_secs(1);
/// RFC 5280 §4.1.2.5: the notAfter of a certificate with no well-defined expiration.
pub(crate) const NO_EXPIRY: u64 = 253_402_300_799;
/// Writes before the leader is killed and after: enough to pass the range's log group's room
/// (`log.group_bytes` below) on each side of the kill.
pub(crate) const WRITES: u64 = 200;
/// The entries a log group retains (the configuration's `log.group_entries`).
pub(crate) const GROUP_ENTRIES: u64 = 1024;
/// The bytes a log group retains (the configuration's `log.group_bytes`): the least that holds one
/// ready of the stated range settings (`mantle_range::ready_held`), so the writes pass it: 3,072
/// bytes in up to 237 entries of the least a range proposes, each with its 5-byte shell.
pub(crate) const GROUP_BYTES: u64 = 3_072 + 237 * 5;
/// The engine's page (the configuration's `engine.page_size`), the engine's own small test
/// configuration's (crates/engine/tests/pool_startup.rs).
pub(crate) const STORE_PAGE: usize = 4096;
/// The engine's memtable (the configuration's `engine.memtable_bytes`): a page's.
pub(crate) const MEMTABLE: usize = STORE_PAGE;

/// Every number the nodes run with, as the range's process test and the cluster test state them:
/// the range's settings and rules, the log's and the engine's sizes (the engine's own small test
/// configuration, crates/engine/tests/pool_startup.rs), and the calibration's probe budget the
/// engine's bench gives each probe.
pub(crate) const STATED: &str = "\
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
stop_ms = 10000
chunk_bytes = 8388608
";

/// The largest chunk the cell's gateways write (`chunk_bytes` above): `mantle_gateway::layout::CHUNK`,
/// Tectonic's 8 MiB.
pub(crate) const CHUNK_BYTES: u64 = 8 << 20;
/// The gateway's identity: a client's certificate the nodes of a storage cell name a gateway, which
/// moves chunks (node.md §3.6).
pub(crate) const GATEWAY: u64 = CLIENT + CLIENTS + 1;
/// The clients of objects: client `CLIENT + n`'s object operations are asked by the client
/// `OBJECT_CLIENT + n`, a certificate the authority issued for the client role, pinned nowhere and
/// acting for an account of its own (node.md §3.6), its id past every pinned peer's.
pub(crate) const OBJECT_CLIENT: u64 = GATEWAY + 1;

/// The object client that asks client `client`'s object operations.
pub(crate) fn object_client(client: u64) -> u64 {
    OBJECT_CLIENT + (client - CLIENT)
}

/// The account object client `id` acts for: twelve digits, its own.
pub(crate) fn account(id: u64) -> String {
    format!("{id:012}")
}

/// The object clients' certificates and keys, by id, each issued for the client role by the
/// authority holding `key` and named `name`.
pub(crate) fn object_clients(
    key: &KeyPair,
    name: &str,
    validity: Validity,
) -> BTreeMap<u64, (Vec<u8>, Vec<u8>)> {
    let mut objects = BTreeMap::new();
    for client in CLIENT..=CLIENT + CLIENTS {
        let id = object_client(client);
        let own = KeyPair::generate().unwrap();
        let certificate = mantle_cert::client(
            key,
            name,
            own.public_key(),
            (&client_name(id), &account(id)),
            (validity, Serial::draw().unwrap()),
        )
        .unwrap();
        objects.insert(id, (certificate, own.pkcs8().to_vec()));
    }
    objects
}
/// The bytes of the volume each node of a storage cell keeps, in its directory's `device`, read as a
/// device not measured is (`mantle_chunk::Reads::default`: one read at a time, nothing read past
/// what is asked) and written with no layout pass; its layout, every volume's, is
/// `VOLUME_STATED`: 4 MiB segments, so a chunk of up to a segment's payload, and 4 KiB checksum
/// blocks, chunk-store's own test volume's (crates/chunk/tests/common/mod.rs).
pub(crate) const VOLUME_BYTES: u64 = 64 << 20;
pub(crate) const VOLUME_STATED: &str = "\
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
pub(crate) const OBJECT_VOLUME_BYTES: u64 = 256 << 20;
pub(crate) const OBJECT_VOLUME_STATED: &str = "\
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
/// for its file's write, two blocks going down or being read at once, four operations at once, as
/// many clients as the cell's object clients, S3's 10,000 buckets an owner (research/05 §10.4), 64 rows a coordinator's read; and the
/// durability a block's scheme meets: Backblaze's 2025 drive AFR, 1.36% a drive-year, per hour;
/// repair in a day, the slow end research/15 cites ([Cha12]); S3's eleven nines a year
/// (research/15, "S3's target").
pub(crate) const GATEWAY_STATED: &str = "\
gateway.handover_ns = 30000000000
gateway.window = 2
gateway.objects = 4
gateway.clients = 4
gateway.bucket_quota = 10000
gateway.scan_rows = 64
durability.chunk_rate = 0.0000015525114155251142
durability.repair_rate = 0.041666666666666664
durability.target = 0.00000000001
";

/// The cell's root key file in `dir`: 32 bytes only its owner may read.
pub(crate) fn root_key(dir: &Path) -> PathBuf {
    let path = dir.join("root.key");
    key_file(&path, &[0x5a; 32]);
    path
}

/// A key file at `path` holding `bytes`, readable by its owner alone from the moment it exists, as
/// `mantle init` writes one and `mantle serve` reads one (`mantle_node::secret`).
pub(crate) fn key_file(path: &Path, bytes: &[u8]) {
    mantle_node::secret::create(path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
}

/// The message bounds the nodes state for these settings (`mantle_node::node::message_bounds`):
/// the client states the same.
/// A row's bounds (`rules.max_key_bytes`, `rules.max_value_bytes`): the most of an equal key and
/// value that the engine's page and memtable hold (`mantle_range::holds_rows`).
pub(crate) fn held_row() -> usize {
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

pub(crate) fn bounds() -> Bounds {
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

/// A value a `mantle serve` process says it runs with: `basis KEY = VALUE (KIND: WHY)`
/// (`mantle_node::derive::Basis`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Basis {
    pub(crate) key: String,
    pub(crate) value: String,
    pub(crate) kind: String,
    pub(crate) why: String,
}

impl Basis {
    pub(crate) fn parse(said: &str) -> Option<Self> {
        let (key, rest) = said.split_once(" = ")?;
        let (value, rest) = rest.split_once(" (")?;
        let (kind, why) = rest.split_once(": ")?;
        Some(Self {
            key: key.to_owned(),
            value: value.to_owned(),
            kind: kind.to_owned(),
            why: why.strip_suffix(')')?.to_owned(),
        })
    }
}

/// The message bounds `mantle serve`'s nodes derive for their ranges (`mantle_node::derive::cell`,
/// whose settings no identity count changes): their clients state the same.
pub(crate) fn served_bounds() -> Bounds {
    use mantle_meta::apply::Layer;
    use mantle_node::node::{Hosted, message_bounds};
    use mantle_range::{ConfState, Range};
    let (settings, rules) = mantle_node::derive::cell(1).unwrap();
    message_bounds(
        &[Hosted {
            group: GROUP,
            range: Range {
                layer: Layer::Name,
                rules,
                boot: ConfState::default(),
                settings,
            },
            engine: (),
        }],
        CHUNK_BYTES,
    )
    .unwrap()
}

/// A node's report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Line {
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
        /// While a command or read the node asks of the range itself waits paused, the longest
        /// the pause may last (`Status::asking`, else zero).
        asking: Duration,
    },
    /// Whether the node waits on its own device (`Node::held`), said as it changes.
    Held(bool),
    /// The node's log as it said with its last status: how long its flush had been out, if one
    /// was, and the longest an update has waited for its commit.
    Log(Option<Duration>, Duration),
    /// A volume the node holds.
    Volume(u128),
    /// The S3 endpoint a node read its keys for: where it listens, the region it serves and its
    /// access keys' ids.
    S3(SocketAddr, String, Vec<String>),
    /// Where the node's S3 listener listens, once bound, and the connections it holds at once.
    S3Bound(SocketAddr, u64),
    /// The node's chunk bytes as a `chunks` line asked: received over QUIC, put on its own
    /// volumes in-process, and held by its volumes.
    Chunks(u64, u64, u64),
    /// The root range took the node's enlistment of itself and its volumes.
    Enlisted,
    /// What the node's failure detectors start from (node.md §2.4, item 7): the cell's node time
    /// run and crashes, and this node's part of them.
    History(Lived, Lived),
    /// A value a `mantle serve` process runs with, said before it listens.
    Basis(Basis),
    /// The bytes the node's transfers moved, said as they move, and its transport's serve period:
    /// the longest an exchange it carries may go without moving before the transport refuses it.
    Moved(u64, Duration),
    /// The node began its orderly stop at a signal.
    Stopping,
    /// Its orderly stop began to hand a range's lead over: the range's group, and the voter it goes
    /// to.
    Handing(u128, u64),
    /// Its orderly stop handed a range's lead over: the range's group, and the member that then
    /// led it.
    Handed(u128, u64),
    /// Its orderly stop is done: the process ends.
    Stopped,
}

/// A history as a node says it: node time run and crashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Lived {
    pub(crate) exposure: Duration,
    pub(crate) crashes: u64,
}

pub(crate) fn parse(line: &str) -> Option<Line> {
    if let Some(said) = line.strip_prefix("basis ") {
        return Basis::parse(said).map(Line::Basis);
    }
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
        ["chunks", received, written, held] => Some(Line::Chunks(
            received.parse().ok()?,
            written.parse().ok()?,
            held.parse().ok()?,
        )),
        ["s3-bound", at, connections] => {
            Some(Line::S3Bound(at.parse().ok()?, connections.parse().ok()?))
        }
        ["s3", listen, region, ids] => Some(Line::S3(
            listen.parse().ok()?,
            (*region).to_owned(),
            ids.split(',').map(str::to_owned).collect(),
        )),
        ["enlisted"] => Some(Line::Enlisted),
        ["history", cell_ns, cell_crashes, own_ns, own_crashes] => Some(Line::History(
            Lived {
                exposure: Duration::from_nanos(cell_ns.parse().ok()?),
                crashes: cell_crashes.parse().ok()?,
            },
            Lived {
                exposure: Duration::from_nanos(own_ns.parse().ok()?),
                crashes: own_crashes.parse().ok()?,
            },
        )),
        ["stopping"] => Some(Line::Stopping),
        ["handing", group, to] => Some(Line::Handing(
            u128::from_str_radix(group, 16).ok()?,
            to.parse().ok()?,
        )),
        ["handed", group, leader] => Some(Line::Handed(
            u128::from_str_radix(group, 16).ok()?,
            leader.parse().ok()?,
        )),
        ["stopped"] => Some(Line::Stopped),
        ["held", held] => Some(Line::Held(match *held {
            "0" => false,
            "1" => true,
            _ => return None,
        })),
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
            asking,
        ] => Some(Line::Status {
            group: u128::from_str_radix(group, 16).ok()?,
            term: term.parse().ok()?,
            leader: leader.parse().ok()?,
            commit: commit.parse().ok()?,
            applied: applied.parse().ok()?,
            patience: Duration::from_nanos(patience.parse().ok()?),
            reconnect: Duration::from_nanos(reconnect.parse().ok()?),
            measuring: Duration::from_nanos(measuring.parse().ok()?),
            asking: Duration::from_nanos(asking.parse().ok()?),
        }),
        _ => None,
    }
}

/// What of a status is movement: all of it.
pub(crate) fn movement(line: &Line) -> Option<(u64, u64, u64, u64, Duration, Duration, Duration)> {
    match line {
        Line::Status {
            term,
            leader,
            commit,
            applied,
            reconnect,
            measuring,
            asking,
            ..
        } => Some((
            *term, *leader, *commit, *applied, *reconnect, *measuring, *asking,
        )),
        Line::Bound(..)
        | Line::Replaced(..)
        | Line::Link(..)
        | Line::Log(..)
        | Line::Held(..)
        | Line::Volume(..)
        | Line::S3(..)
        | Line::S3Bound(..)
        | Line::Chunks(..)
        | Line::Enlisted
        | Line::History(..)
        | Line::Basis(..)
        | Line::Moved(..)
        | Line::Stopping
        | Line::Handing(..)
        | Line::Handed(..)
        | Line::Stopped => None,
    }
}

/// What the test hears: a process's line, by the node and the process's generation, or what a
/// workload client tells.
pub(crate) enum Heard {
    Line(u64, u64, Line),
    /// A process's output ended, by the node and the process's generation: the process ended.
    Ended(u64, u64),
    Told(Told),
}

/// The files of the cell's authority, each node's certificate and key, and the clients'.
pub(crate) struct Pki {
    /// The range's founding voters: nodes 1 to `voters`. A node past them is a spare.
    pub(crate) voters: u64,
    pub(crate) authority: PathBuf,
    pub(crate) authority_der: Vec<u8>,
    /// By node id: the certificate's file and the key's, and the certificate.
    pub(crate) nodes: BTreeMap<u64, (PathBuf, PathBuf, Vec<u8>)>,
    /// By client id: the certificate's file, the certificate and the key.
    pub(crate) clients: BTreeMap<u64, (PathBuf, Vec<u8>, Vec<u8>)>,
    /// By object client id: its certificate, the client role's, and its key.
    pub(crate) objects: BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
    /// The gateway's certificate's file, its certificate and its key.
    pub(crate) gateway: (PathBuf, Vec<u8>, Vec<u8>),
    /// The cell's root key's first generation, which every node that hosts objects holds
    /// (encryption.md §2): only its owner may read it, as `mantle init` makes it.
    pub(crate) root_key: PathBuf,
    /// Whether the nodes run as `mantle serve`, whose range settings they derive, which the
    /// clients' message bounds follow.
    pub(crate) serving: bool,
}

impl Pki {
    /// The authority, `nodes` nodes' certificates and keys, the first `voters` of them the
    /// range's founding voters, and the clients', written in `dir`.
    pub(crate) fn new(dir: &Path, voters: u64, nodes: u64) -> Self {
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
            key_file(&k, &pkcs8);
            issued.insert(id, (c, k, certificate));
        }
        let mut clients = BTreeMap::new();
        for id in CLIENT..=CLIENT + CLIENTS {
            let (certificate, pkcs8) = issue(&format!("client-{id}"));
            let c = dir.join(format!("client-{id}.der"));
            std::fs::write(&c, &certificate).unwrap();
            clients.insert(id, (c, certificate, pkcs8));
        }
        let objects = object_clients(&key, "mantle test cell", validity);
        let (certificate, pkcs8) = issue(&format!("gateway-{GATEWAY}"));
        let c = dir.join(format!("gateway-{GATEWAY}.der"));
        std::fs::write(&c, &certificate).unwrap();
        Self {
            voters,
            authority,
            authority_der,
            nodes: issued,
            clients,
            objects,
            gateway: (c, certificate, pkcs8),
            root_key: root_key(dir),
            serving: false,
        }
    }

    /// The message bounds the nodes state, which their clients state the same, and the least
    /// transport a client of `role` holds for them as `Client::open` states its own.
    pub(crate) fn messages(&self, role: Role) -> (Bounds, u64) {
        let messages = if self.serving {
            served_bounds()
        } else {
            bounds()
        };
        let least = mantle_protocol::bounds::least_transport(&mantle_protocol::bounds::Stated {
            peers: self.nodes.len(),
            ranges: 1,
            inflight: 0,
            transfers: usize::from(role == Role::Gateway),
            transport_bytes: 0,
            messages,
            max_head: mantle_node::node::MAX_HEAD,
        })
        .unwrap();
        (messages, least)
    }

    /// Node `id`'s configuration file in `dir`, hosting `ranges`: on a storage cell, with its
    /// volume and the gateway.
    pub(crate) fn config(
        &self,
        id: u64,
        dir: &Path,
        shape: (bool, &[(u128, &str)], &str, bool),
    ) -> PathBuf {
        let (stored, ranges, domain, gateway) = shape;
        let (certificate, key, _) = &self.nodes[&id];
        let voters: Vec<String> = (1..=self.voters).map(|v| v.to_string()).collect();
        // Each peer's share holds a receive chunk, the largest datagram QUIC carries, for each of
        // the node's four classes: its nodes, the pinned clients and gateway, and the object
        // clients a node that hosts objects admits.
        let objects = if gateway { self.objects.len() } else { 0 };
        let peers = self.nodes.len() - 1 + self.clients.len() + usize::from(stored) + objects;
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
            text.push_str(&format!("peer = {client} gateway {}\n", c.display()));
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

    /// Node `id`'s `mantle serve` file in `dir`, its operator's statements alone, hosting
    /// `ranges`: its identity, where it listens, the cell's founding voters, its peers and, on a
    /// storage cell, its volume of the bytes `config`'s takes and its failure domain, and its root
    /// key where it hosts objects. Every other value its process measures or derives.
    pub(crate) fn stated(
        &self,
        id: u64,
        dir: &Path,
        shape: (bool, &[(u128, &str)], &str, bool),
    ) -> PathBuf {
        let (stored, ranges, domain, gateway) = shape;
        let (certificate, key, _) = &self.nodes[&id];
        let voters: Vec<String> = (1..=self.voters).map(|v| v.to_string()).collect();
        let mut text = format!(
            "id = {id}\ndir = {}\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\nauthority = {}\n\
             certificate = {}\nkey = {}\nvoters = {}\n",
            dir.display(),
            self.authority.display(),
            certificate.display(),
            key.display(),
            voters.join(","),
        );
        for (group, layer) in ranges {
            text.push_str(&format!("range = {group:x} {layer}\n"));
        }
        if gateway {
            text.push_str(&format!("root_key = 1 {}\n", self.root_key.display()));
        }
        for (peer, (c, _, _)) in self.nodes.iter().filter(|(p, _)| **p != id) {
            text.push_str(&format!("peer = {peer} node {}\n", c.display()));
        }
        for (client, (c, _, _)) in &self.clients {
            text.push_str(&format!("peer = {client} gateway {}\n", c.display()));
        }
        if stored {
            let bytes = if gateway {
                OBJECT_VOLUME_BYTES
            } else {
                VOLUME_BYTES
            };
            text.push_str(&format!(
                "peer = {GATEWAY} gateway {}\nvolume = {} {bytes}\ndomain = {domain}\n\
                 root.max_nodes = {nodes}\nroot.max_volumes = {nodes}\n",
                self.gateway.0.display(),
                dir.join("device").display(),
                nodes = self.nodes.len(),
            ));
        }
        let path = dir.join("serve.conf");
        std::fs::write(&path, text).unwrap();
        path
    }

    /// Client `id`'s settings: it knows every node, and asks the ranges as a gateway does.
    pub(crate) fn client_settings(&self, id: u64) -> ClientSettings {
        let (_, certificate, key) = &self.clients[&id];
        self.settings(id, (certificate, key), Role::Gateway)
    }

    /// Object client `id`'s settings: it knows every node, and its certificate is the client
    /// role's, which no node pins.
    pub(crate) fn object_settings(&self, id: u64) -> ClientSettings {
        let (certificate, key) = &self.objects[&id];
        self.settings(id, (certificate, key), Role::Client)
    }

    /// The settings of `id`, holding `certificate` and its `key`, as `role`.
    fn settings(&self, id: u64, (certificate, key): (&[u8], &[u8]), role: Role) -> ClientSettings {
        ClientSettings {
            id,
            bind: "127.0.0.1:0".parse().unwrap(),
            credentials: Credentials {
                chain: vec![CertificateDer::from(certificate.to_vec())],
                key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.to_vec())),
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
            role,
            messages: self.messages(role).0,
            transport_bytes: u64::try_from(self.nodes.len() * 3 * hyper_transport::RECEIVE_CHUNK)
                .unwrap()
                .max(self.messages(role).1),
            seed: None,
        }
    }

    /// The gateway's settings: it knows every node, and moves chunks.
    pub(crate) fn gateway_settings(&self) -> ClientSettings {
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
            messages: self.messages(Role::Gateway).0,
            // A share for the request class's replies and the transfer class's, each node's.
            transport_bytes: u64::try_from(self.nodes.len() * 4 * hyper_transport::RECEIVE_CHUNK)
                .unwrap()
                .max(self.messages(Role::Gateway).1),
            seed: None,
        }
    }
}

/// A node's process, its input, where it said it listens, and its generation: the count of
/// processes the cell started before it.
pub(crate) struct Running {
    pub(crate) child: Child,
    pub(crate) input: ChildStdin,
    pub(crate) bound: Option<(SocketAddr, SocketAddr)>,
    pub(crate) generation: u64,
}

/// How the nodes reach each other: directly, or through the test's relay, which partitions them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Network {
    Direct,
    Relayed,
}

/// The cell: each node's directory and process, what the test heard of them and of the clients,
/// and the relay of a partitioned cell.
pub(crate) struct Cell {
    /// The cell's node identities, 1 to `nodes`: the founding voters, then any spares.
    pub(crate) nodes: u64,
    pub(crate) pki: Pki,
    pub(crate) dirs: BTreeMap<u64, PathBuf>,
    pub(crate) running: BTreeMap<u64, Running>,
    pub(crate) generations: u64,
    pub(crate) tell: Sender<Heard>,
    pub(crate) heard: Receiver<Heard>,
    /// Each workload client's operations done, and how long it gives an exchange.
    pub(crate) progress: BTreeMap<u64, u64>,
    pub(crate) clients: BTreeMap<u64, Duration>,
    /// Each node's ranges' statuses as last said, by node and group.
    pub(crate) status: BTreeMap<(u64, u128), Line>,
    /// The replacements a leader ended: the range's group, the member lost for good and the one in
    /// its place.
    pub(crate) replaced: BTreeSet<(u128, u64, u64)>,
    /// Each node's link to each peer as it last said, by node and peer.
    pub(crate) links: BTreeMap<(u64, u64), String>,
    /// Each node's log as it last said, and when that was heard: its flush out, if one was, and the
    /// longest an update has waited for its commit.
    pub(crate) log: BTreeMap<u64, (Instant, Option<Duration>, Duration)>,
    /// The bytes each node's transfers moved as it last said, and its transport's serve period: a
    /// transfer that lives moves within one, so the quiet period is never shorter.
    pub(crate) transfers: BTreeMap<u64, (u64, Duration)>,
    pub(crate) relay: Option<Relay>,
    /// Where the running workload's clients are told of a node started again.
    pub(crate) learns: Vec<Sender<(u64, SocketAddr)>>,
    pub(crate) moved_at: Instant,
    /// Since the last movement, how long the cell held still for its devices, each stretch at
    /// most `Cell::held_bound`, and when the stretch under way began, if one is.
    pub(crate) held_for: Duration,
    pub(crate) held_since: Option<Instant>,
    /// The running nodes whose last word was that they wait on their own device.
    pub(crate) held: BTreeSet<u64>,
    /// Whether each node keeps a volume, the volume each running node said it holds, and the
    /// nodes whose running process said the root range took its enlistment.
    pub(crate) stored: bool,
    pub(crate) volumes: BTreeMap<u64, u128>,
    pub(crate) enlisted: BTreeSet<u64>,
    /// Each running node's S3 endpoint as it said it read its keys.
    pub(crate) s3: BTreeMap<u64, (SocketAddr, String, Vec<String>)>,
    /// Where each running node's S3 listener listens, as it said once bound, and the connections
    /// it holds at once.
    pub(crate) s3_bound: BTreeMap<u64, (SocketAddr, u64)>,
    /// Each node's chunk bytes as it last answered a `chunks` line.
    pub(crate) chunks: BTreeMap<u64, (u64, u64, u64)>,
    /// What each node's failure detectors started from as its latest process said: the cell's
    /// history and its own part of it.
    pub(crate) histories: BTreeMap<u64, (Lived, Lived)>,
    /// Each node's failure domain, where a test states another than its own name's.
    pub(crate) domains: BTreeMap<u64, String>,
    /// The node whose process the test started to be refused, and that process once it ended.
    pub(crate) refusing: Option<u64>,
    pub(crate) refused: Option<Running>,
    /// Diagnostic: when set, every line heard is said with the milliseconds since.
    pub(crate) trace: Option<std::time::Instant>,
    /// The ranges every node hosts, and whether each hosts objects.
    pub(crate) ranges: &'static [(u128, &'static str)],
    pub(crate) gateway: bool,
    /// Whether each node runs as `mantle serve`, and the values its running process said it runs
    /// with, each with its basis, in the order said.
    pub(crate) serving: bool,
    pub(crate) bases: BTreeMap<u64, Vec<Basis>>,
    /// Each range whose lead a node's orderly stop began to hand over: the node, the range's group
    /// and the voter it goes to; each it handed over: the node, the group and the member that then
    /// led it; and the nodes whose stop said it was done.
    pub(crate) handing: Vec<(u64, u128, u64)>,
    pub(crate) handed: Vec<(u64, u128, u64)>,
    pub(crate) stopped: BTreeSet<u64>,
    /// Each node and range whose status said the range had no leader, since the test last cleared
    /// these: one entry a pair, however often it said so.
    pub(crate) leaderless: BTreeSet<(u64, u128)>,
    /// Each node's configuration as `mantle init` wrote it, for a cell it made (`Cell::made`):
    /// its process serves from that file, which the test does not write.
    pub(crate) made: BTreeMap<u64, PathBuf>,
    pub(crate) _root: tempfile::TempDir,
}

/// Sends `child` the signal that stops a node in order: SIGTERM, by the system's `kill`.
pub(crate) fn stop_signal(child: &Child) {
    let sent = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(sent.success(), "kill -TERM {}: {sent}", child.id());
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
pub(crate) struct Shape {
    pub(crate) voters: u64,
    pub(crate) spares: u64,
    pub(crate) network: Network,
    pub(crate) stored: bool,
    pub(crate) ranges: &'static [(u128, &'static str)],
    /// Whether each node hosts objects (a gateway host).
    pub(crate) gateway: bool,
    /// Whether each node runs as `mantle serve` from a file of its operator's statements alone,
    /// every other value measured or derived, else as `mantle node` told every number.
    pub(crate) serving: bool,
}

impl Cell {
    /// A cell of `nodes` nodes started on `network`, hosting the Name range, each told where the
    /// others listen.
    pub(crate) fn new(nodes: u64, network: Network) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network,
            stored: false,
            ranges: NAME_RANGE,
            gateway: false,
            serving: false,
        })
    }

    /// A cell of `nodes` nodes, each keeping a volume, each told where the others listen.
    pub(crate) fn stored(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: true,
            ranges: STORAGE_RANGES,
            gateway: false,
            serving: false,
        })
    }

    /// A cell of `nodes` nodes hosting a range of each layer, each keeping a volume.
    pub(crate) fn layered_stored(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: true,
            ranges: EVERY_LAYER,
            gateway: false,
            serving: false,
        })
    }

    /// A cell of `nodes` nodes hosting a range of each layer, each keeping a volume and hosting
    /// objects.
    pub(crate) fn gateways(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: true,
            ranges: EVERY_LAYER,
            gateway: true,
            serving: false,
        })
    }

    /// A cell of `nodes` nodes run as `mantle serve` from their operators' statements alone,
    /// hosting a range of each layer, each keeping a volume and hosting objects.
    pub(crate) fn served(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: true,
            ranges: EVERY_LAYER,
            gateway: true,
            serving: true,
        })
    }

    /// A cell `mantle init` makes in a directory of the test's from the arguments `args` gives for
    /// that directory, each founder's directory and identity as init wrote them, run as `mantle
    /// serve` from the configuration init wrote, its volumes' and objects' roles as stated, each
    /// told where the others listen. The test's clients are issued by the cell's authority, whose
    /// key init kept, and pinned in each founder's configuration as gateways, a `peer` line each,
    /// as an operator pins a gateway, and its object clients issued for the client role, pinned
    /// nowhere; init's own output is what the cell is read from: the authority's name and
    /// directory, and each founder's id and directory.
    pub(crate) fn made(args: impl FnOnce(&Path) -> Vec<String>) -> Self {
        Self::made_started(args, false, true)
    }

    /// A cell `mantle init` made, its founders started highest id first, each one's configuration
    /// naming the peers already listening by `localhost` and the ports they bound, as an operator
    /// names a peer by its host: a founder learns on standard input only where the peers started
    /// after it listen.
    pub(crate) fn made_named(args: impl FnOnce(&Path) -> Vec<String>) -> Self {
        Self::made_started(args, true, true)
    }

    /// A cell `mantle init` made, its founders started as [`Cell::made`] starts them, each
    /// configuration as init wrote it: its nodes pinned and nothing else, so no driver of the
    /// harness's runs, and its clients are the native protocol's, made by `mantle init client`.
    pub(crate) fn made_alone(args: impl FnOnce(&Path) -> Vec<String>) -> Self {
        Self::made_started(args, false, false)
    }

    /// The directory the cell was made in, `mantle init`'s OUT.
    pub(crate) fn out(&self) -> PathBuf {
        self._root.path().join("cell")
    }

    fn made_started(args: impl FnOnce(&Path) -> Vec<String>, named: bool, drivers: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let out = root.path().join("cell");
        let made = Command::new(NODE)
            .arg("init")
            .arg(&out)
            .args(args(root.path()))
            .output()
            .unwrap();
        let said = String::from_utf8(made.stdout).unwrap();
        assert!(
            made.status.success(),
            "mantle init: {said}{}",
            String::from_utf8_lossy(&made.stderr)
        );
        let mut name = None;
        let mut authority = None;
        let mut founders = Vec::new();
        for line in said.lines() {
            if let Some(cell) = line.strip_prefix("cell ") {
                name = Some(cell.to_owned());
            } else if let Some(rest) = line.strip_prefix("authority ") {
                // A path ends at a colon and a space, never at a drive's colon.
                authority = rest.split_once(": ").map(|(dir, _)| PathBuf::from(dir));
            } else if let Some(rest) = line.strip_prefix("node ") {
                let (id, rest) = rest.split_once(' ').unwrap();
                let (dir, _) = rest.split_once(": ").unwrap();
                founders.push((id.parse::<u64>().unwrap(), PathBuf::from(dir)));
            }
        }
        let (name, authority) = (name.unwrap(), authority.unwrap());
        let authority_der = std::fs::read(authority.join("authority.der")).unwrap();
        let key =
            KeyPair::from_pkcs8(&std::fs::read(authority.join("authority.key")).unwrap()).unwrap();
        let validity = Validity {
            not_before: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            not_after: NO_EXPIRY,
        };
        let issue = |subject: &str| {
            let own = KeyPair::generate().unwrap();
            let certificate = mantle_cert::node(
                &key,
                &name,
                own.public_key(),
                subject,
                (validity, Serial::draw().unwrap()),
            )
            .unwrap();
            (certificate, own.pkcs8().to_vec())
        };
        let mut clients = BTreeMap::new();
        for id in CLIENT..=CLIENT + CLIENTS {
            let (certificate, pkcs8) = issue(&format!("client-{id}"));
            let c = root.path().join(format!("client-{id}.der"));
            std::fs::write(&c, &certificate).unwrap();
            clients.insert(id, (c, certificate, pkcs8));
        }
        let (certificate, pkcs8) = issue(&format!("gateway-{GATEWAY}"));
        let gateway_file = root.path().join(format!("gateway-{GATEWAY}.der"));
        std::fs::write(&gateway_file, &certificate).unwrap();
        let mut nodes = BTreeMap::new();
        let mut made = BTreeMap::new();
        for (id, dir) in &founders {
            let identity = dir.join("identity");
            let der = std::fs::read(identity.join("node.der")).unwrap();
            nodes.insert(
                *id,
                (identity.join("node.der"), identity.join("node.key"), der),
            );
            let config = dir.join("config");
            if drivers {
                let mut text = std::fs::read_to_string(&config).unwrap();
                for (client, (c, _, _)) in &clients {
                    text.push_str(&format!("peer = {client} gateway {}\n", c.display()));
                }
                std::fs::write(&config, text).unwrap();
            }
            made.insert(*id, config);
        }
        let pki = Pki {
            voters: u64::try_from(founders.len()).unwrap(),
            authority: authority.join("authority.der"),
            authority_der,
            nodes,
            clients,
            objects: object_clients(&key, &name, validity),
            gateway: (gateway_file, certificate, pkcs8),
            root_key: founders[0].1.join("keys").join("1"),
            serving: true,
        };
        let (tell, heard) = channel();
        let mut cell = Self {
            nodes: u64::try_from(founders.len()).unwrap(),
            pki,
            dirs: founders.iter().cloned().collect(),
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
            relay: None,
            learns: Vec::new(),
            moved_at: Instant::now(),
            held_for: Duration::ZERO,
            held_since: None,
            held: BTreeSet::new(),
            stored: true,
            volumes: BTreeMap::new(),
            enlisted: BTreeSet::new(),
            s3: BTreeMap::new(),
            s3_bound: BTreeMap::new(),
            chunks: BTreeMap::new(),
            histories: BTreeMap::new(),
            domains: BTreeMap::new(),
            refusing: None,
            refused: None,
            trace: None,
            ranges: EVERY_LAYER,
            gateway: true,
            serving: true,
            bases: BTreeMap::new(),
            handing: Vec::new(),
            handed: Vec::new(),
            leaderless: BTreeSet::new(),
            stopped: BTreeSet::new(),
            made,
            _root: root,
        };
        if named {
            cell.start_named();
        } else {
            for (id, _) in &founders {
                cell.start(*id);
            }
            cell.introduce();
        }
        cell
    }

    /// Starts the founders highest id first, each configuration naming the peers already
    /// listening; then tells each where the peers started after it listen.
    fn start_named(&mut self) {
        let mut order: Vec<u64> = self.made.keys().copied().collect();
        order.sort_unstable_by(|a, b| b.cmp(a));
        for &id in &order {
            let config = self.made[&id].clone();
            let mut text = std::fs::read_to_string(&config).unwrap();
            for (peer, running) in &self.running {
                let (quic, plane) = running.bound.unwrap();
                let line = format!("peer = {peer} node identity/peers/{peer}.der\n");
                assert!(
                    text.contains(&line),
                    "node {id} names no peer {peer}: {text}"
                );
                let named = format!(
                    "peer = {peer} node identity/peers/{peer}.der localhost:{} localhost:{}\n",
                    quic.port(),
                    plane.port()
                );
                text = text.replace(&line, &named);
            }
            std::fs::write(&config, text).unwrap();
            self.start(id);
        }
        let bound: BTreeMap<u64, (SocketAddr, SocketAddr)> = self
            .running
            .iter()
            .map(|(id, running)| (*id, running.bound.unwrap()))
            .collect();
        for (at, id) in order.iter().enumerate() {
            let running = self.running.get_mut(id).unwrap();
            for peer in &order[at + 1..] {
                let (quic, plane) = bound[peer];
                writeln!(running.input, "learn {peer} {quic} {plane}").unwrap();
            }
            running.input.flush().unwrap();
        }
    }

    /// A cell of `nodes` nodes hosting a range of each layer, each told where the others listen.
    pub(crate) fn layered(nodes: u64) -> Self {
        Self::shaped(Shape {
            voters: nodes,
            spares: 0,
            network: Network::Direct,
            stored: false,
            ranges: EVERY_LAYER,
            gateway: false,
            serving: false,
        })
    }

    /// A cell of `shape`'s voters, each told where the others listen.
    pub(crate) fn shaped(shape: Shape) -> Self {
        let Shape {
            voters,
            spares,
            network,
            stored,
            ranges,
            gateway,
            serving,
        } = shape;
        let nodes = voters + spares;
        let root = tempfile::tempdir().unwrap();
        let mut pki = Pki::new(root.path(), voters, nodes);
        pki.serving = serving;
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
            held: BTreeSet::new(),
            stored,
            volumes: BTreeMap::new(),
            enlisted: BTreeSet::new(),
            s3: BTreeMap::new(),
            s3_bound: BTreeMap::new(),
            chunks: BTreeMap::new(),
            histories: BTreeMap::new(),
            domains: BTreeMap::new(),
            refusing: None,
            refused: None,
            trace: None,
            ranges,
            gateway,
            serving,
            bases: BTreeMap::new(),
            handing: Vec::new(),
            handed: Vec::new(),
            leaderless: BTreeSet::new(),
            stopped: BTreeSet::new(),
            made: BTreeMap::new(),
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
    pub(crate) fn start(&mut self, id: u64) {
        self.spawn_process(id, Stdio::inherit());
    }

    /// Starts node `id`'s process on its directory, its standard error to `stderr`, a thread
    /// forwarding its lines, and waits until it listens.
    pub(crate) fn spawn_process(&mut self, id: u64, stderr: Stdio) {
        // What its last process said its detectors started from is that process's, not this one's.
        self.histories.remove(&id);
        let config = self.config_of(id);
        let mut child = Command::new(NODE)
            .arg(if self.serving { "serve" } else { "node" })
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
        self.bases.remove(&id);
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
    pub(crate) fn listening(&mut self, id: u64) {
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
    pub(crate) fn start_damaged(&mut self, id: u64) -> String {
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
    pub(crate) fn config_of(&self, id: u64) -> PathBuf {
        if let Some(config) = self.made.get(&id) {
            return config.clone();
        }
        let domain = self
            .domains
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("n{id}"));
        let shape = (self.stored, self.ranges, domain.as_str(), self.gateway);
        if self.serving {
            self.pki.stated(id, &self.dirs[&id], shape)
        } else {
            self.pki.config(id, &self.dirs[&id], shape)
        }
    }

    /// Starts node `id`'s process, which must listen and then end on a failure of its own: what
    /// it said on standard error. The cell learns where it listens, and the watch waits for its end
    /// as for any fact the cell reports.
    pub(crate) fn start_refused(&mut self, id: u64) -> String {
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

    /// Asks node `id`'s process to stop in order (node.md §1.7) and waits for its end, as the cell
    /// reports it: how it exited. On Unix the request is SIGTERM; on Windows a `stop` line, since a
    /// console's Ctrl-Break reaches only a process group sharing the sender's console (Microsoft's
    /// `GenerateConsoleCtrlEvent`), which a test's process may not have. The cell's watch goes on
    /// meanwhile, so a stop that holds still past the quiet period fails the test.
    pub(crate) fn stop(&mut self, id: u64) -> std::process::ExitStatus {
        self.refusing = Some(id);
        if cfg!(unix) {
            stop_signal(&self.running[&id].child);
        } else {
            self.say_to(id, b"stop\n");
        }
        self.ended()
    }

    /// Asks node `id`'s process to stop twice in one write, which its input takes whole, so the
    /// second request comes as the stop begins: how the process exited.
    pub(crate) fn stop_twice(&mut self, id: u64) -> std::process::ExitStatus {
        self.refusing = Some(id);
        self.say_to(id, b"stop\nstop\n");
        self.ended()
    }

    /// Node `id`'s chunk bytes, asked now: received over QUIC, put on its own volumes in-process,
    /// and held by its volumes.
    pub(crate) fn chunk_bytes(&mut self, id: u64) -> (u64, u64, u64) {
        self.chunks.remove(&id);
        self.say_to(id, b"chunks\n");
        self.until(|c| c.chunks.get(&id).copied())
    }

    /// Writes `bytes` to node `id`'s standard input, in one write.
    fn say_to(&mut self, id: u64, bytes: &[u8]) {
        let input = &mut self.running.get_mut(&id).unwrap().input;
        input.write_all(bytes).unwrap();
        input.flush().unwrap();
    }

    /// Waits for the process the test expects to end (`refusing`) to end: how it exited.
    fn ended(&mut self) -> std::process::ExitStatus {
        let mut running = self.until(|c| c.refused.take());
        self.refusing = None;
        running.child.wait().unwrap()
    }

    /// Overwrites the first two pages of node `id`'s engine store, where both its superblocks
    /// lie, with bytes it never wrote: a device that returned other bytes than it acknowledged.
    pub(crate) fn damage_store(&self, id: u64) {
        let path = self.dirs[&id].join(format!("range-{GROUP:032x}.store"));
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.write_all(&[0xA5; 2 * STORE_PAGE]).unwrap();
        file.sync_all().unwrap();
    }

    /// Tells every running node to replace `failed`, a member lost for good, with `joining`.
    pub(crate) fn replace(&mut self, failed: u64, joining: u64) {
        for running in self.running.values_mut() {
            writeln!(running.input, "replace {failed} {joining}").unwrap();
            running.input.flush().unwrap();
        }
    }

    /// Kills node `id`'s process, as a power cut takes it.
    pub(crate) fn kill(&mut self, id: u64) {
        let mut running = self.running.remove(&id).unwrap();
        running.child.kill().unwrap();
        running.child.wait().unwrap();
        self.status.retain(|(node, _), _| *node != id);
        self.held.remove(&id);
        self.log.remove(&id);
        self.transfers.remove(&id);
        self.hold();
    }

    /// The quiet period: the longest any node states it may go without moving, a link to a peer it
    /// dials may, should that peer fall silent, or a command or read it asks itself may wait
    /// paused, or any client gives an exchange, never less than the initial RTO.
    pub(crate) fn quiet(&self) -> Duration {
        self.status
            .values()
            .filter_map(|l| match l {
                Line::Status {
                    patience,
                    reconnect,
                    measuring,
                    asking,
                    ..
                } => Some((*patience).max(*reconnect).max(*measuring).max(*asking)),
                Line::Bound(..)
                | Line::Replaced(..)
                | Line::Link(..)
                | Line::Log(..)
                | Line::Held(..)
                | Line::Volume(..)
                | Line::S3(..)
                | Line::S3Bound(..)
                | Line::Chunks(..)
                | Line::Enlisted
                | Line::History(..)
                | Line::Basis(..)
                | Line::Moved(..)
                | Line::Stopping
                | Line::Handing(..)
                | Line::Handed(..)
                | Line::Stopped => None,
            })
            .chain(self.clients.values().copied())
            .chain(self.transfers.values().map(|(_, serve)| *serve))
            .fold(INITIAL_RTO, Duration::max)
    }

    /// How long one stretch of the cell holding still for its devices goes uncounted: the longest
    /// any node's log has taken and the quiet period besides.
    pub(crate) fn held_bound(&self, quiet: Duration) -> Duration {
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
    pub(crate) fn held_by_devices(&self) -> bool {
        let held = self
            .running
            .keys()
            .filter(|id| self.held.contains(id))
            .count();
        held > 0 && 2 * held >= self.running.len()
    }

    /// Begins or ends a stretch of the cell holding still for its devices, as it now stands.
    pub(crate) fn hold(&mut self) {
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
    pub(crate) fn moved(&mut self) {
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
    pub(crate) fn deadline(&self, quiet: Duration) -> Instant {
        let holding = if self.held_since.is_some() {
            self.held_bound(quiet)
        } else {
            Duration::ZERO
        };
        self.moved_at + quiet + self.held_for + holding
    }

    /// When the watch ends for the flushes out: each running node's, as last said, past the
    /// longest any node's log has taken and `quiet` besides, as the cluster's watch has it.
    pub(crate) fn flush_deadline(&self, quiet: Duration) -> Instant {
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
    pub(crate) fn until<T>(&mut self, mut done: impl FnMut(&mut Self) -> Option<T>) -> T {
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
    pub(crate) fn take_heard(&mut self) {
        loop {
            match self.heard.try_recv() {
                Ok(heard) => self.take(heard),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }

    pub(crate) fn take(&mut self, heard: Heard) {
        match heard {
            Heard::Line(id, generation, line) => {
                if let Some(t0) = self.trace {
                    eprintln!(
                        "diag-line t_ms={} wall_ms={} node={id:x} gen={generation} {line:?}",
                        t0.elapsed().as_millis(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis())
                            .unwrap_or(0)
                    );
                }
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
                    Line::Status { group, leader, .. } => {
                        if leader == 0 {
                            self.leaderless.insert((id, group));
                        }
                        // A status like the one last said, as a process started again says it
                        // first, moves nothing.
                        let key = (id, group);
                        let moved = self.status.get(&key).and_then(movement) != movement(&line);
                        self.status.insert(key, line);
                        if !moved {
                            self.hold();
                            return;
                        }
                    }
                    Line::Held(held) => {
                        // Whether the node waits on its device begins or ends a stretch held still,
                        // and moves nothing.
                        if held {
                            self.held.insert(id);
                        } else {
                            self.held.remove(&id);
                        }
                        self.hold();
                        return;
                    }
                    Line::Log(flushing, longest) => {
                        self.log.insert(id, (Instant::now(), flushing, longest));
                        return;
                    }
                    Line::Volume(volume) => {
                        self.volumes.insert(id, volume);
                    }
                    Line::S3(listen, region, ids) => {
                        self.s3.insert(id, (listen, region, ids));
                    }
                    Line::S3Bound(at, connections) => {
                        self.s3_bound.insert(id, (at, connections));
                    }
                    Line::Chunks(received, written, held) => {
                        self.chunks.insert(id, (received, written, held));
                    }
                    Line::Enlisted => {
                        self.enlisted.insert(id);
                    }
                    Line::History(cell, own) => {
                        self.histories.insert(id, (cell, own));
                    }
                    Line::Stopping => {}
                    Line::Handing(group, to) => self.handing.push((id, group, to)),
                    Line::Handed(group, leader) => self.handed.push((id, group, leader)),
                    Line::Stopped => {
                        self.stopped.insert(id);
                    }
                    Line::Basis(basis) => {
                        self.bases.entry(id).or_default().push(basis);
                        return;
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
                if let Some(t0) = self.trace {
                    eprintln!(
                        "diag-line t_ms={} wall_ms={} node={id:x} gen={generation} ended",
                        t0.elapsed().as_millis(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis())
                            .unwrap_or(0)
                    );
                }
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
                    self.held.remove(&id);
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
    pub(crate) fn introduce(&mut self) {
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
    pub(crate) fn deafen_plane(&self, id: u64, deaf: bool) {
        self.relay.as_ref().unwrap().deafen_plane(id, deaf);
    }

    /// Cuts node `id` from every other node, both ways, or joins it to them again.
    pub(crate) fn isolate(&self, id: u64, cut: bool) {
        let relay = self.relay.as_ref().unwrap();
        for peer in self.dirs.keys().copied().filter(|peer| *peer != id) {
            relay.join(id, peer, !cut);
        }
    }

    /// The leader of `group`'s range every running node names, in one term.
    pub(crate) fn leader(&mut self, group: u128) -> u64 {
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
    pub(crate) fn applied(&self, id: u64, group: u128) -> u64 {
        match self.status.get(&(id, group)) {
            Some(Line::Status { applied, .. }) => *applied,
            _ => 0,
        }
    }

    /// Waits until every running node has applied as far as the others in `group`'s range. A
    /// node's commit as its log holds it may trail what it applied by the commit that moved alone,
    /// which rides the group's next write (hyper-durable), so applied is the measure.
    pub(crate) fn converged(&mut self, group: u128) -> u64 {
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
    pub(crate) fn bound_quic(&self) -> Vec<(u64, SocketAddr)> {
        self.running
            .iter()
            .filter_map(|(id, r)| Some((*id, r.bound?.0)))
            .collect()
    }

    /// Opens the bucket's gate, which every put needs, through the writer client.
    pub(crate) fn open_bucket(&mut self) {
        let (settings, bound) = (self.pki.client_settings(CLIENT), self.bound_quic());
        let opened = std::thread::spawn(move || {
            client_runtime(settings.nodes.len() + 1)
                .block_on(async move {
                    let mut client = Client::open(settings).await.unwrap();
                    for (node, quic) in bound {
                        client.learn(node, quic).unwrap();
                    }
                    crate::history::open_bucket(&mut client, GROUP, CLIENT).await
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
    pub(crate) fn start_workload(
        &mut self,
        plan: Plan,
    ) -> (Vec<JoinHandle<Part>>, Vec<Sender<()>>) {
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
    pub(crate) fn tell_workload(&self, id: u64) {
        let quic = self.running[&id].bound.unwrap().0;
        for learn in &self.learns {
            // A client done with its operations has dropped its end.
            let _ = learn.send((id, quic));
        }
    }

    /// Waits until the workload's clients, `ops` operations each, have done `quarters` quarters
    /// of them in all.
    pub(crate) fn done_by(&mut self, ops: u64, quarters: u64) {
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
    pub(crate) fn volume(&mut self, id: u64) -> u128 {
        self.until(|c| c.volumes.get(&id).copied())
    }

    /// A client of identity `id` begins anew: what one before it told is forgotten, and until it
    /// measures an exchange its patience is the idle timeout, three probe timeouts at the initial
    /// estimates (RFC 9000 §10.1), the least `Client::patience` states once it has measured one.
    /// Its first exchange is silent past one second when its first flight is lost (RFC 9002's
    /// initial probe timeout is 1.024 s) or its node waits on a device others share: under two
    /// serve suites at once, 1% of first operations took more than 2.0 s
    /// (measurements/2026-10-25-first-operations-on-a-loaded-host.md).
    fn client_begins(&mut self, id: u64) {
        self.progress.remove(&id);
        self.clients
            .insert(id, mantle_protocol::bounds::idle_timeout());
    }

    /// Starts the gateway on `ops`, a client of its own knowing every node and told where the
    /// running ones listen.
    pub(crate) fn start_gateway(&mut self, ops: Vec<Chunked>) -> JoinHandle<Vec<ChunkOutcome>> {
        self.client_begins(GATEWAY);
        let (settings, bound) = (self.pki.gateway_settings(), self.bound_quic());
        let tell = self.tell.clone();
        std::thread::spawn(move || gateway(settings, bound, ops, tell))
    }

    /// Waits until the gateway has done `done` of its operations.
    pub(crate) fn gateway_done(&mut self, done: u64) {
        self.until(|c| {
            if c.progress.get(&GATEWAY).is_some_and(|d| *d >= done) {
                Some(())
            } else {
                None
            }
        });
    }

    /// The outcomes of `ops`, which the gateway does in turn.
    pub(crate) fn through_gateway(&mut self, ops: Vec<Chunked>) -> Vec<ChunkOutcome> {
        let done = ops.len() as u64;
        let thread = self.start_gateway(ops);
        self.gateway_done(done);
        thread.join().unwrap()
    }

    /// What `ops` came to, each an object's request asked of a node with its data, which the
    /// writer client asks in turn, knowing every node and told where the running ones listen.
    pub(crate) fn objects(&mut self, ops: Vec<(u64, Op, Vec<u8>)>) -> Vec<ObjectOutcome> {
        self.objects_as(CLIENT, ops)
    }

    /// What `ops` came to, asked as [`Cell::objects`] asks them, by client `client`.
    pub(crate) fn objects_as(
        &mut self,
        client: u64,
        ops: Vec<(u64, Op, Vec<u8>)>,
    ) -> Vec<ObjectOutcome> {
        let id = object_client(client);
        self.client_begins(id);
        let done = ops.len() as u64;
        let (settings, bound) = (self.pki.object_settings(id), self.bound_quic());
        let tell = self.tell.clone();
        let thread = std::thread::spawn(move || objects(settings, bound, ops, tell));
        self.until(|c| {
            if c.progress.get(&id).is_some_and(|d| *d >= done) {
                Some(())
            } else {
                None
            }
        });
        thread.join().unwrap()
    }

    /// The root range's volume table, read a page at a time through the writer client.
    pub(crate) fn table(&mut self) -> Vec<Placed> {
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
    /// Waits until node `id`'s running process says what its failure detectors start from: the
    /// cell's history and its own part of it.
    pub(crate) fn history(&mut self, id: u64) -> (Lived, Lived) {
        self.until(|c| c.histories.get(&id).copied())
    }

    pub(crate) fn enlisted(&mut self, id: u64) {
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
    pub(crate) fn read(&mut self, reads: Vec<(u128, Query)>) -> Vec<Result<Read, ClientError>> {
        self.client_begins(CLIENT);
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
    pub(crate) fn asked(&mut self, asks: Vec<(u128, Entry)>) -> Vec<Vec<Answer>> {
        self.client_begins(CLIENT);
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
pub(crate) fn client_runtime(sockets: usize) -> LocalRuntime {
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
pub(crate) fn writer(
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
pub(crate) fn asker(
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
pub(crate) fn objects(
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
pub(crate) fn reader(
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
pub(crate) enum Chunked {
    Put(u64, u128, ChunkKey, Vec<u8>, u32),
    Get(u64, u128, ChunkKey, (u64, u64)),
}

impl Chunked {
    /// A put of `bytes` with their own CRC-32C.
    pub(crate) fn put(node: u64, volume: u128, key: ChunkKey, bytes: Vec<u8>) -> Self {
        let crc32c = mantle_crc::crc32c(&bytes);
        Self::Put(node, volume, key, bytes, crc32c)
    }
}

/// The gateway: `ops` in turn through a client of its own in the gateway's role, each with no
/// deadline of its own, as the writer's commands have none (an exchange with a node gone ends at
/// its connection's idle timeout, a dial to one at its handshake's, and the test's watch ends one
/// never answered), each outcome kept and each told as progress.
pub(crate) fn gateway(
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
pub(crate) const CHUNK_LENS: [usize; 6] = [1, 4095, 4097, 65_528, 300_001, 1 << 20];

/// Fails at the first outcome that is not the one wanted, naming the operation.
pub(crate) fn outcomes_are(got: &[ChunkOutcome], want: &[ChunkOutcome]) {
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
pub(crate) fn brief(outcome: &ChunkOutcome) -> String {
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
pub(crate) fn chunk_key(n: u64) -> ChunkKey {
    ChunkKey {
        block: 0x6368_756e_6b00_0000_0000_0000_0000_0000 | u128::from(n),
        epoch: 1,
        index: 0,
    }
}

/// `mantle init`'s arguments for a cell of three under `root`: a machines file's two founders and a
/// `--node`'s, each with a volume of its own in a failure domain of its own.
pub(crate) fn three_founders(root: &Path) -> Vec<String> {
    let dirs: Vec<PathBuf> = (1..=3)
        .map(|n| {
            let dir = root.join(format!("d{n}"));
            std::fs::create_dir(&dir).unwrap();
            dir
        })
        .collect();
    let file = root.join("machines.conf");
    let mut text = String::from("max_nodes = 3\nmax_volumes = 3\n");
    for (n, dir) in dirs.iter().take(2).enumerate() {
        text.push_str(&format!(
            "[node]\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\ndomain = rack-{n}\nvolume = {} 256M\n",
            dir.display()
        ));
    }
    std::fs::write(&file, text).unwrap();
    vec![
        "--founders".into(),
        file.display().to_string(),
        "--node".into(),
        format!(
            "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=rack-2,volume={}:256M",
            dirs[2].display()
        ),
    ]
}

/// Chunk `n`'s bytes: `len` of them, each a function of `n` and its place, so a chunk read back
/// from another's bytes, or shifted, differs.
pub(crate) fn chunk_bytes(n: u64, len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| {
            i.wrapping_mul(0x9E37_79B9)
                .wrapping_add(n * 131)
                .to_le_bytes()[0]
        })
        .collect()
}

/// Object `n`'s bytes: `len` of them, each a function of `n` and its place.
pub(crate) fn object_bytes(n: u64, len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| {
            i.wrapping_mul(0x2545_F491)
                .wrapping_add(n * 7)
                .to_le_bytes()[1]
        })
        .collect()
}

/// The outcome's reply, which must be done, and its bytes.
pub(crate) fn done(outcome: &ObjectOutcome, what: &str) -> Vec<u8> {
    match outcome {
        ObjectOutcome::Replied { reply, bytes } if reply.status == ObjectStatus::Done => {
            bytes.clone()
        }
        other => panic!("{what}: {other:?}"),
    }
}

/// The outcome's reply, which must have come, and its bytes.
pub(crate) fn replied(outcome: &ObjectOutcome, what: &str) -> (Reply, Vec<u8>) {
    match outcome {
        ObjectOutcome::Replied { reply, bytes } => ((**reply).clone(), bytes.clone()),
        other => panic!("{what}: {other:?}"),
    }
}

/// A PutObject of `length` bytes at `key` in `bucket`, as operation `id`, unconditional and
/// with no checksum.
pub(crate) fn put_op(bucket: &str, key: &str, id: u128, length: u64) -> Op {
    Op::Put {
        bucket: bucket.into(),
        key: key.into(),
        id,
        length,
        headers: Headers::default(),
        checksum: None,
        md5: None,
        trailing: None,
    }
}

/// A GetObject of the whole of `key` in `bucket`, or of the range S3's `Range` names.
pub(crate) fn get_op(bucket: &str, key: &str, range: Option<String>) -> Op {
    Op::Get {
        bucket: bucket.into(),
        key: key.into(),
        headers: Headers {
            range,
            ..Headers::default()
        },
    }
}
