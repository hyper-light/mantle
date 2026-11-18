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
pub(crate) use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError, channel};
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
/// How long the cell may hold still for its devices, a stretch at a time, and a node's log flush
/// may be out, before the watch counts it: as long as a node waits on its own busy device, which
/// is as long as its client waits (`bounds::STOCK_PATIENCE`, research/77 D2). Past it the product
/// itself gives up on the device, so the watch does too.
pub(crate) const DEVICE_PATIENCE: Duration = mantle_protocol::bounds::STOCK_PATIENCE;
/// How often the watch wakes to measure its own process's holds: each timed wake's lateness
/// against its scheduled instant on the monotonic clock is time the host did not run the watch,
/// which it counts against the host, not the cell, so a hold is seen to within one interval. One
/// millisecond is jHiccup's resolution, the interval its meter thread sleeps and times each wake
/// over (github.com/giltene/jHiccup, `HiccupMeter.java`, `resolutionMs = 1.0`).
pub(crate) const HOLD_TICK: Duration = Duration::from_millis(1);

/// The most the work a wait awaits goes without advancing ([`Progress`]), and the whole of a wait
/// that reads none, however the cell moves meanwhile ([`Cell::waiting_on`]): a client's whole
/// patience for one operation, past which no client of the cell waits for what it asked. It asks an
/// operation at most `ATTEMPTS` times in a row with nothing taken, each attempt's dial, its dial
/// again once its name is looked up again and its exchange ending once the stock patience passes
/// with nothing moving (`Client::period_ns`, research/77 D2), and each pause between attempts at
/// most `SLOWED_MOST` (`Client::slowed_wait`). A link a node dials ends within it as well: its
/// dial's handshake within the idle timeout, under the stock patience, and its next dial within its
/// stated reconnect, under the idle timeout (`bounds::pause`). So each kind of work a wait reads
/// takes its next step within it, or never: an operation is answered or its client gives up, an
/// entry or a chunk crosses a link that is up or one dialed again, and a link changes state. A
/// fact the cell has not stated by then, the cell moving all along, is one it moves without
/// reaching.
pub(crate) fn wait_patience() -> Duration {
    let waits_per_attempt = 3;
    (mantle_protocol::bounds::STOCK_PATIENCE * waits_per_attempt + mantle_client::SLOWED_MOST)
        * mantle_client::ATTEMPTS
}

/// What a wait reads of the work it awaits ([`Cell::waiting_on`]): a count that grows as that work
/// goes on. The wait goes on while the count passes the highest it reached in the wait within
/// [`wait_patience`] of the last time it did; a count that falls (a node killed, a client begun
/// anew) and rises again moves the wait only past that highest. Each count has a ceiling the work
/// reaches, so a wait whose fact never comes ends one patience after its work stops.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Progress<'a> {
    /// None: the wait ends one patience after it began, however the cell moves. A process's start
    /// or end, a thread's end, a leader's election, a record a node says once, and a member's
    /// standing, each a step the cell takes or does not.
    Unmeasured,
    /// Client `id`'s operations, as it told each done: its work's ceiling is its operations.
    Operations(u64),
    /// Every client's operations, as each told them done, in all: the workload's.
    Workload,
    /// The ranges of nodes `ids`, each as far as the node applied it and no further than the
    /// furthest running node had as the wait began: nodes catching up, whose count stops once they
    /// have.
    CaughtUp(&'a [u64]),
    /// The cell's settle: the chunks each node's repair rebuilt and the bytes each node's transfers
    /// moved, in all, each as far as the blocks to move.
    Settle,
    /// The links whose state, as their node last said it, ends with this: at most every pair's.
    Links(&'a str),
    /// A count the caller keeps of its own work, as of the caller's last look.
    Counted(&'a std::cell::Cell<u64>),
}

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
/// the range's settings and rules, and the log's and the engine's sizes (the engine's own small test
/// configuration, crates/engine/tests/pool_startup.rs). The calibration's probe budget is the node's
/// own (`probe_budget_ms`).
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
rules.expiries_per_entry = 8
log.segment_bytes = 65536
log.group_cache = 4096
log.queue_submissions = 16
log.waits = measured
engine.extent_pages = 4
engine.max_extents = 256
engine.fanout = 3
engine.leaf_entries = 96
stop_ms = 10000
chunk_bytes = 8388608
";

/// The log's segments, a segment's length (`log.segment_bytes` above), and what each of its groups
/// retains at most: the configuration's `log.max_segments`, `log.group_entries` and
/// `log.group_bytes`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NodeLog {
    pub(crate) max_segments: u64,
    pub(crate) group_entries: u64,
    pub(crate) group_bytes: u64,
}

/// The log most cells run: 64 segments, its groups bound by [`GROUP_ENTRIES`] and [`GROUP_BYTES`],
/// which compaction frees long before the file fills.
pub(crate) const STATED_LOG: NodeLog = NodeLog {
    max_segments: 64,
    group_entries: GROUP_ENTRIES,
    group_bytes: GROUP_BYTES,
};

/// The bytes of a segment of the stated log (`log.segment_bytes` above).
pub(crate) const LOG_SEGMENT: u64 = 65_536;

/// hyper-log's least file, three segments, its groups bound by what the file holds, every byte of
/// it a group's: the file refuses writes for room (`Full`) before any group's own bound
/// (`Backlog`), and the nodes compact only then (node.md §6.2).
pub(crate) const LEAST_LOG: NodeLog = NodeLog {
    max_segments: 3,
    group_entries: 4 * LOG_SEGMENT,
    group_bytes: 4 * LOG_SEGMENT,
};

/// The probe budget a node derives for its machine's calibration (`derive::probe_budget`), in the
/// configuration's milliseconds, as `mantle init` states it: Wilks' 59 samples at the platform's
/// scheduling quantum each. A budget below one quantum (the 40 ms these cells stated, Windows'
/// quantum being 93.75 ms) refused every wake probe beside 32 busy loops on a 4-CPU windows-2025
/// runner, its first round's waiter not yet run when the probe's whole span had passed, 40 of 40,
/// where the derived budget refused none of 10 (diag run 43678610491).
pub(crate) fn probe_budget_ms() -> u128 {
    mantle_node::derive::probe_budget()
        .unwrap()
        .as_nanos()
        .div_ceil(1_000_000)
}

/// This machine's calibration record (`mantle_node::process::calibration`), measured once a test
/// binary at the budget a node derives ([`probe_budget_ms`]), as `mantle init` measures a cell of
/// one's machine, and kept in each node's directory before its first process starts: a node takes a
/// kept record while the machine's key still matches it (node.md §1.6), so the nodes of a cell
/// measure their one machine once, not each under the others' start.
pub(crate) fn calibration_record() -> &'static [u8] {
    static RECORD: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    RECORD.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let budget = mantle_node::derive::probe_budget().unwrap();
        mantle_node::process::calibration(dir.path(), budget).unwrap();
        std::fs::read(dir.path().join(mantle_node::process::CALIBRATION)).unwrap()
    })
}

/// Keeps this machine's calibration record in node directory `dir`, unless one is there already.
pub(crate) fn keep_calibration(dir: &Path) {
    let path = dir.join(mantle_node::process::CALIBRATION);
    if !path.exists() {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(&path, calibration_record()).unwrap();
    }
}

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
/// for its file's write, two blocks going down or being read at once, four operations at once,
/// their bytes four of the most an operation holds at that window (`gateway_bytes`), as many
/// clients as the cell's object clients, S3's 10,000 buckets an owner (research/05 §10.4), 64 rows a coordinator's read; and the
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

/// The bytes `GATEWAY_STATED`'s four operations hold at its window of two blocks, each at the most
/// an operation holds under any scheme a block may take (`mantle_node::derive::Charges`).
pub(crate) fn gateway_bytes() -> u64 {
    let schemes = mantle_ec::durability::candidates();
    4 * mantle_node::derive::Charges::new(2, &schemes)
        .unwrap()
        .most()
}

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
                members: 0,
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
                members: 0,
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
        /// The heartbeats taken from the peers not yet judged (`Status::measuring`, else zero):
        /// what moves while the node measures its links.
        heard: u64,
    },
    /// Whether the node waits on its own device (`Node::held`), said as it changes.
    Held(bool),
    /// The longest poll the node's shard has run since its process began, said as it grows: the
    /// longest the node's thread was held from every exchange it carries.
    LongestPoll(Duration),
    /// A stall hyper-rt's watchdog reported on one of the node's shards, its first the node's own,
    /// as the node said it, at most once a shard each half serve period.
    Stall(u64, Stall),
    /// The node's log as it said with its last status: how long its flush had been out, if one
    /// was, and the longest an update has waited for its commit.
    Log(Option<Duration>, Duration),
    /// The log's refusals for room as the node said them as they moved: its rounds of compaction,
    /// each asked when the log refused a write for room, and the liveness stream's writes the log
    /// refused for room.
    Room(u64, u64),
    /// A volume the node holds.
    Volume(u128),
    /// The S3 endpoint a node read its keys for: where it listens, the region it serves and its
    /// access keys' ids.
    S3(SocketAddr, String, Vec<String>),
    /// Where the node's S3 listener listens, once bound, and the connections it holds at once.
    S3Bound(SocketAddr, u64),
    /// The node's chunk bytes as a `chunks` line asked: received over QUIC, put on its own
    /// volumes in-process, held by its volumes, and of the first two kept by none.
    Chunks(u64, u64, u64, u64),
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
    /// Its orderly stop is done: the process ends. With the sleeps of the node's loop that found
    /// every timer of its shard taken.
    Stopped(u64),
    /// The cell's repair as this node runs it (docs/design/repair.md §5), said as it changes.
    Repair(Repairing),
    /// The root range took the node's leave: its volumes are leaving, and a member's standing with
    /// them (membership.md §4).
    Leaving,
    /// A member's standing as the node's member of the root range holds it (membership.md §1): the
    /// member, its state, and since when it is away, 0 while it is seen (its presence, §8).
    Standing(u64, String, u64),
    /// A range's configuration as the node's member applied it: its group, voters and learners.
    Config(u128, Vec<u64>, Vec<u64>),
    /// The root range removed the leaving member (membership.md §4 step 5).
    Left,
    /// An operator's statement that a member is gone, answered: the member and the answer.
    Gone(u64, String),
}

/// Ids as a `config` line says them, comma-separated, `-` for none.
fn ids_said(text: &str) -> Option<Vec<u64>> {
    if text == "-" {
        return Some(Vec::new());
    }
    text.split(',').map(|id| id.parse().ok()).collect()
}

/// A `repair` line: whether the node runs the cell's repair, whether its last pass ended with
/// nothing it could do at the root and Block ranges' applied indexes it began at, and its counts,
/// the failure domains that took chunks as that pass began among them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Repairing {
    pub(crate) leading: bool,
    pub(crate) settled: bool,
    pub(crate) root_applied: u64,
    pub(crate) block_applied: u64,
    pub(crate) rebuilt: u64,
    pub(crate) timing: u64,
    pub(crate) commands: u64,
    pub(crate) unplaced: u64,
    pub(crate) unreadable: u64,
    pub(crate) corrupt: u64,
    pub(crate) waiting: u64,
    pub(crate) short: u64,
    pub(crate) lost: u64,
    pub(crate) takers: u64,
    pub(crate) running: u64,
}

/// A history as a node says it: node time run and crashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Lived {
    pub(crate) exposure: Duration,
    pub(crate) crashes: u64,
}

/// A stall as a node said it (`stall SHARD SITE LABEL HELD_NS CPU_NS WHOSE MISSED`): the spawn
/// site of the task whose poll held the shard and its label, each `-` for none; how long the poll
/// had held the shard when the watchdog reported it, and the CPU the shard's thread ran meanwhile,
/// none where it had no reading; whose the time was; and the reports the node left unsaid before
/// it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Stall {
    pub(crate) site: String,
    pub(crate) label: String,
    pub(crate) held: Duration,
    pub(crate) cpu: Option<Duration>,
    pub(crate) whose: String,
    pub(crate) missed: u64,
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
        // The room past them (fragments, their budget, free segment bytes) no test here reads.
        ["chunks", received, written, held, unkept, ..] => Some(Line::Chunks(
            received.parse().ok()?,
            written.parse().ok()?,
            held.parse().ok()?,
            unkept.parse().ok()?,
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
        ["stopped", waits] => Some(Line::Stopped(waits.parse().ok()?)),
        ["leaving"] => Some(Line::Leaving),
        ["standing", id, state, absent] => Some(Line::Standing(
            id.parse().ok()?,
            (*state).to_owned(),
            absent.parse().ok()?,
        )),
        ["config", group, voters, learners] => Some(Line::Config(
            u128::from_str_radix(group, 16).ok()?,
            ids_said(voters)?,
            ids_said(learners)?,
        )),
        ["left"] => Some(Line::Left),
        ["gone", id, outcome @ ..] => Some(Line::Gone(id.parse().ok()?, outcome.join(" "))),
        [
            "repair",
            leading,
            settled,
            root_applied,
            block_applied,
            rebuilt,
            timing,
            commands,
            unplaced,
            unreadable,
            corrupt,
            waiting,
            short,
            lost,
            takers,
            running,
        ] => {
            let flag = |f: &str| match f {
                "0" => Some(false),
                "1" => Some(true),
                _ => None,
            };
            Some(Line::Repair(Repairing {
                leading: flag(leading)?,
                settled: flag(settled)?,
                root_applied: root_applied.parse().ok()?,
                block_applied: block_applied.parse().ok()?,
                rebuilt: rebuilt.parse().ok()?,
                timing: timing.parse().ok()?,
                commands: commands.parse().ok()?,
                unplaced: unplaced.parse().ok()?,
                unreadable: unreadable.parse().ok()?,
                corrupt: corrupt.parse().ok()?,
                waiting: waiting.parse().ok()?,
                short: short.parse().ok()?,
                lost: lost.parse().ok()?,
                takers: takers.parse().ok()?,
                running: running.parse().ok()?,
            }))
        }
        ["longest-poll", longest] => Some(Line::LongestPoll(Duration::from_nanos(
            longest.parse().ok()?,
        ))),
        ["stall", shard, site, label, held, cpu, whose, missed] => Some(Line::Stall(
            shard.parse().ok()?,
            Stall {
                site: (*site).to_owned(),
                label: (*label).to_owned(),
                held: Duration::from_nanos(held.parse().ok()?),
                cpu: match *cpu {
                    "-" => None,
                    cpu => Some(Duration::from_nanos(cpu.parse().ok()?)),
                },
                whose: (*whose).to_owned(),
                missed: missed.parse().ok()?,
            },
        )),
        ["held", held] => Some(Line::Held(match *held {
            "0" => false,
            "1" => true,
            _ => return None,
        })),
        ["moved", bytes, serve] => Some(Line::Moved(
            bytes.parse().ok()?,
            Duration::from_nanos(serve.parse().ok()?),
        )),
        ["room", compactions, waits] => {
            Some(Line::Room(compactions.parse().ok()?, waits.parse().ok()?))
        }
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
            heard,
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
            heard: heard.parse().ok()?,
        }),
        _ => None,
    }
}

/// A status's movement: its term, leader, commit, applied index, reconnect bound, measuring
/// interval, asking bound and the heartbeats taken while measuring.
pub(crate) type Movement = (u64, u64, u64, u64, Duration, Duration, Duration, u64);

/// What of a status is movement: all of it.
pub(crate) fn movement(line: &Line) -> Option<Movement> {
    match line {
        Line::Status {
            term,
            leader,
            commit,
            applied,
            reconnect,
            measuring,
            asking,
            heard,
            ..
        } => Some((
            *term, *leader, *commit, *applied, *reconnect, *measuring, *asking, *heard,
        )),
        Line::Bound(..)
        | Line::Replaced(..)
        | Line::Link(..)
        | Line::Log(..)
        | Line::Room(..)
        | Line::Held(..)
        | Line::LongestPoll(..)
        | Line::Stall(..)
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
        | Line::Stopped(..)
        | Line::Repair(..)
        | Line::Leaving
        | Line::Standing(..)
        | Line::Config(..)
        | Line::Left
        | Line::Gone(..) => None,
    }
}

/// What the test hears: a process's line, by the node and the process's generation, or what a
/// workload client tells.
pub(crate) enum Heard {
    Line(u64, u64, Line),
    /// A process's output ended, by the node and the process's generation: the process ended. The
    /// last lines it said on standard error where the cell echoed it ([`Stderr::Echoed`]).
    Ended(u64, u64, Vec<String>),
    Told(Told),
}

/// What the cell does with a node process's standard error.
pub(crate) enum Stderr {
    /// Echoed to the test's own as it comes, its last [`STDERR_LINES`] kept for the panic of an end
    /// the cell did not ask for, which says why the process ended.
    Echoed,
    /// Left whole in its pipe, which the caller reads once the process ends.
    Piped,
}

/// The lines of a node's standard error the panic of its unasked end carries: a node that refuses
/// to run says why in one line, its step and its cause, and the lines before it what it reached.
const STDERR_LINES: usize = 20;

/// Reads `stderr` to its end on a thread of its own: each line echoed to the test's standard error
/// as the node said it, and the last [`STDERR_LINES`] of them what the thread answers. Bytes that are
/// not UTF-8 are echoed and kept lossily, so the pipe is drained to its end and the node never
/// waits on a full one.
fn echoed(stderr: std::process::ChildStderr) -> JoinHandle<Vec<String>> {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut last = std::collections::VecDeque::with_capacity(STDERR_LINES);
        let mut line = Vec::new();
        loop {
            line.clear();
            match reader.read_until(b'\n', &mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let _ = std::io::stderr().write_all(&line);
            let text = String::from_utf8_lossy(&line);
            if last.len() == STDERR_LINES {
                last.pop_front();
            }
            last.push_back(text.trim_end_matches(['\r', '\n']).to_owned());
        }
        last.into()
    })
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
    /// The log each `mantle node` runs with.
    pub(crate) log: NodeLog,
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
            log: STATED_LOG,
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
            replicas: 0,
            clients: 0,
            pool: 0,
            ranges: 1,
            inflight: 0,
            lanes: 1,
            transfers: 0,
            asked: usize::from(role == Role::Gateway),
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
             transport_bytes = {}\nlog.max_segments = {}\nlog.group_entries = {}\nlog.group_bytes = {}\n\
             engine.page_size = {STORE_PAGE}\nengine.memtable_bytes = {MEMTABLE}\n\
             rules.max_key_bytes = {held}\nrules.max_value_bytes = {held}\n\
             runtime.probe_budget_ms = {}\n{STATED}",
            dir.display(),
            self.authority.display(),
            certificate.display(),
            key.display(),
            voters.join(","),
            peers * 4 * hyper_transport::RECEIVE_CHUNK,
            self.log.max_segments,
            self.log.group_entries,
            self.log.group_bytes,
            probe_budget_ms(),
        );
        for (group, layer) in ranges {
            text.push_str(&format!("range = {group:x} {layer}\n"));
        }
        // What the node asks itself: a gateway's chunks for a put's window of blocks going down,
        // each copied to every node, and its commands and reads; else its one enlistment.
        if gateway {
            text.push_str(&format!(
                "chunk_asks = 12\ncommand_asks = 8\nread_asks = 8\nroot_key = 1 {}\n\
                 {GATEWAY_STATED}gateway.objects_bytes = {}\n",
                self.root_key.display(),
                gateway_bytes(),
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
    /// Each running node's log refusals for room as its process last said them: its rounds of
    /// compaction and its liveness stream's waits.
    pub(crate) room: BTreeMap<u64, (u64, u64)>,
    /// Each running node's longest poll as its process last said it, and when that was heard: a
    /// process says it as it grows, so when tells which stretch of the test ran it.
    pub(crate) polls: BTreeMap<u64, (Instant, Duration)>,
    /// Each running node's latest stall on each of its shards as its process said it, and when
    /// that was heard, by node and shard: one a node's shard at most.
    pub(crate) stalls: BTreeMap<(u64, u64), (Instant, Stall)>,
    /// The bytes each node's transfers moved as it last said, and its transport's serve period: a
    /// transfer that lives moves within one, so the quiet period is never shorter.
    pub(crate) transfers: BTreeMap<u64, (u64, Duration)>,
    pub(crate) relay: Option<Relay>,
    /// Where the running workload's clients are told of a node started again.
    pub(crate) learns: Vec<Sender<(u64, SocketAddr)>>,
    pub(crate) moved_at: Instant,
    /// When a node's line or a process's end last moved the cell: what a client's progress, a
    /// wait's own probe among them, does not move.
    pub(crate) stirred_at: Instant,
    /// Since the last movement, how long the cell held still for its devices, each stretch at
    /// most `DEVICE_PATIENCE`, and when the stretch under way began, if one is.
    pub(crate) held_for: Duration,
    pub(crate) held_since: Option<Instant>,
    /// Since the last movement, how long the host held the watch's own process past the wakes it
    /// scheduled (`HOLD_TICK`): a test judges the cell, not the host's scheduler.
    pub(crate) host_held: Duration,
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
    pub(crate) chunks: BTreeMap<u64, (u64, u64, u64, u64)>,
    /// What each node's failure detectors started from as its latest process said: the cell's
    /// history and its own part of it.
    pub(crate) histories: BTreeMap<u64, (Lived, Lived)>,
    /// Each node's failure domain, where a test states another than its own name's.
    pub(crate) domains: BTreeMap<u64, String>,
    /// The node whose process the test started to be refused, and that process once it ended.
    pub(crate) refusing: Option<u64>,
    pub(crate) refused: Option<Running>,
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
    /// The cell's repair as each running node last said it, and the nodes whose leave the root
    /// range took.
    pub(crate) repairs: BTreeMap<u64, Repairing>,
    pub(crate) leaving: BTreeSet<u64>,
    /// Each range's configuration as each node's member last said it applied it, by node and
    /// group: its voters and its learners (membership.md §6).
    pub(crate) configs: BTreeMap<(u64, u128), (Vec<u64>, Vec<u64>)>,
    /// Each member's standing as each node last said its root range holds it, by node and member:
    /// its state and since when it is away.
    pub(crate) standings: BTreeMap<(u64, u64), (String, u64)>,
    /// The nodes whose running process said the root range removed it.
    pub(crate) left: BTreeSet<u64>,
    /// Each operator's statement of a member gone, as the node it was stated to answered it.
    pub(crate) gone: BTreeMap<u64, String>,
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

    /// A cell of `nodes` nodes hosting a range of each layer, each keeping a volume, hosting
    /// objects and running with `log`.
    pub(crate) fn gateways_on(nodes: u64, log: NodeLog) -> Self {
        Self::shaped_logged(
            Shape {
                voters: nodes,
                spares: 0,
                network: Network::Direct,
                stored: true,
                ranges: EVERY_LAYER,
                gateway: true,
                serving: false,
            },
            log,
        )
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
        Self::made_started(args, false, true, Network::Direct)
    }

    /// A cell `mantle init` made as [`Cell::made`] makes it, each founder's memory stated at what
    /// `mantle derive` says the node needs on this machine (`memory.need`), as an operator states
    /// it, never a figure picked (docs/design/kubernetes.md): its floor at the most its devices can
    /// be measured to take, with as many operations as one stock client asks of its S3 endpoint at
    /// once, and every want whole. A figure picked passes some machine's floor, and the node
    /// refuses it at its start: four gibibytes, below the floor of 4,940,600,296 bytes the nodes
    /// derived on CI's windows-2025 runner (run 44286448798). The derivation measures each
    /// founder's devices and keeps their profiles, which its first start takes.
    pub(crate) fn made_at_need(args: impl FnOnce(&Path) -> Vec<String>) -> Self {
        let mut cell = Self::made_unstarted(args, true, Network::Direct);
        let founders: Vec<u64> = cell.made.keys().copied().collect();
        for &id in &founders {
            let config = cell.config_of(id);
            keep_calibration(&cell.dirs[&id]);
            let derived = Command::new(NODE)
                .arg("derive")
                .arg(&config)
                .stdin(Stdio::null())
                .output()
                .unwrap();
            assert!(
                derived.status.success(),
                "node {id}'s derivation: {}",
                String::from_utf8_lossy(&derived.stderr)
            );
            let need = String::from_utf8_lossy(&derived.stdout)
                .lines()
                .filter_map(|line| Basis::parse(line.strip_prefix("basis ")?))
                .find(|basis| basis.key == "memory.need")
                .unwrap_or_else(|| panic!("node {id}'s derivation says no memory.need"))
                .value;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&config)
                .unwrap();
            writeln!(file, "memory = {need}").unwrap();
        }
        for id in founders {
            cell.start(id);
        }
        cell.introduce();
        cell
    }

    /// A cell `mantle init` made as [`Cell::made`] makes it, its founders past the first `started`
    /// by id made and never started: each joins the running cell when the test starts it
    /// (`Cell::start`, `Cell::introduce`), its volumes new to the cell's table.
    pub(crate) fn made_partly(args: impl FnOnce(&Path) -> Vec<String>, started: usize) -> Self {
        let mut cell = Self::made_unstarted(args, true, Network::Direct);
        let founders: Vec<u64> = cell.made.keys().copied().collect();
        for id in founders.into_iter().take(started) {
            cell.start(id);
        }
        cell.introduce();
        cell
    }

    /// A cell `mantle init` made, its founders started highest id first, each one's configuration
    /// naming the peers already listening by `localhost` and the ports they bound, as an operator
    /// names a peer by its host: a founder learns on standard input only where the peers started
    /// after it listen.
    pub(crate) fn made_named(args: impl FnOnce(&Path) -> Vec<String>) -> Self {
        Self::made_started(args, true, true, Network::Direct)
    }

    /// A cell `mantle init` made, its founders started as [`Cell::made`] starts them, each
    /// configuration as init wrote it: its nodes pinned and nothing else, so no driver of the
    /// harness's runs, and its clients are the native protocol's, made by `mantle init client`.
    pub(crate) fn made_alone(args: impl FnOnce(&Path) -> Vec<String>) -> Self {
        Self::made_started(args, false, false, Network::Direct)
    }

    /// A cell [`Cell::made_alone`] makes, its founders reaching each other through the test's
    /// relay.
    pub(crate) fn made_alone_relayed(args: impl FnOnce(&Path) -> Vec<String>) -> Self {
        Self::made_started(args, false, false, Network::Relayed)
    }

    /// The directory the cell was made in, `mantle init`'s OUT.
    pub(crate) fn out(&self) -> PathBuf {
        self._root.path().join("cell")
    }

    fn made_started(
        args: impl FnOnce(&Path) -> Vec<String>,
        named: bool,
        drivers: bool,
        network: Network,
    ) -> Self {
        let mut cell = Self::made_unstarted(args, drivers, network);
        if named {
            cell.start_named();
        } else {
            let founders: Vec<u64> = cell.made.keys().copied().collect();
            for id in founders {
                cell.start(id);
            }
            cell.introduce();
        }
        cell
    }

    /// A cell `mantle init` made, none of its founders started.
    fn made_unstarted(
        args: impl FnOnce(&Path) -> Vec<String>,
        drivers: bool,
        network: Network,
    ) -> Self {
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
            log: STATED_LOG,
        };
        let (tell, heard) = channel();
        Self {
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
            room: BTreeMap::new(),
            polls: BTreeMap::new(),
            stalls: BTreeMap::new(),
            transfers: BTreeMap::new(),
            relay: (network == Network::Relayed)
                .then(|| Relay::start(&founders.iter().map(|(id, _)| *id).collect::<Vec<_>>())),
            learns: Vec::new(),
            moved_at: Instant::now(),
            stirred_at: Instant::now(),
            held_for: Duration::ZERO,
            held_since: None,
            host_held: Duration::ZERO,
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
            ranges: EVERY_LAYER,
            gateway: true,
            serving: true,
            bases: BTreeMap::new(),
            handing: Vec::new(),
            handed: Vec::new(),
            leaderless: BTreeSet::new(),
            stopped: BTreeSet::new(),
            made,
            repairs: BTreeMap::new(),
            leaving: BTreeSet::new(),
            configs: BTreeMap::new(),
            standings: BTreeMap::new(),
            left: BTreeSet::new(),
            gone: BTreeMap::new(),
            _root: root,
        }
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
        Self::shaped_logged(shape, STATED_LOG)
    }

    /// A cell of `shape`'s voters, each running with `log`, each told where the others listen.
    pub(crate) fn shaped_logged(shape: Shape, log: NodeLog) -> Self {
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
        pki.log = log;
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
            room: BTreeMap::new(),
            polls: BTreeMap::new(),
            stalls: BTreeMap::new(),
            transfers: BTreeMap::new(),
            relay: (network == Network::Relayed)
                .then(|| Relay::start(&(1..=nodes).collect::<Vec<_>>())),
            learns: Vec::new(),
            moved_at: Instant::now(),
            stirred_at: Instant::now(),
            held_for: Duration::ZERO,
            held_since: None,
            host_held: Duration::ZERO,
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
            ranges,
            gateway,
            serving,
            bases: BTreeMap::new(),
            handing: Vec::new(),
            handed: Vec::new(),
            leaderless: BTreeSet::new(),
            stopped: BTreeSet::new(),
            made: BTreeMap::new(),
            repairs: BTreeMap::new(),
            leaving: BTreeSet::new(),
            configs: BTreeMap::new(),
            standings: BTreeMap::new(),
            left: BTreeSet::new(),
            gone: BTreeMap::new(),
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
    #[track_caller]
    pub(crate) fn start(&mut self, id: u64) {
        self.spawn_process(id, Stderr::Echoed);
    }

    /// Starts node `id`'s process on its directory, its standard error as `stderr` says, a thread
    /// forwarding its lines, and waits until it listens.
    #[track_caller]
    pub(crate) fn spawn_process(&mut self, id: u64, stderr: Stderr) {
        // What its last process said its detectors started from is that process's, not this one's.
        self.histories.remove(&id);
        let config = self.config_of(id);
        keep_calibration(&self.dirs[&id]);
        let mut child = Command::new(NODE)
            .arg(if self.serving { "serve" } else { "node" })
            .arg(&config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let said = match stderr {
            Stderr::Echoed => Some(echoed(child.stderr.take().unwrap())),
            Stderr::Piped => None,
        };
        self.volumes.remove(&id);
        self.enlisted.remove(&id);
        self.bases.remove(&id);
        // What its last process said of the cell's members and ranges is that process's.
        self.configs.retain(|(node, _), _| *node != id);
        self.standings.retain(|(node, _), _| *node != id);
        self.leaving.remove(&id);
        self.left.remove(&id);
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
            // Its standard error ends as its output does, with the process: what it said last
            // is all said once its reader ends.
            let last = said
                .map(|reader| reader.join().unwrap_or_default())
                .unwrap_or_default();
            let _ = tell.send(Heard::Ended(id, generation, last));
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
    /// system's and the device's work, with no law of the node's to bound it: the watch gives it,
    /// past the quiet period, as long as the cell may hold still for a device (`DEVICE_PATIENCE`),
    /// the longest the product itself waits on one.
    #[track_caller]
    pub(crate) fn listening(&mut self, id: u64) {
        let what = format!("node {id}'s start");
        self.waiting_on(&what, DEVICE_PATIENCE, Progress::Unmeasured, |c| {
            match c.running.get(&id) {
                Some(running) => running.bound.map(|_| ()),
                None => panic!("node {id} ended before it listened: {:?}", c.status),
            }
        });
    }

    /// Starts node `id`'s process on a directory whose store is damaged, and waits for its end:
    /// it must refuse the store, a typed corruption error, before it listens. What it said on
    /// standard error.
    pub(crate) fn start_damaged(&mut self, id: u64) -> String {
        let config = self.config_of(id);
        keep_calibration(&self.dirs[&id]);
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
    #[track_caller]
    pub(crate) fn start_refused(&mut self, id: u64) -> String {
        self.refusing = Some(id);
        self.spawn_process(id, Stderr::Piped);
        self.introduce();
        let mut running = self.until(|c| c.refused.take());
        self.refusing = None;
        let status = self.exited(id, &mut running);
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
    #[track_caller]
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
    /// held by its volumes, and of the first two kept by none.
    pub(crate) fn chunk_bytes(&mut self, id: u64) -> (u64, u64, u64, u64) {
        self.chunks.remove(&id);
        self.say_to(id, b"chunks\n");
        self.until(|c| c.chunks.get(&id).copied())
    }

    /// Writes `bytes` to node `id`'s standard input, in one write.
    pub(crate) fn say_to(&mut self, id: u64, bytes: &[u8]) {
        let input = &mut self.running.get_mut(&id).unwrap().input;
        input.write_all(bytes).unwrap();
        input.flush().unwrap();
    }

    /// Waits for the process the test expects to end (`refusing`) to end: how it exited.
    #[track_caller]
    pub(crate) fn ended(&mut self) -> std::process::ExitStatus {
        let mut running = self.until(|c| c.refused.take());
        let id = self.refusing.take().unwrap();
        self.exited(id, &mut running)
    }

    /// How node `id`'s process `running` exited, its output ended: waited on under the watch, as
    /// its output's end was.
    #[track_caller]
    fn exited(&mut self, id: u64, running: &mut Running) -> std::process::ExitStatus {
        let what = format!("node {id}'s exit");
        self.waiting_on(&what, Duration::ZERO, Progress::Unmeasured, |_| {
            running.child.try_wait().unwrap()
        })
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

    /// A node granted to join the cell `mantle init` made (`mantle init grant`, membership.md §7),
    /// stated by `statements` as `--node` states a founder, its directory under the test's root;
    /// pinning the test's clients as gateways, as the founders' configurations do; known to the
    /// test's clients by its certificate. Its id; no process runs it yet.
    pub(crate) fn grant(&mut self, statements: &str) -> u64 {
        let dir = self
            ._root
            .path()
            .join(format!("granted-{}", self.dirs.len()));
        let made = Command::new(NODE)
            .args(["init", "grant"])
            .arg(&dir)
            .arg("--cell")
            .arg(self.out())
            .args(["--node", statements])
            .output()
            .unwrap();
        let said = String::from_utf8(made.stdout).unwrap();
        assert!(
            made.status.success(),
            "mantle init grant: {said}{}",
            String::from_utf8_lossy(&made.stderr)
        );
        let id: u64 = said
            .lines()
            .find_map(|l| l.strip_prefix("granted "))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|id| id.parse().ok())
            .unwrap_or_else(|| panic!("mantle init grant said no grant: {said}"));
        let config = dir.join("config");
        let mut text = std::fs::read_to_string(&config).unwrap();
        for (client, (c, _, _)) in &self.pki.clients {
            text.push_str(&format!("peer = {client} gateway {}\n", c.display()));
        }
        std::fs::write(&config, text).unwrap();
        let identity = dir.join("identity");
        let der = std::fs::read(identity.join("node.der")).unwrap();
        self.pki.nodes.insert(
            id,
            (identity.join("node.der"), identity.join("node.key"), der),
        );
        self.dirs.insert(id, dir);
        self.made.insert(id, config);
        self.nodes = u64::try_from(self.dirs.len()).unwrap();
        id
    }

    /// Asks node `id`'s process to leave the cell in order (membership.md §4) and waits for its
    /// end, as the cell reports it: how it exited.
    pub(crate) fn leave(&mut self, id: u64) -> std::process::ExitStatus {
        self.refusing = Some(id);
        self.say_to(id, b"leave\n");
        self.ended()
    }

    /// Waits until each of `observers` says each range's configuration is what `wanted` takes:
    /// its voters and its learners as that node's member applied them, each range one a node said
    /// a configuration of (a cell `mantle init` made draws its ranges' groups). It goes on while
    /// the observers catch their ranges up, as a learner does before it votes.
    pub(crate) fn configured(
        &mut self,
        observers: &[u64],
        wanted: impl Fn(&[u64], &[u64]) -> bool,
    ) {
        self.until_by(Progress::CaughtUp(observers), |c| {
            let groups: BTreeSet<u128> = c.configs.keys().map(|(_, group)| *group).collect();
            let configured = groups.len() == c.ranges.len()
                && observers.iter().all(|node| {
                    groups.iter().all(|group| {
                        c.configs
                            .get(&(*node, *group))
                            .is_some_and(|(voters, learners)| wanted(voters, learners))
                    })
                });
            if configured { Some(()) } else { None }
        });
    }

    /// Kills node `id`'s process, as a power cut takes it.
    pub(crate) fn kill(&mut self, id: u64) {
        let mut running = self.running.remove(&id).unwrap();
        running.child.kill().unwrap();
        running.child.wait().unwrap();
        self.status.retain(|(node, _), _| *node != id);
        self.repairs.remove(&id);
        self.held.remove(&id);
        self.log.remove(&id);
        self.room.remove(&id);
        self.polls.remove(&id);
        self.stalls.retain(|(node, _), _| *node != id);
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
                | Line::Room(..)
                | Line::Held(..)
                | Line::LongestPoll(..)
                | Line::Stall(..)
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
                | Line::Stopped(..)
                | Line::Repair(..)
                | Line::Leaving
                | Line::Standing(..)
                | Line::Config(..)
                | Line::Left
                | Line::Gone(..) => None,
            })
            .chain(self.clients.values().copied())
            .chain(self.transfers.values().map(|(_, serve)| *serve))
            .fold(INITIAL_RTO, Duration::max)
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
                self.held_for += now.saturating_duration_since(since).min(DEVICE_PATIENCE);
                self.held_since = None;
            }
            (None, false) | (Some(_), true) => {}
        }
    }

    /// Something moved: the watch begins again.
    pub(crate) fn moved(&mut self) {
        self.moved_since(Instant::now());
    }

    /// The watch begins again from `at`, when something moved.
    fn moved_since(&mut self, at: Instant) {
        self.moved_at = at;
        self.held_for = Duration::ZERO;
        self.host_held = Duration::ZERO;
        self.held_since = if self.held_by_devices() {
            Some(at)
        } else {
            None
        };
    }

    /// When the watch ends: `quiet` after the last movement, past the stretches the cell held still
    /// for its devices, the one under way counted to its bound.
    pub(crate) fn deadline(&self, quiet: Duration) -> Instant {
        let holding = if self.held_since.is_some() {
            DEVICE_PATIENCE
        } else {
            Duration::ZERO
        };
        self.moved_at + quiet + self.held_for + holding
    }

    /// When the watch ends for the flushes out: each running node's, as last said, `DEVICE_PATIENCE`
    /// past the flush's start, as the cluster's watch has it.
    pub(crate) fn flush_deadline(&self) -> Instant {
        self.log
            .iter()
            .filter(|(id, _)| self.running.contains_key(id))
            .filter_map(|(_, (heard, flushing, _))| {
                Some(*heard + DEVICE_PATIENCE.saturating_sub(*flushing.as_ref()?))
            })
            .max()
            .unwrap_or_else(Instant::now)
    }

    /// Takes what it hears until `done` holds; fails once a quiet period passes with nothing, past
    /// the time the host held the watch: each wait at most `HOLD_TICK`, and a wait that ends late
    /// moves the watch's end by its lateness, so each spends exactly the span it scheduled. Fails
    /// too once [`wait_patience`] passes from its start, past the time the host held the watch
    /// meanwhile, however the cell moves: a cell that keeps moving without `done` ever holding
    /// holds no wait open. A failure names the wait by its caller and says what the cell said last.
    #[track_caller]
    pub(crate) fn until<T>(&mut self, done: impl FnMut(&mut Self) -> Option<T>) -> T {
        self.waiting_on("the wait", Duration::ZERO, Progress::Unmeasured, done)
    }

    /// [`Cell::until`] for work `progress` reads: its patience runs from the last time that work
    /// advanced, not from the wait's start.
    #[track_caller]
    pub(crate) fn until_by<T>(
        &mut self,
        progress: Progress<'_>,
        done: impl FnMut(&mut Self) -> Option<T>,
    ) -> T {
        self.waiting_on("the wait", Duration::ZERO, progress, done)
    }

    /// [`Cell::until_by`] for `what`, as its failure names it, its quiet period and its patience
    /// each longer by `allowance`.
    #[track_caller]
    pub(crate) fn waiting_on<T>(
        &mut self,
        what: &str,
        allowance: Duration,
        progress: Progress<'_>,
        mut done: impl FnMut(&mut Self) -> Option<T>,
    ) -> T {
        let at = std::panic::Location::caller();
        let patience = wait_patience() + allowance;
        let began = Instant::now();
        let furthest = self.furthest();
        // The awaited work at its highest in this wait, when it last rose there, and the host's
        // holds since, which no movement of the cell forgets, as the watch's are.
        let mut highest = self.measured(progress, &furthest);
        let mut rose = began;
        let mut host_held = Duration::ZERO;
        self.moved();
        loop {
            let watch = (
                self.moved_at,
                self.held_for,
                self.held_since,
                self.host_held,
            );
            if let Some(found) = done(self) {
                return found;
            }
            let measured = self.measured(progress, &furthest);
            if measured > highest {
                (highest, rose, host_held) = (measured, Instant::now(), Duration::ZERO);
            }
            assert!(
                rose.elapsed() <= patience + host_held,
                "{what} at {at}: not so, its work {progress:?} at {highest} and no further within \
                 {patience:?}, {:?} into the wait, besides {host_held:?} the host held the watch, \
                 though the cell moved: {}",
                began.elapsed(),
                self.seen()
            );
            // What `done` waited on itself, a read it asked under a watch of its own, is not the
            // cell moving toward the fact waited on: this watch goes on from where it was, moved
            // only where a node moved the cell meanwhile.
            if self.stirred_at > watch.0 {
                self.moved_since(self.stirred_at);
            } else {
                (
                    self.moved_at,
                    self.held_for,
                    self.held_since,
                    self.host_held,
                ) = watch;
                self.hold();
            }
            let quiet = self.quiet() + allowance;
            let now = Instant::now();
            let left = (self.deadline(quiet).max(self.flush_deadline()) + self.host_held)
                .saturating_duration_since(now);
            let wait = left.min(HOLD_TICK);
            let woke_by = now + wait;
            match self.heard.recv_timeout(wait) {
                Ok(heard) => self.take(heard),
                Err(RecvTimeoutError::Timeout) if !left.is_zero() => {
                    let late = Instant::now().saturating_duration_since(woke_by);
                    self.host_held += late;
                    host_held += late;
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => panic!(
                    "{what} at {at}: quiet for {quiet:?} with nothing moved, besides {:?} held still \
                     for the devices and {:?} the host held the watch: {}",
                    self.held_for,
                    self.host_held,
                    self.seen()
                ),
            }
        }
    }

    /// Each range's applied index at its furthest running node, as each last said it.
    fn furthest(&self) -> BTreeMap<u128, u64> {
        let mut furthest = BTreeMap::new();
        for &(id, group) in self.status.keys() {
            if self.running.contains_key(&id) {
                let applied = self.applied(id, group);
                furthest
                    .entry(group)
                    .and_modify(|f: &mut u64| *f = (*f).max(applied))
                    .or_insert(applied);
            }
        }
        furthest
    }

    /// The count `progress` reads now, nodes catching up each range to its applied index in
    /// `furthest`.
    fn measured(&self, progress: Progress<'_>, furthest: &BTreeMap<u128, u64>) -> u64 {
        match progress {
            Progress::Unmeasured => 0,
            Progress::Operations(id) => self.progress.get(&id).copied().unwrap_or(0),
            Progress::Workload => self.progress.values().sum(),
            Progress::CaughtUp(ids) => ids
                .iter()
                .flat_map(|id| {
                    furthest
                        .iter()
                        .map(|(group, f)| self.applied(*id, *group).min(*f))
                })
                .sum(),
            Progress::Settle => {
                let rebuilt: u64 = self.repairs.values().map(|r| r.rebuilt).sum();
                let moved: u64 = self.transfers.values().map(|(bytes, _)| bytes).sum();
                rebuilt + moved
            }
            Progress::Links(state) => self
                .links
                .values()
                .filter(|link| link.ends_with(state))
                .count() as u64,
            Progress::Counted(count) => count.get(),
        }
    }

    /// What the cell said last, for a wait's failure: each range's status at each node, the
    /// clients' progress, each node's link to each peer, its log, its room, its longest poll, its
    /// stalls and its repair.
    fn seen(&self) -> String {
        format!(
            "{:?}, progress {:?}, links {:?}, log {:?}, room {:?}, longest polls {:?}, stalls {:?}, \
             repairs {:?}",
            self.status,
            self.progress,
            self.links,
            self.log,
            self.room,
            self.polls,
            self.stalls,
            self.repairs
        )
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
                    // A refusal for room moves nothing, and a round of compaction moves the cell
                    // only as its ranges apply and write again, which their statuses say: a node
                    // whose log refuses every write may count refusals for good.
                    Line::Room(compactions, waits) => {
                        self.room.insert(id, (compactions, waits));
                        return;
                    }
                    // A poll grown longer moves nothing: the cell moves as the node goes on.
                    Line::LongestPoll(longest) => {
                        self.polls.insert(id, (Instant::now(), longest));
                        return;
                    }
                    // Nor does a stall: the node says it once its own shard is let go, or while
                    // another shard is held.
                    Line::Stall(shard, stall) => {
                        self.stalls.insert((id, shard), (Instant::now(), stall));
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
                    Line::Chunks(received, written, held, unkept) => {
                        self.chunks.insert(id, (received, written, held, unkept));
                    }
                    Line::Enlisted => {
                        self.enlisted.insert(id);
                    }
                    Line::History(cell, own) => {
                        self.histories.insert(id, (cell, own));
                    }
                    Line::Stopping => {}
                    Line::Repair(repairing) => {
                        self.repairs.insert(id, repairing);
                    }
                    Line::Leaving => {
                        self.leaving.insert(id);
                    }
                    Line::Handing(group, to) => self.handing.push((id, group, to)),
                    Line::Handed(group, leader) => self.handed.push((id, group, leader)),
                    Line::Standing(member, state, absent) => {
                        self.standings.insert((id, member), (state, absent));
                    }
                    Line::Config(group, voters, learners) => {
                        self.configs.insert((id, group), (voters, learners));
                    }
                    Line::Left => {
                        self.left.insert(id);
                    }
                    Line::Gone(member, outcome) => {
                        self.gone.insert(member, outcome);
                    }
                    Line::Stopped(waits) => {
                        // Each wait the node's loop holds at once has a timer of its own: a sleep
                        // that found none waited for one to free, and each release ended the
                        // node's turn at once, a stop that turned without pause until it ended
                        // (`process::TIMERS`).
                        assert_eq!(
                            waits, 0,
                            "node {id}'s stop waited {waits} times for a timer"
                        );
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
            // A process ended that the cell did not kill: it failed, and its last words say why.
            Heard::Ended(id, generation, last) => {
                if self
                    .running
                    .get(&id)
                    .is_some_and(|r| r.generation == generation)
                {
                    // A process the test starts to be refused ends as the test expects.
                    if self.refusing != Some(id) {
                        panic!(
                            "node {id} ended: {:?}; the last {} lines of its standard error:\n{}",
                            self.status,
                            last.len(),
                            last.join("\n")
                        );
                    }
                    self.refused = self.running.remove(&id);
                    self.status.retain(|(node, _), _| *node != id);
                    self.held.remove(&id);
                    self.log.remove(&id);
                    self.room.remove(&id);
                    self.polls.remove(&id);
                    self.stalls.retain(|(node, _), _| *node != id);
                    self.transfers.remove(&id);
                }
            }
            // A client's progress moves the watch waiting on it, not the cell.
            Heard::Told(Told::Progress(client, done, patience)) => {
                self.progress.insert(client, done);
                if let Some(patience) = patience {
                    self.clients.insert(client, patience);
                }
                self.moved();
                return;
            }
            Heard::Told(Told::Failed(client, why)) => panic!("client {client} failed: {why}"),
        }
        self.moved();
        self.stirred_at = self.moved_at;
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

    /// Paces the nodes' QUIC links to each other, both ways, as `pace` says, or frees them.
    pub(crate) fn pace(&self, pace: Option<crate::relay::Pace>) {
        self.relay.as_ref().unwrap().pace(pace);
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
        let running: Vec<u64> = self.running.keys().copied().collect();
        self.until_by(Progress::CaughtUp(&running), |c| {
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
        self.until_by(Progress::Workload, |c| {
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
    pub(crate) fn client_begins(&mut self, id: u64) {
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

    /// What `thread` came to, the work of client `id`, `done` operations each told as its progress:
    /// waited on under the watch until it told the last, then joined. Its client began
    /// (`Cell::client_begins`) before it started.
    #[track_caller]
    pub(crate) fn joined<T>(&mut self, id: u64, done: u64, thread: JoinHandle<T>) -> T {
        let what = format!("client {id}'s {done} operations");
        self.waiting_on(&what, Duration::ZERO, Progress::Operations(id), |c| {
            if c.progress.get(&id).copied().unwrap_or(0) >= done {
                Some(())
            } else {
                None
            }
        });
        thread.join().unwrap()
    }

    /// What `thread` came to, waited on under the watch until it ended, then joined: work that
    /// blocks with no bound of its own, as a request through an S3 listener does, bounded by the
    /// watch. A failure, the watch's or the thread's own, names `what` at its caller.
    #[track_caller]
    pub(crate) fn watched<T>(&mut self, what: &str, thread: JoinHandle<T>) -> T {
        self.waiting_on(what, Duration::ZERO, Progress::Unmeasured, |_| {
            if thread.is_finished() { Some(()) } else { None }
        });
        match thread.join() {
            Ok(came) => came,
            Err(panicked) => {
                let said = panicked
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panicked.downcast_ref::<&str>().copied())
                    .unwrap_or("a panic with no message");
                panic!("{what} at {}: {said}", std::panic::Location::caller())
            }
        }
    }

    /// Waits until the gateway has done `done` of its operations.
    pub(crate) fn gateway_done(&mut self, done: u64) {
        self.until_by(Progress::Operations(GATEWAY), |c| {
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
    #[track_caller]
    pub(crate) fn objects(&mut self, ops: Vec<(u64, Op, Vec<u8>)>) -> Vec<ObjectOutcome> {
        self.objects_as(CLIENT, ops)
    }

    /// What `ops` came to, asked as [`Cell::objects`] asks them, by client `client`.
    #[track_caller]
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
        self.joined(id, done, thread)
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

    /// Waits until node `id`'s running process says what its failure detectors start from: the
    /// cell's history and its own part of it.
    pub(crate) fn history(&mut self, id: u64) -> (Lived, Lived) {
        self.until(|c| c.histories.get(&id).copied())
    }

    /// Waits until node `id`'s running process says the root range took its enlistment, which it
    /// asks until its member of the root range applies the answer: the wait goes on while the node
    /// catches its ranges up.
    #[track_caller]
    pub(crate) fn enlisted(&mut self, id: u64) {
        self.until_by(Progress::CaughtUp(&[id]), |c| {
            if c.enlisted.contains(&id) {
                Some(())
            } else {
                None
            }
        });
    }

    /// Waits until every running node judges each of its peers and has measured each range's
    /// patience (`patience` stated, `measuring` none): a failover from here is one the nodes'
    /// detectors judge, which a request's steps wait out (node.md §5.4). A node started again
    /// measures its links anew and suspects no peer meanwhile, so a leader killed before it judges
    /// them is not replaced while they are young.
    #[track_caller]
    pub(crate) fn judging(&mut self) {
        self.until(|c| {
            let groups: BTreeSet<u128> = c.status.keys().map(|&(_, group)| group).collect();
            let judged = c.running.keys().all(|&id| {
                groups.iter().all(|&group| {
                    matches!(
                        c.status.get(&(id, group)),
                        Some(Line::Status { patience, measuring, .. })
                            if !patience.is_zero() && measuring.is_zero()
                    )
                })
            });
            if judged { Some(()) } else { None }
        });
    }

    /// What `reads` read, each a query of a range's group, which the writer client asks in turn,
    /// knowing every node and told where the running ones listen.
    #[track_caller]
    pub(crate) fn read(&mut self, reads: Vec<(u128, Query)>) -> Vec<Result<Read, ClientError>> {
        self.client_begins(CLIENT);
        let done = reads.len() as u64;
        let (settings, bound) = (self.pki.client_settings(CLIENT), self.bound_quic());
        let tell = self.tell.clone();
        let thread = std::thread::spawn(move || reader(settings, bound, reads, tell));
        self.joined(CLIENT, done, thread)
    }

    /// The answers to `asks`, each an entry to a range's group, which the writer client asks in
    /// turn, knowing every node and told where the running ones listen.
    #[track_caller]
    pub(crate) fn asked(&mut self, asks: Vec<(u128, Entry)>) -> Vec<Vec<Answer>> {
        self.client_begins(CLIENT);
        let done = asks.len() as u64;
        let (settings, bound) = (self.pki.client_settings(CLIENT), self.bound_quic());
        let tell = self.tell.clone();
        let thread = std::thread::spawn(move || asker(settings, bound, asks, tell));
        self.joined(CLIENT, done, thread)
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
                        let why = format!("object operation {done} through node {node}: {error}");
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
                let (node, outcome) = match op {
                    Chunked::Put(node, volume, key, bytes, crc32c) => (
                        node,
                        client
                            .put_chunk(node, (volume, key), &bytes, crc32c, u64::MAX)
                            .await,
                    ),
                    Chunked::Get(node, volume, key, range) => (
                        node,
                        client.get_chunk(node, (volume, key), range, u64::MAX).await,
                    ),
                };
                match outcome {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(error) => {
                        let why = format!("chunk operation {done} through node {node}: {error}");
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
        metadata: Vec::new(),
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
        version: None,
    }
}
