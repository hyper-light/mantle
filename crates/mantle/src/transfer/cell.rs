//! A fresh cell of three for one case of `mantle bench transfer`: made by `mantle init` in a
//! directory of the run's, on loopback, each founder's volume stated at the bytes the case gives
//! it and in a failure domain of its own, and each founder run as `mantle serve` from what init
//! wrote, a child of this process. Each founder states its addresses with port 0, so each binds
//! what the operating system gives it, and once every one is bound each is told where the others
//! listen (`learn`), as an operator tells a node its peers; every founder pins the harness's gateway
//! before it starts. A child's status lines (`bound`, `s3-bound`, `enlisted`, each range's
//! `status`, and `chunks` and `counts` when asked) are read on a thread of its own, one for each node and no
//! more, and every wait ends on the line it waits for or on a process's output ending, never on a
//! guess of time; an answer asked of a running node, its `chunks` or `counts`, is waited for at
//! most the stock S3 clients' patience, as a node waits on a request (research/77 D2).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::Instant;

use mantle_node::process::{NativeShards, RangeShards};

use super::place::Gateway;

/// The founders a case's cell has: three, one failure domain each, so a block's three copies sit
/// on three nodes (durability.md §5; research/76).
pub const FOUNDERS: usize = 3;

#[derive(Debug)]
pub enum CellError {
    Io(String, std::io::Error),
    /// `mantle init` failed, or said what this runner cannot read.
    Init(String),
    /// A node's process ended before it said what it was waited for.
    Ended(u64),
    /// A running node did not answer what it was asked within the stock S3 clients' patience.
    Silent(u64),
}

impl std::fmt::Display for CellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(what, e) => write!(f, "{what}: {e}"),
            Self::Init(why) => write!(f, "mantle init: {why}"),
            Self::Ended(id) => write!(f, "node {id}'s process ended"),
            Self::Silent(id) => write!(
                f,
                "node {id} did not answer within the stock S3 clients' patience"
            ),
        }
    }
}

fn io(what: &str) -> impl FnOnce(std::io::Error) -> CellError + '_ {
    move |e| CellError::Io(what.to_owned(), e)
}

/// A node's chunks as it answers `chunks`: the bytes received from peers over QUIC, put on its own
/// volumes in-process, held by its volumes, and of the first two kept by none; and its volumes'
/// room, in the two things that refuse a put `Full`: the fragments their indexes hold and the most
/// they may, and the bytes of their free segments.
#[derive(Clone, Copy, Debug)]
pub struct Chunks {
    pub received: u64,
    pub written: u64,
    pub held: u64,
    pub unkept: u64,
    pub fragments: u64,
    pub budget: u64,
    pub free: u64,
}

/// A node's process's own account as it answers `counts` (`mantle_node::process::answer_counts`):
/// its allocator's counts since it was last asked, none where the build counts no allocations
/// (only a `count-allocs` build does), and the page faults and context switches its process was
/// charged since it began, none where the OS does not say them apart; then its write path's
/// (`mantle_node::process::write_path_counts`): its log's frames, their updates and its flushes,
/// and the entries it proposed and their commands, since it began, and its log's commit waits
/// since it was last asked, their count and three quantiles in nanoseconds, none where none ended;
/// then the table of what held its shard (`mantle_node::process::slow_poll_counts`), none where
/// the answer carries none; then the stalls hyper-rt's watchdog counted on each of its shards, its
/// own first (`mantle_node::process::stall_counts`), a shard's `None` where it was gone, none where
/// the answer carries none.
#[derive(Clone, Debug, Default)]
pub struct Counted {
    pub allocations: Option<u64>,
    pub reallocations: Option<u64>,
    pub moved: Option<u64>,
    pub bytes: Option<u64>,
    pub live: Option<i64>,
    pub peak: Option<i64>,
    pub minor: Option<u64>,
    pub major: Option<u64>,
    pub switches: Option<u64>,
    pub frames: Option<u64>,
    pub updates: Option<u64>,
    pub flushes: Option<u64>,
    pub entries: Option<u64>,
    pub commands: Option<u64>,
    pub waits: Option<u64>,
    pub wait_p50_ns: Option<u64>,
    pub wait_p99_ns: Option<u64>,
    pub wait_max_ns: Option<u64>,
    /// The sends a full lane between the node's shards refused (`lane_full_KIND`): Raft messages,
    /// proposals, reads, and every other kind summed (node.md §2.9).
    pub lane_full_step: Option<u64>,
    pub lane_full_propose: Option<u64>,
    pub lane_full_read: Option<u64>,
    pub lane_full_other: Option<u64>,
    /// The proposals and reads far ranges refused, and the stream's words held for want of room.
    pub far_refused_proposals: Option<u64>,
    pub far_refused_reads: Option<u64>,
    pub stream_held: Option<u64>,
    /// What made the node's devices flush (`mantle_node::process::flush_counts`): the log's
    /// confirmations written on their own and its slots filled ahead, the liveness stream's
    /// writes, the ranges' writes by what they carried, the engines' checkpoints and their stores'
    /// flushes, and the puts its chunk volumes took.
    pub confirmations: Option<u64>,
    pub fills: Option<u64>,
    pub liveness_writes: Option<u64>,
    pub entry_writes: Option<u64>,
    pub hard_state_writes: Option<u64>,
    pub commit_writes: Option<u64>,
    pub start_writes: Option<u64>,
    pub checkpoints: Option<u64>,
    pub engine_syncs: Option<u64>,
    pub chunk_puts: Option<u64>,
    /// The node's replay at open (`mantle_node::node::Node::replayed`; node.md §2.6): the entries
    /// its ranges re-applied past their engines' durable indexes, their bytes and the span, none
    /// while a range replays or where none had any.
    pub replay_entries: Option<u64>,
    pub replay_bytes: Option<u64>,
    pub replay_ns: Option<u64>,
    pub slow: Option<Vec<Slow>>,
    pub stalls: Option<Vec<Option<u64>>>,
}

/// An entry of the table of what held a node's shard, as its `counts` answer says it: the spawn
/// site of the tasks whose polls held the shard past twice its quantum and the label they had as
/// those polls ended, each `-` for none; the polls, how long they held the shard in all and at
/// most, in nanoseconds, and of them the task's and the host's. Each count is since the shard
/// began.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Slow {
    pub site: String,
    pub label: String,
    pub polls: u64,
    pub held_ns: u64,
    pub longest_ns: u64,
    pub task: u64,
    pub host: u64,
}

impl Counted {
    /// A `counts` line's fields past its first two words, each a name and a value or `-`. The
    /// table's entries are as many as its `slow_entries` says, each begun by its `slow_site`, and
    /// at most the table's own entries ([`hyper_rt::slow::ENTRIES`] and its unkeyed one); the
    /// stalls are as many shards' as its `stall_shards` says, at most the shards a process
    /// registers ([`hyper_rt::registry::MAX_SHARDS`]), each a count or `-`: an answer saying more,
    /// or an entry's or a shard's field before its count or not a count, is not read.
    fn parse(fields: &[&str]) -> Option<Self> {
        let mut counted = Self::default();
        let most = hyper_rt::slow::ENTRIES.checked_add(1)?;
        let mut entries = None;
        let mut shards = None;
        for pair in fields.chunks(2) {
            let [name, value] = pair else { return None };
            let unsigned = || value.parse::<u64>().ok();
            let signed = || value.parse::<i64>().ok();
            if let Some(field) = name.strip_prefix("stall_") {
                let stalls = counted.stalls.get_or_insert_with(Vec::new);
                match field {
                    "shards" => {
                        let n = value
                            .parse::<usize>()
                            .ok()
                            .filter(|n| *n <= hyper_rt::registry::MAX_SHARDS)?;
                        shards = Some(n);
                        stalls.reserve_exact(n);
                    }
                    "episodes" if stalls.len() < shards? => stalls.push(match *value {
                        "-" => None,
                        count => Some(count.parse().ok()?),
                    }),
                    _ => return None,
                }
                continue;
            }
            if let Some(kind) = name.strip_prefix("lane_full_") {
                let full = unsigned()?;
                let (slot, add) = match kind {
                    "step" => (&mut counted.lane_full_step, false),
                    "propose" => (&mut counted.lane_full_propose, false),
                    "read" => (&mut counted.lane_full_read, false),
                    _ => (&mut counted.lane_full_other, true),
                };
                *slot = match (*slot, add) {
                    (Some(sum), true) => Some(sum.saturating_add(full)),
                    _ => Some(full),
                };
                continue;
            }
            if let Some(field) = name.strip_prefix("slow_") {
                let slow = counted.slow.get_or_insert_with(Vec::new);
                match field {
                    "entries" => {
                        let n = value.parse::<usize>().ok().filter(|n| *n <= most)?;
                        entries = Some(n);
                        slow.reserve_exact(n);
                    }
                    "site" if slow.len() < entries? => slow.push(Slow {
                        site: (*value).to_owned(),
                        ..Slow::default()
                    }),
                    "label" => slow.last_mut()?.label = (*value).to_owned(),
                    "polls" => slow.last_mut()?.polls = unsigned()?,
                    "held_ns" => slow.last_mut()?.held_ns = unsigned()?,
                    "longest_ns" => slow.last_mut()?.longest_ns = unsigned()?,
                    "task" => slow.last_mut()?.task = unsigned()?,
                    "host" => slow.last_mut()?.host = unsigned()?,
                    _ => return None,
                }
                continue;
            }
            match *name {
                "allocs" => counted.allocations = unsigned(),
                "reallocs" => counted.reallocations = unsigned(),
                "moved" => counted.moved = unsigned(),
                "bytes" => counted.bytes = unsigned(),
                "live" => counted.live = signed(),
                "peak" => counted.peak = signed(),
                "minor" => counted.minor = unsigned(),
                "major" => counted.major = unsigned(),
                "switches" => counted.switches = unsigned(),
                "frames" => counted.frames = unsigned(),
                "updates" => counted.updates = unsigned(),
                "flushes" => counted.flushes = unsigned(),
                "entries" => counted.entries = unsigned(),
                "commands" => counted.commands = unsigned(),
                "waits" => counted.waits = unsigned(),
                "wait_p50_ns" => counted.wait_p50_ns = unsigned(),
                "wait_p99_ns" => counted.wait_p99_ns = unsigned(),
                "wait_max_ns" => counted.wait_max_ns = unsigned(),
                "far_refused_proposals" => counted.far_refused_proposals = unsigned(),
                "far_refused_reads" => counted.far_refused_reads = unsigned(),
                "stream_held" => counted.stream_held = unsigned(),
                "confirmations" => counted.confirmations = unsigned(),
                "fills" => counted.fills = unsigned(),
                "liveness_writes" => counted.liveness_writes = unsigned(),
                "entry_writes" => counted.entry_writes = unsigned(),
                "hard_state_writes" => counted.hard_state_writes = unsigned(),
                "commit_writes" => counted.commit_writes = unsigned(),
                "start_writes" => counted.start_writes = unsigned(),
                "checkpoints" => counted.checkpoints = unsigned(),
                "engine_syncs" => counted.engine_syncs = unsigned(),
                "chunk_puts" => counted.chunk_puts = unsigned(),
                "replay_entries" => counted.replay_entries = unsigned(),
                "replay_bytes" => counted.replay_bytes = unsigned(),
                "replay_ns" => counted.replay_ns = unsigned(),
                _ => {}
            }
        }
        if counted.slow.as_ref().map(Vec::len) != entries
            || counted.stalls.as_ref().map(Vec::len) != shards
        {
            return None;
        }
        Some(counted)
    }
}

/// What a node's process said, as this runner reads it.
enum Said {
    /// Its QUIC listener's address and its datagram plane's, once bound.
    Bound(SocketAddr, SocketAddr),
    /// Its S3 listener's address, once bound.
    S3(SocketAddr),
    /// The region its S3 endpoint serves and its clients sign for.
    Region(String),
    /// The root range took its enlistment.
    Enlisted,
    /// Its chunks, asked.
    Chunks(Chunks),
    /// Its process's counts, asked: boxed, the largest of what a node says by far, read twice a
    /// phase.
    Counts(Box<Counted>),
    /// A range's leader as the node knows it now, none while it knows of none.
    Status(u128, u64),
    /// The native clients it admits at once, as it derived them (`basis gateway.clients`).
    Clients(usize),
    /// Its output ended: the process is gone.
    Ended,
}

fn said(line: &str) -> Option<Said> {
    let fields: Vec<&str> = line.split_whitespace().collect();
    match fields.as_slice() {
        ["bound", quic, plane] => Some(Said::Bound(quic.parse().ok()?, plane.parse().ok()?)),
        ["s3-bound", at, _connections] => at.parse().ok().map(Said::S3),
        ["s3", _listen, region, _ids] => Some(Said::Region((*region).to_owned())),
        ["enlisted"] => Some(Said::Enlisted),
        ["basis", "gateway.clients", "=", clients, ..] => clients.parse().ok().map(Said::Clients),
        ["counts", _counting, rest @ ..] => Counted::parse(rest).map(|c| Said::Counts(Box::new(c))),
        ["status", group, _term, leader, ..] => Some(Said::Status(
            u128::from_str_radix(group, 16).ok()?,
            leader.parse().ok()?,
        )),
        [
            "chunks",
            received,
            written,
            held,
            unkept,
            fragments,
            budget,
            free,
        ] => Some(Said::Chunks(Chunks {
            received: received.parse().ok()?,
            written: written.parse().ok()?,
            held: held.parse().ok()?,
            unkept: unkept.parse().ok()?,
            fragments: fragments.parse().ok()?,
            budget: budget.parse().ok()?,
            free: free.parse().ok()?,
        })),
        _ => None,
    }
}

/// A founder: its id and directory as init wrote them, and its process while it runs.
struct Node {
    id: u64,
    dir: PathBuf,
    child: Option<Child>,
    /// Its standard input, held open: a node reads its operator's commands there.
    input: Option<ChildStdin>,
    /// Which of its processes this is, so a line a killed one said is not taken as its successor's.
    generation: u64,
    quic: Option<SocketAddr>,
    plane: Option<SocketAddr>,
    s3: Option<SocketAddr>,
    region: Option<String>,
    enlisted: bool,
    chunks: Option<Chunks>,
    counts: Option<Counted>,
    /// Each range's leader as its process last said, zero for none.
    leaders: BTreeMap<u128, u64>,
    /// The native clients it admits at once, as it last said.
    clients: Option<usize>,
}

/// Where the harness's clients of a cell on loopback bind.
const LOOPBACK: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);

/// What a case's phases reach a cell by: the cell this runner makes and runs ([`Cell`]) or one
/// running elsewhere (`super::outside::Outside`).
pub trait Reach {
    /// The directory `mantle init` made the cell in.
    fn out(&self) -> &Path;
    /// The gateway every founder pins.
    fn gateway(&self) -> &Gateway;
    /// Each node's id and where its QUIC listener listens.
    fn quic(&self) -> Vec<(u64, SocketAddr)>;
    /// Each node's S3 listener, `HOST:PORT`, and the region its endpoint signs for.
    fn s3(&self) -> Vec<(String, String)>;
    /// Where the harness's clients bind: an address of the family its nodes are dialed at.
    fn bind(&self) -> SocketAddr;
    /// The nodes the cell names by a host name, each with its name: none for a cell on loopback,
    /// whose nodes are dialed at the addresses they say.
    fn names(&self) -> Vec<(u64, String)> {
        Vec::new()
    }
}

impl Cell {
    /// The native clients the cell's nodes admit at once, as each said it derived them: a client
    /// past them is refused at its node's admission (node.md §3.6).
    pub fn native_clients(&self) -> usize {
        self.nodes
            .iter()
            .map(|n| n.clients.unwrap_or(0))
            .fold(0usize, usize::saturating_add)
    }
}

impl Reach for Cell {
    fn out(&self) -> &Path {
        &self.out
    }

    fn gateway(&self) -> &Gateway {
        &self.gateway
    }

    fn quic(&self) -> Vec<(u64, SocketAddr)> {
        self.nodes
            .iter()
            .filter_map(|n| n.quic.map(|q| (n.id, q)))
            .collect()
    }

    fn s3(&self) -> Vec<(String, String)> {
        self.nodes
            .iter()
            .filter_map(|n| Some((n.s3?.to_string(), n.region.clone()?)))
            .collect()
    }

    fn bind(&self) -> SocketAddr {
        LOOPBACK
    }
}

/// A running cell of three, its processes this runner's children.
pub struct Cell {
    exe: PathBuf,
    out: PathBuf,
    nodes: Vec<Node>,
    tell: Sender<(usize, u64, Said)>,
    heard: Receiver<(usize, u64, Said)>,
    /// The gateway every founder pins, which the harness reads the cell's metadata as.
    gateway: Gateway,
}

impl Cell {
    /// A cell made in `dir`, which holds nothing yet, each founder's volume stated at
    /// `volume_bytes`, the shards it places its ranges over at `range_shards` (node.md §2.9) and
    /// those it drives its native requests on at `native_shards` (step 6f), every founder started
    /// and enlisted.
    pub fn make(
        dir: &Path,
        volume_bytes: u64,
        (range_shards, native_shards): (RangeShards, NativeShards),
    ) -> Result<Self, CellError> {
        let exe = std::env::current_exe().map_err(io("this program's path"))?;
        let machines = dir.join("machines.conf");
        let mut text = format!("max_nodes = {FOUNDERS}\nmax_volumes = {FOUNDERS}\n");
        for n in 0..FOUNDERS {
            let volume = dir.join(format!("volume-{n}"));
            std::fs::create_dir_all(&volume).map_err(io("a founder's volume directory"))?;
            text.push_str(&format!(
                "[node]\nquic = 127.0.0.1:0\nplane = 127.0.0.1:0\ns3 = 127.0.0.1:0\n\
                 domain = rack-{n}\nvolume = {} {volume_bytes}\n",
                volume.display()
            ));
        }
        std::fs::write(&machines, text).map_err(io("the machines file"))?;
        let out = dir.join("cell");
        let made = Command::new(&exe)
            .arg("init")
            .arg(&out)
            .arg("--founders")
            .arg(&machines)
            .output()
            .map_err(io("mantle init"))?;
        let stdout = String::from_utf8_lossy(&made.stdout);
        if !made.status.success() {
            return Err(CellError::Init(format!(
                "{stdout}{}",
                String::from_utf8_lossy(&made.stderr)
            )));
        }
        let mut nodes = Vec::new();
        for line in stdout.lines() {
            let Some(rest) = line.strip_prefix("node ") else {
                continue;
            };
            let (id, rest) = rest
                .split_once(' ')
                .ok_or_else(|| CellError::Init(format!("a node line: {line}")))?;
            let (founder, _) = rest
                .split_once(": ")
                .ok_or_else(|| CellError::Init(format!("a node line: {line}")))?;
            nodes.push(Node {
                id: id
                    .parse()
                    .map_err(|_| CellError::Init(format!("a node's id: {line}")))?,
                dir: PathBuf::from(founder),
                child: None,
                input: None,
                generation: 0,
                quic: None,
                plane: None,
                s3: None,
                region: None,
                enlisted: false,
                chunks: None,
                counts: None,
                leaders: BTreeMap::new(),
                clients: None,
            });
        }
        if nodes.len() != FOUNDERS {
            return Err(CellError::Init(format!(
                "{} founders said, not {FOUNDERS}: {stdout}",
                nodes.len()
            )));
        }
        // Each founder's operator's statements, `mantle init`'s, and the shards its ranges and its
        // native requests take.
        for node in &nodes {
            let config = node.dir.join("config");
            let mut text = std::fs::read_to_string(&config).map_err(io("a founder's config"))?;
            text.push_str(&format!(
                "range_shards = {range_shards}\nnative_shards = {native_shards}\n"
            ));
            std::fs::write(&config, text).map_err(io("a founder's config"))?;
        }
        // Pinned before any founder starts: a founder reads its peers once, at its start.
        crate::init::pin_gateway(&mut Vec::new(), &out.join(super::place::GATEWAY), &out)
            .map_err(|e| CellError::Init(format!("the gateway: {e}")))?;
        let gateway = Gateway::open(&out, LOOPBACK)
            .map_err(|e| CellError::Init(format!("the gateway: {e}")))?;
        let (tell, heard) = channel();
        let mut cell = Self {
            exe,
            out,
            nodes,
            tell,
            heard,
            gateway,
        };
        // Started together, then waited for: each measures its machine and its device at its
        // first `mantle serve` before it binds (node.md §1.7), and those measurements overlap.
        for at in 0..FOUNDERS {
            cell.spawn(at)?;
        }
        for at in 0..FOUNDERS {
            cell.listening(at)?;
        }
        cell.introduce()?;
        for at in 0..FOUNDERS {
            cell.until(at, |node| node.enlisted, None)?;
        }
        Ok(cell)
    }

    /// Starts node `at`'s process again on its directory, tells every running node where the
    /// others listen now, its own ports chosen anew, and waits until the root range took its
    /// enlistment.
    pub fn restart(&mut self, at: usize) -> Result<(), CellError> {
        self.spawn(at)?;
        self.listening(at)?;
        self.introduce()?;
        self.until(at, |node| node.enlisted, None)
    }

    /// Waits until node `at`'s listeners both listen.
    fn listening(&mut self, at: usize) -> Result<(), CellError> {
        self.until(
            at,
            |node| node.quic.is_some() && node.s3.is_some() && node.region.is_some(),
            None,
        )
    }

    /// Starts node `at`'s process, its output read on a thread of its own.
    fn spawn(&mut self, at: usize) -> Result<(), CellError> {
        let tell = self.tell.clone();
        let node = self
            .nodes
            .get_mut(at)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
        let mut child = Command::new(&self.exe)
            .arg("serve")
            .arg(node.dir.join("config"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(io("mantle serve"))?;
        let output = child.stdout.take().ok_or_else(|| {
            CellError::Io(
                "mantle serve's output".into(),
                std::io::Error::from(std::io::ErrorKind::BrokenPipe),
            )
        })?;
        node.input = child.stdin.take();
        node.child = Some(child);
        node.generation = node.generation.saturating_add(1);
        node.quic = None;
        node.plane = None;
        node.s3 = None;
        node.region = None;
        node.enlisted = false;
        node.leaders.clear();
        let generation = node.generation;
        std::thread::Builder::new()
            .name(format!("node-{}", node.id))
            .spawn(move || {
                // A node says a range's status each time its commit moves, several times an
                // object, and the cell takes what its threads tell only as it waits: each range's
                // leader is told as it changes, the one fact of a status the cell waits on, so what
                // waits to be taken is bounded by the cell's events, not by its objects.
                let mut leaders: BTreeMap<u128, u64> = BTreeMap::new();
                for line in BufReader::new(output).lines() {
                    let Ok(line) = line else { break };
                    let Some(said) = said(&line) else {
                        continue;
                    };
                    if let Said::Status(group, leader) = said
                        && leaders.insert(group, leader) == Some(leader)
                    {
                        continue;
                    }
                    if tell.send((at, generation, said)).is_err() {
                        return;
                    }
                }
                let _ = tell.send((at, generation, Said::Ended));
            })
            .map_err(io("a node's reader thread"))?;
        Ok(())
    }

    /// Waits until node `at` holds `done`, taking every node's lines meanwhile; a running node's
    /// process ending first, its own or another's, is an error, and so is `by` passing first.
    fn until(
        &mut self,
        at: usize,
        done: impl Fn(&Node) -> bool,
        by: Option<Instant>,
    ) -> Result<(), CellError> {
        loop {
            let node = self
                .nodes
                .get(at)
                .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
            if done(node) {
                return Ok(());
            }
            let id = node.id;
            if !self.hear(by)? {
                return Err(CellError::Silent(id));
            }
        }
    }

    /// When an answer asked of a node now is waited for at most: the stock S3 clients' patience
    /// (`bounds::STOCK_PATIENCE`, research/77 D2), the most a node waits on any request that
    /// states no patience of its own, as no wait is without a bound (CLAUDE.md §2). A node that
    /// says nothing for that long while it runs is taken for wedged, not waited on without end.
    fn answer_by() -> Result<Instant, CellError> {
        Instant::now()
            .checked_add(mantle_protocol::bounds::STOCK_PATIENCE)
            .ok_or_else(|| CellError::Init("the stock patience past the clock".into()))
    }

    /// Tells each running node where every other listens, its QUIC and its plane: `learn ID QUIC
    /// PLANE` on its standard input, a line a peer.
    fn introduce(&mut self) -> Result<(), CellError> {
        let bound: Vec<(u64, SocketAddr, SocketAddr)> = self
            .nodes
            .iter()
            .filter_map(|n| Some((n.id, n.quic?, n.plane?)))
            .collect();
        for node in &mut self.nodes {
            let Some(input) = node.input.as_mut() else {
                continue;
            };
            for (peer, quic, plane) in bound.iter().filter(|(peer, ..)| *peer != node.id) {
                writeln!(input, "learn {peer} {quic} {plane}").map_err(io("a node's input"))?;
            }
            input.flush().map_err(io("a node's input"))?;
        }
        Ok(())
    }

    /// Node `at`'s chunks, asked now on its standard input ([`Chunks`]).
    pub fn chunks(&mut self, at: usize) -> Result<Chunks, CellError> {
        let node = self
            .nodes
            .get_mut(at)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
        node.chunks = None;
        let input = node.input.as_mut().ok_or(CellError::Ended(node.id))?;
        input.write_all(b"chunks\n").map_err(io("a node's input"))?;
        input.flush().map_err(io("a node's input"))?;
        self.until(at, |node| node.chunks.is_some(), Some(Self::answer_by()?))?;
        self.nodes
            .get(at)
            .and_then(|node| node.chunks)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))
    }

    /// Node `at`'s process's counts, asked now on its standard input ([`Counted`]): a node
    /// answers with what it counted since it was last asked, and counts afresh from there.
    pub fn counts(&mut self, at: usize) -> Result<Counted, CellError> {
        let node = self
            .nodes
            .get_mut(at)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
        node.counts = None;
        let input = node.input.as_mut().ok_or(CellError::Ended(node.id))?;
        input.write_all(b"counts\n").map_err(io("a node's input"))?;
        input.flush().map_err(io("a node's input"))?;
        self.until(at, |node| node.counts.is_some(), Some(Self::answer_by()?))?;
        self.nodes
            .get(at)
            .and_then(|node| node.counts.clone())
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))
    }

    /// Node `at`'s process's id while it runs, which the OS's account of it is read by.
    pub fn pid(&self, at: usize) -> Option<u32> {
        self.nodes.get(at)?.child.as_ref().map(Child::id)
    }

    /// Waits until every running node names a running node as the leader of every range any of
    /// them knows: after a kill, until each range the killed node led has elected another, the
    /// fact a read through the others needs.
    pub fn led(&mut self) -> Result<(), CellError> {
        loop {
            let running: Vec<u64> = self
                .nodes
                .iter()
                .filter(|n| n.child.is_some())
                .map(|n| n.id)
                .collect();
            let groups: std::collections::BTreeSet<u128> = self
                .nodes
                .iter()
                .filter(|n| n.child.is_some())
                .flat_map(|n| n.leaders.keys().copied())
                .collect();
            let led = self.nodes.iter().filter(|n| n.child.is_some()).all(|n| {
                groups.iter().all(|g| {
                    n.leaders
                        .get(g)
                        .is_some_and(|leader| running.contains(leader))
                })
            });
            if led && !groups.is_empty() {
                return Ok(());
            }
            self.hear(None)?;
        }
    }

    /// Takes the next line any node's process said, by `by` where it is given: whether one came
    /// by then. A running node's process ending is an error.
    fn hear(&mut self, by: Option<Instant>) -> Result<bool, CellError> {
        let ended = || CellError::Init("every node's reader ended".into());
        let heard = match by {
            None => self.heard.recv().map_err(|_| ended())?,
            Some(by) => match self
                .heard
                .recv_timeout(by.saturating_duration_since(Instant::now()))
            {
                Ok(heard) => heard,
                Err(RecvTimeoutError::Timeout) => return Ok(false),
                Err(RecvTimeoutError::Disconnected) => return Err(ended()),
            },
        };
        let (from, generation, said) = heard;
        let Some(node) = self.nodes.get_mut(from) else {
            return Ok(true);
        };
        if generation != node.generation {
            return Ok(true);
        }
        match said {
            Said::Bound(quic, plane) => {
                node.quic = Some(quic);
                node.plane = Some(plane);
            }
            Said::S3(s3) => node.s3 = Some(s3),
            Said::Region(region) => node.region = Some(region),
            Said::Enlisted => node.enlisted = true,
            Said::Clients(clients) => node.clients = Some(clients),
            Said::Chunks(chunks) => node.chunks = Some(chunks),
            Said::Counts(counts) => node.counts = Some(*counts),
            Said::Status(group, leader) => {
                node.leaders.insert(group, leader);
            }
            Said::Ended => {
                node.input = None;
                if node.child.take().is_some() {
                    return Err(CellError::Ended(node.id));
                }
            }
        }
        Ok(true)
    }

    /// Kills node `at`'s process, `SIGKILL` on Unix and `TerminateProcess` on Windows, and reaps
    /// it.
    pub fn kill(&mut self, at: usize) -> Result<(), CellError> {
        let node = self
            .nodes
            .get_mut(at)
            .ok_or_else(|| CellError::Init(format!("no founder {at}")))?;
        node.input = None;
        if let Some(mut child) = node.child.take() {
            child.kill().map_err(io("a node's kill"))?;
            child.wait().map_err(io("a node's end"))?;
        }
        Ok(())
    }
}

impl Drop for Cell {
    fn drop(&mut self) {
        for at in 0..self.nodes.len() {
            let _ = self.kill(at);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `counts` answer is read whole: the binary's account, then the write path's counts the
    /// node says after it, a quantile none ended said `-`.
    #[test]
    fn a_counts_answer_is_read_with_its_write_path() {
        let line = "counts counted allocs 1 reallocs 2 moved 3 bytes 4 live 5 peak 6 minor 7 major - \
                    switches 8 frames 9 updates 10 flushes 11 entries 12 commands 13 waits 14 \
                    wait_p50_ns 15 wait_p99_ns 16 wait_max_ns 17";
        let Some(Said::Counts(c)) = said(line) else {
            panic!("a counts answer not read: {line}");
        };
        assert_eq!(
            (c.allocations, c.peak, c.major, c.switches),
            (Some(1), Some(6), None, Some(8))
        );
        assert_eq!(
            (c.frames, c.updates, c.flushes, c.entries, c.commands),
            (Some(9), Some(10), Some(11), Some(12), Some(13))
        );
        assert_eq!(
            (c.waits, c.wait_p50_ns, c.wait_p99_ns, c.wait_max_ns),
            (Some(14), Some(15), Some(16), Some(17))
        );

        let line = "counts uncounted frames 0 updates 0 flushes 0 entries 0 commands 0 waits 0 \
                    wait_p50_ns - wait_p99_ns - wait_max_ns -";
        let Some(Said::Counts(c)) = said(line) else {
            panic!("a counts answer not read: {line}");
        };
        assert_eq!((c.allocations, c.frames, c.waits), (None, Some(0), Some(0)));
        assert_eq!(
            (c.wait_p50_ns, c.wait_p99_ns, c.wait_max_ns),
            (None, None, None)
        );
        assert_eq!(c.slow, None, "an answer with no table");
        assert_eq!(c.stalls, None, "an answer with no stalls");
    }

    /// The lanes between a node's shards, as the node says them in its `counts` answer
    /// (`mantle_node::process::lane_counts`), are read back: Raft messages, proposals and reads by
    /// kind, every other kind summed, and the far ranges' refusals and the stream's words held.
    #[test]
    fn a_counts_answers_lanes_are_read_back() {
        let mut line = "counts uncounted frames 1".to_owned();
        let counts = mantle_node::node::LaneCounts {
            full: vec![
                ("adopt", 1),
                ("step", 2),
                ("propose", 3),
                ("read", 4),
                ("measure", 5),
            ],
            held_words: 6,
            refused_proposals: 7,
            refused_reads: 8,
        };
        mantle_node::process::lane_counts(&mut line, &counts).unwrap();
        let Some(Said::Counts(c)) = said(&line) else {
            panic!("a counts answer not read: {line}");
        };
        assert_eq!(
            (
                c.lane_full_step,
                c.lane_full_propose,
                c.lane_full_read,
                c.lane_full_other
            ),
            (Some(2), Some(3), Some(4), Some(6))
        );
        assert_eq!(
            (c.far_refused_proposals, c.far_refused_reads, c.stream_held),
            (Some(7), Some(8), Some(6))
        );
        let Some(Said::Counts(c)) = said("counts uncounted frames 1") else {
            panic!("a counts answer not read");
        };
        assert_eq!((c.lane_full_step, c.stream_held), (None, None));
    }

    /// What made a node's devices flush, as the node says it in its `counts` answer
    /// (`mantle_node::process::flush_counts`), is read back cause by cause.
    #[test]
    fn a_counts_answers_flush_causes_are_read_back() {
        let mut line = "counts uncounted frames 1".to_owned();
        let causes = mantle_node::node::FlushCauses {
            liveness: 3,
            entries: 4,
            hard_states: 5,
            commits: 6,
            starts: 7,
            checkpoints: 8,
            engine_syncs: 16,
            chunk_puts: 9,
        };
        mantle_node::process::flush_counts(&mut line, (1, 2), &causes).unwrap();
        let Some(Said::Counts(c)) = said(&line) else {
            panic!("a counts answer not read: {line}");
        };
        assert_eq!(
            (
                c.confirmations,
                c.fills,
                c.liveness_writes,
                c.entry_writes,
                c.hard_state_writes
            ),
            (Some(1), Some(2), Some(3), Some(4), Some(5))
        );
        assert_eq!(
            (
                c.commit_writes,
                c.start_writes,
                c.checkpoints,
                c.engine_syncs,
                c.chunk_puts
            ),
            (Some(6), Some(7), Some(8), Some(16), Some(9))
        );
        let Some(Said::Counts(c)) = said("counts uncounted frames 1") else {
            panic!("a counts answer not read");
        };
        assert_eq!((c.confirmations, c.chunk_puts), (None, None));
    }

    /// The stalls on each of a node's shards, as the node says them in its `counts` answer
    /// (`mantle_node::process::stall_counts`), are read back shard by shard, a shard gone none;
    /// an answer whose shards are not as many as it says, more than a process registers, a
    /// shard's count before how many or one not a count, is not read.
    #[test]
    fn a_counts_answers_stalls_are_read_back() {
        let mut line = "counts uncounted frames 1".to_owned();
        mantle_node::process::slow_poll_counts(&mut line, &[]).unwrap();
        mantle_node::process::stall_counts(&mut line, &[Some(0), Some(3), None]).unwrap();
        let Some(Said::Counts(c)) = said(&line) else {
            panic!("a counts answer not read: {line}");
        };
        assert_eq!(c.frames, Some(1));
        assert_eq!(c.slow, Some(Vec::new()));
        assert_eq!(c.stalls, Some(vec![Some(0), Some(3), None]));

        let most = hyper_rt::registry::MAX_SHARDS;
        for bad in [
            "counts uncounted stall_shards 2 stall_episodes 1".to_owned(),
            "counts uncounted stall_shards 1 stall_episodes 1 stall_episodes 1".to_owned(),
            format!("counts uncounted stall_shards {}", most + 1),
            "counts uncounted stall_episodes 1 stall_shards 1".to_owned(),
            "counts uncounted stall_shards 1 stall_episodes x".to_owned(),
            "counts uncounted stall_shards 1 stall_held 1".to_owned(),
        ] {
            assert!(!matches!(said(&bad), Some(Said::Counts(_))), "read: {bad}");
        }
        let full = format!(
            "counts uncounted stall_shards {most} {}",
            vec!["stall_episodes 0"; most].join(" ")
        );
        let Some(Said::Counts(c)) = said(&full) else {
            panic!("every shard's stalls not read");
        };
        assert_eq!(c.stalls.map(|s| s.len()), Some(most));
    }

    /// The table of what held a node's shard, as the node says it in its `counts` answer
    /// (`mantle_node::process::slow_poll_counts`), is read back entry by entry; an answer whose
    /// entries are not as many as it says, more than the table holds, or a field outside an entry
    /// is not read.
    #[test]
    fn a_counts_answers_slow_poll_table_is_read_back() {
        static OP: hyper_rt::slow::Label = hyper_rt::slow::Label::new("GetObject");
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
                host_polls: 1,
            },
        ];
        let mut line =
            "counts uncounted frames 1 updates 2 flushes 3 entries 4 commands 5 waits 0 \
                        wait_p50_ns - wait_p99_ns - wait_max_ns -"
                .to_owned();
        mantle_node::process::slow_poll_counts(&mut line, &polls).unwrap();
        let Some(Said::Counts(c)) = said(&line) else {
            panic!("a counts answer not read: {line}");
        };
        assert_eq!((c.frames, c.commands), (Some(1), Some(5)));
        let at = format!("{}:{}:{}", site.file(), site.line(), site.column());
        assert_eq!(
            c.slow,
            Some(vec![
                Slow {
                    site: at,
                    label: "GetObject".to_owned(),
                    polls: 3,
                    held_ns: 900,
                    longest_ns: 500,
                    task: 2,
                    host: 1,
                },
                Slow {
                    site: "-".to_owned(),
                    label: "-".to_owned(),
                    polls: 1,
                    held_ns: 7,
                    longest_ns: 7,
                    task: 0,
                    host: 1,
                },
            ])
        );

        // An empty table is a table, not none.
        let mut empty = "counts uncounted".to_owned();
        mantle_node::process::slow_poll_counts(&mut empty, &[]).unwrap();
        let Some(Said::Counts(c)) = said(&empty) else {
            panic!("a counts answer not read: {empty}");
        };
        assert_eq!(c.slow, Some(Vec::new()));

        // Fewer entries than said, more than a table holds, a field outside an entry, a count that
        // is not one.
        let entry = "slow_site a.rs:1:2 slow_label - slow_polls 1 slow_held_ns 1 slow_longest_ns 1 \
                     slow_task 0 slow_host 0";
        let most = hyper_rt::slow::ENTRIES + 1;
        for bad in [
            format!("counts uncounted slow_entries 2 {entry}"),
            format!("counts uncounted slow_entries {} {entry}", most + 1),
            format!("counts uncounted slow_entries 0 {entry}"),
            format!("counts uncounted slow_label x slow_entries 1 {entry}"),
            "counts uncounted slow_entries 1 slow_site a.rs:1:2 slow_polls -".to_owned(),
        ] {
            assert!(!matches!(said(&bad), Some(Said::Counts(_))), "read: {bad}");
        }
        let entries = vec![entry; most].join(" ");
        let full = format!("counts uncounted slow_entries {most} {entries}");
        let Some(Said::Counts(c)) = said(&full) else {
            panic!("a full table not read");
        };
        assert_eq!(c.slow.map(|s| s.len()), Some(most));
    }
}
