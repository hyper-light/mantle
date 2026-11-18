//! `mantle bench doors`: each operation's cost through both of mantle's doors, the native protocol
//! (mantle-client) and the S3 listener, against a cell of three `mantle serve` processes this
//! program makes and runs (docs/design/measurement.md §12; research/70). The load is mantle's own:
//! the native door's requests are mantle-client's, and the S3 listener's are written here by
//! mantle's HTTP codec (each request's head, an `aws-chunked` body's chunks and trailer, and every
//! response body's decoding) and signed with mantle-s3's SigV4. The stock clients stay the
//! correctness suite (`mantle bench transfer`).
//!
//! Every object's bytes are made from the run's seed and the object's name
//! (`transfer::generate`), and every read is checked by making them again; an ETag is checked
//! against the MD5 of those bytes for each phase's first [`ETAGS`] keys. A set's acknowledged
//! objects are a bit each (`transfer::Oracle`). Three controls, a byte flipped on its way to the
//! check, a key never put and an ETag expected of another object, must each be reported as
//! failures, or the run is not exact; a door's controls whose objects could not be put are not
//! run, so not caught. A phase's failures are told apart: `refused`, answered with an error;
//! `unanswered`, throttled or lost on every attempt (each operation asked again as the AWS SDKs ask
//! one, a listing's page and a control's put among them), whose outcome is unknown, as S3 leaves a
//! 5xx's; `differed`, a read whose bytes or length were not what was put; and the checks' others
//! (an ETag, a size, a listed key not the set's or out of order, an acknowledged object not listed
//! or missing), the rest. An operation whose object's put, upload or part failed before it is
//! not asked at all: `skipped`, counted and timed not at all, so a size's reads, as a set's, read
//! only what was acknowledged (`transfer::Oracle`, a bit a put).
//!
//! A phase is one door, one operation and one size, and says one line: what the three servers
//! counted while it ran, per operation (each answers `counts` on its standard input: its
//! allocator's counts in a `count-allocs` build, its faults and switches in any), what the OS
//! charged each server's process (read by pid), what the harness's own thread allocated for the
//! doors beside its own work, which is set aside (`alloc::aside`), and the latency of each
//! operation from its intended start (measurement.md §10), and the write path's counts the servers
//! answer beside their own (`counts`): the cell's commands an entry its leaders proposed, its logs'
//! updates a frame, and their frames and flushes an operation, every server's summed; and its
//! logs' commit waits over the phase, each quantile the slowest server's, as quantiles do not sum
//! ([`log_wait`]); the lanes between the servers' shards ([`lane_columns`]): the sends a full lane
//! refused by kind, the proposals and reads far ranges refused, and the stream's words held, each
//! every server's growth summed; and what held the servers' shards ([`slow_grown`]): the polls past twice a
//! shard's quantum an operation, and the label whose polls held the shards longest in all
//! ([`slow_columns`]), each entry of the servers' tables that grew over the phase said on a line
//! of its own after the phase's, `doors DOOR OP SIZE CONDITION slow`, then its server, spawn
//! site, label, polls, how long they held the shard in all and at most and whose time they were,
//! each a name and a value; and the polls hyper-rt's watchdog saw hold a server's shard past its
//! limit ([`stalls_grown`]), every server's shards' summed (`stalls`), so a phase says whether any
//! shard stalled, each shard that did said on a line of its own after the phase's, `doors DOOR OP
//! SIZE CONDITION stall`, then its server, its shard and its stalls. The servers' peak live heap
//! over the
//! phase, above what they held as it began, is given per request, over the most requests the
//! harness had out at once, and per connection, over the connections the phase held. Logical
//! clients are tasks on one hyper-rt shard, each holding one connection and the buffers its
//! operations work in ([`Logical`]), an HTTP connection carrying one request at a time; open loop,
//! their requests arrive as a Poisson process and each waits for a free client; closed, a ladder
//! of clients doubles while throughput grows, up to `--clients`, each step judged in rounds as a
//! benchmark point is (measurement.md §5, [`ROUNDS`]) and its growth told by their intervals, the
//! ladder ending three steps after the last that grew ([`FLAT`]), or at a step that left an
//! operation unanswered, which asks no more once one is ([`Climb::after`]). Each phase says the
//! threads the process ran as the OS counts them, read as its clients' requests were out, and what
//! the harness's own work held at its peak: a phase's operations allocate nothing of the
//! harness's, so neither grows with the clients or the objects.
//!
//! A measure this build or this OS does not give prints `-`: the allocators' counts outside a
//! `count-allocs` build, instructions and cycles where the OS does not count them per process
//! (Linux: `usage::of` reads none; `--perf` counts them with `perf stat`, [`Perf`]), a major fault
//! count on Windows, and datagrams on the HTTP door, which has none of its own. The native door's
//! wire bytes and datagrams are what its clients' sockets sent and received (`Client::wire`), the
//! HTTP door's the octets each connection wrote and read.
//! A quantile is a dash below the samples that resolve it at 95% (`quantile::samples_for`; §12's
//! rounds).
//!
//! Against an `--endpoint`, an S3 server this program did not start, the HTTP door's phases,
//! controls and slope run as they do against a cell, the server's faults, CPU and footprint read
//! by the pids `--server-pid` names, its allocations unknown; a phase it refuses whole is reported
//! excluded with its answer.

use std::cell::RefCell;
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel as watch_channel};
use std::time::{Duration, Instant};

use hyper_measure::alloc::{self, Counts};
use hyper_measure::open_loop::Arrivals;
use hyper_measure::quantile::{Level, ranks, samples_for};
use hyper_measure::usage::{self, Usage};
use hyper_rt::combine::{Either, TaskSet, race2};
use hyper_rt::futures::{now_ns, sleep};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::tcp::TcpStream;
use mantle_client::{Client, Native, Rewind, Settings, Sink, Source, TRAILER, Wire};
use mantle_disk::histogram::Histogram;
use mantle_disk::rounds::{self, Judgment, Policy};
use mantle_http::write::Length;
use mantle_node::process::RangeShards;
use mantle_protocol::object::{Headers, List, Listing, Op, Reply, Status};
use mantle_s3::checksum::{self, Algorithm, Hasher};
use mantle_s3::list::Resume;
use mantle_s3::{crypto, sigv4};

use crate::drive;
use crate::transfer::cell::{self, Cell, Counted, Reach, Slow};
use crate::transfer::generate::{Generator, Object};
use crate::transfer::native::{Checked, Flipped, Made, Unwatched};
use crate::transfer::{Oracle, size as small_size};

/// The bucket every phase's objects are in.
const BUCKET: &str = "doors";

/// The keys of a phase whose ETags are checked: the first of its keys, each one's MD5 made once
/// before the phase from the generator, so the check holds no table that grows with the phase.
const ETAGS: u64 = 16;

/// How a closed ladder's step is judged: in rounds, ten at least and thirty at most, ending once
/// they are independent and their median lies within ±5% at 95% (measurement.md §5,
/// `Policy::STANDARD`), as a benchmark point's are. A step's growth over the best before it is told
/// by their intervals, not by one throughput each: a single step's rate moved 13–30% from run to
/// run on the doors (research/71), so a ladder that stopped at the first step not a tenth above
/// the last ended where one noisy step fell.
const ROUNDS: Policy = Policy::STANDARD;

/// The steps in a row a closed ladder takes without growth before it ends: BBR's estimate that
/// the pipe is full, "after 3 rounds w/o significant bw growth" (Linux v6.12
/// `net/ipv4/tcp_bbr.c`, `bbr_full_bw_cnt`; research/87 [6]), so one step a stall slowed does not
/// end the ladder below its knee, and the steps past it are measured.
const FLAT: u32 = 3;

/// The wait the harness's watch asks at a time ([`Watch`]): long enough that Linux's default timer
/// slack, 50 µs a wait (prctl(2), `PR_SET_TIMERSLACK`), adds under a tenth of [`ROUNDS`]'s ±5% to
/// what it counts as paused, `50 µs / (5% / 10)`.
const TICK: Duration = Duration::from_millis(10);

/// How long the harness's process went without running past the waits it asked: a thread that
/// waits for the run's end a tick at a time and counts by how much each wait overran. A process
/// stopped whole (its cgroup frozen, SIGSTOP, or a machine's pacer duty-cycling the work it runs
/// while its disks fall behind) stops the cell this program runs and its clients alike, and every
/// operation out meanwhile counts the stop in its latency. A phase says how long its process was
/// stopped, so a stop is not read as the cell's.
#[derive(Default)]
struct Watch {
    paused_ns: AtomicU64,
    longest_ns: AtomicU64,
}

impl Watch {
    /// Waits a tick at a time for `ended` to be sent or dropped, counting each wait's overrun.
    fn run(&self, ended: &Receiver<()>) {
        loop {
            let began = Instant::now();
            if !matches!(ended.recv_timeout(TICK), Err(RecvTimeoutError::Timeout)) {
                return;
            }
            let over = began.elapsed().saturating_sub(TICK);
            let over = u64::try_from(over.as_nanos()).unwrap_or(u64::MAX);
            self.paused_ns.fetch_add(over, Ordering::Relaxed);
            self.longest_ns.fetch_max(over, Ordering::Relaxed);
        }
    }

    /// The time paused since the last take, and the longest pause in it, in nanoseconds.
    fn take(&self) -> (u64, u64) {
        (
            self.paused_ns.swap(0, Ordering::Relaxed),
            self.longest_ns.swap(0, Ordering::Relaxed),
        )
    }
}

/// A ListObjectsV2 page's keys at most: S3's 1,000 (`mantle_s3::body::MAX_OBJECTS`).
const PAGE: u32 = 1000;

/// Connections the S3 listener holds for the slope's two points (measurement.md §10): two and eight
/// on each of the three nodes, the lower past the first, so its fixed cost is not in the slope, and
/// the higher four times it, within an S3 listener's budget of ten (research/72).
const SLOPE_LOW: usize = 6;
const SLOPE_HIGH: usize = 24;

/// An `aws-chunked` body's chunk: botocore's 1 MiB, as `mantle bench s3` sends it.
const CHUNK: u64 = 1 << 20;

/// A response body kept to be read (an XML document): at most this, past a ListObjectsV2 page of
/// 1,000 keys of S3's 1,024 octets each.
const KEEP: usize = 2 << 20;

/// A control's object: a byte past a 64 KiB segment, so its bytes cross a segment's boundary.
const CONTROL: u64 = 65_537;

/// The key no phase puts, which the missing-object control reads.
const NEVER_PUT: &str = "control-never-put";

/// The 95% coverage a quantile is resolved at (measurement.md §5a): 6 samples for a median, 368
/// for a p99, 3,688 for a p99.9.
fn coverage() -> Option<Level> {
    Level::new(19, 20).ok()
}

#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn failed(e: impl std::fmt::Display) -> Error {
    Error(e.to_string())
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        failed(e)
    }
}

/// How requests arrive.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Load {
    /// A Poisson process of this many requests a second in all, latency from each one's intended
    /// start.
    Open(f64),
    /// A closed ladder: clients doubling from one while throughput grows.
    Closed,
}

/// `--load`: `open:RATE` or `closed`.
pub fn load_arg(text: &str) -> Result<Load, String> {
    if text == "closed" {
        return Ok(Load::Closed);
    }
    let rate = text
        .strip_prefix("open:")
        .and_then(|r| r.parse::<f64>().ok())
        .filter(|r| r.is_finite() && *r > 0.0)
        .ok_or_else(|| format!("{text} is not open:RATE or closed"))?;
    Ok(Load::Open(rate))
}

/// `--sizes`: a byte count as `init::parse_bytes` reads one, or 0.
pub fn size_arg(text: &str) -> Result<u64, String> {
    if text.trim() == "0" {
        return Ok(0);
    }
    crate::init::parse_bytes(text)
}

pub struct Options {
    pub dir: PathBuf,
    pub seed: u64,
    pub sizes: Vec<u64>,
    pub objects: Vec<u64>,
    pub load: Load,
    pub clients: usize,
    pub ops: u64,
    pub bytes: u64,
    pub volume: u64,
    pub condition: String,
    /// An S3 server outside this program the HTTP door's phases run against, instead of a cell.
    pub endpoint: Option<Endpoint>,
    /// Count the servers' instructions and cycles a phase with `perf stat` ([`Perf`]).
    pub perf: bool,
    /// The operations whose phases run; every one when empty. A phase reads the objects the
    /// phases before it made, so a read or a delete is asked beside the put it reads.
    pub kinds: Vec<Kind>,
    /// The native door's clients reach the nodes through the relay condition's path
    /// (`crate::relay`), and only the native door runs.
    pub relay: bool,
    /// The shards each server of the cell places its ranges over (`range_shards`, node.md §2.9).
    pub range_shards: RangeShards,
}

/// An S3 server this program did not start (`--endpoint`): the HTTP door's phases, controls and
/// slope run against it as against a cell's listeners, and its processes' faults, CPU and footprint
/// are read by their pids. Its lines name the door `http@ARM`.
pub struct Endpoint {
    pub arm: String,
    pub at: SocketAddr,
    pub bucket: String,
    pub id: String,
    pub secret: String,
    pub region: String,
    pub pids: Vec<u32>,
}

/// `--endpoint`: `http://HOST:PORT`, the host a name or an address.
pub fn endpoint_arg(text: &str) -> Result<SocketAddr, String> {
    use std::net::ToSocketAddrs;
    let authority = text
        .strip_prefix("http://")
        .ok_or_else(|| format!("{text} is not http://HOST:PORT"))?
        .trim_end_matches('/');
    authority
        .to_socket_addrs()
        .map_err(|e| format!("{authority}: {e}"))?
        .next()
        .ok_or_else(|| format!("{authority} names no address"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Door {
    Native,
    Http,
}

const DOORS: [Door; 2] = [Door::Native, Door::Http];

impl Door {
    fn name(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Http => "http",
        }
    }

    fn at(self) -> usize {
        match self {
            Self::Native => 0,
            Self::Http => 1,
        }
    }
}

/// An operation a phase asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Put,
    Stream,
    Get,
    Range,
    Head,
    Delete,
    List,
    Create,
    Part,
    Parts,
    Complete,
    Abort,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Put => "PUT",
            Self::Stream => "PUT-STREAM",
            Self::Get => "GET",
            Self::Range => "GET-RANGE",
            Self::Head => "HEAD",
            Self::Delete => "DELETE",
            Self::List => "LIST",
            Self::Create => "MPU-CREATE",
            Self::Part => "MPU-PART",
            Self::Parts => "MPU-LIST-PARTS",
            Self::Complete => "MPU-COMPLETE",
            Self::Abort => "MPU-ABORT",
        }
    }

    const ALL: [Self; 12] = [
        Self::Put,
        Self::Stream,
        Self::Get,
        Self::Range,
        Self::Head,
        Self::Delete,
        Self::List,
        Self::Create,
        Self::Part,
        Self::Parts,
        Self::Complete,
        Self::Abort,
    ];
}

/// `--kinds`: an operation by the name its lines give it (`PUT`, `GET-RANGE`, `MPU-PART`, ...).
pub fn kind_arg(text: &str) -> Result<Kind, String> {
    Kind::ALL
        .into_iter()
        .find(|k| k.name() == text)
        .ok_or_else(|| format!("{text} is no operation's name"))
}

/// What an operation came to.
enum Came {
    Done,
    Failed(String),
    /// A read whose bytes or length were not what was put: a failure, counted apart as well.
    Differed(String),
    /// The server answered with a refusal (an HTTP status past 2xx), or an operation needed what a
    /// refused one would have made: a failure, which against an endpoint may exclude its phase.
    Refused(String),
    /// Refused `SlowDown` (503): asked again as the AWS SDKs ask a throttled request again.
    Slowed,
    /// Its exchange ended unanswered (`ClientError::Lost`, said): asked again with its identity,
    /// which the node answers from what it made.
    Lost(String),
    /// Throttled or lost on every attempt it was given: the server's failure to answer, counted
    /// apart from the checks' failures.
    Unanswered(String),
    /// Not asked: what it reads or completes was never acknowledged, its put, upload or part
    /// having failed before it. Counted, and no latency is recorded for it.
    Skipped,
}

/// A native reply, or what asking it came to where the server or the wire ended it: one
/// operation's outcome, counted, the run going on. Only the harness's own failures, a client's
/// bounds or runtime, its source or sink, a body out of its stated order, end the run.
fn answered(
    asked: Result<Reply, mantle_client::ClientError>,
) -> Result<Result<Reply, Came>, Error> {
    use mantle_client::ClientError;
    use mantle_client::Refusal;
    match asked {
        Ok(reply) if reply.status == Status::SlowDown => Ok(Err(Came::Slowed)),
        Ok(reply) => Ok(Ok(reply)),
        // Its exchange ended unanswered, or its connection ended under it before it was answered:
        // asked again with its identity (`Work::again_after`), as the node answers a request
        // again from what it made.
        Err(e @ (ClientError::Lost { .. } | ClientError::Transport(_)))
            if !matches!(e, ClientError::Transport(Refusal::Order)) =>
        {
            Ok(Err(Came::Lost(e.to_string())))
        }
        // The node refused the client, for a reason asking again does not change.
        Err(e @ ClientError::Refused(_)) => Ok(Err(Came::Refused(e.to_string()))),
        // Refused before a node took it on each of the client's own attempts.
        Err(e @ ClientError::Untaken(_)) => Ok(Err(Came::Unanswered(e.to_string()))),
        // A reply whose bytes are not those its checksum names: a read not what was put.
        Err(e @ ClientError::Corrupt(_)) => Ok(Err(Came::Differed(e.to_string()))),
        // A reply that does not hold, or an upload's offset none of its runs reach: a check.
        Err(e @ (ClientError::Reply(_) | ClientError::Offset { .. })) => {
            Ok(Err(Came::Failed(e.to_string())))
        }
        Err(e) => Err(failed(e)),
    }
}

/// The first pause before a slowed request is asked again, doubling each time up to the most:
/// botocore's standard retry mode, `rand(0, 1) * min(2 ** attempt, 20)` seconds, as mantle-client
/// waits on its own requests (research/77 [5]).
const SLOWED_BASE_NS: u64 = 1_000_000_000;
const SLOWED_MOST_NS: u64 = 20_000_000_000;

/// An object's key and its bytes' name: the same, so a read of one key answered with another's
/// bytes fails its check.
fn sized_key(door: Door, size: u64, kind: char, i: u64) -> Result<String, Error> {
    let mut key = String::new();
    write_sized(&mut key, door, (size, kind, i))?;
    Ok(key)
}

/// [`sized_key`] written into `into`.
fn write_sized(
    into: &mut String,
    door: Door,
    (size, kind, i): (u64, char, u64),
) -> Result<(), Error> {
    write_key(into, format_args!("{}/{size}/{kind}{i}", door.name()))
}

/// Writes a key over what `into` held: a logical client's key buffer, which holds any S3 key
/// ([`Logical`]), so writing one allocates nothing. A key longer than S3's is refused.
fn write_key(into: &mut String, key: std::fmt::Arguments<'_>) -> Result<(), Error> {
    use std::fmt::Write as _;
    into.clear();
    into.write_fmt(key).map_err(failed)?;
    if into.len() > mantle_s3::route::MAX_KEY {
        return Err(failed(format!(
            "a key past S3's {} octets",
            mantle_s3::route::MAX_KEY
        )));
    }
    Ok(())
}

/// Which keys a phase's operation `j` asks, and of what length.
#[derive(Clone)]
enum Keys {
    /// A size's objects: kind `p` put whole, `s` streamed, `m` multipart; read modulo `made`
    /// where `made` is set.
    Sized {
        size: u64,
        kind: char,
        made: Option<u64>,
    },
    /// A set of `count` small objects, those of `door` (`transfer::door`): its own, or the
    /// other's, read only where acknowledged.
    Set { count: u64, own: Door },
}

/// A phase's shared facts, which its tasks read.
struct Work {
    door: Door,
    kind: Kind,
    keys: Keys,
    generator: Rc<Generator>,
    /// The MD5 of each of the phase's first [`ETAGS`] objects' bytes.
    etags: Rc<Vec<[u8; 16]>>,
    /// A size's multipart uploads: each one's ID and its one part's ETag, by index.
    uploads: Rc<RefCell<Vec<Upload>>>,
    oracle: Rc<RefCell<Oracle>>,
    listing: Rc<RefCell<Paging>>,
    /// The times the phase's operations were asked again.
    again: std::cell::Cell<u64>,
}

/// A listing's place: where its next page starts, whether it ended, the keys it listed, of them
/// those acknowledged, and the set's index of the last, which the next key must sort after.
#[derive(Default)]
struct Paging {
    next: Option<Next>,
    ended: bool,
    listed: u64,
    acknowledged: u64,
    last: Option<u64>,
}

/// What a listing page's keys came to: how many it listed, of them those acknowledged, the set's
/// index of the last, and the first key that was no key of the set or did not sort after the one
/// before it.
struct Page {
    keys: u64,
    acknowledged: u64,
    last: Option<u64>,
    wrong: Option<String>,
}

/// Where the next page of a listing starts: the native door's key, the S3 listener's token.
enum Next {
    Native(Resume),
    Token(String),
}

/// What a size's phases share: the ETags their objects' bytes give, and their uploads.
type Common<'a> = (
    &'a Rc<Vec<[u8; 16]>>,
    &'a Rc<RefCell<Vec<Upload>>>,
    &'a Rc<RefCell<Oracle>>,
);

#[derive(Clone, Default)]
struct Upload {
    id: String,
    part: Option<String>,
}

/// A logical client's connection.
enum Conn {
    Native(Box<Client>),
    Http(Box<Http>),
}

impl Conn {
    /// What the connection's sockets moved since it opened: the native client's datagrams and
    /// their payload, the HTTP connection's octets written and read (TCP has no datagrams of its
    /// own to say).
    fn wire(&self) -> Wire {
        match self {
            Self::Native(client) => client.wire(),
            Self::Http(http) => Wire {
                bytes: http.wire,
                datagrams: 0,
            },
        }
    }
}

/// A logical client: its connection, and the harness's own buffers its operations work in, made
/// with the connection and kept with it, so a phase's operations allocate nothing of the
/// harness's: the key of the operation at hand, written in place (room for any S3 key,
/// `mantle_s3::route::MAX_KEY`), and the scratch a read's bytes are made again in to be checked
/// (mantle-client's piece, the most a read hands over at once).
struct Logical {
    conn: Conn,
    key: String,
    scratch: Vec<u8>,
}

impl Logical {
    /// The client of `conn`, its buffers set aside as the harness's own.
    fn new(conn: Conn) -> Self {
        harness(|| Self {
            conn,
            key: String::with_capacity(mantle_s3::route::MAX_KEY),
            scratch: vec![0u8; mantle_client::PIECE],
        })
    }
}

/// What `held`'s connections moved since each opened, summed.
fn wired(held: &[Logical]) -> Wire {
    held.iter().fold(Wire::default(), |sum, logical| {
        let wire = logical.conn.wire();
        Wire {
            bytes: sum.bytes.saturating_add(wire.bytes),
            datagrams: sum.datagrams.saturating_add(wire.datagrams),
        }
    })
}

/// The connections `held`'s HTTP clients opened: each one's first, and each opened again after
/// its listener let one go.
fn reopened(held: &[Logical]) -> u64 {
    held.iter().fold(0u64, |sum, logical| match &logical.conn {
        Conn::Http(http) => sum.saturating_add(http.opened),
        Conn::Native(_) => sum,
    })
}

/// The harness's own work: counted apart from the doors' (`alloc::aside`). What it runs never
/// waits, so no other task's allocations fall within it.
fn harness<T>(work: impl FnOnce() -> T) -> T {
    alloc::aside();
    let done = work();
    alloc::back();
    done
}

/// A source or sink of the harness's whose work is the harness's own: [`Made`]'s bytes made and
/// [`Checked`]'s checked are set aside. Neither ever waits, so each call completes in the one poll
/// the aside spans.
struct Aside<T>(T);

impl<S: Source> Source for Aside<S> {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        alloc::aside();
        let read = self.0.read(into).await;
        alloc::back();
        read
    }
}

impl<S: Rewind> Rewind for Aside<S> {
    async fn rewind(&mut self, offset: u64) -> std::io::Result<()> {
        self.0.rewind(offset).await
    }
}

impl<K: Sink> Sink for Aside<K> {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        alloc::aside();
        let wrote = self.0.write(bytes).await;
        alloc::back();
        wrote
    }
}

/// A body the HTTP door sends.
enum Body<'a> {
    None,
    /// An object's bytes from `from` to `to`, made as they are sent, framed by length.
    Made(&'a Object<'a>, u64, u64),
    /// An object's bytes, `aws-chunked` with a trailing CRC-32.
    Chunked(&'a Object<'a>, u64),
    Text(&'a [u8]),
}

/// Why an exchange on a connection did not end in an answer: `Again` where the connection ended
/// before any of the answer came, which a connection opened anew may answer.
enum Attempt {
    Again(Error),
    Failed(Error),
}

/// A response's head, as the HTTP door reads it.
struct Answer {
    status: u16,
    etag: Option<String>,
    length: Option<u64>,
}

/// `io`, a read or a write of an HTTP connection, waited on as the AWS SDKs wait on one: at most
/// the stock S3 clients' patience, 60 s between bytes, botocore's `read_timeout` and the AWS CLI's
/// `--cli-read-timeout` by default (`bounds::STOCK_PATIENCE`, research/77 [9, 10]), past which a
/// listener that said nothing is taken for gone, a typed failure. An exchange whose answer had not
/// begun is asked once more on a connection opened anew (`Http::ask`), as one the listener let go.
async fn patient<T>(io: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
    let patience =
        u64::try_from(mantle_protocol::bounds::STOCK_PATIENCE.as_nanos()).map_err(failed)?;
    match race2(std::pin::pin!(io), std::pin::pin!(sleep(patience))).await {
        Either::First(done) => done,
        Either::Second(slept) => {
            slept.map_err(|e| failed(format!("{e:?}")))?;
            Err(failed(
                "the listener said nothing for the stock S3 clients' patience",
            ))
        }
    }
}

/// A connection to an S3 listener, a request at a time.
struct Http {
    at: SocketAddr,
    /// The listener as each request's `host` names it.
    host: String,
    /// A request's head, written into this before it is sent, the next written over it.
    head: Vec<u8>,
    stream: Option<TcpStream>,
    /// What was read and not yet taken.
    read: Vec<u8>,
    held: usize,
    piece: Vec<u8>,
    /// Octets written and read on it.
    wire: u64,
    /// Connections opened: the first and each one opened again after the listener let one go.
    opened: u64,
    signer: Rc<Signer>,
}

/// The S3 key and region every request is signed with, and the bucket each names.
struct Signer {
    id: String,
    secret: String,
    region: String,
    bucket: String,
}

impl Signer {
    /// The key `mantle init` wrote, as AWS's shared credentials file states it.
    fn read(cell: &Path, region: String) -> Result<Self, Error> {
        let text = std::fs::read_to_string(cell.join("s3").join("credentials"))?;
        let value = |name: &str| {
            text.lines().find_map(|line| {
                let (n, v) = line.split_once('=')?;
                (n.trim() == name).then(|| v.trim().to_owned())
            })
        };
        Ok(Self {
            id: value("aws_access_key_id").ok_or_else(|| failed("no access key id"))?,
            secret: value("aws_secret_access_key").ok_or_else(|| failed("no secret key"))?,
            region,
            bucket: BUCKET.to_owned(),
        })
    }

    /// `request`'s head for the listener named `host`, signed now (05 §1.2), written by mantle's
    /// codec over what `out` held: every header but a range signed, the length the codec's to
    /// write (`mantle_http::write::request`).
    fn head(&self, out: &mut Vec<u8>, host: &str, request: &Request<'_>) -> Result<(), Error> {
        let (stamp, date) = crate::bench_s3::timestamp().map_err(failed)?;
        let mut all: Vec<(&str, &str)> =
            Vec::with_capacity(request.headers.len().saturating_add(3));
        all.extend_from_slice(&[
            ("host", host),
            ("x-amz-date", &stamp),
            ("x-amz-content-sha256", request.payload),
        ]);
        all.extend_from_slice(request.headers);
        let mut signed: Vec<&str> = all
            .iter()
            .map(|(n, _)| *n)
            .filter(|n| *n != "range")
            .collect();
        signed.sort_unstable();
        let canonical = sigv4::canonical_request(
            &sigv4::Request {
                method: request.method,
                path: request.path,
                query: request.query,
                headers: &all,
            },
            &signed,
            request.payload,
            None,
        )
        .map_err(|e| failed(format!("{e:?}")))?;
        let signature = sigv4::sign(&self.secret, &stamp, &date, &self.region, &canonical)
            .map_err(|e| failed(format!("{e:?}")))?;
        let scope = sigv4::scope(&date, &self.region, "s3");
        let names = signed.join(";");
        let hex = sigv4::hex(&signature);
        let authorization: [&[u8]; 9] = [
            sigv4::ALGORITHM.as_bytes(),
            b" Credential=",
            self.id.as_bytes(),
            b"/",
            scope.as_bytes(),
            b", SignedHeaders=",
            names.as_bytes(),
            b", Signature=",
            hex.as_bytes(),
        ];
        let values: Vec<[&[u8]; 1]> = all.iter().map(|(_, v)| [v.as_bytes()]).collect();
        let mut fields: Vec<(&str, &[&[u8]])> = all
            .iter()
            .zip(&values)
            .map(|((name, _), value)| (*name, &value[..]))
            .collect();
        fields.push(("authorization", authorization.as_slice()));
        let parts: [&[u8]; 3] = [request.path.as_bytes(), b"?", request.query.as_bytes()];
        let target = if request.query.is_empty() {
            parts.get(..1).unwrap_or_default()
        } else {
            parts.as_slice()
        };
        out.clear();
        mantle_http::write::request(out, request.method, target, &fields, request.length)
            .map_err(failed)
    }
}

/// A request to sign and send: its method, its path and query, the headers it adds to those every
/// request signs, its payload as `x-amz-content-sha256` names it, and its body's length.
struct Request<'r> {
    method: &'r str,
    path: &'r str,
    query: &'r str,
    headers: &'r [(&'r str, &'r str)],
    payload: &'r str,
    length: Length,
}

impl<'r> Request<'r> {
    /// A request with no body and no header of its own.
    fn bare(method: &'r str, path: &'r str, query: &'r str) -> Self {
        Self {
            method,
            path,
            query,
            headers: &[],
            payload: sigv4::EMPTY_SHA256,
            length: Length::None,
        }
    }
}

/// An `aws-chunked` body's octets on the wire for `length` bytes of object: each chunk's head, its
/// data and its end, the last chunk and the trailer, written by mantle's codec into `scratch` to be
/// counted (a CRC-32's base64 is always eight octets).
fn chunked_length(length: u64, scratch: &mut Vec<u8>) -> Result<u64, Error> {
    let mut total = 0u64;
    let mut left = length;
    while left > 0 {
        let n = left.min(CHUNK);
        scratch.clear();
        mantle_http::write::chunk(scratch, usize::try_from(n).map_err(failed)?);
        let framing = u64::try_from(
            scratch
                .len()
                .saturating_add(mantle_http::write::CHUNK_END.len()),
        )
        .map_err(failed)?;
        total = total
            .checked_add(n)
            .and_then(|t| t.checked_add(framing))
            .ok_or_else(|| failed("a body past u64"))?;
        left = left.saturating_sub(n);
    }
    scratch.clear();
    mantle_http::write::last(scratch, &[(Algorithm::Crc32.header(), b"AAAAAAAA")])
        .map_err(failed)?;
    total
        .checked_add(u64::try_from(scratch.len()).map_err(failed)?)
        .ok_or_else(|| failed("a body past u64"))
}

impl Http {
    fn new(at: SocketAddr, signer: Rc<Signer>) -> Self {
        Self {
            at,
            host: at.to_string(),
            head: Vec::new(),
            stream: None,
            read: vec![0u8; mantle_client::PIECE],
            held: 0,
            piece: Vec::with_capacity(mantle_client::PIECE),
            wire: 0,
            opened: 0,
            signer,
        }
    }

    async fn stream(&mut self) -> Result<&TcpStream, Error> {
        if self.stream.is_none() {
            self.stream = Some(
                TcpStream::connect(self.at)
                    .await
                    .map_err(|e| failed(format!("connecting: {e:?}")))?,
            );
            self.opened = self.opened.saturating_add(1);
            self.held = 0;
        }
        self.stream.as_ref().ok_or_else(|| failed("no connection"))
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let stream = self.stream().await?;
        patient(async {
            stream
                .write_all(bytes)
                .await
                .map_err(|e| failed(format!("writing: {e:?}")))
        })
        .await?;
        self.wire = self
            .wire
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    /// Reads more octets after what is held; zero at the connection's end.
    async fn fill(&mut self) -> Result<usize, Error> {
        if self.held == self.read.len() {
            return Err(failed("a response head past the read buffer"));
        }
        let stream = self
            .stream
            .as_ref()
            .ok_or_else(|| failed("no connection"))?;
        let into = self
            .read
            .get_mut(self.held..)
            .ok_or_else(|| failed("the read buffer"))?;
        let n = patient(async {
            stream
                .read(into)
                .await
                .map_err(|e| failed(format!("reading: {e:?}")))
        })
        .await?;
        self.held = self.held.saturating_add(n);
        self.wire = self
            .wire
            .saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        Ok(n)
    }

    /// Drops the first `n` octets held.
    fn take(&mut self, n: usize) {
        let n = n.min(self.held);
        self.read.copy_within(n..self.held, 0);
        self.held = self.held.saturating_sub(n);
    }

    /// Signs `request` and sends it, and reads its response, its body into `sink`: once again on a
    /// connection opened anew where the listener had let the last one go before answering (a quiet
    /// connection's end, node.md §4.1), which its octets so far tell. Its head is written into the
    /// connection's own buffer, which the next request's is written over.
    async fn ask(
        &mut self,
        request: &Request<'_>,
        body: &Body<'_>,
        bodiless: bool,
        sink: &mut impl Sink,
    ) -> Result<Answer, Error> {
        let mut head = std::mem::take(&mut self.head);
        if let Err(e) = self.signer.head(&mut head, &self.host, request) {
            self.head = head;
            return Err(e);
        }
        let asked = match self.ask_once(&head, body, bodiless, sink).await {
            Ok(answer) => Ok(answer),
            Err(Attempt::Failed(e)) => Err(e),
            Err(Attempt::Again(_)) => {
                self.stream = None;
                self.ask_once(&head, body, bodiless, sink)
                    .await
                    .map_err(|(Attempt::Again(e) | Attempt::Failed(e))| e)
            }
        };
        self.head = head;
        asked
    }

    /// One exchange on the connection held, or on one opened for it.
    async fn ask_once(
        &mut self,
        head: &[u8],
        body: &Body<'_>,
        bodiless: bool,
        sink: &mut impl Sink,
    ) -> Result<Answer, Attempt> {
        let sent = async {
            self.write(head).await?;
            self.send(body).await
        };
        sent.await.map_err(Attempt::Again)?;
        let end = loop {
            if let Some(end) = self
                .read
                .get(..self.held)
                .and_then(|r| r.windows(4).position(|w| w == b"\r\n\r\n"))
            {
                break end.saturating_add(4);
            }
            match self.fill().await {
                Ok(0) | Err(_) if self.held == 0 => {
                    return Err(Attempt::Again(failed(
                        "the listener closed before answering",
                    )));
                }
                Ok(0) => {
                    return Err(Attempt::Failed(failed(
                        "the listener closed within an answer's head",
                    )));
                }
                Ok(_) => {}
                Err(e) => return Err(Attempt::Failed(e)),
            }
        };
        let text = std::str::from_utf8(self.read.get(..end).unwrap_or_default())
            .map_err(|_| Attempt::Failed(failed("a response head that is not UTF-8")))?;
        let status: u16 = text
            .split(' ')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| Attempt::Failed(failed(format!("a status line: {text}"))))?;
        let field = |name: &str| {
            text.lines().find_map(|l| {
                let (n, v) = l.split_once(':')?;
                n.eq_ignore_ascii_case(name).then(|| v.trim())
            })
        };
        let length = field("content-length").and_then(|v| v.parse::<u64>().ok());
        let chunked = field("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
        let etag = field("etag").map(|e| e.trim_matches('"').to_owned());
        self.take(end);
        let framing = match (bodiless || status == 204 || status == 304, chunked, length) {
            (true, ..) => mantle_http::Framing::None,
            (false, true, _) => mantle_http::Framing::Chunked,
            (false, false, Some(n)) => mantle_http::Framing::Length(n),
            (false, false, None) => mantle_http::Framing::None,
        };
        let mut decoder = mantle_http::Body::new(framing);
        let short = || Attempt::Failed(failed("the listener ended a body short"));
        while !decoder.done() {
            if self.held == 0 && self.fill().await.map_err(Attempt::Failed)? == 0 {
                return Err(short());
            }
            let input = self.read.get(..self.held).unwrap_or_default();
            let step = decoder
                .decode(input)
                .map_err(|e| Attempt::Failed(failed(e)))?;
            if let Some(data) = input.get(step.data.clone())
                && !data.is_empty()
            {
                sink.write(data)
                    .await
                    .map_err(|e| Attempt::Failed(failed(e)))?;
            }
            self.take(step.consumed);
            if step.consumed == 0 && !step.end && self.fill().await.map_err(Attempt::Failed)? == 0 {
                return Err(short());
            }
        }
        Ok(Answer {
            status,
            etag,
            length,
        })
    }

    /// Sends `body`, its bytes made or framed in the connection's own piece, which goes back to the
    /// connection whatever the send came to: a write the listener refused mid-body would otherwise
    /// drop it, and the next request make another, the harness's own allocation each time.
    async fn send(&mut self, body: &Body<'_>) -> Result<(), Error> {
        let mut piece = std::mem::take(&mut self.piece);
        let sent = self.send_in(body, &mut piece).await;
        self.piece = piece;
        sent
    }

    async fn send_in(&mut self, body: &Body<'_>, piece: &mut Vec<u8>) -> Result<(), Error> {
        match body {
            Body::None => Ok(()),
            Body::Text(text) => self.write(text).await,
            Body::Made(object, from, to) => {
                let mut at = *from;
                while at < *to {
                    let n = usize::try_from(to.saturating_sub(at))
                        .unwrap_or(usize::MAX)
                        .min(mantle_client::PIECE);
                    harness(|| {
                        piece.resize(n, 0);
                        object.fill(at, piece)
                    })
                    .map_err(failed)?;
                    self.write(piece).await?;
                    at = at.saturating_add(u64::try_from(n).map_err(failed)?);
                }
                Ok(())
            }
            Body::Chunked(object, length) => {
                let mut crc = Hasher::new(Algorithm::Crc32).map_err(failed)?;
                let mut at = 0u64;
                while at < *length {
                    let n =
                        usize::try_from(length.saturating_sub(at).min(CHUNK)).map_err(failed)?;
                    piece.clear();
                    mantle_http::write::chunk(piece, n);
                    let start = piece.len();
                    harness(|| {
                        piece.resize(start.saturating_add(n), 0);
                        object.fill(at, piece.get_mut(start..).unwrap_or_default())
                    })
                    .map_err(failed)?;
                    crc.update(piece.get(start..).unwrap_or_default())
                        .map_err(failed)?;
                    piece.extend_from_slice(mantle_http::write::CHUNK_END);
                    self.write(piece).await?;
                    at = at.saturating_add(u64::try_from(n).map_err(failed)?);
                }
                let value = crc.finish().map_err(failed)?.to_base64();
                piece.clear();
                mantle_http::write::last(piece, &[(Algorithm::Crc32.header(), value.as_bytes())])
                    .map_err(failed)?;
                self.write(piece).await
            }
        }
    }
}

/// A sink that keeps up to [`KEEP`] octets: an XML answer's.
struct Kept(Vec<u8>);

impl Sink for Kept {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if self.0.len().saturating_add(bytes.len()) > KEEP {
            return Err(std::io::Error::other("an answer past what is kept"));
        }
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

/// The text of the first element `name` in `xml`.
fn element<'x>(xml: &'x str, name: &str) -> Option<&'x str> {
    let open = format!("<{name}>");
    let start = xml.find(&open)?.checked_add(open.len())?;
    let rest = xml.get(start..)?;
    rest.get(..rest.find(&format!("</{name}>"))?)
}

/// A phase's tally, shared by its tasks.
struct Tally {
    latency: Histogram,
    lateness: Histogram,
    ops: u64,
    failed: u64,
    /// Of the failed, those the server refused ([`Came::Refused`]).
    refused: u64,
    /// Of the failed, the reads whose bytes or length were not what was put ([`Came::Differed`]).
    differed: u64,
    /// Of the failed, those never answered, throttled or lost on every attempt
    /// ([`Came::Unanswered`]).
    unanswered: u64,
    /// Of the operations, those not asked for want of what a failed one before them would have
    /// made ([`Came::Skipped`]).
    skipped: u64,
    /// The most requests out at once.
    most: usize,
    /// The most threads the process ran as the OS counted them, read each time more requests
    /// were out at once than before.
    threads: usize,
    /// The first failure of each kind's reason: a check's, a refusal's and an unanswered
    /// operation's, each moved in as it came, so keeping them allocates nothing of the harness's.
    first: [Option<String>; 3],
}

/// A failure's kind, its place in [`Tally::first`] and the word its line is tagged with.
#[derive(Clone, Copy)]
enum Failure {
    Check,
    Refusal,
    Unanswered,
}

impl Failure {
    const ALL: [Self; 3] = [Self::Check, Self::Refusal, Self::Unanswered];

    fn at(self) -> usize {
        match self {
            Self::Check => 0,
            Self::Refusal => 1,
            Self::Unanswered => 2,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Check => "failure",
            Self::Refusal => "refusal",
            Self::Unanswered => "unanswered",
        }
    }
}

impl Tally {
    /// Keeps `why` as its kind's first failure, where there is none yet.
    fn keep_first(&mut self, kind: Failure, why: String) {
        if let Some(slot) = self.first.get_mut(kind.at()) {
            slot.get_or_insert(why);
        }
    }
}

/// When a phase's operations are to start: each one's index, and its intended start on the
/// runtime's clock, a Poisson process's arrivals open loop, the moment a client takes it closed.
struct Schedule {
    arrivals: Option<Arrivals>,
    next: u64,
    end: u64,
}

impl Schedule {
    fn new(load: Load, seed: u64, (from, end): (u64, u64), start_ns: u64) -> Result<Self, Error> {
        let arrivals = match load {
            Load::Open(rate) => Some(Arrivals::poisson(rate, seed, start_ns).map_err(failed)?),
            Load::Closed => None,
        };
        Ok(Self {
            arrivals,
            next: from,
            end,
        })
    }

    /// Asks no more operations past those taken.
    fn stop(&mut self) {
        self.end = self.next;
    }

    fn take(&mut self, now: u64) -> Result<Option<(u64, u64)>, Error> {
        if self.next >= self.end {
            return Ok(None);
        }
        let i = self.next;
        self.next = self.next.saturating_add(1);
        let at = match self.arrivals.as_mut() {
            Some(arrivals) => arrivals.next_arrival().map_err(failed)?,
            None => now,
        };
        Ok(Some((i, at)))
    }
}

/// What a phase's tasks share: its schedule, its tally, the requests out now and, for a closed
/// step, its rounds.
struct Shared {
    schedule: Schedule,
    tally: Tally,
    out: usize,
    error: Option<Error>,
    done: Vec<Logical>,
    rounds: Option<Rounds>,
}

/// A closed step's rounds as its operations complete (measurement.md §5a): each round `size`
/// operations, its throughput their count over the time since the round before it ended. The
/// first, in which the step's clients ramp up from none, is not kept, as a benchmark's first
/// iteration is not (Georges et al., OOPSLA 2007, §4.1; measurement.md §5a). Once [`ROUNDS`] finds
/// the kept rounds enough, the step asks no more.
struct Rounds {
    size: u64,
    counted: u64,
    began_ns: u64,
    warm: bool,
    rates: Vec<f64>,
}

impl Rounds {
    fn new(size: u64, now: u64) -> Self {
        Self {
            size: size.max(1),
            counted: 0,
            began_ns: now,
            warm: false,
            rates: Vec::with_capacity(ROUNDS.limit()),
        }
    }

    /// One more of the step's operations completed at `now`: whether its rounds are enough.
    fn completed(&mut self, now: u64) -> bool {
        self.counted = self.counted.saturating_add(1);
        if self.counted < self.size {
            return false;
        }
        let took = now.saturating_sub(self.began_ns).max(1);
        self.began_ns = now;
        self.counted = 0;
        if !std::mem::replace(&mut self.warm, true) {
            return false;
        }
        if self.rates.len() < ROUNDS.limit() {
            self.rates.push(self.size as f64 * 1e9 / took as f64);
        }
        ROUNDS.enough(&rounds::judge(&self.rates))
    }
}

/// The operations a closed step's round counts at `clients`, measurement.md §5a's round: enough
/// that the p99 the step reports has its 95% interval within ten rounds, and that the operations
/// at a round's edges, which ran with fewer than `clients` in it, cost its throughput at most
/// [`ROUNDS`]'s precision: `max(⌈368 / 10⌉, ⌈clients / (2 · 5%)⌉)`.
fn round_ops(clients: usize) -> Result<u64, Error> {
    let p99 = Level::new(99, 100).map_err(failed)?;
    let quantile = samples_for(p99, coverage().ok_or_else(|| failed("a coverage"))?)
        .map_err(failed)?
        .div_ceil(10);
    let clients = u64::try_from(clients).map_err(failed)?;
    // Rounded up as seconds are, which checks the value finite, not negative and within u64.
    let edges = Duration::try_from_secs_f64((clients as f64 / (2.0 * ROUNDS.precision)).ceil())
        .map_err(failed)?
        .as_secs();
    Ok(quantile.max(edges))
}

/// The operations a closed step at `clients` asks at most: its ramp's round and [`ROUNDS`]'s
/// limit of rounds after it, and those its clients still have out when the rounds are enough.
fn step_ops(clients: usize) -> Result<u64, Error> {
    let rounds = u64::try_from(ROUNDS.limit())
        .map_err(failed)?
        .saturating_add(1);
    Ok(round_ops(clients)?
        .saturating_mul(rounds)
        .saturating_add(u64::try_from(clients).map_err(failed)?))
}

/// A closed step's throughput as its rounds judged it: the median of its state of most rounds,
/// and the bounds its growth is told by, its median's 95% interval, or the least and most its
/// rounds measured where they carry none (rounds ordered in time, or too few).
#[derive(Debug, Clone, Copy, PartialEq)]
struct Stepped {
    clients: usize,
    median: f64,
    low: f64,
    high: f64,
}

impl Stepped {
    /// The step its rounds judged, none where it kept none.
    fn of(clients: usize, judged: &Judgment) -> Option<Self> {
        let state = judged
            .states
            .iter()
            .filter(|s| !s.rounds.is_empty())
            .max_by_key(|s| s.rounds.len())?;
        let (low, high) = state.interval.unwrap_or((state.min, state.max));
        Some(Self {
            clients,
            median: state.median,
            low,
            high,
        })
    }
}

/// Where a closed ladder stands after a step: the best step so far, of the greatest median; the
/// steps in a row since one grew, its interval wholly above the best's; and whether it ends.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Climb {
    best: Option<Stepped>,
    flat: u32,
}

impl Climb {
    /// The ladder after `step`, of whose operations `unanswered` went unanswered: whether it goes
    /// on. A step that left one unanswered ends the ladder and is not its best: the cell did not
    /// answer its load within its own patience, so the step is past the knee the ladder looks for
    /// (measurement.md §12), and doubling its clients asks the cell to refuse more.
    fn after(&mut self, step: Stepped, unanswered: u64) -> bool {
        if unanswered > 0 {
            return false;
        }
        let grew = self.best.is_none_or(|best| step.low > best.high);
        if self.best.is_none_or(|best| step.median > best.median) {
            self.best = Some(step);
        }
        self.flat = if grew { 0 } else { self.flat.saturating_add(1) };
        self.flat < FLAT
    }
}

/// The threads this process runs as the OS counts them, read with this thread's counting paused:
/// the read is the harness's own, and on Linux it reads a file into a buffer of its own.
fn threads_now() -> usize {
    alloc::pause();
    let threads = drive::thread_count();
    alloc::resume();
    threads
}

/// What a phase's clients ask: operation `j` on a client's connection, and what it came to.
trait Ask {
    async fn ask(&self, logical: &mut Logical, j: u64) -> Result<Came, Error>;
}

impl Ask for Work {
    async fn ask(&self, logical: &mut Logical, j: u64) -> Result<Came, Error> {
        self.run(logical, j).await
    }
}

/// One client's part of a phase: operations taken from the schedule until it ends, each at its
/// intended start, its latency from then.
async fn client<A: Ask>(mut logical: Logical, work: Rc<A>, shared: Rc<RefCell<Shared>>) {
    loop {
        let taken = harness(|| shared.borrow_mut().schedule.take(now_ns()));
        let (j, intended) = match taken {
            Ok(Some(taken)) => taken,
            Ok(None) => break,
            Err(e) => {
                shared.borrow_mut().error = Some(e);
                break;
            }
        };
        let now = now_ns();
        if intended > now && sleep(intended.saturating_sub(now)).await.is_err() {
            shared.borrow_mut().error = Some(failed("a runtime timer"));
            break;
        }
        let started = now_ns();
        let busiest = harness(|| {
            let mut shared = shared.borrow_mut();
            shared.out = shared.out.saturating_add(1);
            let busiest = shared.out > shared.tally.most;
            shared.tally.most = shared.tally.most.max(shared.out);
            busiest
        });
        if busiest {
            let threads = threads_now();
            harness(|| {
                let tally = &mut shared.borrow_mut().tally;
                tally.threads = tally.threads.max(threads);
            });
        }
        let came = work.ask(&mut logical, j).await;
        let took = now_ns().saturating_sub(intended);
        let ended = now_ns();
        let stop = harness(|| {
            let mut shared = shared.borrow_mut();
            let shared = &mut *shared;
            shared.out = shared.out.saturating_sub(1);
            // A closed step's round ends with its operations, an operation not asked counting in
            // none; once the rounds are enough, the step asks no more. Nor once one of its
            // operations went unanswered, throttled or lost on every attempt: the cell no longer
            // answers the load offered within its own patience, so the step past it measures
            // refusals, and each operation more is one more the cell is asked to refuse. A
            // harness's error ends the phase, so it asks no more either. The operations out end on
            // their own bounds.
            let enough = !matches!(came, Ok(Came::Skipped))
                && shared
                    .rounds
                    .as_mut()
                    .is_some_and(|rounds| rounds.completed(ended));
            let unanswered = shared.rounds.is_some()
                && matches!(came, Ok(Came::Unanswered(_) | Came::Lost(_) | Came::Slowed));
            if enough || unanswered || came.is_err() {
                shared.schedule.stop();
            }
            let tally = &mut shared.tally;
            tally.ops = tally.ops.saturating_add(1);
            if !matches!(came, Ok(Came::Skipped)) {
                tally.latency.record(took);
                tally.lateness.record(started.saturating_sub(intended));
            }
            match came {
                Ok(Came::Done) => false,
                Ok(Came::Skipped) => {
                    tally.skipped = tally.skipped.saturating_add(1);
                    false
                }
                Ok(Came::Failed(why)) => {
                    tally.failed = tally.failed.saturating_add(1);
                    tally.keep_first(Failure::Check, why);
                    false
                }
                Ok(Came::Differed(why)) => {
                    tally.failed = tally.failed.saturating_add(1);
                    tally.differed = tally.differed.saturating_add(1);
                    tally.keep_first(Failure::Check, why);
                    false
                }
                Ok(Came::Refused(why)) => {
                    tally.failed = tally.failed.saturating_add(1);
                    tally.refused = tally.refused.saturating_add(1);
                    tally.keep_first(Failure::Refusal, why);
                    false
                }
                Ok(Came::Unanswered(why) | Came::Lost(why)) => {
                    tally.failed = tally.failed.saturating_add(1);
                    tally.unanswered = tally.unanswered.saturating_add(1);
                    tally.keep_first(Failure::Unanswered, why);
                    false
                }
                // Settled into `Unanswered` before it is tallied (`Work::settled`): counted so.
                Ok(Came::Slowed) => {
                    tally.failed = tally.failed.saturating_add(1);
                    tally.unanswered = tally.unanswered.saturating_add(1);
                    false
                }
                Err(e) => {
                    shared.error = Some(e);
                    true
                }
            }
        });
        if stop {
            break;
        }
    }
    harness(|| shared.borrow_mut().done.push(logical));
}

/// A phase's operations `from..end` on `clients`, a task each, a closed step's in rounds of
/// `round` operations until they are enough or one goes unanswered: the clients back, the tally
/// and the rounds' rates.
async fn phase<A: Ask + 'static>(
    clients: Vec<Logical>,
    work: Rc<A>,
    schedule: (Load, u64, u64, u64),
    round: Option<u64>,
) -> Result<(Vec<Logical>, Tally, Vec<f64>), Error> {
    let (load, seed, from, end) = schedule;
    let count = clients.len();
    let shared = harness(|| {
        Ok::<_, Error>(Rc::new(RefCell::new(Shared {
            rounds: round.map(|size| Rounds::new(size, now_ns())),
            schedule: Schedule::new(load, seed, (from, end), now_ns())?,
            tally: Tally {
                latency: Histogram::new(),
                lateness: Histogram::new(),
                ops: 0,
                failed: 0,
                refused: 0,
                differed: 0,
                unanswered: 0,
                skipped: 0,
                most: 0,
                threads: 0,
                first: [None, None, None],
            },
            out: 0,
            error: None,
            done: Vec::with_capacity(count),
        })))
    })?;
    // The clients' tasks are the harness's machinery, set aside as its own work.
    let mut tasks = harness(|| TaskSet::new(count)).map_err(|e| failed(format!("{e:?}")))?;
    for logical in clients {
        harness(|| tasks.spawn(client(logical, Rc::clone(&work), Rc::clone(&shared))))
            .map_err(|e| failed(format!("{e:?}")))?;
    }
    while let Some((_, ended)) = tasks.join_next().await {
        ended.map_err(|e| failed(format!("a client's task: {e:?}")))?;
    }
    let shared = Rc::try_unwrap(shared)
        .map_err(|_| failed("a client outlived its phase"))?
        .into_inner();
    match shared.error {
        Some(e) => Err(e),
        None => Ok((
            shared.done,
            shared.tally,
            shared.rounds.map(|r| r.rates).unwrap_or_default(),
        )),
    }
}

impl Work {
    /// Operation `j`'s key, written into `into`, and its object's index and length; none where what
    /// it reads was never acknowledged, so it is not asked: a size's object whose put failed, a
    /// set's other door's object that was not put.
    fn key(&self, j: u64, into: &mut String) -> Result<Option<(u64, u64)>, Error> {
        harness(|| match &self.keys {
            Keys::Sized { size, kind, made } => {
                let i = match made {
                    Some(made) => j
                        .checked_rem(*made)
                        .ok_or_else(|| failed("no objects to read"))?,
                    None => j,
                };
                let reads = matches!(self.kind, Kind::Get | Kind::Range | Kind::Head);
                if reads && !self.oracle.borrow().has(i) {
                    return Ok(None);
                }
                write_sized(into, self.door, (*size, *kind, i))?;
                Ok(Some((i, *size)))
            }
            Keys::Set { count, own } => {
                let i = j
                    .checked_mul(2)
                    .and_then(|i| i.checked_add(u64::try_from(own.at()).ok()?))
                    .ok_or_else(|| failed("a set's index"))?;
                if i >= *count {
                    return Err(failed("a set's index past its set"));
                }
                if *own != self.door && !self.oracle.borrow().has(i) {
                    return Ok(None);
                }
                let sizes = self.generator.object("sizes").map_err(failed)?;
                write_key(into, format_args!("set{count}/{i}"))?;
                Ok(Some((i, small_size(&sizes, i).map_err(failed)?)))
            }
        })
    }

    /// The ETag object `i`'s bytes have where the phase knows it.
    fn etag(&self, i: u64) -> Option<[u8; 16]> {
        usize::try_from(i)
            .ok()
            .and_then(|i| self.etags.get(i).copied())
    }

    async fn run(&self, logical: &mut Logical, j: u64) -> Result<Came, Error> {
        let Logical { conn, key, scratch } = logical;
        if self.kind == Kind::List {
            let mut again = 0u32;
            let came = loop {
                let came = self.list(conn, key).await?;
                if !self.again_after(&came, j, &mut again).await? {
                    break came;
                }
            };
            return Ok(self.settled(came, again));
        }
        let Some((i, length)) = self.key(j, key)? else {
            return Ok(Came::Skipped);
        };
        let key = key.as_str();
        let object = harness(|| self.generator.object(key)).map_err(failed)?;
        let id = mantle_client::identity().map_err(failed)?;
        let mut again = 0u32;
        let came = loop {
            let came = match conn {
                Conn::Native(client) => {
                    self.native(client, (i, key, length, id), &object, scratch)
                        .await?
                }
                Conn::Http(http) => self.http(http, (i, key, length), &object, scratch).await?,
            };
            if !self.again_after(&came, j, &mut again).await? {
                break came;
            }
        };
        // An object put is acknowledged where its phase keeps a record of them: a set's, and the
        // put of a size whose objects its reads read.
        if matches!(came, Came::Done) && matches!(self.kind, Kind::Put | Kind::Stream) {
            harness(|| self.oracle.borrow_mut().set(i));
        }
        Ok(self.settled(came, again))
    }

    /// Whether operation `j` is asked again after `came`, as the AWS SDKs ask a request again: one
    /// throttled (`SlowDown`) after a pause drawn as botocore's standard retry mode draws it, one
    /// whose exchange ended unanswered at once, each up to mantle-client's attempts in all. Counts
    /// the asking in `again` and in the phase's.
    async fn again_after(&self, came: &Came, j: u64, again: &mut u32) -> Result<bool, Error> {
        let retry = matches!(came, Came::Slowed | Came::Lost(_));
        if !retry || again.saturating_add(1) >= mantle_client::ATTEMPTS {
            return Ok(false);
        }
        *again = again.saturating_add(1);
        harness(|| self.again.set(self.again.get().saturating_add(1)));
        if matches!(came, Came::Slowed) {
            let cap = SLOWED_BASE_NS
                .saturating_mul(
                    1u64.checked_shl(again.saturating_sub(1))
                        .unwrap_or(u64::MAX),
                )
                .min(SLOWED_MOST_NS);
            let pause = mantle_protocol::bounds::full_jitter(
                std::time::Duration::from_nanos(cap),
                j,
                *again,
            );
            sleep(u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX))
                .await
                .map_err(|e| failed(format!("{e:?}")))?;
        }
        Ok(true)
    }

    /// What an operation came to once it was asked as often as it may be: throttled or lost on its
    /// every attempt, it was never answered, a failure of the server's to answer rather than of a
    /// check (`Came::Unanswered`).
    fn settled(&self, came: Came, again: u32) -> Came {
        let asked = again.saturating_add(1);
        match came {
            Came::Slowed => Came::Unanswered(format!("SlowDown, asked {asked} times")),
            Came::Lost(why) => Came::Unanswered(format!("unanswered ({why}), asked {asked} times")),
            came => came,
        }
    }

    /// The range a ranged get reads of an object of `length` bytes: its middle third, a byte at
    /// least, first and last.
    fn range(length: u64) -> (u64, u64) {
        let third = length.checked_div(3).unwrap_or(0);
        let last = third
            .saturating_add(third.max(1))
            .saturating_sub(1)
            .min(length.saturating_sub(1));
        (third.min(last), last)
    }

    fn upload(&self, i: u64) -> Option<Upload> {
        let i = usize::try_from(i).ok()?;
        self.uploads
            .borrow()
            .get(i)
            .cloned()
            .filter(|u| !u.id.is_empty())
    }

    fn uploaded(&self, i: u64, upload: Upload) {
        harness(|| {
            if let Ok(i) = usize::try_from(i)
                && let Some(slot) = self.uploads.borrow_mut().get_mut(i)
            {
                *slot = upload;
            }
        });
    }

    /// Whether an answer's ETag is the one object `i`'s bytes give, where the phase knows it.
    fn etag_ok(&self, i: u64, got: &str, multipart: bool) -> Result<Came, Error> {
        let Some(md5) = self.etag(i) else {
            return Ok(Came::Done);
        };
        let expected = if multipart {
            checksum::multipart_etag(&[md5]).map_err(failed)?
        } else {
            checksum::etag(&md5)
        };
        Ok(if got == expected {
            Came::Done
        } else {
            Came::Failed(format!("ETag {got}, not {expected}"))
        })
    }

    fn checked(&self, wrong: Option<u64>, took: u64, length: u64) -> Came {
        match wrong {
            Some(at) => Came::Differed(format!("byte {at} not the generator's")),
            None if took != length => Came::Differed(format!("{took} bytes, not {length}")),
            None => Came::Done,
        }
    }

    async fn native(
        &self,
        client: &mut Client,
        (i, key, length, id): (u64, &str, u64, u128),
        object: &Object<'_>,
        scratch: &mut [u8],
    ) -> Result<Came, Error> {
        let bucket = BUCKET.to_owned();
        // A read is asked only of an object whose put was acknowledged (`Work::key`), so one with no
        // such key is an acknowledged object missing: a check's failure. Every other status past
        // done is the server's refusal.
        let reads = matches!(self.kind, Kind::Get | Kind::Range | Kind::Head);
        let refused = |reply: &Reply| {
            if reads && reply.status == Status::NoSuchKey {
                Came::Failed(format!(
                    "an acknowledged object missing: {:?}",
                    reply.status
                ))
            } else {
                Came::Refused(format!("{:?}", reply.status))
            }
        };
        Ok(match self.kind {
            Kind::Put => {
                let op = Op::Put {
                    bucket,
                    key: key.to_owned(),
                    id,
                    length,
                    headers: Headers::default(),
                    checksum: None,
                    md5: None,
                    trailing: Some(TRAILER),
                    metadata: Vec::new(),
                };
                let reply = match answered(
                    client
                        .put(&op, &mut Aside(Made::new(object, length)), u64::MAX)
                        .await,
                )? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                if reply.status != Status::Done {
                    refused(&reply)
                } else {
                    self.etag_ok(i, &reply.etag, false)?
                }
            }
            Kind::Stream => {
                let native = Native {
                    bucket,
                    key: key.to_owned(),
                    id,
                    upload: None,
                    checksum: TRAILER,
                    headers: Headers::default(),
                    metadata: Vec::new(),
                };
                let reply = match answered(
                    client
                        .upload(
                            &native,
                            (&mut Aside(Made::new(object, length)), length),
                            &mut Unwatched,
                            u64::MAX,
                        )
                        .await,
                )? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                if reply.status != Status::Done {
                    refused(&reply)
                } else {
                    Came::Done
                }
            }
            Kind::Get | Kind::Range => {
                let (from, to) = if self.kind == Kind::Range {
                    Self::range(length)
                } else {
                    (0, length.saturating_sub(1))
                };
                let headers = Headers {
                    range: (self.kind == Kind::Range).then(|| format!("bytes={from}-{to}")),
                    ..Headers::default()
                };
                let op = Op::Get {
                    bucket,
                    key: key.to_owned(),
                    headers,
                    version: None,
                };
                let mut sink = Aside(Checked::starting(object, scratch, from));
                let reply = match answered(client.get(&op, &mut sink, u64::MAX).await)? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                let end = if length == 0 { 0 } else { to.saturating_add(1) };
                if reply.status != Status::Done {
                    refused(&reply)
                } else {
                    self.checked(sink.0.wrong, sink.0.len(), end)
                }
            }
            Kind::Head => {
                let reply = match answered(
                    client
                        .call(
                            &Op::Head {
                                bucket,
                                key: key.to_owned(),
                                headers: Headers::default(),
                                version: None,
                            },
                            u64::MAX,
                        )
                        .await,
                )? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                if reply.status != Status::Done {
                    refused(&reply)
                } else if reply.size != length {
                    Came::Failed(format!("size {}, not {length}", reply.size))
                } else {
                    self.etag_ok(i, &reply.etag, false)?
                }
            }
            Kind::Delete => {
                let reply = match answered(
                    client
                        .call(
                            &Op::Delete {
                                bucket,
                                key: key.to_owned(),
                                id,
                                headers: Headers::default(),
                                version: None,
                            },
                            u64::MAX,
                        )
                        .await,
                )? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                if reply.status != Status::Done {
                    refused(&reply)
                } else {
                    Came::Done
                }
            }
            Kind::Create => {
                let reply = match answered(
                    client
                        .call(
                            &Op::CreateUpload {
                                bucket,
                                key: key.to_owned(),
                                id,
                                checksum: None,
                                metadata: Vec::new(),
                            },
                            u64::MAX,
                        )
                        .await,
                )? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                if reply.status != Status::Done {
                    refused(&reply)
                } else {
                    self.uploaded(
                        i,
                        Upload {
                            id: reply.upload,
                            part: None,
                        },
                    );
                    Came::Done
                }
            }
            Kind::Part => {
                let Some(upload) = self.upload(i) else {
                    return Ok(Came::Skipped);
                };
                let op = Op::UploadPart {
                    bucket,
                    key: key.to_owned(),
                    upload: upload.id.clone(),
                    number: 1,
                    id,
                    length,
                    checksum: None,
                    md5: None,
                    trailing: None,
                };
                let reply = match answered(
                    client
                        .put(&op, &mut Aside(Made::new(object, length)), u64::MAX)
                        .await,
                )? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                if reply.status != Status::Done {
                    refused(&reply)
                } else {
                    let came = self.etag_ok(i, &reply.etag, false)?;
                    self.uploaded(
                        i,
                        Upload {
                            id: upload.id,
                            part: Some(reply.etag),
                        },
                    );
                    came
                }
            }
            Kind::Parts => {
                let Some(upload) = self.upload(i) else {
                    return Ok(Came::Skipped);
                };
                if upload.part.is_none() {
                    return Ok(Came::Skipped);
                }
                let reply = match answered(
                    client
                        .call(
                            &Op::ListParts {
                                bucket,
                                key: key.to_owned(),
                                upload: upload.id,
                                after: 0,
                                max: PAGE,
                            },
                            u64::MAX,
                        )
                        .await,
                )? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                match reply.listing {
                    Some(Listing::Parts(page)) if page.parts.len() == 1 => Came::Done,
                    _ if reply.status != Status::Done => refused(&reply),
                    _ => Came::Failed("not the one part put".into()),
                }
            }
            Kind::Complete => {
                let Some(Upload {
                    id: upload,
                    part: Some(part),
                }) = self.upload(i)
                else {
                    return Ok(Came::Skipped);
                };
                let op = Op::Complete {
                    bucket,
                    key: key.to_owned(),
                    upload,
                    id,
                    headers: Headers::default(),
                    parts: vec![(1, part)],
                };
                let reply = match answered(client.call(&op, u64::MAX).await)? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                if reply.status != Status::Done {
                    refused(&reply)
                } else {
                    self.etag_ok(i, &reply.etag, true)?
                }
            }
            Kind::Abort => {
                let Some(upload) = self.upload(i) else {
                    return Ok(Came::Skipped);
                };
                let reply = match answered(
                    client
                        .call(
                            &Op::Abort {
                                bucket,
                                key: key.to_owned(),
                                upload: upload.id,
                                id,
                            },
                            u64::MAX,
                        )
                        .await,
                )? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                if reply.status != Status::Done {
                    refused(&reply)
                } else {
                    Came::Done
                }
            }
            Kind::List => Came::Done,
        })
    }

    async fn http(
        &self,
        http: &mut Http,
        (i, key, length): (u64, &str, u64),
        object: &Object<'_>,
        scratch: &mut [u8],
    ) -> Result<Came, Error> {
        let path = format!("/{}/{key}", http.signer.bucket);
        let unsigned = "UNSIGNED-PAYLOAD";
        // A refusal says the server's answer: its status and its error's code and message. A read
        // is asked only of an object whose put was acknowledged (`Work::key`), so a 404 to one is
        // an acknowledged object missing: a check's failure, not the server's refusal.
        let reads = matches!(self.kind, Kind::Get | Kind::Range | Kind::Head);
        let refused = |answer: &Answer, body: &Kept| {
            if answer.status == 503 {
                return Came::Slowed;
            }
            let text = String::from_utf8_lossy(&body.0);
            let said = format!(
                "HTTP {} {} {}",
                answer.status,
                element(&text, "Code").unwrap_or_default(),
                element(&text, "Message").unwrap_or_default()
            );
            if reads && answer.status == 404 {
                Came::Failed(format!("an acknowledged object missing: {said}"))
            } else {
                Came::Refused(said)
            }
        };
        let mut dropped = Kept(Vec::new());
        Ok(match self.kind {
            Kind::Put | Kind::Part => {
                let query = match (self.kind, self.upload(i)) {
                    (Kind::Part, Some(upload)) => format!(
                        "partNumber=1&uploadId={}",
                        sigv4::uri_encode(upload.id.as_bytes(), true)
                    ),
                    (Kind::Part, None) => return Ok(Came::Skipped),
                    _ => String::new(),
                };
                let request = Request {
                    payload: unsigned,
                    length: Length::Known(length),
                    ..Request::bare("PUT", &path, &query)
                };
                let answer = http
                    .ask(
                        &request,
                        &Body::Made(object, 0, length),
                        false,
                        &mut dropped,
                    )
                    .await?;
                let etag = answer.etag.clone().unwrap_or_default();
                if answer.status != 200 {
                    refused(&answer, &dropped)
                } else if self.kind == Kind::Part {
                    let came = self.etag_ok(i, &etag, false)?;
                    if let Some(upload) = self.upload(i) {
                        self.uploaded(
                            i,
                            Upload {
                                id: upload.id,
                                part: Some(etag),
                            },
                        );
                    }
                    came
                } else {
                    self.etag_ok(i, &etag, false)?
                }
            }
            Kind::Stream => {
                let encoded = chunked_length(length, &mut http.piece)?;
                let decoded = length.to_string();
                let headers = [
                    ("content-encoding", "aws-chunked"),
                    ("x-amz-decoded-content-length", decoded.as_str()),
                    ("x-amz-trailer", Algorithm::Crc32.header()),
                ];
                let request = Request {
                    headers: &headers,
                    payload: "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                    length: Length::Known(encoded),
                    ..Request::bare("PUT", &path, "")
                };
                let answer = http
                    .ask(
                        &request,
                        &Body::Chunked(object, length),
                        false,
                        &mut dropped,
                    )
                    .await?;
                if answer.status != 200 {
                    refused(&answer, &dropped)
                } else {
                    Came::Done
                }
            }
            Kind::Get | Kind::Range => {
                let (from, to) = if self.kind == Kind::Range {
                    Self::range(length)
                } else {
                    (0, length.saturating_sub(1))
                };
                let range = (self.kind == Kind::Range).then(|| format!("bytes={from}-{to}"));
                let ranged;
                let headers: &[(&str, &str)] = match &range {
                    Some(range) => {
                        ranged = [("range", range.as_str())];
                        &ranged
                    }
                    None => &[],
                };
                let request = Request {
                    headers,
                    ..Request::bare("GET", &path, "")
                };
                let mut sink = Aside(Checked::starting(object, scratch, from));
                let answer = http.ask(&request, &Body::None, false, &mut sink).await?;
                let end = if length == 0 { 0 } else { to.saturating_add(1) };
                let expected = if self.kind == Kind::Range { 206 } else { 200 };
                if answer.status != expected {
                    refused(&answer, &dropped)
                } else {
                    self.checked(sink.0.wrong, sink.0.len(), end)
                }
            }
            Kind::Head => {
                let request = Request::bare("HEAD", &path, "");
                let answer = http.ask(&request, &Body::None, true, &mut dropped).await?;
                if answer.status != 200 {
                    refused(&answer, &dropped)
                } else if answer.length != Some(length) {
                    Came::Failed(format!("size {:?}, not {length}", answer.length))
                } else {
                    self.etag_ok(i, answer.etag.as_deref().unwrap_or_default(), false)?
                }
            }
            Kind::Delete | Kind::Abort => {
                let query = match (self.kind, self.upload(i)) {
                    (Kind::Abort, Some(upload)) => {
                        format!("uploadId={}", sigv4::uri_encode(upload.id.as_bytes(), true))
                    }
                    (Kind::Abort, None) => return Ok(Came::Skipped),
                    _ => String::new(),
                };
                let request = Request::bare("DELETE", &path, &query);
                let answer = http.ask(&request, &Body::None, false, &mut dropped).await?;
                if answer.status != 204 {
                    refused(&answer, &dropped)
                } else {
                    Came::Done
                }
            }
            Kind::Create => {
                let request = Request::bare("POST", &path, "uploads=");
                let mut kept = Kept(Vec::new());
                let answer = http.ask(&request, &Body::None, false, &mut kept).await?;
                let text = String::from_utf8_lossy(&kept.0);
                match element(&text, "UploadId") {
                    Some(id) if answer.status == 200 => {
                        self.uploaded(
                            i,
                            Upload {
                                id: id.to_owned(),
                                part: None,
                            },
                        );
                        Came::Done
                    }
                    _ => refused(&answer, &kept),
                }
            }
            Kind::Parts => {
                let Some(upload) = self.upload(i) else {
                    return Ok(Came::Skipped);
                };
                if upload.part.is_none() {
                    return Ok(Came::Skipped);
                }
                let query = format!("uploadId={}", sigv4::uri_encode(upload.id.as_bytes(), true));
                let request = Request::bare("GET", &path, &query);
                let mut kept = Kept(Vec::new());
                let answer = http.ask(&request, &Body::None, false, &mut kept).await?;
                let parts = String::from_utf8_lossy(&kept.0).matches("<Part>").count();
                if answer.status != 200 {
                    refused(&answer, &kept)
                } else if parts != 1 {
                    Came::Failed(format!("{parts} parts listed, not 1"))
                } else {
                    Came::Done
                }
            }
            Kind::Complete => {
                let Some(Upload {
                    id: upload,
                    part: Some(part),
                }) = self.upload(i)
                else {
                    return Ok(Came::Skipped);
                };
                let query = format!("uploadId={}", sigv4::uri_encode(upload.as_bytes(), true));
                let body = format!(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{part}\"</ETag></Part></CompleteMultipartUpload>"
                );
                let request = Request {
                    payload: unsigned,
                    length: Length::Known(u64::try_from(body.len()).map_err(failed)?),
                    ..Request::bare("POST", &path, &query)
                };
                let mut kept = Kept(Vec::new());
                let answer = http
                    .ask(&request, &Body::Text(body.as_bytes()), false, &mut kept)
                    .await?;
                let text = String::from_utf8_lossy(&kept.0);
                // The ETag's quotes as XML may escape them: `&quot;` or `&#34;`.
                let unquoted = |etag: &str| etag.replace("&quot;", "").replace("&#34;", "");
                match element(&text, "ETag") {
                    Some(etag) if answer.status == 200 => {
                        self.etag_ok(i, unquoted(etag).trim_matches('"'), true)?
                    }
                    _ if answer.status != 200 => {
                        Came::Refused(format!("HTTP {}: {text}", answer.status))
                    }
                    _ => Came::Failed(format!("HTTP {}: {text}", answer.status)),
                }
            }
            Kind::List => Came::Done,
        })
    }

    /// The listing's next page, from where the last one ended. Each key it lists must be one of the
    /// set's, after the key before it, and once the listing ends every acknowledged object must
    /// have been listed. A key whose put was not acknowledged may be listed or not: a put throttled
    /// or unanswered on every attempt may still have been written, as S3 leaves a 5xx's outcome
    /// unknown. The set's prefix is written into the client's key buffer, and where the page
    /// starts is moved out of the listing and the next page's moved in, so the harness allocates
    /// nothing for a page; a page not had puts its start back, so asking again asks the same page.
    async fn list(&self, conn: &mut Conn, prefix: &mut String) -> Result<Came, Error> {
        let Keys::Set { count, .. } = self.keys else {
            return Ok(Came::Failed("a listing of no set".into()));
        };
        let (start, last) = harness(|| {
            let mut listing = self.listing.borrow_mut();
            ((!listing.ended).then(|| listing.next.take()), listing.last)
        });
        let Some(start) = start else {
            return Ok(Came::Done);
        };
        harness(|| write_key(prefix, format_args!("set{count}/")))?;
        let (page, next) = match self
            .page(conn, prefix, start.as_ref(), (last, count))
            .await?
        {
            Ok(page) => page,
            Err(came) => {
                harness(|| self.listing.borrow_mut().next = start);
                return Ok(came);
            }
        };
        let (ended, listed, seen, acknowledged) = harness(|| {
            let mut listing = self.listing.borrow_mut();
            listing.listed = listing.listed.saturating_add(page.keys);
            listing.acknowledged = listing.acknowledged.saturating_add(page.acknowledged);
            listing.last = page.last;
            listing.ended = next.is_none();
            listing.next = next;
            (
                listing.ended,
                listing.listed,
                listing.acknowledged,
                self.oracle.borrow().acknowledged(),
            )
        });
        Ok(if let Some(wrong) = page.wrong {
            Came::Failed(wrong)
        } else if ended && seen != acknowledged {
            Came::Failed(format!(
                "{seen} of the {acknowledged} acknowledged objects listed, {listed} keys in all"
            ))
        } else {
            Came::Done
        })
    }

    /// Counts `key`, listed under the set's `prefix`, into `page` ([`set_key`]): the page's first
    /// key that is wrong is kept, and each right one counted acknowledged or not.
    fn tally(&self, page: &mut Page, prefix: &str, key: &str, count: u64) {
        page.keys = page.keys.saturating_add(1);
        if page.wrong.is_some() {
            return;
        }
        match set_key(prefix, key, count, page.last) {
            Ok(i) => {
                page.last = Some(i);
                if self.oracle.borrow().has(i) {
                    page.acknowledged = page.acknowledged.saturating_add(1);
                }
            }
            Err(wrong) => page.wrong = Some(wrong),
        }
    }

    /// A page of the keys under `prefix` from `start`, each counted into what it came to after the
    /// set's key `last` of a set of `count` ([`Work::tally`]), and where the next starts; or what
    /// asking it came to where it was not had.
    async fn page(
        &self,
        conn: &mut Conn,
        prefix: &str,
        start: Option<&Next>,
        (last, count): (Option<u64>, u64),
    ) -> Result<Result<(Page, Option<Next>), Came>, Error> {
        let mut tallied = Page {
            keys: 0,
            acknowledged: 0,
            last,
            wrong: None,
        };
        match conn {
            Conn::Native(client) => {
                let op = Op::List(List {
                    bucket: BUCKET.to_owned(),
                    prefix: prefix.to_owned(),
                    delimiter: String::new(),
                    start: match start {
                        Some(Next::Native(resume)) => Some(resume.clone()),
                        _ => None,
                    },
                    max_keys: PAGE,
                    fetch_owner: false,
                });
                let reply = match answered(client.call(&op, u64::MAX).await)? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(Err(came)),
                };
                Ok(match reply.listing {
                    Some(Listing::Keys(page)) => {
                        harness(|| {
                            for listed in &page.contents {
                                self.tally(&mut tallied, prefix, &listed.key, count);
                            }
                        });
                        Ok((
                            tallied,
                            if page.truncated {
                                page.next.map(Next::Native)
                            } else {
                                None
                            },
                        ))
                    }
                    _ => Err(Came::Refused(format!("{:?}", reply.status))),
                })
            }
            Conn::Http(http) => {
                let token = match start {
                    Some(Next::Token(token)) => format!(
                        "&continuation-token={}",
                        sigv4::uri_encode(token.as_bytes(), true)
                    ),
                    _ => String::new(),
                };
                let query = format!(
                    "list-type=2{token}&max-keys={PAGE}&prefix={}",
                    sigv4::uri_encode(prefix.as_bytes(), true)
                );
                let path = format!("/{}", http.signer.bucket);
                let mut kept = Kept(Vec::new());
                let answer = http
                    .ask(
                        &Request::bare("GET", &path, &query),
                        &Body::None,
                        false,
                        &mut kept,
                    )
                    .await?;
                let text = String::from_utf8_lossy(&kept.0);
                if answer.status == 503 {
                    return Ok(Err(Came::Slowed));
                }
                if answer.status != 200 {
                    return Ok(Err(Came::Refused(format!(
                        "HTTP {} {}",
                        answer.status,
                        element(&text, "Code").unwrap_or_default()
                    ))));
                }
                let next = (element(&text, "IsTruncated") == Some("true"))
                    .then(|| {
                        element(&text, "NextContinuationToken").map(|t| Next::Token(t.to_owned()))
                    })
                    .flatten();
                harness(|| {
                    for key in keys(&text) {
                        self.tally(&mut tallied, prefix, key, count);
                    }
                });
                Ok(Ok((tallied, next)))
            }
        }
    }
}

/// The index in a set of `count` of `key`, listed under the set's `prefix` after the set's key
/// `last`: `{prefix}{i}` with `i` below `count` in decimal with no leading zero, sorting after
/// `last`'s key byte by byte, as "List results are always returned in UTF-8 binary order"
/// (research/05); or what is wrong with it.
fn set_key(prefix: &str, key: &str, count: u64, last: Option<u64>) -> Result<u64, String> {
    let digits = key.strip_prefix(prefix).unwrap_or_default();
    let canonical = !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'));
    let Some(i) = canonical
        .then(|| digits.parse::<u64>().ok())
        .flatten()
        .filter(|i| *i < count)
    else {
        return Err(format!("{key} listed, no key of the set"));
    };
    if let Some(last) = last {
        let mut before = [0u8; 20];
        if decimal(last, &mut before).is_none_or(|before| before >= digits.as_bytes()) {
            return Err(format!("{key} listed after {prefix}{last}"));
        }
    }
    Ok(i)
}

/// Each `<Key>` element's text in a ListObjectsV2 document, in the document's order.
fn keys(xml: &str) -> impl Iterator<Item = &str> {
    const OPEN: &str = "<Key>";
    const CLOSE: &str = "</Key>";
    let mut rest = xml;
    std::iter::from_fn(move || {
        let start = rest.find(OPEN)?.checked_add(OPEN.len())?;
        let after = rest.get(start..)?;
        let end = after.find(CLOSE)?;
        let key = after.get(..end)?;
        rest = after.get(end.checked_add(CLOSE.len())?..)?;
        Some(key)
    })
}

/// `v` in decimal, written into `into`: the digits a set's key ends with.
fn decimal(v: u64, into: &mut [u8; 20]) -> Option<&[u8]> {
    let room = into.len();
    let left = {
        let mut rest: &mut [u8] = into;
        write!(rest, "{v}").ok()?;
        rest.len()
    };
    into.get(..room.checked_sub(left)?)
}

/// The cell, its doors, and the connections each door holds between phases.
struct Bench<'o> {
    options: &'o Options,
    /// The harness's process's pauses ([`Watch`]).
    watch: &'o Watch,
    /// The cell this program runs, none against an `--endpoint`.
    cell: Option<Cell>,
    rt: LocalRuntime,
    /// The relay condition's path the native clients dial through, when `--relay` asks for it.
    relay: Option<crate::relay::Relay>,
    client_dir: PathBuf,
    listeners: Vec<SocketAddr>,
    signer: Rc<Signer>,
    generator: Rc<Generator>,
    /// Each door's clients between the steps that use them.
    pools: [Vec<Logical>; 2],
    /// The native clients the cell admits at once: each node's `gateway.clients` identities.
    native_most: usize,
    /// Connections each door opened: its handshakes.
    opened: [u64; 2],
    counting: bool,
    exact: bool,
    ops: u64,
    failures: u64,
    /// Of the failures, the reads whose bytes or length differed from what was put.
    differed: u64,
    /// Of the failures, the operations the server refused, and those it never answered; and the
    /// operations not asked for want of what a failed one would have made.
    refused: u64,
    unanswered: u64,
    skipped: u64,
    /// The most the harness's own work held in any phase, set aside (a counting build's).
    harness_peak: i64,
    /// The most threads the process ran in any phase.
    threads: usize,
    phases: u64,
}

/// The servers' account between two reads: each one's counts as it answered, and what its OS
/// charged it.
struct Servers {
    counts: Vec<Counted>,
    usage: Vec<Option<Usage>>,
}

/// Sums each server's value, none where any server's is none.
fn summed(mut values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    values.try_fold(0.0, |sum, v| Some(sum + v?))
}

/// Each server's growth of a count it answers since it began, from its answer `before` to its
/// answer `after` (a server a slot in each), summed: none where any server's is none.
fn grown(before: &[Counted], after: &[Counted], f: fn(&Counted) -> Option<u64>) -> Option<f64> {
    summed(
        after
            .iter()
            .zip(before)
            .map(|(a, b)| Some(f(a)?.saturating_sub(f(b)?) as f64)),
    )
}

/// A phase line's write-path columns, from the servers' `counts` answers at its start and end: the
/// cell's commands an entry its leaders proposed, its logs' updates a frame, and their frames and
/// flushes an operation, each count every server's growth over the phase summed ([`grown`]); then
/// its logs' commit waits' median, 99th percentile and longest ([`log_wait`]).
fn write_path(before: &[Counted], after: &[Counted], ops: u64) -> String {
    let grew = |f: fn(&Counted) -> Option<u64>| grown(before, after, f);
    let ops = Some(ops as f64);
    format!(
        " commands/entry {} updates/frame {} log_frames/op {} log_flushes/op {} log_wait_p50_us {} \
         log_wait_p99_us {} log_wait_max_us {}",
        ratio(grew(|c| c.commands), grew(|c| c.entries)),
        ratio(grew(|c| c.updates), grew(|c| c.frames)),
        ratio(grew(|c| c.frames), ops),
        ratio(grew(|c| c.flushes), ops),
        log_wait(after, Some(Level::P50), |c| c.wait_p50_ns),
        log_wait(after, Some(Level::P99), |c| c.wait_p99_ns),
        log_wait(after, None, |c| c.wait_max_ns),
    )
}

/// A phase line's columns of the lanes between the servers' shards (node.md §2.9), each every
/// server's growth over the phase summed ([`grown`]): the sends a full lane refused, Raft messages,
/// proposals, reads and every other kind summed; the proposals and reads far ranges refused, each
/// answered to its asker at once; and the stream's words held for want of room. A dash where a
/// server answered none (an endpoint's).
fn lane_columns(before: &[Counted], after: &[Counted]) -> String {
    let grew = |f: fn(&Counted) -> Option<u64>| {
        grown(before, after, f).map_or_else(|| "-".to_owned(), |n| format!("{n:.0}"))
    };
    format!(
        " lane_full_step {} lane_full_propose {} lane_full_read {} lane_full_other {} \
         far_refused_proposals {} far_refused_reads {} stream_held {}",
        grew(|c| c.lane_full_step),
        grew(|c| c.lane_full_propose),
        grew(|c| c.lane_full_read),
        grew(|c| c.lane_full_other),
        grew(|c| c.far_refused_proposals),
        grew(|c| c.far_refused_reads),
        grew(|c| c.stream_held),
    )
}

/// What a server's table of what held its shard counted over a phase for one spawn site and label
/// (`Counted::slow`): the growth of its entry from the server's answer at the phase's start to its
/// answer at the end, and the phase's longest poll where it passed every one before the phase (the
/// table keeps one longest since the shard began), else none.
#[derive(Debug, PartialEq, Eq)]
struct Grown<'a> {
    server: usize,
    site: &'a str,
    label: &'a str,
    polls: u64,
    held_ns: u64,
    longest_ns: Option<u64>,
    task: u64,
    host: u64,
}

/// Each server's table's growth over a phase, entry by entry, from its answers `before` and `after`
/// (a server a slot in each): an entry the start's answer had not counted whole, one that grew no
/// poll left out. At most every server's table's entries, each bounded as its answer is read
/// (`Counted::parse`). None where a server answered no table (an endpoint's).
fn slow_grown<'a>(before: &[Counted], after: &'a [Counted]) -> Option<Vec<Grown<'a>>> {
    let mut grown = Vec::new();
    for (server, (was, now)) in before.iter().zip(after).enumerate() {
        let (was, now) = (was.slow.as_ref()?, now.slow.as_ref()?);
        for entry in now {
            let then = was
                .iter()
                .find(|e| e.site == entry.site && e.label == entry.label);
            let less =
                |f: fn(&Slow) -> u64| f(entry).saturating_sub(then.map(f).unwrap_or_default());
            let polls = less(|e| e.polls);
            if polls == 0 {
                continue;
            }
            grown.push(Grown {
                server,
                site: &entry.site,
                label: &entry.label,
                polls,
                held_ns: less(|e| e.held_ns),
                longest_ns: Some(entry.longest_ns)
                    .filter(|longest| then.is_none_or(|t| *longest > t.longest_ns)),
                task: less(|e| e.task),
                host: less(|e| e.host),
            });
        }
    }
    Some(grown)
}

/// A phase line's columns of what held the servers' shards ([`slow_grown`]): the polls past twice
/// a shard's quantum an operation, every server's summed, to the hundredth; and the label whose
/// polls held the shards longest in all, summed over its sites and the servers. A dash where a
/// server answered no table or the phase ran no operation, and for the label where no labelled
/// poll grew.
fn slow_columns(grown: Option<&[Grown<'_>]>, ops: u64) -> String {
    let polls = grown.map(|g| g.iter().fold(0u64, |sum, e| sum.saturating_add(e.polls)) as f64);
    // Each label's hold over the phase: at most the entries that grew.
    let mut held: Vec<(&str, u64)> = Vec::new();
    for e in grown.unwrap_or_default().iter().filter(|e| e.label != "-") {
        match held.iter_mut().find(|(label, _)| *label == e.label) {
            Some((_, ns)) => *ns = ns.saturating_add(e.held_ns),
            None => held.push((e.label, e.held_ns)),
        }
    }
    let slowest = held
        .iter()
        .max_by_key(|(_, ns)| *ns)
        .map_or("-", |(label, _)| *label);
    format!(
        " slow_polls/op {} slowest_label {slowest}",
        ratio(polls, Some(ops as f64))
    )
}

/// A server's shard that stalled over a phase: the polls hyper-rt's watchdog saw hold it past its
/// limit (`Counted::stalls`), its count's growth from the server's answer at the phase's start to
/// its answer at the end.
#[derive(Debug, PartialEq, Eq)]
struct Stalled {
    server: usize,
    shard: usize,
    stalls: u64,
}

/// Each server's shards' stalls over a phase, shard by shard, from its answers `before` and `after`
/// (a server a slot in each): a shard the start's answer did not count counted whole, one gone by
/// the end or that grew none left out. At most every server's shards, each answer's bounded as it
/// is read (`Counted::parse`). None where a server answered no stalls (an endpoint's).
fn stalls_grown(before: &[Counted], after: &[Counted]) -> Option<Vec<Stalled>> {
    let mut grown = Vec::new();
    for (server, (was, now)) in before.iter().zip(after).enumerate() {
        let (was, now) = (was.stalls.as_ref()?, now.stalls.as_ref()?);
        for (shard, count) in now.iter().enumerate() {
            let Some(count) = count else { continue };
            let then = was.get(shard).copied().flatten().unwrap_or_default();
            let stalls = count.saturating_sub(then);
            if stalls > 0 {
                grown.push(Stalled {
                    server,
                    shard,
                    stalls,
                });
            }
        }
    }
    Some(grown)
}

/// A phase line's column of the servers' stalls ([`stalls_grown`]): the polls the watchdog saw
/// hold a shard past its limit over the phase, every server's shards' summed, so a phase says
/// whether any shard stalled; a dash where a server answered none.
fn stall_column(grown: Option<&[Stalled]>) -> String {
    match grown {
        Some(grown) => format!(
            " stalls {}",
            grown
                .iter()
                .fold(0u64, |sum, shard| sum.saturating_add(shard.stalls))
        ),
        None => " stalls -".to_owned(),
    }
}

/// `value / ops` to a tenth, or `-` where there is no value or no operation.
fn per(value: Option<f64>, ops: u64) -> String {
    match value {
        Some(v) if ops > 0 => format!("{:.1}", v / ops as f64),
        _ => "-".to_owned(),
    }
}

fn micros(ns: u64) -> String {
    format!("{:.1}", ns as f64 / 1_000.0)
}

/// `of / per` to a hundredth, or `-` where either is unknown or `per` is none: a ratio of the write
/// path's counts over a phase (commands an entry, updates a frame, frames and flushes an operation).
fn ratio(of: Option<f64>, per: Option<f64>) -> String {
    match (of, per) {
        (Some(of), Some(per)) if per > 0.0 => format!("{:.2}", of / per),
        _ => "-".to_owned(),
    }
}

/// A commit-wait quantile of the phase's in microseconds, `f` of each server's `counts` answer at the
/// phase's end, which counts the waits its log's updates ended since the answer at its start: the
/// largest of the servers', a quantile of each server's own log, as quantiles do not sum. A dash
/// unless every server answered one over the waits that resolve `level` at 95%
/// (`quantile::samples_for`, as the phase's latencies are dashed), or, for the longest (`None`),
/// over one wait at least.
fn log_wait(after: &[Counted], level: Option<Level>, f: fn(&Counted) -> Option<u64>) -> String {
    let needed = match level {
        Some(level) => coverage().and_then(|c| samples_for(level, c).ok()),
        None => Some(1),
    };
    let resolved = |c: &Counted| match (c.waits, needed) {
        (Some(waits), Some(needed)) => waits >= needed,
        _ => false,
    };
    if after.is_empty() || !after.iter().all(resolved) {
        return "-".to_owned();
    }
    after
        .iter()
        .map(f)
        .try_fold(0, |most: u64, ns| Some(most.max(ns?)))
        .map_or_else(|| "-".to_owned(), micros)
}

/// A latency quantile in microseconds, a dash below the samples that resolve it.
fn quantile(latency: &Histogram, level: Level, ppm: u32) -> String {
    match coverage().and_then(|c| samples_for(level, c).ok()) {
        Some(needed) if latency.count() >= needed => micros(latency.quantile(ppm)),
        _ => "-".to_owned(),
    }
}

/// The median's 95% order-statistic interval in microseconds, its two ends (Le Boudec, Theorem
/// 2.1; `quantile::ranks`), each read from the histogram at its rank; a dash for an end the
/// samples do not reach.
fn median_interval(latency: &Histogram) -> String {
    let n = latency.count();
    let at = |rank: Option<usize>| {
        let rank = u64::try_from(rank?).ok()?;
        let ppm = u32::try_from(rank.checked_mul(1_000_000)?.div_ceil(n.max(1))).ok()?;
        Some(micros(latency.quantile(ppm)))
    };
    match usize::try_from(n)
        .ok()
        .zip(coverage())
        .and_then(|(n, c)| ranks(n, Level::P50, c).ok())
    {
        Some(r) => format!(
            "{} {}",
            at(r.lower).unwrap_or_else(|| "-".to_owned()),
            at(r.upper).unwrap_or_else(|| "-".to_owned())
        ),
        None => "- -".to_owned(),
    }
}

/// A size as a label: 8MiB, 64KiB, 65535B.
fn label(size: u64) -> String {
    crate::display::size(usize::try_from(size).unwrap_or(usize::MAX)).replace(' ', "")
}

/// One measured phase's facts for its line.
struct Measured<'a> {
    door: Door,
    kind: Kind,
    size: &'a str,
    clients: usize,
    elapsed_ns: u64,
    tally: Tally,
    /// What the harness's thread allocated for the doors, its own work set aside, and what that
    /// work came to: in a counting build.
    harness: Option<(Counts, Counts)>,
    generator_ns: u64,
    wire: Wire,
    again: u64,
    /// `perf stat`'s counts across the phase, where `--perf` counted them.
    perf: Option<Counters>,
    excluded: bool,
    /// A closed step's rounds as `ROUNDS` judged them.
    judged: Option<&'a Judgment>,
    /// The time the harness's process was paused over the phase, and its longest pause, in
    /// nanoseconds ([`Watch`]).
    paused: (u64, u64),
}

impl<'o> Bench<'o> {
    /// The servers' counts now, which each server's counting restarts from, and their accounts.
    fn servers(&mut self) -> Result<Servers, Error> {
        let mut counts = Vec::with_capacity(cell::FOUNDERS);
        let mut usage = Vec::with_capacity(cell::FOUNDERS);
        match (self.cell.as_mut(), &self.options.endpoint) {
            (Some(cell), _) => {
                for at in 0..cell::FOUNDERS {
                    counts.push(cell.counts(at).map_err(failed)?);
                    usage.push(cell.pid(at).and_then(|pid| usage::of(pid).ok()));
                }
            }
            (None, Some(endpoint)) => {
                for &pid in &endpoint.pids {
                    counts.push(faults_of(pid));
                    usage.push(usage::of(pid).ok());
                }
            }
            (None, None) => return Err(failed("no cell and no endpoint")),
        }
        Ok(Servers { counts, usage })
    }

    fn cell(&self) -> Result<&Cell, Error> {
        self.cell
            .as_ref()
            .ok_or_else(|| failed("the native door needs a cell"))
    }

    /// The servers' processes: the cell's, or the endpoint's as `--server-pid` named them.
    fn pids(&self) -> Vec<u32> {
        match (&self.cell, &self.options.endpoint) {
            (Some(cell), _) => (0..cell::FOUNDERS).filter_map(|at| cell.pid(at)).collect(),
            (None, Some(endpoint)) => endpoint.pids.clone(),
            (None, None) => Vec::new(),
        }
    }

    /// The doors measured: both through a cell, the HTTP door alone against an endpoint.
    fn doors(&self) -> &'static [Door] {
        match (&self.cell, &self.relay) {
            // The relay condition is the native door's (research/70, "Conditions").
            (Some(_), Some(_)) => &[Door::Native],
            (Some(_), None) => &DOORS,
            (None, _) => &[Door::Http],
        }
    }

    /// A door as its lines name it: `http@ARM` against an endpoint.
    fn named(&self, door: Door) -> String {
        match (door, &self.options.endpoint) {
            (Door::Http, Some(endpoint)) => format!("http@{}", endpoint.arm),
            _ => door.name().to_owned(),
        }
    }

    /// `want` clients of `door` held, more opened as needed, each answered a first request.
    fn hold(&mut self, door: Door, want: usize) -> Result<Vec<Logical>, Error> {
        let pool = self
            .pools
            .get_mut(door.at())
            .ok_or_else(|| failed("a door's pool"))?;
        // A native client held here past its node's idle timeout dials anew itself
        // (`mantle_client`'s `forget_quiet`); an HTTP connection the server closed is opened again
        // by `Http`.
        let mut held: Vec<Logical> = pool.drain(..want.min(pool.len())).collect();
        let more = want.saturating_sub(held.len());
        if more > 0 {
            let opened = self.open(door, more)?;
            held.extend(opened.into_iter().map(|(logical, _)| logical));
        }
        Ok(held)
    }

    /// `n` clients of `door` opened in turn, each answered a first request (a HEAD of a key never
    /// put): each with its time from its opening to that answer.
    fn open(&mut self, door: Door, n: usize) -> Result<Vec<(Logical, u64)>, Error> {
        let mut made = Vec::with_capacity(n);
        let first = usize::try_from(self.opened.get(door.at()).copied().unwrap_or(0)).unwrap_or(0);
        let spread = match door {
            Door::Native => cell::FOUNDERS,
            Door::Http => self.listeners.len(),
        };
        for k in 0..n {
            let at = first.saturating_add(k).checked_rem(spread).unwrap_or(0);
            let conn = match door {
                Door::Native => {
                    // An identity of its own: a node holds two connections of one identity at most
                    // (`mantle_protocol::bounds::CONNECTIONS_PER_PEER`), and each logical client is
                    // a client of its own, as agents are.
                    let number = self.opened.first().copied().unwrap_or(0);
                    let dir = self.options.dir.join("clients").join(number.to_string());
                    let settings = harness(|| {
                        crate::init::client(&mut Vec::new(), &dir, self.cell()?.out())
                            .map_err(failed)?;
                        self.settings(&dir, at)
                    })?;
                    Opening::Native(settings)
                }
                Door::Http => {
                    let listener = *self
                        .listeners
                        .get(at)
                        .ok_or_else(|| failed("no listener"))?;
                    Opening::Http(Box::new(Http::new(listener, Rc::clone(&self.signer))))
                }
            };
            let (conn, took) = self
                .rt
                .block_on(conn.open())
                .map_err(|e| failed(format!("{e:?}")))??;
            if let Some(count) = self.opened.get_mut(door.at()) {
                *count = count.saturating_add(1);
            }
            made.push((Logical::new(conn), took));
        }
        Ok(made)
    }

    /// The settings of the native client `mantle init client` made in `dir`, asking node `first`
    /// first, its key read from the client's directory.
    fn settings(&self, dir: &Path, first: usize) -> Result<Settings, Error> {
        let configured = crate::client::configured(dir).map_err(failed)?;
        let cell = self.cell()?;
        // Under the relay condition each client dials the nodes at a link set of its own, and
        // knows no name to look a node up by again, which would reach it past the relay.
        let (nodes, names) = match &self.relay {
            Some(relay) => (
                relay.take(first).map_err(|e| failed(e.to_string()))?,
                Vec::new(),
            ),
            None => {
                let mut nodes = cell.quic();
                let first = first.min(nodes.len());
                nodes.rotate_left(first);
                (nodes, cell.names())
            }
        };
        Ok(Settings {
            id: configured.id,
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            credentials: configured.credentials,
            transport_bytes: mantle_client::least_transport(nodes.len()).map_err(failed)?,
            nodes,
            names,
        })
    }

    /// One phase on `clients` connections, operations `from..end`, and its line: the operations it
    /// asked, its throughput, a closed step's in rounds (`in_rounds`) as they judged it, and its
    /// operations unanswered.
    fn step(
        &mut self,
        out: &mut impl Write,
        work: &Rc<Work>,
        (size, clients): (&str, usize),
        (from, end, in_rounds): (u64, u64, bool),
    ) -> Result<(u64, Stepped, u64), Error> {
        let round = match (in_rounds, self.options.load) {
            (true, Load::Closed) => Some(round_ops(clients)?),
            _ => None,
        };
        let held = self.hold(work.door, clients)?;
        let wire_before = wired(&held);
        let opened_before = reopened(&held);
        self.phases = self.phases.saturating_add(1);
        let seed = self.options.seed.wrapping_add(self.phases);
        let before = self.servers()?;
        let perf = if self.options.perf {
            Some(Perf::start(
                &self.pids(),
                &self.options.dir.join("perf.csv"),
            )?)
        } else {
            None
        };
        let began = Instant::now();
        let cpu = drive::cpu();
        self.watch.take();
        alloc::begin();
        let ran = self.rt.block_on(phase(
            held,
            Rc::clone(work),
            (self.options.load, seed, from, end),
            round,
        ));
        let total = alloc::end();
        let paused = self.watch.take();
        let aside = alloc::read_aside();
        let generator_ns =
            u64::try_from(drive::cpu().saturating_sub(cpu).as_nanos()).unwrap_or(u64::MAX);
        let elapsed_ns = u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let perf = perf.map(Perf::end).transpose()?;
        let (held, tally, rates) = ran.map_err(|e| failed(format!("{e:?}")))??;
        let judged = round.map(|_| harness(|| rounds::judge(&rates)));
        let after = self.servers()?;
        let wire_after = wired(&held);
        let wire = Wire {
            bytes: wire_after.bytes.saturating_sub(wire_before.bytes),
            datagrams: wire_after.datagrams.saturating_sub(wire_before.datagrams),
        };
        let reopened = reopened(&held).saturating_sub(opened_before);
        if let Some(count) = self.opened.get_mut(work.door.at()) {
            *count = count.saturating_add(reopened);
        }
        if let Some(pool) = self.pools.get_mut(work.door.at()) {
            pool.extend(held);
        }
        let (ops, unanswered) = (tally.ops, tally.unanswered);
        let rate = ops as f64 / (elapsed_ns.max(1) as f64 / 1e9);
        // A server outside this program that refuses every operation of a phase does not offer
        // it: the phase is excluded with the server's answer, not counted as a failed check. A
        // check that failed on an answer it gave is a failure.
        let excluded = self.cell.is_none() && ops > 0 && tally.refused == ops;
        self.ops = self.ops.saturating_add(ops);
        if !excluded {
            self.failures = self.failures.saturating_add(tally.failed);
            self.differed = self.differed.saturating_add(tally.differed);
            self.refused = self.refused.saturating_add(tally.refused);
            self.unanswered = self.unanswered.saturating_add(tally.unanswered);
            self.skipped = self.skipped.saturating_add(tally.skipped);
            self.exact &= tally.failed == 0;
        }
        self.threads = self.threads.max(tally.threads);
        if self.counting {
            self.harness_peak = self.harness_peak.max(aside.peak);
        }
        let measured = Measured {
            door: work.door,
            kind: work.kind,
            size,
            clients,
            elapsed_ns,
            tally,
            harness: self.counting.then(|| (total.less(&aside), aside)),
            generator_ns,
            wire,
            again: work.again.replace(0),
            perf,
            excluded,
            judged: judged.as_ref(),
            paused,
        };
        self.line(out, &measured, (&before, &after))?;
        // A step not judged in rounds, or whose rounds kept none, is its one throughput.
        let point = Stepped {
            clients,
            median: rate,
            low: rate,
            high: rate,
        };
        Ok((
            ops,
            judged
                .as_ref()
                .and_then(|j| Stepped::of(clients, j))
                .unwrap_or(point),
            unanswered,
        ))
    }

    fn line(
        &self,
        out: &mut impl Write,
        m: &Measured<'_>,
        (before, after): (&Servers, &Servers),
    ) -> Result<(), Error> {
        let ops = m.tally.ops;
        let counted = |f: fn(&Counted) -> Option<u64>| {
            summed(after.counts.iter().map(|c| f(c).map(|v| v as f64)))
        };
        let grew = |f: fn(&Counted) -> Option<u64>| grown(&before.counts, &after.counts, f);
        let charged = |f: fn(&Usage) -> Option<u64>| {
            summed(
                after
                    .usage
                    .iter()
                    .zip(&before.usage)
                    .map(|(a, b)| f(&a.as_ref()?.since(b.as_ref()?)).map(|v| v as f64)),
            )
        };
        // Each server's peak live heap over the phase, above what it held as the phase began (its
        // count restarts at each read), summed: the most the cell could have held for the phase
        // at once, an upper bound.
        let peak = summed(after.counts.iter().map(|c| c.peak.map(|p| p.max(0) as f64)));
        let footprint = summed(
            after
                .usage
                .iter()
                .map(|u| u.as_ref().and_then(|u| u.footprint).map(|f| f as f64)),
        );
        let perf = |f: fn(&Counters) -> Option<u64>| m.perf.as_ref().and_then(f).map(|v| v as f64);
        let clients = u64::try_from(m.clients).unwrap_or(u64::MAX);
        let most = u64::try_from(m.tally.most).unwrap_or(u64::MAX);
        let mine = |f: fn(&Counts) -> u64| m.harness.as_ref().map(|(doors, _)| f(doors) as f64);
        let own = |f: fn(&Counts) -> i64| {
            m.harness
                .as_ref()
                .map_or_else(|| "-".to_owned(), |(_, own)| f(own).max(0).to_string())
        };
        let open = matches!(self.options.load, Load::Open(_));
        let t = &m.tally;
        let slow = slow_grown(&before.counts, &after.counts);
        let stalled = stalls_grown(&before.counts, &after.counts);
        writeln!(
            out,
            "doors {} {} {} {} clients {} elapsed_ms {} ops {ops} allocs/op {} reallocs/op {} moved/op {} bytes/op {} peak_live/request {} peak_live/connection {} \
             client_allocs/op {} client_bytes/op {} minor_faults/op {} major_faults/op {} user_ns/op {} system_ns/op {} instructions/op {} cycles/op {} switches/op {} \
             wire_bytes/op {} datagrams/op {} server_footprint_bytes {} harness_allocs/op {} harness_peak_live_bytes {} out_most {} threads {} \
             latency_us p50 {} p50_95 {} p99 {} p99.9 {} p99.99 {} max {} generator_cpu_ns/op {} lateness_us p99 {} refused {} unanswered {} skipped {} differed {} again {} failed {}{}{}{}{}",
            self.named(m.door),
            m.kind.name(),
            m.size,
            self.options.condition,
            m.clients,
            m.elapsed_ns.checked_div(1_000_000).unwrap_or(0),
            per(counted(|c| c.allocations), ops),
            per(counted(|c| c.reallocations), ops),
            per(counted(|c| c.moved), ops),
            per(counted(|c| c.bytes), ops),
            per(peak, most),
            per(peak, clients),
            per(mine(|c| c.allocations), ops),
            per(mine(|c| c.bytes), ops),
            per(grew(|c| c.minor), ops),
            per(grew(|c| c.major), ops),
            per(charged(|u| Some(u.user_ns)), ops),
            per(charged(|u| Some(u.system_ns)), ops),
            per(
                perf(|p| p.instructions).or(charged(|u| u.instructions)),
                ops
            ),
            per(perf(|p| p.cycles).or(charged(|u| u.cycles)), ops),
            per(grew(|c| c.switches), ops),
            per(Some(m.wire.bytes as f64), ops),
            match m.door {
                Door::Native => per(Some(m.wire.datagrams as f64), ops),
                Door::Http => "-".to_owned(),
            },
            footprint.map_or_else(|| "-".to_owned(), |f| format!("{f:.0}")),
            per(
                m.harness.as_ref().map(|(_, own)| own.allocations as f64),
                ops
            ),
            own(|c| c.peak),
            t.most,
            t.threads,
            quantile(&t.latency, Level::P50, 500_000),
            median_interval(&t.latency),
            quantile(&t.latency, Level::P99, 990_000),
            quantile(&t.latency, Level::P999, 999_000),
            quantile(&t.latency, Level::P9999, 999_900),
            if ops > 0 {
                micros(t.latency.max())
            } else {
                "-".to_owned()
            },
            per(Some(m.generator_ns as f64), ops),
            if open {
                quantile(&t.lateness, Level::P99, 990_000)
            } else {
                "-".to_owned()
            },
            t.refused,
            t.unanswered,
            t.skipped,
            t.differed,
            m.again,
            t.failed,
            write_path(&before.counts, &after.counts, ops),
            lane_columns(&before.counts, &after.counts),
            slow_columns(slow.as_deref(), ops),
            stall_column(stalled.as_deref()),
        )?;
        self.step_line(out, m)?;
        for e in slow.iter().flatten() {
            writeln!(
                out,
                "doors {} {} {} {} slow server {} site {} label {} polls {} held_us {} longest_us {} task {} host {}",
                self.named(m.door),
                m.kind.name(),
                m.size,
                self.options.condition,
                e.server,
                e.site,
                e.label,
                e.polls,
                micros(e.held_ns),
                e.longest_ns.map_or_else(|| "-".to_owned(), micros),
                e.task,
                e.host,
            )?;
        }
        for s in stalled.iter().flatten() {
            writeln!(
                out,
                "doors {} {} {} {} stall server {} shard {} stalls {}",
                self.named(m.door),
                m.kind.name(),
                m.size,
                self.options.condition,
                s.server,
                s.shard,
                s.stalls,
            )?;
        }
        for kind in Failure::ALL {
            let Some(why) = t.first.get(kind.at()).and_then(Option::as_ref) else {
                continue;
            };
            let tag = match kind {
                Failure::Refusal if m.excluded => "excluded",
                kind => kind.tag(),
            };
            writeln!(
                out,
                "doors {} {} {} {} {tag}: {why}",
                self.named(m.door),
                m.kind.name(),
                m.size,
                self.options.condition,
            )?;
        }
        out.flush()?;
        Ok(())
    }

    /// A phase's step, a line after its phase's: how long the harness's process was paused over
    /// it ([`Watch`]), and a closed step's rounds: how many were kept and how they are ordered in
    /// time (measurement.md §2), and each state's median throughput, its 95% interval where it
    /// has one, and its rounds, the state of most rounds first.
    fn step_line(&self, out: &mut impl Write, m: &Measured<'_>) -> Result<(), Error> {
        write!(
            out,
            "doors {} {} {} {} step clients {} paused_ms {} longest_pause_ms {}",
            self.named(m.door),
            m.kind.name(),
            m.size,
            self.options.condition,
            m.clients,
            m.paused.0.checked_div(1_000_000).unwrap_or(0),
            m.paused.1.checked_div(1_000_000).unwrap_or(0),
        )?;
        let Some(judged) = m.judged else {
            writeln!(out)?;
            return Ok(());
        };
        write!(
            out,
            " rounds kept {} order {:?} lag1 {:.2}",
            judged.rounds, judged.order, judged.lag1,
        )?;
        let mut states: Vec<&rounds::State> = judged.states.iter().collect();
        states.sort_by_key(|s| std::cmp::Reverse(s.rounds.len()));
        for state in states {
            let (low, high) = state.interval.map_or_else(
                || ("-".to_owned(), "-".to_owned()),
                |(l, h)| (format!("{l:.1}"), format!("{h:.1}")),
            );
            write!(
                out,
                " state rounds {} ops/s median {:.1} interval {low} {high} min {:.1} max {:.1}",
                state.rounds.len(),
                state.median,
                state.min,
                state.max,
            )?;
        }
        writeln!(out)?;
        Ok(())
    }

    /// A phase's steps: open loop, one of `n` operations from `from` on `--clients` clients;
    /// closed, a ladder from one client doubling up to `--clients`, each step judged in rounds
    /// ([`ROUNDS`]) where `n` holds them and asking at most `n` operations further on, and ending
    /// [`FLAT`] steps after the last whose interval lay wholly above the best's before it, or at a
    /// step that left an operation unanswered, which is not its best ([`Climb::after`]). A step
    /// whose `n` holds no judged rounds (a large object's, its bytes capped) is its one throughput.
    /// Operations end at `end` where the phase's keys end; `finish` runs what the ladder left of
    /// them at its best step's clients. Says how far the steps went.
    fn ladder(
        &mut self,
        out: &mut impl Write,
        work: &Rc<Work>,
        size: &str,
        (from, n, end, finish): (u64, u64, u64, bool),
    ) -> Result<u64, Error> {
        let budget = self.clients(work.door);
        if let Load::Open(_) = self.options.load {
            let end = from.saturating_add(n).min(end);
            self.step(out, work, (size, budget), (from, end, false))?;
            return Ok(end);
        }
        let judged = |clients: usize| -> Result<bool, Error> {
            let least = round_ops(clients)?
                .saturating_mul(u64::try_from(ROUNDS.min).map_err(failed)?.saturating_add(1));
            Ok(n >= least)
        };
        let mut climb = Climb {
            best: None,
            flat: 0,
        };
        let (mut at, mut clients) = (from, 1usize);
        while at < end {
            let to = at.saturating_add(n.min(step_ops(clients)?)).min(end);
            let (asked, stepped, unanswered) =
                self.step(out, work, (size, clients), (at, to, judged(clients)?))?;
            at = at.saturating_add(asked).min(to);
            if !climb.after(stepped, unanswered) || clients >= budget {
                if unanswered > 0 {
                    writeln!(
                        out,
                        "doors {} {} {size} {} ladder ended clients {clients} unanswered {unanswered}",
                        self.named(work.door),
                        work.kind.name(),
                        self.options.condition,
                    )?;
                }
                break;
            }
            clients = clients.saturating_mul(2).min(budget);
        }
        if let Some(best) = climb.best {
            writeln!(
                out,
                "doors {} {} {size} {} ladder best clients {} ops/s median {:.1} low {:.1} high {:.1}",
                self.named(work.door),
                work.kind.name(),
                self.options.condition,
                best.clients,
                best.median,
                best.low,
                best.high,
            )?;
        }
        if finish && at < end {
            let clients = climb.best.map_or(1, |best| best.clients);
            self.step(out, work, (size, clients), (at, end, false))?;
            at = end;
        }
        Ok(at)
    }

    /// The operations a phase through `door`'s steps ask at most, each at most `n`: open loop, one
    /// step's; closed, each step's of a ladder doubling from one client to the door's, as many as
    /// its rounds may ask ([`step_ops`]).
    fn ladder_most(&self, door: Door, n: u64) -> Result<u64, Error> {
        if let Load::Open(_) = self.options.load {
            return Ok(n);
        }
        let budget = self.clients(door);
        let (mut most, mut clients) = (0u64, 1usize);
        loop {
            most = most.saturating_add(n.min(step_ops(clients)?));
            if clients >= budget {
                return Ok(most);
            }
            clients = clients.saturating_mul(2).min(budget);
        }
    }

    /// The clients a phase through `door` runs at most: `--clients`, and through the native door
    /// no more than the cell admits, each node `gateway.clients` client identities at once.
    fn clients(&self, door: Door) -> usize {
        match door {
            Door::Native => self.options.clients.min(self.native_most),
            Door::Http => self.options.clients,
        }
        .max(1)
    }

    /// Drops `door`'s clients on the runtime, where a native client's drop tells its node it
    /// closed (QUIC's CONNECTION_CLOSE), so the node frees its identity at once.
    fn release(&mut self, clients: Vec<Logical>) -> Result<(), Error> {
        self.rt
            .block_on(async move { drop(clients) })
            .map_err(|e| failed(format!("{e:?}")))
    }

    fn work(&self, door: Door, kind: Kind, keys: Keys) -> Result<Work, Error> {
        Ok(Work {
            door,
            kind,
            keys,
            generator: Rc::clone(&self.generator),
            etags: Rc::new(Vec::new()),
            uploads: Rc::new(RefCell::new(Vec::new())),
            oracle: Rc::new(RefCell::new(Oracle::new(0).map_err(failed)?)),
            listing: Rc::new(RefCell::new(Paging::default())),
            again: std::cell::Cell::new(0),
        })
    }

    /// The MD5 of each of the first `count` objects of `door`'s `kind` at `size`, [`ETAGS`] at
    /// most, made from the generator before the phases that check them.
    fn etags(
        &self,
        door: Door,
        (size, kind): (u64, char),
        count: u64,
    ) -> Result<Rc<Vec<[u8; 16]>>, Error> {
        let mut piece = vec![0u8; mantle_client::PIECE];
        let etags: Result<Vec<_>, Error> = (0..count.min(ETAGS))
            .map(|i| {
                let object = self
                    .generator
                    .object(&sized_key(door, size, kind, i)?)
                    .map_err(failed)?;
                md5(&object, size, &mut piece)
            })
            .collect();
        Ok(Rc::new(etags?))
    }

    /// A size's phase of `kind` through `door` on `keys`, its ETags and uploads shared with the
    /// size's other phases, operations `from..end`, `n` a step: how far its steps went.
    fn sized_phase(
        &mut self,
        out: &mut impl Write,
        (door, kind, keys): (Door, Kind, Keys),
        shared: Common<'_>,
        (name, from, n, end): (&str, u64, u64, u64),
    ) -> Result<u64, Error> {
        if !self.options.kinds.is_empty() && !self.options.kinds.contains(&kind) {
            return Ok(0);
        }
        let work = Rc::new(Work {
            etags: Rc::clone(shared.0),
            uploads: Rc::clone(shared.1),
            oracle: Rc::clone(shared.2),
            ..self.work(door, kind, keys)?
        });
        self.ladder(out, &work, name, (from, n, end, false))
    }

    /// Every phase of one size through one door: puts whole and streamed, reads whole and ranged,
    /// heads, a multipart upload's steps, and deletes. An object past S3's single PUT is streamed
    /// through the native door alone.
    fn sized(&mut self, out: &mut impl Write, door: Door, size: u64) -> Result<(), Error> {
        let name = label(size);
        let n = self
            .options
            .ops
            .min(
                self.options
                    .bytes
                    .checked_div(size)
                    .unwrap_or(u64::MAX)
                    .max(1),
            )
            .max(1);
        let whole = size <= mantle_s3::body::MAX_UPLOAD;
        if !whole && door == Door::Http {
            writeln!(
                out,
                "doors {} {name} {}: past S3's single PUT and part ({} bytes), not asked",
                self.named(door),
                self.options.condition,
                mantle_s3::body::MAX_UPLOAD
            )?;
            return Ok(());
        }
        // A whole object's ETag is the MD5 of its bytes; a larger one's is not, and goes unchecked.
        let etags = if whole {
            self.etags(door, (size, 'p'), n)?
        } else {
            Rc::new(Vec::new())
        };
        let uploads = Rc::new(RefCell::new(Vec::new()));
        // The most objects a phase's steps make: one step open loop; closed, a ladder's steps, its
        // clients doubling up to the door's, each asking what its rounds may within `n`.
        let most = self.ladder_most(door, n)?;
        // The objects the reads read whose puts were acknowledged, a bit each, which the put that
        // made them keeps and the reads ask of; every other phase keeps none.
        let put = Rc::new(RefCell::new(Oracle::new(most).map_err(failed)?));
        let none = Rc::new(RefCell::new(Oracle::new(0).map_err(failed)?));
        let keyed = |kind: char, made: Option<u64>| Keys::Sized { size, kind, made };
        let phase = |kind: Kind, keys: Keys| (door, kind, keys);
        let mut made = 0;
        if whole {
            made = self.sized_phase(
                out,
                phase(Kind::Put, keyed('p', None)),
                (&etags, &uploads, &put),
                (&name, 0, n, u64::MAX),
            )?;
        }
        let streamed = self.sized_phase(
            out,
            phase(Kind::Stream, keyed('s', None)),
            (&etags, &uploads, if whole { &none } else { &put }),
            (&name, 0, n, u64::MAX),
        )?;
        let (read, made) = if whole { ('p', made) } else { ('s', streamed) };
        for kind in [Kind::Get, Kind::Range, Kind::Head] {
            if kind != Kind::Range || size > 0 {
                self.sized_phase(
                    out,
                    phase(kind, keyed(read, Some(made))),
                    (&etags, &uploads, &put),
                    (&name, 0, n, u64::MAX),
                )?;
            }
        }
        if whole {
            let parts = self.etags(door, (size, 'm'), n)?;
            // A slot for each upload a ladder of creates may make, each create filling its own.
            uploads
                .borrow_mut()
                .resize(usize::try_from(most).map_err(failed)?, Upload::default());
            let shared = (&parts, &uploads, &none);
            let created = self.sized_phase(
                out,
                phase(Kind::Create, keyed('m', None)),
                shared,
                (&name, 0, n, most),
            )?;
            let parted = self.sized_phase(
                out,
                phase(Kind::Part, keyed('m', None)),
                shared,
                (&name, 0, n, created),
            )?;
            self.sized_phase(
                out,
                phase(Kind::Parts, keyed('m', Some(parted.max(1)))),
                shared,
                (&name, 0, n, u64::MAX),
            )?;
            // The first half of the uploads parted is completed, the rest aborted.
            let half = parted.saturating_sub(parted.checked_div(2).unwrap_or(0));
            self.sized_phase(
                out,
                phase(Kind::Complete, keyed('m', None)),
                shared,
                (&name, 0, n, half),
            )?;
            self.sized_phase(
                out,
                phase(Kind::Abort, keyed('m', None)),
                shared,
                (&name, half, n, created),
            )?;
        }
        self.sized_phase(
            out,
            phase(Kind::Delete, keyed(read, None)),
            (&etags, &uploads, &none),
            (&name, 0, n, made),
        )?;
        Ok(())
    }

    /// A set of `count` small objects: each door puts its half, lists the whole set a page at a
    /// time, reads the other's half back where acknowledged, and deletes its own.
    fn set(&mut self, out: &mut impl Write, count: u64) -> Result<(), Error> {
        let name = format!("set{count}");
        let oracle = Rc::new(RefCell::new(Oracle::new(count).map_err(failed)?));
        // A door reads the other's half where both run; alone, its own.
        let both = self.doors().len() > 1;
        let other = |door: Door| {
            if door == Door::Native {
                Door::Http
            } else {
                Door::Native
            }
        };
        for (kind, crossed) in [
            (Kind::Put, false),
            (Kind::List, false),
            (Kind::Get, true),
            (Kind::Delete, false),
        ] {
            for &door in self.doors() {
                let own = if crossed && both { other(door) } else { door };
                let work = Rc::new(Work {
                    oracle: Rc::clone(&oracle),
                    ..self.work(door, kind, Keys::Set { count, own })?
                });
                if kind == Kind::List {
                    // A listing's pages follow each other: one client, as many pages as the set fills.
                    let pages = count
                        .checked_div(u64::from(PAGE))
                        .unwrap_or(0)
                        .saturating_add(1);
                    self.step(out, &work, (&name, 1), (0, pages, false))?;
                    continue;
                }
                // Each door's half of the set, every object of it: open loop in one step, closed
                // in the ladder's steps and the rest at its best.
                let half = count
                    .saturating_sub(u64::try_from(own.at()).map_err(failed)?)
                    .div_ceil(2);
                let n = match self.options.load {
                    Load::Open(_) => half,
                    Load::Closed => self.options.ops.max(1),
                };
                self.ladder(out, &work, &name, (0, n, half, true))?;
            }
        }
        Ok(())
    }
}

/// The MD5 of `object`'s `length` bytes, made a piece at a time.
fn md5(object: &Object<'_>, length: u64, piece: &mut [u8]) -> Result<[u8; 16], Error> {
    let mut digest = crypto::Digest::new(&crypto::MD5).map_err(failed)?;
    let mut at = 0u64;
    while at < length {
        let n = usize::try_from(length.saturating_sub(at))
            .unwrap_or(usize::MAX)
            .min(piece.len());
        let made = piece.get_mut(..n).ok_or_else(|| failed("a piece"))?;
        object.fill(at, made).map_err(failed)?;
        digest.update(made).map_err(failed)?;
        at = at.saturating_add(u64::try_from(n).map_err(failed)?);
    }
    digest.finish_array().map_err(failed)
}

/// A connection being opened, on the runtime.
enum Opening {
    Native(Settings),
    Http(Box<Http>),
}

impl Opening {
    /// Opens the connection and asks its first request, a HEAD of a key never put: the connection,
    /// and the nanoseconds from its opening to that answer.
    async fn open(self) -> Result<(Conn, u64), Error> {
        let began = now_ns();
        let conn = match self {
            Self::Native(settings) => {
                let mut client = Client::open(settings).await.map_err(failed)?;
                let op = Op::Head {
                    bucket: BUCKET.to_owned(),
                    key: NEVER_PUT.to_owned(),
                    headers: Headers::default(),
                    version: None,
                };
                let reply = client.call(&op, u64::MAX).await.map_err(failed)?;
                if reply.status != Status::NoSuchKey {
                    return Err(failed(format!(
                        "a new client's first request: {:?}",
                        reply.status
                    )));
                }
                Conn::Native(Box::new(client))
            }
            Self::Http(mut http) => {
                let path = format!("/{}/{NEVER_PUT}", http.signer.bucket);
                let answer = http
                    .ask(
                        &Request::bare("HEAD", &path, ""),
                        &Body::None,
                        true,
                        &mut Kept(Vec::new()),
                    )
                    .await?;
                if answer.status != 404 {
                    return Err(failed(format!(
                        "a new connection's first request: HTTP {}",
                        answer.status
                    )));
                }
                Conn::Http(http)
            }
        };
        Ok((conn, now_ns().saturating_sub(began)))
    }
}

/// The runtime the harness's clients run on, one shard on this thread: a task for each of
/// `conns` clients and the phase's own, a timer for each client's wait for its intended start and
/// two for a native client's waits or one for an HTTP connection's ([`patient`]), and the sockets
/// of as many native clients.
fn runtime(conns: usize) -> Result<LocalRuntime, Error> {
    let tasks = conns.checked_add(2).ok_or_else(|| failed("tasks"))?;
    let timers = conns
        .checked_mul(3)
        .and_then(|t| t.checked_add(2))
        .ok_or_else(|| failed("timers"))?;
    let sockets = mantle_client::sockets(cell::FOUNDERS)
        .map_err(failed)?
        .checked_mul(conns)
        .ok_or_else(|| failed("sockets"))?;
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: tasks,
        timers_per_shard: timers,
        interests_per_shard: interests_for(tasks).max(sockets.saturating_mul(2)),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).map_err(failed)?,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .map_err(|e| failed(format!("{e:?}")))
}

/// What a `mantle serve` process's `counts` answer begins with, the words after `counts`
/// ([`mantle_node::process::answer_counts`]; the node says its write path's counts after them): in
/// a `count-allocs` build its allocator's counts across every thread since it was last asked,
/// counting afresh from there (a few events another thread makes between the read and the restart
/// are counted in neither window); in any build, its faults and context switches since it began.
pub fn counts_line() -> String {
    let faults = hyper_measure::faults::read().ok();
    let minor = faults
        .map(|f| f.minor.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let major = faults
        .and_then(|f| f.major)
        .map(|m| m.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let switches = hyper_measure::faults::switches()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "-".to_owned());
    let counted = if cfg!(feature = "count-allocs") {
        let c = alloc::read_process();
        alloc::begin_process();
        format!(
            "counted allocs {} reallocs {} moved {} bytes {} live {} peak {}",
            c.allocations, c.reallocations, c.moved, c.bytes, c.live, c.peak
        )
    } else {
        "uncounted allocs - reallocs - moved - bytes - live - peak -".to_owned()
    };
    format!("{counted} minor {minor} major {major} switches {switches}")
}

/// Runs every phase of `options` against a cell of three made under its directory: whether every
/// check was exact and each control was caught.
pub fn doors(out: &mut impl Write, options: &Options) -> Result<bool, Error> {
    let messages = mantle_node::derive::messages().map_err(failed)?;
    mantle_protocol::classes::state_bounds(messages).map_err(failed)?;
    std::fs::create_dir_all(options.dir.parent().unwrap_or(Path::new(".")))?;
    std::fs::create_dir(&options.dir)?;
    let counting = alloc::installed();
    writeln!(
        out,
        "doors run seed {} condition {} range_shards {} load {} clients {} ops {} counting {}",
        options.seed,
        options.condition,
        options.range_shards,
        match options.load {
            Load::Open(rate) => format!("open:{rate}"),
            Load::Closed => "closed".to_owned(),
        },
        options.clients,
        options.ops,
        if counting {
            "yes"
        } else {
            "no (a count-allocs build counts allocations)"
        }
    )?;
    let client_dir = options.dir.join("client");
    let (cell, listeners, signer) = match &options.endpoint {
        Some(endpoint) => {
            writeln!(
                out,
                "doors endpoint http@{} at {} bucket {} region {} pids {:?}",
                endpoint.arm, endpoint.at, endpoint.bucket, endpoint.region, endpoint.pids
            )?;
            let signer = Signer {
                id: endpoint.id.clone(),
                secret: endpoint.secret.clone(),
                region: endpoint.region.clone(),
                bucket: endpoint.bucket.clone(),
            };
            (None, vec![endpoint.at], signer)
        }
        None => {
            let mut cell =
                Cell::make(&options.dir, options.volume, options.range_shards).map_err(failed)?;
            // Every range led before the first phase, so no phase measures the cell's first
            // elections.
            cell.led().map_err(failed)?;
            crate::init::client(&mut Vec::new(), &client_dir, cell.out()).map_err(failed)?;
            let s3 = cell.s3();
            let region = s3
                .first()
                .map(|(_, r)| r.clone())
                .ok_or_else(|| failed("no S3 listener"))?;
            let signer = Signer::read(cell.out(), region)?;
            // A cell on loopback says each listener as the address it bound.
            let listeners = s3
                .iter()
                .map(|(at, _)| at.parse::<SocketAddr>().map_err(failed))
                .collect::<Result<Vec<_>, _>>()?;
            (Some(cell), listeners, signer)
        }
    };
    let watch = Watch::default();
    let (end, ended) = watch_channel::<()>();
    std::thread::scope(|scope| {
        let watching = &watch;
        let watching = std::thread::Builder::new()
            .name("doors-watch".to_owned())
            .spawn_scoped(scope, move || watching.run(&ended))?;
        let ran = run(
            out,
            options,
            &watch,
            (cell, listeners, signer, client_dir, counting),
        );
        drop(end);
        watching
            .join()
            .map_err(|_| failed("the harness's watch ended unwound"))?;
        ran
    })
}

/// The phases, controls and slope of `doors`, on its cell or endpoint, its process watched by
/// `watch`.
fn run(
    out: &mut impl Write,
    options: &Options,
    watch: &Watch,
    (cell, listeners, signer, client_dir, counting): (
        Option<Cell>,
        Vec<SocketAddr>,
        Signer,
        PathBuf,
        bool,
    ),
) -> Result<bool, Error> {
    let most = options
        .clients
        .saturating_mul(2)
        .saturating_add(SLOPE_HIGH)
        .saturating_add(1);
    // Each node admits as many native clients at once as it derived (`gateway.clients`), so a
    // client past them is refused at admission (node.md §3.6): the native door's clients stay
    // within. An endpoint this program did not start has no native door.
    let native_most = cell.as_ref().map_or(0, Cell::native_clients);
    writeln!(
        out,
        "doors native clients at most {native_most}: each node admits gateway.clients identities at once"
    )?;
    // The relay condition: a link set for each client the harness holds at most, each direction
    // holding what one client's transport may have unacknowledged or unread.
    let relay = match (options.relay, &cell) {
        (false, _) => None,
        (true, None) => {
            return Err(failed(
                "--relay asks the cell's own nodes, which an --endpoint has none of",
            ));
        }
        (true, Some(cell)) => {
            let nodes = cell.quic();
            let held = mantle_client::least_transport(nodes.len()).map_err(failed)?;
            let relay = crate::relay::Relay::start(&client_dir, &nodes, most, (held, options.seed))
                .map_err(|e| failed(e.to_string()))?;
            writeln!(
                out,
                "doors relay one_way_ns {} jitter_ns {} loss {}/{} in runs of {}/{} link_sets {most} held_bytes {held}",
                crate::relay::ONE_WAY_NS,
                crate::relay::JITTER_NS,
                crate::relay::LOST,
                crate::relay::SENT,
                crate::relay::LOST,
                crate::relay::RUNS,
            )?;
            Some(relay)
        }
    };
    let mut bench = Bench {
        options,
        watch,
        listeners,
        cell,
        rt: runtime(most)?,
        relay,
        client_dir,
        signer: Rc::new(signer),
        generator: Rc::new(Generator::new(options.seed).map_err(failed)?),
        pools: [Vec::new(), Vec::new()],
        native_most,
        opened: [0, 0],
        counting,
        exact: true,
        ops: 0,
        failures: 0,
        differed: 0,
        refused: 0,
        unanswered: 0,
        skipped: 0,
        harness_peak: 0,
        threads: 0,
        phases: 0,
    };
    let idle = drive::thread_count();
    bench.create()?;
    let doors = bench.doors();
    for &door in doors {
        for &size in &options.sizes {
            bench.sized(out, door, size)?;
        }
    }
    for &count in &options.objects {
        bench.set(out, count)?;
    }
    let mut caught = 0u32;
    for &door in doors {
        caught = caught.saturating_add(bench.controls(out, door)?);
    }
    for &door in doors {
        bench.slope(out, door)?;
    }
    writeln!(
        out,
        "doors handshakes native {} http {}",
        bench.opened[0], bench.opened[1]
    )?;
    // The relay's own count: what its chain lost, what passed a direction's bytes, and how late it
    // sent what it held, so its cost is not charged to the stack (research/70 [12]).
    if let Some(relay) = bench.relay.take() {
        let said = relay.stop().map_err(|e| failed(e.to_string()))?;
        writeln!(
            out,
            "doors relay taken {} lost {} over {} sent {} late_mean_ns {} late_max_ns {}",
            said.taken,
            said.lost,
            said.over,
            said.sent,
            said.late_ns.checked_div(said.sent).unwrap_or(0),
            said.latest_ns,
        )?;
    }
    writeln!(
        out,
        "doors harness peak_live_bytes {} threads {} idle_threads {idle}",
        if bench.counting {
            bench.harness_peak.max(0).to_string()
        } else {
            "-".to_owned()
        },
        bench.threads,
    )?;
    let controls = u32::try_from(doors.len()).unwrap_or(0).saturating_mul(3);
    writeln!(
        out,
        "doors verified ops {} differed {} refused {} unanswered {} skipped {} failed {} controls caught {caught} of {controls}",
        bench.ops, bench.differed, bench.refused, bench.unanswered, bench.skipped, bench.failures
    )?;
    Ok(bench.exact && caught == controls)
}

impl Bench<'_> {
    /// The run's bucket, made through the native door; against an endpoint, through the HTTP door,
    /// a bucket it already holds for this key as good.
    fn create(&mut self) -> Result<(), Error> {
        if self.cell.is_none() {
            let at = *self
                .listeners
                .first()
                .ok_or_else(|| failed("no listener"))?;
            let mut http = Http::new(at, Rc::clone(&self.signer));
            let path = format!("/{}", http.signer.bucket);
            let answer = self
                .rt
                .block_on(async move {
                    let mut kept = Kept(Vec::new());
                    let request = Request {
                        length: Length::Known(0),
                        ..Request::bare("PUT", &path, "")
                    };
                    let answer = http.ask(&request, &Body::None, false, &mut kept).await?;
                    Ok::<_, Error>((answer.status, String::from_utf8_lossy(&kept.0).into_owned()))
                })
                .map_err(|e| failed(format!("{e:?}")))??;
            return match answer {
                (200, _) => Ok(()),
                (409, text) if text.contains("BucketAlreadyOwnedByYou") => Ok(()),
                (status, text) => Err(failed(format!("the bucket: HTTP {status} {text}"))),
            };
        }
        let settings = self.settings(&self.client_dir, 0)?;
        let made = self
            .rt
            .block_on(async move {
                let mut client = Client::open(settings).await.map_err(failed)?;
                let op = Op::Create {
                    bucket: BUCKET.to_owned(),
                    id: mantle_client::identity().map_err(failed)?,
                };
                client.call(&op, u64::MAX).await.map_err(failed)
            })
            .map_err(|e| failed(format!("{e:?}")))??;
        if made.status == Status::Done {
            Ok(())
        } else {
            Err(failed(format!("the bucket: {:?}", made.status)))
        }
    }

    /// The three controls through `door`, each a fault the checks must report as a failure: a byte
    /// flipped on its way to the check, a key never put, and an ETag expected of another object.
    /// The controls caught.
    fn controls(&mut self, out: &mut impl Write, door: Door) -> Result<u32, Error> {
        let mut held = self.hold(door, 1)?;
        let logical = held.pop().ok_or_else(|| failed("no connection"))?;
        let generator = Rc::clone(&self.generator);
        let mut piece = vec![0u8; mantle_client::PIECE];
        let other = md5(
            &generator.object(&control_key(door, 'b')).map_err(failed)?,
            CONTROL,
            &mut piece,
        )?;
        let work = Rc::new(self.work(
            door,
            Kind::Put,
            Keys::Sized {
                size: CONTROL,
                kind: 'c',
                made: None,
            },
        )?);
        let flip = CONTROL.checked_div(2).unwrap_or(0);
        let (logical, said) = self
            .rt
            .block_on(async move {
                let mut logical = logical;
                // Each object put as every operation is, asked again where throttled or lost
                // (`Work::again_after`); one never put leaves this door's controls unrun.
                for (j, name) in [(0u64, 'a'), (1, 'b')] {
                    let key = control_key(door, name);
                    let object = generator.object(&key).map_err(failed)?;
                    let id = mantle_client::identity().map_err(failed)?;
                    let mut again = 0u32;
                    let put = loop {
                        let Logical { conn, scratch, .. } = &mut logical;
                        let came = match conn {
                            Conn::Native(client) => {
                                work.native(client, (u64::MAX, &key, CONTROL, id), &object, scratch)
                                    .await?
                            }
                            Conn::Http(http) => {
                                work.http(http, (u64::MAX, &key, CONTROL), &object, scratch)
                                    .await?
                            }
                        };
                        if !work.again_after(&came, j, &mut again).await? {
                            break work.settled(came, again);
                        }
                    };
                    let why = match put {
                        Came::Done => continue,
                        Came::Failed(why)
                        | Came::Refused(why)
                        | Came::Differed(why)
                        | Came::Unanswered(why)
                        | Came::Lost(why) => why,
                        Came::Slowed => "SlowDown".to_owned(),
                        Came::Skipped => "not asked".to_owned(),
                    };
                    let unput = format!("its object {key} was not put: {why}");
                    return Ok::<_, Error>((logical, Err(unput)));
                }
                let key = control_key(door, 'a');
                let object = generator.object(&key).map_err(failed)?;
                let Logical { conn, scratch, .. } = &mut logical;
                let flipped = read(conn, &key, (&object, scratch), Some(flip)).await?;
                let never = generator.object(NEVER_PUT).map_err(failed)?;
                let missing = read(conn, NEVER_PUT, (&never, scratch), None).await?;
                let etag = head_etag(conn, &key).await?;
                Ok::<_, Error>((logical, Ok([flipped, missing, etag])))
            })
            .map_err(|e| failed(format!("{e:?}")))??;
        if let Some(pool) = self.pools.get_mut(door.at()) {
            pool.push(logical);
        }
        // Unrun, none of this door's controls is caught, so the run is not exact.
        let [flipped, missing, etag] = match said {
            Ok(said) => said,
            Err(why) => {
                writeln!(out, "doors control {} NOT run: {why}", self.named(door))?;
                return Ok(0);
            }
        };
        let expected = checksum::etag(&other);
        let judged = [
            (
                "flipped-byte",
                matches!(flipped, Read::Wrong(at) if at == flip),
                format!("{flipped}, the byte at {flip} flipped"),
            ),
            (
                "missing-object",
                matches!(missing, Read::Missing),
                format!("{missing}, a key never put"),
            ),
            (
                "wrong-etag",
                matches!(&etag, Read::Etag(got) if *got != expected),
                format!("{etag}, {expected} expected"),
            ),
        ];
        let mut caught = 0u32;
        for (name, failure, why) in judged {
            if failure {
                caught = caught.saturating_add(1);
                writeln!(
                    out,
                    "doors control {} {name} failed: {why} (caught)",
                    self.named(door)
                )?;
            } else {
                writeln!(
                    out,
                    "doors control {} {name} NOT caught: {why}",
                    self.named(door)
                )?;
            }
        }
        Ok(caught)
    }

    /// Each connection's cost through `door` (measurement.md §10): what the servers and this
    /// thread hold at two connection counts, over the difference ([`SLOPE_LOW`] and
    /// [`SLOPE_HIGH`] through the S3 listener; a client a node and [`SLOPE_HIGH`] within what the
    /// cell admits through the native door), and the handshake's counts, each connection opened and its first
    /// request answered.
    fn slope(&mut self, out: &mut impl Write, door: Door) -> Result<(), Error> {
        let held = self
            .pools
            .get_mut(door.at())
            .map(std::mem::take)
            .unwrap_or_default();
        self.release(held)?;
        // The native door's points: a client on each node, and the S3 door's higher point within
        // what the cell admits, which the nodes derive from their memory and may be thousands, past
        // the clients this harness's runtime holds.
        let (low_count, high_count) = match door {
            Door::Native => (cell::FOUNDERS, SLOPE_HIGH.min(self.native_most)),
            Door::Http => (SLOPE_LOW, SLOPE_HIGH),
        };
        if high_count <= low_count {
            writeln!(
                out,
                "doors {} slope {}: the cell admits {high_count} connections, no second point",
                self.named(door),
                self.options.condition
            )?;
            return Ok(());
        }
        let low = self.open(door, low_count)?;
        let before = self.servers()?;
        alloc::begin();
        let more = self.open(door, high_count.saturating_sub(low_count))?;
        let total = alloc::end();
        // The buffers each logical client is made with are the harness's own, set aside: what the
        // doors' connections hold on this side is the rest.
        let aside = alloc::read_aside();
        let client = total.less(&aside);
        let client_live = total.live.saturating_sub(aside.live);
        let after = self.servers()?;
        let mut took = Histogram::new();
        for (_, ns) in &more {
            took.record(*ns);
        }
        let added = u64::try_from(more.len()).unwrap_or(u64::MAX);
        let footprint = |s: &Servers| {
            summed(
                s.usage
                    .iter()
                    .map(|u| u.as_ref().and_then(|u| u.footprint).map(|f| f as f64)),
            )
        };
        let footprint = footprint(&after)
            .zip(footprint(&before))
            .map(|(a, b)| a - b);
        let live = summed(after.counts.iter().map(|c| c.live.map(|l| l as f64)));
        let counted = |f: fn(&Counted) -> Option<u64>| {
            summed(after.counts.iter().map(|c| f(c).map(|v| v as f64)))
        };
        let mine = |v: f64| if self.counting { Some(v) } else { None };
        writeln!(
            out,
            "doors {} slope {} connections {low_count} {high_count} server_live_bytes/connection {} server_footprint_bytes/connection {} client_live_bytes/connection {}",
            self.named(door),
            self.options.condition,
            per(live, added),
            per(footprint, added),
            per(mine(client_live as f64), added),
        )?;
        writeln!(
            out,
            "doors {} handshake {} handshakes {added} allocs/handshake {} bytes/handshake {} client_allocs/handshake {} client_bytes/handshake {} latency_us p50 {} max {}",
            self.named(door),
            self.options.condition,
            per(counted(|c| c.allocations), added),
            per(counted(|c| c.bytes), added),
            per(mine(client.allocations as f64), added),
            per(mine(client.bytes as f64), added),
            quantile(&took, Level::P50, 500_000),
            micros(took.max()),
        )?;
        let all = low
            .into_iter()
            .chain(more)
            .map(|(logical, _)| logical)
            .collect();
        self.release(all)
    }
}

/// A server's faults as Linux's `/proc/PID/stat` counts them for the whole process (proc(5):
/// `minflt` and `majflt`, its 10th and 12th fields), for a process this program did not start and
/// cannot ask; none where there is no such file (off Linux). Its switches are not read: `status`
/// counts them per thread, and a thread that ended takes its own with it.
fn faults_of(pid: u32) -> Counted {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    // The fields past the command's closing parenthesis, the 3rd (the state) first.
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    let field = |n: usize| fields.get(n.checked_sub(3)?)?.parse::<u64>().ok();
    Counted {
        minor: field(10),
        major: field(12),
        ..Counted::default()
    }
}

/// `perf stat` counting the servers' user-space instructions and cycles across one phase
/// (`--perf`, Linux; perf-stat(1)): started on the servers' pids with its events disabled
/// (`--delay -1`), enabled before the phase and disabled after it through its control descriptors,
/// its standard input and output (`--control fd:0,1`), each command acknowledged before going on,
/// so its count spans the phase; then ended with SIGINT, on which it writes its counts to `out`.
/// The events count user space only (`:u`), which `perf_event_paranoid` 2 lets a user count of
/// its own processes.
struct Perf {
    child: Child,
    ack: BufReader<ChildStdout>,
    out: PathBuf,
}

impl Perf {
    fn start(pids: &[u32], out: &Path) -> Result<Self, Error> {
        let pids: Vec<String> = pids.iter().map(u32::to_string).collect();
        let mut child = Command::new("perf")
            .args(["stat", "-x,", "-e", "instructions:u,cycles:u", "-D", "-1"])
            .args(["--control", "fd:0,1", "-o"])
            .arg(out)
            .args(["-p", &pids.join(",")])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| failed(format!("perf: {e}")))?;
        let ack = BufReader::new(child.stdout.take().ok_or_else(|| failed("perf's output"))?);
        let mut perf = Self {
            child,
            ack,
            out: out.to_owned(),
        };
        perf.command("enable")?;
        Ok(perf)
    }

    /// Sends a control command and reads its acknowledgement: `ack`, which perf may precede with a
    /// NUL. Its output ending, perf ended.
    fn command(&mut self, command: &str) -> Result<(), Error> {
        let control = self
            .child
            .stdin
            .as_mut()
            .ok_or_else(|| failed("perf's input"))?;
        writeln!(control, "{command}")?;
        control.flush()?;
        let mut line = String::new();
        self.ack.read_line(&mut line)?;
        if line.trim_matches(['\0', '\n']) == "ack" {
            Ok(())
        } else {
            Err(failed(format!("perf answered {command} with {line:?}")))
        }
    }

    /// Disables the events, ends perf with SIGINT (by kill(1), this program holding no signal
    /// call) and reads its instructions and cycles.
    fn end(mut self) -> Result<Counters, Error> {
        self.command("disable")?;
        Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status()?;
        self.child.wait()?;
        let text = std::fs::read_to_string(&self.out)?;
        // A CSV line: the count, its unit, the event, ...; `<not counted>` where none ran.
        let count = |event: &str| {
            text.lines().find_map(|line| {
                let mut fields = line.split(',');
                let value = fields.next()?;
                (fields.nth(1)? == event).then(|| value.parse::<u64>().ok())?
            })
        };
        Ok(Counters {
            instructions: count("instructions:u"),
            cycles: count("cycles:u"),
        })
    }
}

/// The servers' user-space instructions and cycles across a phase, as `perf stat` counted them.
struct Counters {
    instructions: Option<u64>,
    cycles: Option<u64>,
}

fn control_key(door: Door, name: char) -> String {
    format!("control/{}/{name}", door.name())
}

/// What a control's read came to.
enum Read {
    Done,
    Wrong(u64),
    Missing,
    Etag(String),
    Other(String),
}

impl std::fmt::Display for Read {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Done => f.write_str("read whole and right"),
            Self::Wrong(at) => write!(f, "byte {at} wrong"),
            Self::Missing => f.write_str("no such key"),
            Self::Etag(etag) => write!(f, "ETag {etag}"),
            Self::Other(why) => f.write_str(why),
        }
    }
}

/// Reads `key` whole through `conn`, checked against `object` a piece of `scratch` at a time, the
/// byte at `flip` inverted on its way to the check where given.
async fn read(
    conn: &mut Conn,
    key: &str,
    (object, scratch): (&Object<'_>, &mut [u8]),
    flip: Option<u64>,
) -> Result<Read, Error> {
    let mut sink = Flipped::new(object, scratch, flip.unwrap_or(u64::MAX));
    let found = match conn {
        Conn::Native(client) => {
            let op = Op::Get {
                bucket: BUCKET.to_owned(),
                key: key.to_owned(),
                headers: Headers::default(),
                version: None,
            };
            let reply = client.get(&op, &mut sink, u64::MAX).await.map_err(failed)?;
            match reply.status {
                Status::Done => None,
                Status::NoSuchKey => Some(Read::Missing),
                other => Some(Read::Other(format!("{other:?}"))),
            }
        }
        Conn::Http(http) => {
            let path = format!("/{}/{key}", http.signer.bucket);
            // A refusal's body goes to the check as well, which judges only a 200's.
            let answer = http
                .ask(
                    &Request::bare("GET", &path, ""),
                    &Body::None,
                    false,
                    &mut sink,
                )
                .await?;
            match answer.status {
                200 => None,
                404 => Some(Read::Missing),
                other => Some(Read::Other(format!("HTTP {other}"))),
            }
        }
    };
    Ok(found.unwrap_or(match sink.checked.wrong {
        Some(at) => Read::Wrong(at),
        None => Read::Done,
    }))
}

/// The ETag `key` is answered with on a HEAD through `conn`.
async fn head_etag(conn: &mut Conn, key: &str) -> Result<Read, Error> {
    Ok(match conn {
        Conn::Native(client) => {
            let op = Op::Head {
                bucket: BUCKET.to_owned(),
                key: key.to_owned(),
                headers: Headers::default(),
                version: None,
            };
            let reply = client.call(&op, u64::MAX).await.map_err(failed)?;
            if reply.status == Status::Done {
                Read::Etag(reply.etag)
            } else {
                Read::Other(format!("{:?}", reply.status))
            }
        }
        Conn::Http(http) => {
            let path = format!("/{}/{key}", http.signer.bucket);
            let answer = http
                .ask(
                    &Request::bare("HEAD", &path, ""),
                    &Body::None,
                    true,
                    &mut Kept(Vec::new()),
                )
                .await?;
            match answer.etag {
                Some(etag) if answer.status == 200 => Read::Etag(etag),
                _ => Read::Other(format!("HTTP {}", answer.status)),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A seed and a name make the same bytes each time, and another name other bytes: what every
    /// read is checked against. The bytes are the ones AES-256-CTR makes under the key and counter
    /// the generator derives (research/76), as another implementation made them (LibreSSL 3.3.6's
    /// `openssl enc -aes-256-ctr` over zeros, its key SHA-256 of the key domain and the seed's eight
    /// big-endian octets, its IV the first 16 octets of SHA-256 of the counter domain and the
    /// name), so every target is held to the same bytes, and a name longer than any S3 key is
    /// refused.
    #[test]
    fn the_generator_gives_a_seed_and_names_bytes_again() {
        let (a, b) = (Generator::new(5).unwrap(), Generator::new(5).unwrap());
        let mut one = vec![0u8; 70_000];
        let mut two = vec![0u8; 70_000];
        a.object("native/65536/p3")
            .unwrap()
            .fill(0, &mut one)
            .unwrap();
        b.object("native/65536/p3")
            .unwrap()
            .fill(0, &mut two)
            .unwrap();
        assert_eq!(one, two);
        let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(hex(&one[..16]), "40104471352f49cbc6b0cda094431a47");
        let mut within = [0u8; 20];
        a.object("native/65536/p3")
            .unwrap()
            .fill(65_530, &mut within)
            .unwrap();
        assert_eq!(hex(&within), "4c106b5c6beafe5c3e6b9e4cee828c49c4c64a61");
        b.object("native/65536/p4")
            .unwrap()
            .fill(0, &mut two)
            .unwrap();
        assert_ne!(one, two);
        let mut piece = vec![0u8; 4096];
        let object = a.object("native/65536/p3").unwrap();
        let whole = md5(&object, 70_000, &mut piece).unwrap();
        assert_eq!(hex(&whole), "d3cd765e733efc102a00af6c9ddaa06c");
        assert_eq!(whole, md5(&object, 70_000, &mut vec![0u8; 999]).unwrap());
        let longest = "k".repeat(mantle_s3::route::MAX_KEY);
        assert!(a.object(&longest).is_ok());
        assert!(matches!(
            a.object(&format!("{longest}k")),
            Err(crate::transfer::generate::GenError::Name)
        ));
    }

    /// An operation's own work allocates nothing of the harness's, whatever its key: the key is
    /// written into its client's buffer, its object made from the generator on the stack, and its
    /// read checked in its client's scratch, so the harness holds no more for a set of 100,000
    /// objects than for one of 1,000, nor for more clients. Over a size's operations and a
    /// 100,000-object set's, from their first keys to their last and longest, each read checked
    /// whole, nothing is allocated.
    #[test]
    fn an_operations_own_work_allocates_nothing() {
        assert!(alloc::installed(), "the unit tests count allocations");
        let generator = Rc::new(Generator::new(9).unwrap());
        let mut oracle = Oracle::new(100_000).unwrap();
        for i in 0..100_000 {
            oracle.set(i);
        }
        assert_eq!(oracle.acknowledged(), 100_000);
        let work = |keys: Keys| Work {
            door: Door::Native,
            kind: Kind::Get,
            keys,
            generator: Rc::clone(&generator),
            etags: Rc::new(Vec::new()),
            uploads: Rc::new(RefCell::new(Vec::new())),
            oracle: Rc::new(RefCell::new(oracle.clone())),
            listing: Rc::new(RefCell::new(Paging::default())),
            again: std::cell::Cell::new(0),
        };
        let sized = work(Keys::Sized {
            size: 1_000,
            kind: 'p',
            made: Some(1_000_000),
        });
        let set = work(Keys::Set {
            count: 100_000,
            own: Door::Http,
        });
        let mut key = String::with_capacity(mantle_s3::route::MAX_KEY);
        let mut scratch = vec![0u8; mantle_client::PIECE];
        // A set's objects are below S3's small-object line, 128 KiB (`transfer::size`).
        let mut bytes = vec![0u8; 128 << 10];
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        alloc::begin();
        for j in (0..100u64).chain(49_900..50_000) {
            for work in [&sized, &set] {
                let (_, length) = work.key(j, &mut key).unwrap().unwrap();
                let object = work.generator.object(&key).unwrap();
                let read = bytes.get_mut(..usize::try_from(length).unwrap()).unwrap();
                object.fill(0, read).unwrap();
                let mut checked = Checked::new(&object, &mut scratch);
                let wrote = std::pin::pin!(checked.write(read)).poll(&mut context);
                assert!(matches!(wrote, std::task::Poll::Ready(Ok(()))));
                assert_eq!((checked.wrong, checked.len()), (None, length));
            }
        }
        let counted = alloc::end();
        assert_eq!(
            (counted.allocations, counted.reallocations, counted.bytes),
            (0, 0, 0)
        );
        assert_eq!(key, "set100000/99999");
    }

    /// A latency histogram holds its fixed buckets whatever it records: recording allocates
    /// nothing, its quantiles stay within the buckets' one-sided 2^-5, and a value past its last
    /// bucket is held as its maximum.
    #[test]
    fn a_histogram_holds_no_more_than_its_buckets() {
        assert!(alloc::installed(), "the unit tests count allocations");
        let mut latency = Histogram::new();
        let size = std::mem::size_of_val(&latency);
        alloc::begin();
        let mut value = 1u64;
        for _ in 0..1_000_000 {
            latency.record(value);
            value = value
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407)
                >> 20;
        }
        latency.record(u64::MAX);
        let counted = alloc::end();
        assert_eq!(counted.allocations, 0);
        assert_eq!(std::mem::size_of_val(&latency), size);
        assert_eq!(latency.count(), 1_000_001);
        assert_eq!(latency.max(), u64::MAX);
        let mut exact = Histogram::new();
        for v in 1..=100_000u64 {
            exact.record(v * 1000);
        }
        let p99 = exact.quantile(990_000) as f64;
        assert!(
            (99_000_000.0..=99_000_000.0 * (1.0 + 1.0 / 32.0)).contains(&p99),
            "{p99}"
        );
    }

    /// An open loop's intended starts are the Poisson arrivals its seed gives, exactly, one per
    /// operation, and none past the phase's operations; closed, an operation starts when taken.
    #[test]
    fn an_open_schedule_is_exact_for_its_seed() {
        let mut schedule = Schedule::new(Load::Open(500.0), 42, (10, 1010), 7).unwrap();
        let mut arrivals = Arrivals::poisson(500.0, 42, 7).unwrap();
        for i in 10..1010 {
            assert_eq!(
                schedule.take(0).unwrap(),
                Some((i, arrivals.next_arrival().unwrap()))
            );
        }
        assert_eq!(schedule.take(0).unwrap(), None);
        let mut again = Schedule::new(Load::Open(500.0), 42, (10, 1010), 7).unwrap();
        assert_eq!(
            again.take(0).unwrap(),
            Some((
                10,
                Arrivals::poisson(500.0, 42, 7)
                    .unwrap()
                    .next_arrival()
                    .unwrap()
            ))
        );
        let mut closed = Schedule::new(Load::Closed, 42, (0, 2), 7).unwrap();
        assert_eq!(closed.take(99).unwrap(), Some((0, 99)));
    }

    /// A closed ladder doubles while each step's interval lies wholly above the best's before it,
    /// and ends three steps after the last that grew: one step that did not grow does not end it,
    /// and the best is the step of greatest median.
    #[test]
    fn a_ladder_ends_three_steps_after_its_last_growth() {
        let step = |clients, median: f64, half: f64| Stepped {
            clients,
            median,
            low: median - half,
            high: median + half,
        };
        let mut climb = Climb {
            best: None,
            flat: 0,
        };
        assert!(climb.after(step(1, 30.0, 1.0), 0));
        assert!(climb.after(step(2, 60.0, 2.0), 0));
        assert!(climb.after(step(4, 59.0, 3.0), 0));
        assert!(climb.after(step(8, 120.0, 5.0), 0));
        assert_eq!(climb.flat, 0);
        assert!(climb.after(step(16, 125.0, 6.0), 0));
        assert!(climb.after(step(32, 118.0, 6.0), 0));
        assert!(!climb.after(step(64, 121.0, 6.0), 0));
        assert_eq!(climb.best.unwrap().clients, 16);
    }

    /// A step that left an operation unanswered ends the ladder at once, however its throughput
    /// grew, and is not its best: the best stays the last step whose every operation was answered.
    #[test]
    fn a_ladder_ends_at_a_step_that_left_an_operation_unanswered() {
        let step = |clients, median: f64| Stepped {
            clients,
            median,
            low: median - 1.0,
            high: median + 1.0,
        };
        let mut climb = Climb {
            best: None,
            flat: 0,
        };
        assert!(climb.after(step(1, 30.0), 0));
        assert!(climb.after(step(2, 60.0), 0));
        let before = climb;
        assert!(!climb.after(step(4, 500.0), 1));
        assert_eq!(climb, before);
        assert_eq!(climb.best.unwrap().clients, 2);
        let mut first = Climb {
            best: None,
            flat: 0,
        };
        assert!(!first.after(step(1, 30.0), 3));
        assert_eq!(first.best, None);
    }

    /// A simulated client's operations for [`phase`], each answered after a turn of the runtime but
    /// the `never`-th, which its node never answers: the client's bounds settle it unanswered
    /// (`Work::settled`), as mantle-client gives up an exchange its node says nothing on.
    struct Unanswering {
        never: u64,
    }

    impl Ask for Unanswering {
        async fn ask(&self, _: &mut Logical, j: u64) -> Result<Came, Error> {
            hyper_rt::futures::yield_now().await;
            Ok(if j == self.never {
                Came::Unanswered("never answered".to_owned())
            } else {
                Came::Done
            })
        }
    }

    /// `count` logical clients that are never dialed, for a phase whose operations are simulated.
    fn idle_clients(count: usize) -> Vec<Logical> {
        let signer = Rc::new(Signer {
            id: String::new(),
            secret: String::new(),
            region: String::new(),
            bucket: String::new(),
        });
        let at = SocketAddr::from(([127, 0, 0, 1], 0));
        (0..count)
            .map(|_| Logical::new(Conn::Http(Box::new(Http::new(at, Rc::clone(&signer))))))
            .collect()
    }

    /// A closed step in rounds whose one operation goes unanswered asks no more past it: the step
    /// ends once the operations its other clients had out end, each client back, far short of the
    /// operations its rounds may ask, the unanswered one counted a failure; a step not in rounds,
    /// an open loop's or the rest a ladder left, asks every operation it was given.
    #[test]
    fn a_step_in_rounds_ends_at_its_first_unanswered_operation_and_counts_it() {
        let clients = 4;
        let never = 50;
        let most = step_ops(clients).unwrap();
        let mut rt = runtime(clients).unwrap();
        let work = Rc::new(Unanswering { never });
        let schedule = (Load::Closed, 1, 0, most);
        let round = Some(round_ops(clients).unwrap());
        let (back, tally, rates) = rt
            .block_on(phase(
                idle_clients(clients),
                Rc::clone(&work),
                schedule,
                round,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(back.len(), clients);
        assert_eq!((tally.unanswered, tally.failed), (1, 1));
        let others = u64::try_from(clients - 1).unwrap();
        assert!(
            (never + 1..=never + 1 + others).contains(&tally.ops),
            "{} of {most}",
            tally.ops
        );
        assert!(rates.len() < ROUNDS.min);
        let first = tally.first[Failure::Unanswered.at()].as_deref();
        assert_eq!(first, Some("never answered"));
        let (back, tally, _) = rt
            .block_on(phase(back, work, schedule, None))
            .unwrap()
            .unwrap();
        assert_eq!(back.len(), clients);
        assert_eq!((tally.ops, tally.unanswered), (most, 1));
    }

    /// A step whose rounds kept none is judged by none, so it falls back to its one throughput.
    #[test]
    fn a_step_that_kept_no_rounds_is_not_judged() {
        assert_eq!(Stepped::of(4, &rounds::judge(&[])), None);
        let judged = rounds::judge(&[10.0; 12]);
        let step = Stepped::of(4, &judged).unwrap();
        assert_eq!(
            (step.clients, step.median, step.low, step.high),
            (4, 10.0, 10.0, 10.0)
        );
    }

    /// A step's rounds keep none of its ramp, each after it its operations over the time since the
    /// last ended; they are enough only from ten kept, and then once their median's interval is
    /// within the precision.
    #[test]
    fn a_steps_rounds_keep_all_but_the_ramp_and_end_once_enough() {
        let size = round_ops(1).unwrap();
        assert_eq!(size, 37);
        assert_eq!(round_ops(32).unwrap(), 320);
        let mut rounds = Rounds::new(size, 0);
        let mut now = 0u64;
        let mut enough = false;
        let mut completions = 0u64;
        while !enough {
            now += 1_000_000;
            completions += 1;
            enough = rounds.completed(now);
        }
        // The ramp's round and ten kept, each a thousand a second.
        assert_eq!(completions, size * 11);
        assert_eq!(rounds.rates.len(), 10);
        assert!(rounds.rates.iter().all(|r| (r - 1000.0).abs() < 1e-6));
    }

    /// A `--load` is open at a positive rate or closed; a `--sizes` entry may be zero.
    #[test]
    fn loads_and_sizes_parse() {
        assert_eq!(load_arg("open:250").unwrap(), Load::Open(250.0));
        assert_eq!(load_arg("closed").unwrap(), Load::Closed);
        assert!(load_arg("open:0").is_err());
        assert!(load_arg("open").is_err());
        assert_eq!(size_arg("0").unwrap(), 0);
        assert_eq!(size_arg("128K").unwrap(), 128 << 10);
        assert_eq!(Work::range(1), (0, 0));
        assert_eq!(Work::range(65_537), (21_845, 43_689));
    }

    /// A phase's write-path columns: each count the servers' growth over the phase, summed, as a
    /// ratio to the hundredth (commands an entry, updates a frame, frames and flushes an
    /// operation); each commit-wait quantile the slowest server's, in microseconds. A dash where a
    /// server answered none (an endpoint's), where the phase grew no entry, frame or operation, or
    /// where a server's waits are too few to resolve the quantile.
    #[test]
    fn a_phases_write_path_is_the_servers_growth_and_the_slowest_logs_waits() {
        let at = |frames, updates, flushes, entries, commands| Counted {
            frames: Some(frames),
            updates: Some(updates),
            flushes: Some(flushes),
            entries: Some(entries),
            commands: Some(commands),
            ..Counted::default()
        };
        let waited = |mut c: Counted, waits, p50, p99, max| {
            c.waits = Some(waits);
            c.wait_p50_ns = p50;
            c.wait_p99_ns = p99;
            c.wait_max_ns = max;
            c
        };
        let before = [
            at(10, 20, 20, 5, 5),
            at(100, 300, 200, 0, 0),
            at(7, 7, 14, 3, 6),
        ];
        // Grown: frames 20 + 40 + 20, updates 50 + 80 + 50, flushes 40 + 80 + 40, entries 10 +
        // 10 + 0 and commands 20 + 20 + 0, over 40 operations.
        let after = [
            waited(
                at(30, 70, 60, 15, 25),
                1_000,
                Some(40_000),
                Some(90_000),
                Some(120_000),
            ),
            waited(
                at(140, 380, 280, 10, 20),
                1_000,
                Some(50_000),
                Some(70_000),
                Some(400_000),
            ),
            waited(
                at(27, 57, 54, 3, 6),
                1_000,
                Some(30_000),
                Some(95_000),
                Some(99_000),
            ),
        ];
        assert_eq!(
            write_path(&before, &after, 40),
            " commands/entry 2.00 updates/frame 2.25 log_frames/op 2.00 log_flushes/op 4.00 \
             log_wait_p50_us 50.0 log_wait_p99_us 95.0 log_wait_max_us 400.0"
        );

        // Ten waits on one server resolve its median, not its 99th percentile.
        let mut few = after.clone();
        few[2].waits = Some(10);
        assert_eq!(
            write_path(&before, &few, 40),
            " commands/entry 2.00 updates/frame 2.25 log_frames/op 2.00 log_flushes/op 4.00 \
             log_wait_p50_us 50.0 log_wait_p99_us - log_wait_max_us 400.0"
        );

        // A phase of reads: no entry, frame, flush or wait grew on any server.
        let idle = before.clone().map(|c| waited(c, 0, None, None, None));
        assert_eq!(
            write_path(&before, &idle, 40),
            " commands/entry - updates/frame - log_frames/op 0.00 log_flushes/op 0.00 \
             log_wait_p50_us - log_wait_p99_us - log_wait_max_us -"
        );
        assert_eq!(
            write_path(&before, &after, 0),
            " commands/entry 2.00 updates/frame 2.25 log_frames/op - log_flushes/op - \
             log_wait_p50_us 50.0 log_wait_p99_us 95.0 log_wait_max_us 400.0"
        );

        // An endpoint's servers, read by pid, answer no write path.
        let pids: [Counted; 3] = std::array::from_fn(|_| Counted::default());
        assert_eq!(
            write_path(&pids, &pids, 40),
            " commands/entry - updates/frame - log_frames/op - log_flushes/op - log_wait_p50_us - \
             log_wait_p99_us - log_wait_max_us -"
        );
    }

    /// A phase's lane columns are each server's growth over it, summed; a dash where a server
    /// answered none.
    #[test]
    fn a_phases_lanes_are_each_servers_growth_summed() {
        let at = |step, other, refused, held| Counted {
            lane_full_step: Some(step),
            lane_full_propose: Some(0),
            lane_full_read: Some(1),
            lane_full_other: Some(other),
            far_refused_proposals: Some(refused),
            far_refused_reads: Some(0),
            stream_held: Some(held),
            ..Counted::default()
        };
        let before = [at(1, 0, 2, 0), at(5, 1, 0, 3), at(0, 0, 0, 0)];
        let after = [at(4, 2, 7, 0), at(5, 1, 1, 4), at(10, 0, 0, 0)];
        assert_eq!(
            lane_columns(&before, &after),
            " lane_full_step 13 lane_full_propose 0 lane_full_read 0 lane_full_other 2 \
             far_refused_proposals 6 far_refused_reads 0 stream_held 1"
        );
        let pids: [Counted; 3] = std::array::from_fn(|_| Counted::default());
        assert_eq!(
            lane_columns(&pids, &pids),
            " lane_full_step - lane_full_propose - lane_full_read - lane_full_other - \
             far_refused_proposals - far_refused_reads - stream_held -"
        );
    }

    /// A phase's long polls are each server's table's growth over it, entry by entry: an entry
    /// grown is its counts' difference, one new to the phase is counted whole, one that grew no
    /// poll is left out, and the longest is the phase's only where it passed the one before. The
    /// columns are the polls an operation, every server's summed, and the label that held the
    /// shards longest in all over its sites and servers, the unlabelled set aside; a dash where a
    /// server answered no table, the phase ran no operation, or no labelled poll grew.
    #[test]
    fn a_phases_long_polls_are_each_tables_growth() {
        let slow = |site: &str, label: &str, (polls, held_ns, longest_ns, task, host)| Slow {
            site: site.to_owned(),
            label: label.to_owned(),
            polls,
            held_ns,
            longest_ns,
            task,
            host,
        };
        let table = |entries: Vec<Slow>| Counted {
            slow: Some(entries),
            ..Counted::default()
        };
        let conn = "crates/node/src/listener/mod.rs:317:17";
        let node = "vendor/hyper-rt/src/runtime.rs:90:5";
        let before = [
            table(vec![
                slow(conn, "PutObject", (4, 4_000, 2_000, 3, 1)),
                slow(node, "Put", (2, 9_000, 6_000, 2, 0)),
                slow(node, "-", (1, 500, 500, 0, 0)),
            ]),
            table(vec![slow(conn, "GetObject", (1, 300, 300, 1, 0))]),
        ];
        let after = [
            table(vec![
                // Grew 6 polls, 50 µs, its longest past the one before.
                slow(conn, "PutObject", (10, 54_000, 30_000, 8, 2)),
                // Grew 1 poll, 1 µs, under the longest before.
                slow(node, "Put", (3, 10_000, 6_000, 3, 0)),
                // Grew nothing.
                slow(node, "-", (1, 500, 500, 0, 0)),
            ]),
            table(vec![
                slow(conn, "GetObject", (1, 300, 300, 1, 0)),
                // New to the phase: 2 polls, 70 µs, unlabelled.
                slow(conn, "-", (2, 70_000, 40_000, 0, 2)),
                // New to the phase on this server, though the other's table holds its label.
                slow(node, "Put", (1, 20_000, 20_000, 1, 0)),
            ]),
        ];
        let grown = slow_grown(&before, &after).unwrap();
        let expected = [
            Grown {
                server: 0,
                site: conn,
                label: "PutObject",
                polls: 6,
                held_ns: 50_000,
                longest_ns: Some(30_000),
                task: 5,
                host: 1,
            },
            Grown {
                server: 0,
                site: node,
                label: "Put",
                polls: 1,
                held_ns: 1_000,
                longest_ns: None,
                task: 1,
                host: 0,
            },
            Grown {
                server: 1,
                site: conn,
                label: "-",
                polls: 2,
                held_ns: 70_000,
                longest_ns: Some(40_000),
                task: 0,
                host: 2,
            },
            Grown {
                server: 1,
                site: node,
                label: "Put",
                polls: 1,
                held_ns: 20_000,
                longest_ns: Some(20_000),
                task: 1,
                host: 0,
            },
        ];
        assert_eq!(grown, expected);
        // 10 polls over 40 operations; PutObject held 50 µs, Put 21 µs, and the unlabelled 70 µs
        // names no label.
        assert_eq!(
            slow_columns(Some(&grown), 40),
            " slow_polls/op 0.25 slowest_label PutObject"
        );
        assert_eq!(
            slow_columns(Some(&grown), 0),
            " slow_polls/op - slowest_label PutObject"
        );

        // A phase that grew no labelled poll, or none at all.
        let quiet = slow_grown(&before, &before).unwrap();
        assert!(quiet.is_empty());
        assert_eq!(
            slow_columns(Some(&quiet), 40),
            " slow_polls/op 0.00 slowest_label -"
        );

        // An endpoint's servers, read by pid, answer no table.
        let pids: [Counted; 2] = std::array::from_fn(|_| Counted::default());
        assert_eq!(slow_grown(&pids, &pids), None);
        assert_eq!(slow_grown(&before, &pids), None);
        assert_eq!(slow_columns(None, 40), " slow_polls/op - slowest_label -");
    }

    /// A phase's stalls are each server's shards' growth over it, shard by shard: a shard grown is
    /// its count's difference, one new to the phase is counted whole, one that grew none or was
    /// gone by the end is left out. The column sums every server's, so a phase says whether any
    /// shard stalled: `0` where none did, a dash where a server answered none.
    #[test]
    fn a_phases_stalls_are_each_shards_growth() {
        let stalls = |shards: Vec<Option<u64>>| Counted {
            stalls: Some(shards),
            ..Counted::default()
        };
        let before = [
            stalls(vec![Some(1), Some(0), Some(4)]),
            stalls(vec![Some(0), Some(2)]),
        ];
        let after = [
            // The first grew none, the second two, the third gone.
            stalls(vec![Some(1), Some(2), None]),
            // The second grew one, and a third new to the phase stalled three times.
            stalls(vec![Some(0), Some(3), Some(3)]),
        ];
        let grown = stalls_grown(&before, &after).unwrap();
        assert_eq!(
            grown,
            [
                Stalled {
                    server: 0,
                    shard: 1,
                    stalls: 2,
                },
                Stalled {
                    server: 1,
                    shard: 1,
                    stalls: 1,
                },
                Stalled {
                    server: 1,
                    shard: 2,
                    stalls: 3,
                },
            ]
        );
        assert_eq!(stall_column(Some(&grown)), " stalls 6");

        // A phase in which no shard stalled.
        let quiet = stalls_grown(&before, &before).unwrap();
        assert!(quiet.is_empty());
        assert_eq!(stall_column(Some(&quiet)), " stalls 0");

        // An endpoint's servers, read by pid, answer no stalls.
        let pids: [Counted; 2] = std::array::from_fn(|_| Counted::default());
        assert_eq!(stalls_grown(&pids, &pids), None);
        assert_eq!(stalls_grown(&before, &pids), None);
        assert_eq!(stall_column(None), " stalls -");
    }

    /// A listing's keys are its set's, each after the one before it as S3 orders keys, byte by
    /// byte (1, 10, 11, 19, 2, 9); a key of another prefix, with a leading zero or a sign, at or
    /// past the set's count, repeated or out of order is wrong; and a ListObjectsV2 document's
    /// keys are read in its order.
    #[test]
    fn a_listing_takes_only_its_sets_keys_in_s3s_order() {
        let prefix = "set20/";
        let mut last = None;
        for key in [
            "set20/0", "set20/1", "set20/10", "set20/11", "set20/19", "set20/2", "set20/9",
        ] {
            last = Some(set_key(prefix, key, 20, last).unwrap());
        }
        assert_eq!(last, Some(9));
        for wrong in [
            "set20/20",
            "set20/007",
            "set20/+1",
            "set20/",
            "set2/1",
            "set20/1a",
        ] {
            assert!(set_key(prefix, wrong, 20, None).is_err(), "{wrong}");
        }
        assert!(set_key(prefix, "set20/1", 20, Some(10)).is_err());
        assert!(set_key(prefix, "set20/3", 20, Some(3)).is_err());
        let mut digits = [0u8; 20];
        assert_eq!(
            decimal(u64::MAX, &mut digits),
            Some(&b"18446744073709551615"[..])
        );
        let xml = "<ListBucketResult><Name>b</Name><Contents><Key>set20/0</Key><Size>1</Size>\
                   </Contents><Contents><Key>set20/1</Key></Contents></ListBucketResult>";
        assert_eq!(keys(xml).collect::<Vec<_>>(), ["set20/0", "set20/1"]);
    }
}
