//! `mantle bench doors`: each operation's cost through both of mantle's doors, the native protocol
//! (mantle-client) and the S3 listener, against a cell of three `mantle serve` processes this
//! program makes and runs (docs/design/measurement.md §12; research/78). The load is mantle's own:
//! the native door's requests are mantle-client's, and the S3 listener's are written here, framed
//! with mantle's HTTP codec where a request's framing is the codec's (an `aws-chunked` body's
//! chunks and trailer; every response body's decoding) and signed with mantle-s3's SigV4. The
//! stock clients stay the correctness suite (`mantle bench transfer`).
//!
//! Every object's bytes are made from the run's seed and the object's name
//! (`transfer::generate`), and every read is checked by making them again; an ETag is checked
//! against the MD5 of those bytes for each phase's first [`ETAGS`] keys. A set's acknowledged
//! objects are a bit each (`transfer::Oracle`). Three controls, a byte flipped on its way to the
//! check, a key never put and an ETag expected of another object, must each be reported as
//! failures, or the run is not exact.
//!
//! A phase is one door, one operation and one size, and says one line: what the three servers
//! counted while it ran, per operation (each answers `counts` on its standard input: its
//! allocator's counts in a `count-allocs` build, its faults and switches in any), what the OS
//! charged each server's process (read by pid), what the harness's own thread allocated beside its
//! own work, which is set aside (`alloc::aside`), and the latency of each operation from its
//! intended start (measurement.md §10). Logical clients are tasks on one hyper-rt shard, each
//! holding one connection, an HTTP connection carrying one request at a time; open loop, their
//! requests arrive as a Poisson process and each waits for a free client; closed, a ladder of
//! clients doubles while throughput grows by [`GROWTH`], up to `--clients`.
//!
//! A measure this build or this OS does not give prints `-`: the allocators' counts outside a
//! `count-allocs` build, instructions and cycles where the OS does not count them per process
//! (Linux: `usage::of` reads none; `--perf` counts them with `perf stat`, [`Perf`]), a major fault
//! count on Windows, and the native door's wire bytes and datagrams, which mantle-client does not
//! say (its endpoint's counters are its own).
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
use std::time::Instant;

use hyper_measure::alloc::{self, Counts};
use hyper_measure::open_loop::Arrivals;
use hyper_measure::quantile::{Level, ranks, samples_for};
use hyper_measure::usage::{self, Usage};
use hyper_rt::combine::TaskSet;
use hyper_rt::futures::{now_ns, sleep};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::tcp::TcpStream;
use mantle_client::{Client, Native, Rewind, Settings, Sink, Source, TRAILER};
use mantle_disk::histogram::Histogram;
use mantle_protocol::object::{Headers, List, Listing, Op, Reply, Status};
use mantle_s3::checksum::{self, Algorithm, Hasher};
use mantle_s3::list::Resume;
use mantle_s3::{crypto, sigv4};

use crate::drive;
use crate::transfer::cell::{self, Cell, Counted};
use crate::transfer::generate::{Generator, Object};
use crate::transfer::native::{Checked, Flipped, Made, Unwatched};
use crate::transfer::{Oracle, size as small_size};

/// The bucket every phase's objects are in.
const BUCKET: &str = "doors";

/// The keys of a phase whose ETags are checked: the first of its keys, each one's MD5 made once
/// before the phase from the generator, so the check holds no table that grows with the phase.
const ETAGS: u64 = 16;

/// Throughput a closed ladder's step must add to the last's for the ladder to double again: a
/// tenth, as measurement.md §10's ladder "doubles while appends a second grow", above the run to
/// run spread its repeated runs resolve (research/11 §16).
const GROWTH: f64 = 0.1;

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
enum Kind {
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
}

/// What an operation came to.
enum Came {
    Done,
    Failed(String),
    /// The server answered with a refusal (an HTTP status past 2xx), or an operation needed what a
    /// refused one would have made: a failure, which against an endpoint may exclude its phase.
    Refused(String),
    /// Refused `SlowDown` (503): asked again as the AWS SDKs ask a throttled request again.
    Slowed,
    /// Its exchange ended unanswered (`ClientError::Lost`): asked again with its identity, which
    /// the node answers from what it made.
    Lost,
}

/// A native reply, or what its exchange came to where it is to be asked again.
fn answered(
    asked: Result<Reply, mantle_client::ClientError>,
) -> Result<Result<Reply, Came>, Error> {
    match asked {
        Ok(reply) if reply.status == Status::SlowDown => Ok(Err(Came::Slowed)),
        Ok(reply) => Ok(Ok(reply)),
        Err(mantle_client::ClientError::Lost { .. }) => Ok(Err(Came::Lost)),
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
fn sized_key(door: Door, size: u64, kind: char, i: u64) -> String {
    format!("{}/{size}/{kind}{i}", door.name())
}

fn set_key(count: u64, i: u64) -> String {
    format!("set{count}/{i}")
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

/// A listing's place: where its next page starts, whether it ended, and the keys it listed.
#[derive(Clone, Default)]
struct Paging {
    next: Option<Next>,
    ended: bool,
    listed: u64,
}

/// Where the next page of a listing starts: the native door's key, the S3 listener's token.
#[derive(Clone)]
enum Next {
    Native(Resume),
    Token(String),
}

/// What a size's phases share: the ETags their objects' bytes give, and their uploads.
type Common<'a> = (&'a Rc<Vec<[u8; 16]>>, &'a Rc<RefCell<Vec<Upload>>>);

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

/// A connection to an S3 listener, a request at a time.
struct Http {
    at: SocketAddr,
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

    /// A request's head for the listener at `at`, signed now, `payload` as
    /// `x-amz-content-sha256` names its body (05 §1.2).
    fn head(
        &self,
        at: SocketAddr,
        (method, path, query): (&str, &str, &str),
        headers: &[(&str, String)],
        payload: &str,
    ) -> Result<Vec<u8>, Error> {
        let (stamp, date) = crate::bench_s3::timestamp().map_err(failed)?;
        let mut all: Vec<(&str, String)> = vec![
            ("host", at.to_string()),
            ("x-amz-date", stamp.clone()),
            ("x-amz-content-sha256", payload.to_owned()),
        ];
        all.extend(headers.iter().cloned());
        let mut signed: Vec<&str> = all
            .iter()
            .map(|(n, _)| *n)
            .filter(|n| *n != "content-length" && *n != "range")
            .collect();
        signed.sort_unstable();
        let pairs: Vec<(&str, &str)> = all.iter().map(|(n, v)| (*n, v.as_str())).collect();
        let request = sigv4::Request {
            method,
            path,
            query,
            headers: &pairs,
        };
        let canonical = sigv4::canonical_request(&request, &signed, payload, None)
            .map_err(|e| failed(format!("{e:?}")))?;
        let signature = sigv4::sign(&self.secret, &stamp, &date, &self.region, &canonical)
            .map_err(|e| failed(format!("{e:?}")))?;
        let target = if query.is_empty() {
            path.to_owned()
        } else {
            format!("{path}?{query}")
        };
        let mut text = format!("{method} {target} HTTP/1.1\r\n");
        for (name, value) in &all {
            text.push_str(&format!("{name}: {value}\r\n"));
        }
        text.push_str(&format!(
            "authorization: {} Credential={}/{}, SignedHeaders={}, Signature={}\r\n\r\n",
            sigv4::ALGORITHM,
            self.id,
            sigv4::scope(&date, &self.region, "s3"),
            signed.join(";"),
            sigv4::hex(&signature)
        ));
        Ok(text.into_bytes())
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
        stream
            .write_all(bytes)
            .await
            .map_err(|e| failed(format!("writing: {e:?}")))?;
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
        let n = stream
            .read(into)
            .await
            .map_err(|e| failed(format!("reading: {e:?}")))?;
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

    /// Sends a request and reads its response, its body into `sink`: once again on a connection
    /// opened anew where the listener had let the last one go before answering (a quiet
    /// connection's end, node.md §4.1), which its octets so far tell.
    async fn ask(
        &mut self,
        head: &[u8],
        body: &Body<'_>,
        bodiless: bool,
        sink: &mut impl Sink,
    ) -> Result<Answer, Error> {
        match self.ask_once(head, body, bodiless, sink).await {
            Ok(answer) => Ok(answer),
            Err(Attempt::Failed(e)) => Err(e),
            Err(Attempt::Again(_)) => {
                self.stream = None;
                self.ask_once(head, body, bodiless, sink)
                    .await
                    .map_err(|(Attempt::Again(e) | Attempt::Failed(e))| e)
            }
        }
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
        let text = String::from_utf8_lossy(self.read.get(..end).unwrap_or_default()).into_owned();
        self.take(end);
        let status: u16 = text
            .split(' ')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| Attempt::Failed(failed(format!("a status line: {text}"))))?;
        let field = |name: &str| {
            text.lines().find_map(|l| {
                let (n, v) = l.split_once(':')?;
                n.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
            })
        };
        let length = field("content-length").and_then(|v| v.parse::<u64>().ok());
        let chunked = field("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
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
            etag: field("etag").map(|e| e.trim_matches('"').to_owned()),
            length,
        })
    }

    async fn send(&mut self, body: &Body<'_>) -> Result<(), Error> {
        match body {
            Body::None => Ok(()),
            Body::Text(text) => self.write(text).await,
            Body::Made(object, from, to) => {
                let mut at = *from;
                let mut piece = std::mem::take(&mut self.piece);
                while at < *to {
                    let n = usize::try_from(to.saturating_sub(at))
                        .unwrap_or(usize::MAX)
                        .min(mantle_client::PIECE);
                    harness(|| {
                        piece.resize(n, 0);
                        object.fill(at, &mut piece)
                    })
                    .map_err(failed)?;
                    self.write(&piece).await?;
                    at = at.saturating_add(u64::try_from(n).map_err(failed)?);
                }
                self.piece = piece;
                Ok(())
            }
            Body::Chunked(object, length) => {
                let mut crc = Hasher::new(Algorithm::Crc32).map_err(failed)?;
                let mut piece = std::mem::take(&mut self.piece);
                let mut at = 0u64;
                while at < *length {
                    let n =
                        usize::try_from(length.saturating_sub(at).min(CHUNK)).map_err(failed)?;
                    piece.clear();
                    mantle_http::write::chunk(&mut piece, n);
                    let start = piece.len();
                    harness(|| {
                        piece.resize(start.saturating_add(n), 0);
                        object.fill(at, piece.get_mut(start..).unwrap_or_default())
                    })
                    .map_err(failed)?;
                    crc.update(piece.get(start..).unwrap_or_default())
                        .map_err(failed)?;
                    piece.extend_from_slice(mantle_http::write::CHUNK_END);
                    self.write(&piece).await?;
                    at = at.saturating_add(u64::try_from(n).map_err(failed)?);
                }
                let value = crc.finish().map_err(failed)?.to_base64();
                piece.clear();
                mantle_http::write::last(
                    &mut piece,
                    &[(Algorithm::Crc32.header(), value.as_bytes())],
                )
                .map_err(failed)?;
                self.write(&piece).await?;
                self.piece = piece;
                Ok(())
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
    first: Option<String>,
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

/// What a phase's tasks share: its schedule and its tally.
struct Shared {
    schedule: Schedule,
    tally: Tally,
    error: Option<Error>,
    done: Vec<Conn>,
}

/// One client's part of a phase: operations taken from the schedule until it ends, each at its
/// intended start, its latency from then.
async fn client(mut conn: Conn, work: Rc<Work>, shared: Rc<RefCell<Shared>>) {
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
        let came = work.run(&mut conn, j).await;
        let took = now_ns().saturating_sub(intended);
        let stop = harness(|| {
            let mut shared = shared.borrow_mut();
            let tally = &mut shared.tally;
            tally.latency.record(took);
            tally.lateness.record(started.saturating_sub(intended));
            tally.ops = tally.ops.saturating_add(1);
            match came {
                Ok(Came::Done) => false,
                Ok(Came::Failed(why)) => {
                    tally.failed = tally.failed.saturating_add(1);
                    tally.first.get_or_insert(why);
                    false
                }
                Ok(Came::Refused(why)) => {
                    tally.failed = tally.failed.saturating_add(1);
                    tally.refused = tally.refused.saturating_add(1);
                    tally.first.get_or_insert(why);
                    false
                }
                Ok(Came::Slowed | Came::Lost) => {
                    tally.failed = tally.failed.saturating_add(1);
                    tally
                        .first
                        .get_or_insert_with(|| "slowed or unanswered".to_owned());
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
    harness(|| shared.borrow_mut().done.push(conn));
}

/// A phase's operations `from..end` on `conns`, a task each: the connections back, and the tally.
async fn phase(
    conns: Vec<Conn>,
    work: Rc<Work>,
    schedule: (Load, u64, u64, u64),
) -> Result<(Vec<Conn>, Tally), Error> {
    let (load, seed, from, end) = schedule;
    let clients = conns.len();
    let shared = harness(|| {
        Ok::<_, Error>(Rc::new(RefCell::new(Shared {
            schedule: Schedule::new(load, seed, (from, end), now_ns())?,
            tally: Tally {
                latency: Histogram::new(),
                lateness: Histogram::new(),
                ops: 0,
                failed: 0,
                refused: 0,
                first: None,
            },
            error: None,
            done: Vec::with_capacity(clients),
        })))
    })?;
    let mut tasks = TaskSet::new(clients).map_err(|e| failed(format!("{e:?}")))?;
    for conn in conns {
        tasks
            .spawn(client(conn, Rc::clone(&work), Rc::clone(&shared)))
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
        None => Ok((shared.done, shared.tally)),
    }
}

impl Work {
    /// Operation `j`'s key, its object's length, and whether it is to be asked: a set's other
    /// door's objects are read only where acknowledged.
    fn key(&self, j: u64) -> Result<Option<(u64, String, u64)>, Error> {
        harness(|| match &self.keys {
            Keys::Sized { size, kind, made } => {
                let i = match made {
                    Some(made) => j
                        .checked_rem(*made)
                        .ok_or_else(|| failed("no objects to read"))?,
                    None => j,
                };
                Ok(Some((i, sized_key(self.door, *size, *kind, i), *size)))
            }
            Keys::Set { count, own } => {
                let i = j
                    .checked_mul(2)
                    .and_then(|i| i.checked_add(u64::try_from(own.at()).ok()?))
                    .ok_or_else(|| failed("a set's index"))?;
                if i >= *count || (*own != self.door && !self.oracle.borrow().has(i)) {
                    return Ok(None);
                }
                let sizes = self.generator.object("sizes").map_err(failed)?;
                Ok(Some((
                    i,
                    set_key(*count, i),
                    small_size(&sizes, i).map_err(failed)?,
                )))
            }
        })
    }

    /// The ETag object `i`'s bytes have where the phase knows it.
    fn etag(&self, i: u64) -> Option<[u8; 16]> {
        usize::try_from(i)
            .ok()
            .and_then(|i| self.etags.get(i).copied())
    }

    async fn run(&self, conn: &mut Conn, j: u64) -> Result<Came, Error> {
        if self.kind == Kind::List {
            return self.list(conn).await;
        }
        let Some((i, key, length)) = self.key(j)? else {
            return Ok(Came::Done);
        };
        let object = harness(|| self.generator.object(&key)).map_err(failed)?;
        let id = mantle_client::identity().map_err(failed)?;
        let mut again = 0u32;
        let came = loop {
            let came = match conn {
                Conn::Native(client) => self.native(client, (i, &key, length, id), &object).await?,
                Conn::Http(http) => self.http(http, (i, &key, length), &object).await?,
            };
            let retry = matches!(came, Came::Slowed | Came::Lost);
            if !retry || again.saturating_add(1) >= mantle_client::ATTEMPTS {
                break came;
            }
            again = again.saturating_add(1);
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
                    again,
                );
                sleep(u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX))
                    .await
                    .map_err(|e| failed(format!("{e:?}")))?;
            }
        };
        harness(|| {
            self.again
                .set(self.again.get().saturating_add(u64::from(again)))
        });
        if matches!(came, Came::Done)
            && self.kind == Kind::Put
            && matches!(self.keys, Keys::Set { .. })
        {
            harness(|| self.oracle.borrow_mut().set(i));
        }
        Ok(match came {
            Came::Slowed => {
                Came::Failed(format!("SlowDown, asked {} times", again.saturating_add(1)))
            }
            Came::Lost => Came::Failed(format!(
                "unanswered, asked {} times",
                again.saturating_add(1)
            )),
            came => came,
        })
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
            Some(at) => Came::Failed(format!("byte {at} not the generator's")),
            None if took != length => Came::Failed(format!("{took} bytes, not {length}")),
            None => Came::Done,
        }
    }

    async fn native(
        &self,
        client: &mut Client,
        (i, key, length, id): (u64, &str, u64, u128),
        object: &Object<'_>,
    ) -> Result<Came, Error> {
        let bucket = BUCKET.to_owned();
        let refused = |reply: &Reply| Came::Failed(format!("{:?}", reply.status));
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
                };
                let mut sink = Aside(Checked::starting(object, mantle_client::PIECE, from));
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
                    return Ok(Came::Refused("no upload created".into()));
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
                    return Ok(Came::Refused("no upload created".into()));
                };
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
                    return Ok(Came::Refused("no part uploaded".into()));
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
                    return Ok(Came::Refused("no upload created".into()));
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
    ) -> Result<Came, Error> {
        let at = http.at;
        let signer = Rc::clone(&http.signer);
        let path = format!("/{}/{key}", signer.bucket);
        let unsigned = "UNSIGNED-PAYLOAD";
        // A refusal says the server's answer: its status and its error's code and message.
        let refused = |answer: &Answer, body: &Kept| {
            if answer.status == 503 {
                Came::Slowed
            } else {
                let text = String::from_utf8_lossy(&body.0);
                Came::Refused(format!(
                    "HTTP {} {} {}",
                    answer.status,
                    element(&text, "Code").unwrap_or_default(),
                    element(&text, "Message").unwrap_or_default()
                ))
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
                    (Kind::Part, None) => return Ok(Came::Refused("no upload created".into())),
                    _ => String::new(),
                };
                let head = signer.head(
                    at,
                    ("PUT", &path, &query),
                    &[("content-length", length.to_string())],
                    unsigned,
                )?;
                let answer = http
                    .ask(&head, &Body::Made(object, 0, length), false, &mut dropped)
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
                let headers = [
                    ("content-encoding", "aws-chunked".to_owned()),
                    ("content-length", encoded.to_string()),
                    ("x-amz-decoded-content-length", length.to_string()),
                    ("x-amz-trailer", Algorithm::Crc32.header().to_owned()),
                ];
                let head = signer.head(
                    at,
                    ("PUT", &path, ""),
                    &headers,
                    "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                )?;
                let answer = http
                    .ask(&head, &Body::Chunked(object, length), false, &mut dropped)
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
                let headers: Vec<(&str, String)> = if self.kind == Kind::Range {
                    vec![("range", format!("bytes={from}-{to}"))]
                } else {
                    Vec::new()
                };
                let head = signer.head(at, ("GET", &path, ""), &headers, sigv4::EMPTY_SHA256)?;
                let mut sink = Aside(Checked::starting(object, mantle_client::PIECE, from));
                let answer = http.ask(&head, &Body::None, false, &mut sink).await?;
                let end = if length == 0 { 0 } else { to.saturating_add(1) };
                let expected = if self.kind == Kind::Range { 206 } else { 200 };
                if answer.status != expected {
                    refused(&answer, &dropped)
                } else {
                    self.checked(sink.0.wrong, sink.0.len(), end)
                }
            }
            Kind::Head => {
                let head = signer.head(at, ("HEAD", &path, ""), &[], sigv4::EMPTY_SHA256)?;
                let answer = http.ask(&head, &Body::None, true, &mut dropped).await?;
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
                    (Kind::Abort, None) => return Ok(Came::Refused("no upload created".into())),
                    _ => String::new(),
                };
                let head = signer.head(at, ("DELETE", &path, &query), &[], sigv4::EMPTY_SHA256)?;
                let answer = http.ask(&head, &Body::None, false, &mut dropped).await?;
                if answer.status != 204 {
                    refused(&answer, &dropped)
                } else {
                    Came::Done
                }
            }
            Kind::Create => {
                let head =
                    signer.head(at, ("POST", &path, "uploads="), &[], sigv4::EMPTY_SHA256)?;
                let mut kept = Kept(Vec::new());
                let answer = http.ask(&head, &Body::None, false, &mut kept).await?;
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
                    return Ok(Came::Refused("no upload created".into()));
                };
                if upload.part.is_none() {
                    return Ok(Came::Refused("no part uploaded".into()));
                }
                let query = format!("uploadId={}", sigv4::uri_encode(upload.id.as_bytes(), true));
                let head = signer.head(at, ("GET", &path, &query), &[], sigv4::EMPTY_SHA256)?;
                let mut kept = Kept(Vec::new());
                let answer = http.ask(&head, &Body::None, false, &mut kept).await?;
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
                    return Ok(Came::Refused("no part uploaded".into()));
                };
                let query = format!("uploadId={}", sigv4::uri_encode(upload.as_bytes(), true));
                let body = format!(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{part}\"</ETag></Part></CompleteMultipartUpload>"
                );
                let head = signer.head(
                    at,
                    ("POST", &path, &query),
                    &[("content-length", body.len().to_string())],
                    unsigned,
                )?;
                let mut kept = Kept(Vec::new());
                let answer = http
                    .ask(&head, &Body::Text(body.as_bytes()), false, &mut kept)
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

    /// The listing's next page, from where the last one ended; its keys are counted, and once it
    /// ends their number is checked against the objects acknowledged.
    async fn list(&self, conn: &mut Conn) -> Result<Came, Error> {
        let Keys::Set { count, .. } = self.keys else {
            return Ok(Came::Failed("a listing of no set".into()));
        };
        let prefix = format!("set{count}/");
        let Paging {
            next: start, ended, ..
        } = self.listing.borrow().clone();
        if ended {
            return Ok(Came::Done);
        }
        let (keys, next) = match conn {
            Conn::Native(client) => {
                let op = Op::List(List {
                    bucket: BUCKET.to_owned(),
                    prefix,
                    delimiter: String::new(),
                    start: match start {
                        Some(Next::Native(resume)) => Some(resume),
                        _ => None,
                    },
                    max_keys: PAGE,
                    fetch_owner: false,
                });
                let reply = match answered(client.call(&op, u64::MAX).await)? {
                    Ok(reply) => reply,
                    Err(came) => return Ok(came),
                };
                match reply.listing {
                    Some(Listing::Keys(page)) => (
                        page.contents.len(),
                        if page.truncated {
                            page.next.map(Next::Native)
                        } else {
                            None
                        },
                    ),
                    _ => return Ok(Came::Failed(format!("{:?}", reply.status))),
                }
            }
            Conn::Http(http) => {
                let token = match &start {
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
                let head = http.signer.head(
                    http.at,
                    ("GET", &format!("/{}", http.signer.bucket), &query),
                    &[],
                    sigv4::EMPTY_SHA256,
                )?;
                let mut kept = Kept(Vec::new());
                let answer = http.ask(&head, &Body::None, false, &mut kept).await?;
                let text = String::from_utf8_lossy(&kept.0);
                if answer.status != 200 {
                    return Ok(Came::Refused(format!(
                        "HTTP {} {}",
                        answer.status,
                        element(&text, "Code").unwrap_or_default()
                    )));
                }
                let next = (element(&text, "IsTruncated") == Some("true"))
                    .then(|| {
                        element(&text, "NextContinuationToken").map(|t| Next::Token(t.to_owned()))
                    })
                    .flatten();
                (text.matches("<Key>").count(), next)
            }
        };
        let keys = u64::try_from(keys).map_err(failed)?;
        let Paging { ended, listed, .. } = harness(|| {
            let mut listing = self.listing.borrow_mut();
            listing.listed = listing.listed.saturating_add(keys);
            listing.ended = next.is_none();
            listing.next = next;
            listing.clone()
        });
        let acknowledged = harness(|| (0..count).filter(|i| self.oracle.borrow().has(*i)).count());
        let acknowledged = u64::try_from(acknowledged).map_err(failed)?;
        Ok(if ended && listed != acknowledged {
            Came::Failed(format!("{listed} keys listed, {acknowledged} acknowledged"))
        } else {
            Came::Done
        })
    }
}

/// The cell, its doors, and the connections each door holds between phases.
struct Bench<'o> {
    options: &'o Options,
    /// The cell this program runs, none against an `--endpoint`.
    cell: Option<Cell>,
    rt: LocalRuntime,
    client_dir: PathBuf,
    listeners: Vec<SocketAddr>,
    signer: Rc<Signer>,
    generator: Rc<Generator>,
    pools: [Vec<Conn>; 2],
    /// The native clients the cell admits at once: each node's `gateway.clients` identities.
    native_most: usize,
    /// Connections each door opened: its handshakes.
    opened: [u64; 2],
    counting: bool,
    exact: bool,
    ops: u64,
    failures: u64,
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
    harness: Option<Counts>,
    generator_ns: u64,
    wire: Option<u64>,
    again: u64,
    /// `perf stat`'s counts across the phase, where `--perf` counted them.
    perf: Option<Counters>,
    excluded: bool,
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
        if self.cell.is_some() {
            &DOORS
        } else {
            &[Door::Http]
        }
    }

    /// A door as its lines name it: `http@ARM` against an endpoint.
    fn named(&self, door: Door) -> String {
        match (door, &self.options.endpoint) {
            (Door::Http, Some(endpoint)) => format!("http@{}", endpoint.arm),
            _ => door.name().to_owned(),
        }
    }

    /// `want` connections of `door` held, more opened as needed, each answered a first request.
    fn hold(&mut self, door: Door, want: usize) -> Result<Vec<Conn>, Error> {
        let pool = self
            .pools
            .get_mut(door.at())
            .ok_or_else(|| failed("a door's pool"))?;
        let mut conns: Vec<Conn> = pool.drain(..want.min(pool.len())).collect();
        let more = want.saturating_sub(conns.len());
        if more > 0 {
            let opened = self.open(door, more)?;
            conns.extend(opened.into_iter().map(|(conn, _)| conn));
        }
        Ok(conns)
    }

    /// `n` connections of `door` opened in turn, each answered a first request (a HEAD of a key
    /// never put): each with its time from its opening to that answer.
    fn open(&mut self, door: Door, n: usize) -> Result<Vec<(Conn, u64)>, Error> {
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
            let opened = self
                .rt
                .block_on(conn.open())
                .map_err(|e| failed(format!("{e:?}")))??;
            if let Some(count) = self.opened.get_mut(door.at()) {
                *count = count.saturating_add(1);
            }
            made.push(opened);
        }
        Ok(made)
    }

    /// The settings of the native client `mantle init client` made in `dir`, asking node `first`
    /// first, its key read from the client's directory.
    fn settings(&self, dir: &Path, first: usize) -> Result<Settings, Error> {
        let configured = crate::client::configured(dir).map_err(failed)?;
        let mut nodes = self.cell()?.quic();
        let first = first.min(nodes.len());
        nodes.rotate_left(first);
        Ok(Settings {
            id: configured.id,
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            credentials: configured.credentials,
            transport_bytes: mantle_client::least_transport(nodes.len()).map_err(failed)?,
            nodes,
        })
    }

    /// One phase on `clients` connections, operations `from..end`, and its line.
    fn step(
        &mut self,
        out: &mut impl Write,
        work: &Rc<Work>,
        (size, clients): (&str, usize),
        (from, end): (u64, u64),
    ) -> Result<(u64, f64), Error> {
        let conns = self.hold(work.door, clients)?;
        let wire_before: u64 = conns
            .iter()
            .map(|c| if let Conn::Http(h) = c { h.wire } else { 0 })
            .sum();
        let opened_before: u64 = conns
            .iter()
            .map(|c| if let Conn::Http(h) = c { h.opened } else { 0 })
            .sum();
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
        alloc::begin();
        let ran = self.rt.block_on(phase(
            conns,
            Rc::clone(work),
            (self.options.load, seed, from, end),
        ));
        let total = alloc::end();
        let aside = alloc::read_aside();
        let generator_ns =
            u64::try_from(drive::cpu().saturating_sub(cpu).as_nanos()).unwrap_or(u64::MAX);
        let elapsed_ns = u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let perf = perf.map(Perf::end).transpose()?;
        let (conns, tally) = ran.map_err(|e| failed(format!("{e:?}")))??;
        let after = self.servers()?;
        let wire = conns
            .iter()
            .map(|c| if let Conn::Http(h) = c { h.wire } else { 0 })
            .sum::<u64>()
            .saturating_sub(wire_before);
        let reopened = conns
            .iter()
            .map(|c| if let Conn::Http(h) = c { h.opened } else { 0 })
            .sum::<u64>()
            .saturating_sub(opened_before);
        if let Some(count) = self.opened.get_mut(work.door.at()) {
            *count = count.saturating_add(reopened);
        }
        if let Some(pool) = self.pools.get_mut(work.door.at()) {
            pool.extend(conns);
        }
        let ops = tally.ops;
        let rate = ops as f64 / (elapsed_ns.max(1) as f64 / 1e9);
        // A server outside this program that refuses every operation of a phase does not offer
        // it: the phase is excluded with the server's answer, not counted as a failed check. A
        // check that failed on an answer it gave is a failure.
        let excluded = self.cell.is_none() && ops > 0 && tally.refused == ops;
        self.ops = self.ops.saturating_add(ops);
        if !excluded {
            self.failures = self.failures.saturating_add(tally.failed);
            self.exact &= tally.failed == 0;
        }
        let measured = Measured {
            door: work.door,
            kind: work.kind,
            size,
            clients,
            elapsed_ns,
            tally,
            harness: self.counting.then(|| total.less(&aside)),
            generator_ns,
            wire: if work.door == Door::Http {
                Some(wire)
            } else {
                None
            },
            again: work.again.replace(0),
            perf,
            excluded,
        };
        self.line(out, &measured, (&before, &after))?;
        Ok((ops, rate))
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
        let grew = |f: fn(&Counted) -> Option<u64>| {
            summed(
                after
                    .counts
                    .iter()
                    .zip(&before.counts)
                    .map(|(a, b)| Some(f(a)?.saturating_sub(f(b)?) as f64)),
            )
        };
        let charged = |f: fn(&Usage) -> Option<u64>| {
            summed(
                after
                    .usage
                    .iter()
                    .zip(&before.usage)
                    .map(|(a, b)| f(&a.as_ref()?.since(b.as_ref()?)).map(|v| v as f64)),
            )
        };
        let peak = summed(after.counts.iter().map(|c| c.peak.map(|p| p.max(0) as f64)));
        let perf = |f: fn(&Counters) -> Option<u64>| m.perf.as_ref().and_then(f).map(|v| v as f64);
        let clients = u64::try_from(m.clients).unwrap_or(u64::MAX);
        let mine = |f: fn(&Counts) -> u64| m.harness.as_ref().map(|c| f(c) as f64);
        let open = matches!(self.options.load, Load::Open(_));
        let t = &m.tally;
        writeln!(
            out,
            "doors {} {} {} {} clients {} elapsed_ms {} ops {ops} allocs/op {} reallocs/op {} moved/op {} bytes/op {} peak_live/request {} peak_live/connection {} \
             client_allocs/op {} client_bytes/op {} minor_faults/op {} major_faults/op {} user_ns/op {} system_ns/op {} instructions/op {} cycles/op {} switches/op {} \
             wire_bytes/op {} datagrams/op - latency_us p50 {} p50_95 {} p99 {} p99.9 {} p99.99 {} max {} generator_cpu_ns/op {} lateness_us p99 {} again {} failed {}",
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
            per(peak, clients),
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
            per(m.wire.map(|w| w as f64), ops),
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
            m.again,
            t.failed,
        )?;
        if let Some(first) = &t.first {
            writeln!(
                out,
                "doors {} {} {} {} {}: {first}",
                self.named(m.door),
                m.kind.name(),
                m.size,
                self.options.condition,
                if m.excluded { "excluded" } else { "failure" },
            )?;
        }
        out.flush()?;
        Ok(())
    }

    /// A phase's steps: open loop, one of `n` operations from `from` on `--clients` clients;
    /// closed, a ladder from one client doubling while throughput grows by [`GROWTH`], each step
    /// `n` operations further on, up to `--clients`. Operations end at `end` where the phase's
    /// keys end; `finish` runs what the ladder left of them at its best step's clients. Says how
    /// far the steps went.
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
            self.step(out, work, (size, budget), (from, end))?;
            return Ok(end);
        }
        let (mut at, mut clients, mut best) = (from, 1usize, (1usize, 0.0f64));
        while at < end {
            let to = at.saturating_add(n).min(end);
            let (_, rate) = self.step(out, work, (size, clients), (at, to))?;
            at = to;
            let grew = rate > best.1 * (1.0 + GROWTH);
            if rate > best.1 {
                best = (clients, rate);
            }
            if !grew || clients >= budget {
                break;
            }
            clients = clients.saturating_mul(2).min(budget);
        }
        if finish && at < end {
            self.step(out, work, (size, best.0), (at, end))?;
            at = end;
        }
        Ok(at)
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

    /// Drops `door`'s connections on the runtime, where a native client's drop tells its node it
    /// closed (QUIC's CONNECTION_CLOSE), so the node frees its identity at once.
    fn release(&mut self, conns: Vec<Conn>) -> Result<(), Error> {
        self.rt
            .block_on(async move { drop(conns) })
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
                    .object(&sized_key(door, size, kind, i))
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
        let work = Rc::new(Work {
            etags: Rc::clone(shared.0),
            uploads: Rc::clone(shared.1),
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
        let keyed = |kind: char, made: Option<u64>| Keys::Sized { size, kind, made };
        let phase = |kind: Kind, keys: Keys| (door, kind, keys);
        let mut made = 0;
        if whole {
            made = self.sized_phase(
                out,
                phase(Kind::Put, keyed('p', None)),
                (&etags, &uploads),
                (&name, 0, n, u64::MAX),
            )?;
        }
        let streamed = self.sized_phase(
            out,
            phase(Kind::Stream, keyed('s', None)),
            (&etags, &uploads),
            (&name, 0, n, u64::MAX),
        )?;
        let (read, made) = if whole { ('p', made) } else { ('s', streamed) };
        for kind in [Kind::Get, Kind::Range, Kind::Head] {
            if kind != Kind::Range || size > 0 {
                self.sized_phase(
                    out,
                    phase(kind, keyed(read, Some(made))),
                    (&etags, &uploads),
                    (&name, 0, n, u64::MAX),
                )?;
            }
        }
        if whole {
            let parts = self.etags(door, (size, 'm'), n)?;
            // A slot for each upload a ladder of creates may make, each create filling its own.
            let steps = match self.options.load {
                Load::Open(_) => 1,
                Load::Closed => u64::from(self.options.clients.max(1).ilog2()).saturating_add(1),
            };
            let most = n.saturating_mul(steps);
            uploads
                .borrow_mut()
                .resize(usize::try_from(most).map_err(failed)?, Upload::default());
            let shared = (&parts, &uploads);
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
            (&etags, &uploads),
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
                    self.step(out, &work, (&name, 1), (0, pages))?;
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
                let head = http.signer.head(
                    http.at,
                    ("HEAD", &format!("/{}/{NEVER_PUT}", http.signer.bucket), ""),
                    &[],
                    sigv4::EMPTY_SHA256,
                )?;
                let answer = http
                    .ask(&head, &Body::None, true, &mut Kept(Vec::new()))
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
/// two for a native client's waits, and the sockets of as many native clients.
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

/// What a `mantle serve` process answers `counts` with ([`mantle_node::process::answer_counts`]):
/// in a `count-allocs` build its allocator's counts across every thread since it was last asked,
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
    format!("counts {counted} minor {minor} major {major} switches {switches}\n")
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
        "doors run seed {} condition {} load {} clients {} ops {} counting {}",
        options.seed,
        options.condition,
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
            let mut cell = Cell::make(&options.dir, options.volume).map_err(failed)?;
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
            (Some(cell), s3.iter().map(|(at, _)| *at).collect(), signer)
        }
    };
    let most = options
        .clients
        .saturating_mul(2)
        .saturating_add(SLOPE_HIGH)
        .saturating_add(1);
    // Each node admits as many native clients at once as its gateway's floor states, so a client
    // past them is refused at admission (node.md §3.6): the native door's clients stay within.
    let native_most = mantle_node::derive::gateway()
        .map_err(failed)?
        .clients
        .saturating_mul(cell::FOUNDERS);
    writeln!(
        out,
        "doors native clients at most {native_most}: each node admits gateway.clients identities at once"
    )?;
    let mut bench = Bench {
        options,
        listeners,
        cell,
        rt: runtime(most)?,
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
        phases: 0,
    };
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
    let controls = u32::try_from(doors.len()).unwrap_or(0).saturating_mul(3);
    writeln!(
        out,
        "doors verified ops {} failed {} controls caught {caught} of {controls}",
        bench.ops, bench.failures
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
            let head = http.signer.head(
                at,
                ("PUT", &format!("/{}", http.signer.bucket), ""),
                &[("content-length", "0".to_owned())],
                sigv4::EMPTY_SHA256,
            )?;
            let answer = self
                .rt
                .block_on(async move {
                    let mut kept = Kept(Vec::new());
                    let answer = http.ask(&head, &Body::None, false, &mut kept).await?;
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
        let mut conns = self.hold(door, 1)?;
        let conn = conns.pop().ok_or_else(|| failed("no connection"))?;
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
        let (conn, said) = self
            .rt
            .block_on(async move {
                let mut conn = conn;
                for name in ['a', 'b'] {
                    let key = control_key(door, name);
                    let object = generator.object(&key).map_err(failed)?;
                    let put = match &mut conn {
                        Conn::Native(client) => {
                            work.native(
                                client,
                                (
                                    u64::MAX,
                                    &key,
                                    CONTROL,
                                    mantle_client::identity().map_err(failed)?,
                                ),
                                &object,
                            )
                            .await?
                        }
                        Conn::Http(http) => {
                            work.http(http, (u64::MAX, &key, CONTROL), &object).await?
                        }
                    };
                    match put {
                        Came::Done => {}
                        Came::Failed(why) | Came::Refused(why) => {
                            return Err(failed(format!("a control's object was not put: {why}")));
                        }
                        Came::Slowed | Came::Lost => {
                            return Err(failed(
                                "a control's object was not put: slowed or unanswered",
                            ));
                        }
                    }
                }
                let key = control_key(door, 'a');
                let object = generator.object(&key).map_err(failed)?;
                let flipped = read(&mut conn, &key, &object, Some(flip)).await?;
                let never = generator.object(NEVER_PUT).map_err(failed)?;
                let missing = read(&mut conn, NEVER_PUT, &never, None).await?;
                let etag = head_etag(&mut conn, &key).await?;
                Ok((conn, [flipped, missing, etag]))
            })
            .map_err(|e| failed(format!("{e:?}")))??;
        if let Some(pool) = self.pools.get_mut(door.at()) {
            pool.push(conn);
        }
        let [flipped, missing, etag] = said;
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
    /// [`SLOPE_HIGH`] through the S3 listener; a client a node and as many as the cell admits
    /// through the native door), and the handshake's counts, each connection opened and its first
    /// request answered.
    fn slope(&mut self, out: &mut impl Write, door: Door) -> Result<(), Error> {
        let held = self
            .pools
            .get_mut(door.at())
            .map(std::mem::take)
            .unwrap_or_default();
        self.release(held)?;
        // The native door's points: a client on each node, and as many as the cell admits.
        let (low_count, high_count) = match door {
            Door::Native => (cell::FOUNDERS, self.native_most),
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
        let client = alloc::end();
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
            per(mine(client.live as f64), added),
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
        let all = low.into_iter().chain(more).map(|(conn, _)| conn).collect();
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

/// Reads `key` whole through `conn`, checked against `object`, the byte at `flip` inverted on its
/// way to the check where given.
async fn read(
    conn: &mut Conn,
    key: &str,
    object: &Object<'_>,
    flip: Option<u64>,
) -> Result<Read, Error> {
    let mut sink = Flipped::new(object, mantle_client::PIECE, flip.unwrap_or(u64::MAX));
    let found = match conn {
        Conn::Native(client) => {
            let op = Op::Get {
                bucket: BUCKET.to_owned(),
                key: key.to_owned(),
                headers: Headers::default(),
            };
            let reply = client.get(&op, &mut sink, u64::MAX).await.map_err(failed)?;
            match reply.status {
                Status::Done => None,
                Status::NoSuchKey => Some(Read::Missing),
                other => Some(Read::Other(format!("{other:?}"))),
            }
        }
        Conn::Http(http) => {
            let head = http.signer.head(
                http.at,
                ("GET", &format!("/{}/{key}", http.signer.bucket), ""),
                &[],
                sigv4::EMPTY_SHA256,
            )?;
            // A refusal's body goes to the check as well, which judges only a 200's.
            let answer = http.ask(&head, &Body::None, false, &mut sink).await?;
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
            };
            let reply = client.call(&op, u64::MAX).await.map_err(failed)?;
            if reply.status == Status::Done {
                Read::Etag(reply.etag)
            } else {
                Read::Other(format!("{:?}", reply.status))
            }
        }
        Conn::Http(http) => {
            let head = http.signer.head(
                http.at,
                ("HEAD", &format!("/{}/{key}", http.signer.bucket), ""),
                &[],
                sigv4::EMPTY_SHA256,
            )?;
            let answer = http
                .ask(&head, &Body::None, true, &mut Kept(Vec::new()))
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
    /// read is checked against.
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
        b.object("native/65536/p4")
            .unwrap()
            .fill(0, &mut two)
            .unwrap();
        assert_ne!(one, two);
        let mut piece = vec![0u8; 4096];
        let object = a.object("native/65536/p3").unwrap();
        assert_eq!(
            md5(&object, 70_000, &mut piece).unwrap(),
            md5(&object, 70_000, &mut vec![0u8; 999]).unwrap()
        );
    }

    /// A latency histogram holds its fixed buckets whatever it records: recording allocates
    /// nothing (where this build counts allocations), its quantiles stay within the buckets'
    /// one-sided 2^-5, and a value past its last bucket is held as its maximum.
    #[test]
    fn a_histogram_holds_no_more_than_its_buckets() {
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
}
