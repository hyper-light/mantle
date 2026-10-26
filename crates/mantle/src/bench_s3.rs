//! `mantle bench s3`: the S3 listener's own cost (`mantle_node::listener`; node.md §4), over real
//! sockets on loopback, plain HTTP and TLS 1.3, with the node's object host played by a stub on
//! the door's own interface (`mantle_node::door`): a get's bytes from one object in memory, and a
//! put's body taken and dropped. What a row measures is the listener and the client: the codec,
//! SigV4, the handover through the door, the response, the socket and TLS, never the drivers, the
//! ranges or the volumes behind them.
//!
//! The listener and the stub run on one shard, as a node runs them; the client is a thread of its
//! own on one keep-alive connection, a request at a time. A request's row is round trips: their
//! count, median and 99th percentile. An object's row is the AWS CLI's 8 MiB part moved again and again:
//! bytes a second, its body plain or, for a put, `aws-chunked` with a trailing CRC-32 in botocore's
//! 1 MiB chunks, as boto3 sends one by default.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc;
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hyper_rt::combine::{Either3, race3};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::sync::SyncError;
use hyper_tls::pki_types::{CertificateDer, ServerName};
use hyper_tls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned, version};
use mantle_cert::{EndpointKey, EndpointName, Serial, Validity};
use mantle_node::door::{self, Answer, Doorway, Piece};
use mantle_node::listener::{CONNECTIONS, Listener, PIECE, Settings};
use mantle_node::process::AccessKey;
use mantle_protocol::object::{Op, Reply, Status};
use mantle_s3::checksum::{self, Algorithm};
use mantle_s3::sigv4;

use crate::display;

/// The object an object's row moves: the AWS CLI's part, its default `multipart_chunksize` and
/// `multipart_threshold` of `8MB`, a megabyte being 1024² octets to it (the CLI's S3 configuration
/// reference; `awscli/customizations/s3/utils.py`'s `SIZE_SUFFIX`): what one of its requests
/// streams.
const OBJECT: usize = 8 << 20;

/// A put's `aws-chunked` chunk: botocore's, 1 MiB (`httpchecksum.py`'s `AwsChunkedWrapper`
/// `_DEFAULT_CHUNK_SIZE = 1024 * 1024`), which boto3 and the AWS CLI's classic transfers send.
const CHUNK: usize = 1 << 20;

const REGION: &str = "us-east-1";
const KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const ETAG: &str = "0123456789abcdef0123456789abcdef";

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Setup(String),
    Exchange(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Setup(e) => write!(f, "starting the listener: {e}"),
            Self::Exchange(e) => write!(f, "an exchange: {e}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

fn setup(e: impl std::fmt::Debug) -> Error {
    Error::Setup(format!("{e:?}"))
}

fn exchange(e: impl std::fmt::Debug) -> Error {
    Error::Exchange(format!("{e:?}"))
}

/// A request of the stub host's: its answers still to send, in order, and whether its body is
/// still being taken.
struct Job {
    request: door::Request,
    answers: VecDeque<Answer>,
    taking: bool,
}

/// The stub host's turn: each request the door holds taken, a get answered from `object`, a put's
/// body taken to its end and dropped, and each job's answers sent as the listener takes them. The
/// listener wakes the stub as it wakes a node: at each submission, each piece sent and each answer
/// taken.
fn host(doorway: &mut Doorway, jobs: &mut Vec<Job>, object: &Bytes) {
    while let Some(request) = doorway.take() {
        let done = |etag: &str| {
            Answer::Head(Box::new(Reply {
                etag: etag.to_owned(),
                ..Reply::status(Status::Done)
            }))
        };
        let (answers, taking) = match &request.op {
            // `empty` is an object of none; any other key the object.
            Op::Get { key, .. } => {
                let object = if key == EMPTY {
                    Bytes::new()
                } else {
                    object.clone()
                };
                let mut answers = VecDeque::new();
                answers.push_back(Answer::Head(Box::new(Reply {
                    size: u64::try_from(object.len()).unwrap_or(u64::MAX),
                    etag: ETAG.into(),
                    ..Reply::status(Status::Done)
                })));
                let mut at = 0usize;
                while at < object.len() {
                    let end = at.saturating_add(PIECE).min(object.len());
                    answers.push_back(Answer::Bytes(object.slice(at..end)));
                    at = end;
                }
                answers.push_back(Answer::End);
                (answers, false)
            }
            // The listener reads a put's body only once told it is wanted.
            Op::Put { .. } => (VecDeque::from([Answer::Ready]), true),
            _ => (VecDeque::from([done(""), Answer::End]), false),
        };
        jobs.push(Job {
            request,
            answers,
            taking,
        });
    }
    jobs.retain_mut(|job| {
        while job.taking {
            match job.request.body.try_recv() {
                Ok(Some(Piece::Bytes(_))) => {}
                Ok(Some(Piece::End(_))) => {
                    job.taking = false;
                    job.answers.push_back(Answer::Head(Box::new(Reply {
                        etag: ETAG.into(),
                        ..Reply::status(Status::Done)
                    })));
                    job.answers.push_back(Answer::End);
                }
                Ok(None) => break,
                Ok(Some(Piece::Refused(_))) | Err(_) => return false,
            }
        }
        while let Some(answer) = job.answers.pop_front() {
            match job.request.reply.try_send(answer) {
                Ok(()) => {}
                Err(SyncError::Full(answer)) => {
                    job.answers.push_front(answer);
                    return true;
                }
                Err(_) => return false,
            }
        }
        job.taking
    });
}

/// The listener's shard: the stub's task, the listener's and one for each connection it holds,
/// and a socket for the listener and each connection, read and written.
fn runtime() -> Result<LocalRuntime, Error> {
    let connections = usize::try_from(CONNECTIONS).map_err(setup)?;
    let tasks = connections.checked_add(2).ok_or_else(|| setup("tasks"))?;
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: tasks,
        timers_per_shard: 1,
        interests_per_shard: interests_for(tasks),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).map_err(setup)?,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .map_err(setup)
}

/// The listener and its stub host on a shard of their own, serving `tls` where given: where it
/// listens, and the sender whose drop ends them, which wakes the shard's root to see so.
type Serving = (
    SocketAddr,
    hyper_rt::sync::Sender<()>,
    std::thread::JoinHandle<()>,
);

fn serve(tls: Option<(Vec<u8>, Vec<u8>)>) -> Result<Serving, Error> {
    let (bound, at) = mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("listener".into())
        .spawn(move || {
            let told = bound.clone();
            let ran = runtime().and_then(|mut rt| {
                rt.block_on(async move {
                    let (stop, mut stopped) = hyper_rt::sync::channel::<()>(1).map_err(setup)?;
                    let waker = std::future::poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
                    let (mut doorway, submitter) =
                        door::doorway(CONNECTIONS_DOOR, waker).map_err(setup)?;
                    let listener = Listener::bind(
                        Settings {
                            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
                            region: REGION.into(),
                            domains: Vec::new(),
                            access: vec![AccessKey {
                                id: KEY_ID.into(),
                                account: "123456789012".into(),
                                secret: SECRET.into(),
                            }],
                            tls,
                            node: 1,
                        },
                        submitter,
                    )
                    .map_err(setup)?;
                    let _ = told.send(listener.local_addr().map_err(setup).map(|at| (at, stop)));
                    let object = Bytes::from(vec![0xA5u8; OBJECT]);
                    let mut jobs = Vec::new();
                    let stub = std::future::poll_fn(|_| {
                        host(&mut doorway, &mut jobs, &object);
                        Poll::<()>::Pending
                    });
                    match race3(listener.serve(), stub, stopped.recv()).await {
                        Either3::First(e) => Err(setup(e)),
                        Either3::Second(()) | Either3::Third(_) => Ok(()),
                    }
                })
                .map_err(setup)
                .and_then(|ran| ran)
            });
            if let Err(e) = ran {
                let _ = bound.send(Err(e));
            }
        })
        .map_err(setup)?;
    let (at, stop) = at.recv().map_err(setup)??;
    Ok((at, stop, thread))
}

/// The door's bound: one request at a time from the bench's one client.
const CONNECTIONS_DOOR: usize = 1;

/// The key a get of no bytes names.
const EMPTY: &str = "empty";

/// A connection to the listener, plain or under TLS.
enum Connection {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream, ClientConfig>>),
}

impl Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(tls) => tls.read(buf),
        }
    }
}

impl Write for Connection {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(tls) => tls.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(tls) => tls.flush(),
        }
    }
}

/// A connection to the listener at `at`, under TLS 1.3 verified by `authority` where given.
fn connect(at: SocketAddr, authority: Option<&[u8]>) -> Result<Connection, Error> {
    let stream = TcpStream::connect(at).map_err(exchange)?;
    let Some(authority) = authority else {
        return Ok(Connection::Plain(stream));
    };
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(authority.to_vec()))
        .map_err(setup)?;
    let mut config =
        ClientConfig::builder_with_provider(&hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER)
            .with_protocol_versions(&[&version::TLS13])
            .map_err(setup)?
            .with_root_certificates(roots)
            .with_no_client_auth();
    let name = ServerName::try_from("localhost").map_err(setup)?;
    let connection = ClientConnection::new(&mut config, name).map_err(setup)?;
    Ok(Connection::Tls(Box::new(StreamOwned::new(
        connection, config, stream,
    ))))
}

/// `YYYYMMDDTHHMMSSZ` and `YYYYMMDD` now.
fn timestamp() -> Result<(String, String), Error> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(exchange)?
        .as_millis();
    let iso = i64::try_from(millis)
        .ok()
        .and_then(mantle_s3::time::iso8601)
        .ok_or_else(|| exchange("the time"))?;
    let compact: String = iso
        .get(..19)
        .unwrap_or_default()
        .chars()
        .filter(|c| *c != '-' && *c != ':')
        .collect();
    let stamp = format!("{compact}Z");
    let date = stamp.get(..8).unwrap_or_default().to_owned();
    Ok((stamp, date))
}

/// A request's head, signed for `at` with the bench's key, `payload` as `x-amz-content-sha256`
/// names the body. SigV4 holds a signature good for fifteen minutes (05 §1.6), so a row signs
/// once and sends the same head again and again: the client's signing is no part of a round trip.
fn head(
    at: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    payload: &str,
) -> Result<Vec<u8>, Error> {
    let (stamp, date) = timestamp()?;
    let host = at.to_string();
    let mut all: Vec<(String, String)> = vec![
        ("host".into(), host),
        ("x-amz-date".into(), stamp.clone()),
        ("x-amz-content-sha256".into(), payload.to_owned()),
    ];
    all.extend(
        headers
            .iter()
            .map(|(n, v)| ((*n).to_owned(), (*v).to_owned())),
    );
    let mut signed: Vec<&str> = all
        .iter()
        .map(|(n, _)| n.as_str())
        .filter(|n| *n != "content-length")
        .collect();
    signed.sort_unstable();
    let pairs: Vec<(&str, &str)> = all.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
    let request = sigv4::Request {
        method,
        path: target,
        query: "",
        headers: &pairs,
    };
    let canonical = sigv4::canonical_request(&request, &signed, payload, None).map_err(exchange)?;
    let signature = sigv4::sign(SECRET, &stamp, &date, REGION, &canonical).map_err(exchange)?;
    let mut text = format!("{method} {target} HTTP/1.1\r\n");
    for (name, value) in &all {
        text.push_str(&format!("{name}: {value}\r\n"));
    }
    text.push_str(&format!(
        "authorization: {} Credential={KEY_ID}/{}, SignedHeaders={}, Signature={}\r\n\r\n",
        sigv4::ALGORITHM,
        sigv4::scope(&date, REGION, "s3"),
        signed.join(";"),
        sigv4::hex(&signature)
    ));
    Ok(text.into_bytes())
}

/// Sends `head` and `body` on `connection` and reads the response whole, its body's octets read
/// and dropped: its status and its body's length.
fn exchange_once(
    connection: &mut Connection,
    (head, body): (&[u8], &[u8]),
    read: &mut Vec<u8>,
    buf: &mut [u8],
) -> Result<(u16, u64), Error> {
    connection.write_all(head).map_err(exchange)?;
    connection.write_all(body).map_err(exchange)?;
    let end = loop {
        if let Some(end) = read.windows(4).position(|w| w == b"\r\n\r\n") {
            break end.saturating_add(4);
        }
        let n = connection.read(buf).map_err(exchange)?;
        if n == 0 {
            return Err(exchange("the listener closed the connection"));
        }
        read.extend_from_slice(buf.get(..n).unwrap_or_default());
    };
    let text = String::from_utf8_lossy(read.get(..end).unwrap_or_default()).into_owned();
    read.drain(..end);
    let status: u16 = text
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| exchange(&text))?;
    let length: u64 = text
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    let mut left = length;
    let buffered = u64::try_from(read.len()).unwrap_or(u64::MAX).min(left);
    read.drain(..usize::try_from(buffered).unwrap_or(usize::MAX));
    left = left.saturating_sub(buffered);
    while left > 0 {
        let n = connection.read(buf).map_err(exchange)?;
        if n == 0 {
            return Err(exchange("the listener ended a body short"));
        }
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        if n > left {
            let over = usize::try_from(left).unwrap_or(usize::MAX);
            read.extend_from_slice(buf.get(over..).unwrap_or_default());
        }
        left = left.saturating_sub(n);
    }
    Ok((status, length))
}

/// A put's object as `aws-chunked` sends it: [`CHUNK`]-octet chunks, the last of none, and a
/// trailing CRC-32 (05 §1.8–§1.9).
fn aws_chunked(object: &[u8]) -> Result<Vec<u8>, Error> {
    let crc = checksum::checksum(Algorithm::Crc32, object)
        .map_err(exchange)?
        .to_base64();
    let mut out = Vec::new();
    for chunk in object.chunks(CHUNK) {
        out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("0\r\nx-amz-checksum-crc32:{crc}\r\n\r\n").as_bytes());
    Ok(out)
}

/// One row's request: its head and body.
struct Asked {
    what: String,
    head: Vec<u8>,
    body: Vec<u8>,
    /// The object's octets the request moves, for a rate; none for a round trip's row.
    moves: Option<u64>,
}

/// Each row's requests against the listener at `at`.
fn asked(at: SocketAddr) -> Result<Vec<Asked>, Error> {
    let object = vec![0x5Au8; OBJECT];
    let small = vec![0x5Au8; 1 << 10];
    let size = OBJECT.to_string();
    let chunked = aws_chunked(&object)?;
    let encoded = chunked.len().to_string();
    let moved = u64::try_from(OBJECT).map_err(setup)?;
    Ok(vec![
        Asked {
            what: "GET of no bytes".into(),
            head: head(at, "GET", &format!("/b/{EMPTY}"), &[], sigv4::EMPTY_SHA256)?,
            body: Vec::new(),
            moves: None,
        },
        Asked {
            what: "PUT of 1 KiB".into(),
            head: head(
                at,
                "PUT",
                "/b/small",
                &[("content-length", "1024")],
                "UNSIGNED-PAYLOAD",
            )?,
            body: small,
            moves: None,
        },
        Asked {
            what: format!("GET of {}", display::size(OBJECT)),
            head: head(at, "GET", "/b/object", &[], sigv4::EMPTY_SHA256)?,
            body: Vec::new(),
            moves: Some(moved),
        },
        Asked {
            what: format!("PUT of {}", display::size(OBJECT)),
            head: head(
                at,
                "PUT",
                "/b/object",
                &[("content-length", &size)],
                "UNSIGNED-PAYLOAD",
            )?,
            body: object,
            moves: Some(moved),
        },
        Asked {
            what: format!("PUT of {}, aws-chunked", display::size(OBJECT)),
            head: head(
                at,
                "PUT",
                "/b/object",
                &[
                    ("content-encoding", "aws-chunked"),
                    ("content-length", &encoded),
                    ("x-amz-decoded-content-length", &size),
                    ("x-amz-trailer", "x-amz-checksum-crc32"),
                ],
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
            )?,
            body: chunked,
            moves: Some(moved),
        },
    ])
}

/// The value at `percent` of sorted `values`, by nearest rank: the least value with that share
/// of them at or below it.
fn nearest(values: &[u64], percent: usize) -> Option<u64> {
    let rank = values.len().checked_mul(percent)?.div_ceil(100);
    values.get(rank.checked_sub(1)?).copied()
}

pub fn s3(out: &mut impl Write, step: Duration) -> Result<(), Error> {
    writeln!(
        out,
        "  {:<6} {:<30} {:>8} {:>10} {:>10} {:>12}",
        "", "request", "n", "median", "p99", "rate"
    )?;
    // The endpoint's chain as `mantle init` makes it, for the TLS rows.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(setup)?
        .as_secs();
    let validity = Validity {
        not_before: now.saturating_sub(3_600),
        not_after: now.saturating_add(86_400),
    };
    let name = "mantle bench endpoint authority";
    let authority_key = EndpointKey::generate().map_err(setup)?;
    let authority = mantle_cert::endpoint_authority(
        &authority_key,
        name,
        validity,
        Serial::draw().map_err(setup)?,
    )
    .map_err(setup)?;
    let key = EndpointKey::generate().map_err(setup)?;
    let leaf = mantle_cert::endpoint(
        &authority_key,
        name,
        key.public_key(),
        &[EndpointName::Dns("localhost")],
        (validity, Serial::draw().map_err(setup)?),
    )
    .map_err(setup)?;
    for (transport, tls) in [("plain", None), ("TLS", Some((leaf, key.pkcs8().to_vec())))] {
        let trusted = tls.as_ref().map(|_| authority.as_slice());
        let (at, stop, thread) = serve(tls)?;
        let mut connection = connect(at, trusted)?;
        let mut read = Vec::new();
        let mut buf = vec![0u8; 256 << 10];
        for asked in asked(at)? {
            let request = (asked.head.as_slice(), asked.body.as_slice());
            // The connection and the first exchange's allocations warmed first.
            let (status, _) = exchange_once(&mut connection, request, &mut read, &mut buf)?;
            if status != 200 {
                return Err(exchange(format!("{}: {status}", asked.what)));
            }
            let mut took = Vec::new();
            let started = Instant::now();
            while took.is_empty() || started.elapsed() < step {
                let began = Instant::now();
                exchange_once(&mut connection, request, &mut read, &mut buf)?;
                took.push(u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX));
            }
            let secs = started.elapsed().as_secs_f64();
            took.sort_unstable();
            // usize -> f64 rounds above 2^53, far beyond a run's count.
            let rate = asked.moves.map_or_else(
                || format!("{}/s", display::count(took.len() as f64 / secs)),
                |moves| display::rate(moves as f64 * took.len() as f64 / secs),
            );
            writeln!(
                out,
                "  {:<6} {:<30} {:>8} {:>10} {:>10} {:>12}",
                transport,
                asked.what,
                took.len(),
                display::quantile(nearest(&took, 50)),
                display::quantile(nearest(&took, 99)),
                rate,
            )?;
            out.flush()?;
        }
        drop(connection);
        drop(stop);
        thread.join().map_err(|_| setup("the listener's thread"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_row_is_reported_through_a_listener() {
        let mut out = Vec::new();
        s3(&mut out, Duration::from_millis(1)).unwrap();
        let text = String::from_utf8(out).unwrap();
        for transport in ["plain", "TLS"] {
            for row in [
                "GET of no bytes",
                "PUT of 1 KiB",
                "GET of 8 MiB",
                "aws-chunked",
            ] {
                assert!(
                    text.lines()
                        .any(|l| l.contains(transport) && l.contains(row)),
                    "{transport} {row}: {text}"
                );
            }
        }
    }
}
