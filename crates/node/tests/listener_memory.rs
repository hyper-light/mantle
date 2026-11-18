//! What an S3 listener's connection holds with a request in flight (docs/design/node.md §4.5;
//! the constants audit of 2026-11-12, §3 #3): its task, its socket's and TLS's state, its request
//! as the door holds it and the pieces of its body the connection read ahead of the host, counted
//! by hyper-measure's counting allocator on the listener's thread, which the clients' threads do
//! not touch. Each client sends a signed PUT whose body is longer than the pieces a connection
//! reads ahead; the test takes each request from the door and holds every piece of its body, as
//! the host holds them while it writes, so every connection stands at its most whatever order the
//! threads ran in. What one connection adds is the difference between counts of connections, and
//! it is the same for each.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[allow(dead_code)]
#[path = "../../mantle/tests/support/s3.rs"]
mod s3;

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{RecvTimeoutError, sync_channel};
use std::task::Waker;
use std::thread::JoinHandle;
use std::time::Instant;

use hyper_measure::alloc;
use hyper_rt::combine::{Either, race2};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_tls::pki_types::{CertificateDer, ServerName};
use hyper_tls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned, version};
use mantle_cert::{EndpointKey, EndpointName, Serial, Validity};
use mantle_node::door::{self, Piece};
use mantle_node::listener::{Listener, PIECE, Settings, most_pieces, tasks};
use mantle_node::process::AccessKey;
use mantle_protocol::bounds::STOCK_PATIENCE;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The most connections a count takes.
const MOST: u64 = 4;

/// The listener's shard, as `listener.rs` makes it: a task and a timer for each connection it
/// holds, and a socket for it and each.
fn runtime() -> LocalRuntime {
    let connections = usize::try_from(tasks(MOST)).unwrap();
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: connections + 1,
        timers_per_shard: connections + 1,
        interests_per_shard: interests_for(connections + 1),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

fn access() -> AccessKey {
    AccessKey {
        id: "AKIAIOSFODNN7EXAMPLE".into(),
        account: "123456789012".into(),
        secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A body longer than the pieces a connection reads ahead of the host, by two pieces.
fn body() -> Vec<u8> {
    vec![0x5a; 4 * PIECE]
}

/// A signed PUT of [`body`]'s head.
fn put_head(at: SocketAddr, key: &AccessKey) -> String {
    let body = body();
    let payload = mantle_s3::sigv4::hex(&mantle_s3::crypto::sha256(&body).unwrap());
    let length = body.len().to_string();
    let signer = s3::Client::new(
        at,
        s3::Key {
            id: key.id.clone(),
            secret: key.secret.clone(),
        },
        "us-east-1",
    );
    signer.head(
        "PUT",
        "/bucket/key",
        &[("content-length", length.as_str())],
        &payload,
    )
}

/// The endpoint's certificate and key and its authority, as `mantle init` makes them.
struct Chain {
    authority: Vec<u8>,
    leaf: Vec<u8>,
    key: Vec<u8>,
}

fn chain() -> Chain {
    let authority_key = EndpointKey::generate().unwrap();
    let validity = Validity {
        not_before: now() - 3_600,
        not_after: now() + 86_400,
    };
    let name = "mantle s3 endpoint authority";
    let authority =
        mantle_cert::endpoint_authority(&authority_key, name, validity, Serial::draw().unwrap())
            .unwrap();
    let key = EndpointKey::generate().unwrap();
    let leaf = mantle_cert::endpoint(
        &authority_key,
        name,
        key.public_key(),
        &[EndpointName::Address("127.0.0.1".parse().unwrap())],
        (validity, Serial::draw().unwrap()),
    )
    .unwrap();
    Chain {
        authority,
        leaf,
        key: key.pkcs8().to_vec(),
    }
}

/// One client: a PUT whose body the connection reads ahead of the host, over TLS where the
/// chain's authority is given, its connection held open until the listener ends.
fn client(at: SocketAddr, authority: Option<Vec<u8>>) {
    let head = put_head(at, &access());
    let body = body();
    let tcp = TcpStream::connect(at).unwrap();
    // A write the listener does not read past its pieces blocks; the test ends it by ending the
    // listener, which closes the socket.
    match authority {
        None => {
            let mut tcp = tcp;
            let _ = tcp.write_all(head.as_bytes());
            let _ = tcp.write_all(&body);
            let _ = std::io::Read::read(&mut tcp, &mut [0u8; 1]);
        }
        Some(authority) => {
            let mut roots = RootCertStore::empty();
            roots.add(CertificateDer::from(authority)).unwrap();
            let mut config = ClientConfig::builder_with_provider(
                &hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER,
            )
            .with_protocol_versions(&[&version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let connection =
                ClientConnection::new(&mut config, ServerName::try_from("127.0.0.1").unwrap())
                    .unwrap();
            let mut tls = StreamOwned::new(connection, config, tcp);
            let _ = tls.write_all(head.as_bytes());
            let _ = tls.write_all(&body);
            let _ = std::io::Read::read(&mut tls, &mut [0u8; 1]);
        }
    }
}

/// The bytes the listener's thread holds with `connections` connections each at its most, past
/// what it held bound with none, over TLS where `tls`.
///
/// Each wait is bounded by a stock client's patience (`bounds::STOCK_PATIENCE`, research/77 D2),
/// as `listener.rs` bounds its exchanges: the requests' coming to the door, each body's coming
/// whole and the clients' end once the runtime closed their connections, each owed well within
/// it. A wait past its bound fails the test naming it at the test's line rather than holding it,
/// and a client that failed fails the test with its own failure.
#[track_caller]
fn held(connections: usize, tls: bool) -> i64 {
    let line = std::panic::Location::caller();
    let chain = tls.then(chain);
    let patience = u64::try_from(STOCK_PATIENCE.as_nanos()).unwrap();
    // Each client holds a sender until it returns or unwinds: every sender gone, every client
    // ended.
    let (ending, ends) = sync_channel::<()>(0);
    let mut runtime = runtime();
    let (count, clients) = runtime
        .block_on(async move {
            let (mut doorway, submitter) =
                door::doorway(connections, Waker::noop().clone()).unwrap();
            let authority = chain.as_ref().map(|c| c.authority.clone());
            let settings = Settings {
                listen: "127.0.0.1:0".parse().unwrap(),
                region: "us-east-1".into(),
                domains: Vec::new(),
                access: vec![access()],
                tls: chain.map(|c| (c.leaf, c.key)),
                node: 1,
                connections: MOST,
            };
            // The host's own records, made before the count.
            let mut requests = Vec::with_capacity(connections);
            let mut pieces = Vec::with_capacity(connections * 4);
            alloc::begin();
            let listener = Listener::bind(settings, submitter).unwrap();
            let at = listener.local_addr().unwrap();
            let bound = alloc::read().live;
            alloc::pause();
            let clients: Vec<_> = (0..connections)
                .map(|_| {
                    let (authority, ending) = (authority.clone(), ending.clone());
                    std::thread::spawn(move || {
                        client(at, authority);
                        drop(ending);
                    })
                })
                .collect();
            drop(ending);
            // Each client sends its request as it starts: every one is owed at the door within a
            // stock client's patience of now.
            let started = Instant::now();
            alloc::resume();
            // The host's side: each request taken from the door as it comes and its whole body
            // taken as the connection sends it, held as the host holds pieces while it writes;
            // then the count is read. Every octet the client sent has then passed through the
            // connection's buffer into pieces the host holds, whatever order the connections and
            // their clients ran in. Taking only what a connection reads ahead, the count read
            // whatever it had read by then: on one CPU a lone connection had not read its last
            // pieces off the socket, and held 101 KB less (CI run 43762797164). A wait past its
            // bound is returned, not panicked: an unwind here ends the root `RootUnwound`, which
            // names no wait, and a client's own failure is known only once it is joined.
            let hold = async {
                while requests.len() < connections {
                    match doorway.waiting() {
                        // A connection signing with a root key asks the node for no key.
                        Some(_) => match doorway.take() {
                            Some(door::Asked::Op(request)) => requests.push(request),
                            _ => panic!("a root key's connection asked the node for a key"),
                        },
                        // A client ends only once the listener has: one that ended first failed,
                        // or its connection did.
                        None if clients.iter().any(JoinHandle::is_finished) => {
                            let came = requests.len();
                            let why = format!("a client ended with {came} there");
                            return Err(("every request at the door", why));
                        }
                        None if started.elapsed() > STOCK_PATIENCE => {
                            let came = requests.len();
                            let why = format!("{came} came within {STOCK_PATIENCE:?}");
                            return Err(("every request at the door", why));
                        }
                        None => hyper_rt::futures::sleep(1_000_000).await.unwrap(),
                    }
                }
                for (taken, request) in requests.iter_mut().enumerate() {
                    // A put's driver wants its body: the connection reads it from here on.
                    request.reply.try_send(door::Answer::Ready).unwrap();
                    let whole = async {
                        loop {
                            match request.body.recv().await.unwrap() {
                                Piece::Bytes(bytes, _) => {
                                    // The piece is the connection's copy; the list the test
                                    // keeps it in is the test's, uncounted.
                                    alloc::pause();
                                    pieces.push(bytes);
                                    alloc::resume();
                                }
                                Piece::End(_) => break,
                                other => panic!("{other:?}"),
                            }
                        }
                    };
                    // Whole within a stock client's patience: its client wrote it at once, and the
                    // connection waits on a client that long between octets before it lets it go
                    // (`listener::conn`).
                    if hyper_rt::futures::within(patience, whole)
                        .await
                        .unwrap()
                        .is_none()
                    {
                        let why =
                            format!("{taken} came whole, the next not within {STOCK_PATIENCE:?}");
                        return Err(("each body whole", why));
                    }
                }
                let live = alloc::read().live;
                Ok((live, requests, pieces))
            };
            let count = match race2(listener.serve(), hold).await {
                Either::Second(held) => held.map(|(live, requests, pieces)| {
                    drop((requests, pieces));
                    live - bound
                }),
                Either::First(e) => panic!("the listener ended: {e}"),
            };
            alloc::end();
            (count, clients)
        })
        .unwrap();
    // The connections end with the runtime that holds them, and their clients with them: each
    // client's end is owed at once, and waited on for a stock client's patience.
    drop(runtime);
    let ended = matches!(
        ends.recv_timeout(STOCK_PATIENCE),
        Err(RecvTimeoutError::Disconnected)
    );
    for client in clients {
        // A client that failed fails the test with its own failure, ahead of any wait's.
        if (ended || client.is_finished())
            && let Err(failure) = client.join()
        {
            std::panic::resume_unwind(failure);
        }
    }
    let (what, why) = match (count, ended) {
        (Ok(live), true) => return live,
        (Ok(_), false) => {
            let why = format!("not within {STOCK_PATIENCE:?} of their connections' close");
            ("every client's end", why)
        }
        (Err(stopped), _) => stopped,
    };
    let over = if tls { "TLS" } else { "plain HTTP" };
    panic!("{what} of {connections} over {over} at {line}: {why}")
}

/// A connection with a request in flight holds no more than `listener::CONNECTION_BYTES` states,
/// over plain HTTP and over TLS, at every count of connections: one alone, with what the
/// listener's first connection makes once (on macOS std's `Mutex` around the TLS sessions boxes
/// its pthread mutex at the first lock, 64 bytes), is the most one holds, and each past it adds
/// no more.
#[test]
fn a_listener_connection_holds_its_stated_bytes() {
    let bound = mantle_node::listener::CONNECTION_BYTES;
    for tls in [false, true] {
        let connections = [1, 2, 4];
        let counts: Vec<i64> = connections.into_iter().map(|n| held(n, tls)).collect();
        let each = (counts[2] - counts[0]) / 3;
        eprintln!("tls {tls}: held {counts:?}, {each} a connection");
        assert!(each > 0, "{counts:?}");
        for (n, count) in connections.into_iter().zip(&counts) {
            assert!(
                u64::try_from(*count).unwrap() <= u64::try_from(n).unwrap() * bound,
                "tls {tls}: {count} held by {n} connections, past {bound} each"
            );
        }
        assert!(
            u64::try_from(each).unwrap() <= bound,
            "tls {tls}: {each} a connection past {bound}"
        );
    }
}

/// A request a client asks of one connection, twice: once to bring the connection to its rest
/// with every buffer it keeps across requests made, then again, counted.
#[derive(Clone, Copy, Debug)]
enum Method {
    Put,
    Get,
    Head,
    Delete,
}

/// The object a GET is answered from: a static slice, so the host's bytes are no allocation of
/// the listener's thread, as a node's are its own window's (node.md §4.3).
static OBJECT: [u8; 4 * PIECE] = [0x5a; 4 * PIECE];

/// The client's TLS, by version, or plain HTTP.
#[derive(Clone, Copy, Debug)]
enum Over {
    Plain,
    Tls13,
    Tls12,
}

/// A response read whole: its status.
fn answered<R: std::io::Read + ?Sized>(stream: &mut R, head_only: bool) -> u16 {
    let mut read = Vec::new();
    let mut byte = [0u8; 1];
    while !read.ends_with(b"\r\n\r\n") {
        let n = stream.read(&mut byte).unwrap();
        assert!(n > 0, "closed before its head: {read:?}");
        read.push(byte[0]);
    }
    let text = String::from_utf8(read).unwrap();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap();
    let length = text
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length: ")
                .map(|v| v.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    if !head_only {
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).unwrap();
    }
    status
}

/// How a request's head is padded past the fields its client signs: not at all, or with as many
/// empty fields as fit within S3's 8 KB (`mantle_http::MAX_HEAD`), each `a:` and its line's end:
/// CRLF, or the lone LF a line may end at (RFC 9112 §2.2), the shortest field line and so the
/// most fields a head can carry.
#[derive(Clone, Copy, Debug)]
enum Padded {
    No,
    Crlf,
    Lf,
}

/// `head` with as many of `line` as fit within `mantle_http::MAX_HEAD` before its empty line.
fn pad(mut head: String, line: &str) -> String {
    let fields = (mantle_http::MAX_HEAD - head.len()) / line.len();
    eprintln!(
        "a head of {} octets padded with {fields} fields of {line:?}",
        head.len()
    );
    head.truncate(head.len() - "\r\n".len());
    for _ in 0..fields {
        head.push_str(line);
    }
    head.push_str("\r\n");
    head
}

/// The request's head and body, signed, its head padded as `padded` says.
fn asked(at: SocketAddr, method: Method, size: usize, padded: Padded) -> (String, Vec<u8>) {
    let key = access();
    let signer = s3::Client::new(
        at,
        s3::Key {
            id: key.id.clone(),
            secret: key.secret.clone(),
        },
        "us-east-1",
    );
    let body = match method {
        Method::Put => vec![0x5a; size],
        _ => Vec::new(),
    };
    let payload = mantle_s3::sigv4::hex(&mantle_s3::crypto::sha256(&body).unwrap());
    let length = body.len().to_string();
    let (name, headers): (&str, &[(&str, &str)]) = match method {
        Method::Put => ("PUT", &[("content-length", length.as_str())]),
        Method::Get => ("GET", &[]),
        Method::Head => ("HEAD", &[]),
        Method::Delete => ("DELETE", &[]),
    };
    let head = signer.head(name, "/bucket/key", headers, &payload);
    let head = match padded {
        Padded::No => head,
        Padded::Crlf => pad(head, "a:\r\n"),
        Padded::Lf => pad(head, "a:\n"),
    };
    (head, body)
}

/// One client on one connection: the request twice, each answer read whole; told when the first
/// is, the second asked only once it is let go, and the connection then held until the listener
/// ends.
fn asker(
    at: SocketAddr,
    authority: Option<Vec<u8>>,
    over: Over,
    (method, size, padded): (Method, usize, Padded),
    (warm, go, done): (
        std::sync::mpsc::SyncSender<u16>,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<u16>,
    ),
) {
    let tcp = TcpStream::connect(at).unwrap();
    let head_only = matches!(method, Method::Head);
    let exchange = |stream: &mut dyn ReadWrite| {
        for round in 0..2 {
            if round == 1 {
                go.recv().unwrap();
            }
            let (head, body) = asked(at, method, size, padded);
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(&body).unwrap();
            stream.flush().unwrap();
            let status = answered(stream, head_only);
            let told = if round == 0 { &warm } else { &done };
            told.send(status).unwrap();
        }
        let _ = stream.read(&mut [0u8; 1]);
    };
    match authority {
        None => exchange(&mut { tcp }),
        Some(authority) => {
            let mut roots = RootCertStore::empty();
            roots.add(CertificateDer::from(authority)).unwrap();
            let versions: &[&hyper_tls::SupportedProtocolVersion] = match over {
                Over::Tls12 => &[&version::TLS12],
                _ => &[&version::TLS13],
            };
            let mut config = ClientConfig::builder_with_provider(
                &hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER,
            )
            .with_protocol_versions(versions)
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let connection =
                ClientConnection::new(&mut config, ServerName::try_from("127.0.0.1").unwrap())
                    .unwrap();
            exchange(&mut StreamOwned::new(connection, config, tcp));
        }
    }
}

/// A connection's stream, plain or under TLS.
trait ReadWrite: std::io::Read + Write {}
impl<T: std::io::Read + Write> ReadWrite for T {}

/// The host's side of one request, as a node's driver answers it: a put's body taken piece by
/// piece and each piece let go as it comes, as the driver takes it into its own window; a get's
/// bytes the static object's, in pieces.
async fn host(method: Method, size: usize, mut request: Box<door::Request>) {
    use mantle_protocol::object::{Reply, Status};
    let reply = |size: u64| Reply {
        size,
        etag: "0123456789abcdef0123456789abcdef".into(),
        ..Reply::status(Status::Done)
    };
    match method {
        Method::Put => {
            request.reply.send(door::Answer::Ready).await.unwrap();
            loop {
                match request.body.recv().await.unwrap() {
                    Piece::Bytes(piece, _) => drop(piece),
                    Piece::End(_) => break,
                    other => panic!("{other:?}"),
                }
            }
            let head = door::Answer::Head(Box::new(reply(0)));
            request.reply.send(head).await.unwrap();
        }
        Method::Get => {
            let head = door::Answer::Head(Box::new(reply(u64::try_from(size).unwrap())));
            request.reply.send(head).await.unwrap();
            for piece in OBJECT[..size].chunks(PIECE) {
                let bytes = bytes::Bytes::from_static(piece);
                request
                    .reply
                    .send(door::Answer::Bytes(bytes))
                    .await
                    .unwrap();
            }
            request.reply.send(door::Answer::End).await.unwrap();
        }
        Method::Head => {
            let head = door::Answer::Head(Box::new(reply(u64::try_from(size).unwrap())));
            request.reply.send(head).await.unwrap();
        }
        Method::Delete => {
            let head = door::Answer::Head(Box::new(Reply::status(Status::Done)));
            request.reply.send(head).await.unwrap();
        }
    }
}

/// The next request at the door, within a stock client's patience.
async fn next(doorway: &mut door::Doorway, since: Instant) -> Box<door::Request> {
    loop {
        match doorway.take() {
            Some(door::Asked::Op(request)) => return request,
            Some(door::Asked::Key(_)) => panic!("a root key's request asked the node for a key"),
            None if since.elapsed() > STOCK_PATIENCE => panic!("no request within patience"),
            None => hyper_rt::futures::sleep(1_000_000).await.unwrap(),
        }
    }
}

/// What the client says next, within a stock client's patience.
async fn told(from: &std::sync::mpsc::Receiver<u16>, since: Instant) -> u16 {
    loop {
        match from.try_recv() {
            Ok(status) => return status,
            Err(_) if since.elapsed() > STOCK_PATIENCE => panic!("no answer within patience"),
            Err(_) => hyper_rt::futures::sleep(1_000_000).await.unwrap(),
        }
    }
}

/// The listener's thread back at its connection's rest, within a stock client's patience: a client
/// can read an answer whole before its connection has taken the reply's end from the host and let
/// the request go (1,884 octets of a TLS 1.3 GET were once still held), so a count waits on that
/// fact, not on the answer alone, and one never back at rest is counted as it stands.
async fn settled(since: Instant) {
    while alloc::read().live > 0 && since.elapsed() <= STOCK_PATIENCE {
        hyper_rt::futures::sleep(1_000_000).await.unwrap();
    }
}

/// The depth of a put's queue a request is made at: the node's first, before it has measured any,
/// or the most a body's queue holds (`listener::most_pieces`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Depth {
    First,
    Most,
}

/// The listener thread's counts from one connection at rest, its kept buffers made by the same
/// request before, to the second request's answer read whole by its client and the request let
/// go, a put's queue at `depth`.
fn one_request(
    over: Over,
    (method, size, padded): (Method, usize, Padded),
    depth: Depth,
) -> (alloc::Counts, u16) {
    let chain = (!matches!(over, Over::Plain)).then(chain);
    let mut runtime = runtime();
    let (counts, status, client) = runtime
        .block_on(async move {
            let (mut doorway, submitter) = door::doorway(1, Waker::noop().clone()).unwrap();
            // At the most pieces a body's queue holds, the node's learned depth is driven past the
            // cap by pieces the host took a second after their send, each delivered in a
            // nanosecond.
            if depth == Depth::Most {
                let long_ago = Instant::now()
                    .checked_sub(std::time::Duration::from_secs(1))
                    .unwrap();
                for _ in 0..10_000 {
                    if submitter.depth() >= most_pieces() {
                        break;
                    }
                    doorway.took(door::Paced {
                        sent: long_ago,
                        delivered_ns: Some(1),
                    });
                }
                assert!(submitter.depth() >= most_pieces());
            }
            let authority = chain.as_ref().map(|c| c.authority.clone());
            let settings = Settings {
                listen: "127.0.0.1:0".parse().unwrap(),
                region: "us-east-1".into(),
                domains: Vec::new(),
                access: vec![access()],
                tls: chain.map(|c| (c.leaf, c.key)),
                node: 1,
                connections: 1,
            };
            let listener = Listener::bind(settings, submitter).unwrap();
            let at = listener.local_addr().unwrap();
            let (warm, warmed) = sync_channel(1);
            let (go, gone) = sync_channel(1);
            let (done, answered) = sync_channel(1);
            let client = std::thread::spawn(move || {
                asker(
                    at,
                    authority,
                    over,
                    (method, size, padded),
                    (warm, gone, done),
                );
            });
            let started = Instant::now();
            let serve = async {
                let first = next(&mut doorway, started).await;
                host(method, size, first).await;
                let first = told(&warmed, started).await;
                alloc::begin();
                go.send(()).unwrap();
                let second = next(&mut doorway, started).await;
                host(method, size, second).await;
                let second = told(&answered, started).await;
                settled(started).await;
                let counts = alloc::end();
                (counts, first, second)
            };
            match race2(listener.serve(), serve).await {
                Either::Second((counts, first, second)) => {
                    assert_eq!(first, second, "{over:?} {method:?} {size}");
                    (counts, second, client)
                }
                Either::First(e) => panic!("the listener ended: {e}"),
            }
        })
        .unwrap();
    drop(runtime);
    client.join().unwrap();
    (counts, status)
}

/// A request, from its connection at rest to its answer read whole, holds no more than
/// `listener::REQUEST_BYTES` above that rest (one head, one body segment, one TLS record), over plain
/// HTTP, TLS 1.3 and TLS 1.2: a PUT and a GET of 1 octet, of a body spanning several TLS records
/// within one segment, and of one many segments long; a HEAD and a DELETE. Its connection is
/// brought to rest by the same request before the count, so every buffer it keeps across requests
/// is made; and the request leaves nothing behind. The host is the test's own, on the listener's
/// thread (`host`): a put's pieces are let go as it takes them, as a driver takes them into its
/// own window, and a get's bytes are a static slice, as a node's are its own window's, so what is
/// counted is the door's: its request, its pieces, its TLS records, and the host's reply it
/// answers from.
///
/// Each request is made at the depth of a put's queue before the node has measured any, one piece,
/// and at the most a body's queue holds (`listener::most_pieces`), the depth the node learns driven
/// past it. At the most, a put whose body comes in more than one piece, a read or a TLS record each,
/// holds `listener::streamed_bytes`: one head and one TLS record, and the pieces in the host's
/// queue, at most its depth of them. The connection waits for the queue's room before it copies a
/// piece, so it holds none beside them.
///
/// A head padded to S3's 8 KB with empty fields, CRLF-ended (about 1,880 of them) or ended by a
/// lone LF (the most a head carries, about 2,500), holds no more: the door reads a head's fields
/// where the head holds them and copies only those a version keeps.
#[test]
fn a_request_holds_no_more_than_its_stated_bound_above_its_connections_rest() {
    use mantle_node::listener::{REQUEST_BYTES, streamed_bytes};
    // A TLS record's plaintext at its largest (RFC 8446 §5.1): a TLS body's piece.
    const RECORD: usize = 1 << 14;
    let records = 3 * RECORD + 1;
    let many = 4 * PIECE;
    for over in [Over::Plain, Over::Tls13, Over::Tls12] {
        for (method, size, padded) in [
            (Method::Put, 1, Padded::No),
            (Method::Put, 1, Padded::Crlf),
            (Method::Put, 1, Padded::Lf),
            (Method::Put, records, Padded::No),
            (Method::Put, records, Padded::Crlf),
            (Method::Put, records, Padded::Lf),
            (Method::Put, PIECE, Padded::No),
            (Method::Put, many, Padded::No),
            (Method::Get, 1, Padded::No),
            (Method::Get, records, Padded::No),
            (Method::Get, many, Padded::No),
            (Method::Head, many, Padded::No),
            (Method::Delete, 0, Padded::No),
        ] {
            for depth in [Depth::First, Depth::Most] {
                let (counts, status) = one_request(over, (method, size, padded), depth);
                // A body that comes in more than one piece, a read or a TLS record each, may have as
                // many in the queue as its depth.
                let bound = match (method, depth) {
                    (Method::Put, Depth::Most) if size > RECORD => streamed_bytes(),
                    _ => REQUEST_BYTES,
                };
                let case = format!("{over:?} {method:?} {size} {padded:?} {depth:?}");
                eprintln!(
                    "{case}: status {status}, peak {} of {bound}, live after {}, {} allocations, {} \
                 reallocations",
                    counts.peak, counts.live, counts.allocations, counts.reallocations
                );
                assert!(matches!(status, 200 | 204), "{case}: {status}");
                assert!(
                    usize::try_from(counts.peak).unwrap() <= bound,
                    "{case}: a peak of {} past {bound}",
                    counts.peak
                );
                assert!(
                    counts.live <= 0,
                    "{case}: {} left past the answer",
                    counts.live
                );
            }
        }
    }
}

/// A warmed request reallocates none of the door's buffers, from its connection at rest to its
/// answer read whole, over plain HTTP, TLS 1.3 and TLS 1.2: a HEAD, a GET, a PUT of 1 octet, of
/// several TLS records and of many segments, and a DELETE; each buffer the door makes is made at
/// the length it is known to need. What reallocates is hyper-tls's: its deframer grows its buffer
/// from one read (4,096 octets) toward a record at its largest (`listener::TLS_RECORD`, its
/// `MAX_WIRE_SIZE`) a read at a time, four times for a put whose body fills a record, and lets it
/// back to one read once drained (`msgs/deframer/buffers.rs`, `prepare_read`). The host is the
/// test's own (`host`), so what a node's driver reallocates is not counted here.
#[test]
fn a_warmed_request_reallocates_none_of_the_doors_buffers() {
    use mantle_node::listener::TLS_RECORD;
    // hyper-tls's deframer's read (`prepare_read`'s `READ_SIZE`), and a record's plaintext at its
    // largest (RFC 8446 §5.1).
    const READ: usize = 4096;
    const PLAINTEXT: usize = 1 << 14;
    let records = 3 * PLAINTEXT + 1;
    let many = 4 * PIECE;
    let deframer = u64::try_from((TLS_RECORD - READ).div_ceil(READ)).unwrap();
    for over in [Over::Plain, Over::Tls13, Over::Tls12] {
        for (method, size) in [
            (Method::Head, many),
            (Method::Get, many),
            (Method::Put, 1),
            (Method::Put, records),
            (Method::Put, many),
            (Method::Delete, 0),
        ] {
            let (counts, status) = one_request(over, (method, size, Padded::No), Depth::First);
            let expected = match (over, method) {
                (Over::Tls13 | Over::Tls12, Method::Put) if size >= PLAINTEXT => deframer,
                _ => 0,
            };
            assert!(
                matches!(status, 200 | 204),
                "{over:?} {method:?} {size}: {status}"
            );
            assert_eq!(
                counts.reallocations, expected,
                "{over:?} {method:?} {size}: reallocations past hyper-tls's deframer's"
            );
        }
    }
}

/// A warmed request allocates only what crosses to the host, one copy its door does not read and,
/// over TLS, hyper-tls's buffer of each record, from its connection at rest to its answer read
/// whole, over plain HTTP, TLS 1.3 and TLS 1.2: a HEAD, a GET, a PUT of 1 octet and a DELETE. The
/// door parses the head in place in the connection's buffer, routes it to the bucket and key the
/// head holds, checks its signature in buffers on the stack and a put's payload digest in an array,
/// and names the account the node's configuration holds. What it allocates, each named:
///
/// - the request, boxed into the door's queue (`door::Submitter::submit`), which the host takes;
/// - the channel the body crosses on and the one the reply comes back on, each a ring, its room's
///   slots and their shared state (`hyper_rt::sync::channel`), whose other ends the host holds;
/// - the operation's bucket and key, and the account it acts for (`door::Request::principal`),
///   strings of their own because the host holds them past the head's buffer;
/// - a put's piece of body (`Piece::Bytes`), the host's to write;
/// - the access key `sigv4::verify` copies into its `Verified`, which the door does not read, and
///   which is the one it could still cut.
///
/// The host is the test's own (`host`): its reply's box and a reply's etag are the host's, not the
/// door's. hyper-tls copies each record it reads into a buffer of its own
/// (`CommonState::take_received_plaintext`) and writes each into one (`PrefixedPayload`): a
/// request's head is one record, a put's body one more; an answer is one record, a GET's of four
/// segments 17, its head and 262,108 octets at 16,384 a record.
#[test]
fn a_warmed_request_allocates_only_what_crosses_to_the_host() {
    let many = 4 * PIECE;
    for over in [Over::Plain, Over::Tls13, Over::Tls12] {
        for (method, size) in [
            (Method::Head, many),
            (Method::Get, many),
            (Method::Put, 1),
            (Method::Delete, 0),
        ] {
            let (counts, status) = one_request(over, (method, size, Padded::No), Depth::First);
            let put = u64::from(matches!(method, Method::Put));
            // Its box, its two channels of three each, its bucket, key and account; a put's piece.
            let crossing = 1 + 2 * 3 + 3 + put;
            // `Verified::access_key`.
            let unread = 1;
            // The test's host's reply: its box, and its etag but a delete's.
            let host = 1 + u64::from(!matches!(method, Method::Delete));
            let records = match (over, method) {
                (Over::Plain, _) => 0,
                (_, Method::Get) => 1 + 17,
                _ => 1 + put + 1,
            };
            let case = format!("{over:?} {method:?} {size}");
            eprintln!("{case}: {} allocations", counts.allocations);
            assert!(matches!(status, 200 | 204), "{case}: {status}");
            assert_eq!(
                counts.allocations,
                crossing + unread + host + records,
                "{case}: allocations past those named"
            );
        }
    }
}
