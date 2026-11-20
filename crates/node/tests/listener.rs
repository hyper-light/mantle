//! The S3 listener's TLS (docs/design/node.md §4.1; research/84): a listener bound on a real
//! socket with the endpoint's P-256 chain as `mantle init` makes it, reached by a stock client's
//! TLS (hyper-tls, rustls's webpki verifier against the endpoint authority) over TLS 1.3 and over
//! TLS 1.2. Asked: each handshake verifies the endpoint's certificate for the name the client
//! dials, takes HTTP/1.1 by ALPN where the client offers HTTP/2 beside it, and on TLS 1.2 an
//! ECDHE-ECDSA AES-GCM suite; and a request is read and answered over it, one signed with another
//! secret, which the listener refuses itself, `403 SignatureDoesNotMatch`, so no node stands
//! behind it. And the listener's connections (research/72): one past the budget answered `503
//! SlowDown`, never closed with no answer, and quiet ones let go after a stock client's patience.

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

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{RecvTimeoutError, sync_channel};
use std::task::Waker;
use std::thread::JoinHandle;
use std::time::Duration;

use hyper_rt::combine::{Either, race2};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_tls::pki_types::{CertificateDer, ServerName};
use hyper_tls::{
    ClientConfig, ClientConnection, ProtocolVersion, RootCertStore, StreamOwned,
    SupportedProtocolVersion, version,
};
use mantle_cert::{EndpointKey, EndpointName, Serial, Validity};
use mantle_node::door;
use mantle_node::listener::{Listener, REFUSING, Settings, tasks};
use mantle_node::process::AccessKey;
use mantle_protocol::bounds::STOCK_PATIENCE;

/// The connections the tests' listener serves: one stock client's, as many as it answers refused
/// past them.
const CONNECTIONS: u64 = REFUSING;

/// The listener's shard: the root task and a task for each connection it holds, served or
/// answered refused, with a timer for each connection's wait on its client, a socket for it and
/// each, read and written.
fn runtime() -> LocalRuntime {
    let connections = usize::try_from(tasks(CONNECTIONS)).unwrap();
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

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// What one handshake and request came to: the version and suite negotiated, the protocol ALPN
/// chose, and the response's status and body.
#[derive(Debug)]
struct Said {
    version: Option<ProtocolVersion>,
    suite: String,
    alpn: Option<Vec<u8>>,
    status: u16,
    body: String,
}

#[test]
fn the_listener_answers_over_tls_1_3_and_1_2_with_the_endpoint_chain() {
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
        &[
            EndpointName::Dns("localhost"),
            EndpointName::Address("127.0.0.1".parse().unwrap()),
        ],
        (validity, Serial::draw().unwrap()),
    )
    .unwrap();
    let access = access();
    let pkcs8 = key.pkcs8().to_vec();
    let asked = access.clone();
    let (said, client) = runtime()
        .block_on(async move {
            // No node takes what the door holds: every request here is refused before it.
            let (_doorway, submitter) = door::doorway(1, Waker::noop().clone()).unwrap();
            let settings = Settings {
                listen: "127.0.0.1:0".parse().unwrap(),
                region: "us-east-1".into(),
                domains: Vec::new(),
                access: vec![access],
                tls: Some((leaf, pkcs8)),
                node: 1,
                connections: CONNECTIONS,
            };
            let listener = Listener::bind(settings, submitter).unwrap();
            let at = listener.local_addr().unwrap();
            let (told, mut heard) = hyper_rt::sync::channel::<Vec<Said>>(1).unwrap();
            let client = std::thread::spawn(move || {
                let said = [&version::TLS13, &version::TLS12]
                    .into_iter()
                    .map(|version| {
                        let (authority, asked) = (authority.clone(), asked.clone());
                        within(
                            &format!("the exchange over {version:?}"),
                            STOCK_PATIENCE,
                            move || ask(at, &authority, version, &asked),
                        )
                    })
                    .collect();
                told.blocking_send(said).unwrap();
            });
            // A client that failed told nothing: its own failure is the test's, raised on the
            // test's thread once the runtime is left (`joined`).
            match race2(listener.serve(), heard.recv()).await {
                Either::Second(said) => (said.ok(), client),
                Either::First(e) => panic!("the listener ended: {e}"),
            }
        })
        .unwrap();
    let said = joined(said, client);
    let [tls13, tls12] = said.as_slice() else {
        panic!("{said:?}")
    };
    assert_eq!(tls13.version, Some(ProtocolVersion::TLSv1_3), "{tls13:?}");
    assert_eq!(tls12.version, Some(ProtocolVersion::TLSv1_2), "{tls12:?}");
    assert!(
        tls12.suite.contains("ECDHE_ECDSA") && tls12.suite.contains("GCM"),
        "{tls12:?}"
    );
    for said in [tls13, tls12] {
        assert_eq!(said.alpn.as_deref(), Some(&b"http/1.1"[..]), "{said:?}");
        assert_eq!(said.status, 403, "{said:?}");
        assert!(
            said.body.contains("<Code>SignatureDoesNotMatch</Code>"),
            "{said:?}"
        );
    }
}

/// The cell's access key, as the listener holds it.
fn access() -> AccessKey {
    AccessKey {
        id: "AKIAIOSFODNN7EXAMPLE".into(),
        account: "123456789012".into(),
        secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
    }
}

/// `key`'s id signing with another secret: a request the listener refuses itself, `403
/// SignatureDoesNotMatch`, with no node behind it, and the connection kept, as the request has no
/// body.
fn wrong(key: &AccessKey) -> s3::Key {
    s3::Key {
        id: key.id.clone(),
        secret: format!("{}x", key.secret),
    }
}

/// A listener in plain HTTP on loopback holding `access`.
fn plain(access: AccessKey) -> Settings {
    Settings {
        listen: "127.0.0.1:0".parse().unwrap(),
        region: "us-east-1".into(),
        domains: Vec::new(),
        access: vec![access],
        tls: None,
        node: 1,
        connections: CONNECTIONS,
    }
}

/// What `client` does beside the listener bound in plain HTTP, the listener serving until it
/// says.
fn beside<T: Send + 'static>(client: impl FnOnce(SocketAddr) -> T + Send + 'static) -> T {
    let access = access();
    let (said, thread) = runtime()
        .block_on(async move {
            // No node takes what the door holds: every request here is refused before it.
            let (_doorway, submitter) = door::doorway(1, Waker::noop().clone()).unwrap();
            let listener = Listener::bind(plain(access), submitter).unwrap();
            let at = listener.local_addr().unwrap();
            let (told, mut heard) = hyper_rt::sync::channel::<T>(1).unwrap();
            let thread = std::thread::spawn(move || {
                let said = client(at);
                if told.blocking_send(said).is_err() {
                    panic!("the listener's side ended");
                }
            });
            // A client that failed told nothing: its own failure is the test's, raised on the
            // test's thread once the runtime is left (`joined`).
            match race2(listener.serve(), heard.recv()).await {
                Either::Second(said) => (said.ok(), thread),
                Either::First(e) => panic!("the listener ended: {e}"),
            }
        })
        .unwrap();
    joined(said, thread)
}

/// What a client beside the listener told it, once the runtime that ran the listener is left; or
/// the client's own failure, which names its wait and the test's line, raised on the test's
/// thread. Raised within the runtime, an unwind ends its root, and the test fails `RootUnwound`,
/// which names neither.
fn joined<T>(said: Option<T>, client: JoinHandle<()>) -> T {
    match (said, client.join()) {
        (_, Err(failure)) => std::panic::resume_unwind(failure),
        (Some(said), Ok(())) => said,
        (None, Ok(())) => panic!("the client ended telling nothing"),
    }
}

/// A connection past the listener's budget is answered, never closed with no answer (node.md
/// §4.5; research/72). Each of the budget's slots is held by a connection that asked once and
/// stays open, as a stock client's pooled connection does; the next connection's request is
/// answered `503 SlowDown` with `Connection: close`. Closed with no answer instead, it is what the
/// AWS CLI's transfers take worst: a ranged GET whose every attempt met one is dropped, and the
/// copy ends exit 0 with the object short, where a `503` the CLI asks again after a backoff and
/// then fails loudly (research/72, measured 2026-11-02).
#[test]
fn a_connection_past_the_budget_is_answered_slow_down() {
    let (held, past) = beside(|at| {
        let key = wrong(&access());
        let mut held: Vec<s3::Client> = (0..CONNECTIONS)
            .map(|_| s3::Client::new(at, key.clone(), "us-east-1"))
            .collect();
        let statuses: Vec<u16> = held
            .iter_mut()
            .map(|client| asked(client, "a request held open", STOCK_PATIENCE, get))
            .collect();
        let past = answer(at, &key);
        drop(held);
        (statuses, past)
    });
    assert!(held.iter().all(|&status| status == 403), "{held:?}");
    let past = past.unwrap_or_else(|why| panic!("the connection past the budget: {why}"));
    assert_eq!(past.status, 503, "{past:?}");
    assert_eq!(
        past.element("Code").as_deref(),
        Some("SlowDown"),
        "{past:?}"
    );
    assert!(past.closes(), "{past:?}");
}

/// A connection whose client says nothing for a stock client's patience is let go, its slot with
/// it (node.md §4.5): every slot is held by a connection that asked once and went quiet, each is
/// closed by the listener, and a new connection's request is then served (`403`, the listener's
/// own answer), not answered refused. Each close is waited on for at most twice the patience, so a
/// listener that never lets go fails here rather than hangs.
#[test]
fn a_connection_quiet_for_a_stock_clients_patience_is_let_go() {
    let (closed, fresh) = beside(|at| {
        let key = wrong(&access());
        let mut held: Vec<s3::Client> = (0..CONNECTIONS)
            .map(|_| s3::Client::new(at, key.clone(), "us-east-1"))
            .collect();
        for client in &mut held {
            assert_eq!(asked(client, "a request", STOCK_PATIENCE, get), 403);
        }
        let closed = held
            .iter_mut()
            .map(|client| {
                let close = "the listener's close of a quiet connection";
                asked(client, close, 2 * STOCK_PATIENCE, s3::Client::closed)
            })
            .filter(|&closed| closed)
            .count();
        let fresh = answer(at, &key);
        // A client that finds its pooled connection closed asks on a new one, as stock clients
        // do, and is served again.
        let again = asked(&mut held[0], "a request again", STOCK_PATIENCE, get);
        (closed, (fresh, again))
    });
    assert_eq!(closed, usize::try_from(CONNECTIONS).unwrap());
    let (fresh, again) = fresh;
    let fresh = fresh.unwrap_or_else(|why| panic!("a connection after the quiet ones: {why}"));
    assert_eq!(fresh.status, 403, "{fresh:?}");
    assert_eq!(again, 403);
}

/// `GET /` by `client`, and the status it is answered.
fn get(client: &mut s3::Client) -> u16 {
    client.send("GET", "/", &[], b"").status
}

/// What `ask` came to of `client`, the client moved to a thread of its own and back, waited on
/// for at most `bound` (`within`): `s3::Client`'s exchanges block with no bound of their own.
#[track_caller]
fn asked<T: Send + 'static>(
    client: &mut s3::Client,
    what: &str,
    bound: Duration,
    ask: impl FnOnce(&mut s3::Client) -> T + Send + 'static,
) -> T {
    let fresh = s3::Client::new(client.at, client.key.clone(), &client.region);
    let mut moved = std::mem::replace(client, fresh);
    let (moved, came) = within(what, bound, move || {
        let came = ask(&mut moved);
        (moved, came)
    });
    *client = moved;
    came
}

/// What `ask` came to, asked on a thread of its own and waited on for at most `bound`, so an
/// exchange the listener stops answering fails the test rather than holding it: for an exchange, a
/// stock client's patience (`bounds::STOCK_PATIENCE`), as long as such a client waits on the
/// listener between bytes and as `answer` waits on each read; for a quiet connection's close,
/// which the listener owes once that patience has passed, twice it, as the test states. A failure,
/// the wait's or the thread's own, names `what` at the test's own line.
#[track_caller]
fn within<T: Send + 'static>(
    what: &str,
    bound: Duration,
    ask: impl FnOnce() -> T + Send + 'static,
) -> T {
    let at = std::panic::Location::caller();
    let (told, heard) = sync_channel(1);
    let thread = std::thread::spawn(move || {
        // Once the wait failed, nothing hears what it came to.
        let _ = told.send(ask());
    });
    match heard.recv_timeout(bound) {
        Ok(came) => came,
        Err(RecvTimeoutError::Timeout) => panic!("{what} at {at}: no end within {bound:?}"),
        Err(RecvTimeoutError::Disconnected) => {
            let panicked = thread.join().unwrap_err();
            let said = panicked
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panicked.downcast_ref::<&str>().copied())
                .unwrap_or("a panic with no message");
            panic!("{what} at {at}: {said}")
        }
    }
}

/// One request on a connection of its own, signed with `key`, and the response it gets, read
/// whole; or how the connection ended without one.
fn answer(at: SocketAddr, key: &s3::Key) -> Result<s3::Response, String> {
    let client = s3::Client::new(at, key.clone(), "us-east-1");
    let mut stream = TcpStream::connect(at).map_err(|e| format!("connect: {e}"))?;
    stream.set_read_timeout(Some(STOCK_PATIENCE)).unwrap();
    let head = client.head("GET", "/", &[], mantle_s3::sigv4::EMPTY_SHA256);
    stream
        .write_all(head.as_bytes())
        .map_err(|e| format!("closed with no answer, the request's write: {e}"))?;
    let mut read = Vec::new();
    let mut buf = [0u8; 4096];
    let end = loop {
        if let Some(end) = read.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
        match stream.read(&mut buf) {
            Ok(0) => return Err("closed with no answer".into()),
            Ok(n) => read.extend_from_slice(&buf[..n]),
            Err(e) => return Err(format!("closed with no answer: {e}")),
        }
    };
    let head = String::from_utf8(read[..end].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers: Vec<(String, String)> = lines
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (name, value) = l.split_once(':').unwrap();
            (name.trim().to_owned(), value.trim().to_owned())
        })
        .collect();
    let length: usize = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| value.parse().unwrap());
    while read.len() < end + length {
        match stream.read(&mut buf) {
            Ok(0) => return Err("the body ended early".into()),
            Ok(n) => read.extend_from_slice(&buf[..n]),
            Err(e) => return Err(format!("the body: {e}")),
        }
    }
    Ok(s3::Response {
        status,
        headers,
        body: read[end..end + length].to_vec(),
    })
}

/// Asks the listener at `at` over `version` of TLS, its certificate verified under `authority`
/// for `localhost`, a request signed with a secret other than `key`'s.
fn ask(
    at: SocketAddr,
    authority: &[u8],
    version: &'static SupportedProtocolVersion,
    key: &AccessKey,
) -> Said {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(authority.to_vec())).unwrap();
    let mut config =
        ClientConfig::builder_with_provider(&hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER)
            .with_protocol_versions(&[version])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let connection =
        ClientConnection::new(&mut config, ServerName::try_from("localhost").unwrap()).unwrap();
    let mut tls = StreamOwned::new(connection, config, TcpStream::connect(at).unwrap());
    let wrong = s3::Key {
        id: key.id.clone(),
        secret: format!("{}x", key.secret),
    };
    let head = s3::Client::new(at, wrong, "us-east-1").head(
        "GET",
        "/",
        &[],
        mantle_s3::sigv4::EMPTY_SHA256,
    );
    tls.write_all(head.as_bytes()).unwrap();
    let mut read = Vec::new();
    let mut buf = [0u8; 4096];
    let end = loop {
        if let Some(end) = read.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
        let n = tls.read(&mut buf).unwrap();
        assert!(n > 0, "the listener ended the response early");
        read.extend_from_slice(&buf[..n]);
    };
    let head = String::from_utf8(read[..end].to_vec()).unwrap();
    let length: usize = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap();
    while read.len() < end + length {
        let n = tls.read(&mut buf).unwrap();
        assert!(n > 0, "the listener ended the body early");
        read.extend_from_slice(&buf[..n]);
    }
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    Said {
        version: tls.conn.protocol_version(),
        suite: format!(
            "{:?}",
            tls.conn.negotiated_cipher_suite().map(|s| s.suite())
        ),
        alpn: tls.conn.alpn_protocol().map(<[u8]>::to_vec),
        status,
        body: String::from_utf8_lossy(&read[end..end + length]).into_owned(),
    }
}

/// A get's blocks as the test's host gives them: larger than a loopback socket's buffers on both
/// sides together (4 MiB each by default on Linux and macOS), so each fills its socket however fast
/// its client reads. A Windows socket's send quota follows TCP's ideal send backlog, up to 16 MB,
/// and its client's receive window that client's retrieve rate, up to 16 MB (research/72 [9, 11]);
/// it takes all of a send under its quota, so the connection offers it a record at a time and
/// reads a refused send as full there (`listener::io`). Offered a block whole, it took each whole,
/// so no piece said it was full, however slow its client.
const BLOCK: usize = 32 << 20;

/// What a get's connection told the node its client's pace was (`door::Doorway::drained`): the
/// octets its client took once the socket was full and the time they took, summed over the
/// pieces, from a get of `blocks` blocks that the test's host gives as a node's driver gives a
/// block it read, each `read` after the last, and that its client reads `throttle`'s octets at a
/// time, waiting its time after each; none where no piece filled its socket.
fn paced(blocks: usize, read: Duration, throttle: Option<(usize, Duration)>) -> Option<(u64, u64)> {
    use mantle_protocol::object::{Reply, Status};
    let access = access();
    let key = s3::Key {
        id: access.id.clone(),
        secret: access.secret.clone(),
    };
    let size = blocks * BLOCK;
    let (paced, client) = runtime()
        .block_on(async move {
            let (mut doorway, submitter) = door::doorway(1, Waker::noop().clone()).unwrap();
            let listener = Listener::bind(plain(access), submitter).unwrap();
            let at = listener.local_addr().unwrap();
            let (told, mut heard) = hyper_rt::sync::channel::<()>(1).unwrap();
            let client = std::thread::spawn(move || {
                let signer = s3::Client::new(at, key, "us-east-1");
                let payload = mantle_s3::sigv4::hex(&mantle_s3::crypto::sha256(b"").unwrap());
                let head = signer.head("GET", "/bucket/key", &[], &payload);
                let mut tcp = TcpStream::connect(at).unwrap();
                tcp.write_all(head.as_bytes()).unwrap();
                let mut said = Vec::new();
                let mut byte = [0u8; 1];
                while !said.ends_with(b"\r\n\r\n") {
                    assert_eq!(tcp.read(&mut byte).unwrap(), 1, "closed before its head");
                    said.push(byte[0]);
                }
                assert!(said.starts_with(b"HTTP/1.1 200"), "{said:?}");
                let (piece, wait) = throttle.unwrap_or((BLOCK, Duration::ZERO));
                let mut buf = vec![0u8; piece];
                let mut left = size;
                while left > 0 {
                    let n = tcp.read(&mut buf[..piece.min(left)]).unwrap();
                    assert!(n > 0, "closed with {left} octets of its body unread");
                    left -= n;
                    // The client's own thread, no runtime's worker: its wait is its pace.
                    #[allow(clippy::disallowed_methods)]
                    std::thread::sleep(wait);
                }
                if told.blocking_send(()).is_err() {
                    panic!("the listener's side ended");
                }
            });
            let serve = async {
                let started = std::time::Instant::now();
                let request = loop {
                    match doorway.take() {
                        Some(door::Asked::Op(request)) => break request,
                        Some(door::Asked::Key(_)) => panic!("a root key's request asked a key"),
                        None if started.elapsed() > STOCK_PATIENCE => panic!("no request"),
                        None => hyper_rt::futures::sleep(1_000_000).await.unwrap(),
                    }
                };
                let reply = Reply {
                    size: u64::try_from(size).unwrap(),
                    etag: "0123456789abcdef0123456789abcdef".into(),
                    ..Reply::status(Status::Done)
                };
                let reply = door::Answer::Head(Box::new(reply));
                request.reply.send(reply).await.unwrap();
                let block = bytes::Bytes::from(vec![0x5a; BLOCK]);
                for _ in 0..blocks {
                    let read = u64::try_from(read.as_nanos()).unwrap();
                    hyper_rt::futures::sleep(read).await.unwrap();
                    let bytes = door::Answer::Bytes(block.clone(), std::time::Instant::now());
                    request.reply.send(bytes).await.unwrap();
                }
                request.reply.send(door::Answer::End).await.unwrap();
                // The client read every octet: the connection told what it took of each piece as
                // it wrote it, before it took the next.
                heard.recv().await.ok()?;
                doorway.drained()
            };
            match race2(listener.serve(), serve).await {
                Either::Second(paced) => (paced, client),
                Either::First(e) => panic!("the listener ended: {e}"),
            }
        })
        .unwrap();
    client.join().unwrap();
    paced
}

/// The octets a second a pace of `octets` in `ns` is.
fn rate((octets, ns): (u64, u64)) -> u128 {
    u128::from(octets) * 1_000_000_000 / u128::from(ns)
}

/// A get's client is paced by what its connection's socket took once it was full, from then to
/// each piece's last octet written, as a native get's is by its stream's flow-control window
/// (`host::Drain`): one that reads slower than the host reads its blocks, the reply's channel
/// full behind the piece being written, at no more than it reads; and one that reads faster than
/// the host reads them, the channel never full, at its own pace, past the slow one's. The host
/// learns the window a get holds being read from it (`crate::window`). Before, the S3 door timed
/// only a block the reply's channel held behind the one its connection had just begun, so the
/// fast client was never paced and its gets kept the window they were admitted with.
#[test]
fn a_gets_client_is_paced_by_what_its_socket_took_however_fast_it_reads() {
    // 256 KiB a read, 2 ms after each: at most 128 MiB a second.
    let throttle = (256 << 10, Duration::from_millis(2));
    let most = u128::try_from(throttle.0).unwrap() * 1_000_000_000 / throttle.1.as_nanos();
    let slow = paced(2, Duration::ZERO, Some(throttle)).expect("a slow client paced");
    assert!(rate(slow) <= most, "{} past {most}", rate(slow));
    // Blocks read 20 ms apart, the client reading as fast as its socket gives.
    let fast = paced(2, Duration::from_millis(20), None).expect("a fast client paced");
    assert!(rate(fast) > most, "{} within {most}", rate(fast));
}
