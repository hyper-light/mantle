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
use mantle_node::listener::{Listener, PIECE, Settings, tasks};
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

/// A body longer than the pieces a connection reads ahead of the host, by one piece.
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
                                Piece::Bytes(bytes) => {
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

/// A connection with a request in flight holds the same bytes whichever its place, over plain
/// HTTP and over TLS, and no more than `listener::CONNECTION_BYTES` states.
#[test]
fn a_listener_connection_holds_its_stated_bytes() {
    for tls in [false, true] {
        let counts: Vec<i64> = [1, 2, 4].into_iter().map(|n| held(n, tls)).collect();
        let each = (counts[2] - counts[0]) / 3;
        eprintln!("tls {tls}: held {counts:?}, {each} a connection");
        assert!(each > 0, "{counts:?}");
        assert!(
            u64::try_from(each).unwrap() <= mantle_node::listener::CONNECTION_BYTES,
            "tls {tls}: {each} a connection past {}",
            mantle_node::listener::CONNECTION_BYTES
        );
    }
}
