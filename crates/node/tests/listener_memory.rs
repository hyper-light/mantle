//! What an S3 listener's connection holds with a request in flight (docs/design/node.md §4.5;
//! the constants audit of 2026-11-12, §3 #3): its task, its socket's and TLS's state, its request
//! as the door holds it and the pieces of its body the connection read ahead of the host, counted
//! by hyper-measure's counting allocator on the listener's thread, which the clients' threads do
//! not touch. Each client sends a signed PUT whose body is longer than the pieces a connection
//! reads ahead; the test takes each request from the door and holds the pieces it is sent, as the
//! host holds them while it writes, so every connection stands at its most. What one connection
//! adds is the difference between counts of connections, and it is the same for each.

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
use std::task::Waker;

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
fn held(connections: usize, tls: bool) -> i64 {
    let chain = tls.then(chain);
    let mut runtime = runtime();
    let (live, clients) = runtime
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
                    let authority = authority.clone();
                    std::thread::spawn(move || client(at, authority))
                })
                .collect();
            alloc::resume();
            // The host's side: each request taken from the door as it comes and its body's pieces
            // taken as the connection sends them, until each connection has sent what it reads
            // ahead; then the count is read.
            let hold = async {
                while requests.len() < connections {
                    match doorway.waiting() {
                        Some(_) => requests.push(doorway.take().unwrap()),
                        None => hyper_rt::futures::sleep(1_000_000).await.unwrap(),
                    }
                }
                for request in &mut requests {
                    // A put's driver wants its body: the connection reads it from here on.
                    request.reply.try_send(door::Answer::Ready).unwrap();
                    let mut taken = 0;
                    while taken < 2 * PIECE {
                        match request.body.recv().await.unwrap() {
                            Piece::Bytes(bytes) => {
                                taken += bytes.len();
                                pieces.push(bytes);
                            }
                            other => panic!("{other:?}"),
                        }
                    }
                }
                let live = alloc::read().live;
                (live, requests, pieces)
            };
            let live = match race2(listener.serve(), hold).await {
                Either::Second((live, requests, pieces)) => {
                    drop((requests, pieces));
                    live
                }
                Either::First(e) => panic!("the listener ended: {e}"),
            };
            alloc::end();
            (live - bound, clients)
        })
        .unwrap();
    // The connections end with the runtime that holds them, and their clients with them.
    drop(runtime);
    for client in clients {
        let _ = client.join();
    }
    live
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
