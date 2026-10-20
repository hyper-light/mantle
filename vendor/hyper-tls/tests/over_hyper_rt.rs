//! TLS composes on top of hyper-rt's TCP (hyper-rt docs/runtime.md §5.2): hyper-tls is sans-I/O, so a TLS session
//! over a `TcpStream` is the session's bytes read from and written to the stream. Do: a server task and a
//! client task on one shard, an HTTP/1.1 request over TLS 1.3 and a mebibyte response, the certificate from
//! hyper-tls's test PKI checked by name. Expect: the handshake completes as TLS 1.3, the request arrives
//! whole, and every byte of the response arrives in order (enough bytes that the send buffer fills and the
//! writer awaits writability).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation
)]

use std::io::{self, Read, Write};

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::tcp::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use hyper_tls::pki_types::pem::PemObject;
use hyper_tls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use hyper_tls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection};

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Shape: the response body, past any loopback send buffer, so the writer waits on writability.
const BODY: usize = 1 << 20;
/// Shape: the stream reads' buffer, one TLS record's worth and more.
const READ: usize = 1 << 16;
/// Format: the name the test PKI's server certificate carries.
const NAME: &str = "testserver.com";

const CA: &[u8] = include_bytes!("../test-ca/ecdsa-p256/ca.cert");
const CHAIN: &[u8] = include_bytes!("../test-ca/ecdsa-p256/end.fullchain");
const KEY: &[u8] = include_bytes!("../test-ca/ecdsa-p256/end.key");

/// One side's session and its configuration, as hyper-tls drives them.
trait Session {
    fn wants_write(&self) -> bool;
    fn is_handshaking(&self) -> bool;
    fn write_tls(&mut self, sink: &mut dyn Write) -> io::Result<usize>;
    fn read_tls(&mut self, source: &mut dyn Read) -> io::Result<usize>;
    fn process(&mut self);
    fn send(&mut self, data: &[u8]) -> io::Result<usize>;
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    fn version(&self) -> String;
}

macro_rules! session {
    ($conn:ty, $config:ty) => {
        impl Session for ($conn, $config) {
            fn wants_write(&self) -> bool {
                self.0.wants_write()
            }
            fn is_handshaking(&self) -> bool {
                self.0.is_handshaking()
            }
            fn write_tls(&mut self, sink: &mut dyn Write) -> io::Result<usize> {
                self.0.write_tls(sink)
            }
            fn read_tls(&mut self, source: &mut dyn Read) -> io::Result<usize> {
                self.0.read_tls(source)
            }
            fn process(&mut self) {
                self.0.process_new_packets(&mut self.1).unwrap();
            }
            fn send(&mut self, data: &[u8]) -> io::Result<usize> {
                self.0.writer().write(data)
            }
            fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                self.0.reader().read(buf)
            }
            fn version(&self) -> String {
                format!("{:?}", self.0.protocol_version().unwrap())
            }
        }
    };
}
session!(ClientConnection, ClientConfig);
session!(ServerConnection, ServerConfig);

/// Writes everything the session has to send to the stream, awaiting writability as it fills.
async fn flush(session: &mut dyn Session, stream: &TcpStream) {
    let mut out = Vec::new();
    while session.wants_write() {
        out.clear();
        session.write_tls(&mut out).unwrap();
        stream.write_all(&out).await.unwrap();
    }
}

/// Ciphertext read from the stream and not yet taken by the session: the session takes records only as
/// fast as its plaintext is read, so a read's leftover waits here (backpressure, not a bigger buffer).
struct Inbound {
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

impl Inbound {
    fn new() -> Self {
        Self {
            buf: vec![0u8; READ],
            start: 0,
            end: 0,
        }
    }
}

/// Feeds the session what is pending, reading the stream first when nothing is; false at end of stream.
async fn take(session: &mut dyn Session, stream: &TcpStream, inbound: &mut Inbound) -> bool {
    if inbound.start == inbound.end {
        let n = stream.read(&mut inbound.buf).await.unwrap();
        if n == 0 {
            return false;
        }
        (inbound.start, inbound.end) = (0, n);
    }
    let mut pending = &inbound.buf[inbound.start..inbound.end];
    let before = pending.len();
    match session.read_tls(&mut pending) {
        Ok(_) => {}
        // The plaintext buffer is full: the caller reads plaintext before more is taken.
        Err(error) if error.kind() == io::ErrorKind::Other => {}
        Err(error) => panic!("{error}"),
    }
    inbound.start += before - pending.len();
    session.process();
    true
}

async fn handshake(session: &mut dyn Session, stream: &TcpStream, buf: &mut Inbound) {
    while session.is_handshaking() {
        flush(session, stream).await;
        if session.is_handshaking() {
            assert!(
                take(session, stream, buf).await,
                "the peer closed mid-handshake"
            );
        }
    }
    flush(session, stream).await;
}

/// Reads plaintext until `done` says the bytes so far are complete.
async fn read_until(
    session: &mut dyn Session,
    stream: &TcpStream,
    buf: &mut Inbound,
    done: impl Fn(&[u8]) -> bool,
) -> Vec<u8> {
    let mut got = Vec::new();
    let mut plain = vec![0u8; READ];
    while !done(&got) {
        match session.recv(&mut plain) {
            Ok(0) => panic!("the TLS stream ended early"),
            Ok(n) => got.extend_from_slice(&plain[..n]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert!(take(session, stream, buf).await, "the peer closed early");
            }
            Err(error) => panic!("{error}"),
        }
    }
    got
}

async fn send_all(session: &mut dyn Session, stream: &TcpStream, data: &[u8]) {
    let mut sent = 0;
    while sent < data.len() {
        sent += session.send(&data[sent..]).unwrap();
        flush(session, stream).await;
    }
}

fn body() -> Vec<u8> {
    (0..BODY).map(|i| (i % 251) as u8).collect()
}

fn server_config() -> ServerConfig {
    let chain = CertificateDer::pem_slice_iter(CHAIN)
        .map(Result::unwrap)
        .collect();
    ServerConfig::builder()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, PrivateKeyDer::from_pem_slice(KEY).unwrap())
        .unwrap()
}

fn client_config() -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(CA).unwrap())
        .unwrap();
    ClientConfig::builder()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

fn header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|at| at + 4)
}

#[test]
fn an_https_exchange_runs_over_the_runtimes_tcp() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let (version, response) = rt
        .block_on(async {
            let listener =
                TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 4).unwrap();
            let addr = listener.local_addr().unwrap();
            hyper_rt::futures::spawn_detached(async move {
                let stream = listener.accept().await.unwrap();
                let config = server_config();
                let conn = ServerConnection::new(&config).unwrap();
                let mut session = (conn, config);
                let mut buf = Inbound::new();
                handshake(&mut session, &stream, &mut buf).await;
                let request =
                    read_until(&mut session, &stream, &mut buf, |b| header_end(b).is_some()).await;
                assert!(request.starts_with(b"GET /body HTTP/1.1\r\n"));
                let body = body();
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                send_all(&mut session, &stream, head.as_bytes()).await;
                send_all(&mut session, &stream, &body).await;
                session.0.send_close_notify();
                flush(&mut session, &stream).await;
                stream.shutdown(Shutdown::Write).unwrap();
            })
            .unwrap();
            let stream = TcpStream::connect(addr).await.unwrap();
            let mut config = client_config();
            let conn =
                ClientConnection::new(&mut config, ServerName::try_from(NAME).unwrap()).unwrap();
            let mut session = (conn, config);
            let mut buf = Inbound::new();
            handshake(&mut session, &stream, &mut buf).await;
            send_all(
                &mut session,
                &stream,
                format!("GET /body HTTP/1.1\r\nHost: {NAME}\r\n\r\n").as_bytes(),
            )
            .await;
            let response = read_until(&mut session, &stream, &mut buf, |b| {
                header_end(b).is_some_and(|end| b.len() >= end + BODY)
            })
            .await;
            (session.version(), response)
        })
        .unwrap();
    assert_eq!(version, "TLSv1_3");
    let end = header_end(&response).unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert_eq!(response.len(), end + BODY, "the whole body, no more");
    assert!(response[end..] == body()[..], "every byte in order");
}
