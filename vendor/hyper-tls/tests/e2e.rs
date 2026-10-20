//! hyper-tls between real processes over real TCP sockets on loopback (CLAUDE.md §1a), and against
//! upstream rustls 0.23.45, unmodified, in the other process: the interoperation oracle
//! (docs/transport.md §3.2, "Oracle").
//!
//! This binary is both sides. Run as a test it is the client, and for each scenario it spawns
//! itself with `HTLS_SERVER` set as the server: a process that shares nothing with it but the
//! kernel's sockets, serving the scenario's connections one after another and reporting each on
//! its standard output. Every scenario runs three ways: hyper-tls on both sides, a hyper-tls
//! client against an upstream server, and an upstream client against a hyper-tls server.
//!
//! - `tls13`, `tls12`, `tls12-tickets`: a full handshake and then a resumed one, the server's
//!   session cache or ticket and the client's session store carried across connections;
//! - `mutual13`, `mutual12`: the client presents a certificate the server requires, in the full
//!   handshake and the resumed one, and the server reports the certificate it verified;
//! - `untrusted-server13`, `untrusted-server12`: the server presents a chain from an authority the
//!   client does not trust;
//! - `untrusted-client13`, `untrusted-client12`: the client presents one the server does not trust;
//! - `missing-client13`: the server requires a certificate the client does not have;
//! - `wrong-name13`: the server's certificate does not name the server the client asked for.
//!
//! A connection that completes moves a mebibyte each way, every byte checked on both sides. A
//! refused one is refused on both sides with the typed error each implementation gives, and the
//! alert the refusing side sent is the error the other side reports.
//!
//! Every wait is for a fact: bytes from the peer, the end of its stream, or its process ending.
//! The only wall-clock bound is a failure guard on each socket read, far past anything a handshake
//! on loopback needs, so that a wedged peer fails the test instead of hanging it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    missing_docs
)]

use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use hyper_tls::pki_types::pem::PemObject;

/// The failure guard of a socket read: far past what a handshake or a mebibyte on loopback takes,
/// so that a peer that never answers fails the test instead of hanging it. Not a measure of
/// anything; every read ends sooner on the fact it waits for.
const GUARD: Duration = Duration::from_secs(120);
/// The bytes a completed connection moves each way.
const BODY: usize = 1 << 20;

/// Which implementation drives one side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Imp {
    Hyper,
    Upstream,
}

impl Imp {
    fn name(self) -> &'static str {
        match self {
            Self::Hyper => "hyper",
            Self::Upstream => "upstream",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "hyper" => Self::Hyper,
            "upstream" => Self::Upstream,
            _ => panic!("no implementation {name}"),
        }
    }
}

/// The certificate authorities of the test PKI (`test-ca/`): each signs its own server and client
/// chains, and trusts none of the other's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ca {
    /// The authority both sides trust.
    Trusted,
    /// One neither side trusts.
    Stranger,
}

impl Ca {
    fn file(self, name: &str) -> &'static [u8] {
        match (self, name) {
            (Self::Trusted, "ca.cert") => include_bytes!("../test-ca/ecdsa-p256/ca.cert"),
            (Self::Trusted, "end.fullchain") => {
                include_bytes!("../test-ca/ecdsa-p256/end.fullchain")
            }
            (Self::Trusted, "end.key") => include_bytes!("../test-ca/ecdsa-p256/end.key"),
            (Self::Trusted, "client.fullchain") => {
                include_bytes!("../test-ca/ecdsa-p256/client.fullchain")
            }
            (Self::Trusted, "client.key") => include_bytes!("../test-ca/ecdsa-p256/client.key"),
            (Self::Stranger, "end.fullchain") => include_bytes!("../test-ca/eddsa/end.fullchain"),
            (Self::Stranger, "end.key") => include_bytes!("../test-ca/eddsa/end.key"),
            (Self::Stranger, "client.fullchain") => {
                include_bytes!("../test-ca/eddsa/client.fullchain")
            }
            (Self::Stranger, "client.key") => include_bytes!("../test-ca/eddsa/client.key"),
            _ => unreachable!("no file {name}"),
        }
    }
}

/// How a connection must end, on both sides.
#[derive(Clone, Copy, Debug)]
enum Expect {
    /// Completed, of this kind and version, a mebibyte each way.
    Done {
        kind: &'static str,
        version: &'static str,
    },
    /// Refused: the client's error and the server's.
    Refused { client: Refusal, server: Refusal },
}

/// A refusal, as either implementation types it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
    /// `Error::InvalidCertificate(CertificateError::UnknownIssuer)`.
    UnknownIssuer,
    /// `Error::InvalidCertificate(CertificateError::NotValidForName{,Context})`.
    NotValidForName,
    /// `Error::NoCertificatesPresented`.
    NoCertificates,
    /// `Error::AlertReceived` of the named alert.
    Alert(&'static str),
}

/// One scenario: the server's configuration and the connections the client makes to it.
struct Scenario {
    name: &'static str,
    /// The versions the server takes: `13`, `12` or both. The client offers both, always.
    versions: &'static [u8],
    /// The authority whose chain the server presents.
    server_chain: Ca,
    /// Whether the server requires a client certificate from the trusted authority.
    mutual: bool,
    /// The authority whose client chain the client presents, if any.
    client_chain: Option<Ca>,
    /// Whether the server issues TLS 1.2 tickets.
    tickets: bool,
    /// The name the client asks for.
    name_asked: &'static str,
    connections: &'static [Expect],
}

const FULL13: Expect = Expect::Done {
    kind: "Full",
    version: "TLSv1_3",
};
const RESUMED13: Expect = Expect::Done {
    kind: "Resumed",
    version: "TLSv1_3",
};
const FULL12: Expect = Expect::Done {
    kind: "Full",
    version: "TLSv1_2",
};
const RESUMED12: Expect = Expect::Done {
    kind: "Resumed",
    version: "TLSv1_2",
};

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "tls13",
        versions: &[13],
        server_chain: Ca::Trusted,
        mutual: false,
        client_chain: None,
        tickets: false,
        name_asked: "testserver.com",
        connections: &[FULL13, RESUMED13],
    },
    Scenario {
        name: "tls12",
        versions: &[12],
        server_chain: Ca::Trusted,
        mutual: false,
        client_chain: None,
        tickets: false,
        name_asked: "testserver.com",
        connections: &[FULL12, RESUMED12],
    },
    Scenario {
        name: "tls12-tickets",
        versions: &[12],
        server_chain: Ca::Trusted,
        mutual: false,
        client_chain: None,
        tickets: true,
        name_asked: "testserver.com",
        connections: &[FULL12, RESUMED12],
    },
    Scenario {
        name: "mutual13",
        versions: &[13],
        server_chain: Ca::Trusted,
        mutual: true,
        client_chain: Some(Ca::Trusted),
        tickets: false,
        name_asked: "testserver.com",
        connections: &[FULL13, RESUMED13],
    },
    Scenario {
        name: "mutual12",
        versions: &[12],
        server_chain: Ca::Trusted,
        mutual: true,
        client_chain: Some(Ca::Trusted),
        tickets: false,
        name_asked: "testserver.com",
        connections: &[FULL12, RESUMED12],
    },
    Scenario {
        name: "untrusted-server13",
        versions: &[13],
        server_chain: Ca::Stranger,
        mutual: false,
        client_chain: None,
        tickets: false,
        name_asked: "testserver.com",
        connections: &[Expect::Refused {
            client: Refusal::UnknownIssuer,
            server: Refusal::Alert("UnknownCA"),
        }],
    },
    Scenario {
        name: "untrusted-server12",
        versions: &[12],
        server_chain: Ca::Stranger,
        mutual: false,
        client_chain: None,
        tickets: false,
        name_asked: "testserver.com",
        connections: &[Expect::Refused {
            client: Refusal::UnknownIssuer,
            server: Refusal::Alert("UnknownCA"),
        }],
    },
    Scenario {
        name: "untrusted-client13",
        versions: &[13],
        server_chain: Ca::Trusted,
        mutual: true,
        client_chain: Some(Ca::Stranger),
        tickets: false,
        name_asked: "testserver.com",
        connections: &[Expect::Refused {
            client: Refusal::Alert("UnknownCA"),
            server: Refusal::UnknownIssuer,
        }],
    },
    Scenario {
        name: "untrusted-client12",
        versions: &[12],
        server_chain: Ca::Trusted,
        mutual: true,
        client_chain: Some(Ca::Stranger),
        tickets: false,
        name_asked: "testserver.com",
        connections: &[Expect::Refused {
            client: Refusal::Alert("UnknownCA"),
            server: Refusal::UnknownIssuer,
        }],
    },
    Scenario {
        name: "missing-client13",
        versions: &[13],
        server_chain: Ca::Trusted,
        mutual: true,
        client_chain: None,
        tickets: false,
        name_asked: "testserver.com",
        connections: &[Expect::Refused {
            client: Refusal::Alert("CertificateRequired"),
            server: Refusal::NoCertificates,
        }],
    },
    Scenario {
        name: "wrong-name13",
        versions: &[13],
        server_chain: Ca::Trusted,
        mutual: false,
        client_chain: None,
        tickets: false,
        name_asked: "otherserver.com",
        connections: &[Expect::Refused {
            client: Refusal::NotValidForName,
            server: Refusal::Alert("BadCertificate"),
        }],
    },
];

fn scenario(name: &str) -> &'static Scenario {
    SCENARIOS
        .iter()
        .find(|scenario| scenario.name == name)
        .unwrap_or_else(|| panic!("no scenario {name}"))
}

/// A TLS connection as the driver sees it, either implementation and either side, with the
/// configuration it is driven with.
trait Conn {
    fn wants_write(&self) -> bool;
    fn is_handshaking(&self) -> bool;
    fn write_tls(&mut self, sink: &mut dyn Write) -> io::Result<usize>;
    fn read_tls(&mut self, source: &mut dyn Read) -> io::Result<usize>;
    /// Processes what `read_tls` took; a refusal is the implementation's typed error, mapped.
    fn process(&mut self) -> Result<(), String>;
    fn send(&mut self, data: &[u8]) -> io::Result<usize>;
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    fn close(&mut self);
    /// The handshake's kind and version, and the peer's end-entity certificate.
    fn report(&self) -> (String, String, Option<Vec<u8>>);
}

macro_rules! conn_impl {
    ($conn:ty, $config:ty, |$c:ident, $cfg:ident| $process:expr) => {
        impl Conn for ($conn, $config) {
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
            fn process(&mut self) -> Result<(), String> {
                let ($c, $cfg) = self;
                $process.map(|_| ())
            }
            fn send(&mut self, data: &[u8]) -> io::Result<usize> {
                self.0.writer().write(data)
            }
            fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                self.0.reader().read(buf)
            }
            fn close(&mut self) {
                self.0.send_close_notify();
            }
            fn report(&self) -> (String, String, Option<Vec<u8>>) {
                (
                    format!("{:?}", self.0.handshake_kind().unwrap()),
                    format!("{:?}", self.0.protocol_version().unwrap()),
                    self.0
                        .peer_certificates()
                        .and_then(|chain| chain.first())
                        .map(|leaf| leaf.as_ref().to_vec()),
                )
            }
        }
    };
}

conn_impl!(
    hyper_tls::ClientConnection,
    &mut hyper_tls::ClientConfig,
    |conn, config| conn.process_new_packets(config).map_err(hyper::refusal)
);
conn_impl!(
    hyper_tls::ServerConnection,
    &mut hyper_tls::ServerConfig,
    |conn, config| conn.process_new_packets(config).map_err(hyper::refusal)
);
conn_impl!(upstream_rustls::ClientConnection, (), |conn, _config| conn
    .process_new_packets()
    .map_err(upstream::refusal));
conn_impl!(upstream_rustls::ServerConnection, (), |conn, _config| conn
    .process_new_packets()
    .map_err(upstream::refusal));

/// hyper-tls's configurations for a scenario.
mod hyper {
    use hyper_tls::crypto::aws_lc_rs::Ticketer;
    use hyper_tls::pki_types::pem::PemObject;
    use hyper_tls::pki_types::{CertificateDer, PrivateKeyDer};
    use hyper_tls::server::WebPkiClientVerifier;
    use hyper_tls::{CertificateError, ClientConfig, Error, RootCertStore, ServerConfig};

    use super::{Ca, Scenario};

    pub(super) fn refusal(error: Error) -> String {
        match error {
            Error::InvalidCertificate(CertificateError::UnknownIssuer) => "UnknownIssuer".into(),
            Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            ) => "NotValidForName".into(),
            Error::NoCertificatesPresented => "NoCertificates".into(),
            Error::AlertReceived(alert) => format!("Alert({alert:?})"),
            other => format!("{other:?}"),
        }
    }

    fn chain(ca: Ca, name: &str) -> Vec<CertificateDer<'static>> {
        CertificateDer::pem_slice_iter(ca.file(name))
            .map(Result::unwrap)
            .collect()
    }

    fn roots() -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(Ca::Trusted.file("ca.cert")).unwrap())
            .unwrap();
        roots
    }

    pub(super) fn server(scenario: &Scenario) -> ServerConfig {
        let versions: Vec<_> = scenario
            .versions
            .iter()
            .map(|v| match v {
                13 => &hyper_tls::version::TLS13,
                _ => &hyper_tls::version::TLS12,
            })
            .collect();
        let builder = ServerConfig::builder_with_protocol_versions(&versions).unwrap();
        let builder = match scenario.mutual {
            true => builder
                .with_client_cert_verifier(WebPkiClientVerifier::builder(roots()).build().unwrap()),
            false => builder.with_no_client_auth(),
        };
        let key = PrivateKeyDer::from_pem_slice(scenario.server_chain.file("end.key")).unwrap();
        let mut config = builder
            .with_single_cert(chain(scenario.server_chain, "end.fullchain"), key)
            .unwrap();
        if scenario.tickets {
            config.ticketer = Ticketer::new().unwrap();
        }
        config
    }

    pub(super) fn client(scenario: &Scenario) -> ClientConfig {
        let builder = ClientConfig::builder()
            .unwrap()
            .with_root_certificates(roots());
        match scenario.client_chain {
            Some(ca) => {
                let key = PrivateKeyDer::from_pem_slice(ca.file("client.key")).unwrap();
                builder
                    .with_client_auth_cert(chain(ca, "client.fullchain"), key)
                    .unwrap()
            }
            None => builder.with_no_client_auth(),
        }
    }
}

/// Upstream rustls's configurations for a scenario: the same, shared by `Arc` as upstream has it.
mod upstream {
    use std::sync::Arc;

    use upstream_rustls::crypto::aws_lc_rs::Ticketer;
    use upstream_rustls::pki_types::pem::PemObject;
    use upstream_rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use upstream_rustls::server::WebPkiClientVerifier;
    use upstream_rustls::{CertificateError, ClientConfig, Error, RootCertStore, ServerConfig};

    use super::{Ca, Scenario};

    pub(super) fn refusal(error: Error) -> String {
        match error {
            Error::InvalidCertificate(CertificateError::UnknownIssuer) => "UnknownIssuer".into(),
            Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            ) => "NotValidForName".into(),
            Error::NoCertificatesPresented => "NoCertificates".into(),
            Error::AlertReceived(alert) => format!("Alert({alert:?})"),
            other => format!("{other:?}"),
        }
    }

    fn chain(ca: Ca, name: &str) -> Vec<CertificateDer<'static>> {
        CertificateDer::pem_slice_iter(ca.file(name))
            .map(Result::unwrap)
            .collect()
    }

    fn roots() -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(Ca::Trusted.file("ca.cert")).unwrap())
            .unwrap();
        roots
    }

    pub(super) fn server(scenario: &Scenario) -> Arc<ServerConfig> {
        let versions: Vec<_> = scenario
            .versions
            .iter()
            .map(|v| match v {
                13 => &upstream_rustls::version::TLS13,
                _ => &upstream_rustls::version::TLS12,
            })
            .collect();
        let builder = ServerConfig::builder_with_protocol_versions(&versions);
        let builder = match scenario.mutual {
            true => builder.with_client_cert_verifier(
                WebPkiClientVerifier::builder(Arc::new(roots()))
                    .build()
                    .unwrap(),
            ),
            false => builder.with_no_client_auth(),
        };
        let key = PrivateKeyDer::from_pem_slice(scenario.server_chain.file("end.key")).unwrap();
        let mut config = builder
            .with_single_cert(chain(scenario.server_chain, "end.fullchain"), key)
            .unwrap();
        if scenario.tickets {
            config.ticketer = Ticketer::new().unwrap();
        }
        Arc::new(config)
    }

    pub(super) fn client(scenario: &Scenario) -> Arc<ClientConfig> {
        let builder = ClientConfig::builder().with_root_certificates(roots());
        Arc::new(match scenario.client_chain {
            Some(ca) => {
                let key = PrivateKeyDer::from_pem_slice(ca.file("client.key")).unwrap();
                builder
                    .with_client_auth_cert(chain(ca, "client.fullchain"), key)
                    .unwrap()
            }
            None => builder.with_no_client_auth(),
        })
    }
}

/// Sends what `conn` has to send.
fn flush(conn: &mut dyn Conn, socket: &mut TcpStream) -> io::Result<()> {
    while conn.wants_write() {
        conn.write_tls(socket)?;
    }
    Ok(())
}

/// Reads what the peer sent into `conn` and processes it; `Ok(false)` once the peer's stream ended.
fn take(conn: &mut dyn Conn, socket: &mut TcpStream) -> Result<bool, String> {
    match conn.read_tls(socket) {
        Ok(0) => Ok(false),
        Ok(_) => conn.process().map(|()| true),
        Err(error)
            if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::TimedOut =>
        {
            Err(format!(
                "the peer sent nothing for the {GUARD:?} failure guard"
            ))
        }
        Err(error) => Err(format!("io: {error}")),
    }
}

/// Drives the handshake to its end; the refusal, as the implementation types it, if there is one.
/// A side that refuses sends its alert before it returns.
fn handshake(conn: &mut dyn Conn, socket: &mut TcpStream) -> Result<(), String> {
    let result = (|| {
        while conn.is_handshaking() {
            flush(conn, socket).map_err(|error| format!("io: {error}"))?;
            if conn.is_handshaking() && !take(conn, socket)? {
                return Err("the peer closed during the handshake".into());
            }
        }
        flush(conn, socket).map_err(|error| format!("io: {error}"))
    })();
    if result.is_err() {
        let _ = flush(conn, socket);
    }
    result
}

/// Writes all of `body`.
fn send_all(conn: &mut dyn Conn, socket: &mut TcpStream, body: &[u8]) -> Result<(), String> {
    let mut sent = 0;
    while sent < body.len() {
        sent += conn
            .send(&body[sent..])
            .map_err(|error| format!("io: {error}"))?;
        flush(conn, socket).map_err(|error| format!("io: {error}"))?;
    }
    Ok(())
}

/// Reads as many bytes as `body` holds, every one checked against it.
fn receive_all(conn: &mut dyn Conn, socket: &mut TcpStream, body: &[u8]) -> Result<(), String> {
    let mut received = 0;
    let mut buf = vec![0; 65_536];
    while received < body.len() {
        match conn.recv(&mut buf) {
            Ok(0) => return Err("the peer closed its TLS stream early".into()),
            Ok(n) => {
                if buf[..n] != body[received..received + n] {
                    return Err(format!("the bytes at {received} differ"));
                }
                received += n;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if !take(conn, socket)? {
                    return Err("the peer closed mid-body".into());
                }
                // A peer that refused us late (a client certificate, under TLS 1.3) said so in an
                // alert, which processing reports.
            }
            Err(error) => return Err(format!("io: {error}")),
        }
    }
    Ok(())
}

/// Reads until the peer's stream ends, processing what arrives: its close_notify and, for a
/// client, the tickets a TLS 1.3 server sends after the handshake.
fn drain(conn: &mut dyn Conn, socket: &mut TcpStream) -> Result<(), String> {
    while take(conn, socket)? {}
    Ok(())
}

/// The bytes every connection moves: a pattern a misplaced or lost byte cannot match.
fn body() -> Vec<u8> {
    (0..BODY)
        .map(|i| u8::try_from(i % 251).unwrap() ^ u8::try_from((i >> 8) % 256).unwrap())
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The server process: serves the scenario's connections in turn, echoing what each client sends,
/// and reports each on its standard output.
fn server_process(imp: Imp, scenario: &Scenario) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    println!("listening {}", listener.local_addr().unwrap().port());
    io::stdout().flush().unwrap();
    let mut hyper_config = (imp == Imp::Hyper).then(|| hyper::server(scenario));
    let upstream_config = (imp == Imp::Upstream).then(|| upstream::server(scenario));
    let body = body();
    for _ in scenario.connections {
        let (mut socket, _) = listener.accept().unwrap();
        socket.set_read_timeout(Some(GUARD)).unwrap();
        let mut hyper_conn;
        let mut upstream_conn;
        let conn: &mut dyn Conn = match imp {
            Imp::Hyper => {
                let config = hyper_config.as_mut().unwrap();
                hyper_conn = (hyper_tls::ServerConnection::new(config).unwrap(), config);
                &mut hyper_conn
            }
            Imp::Upstream => {
                let config = upstream_config.as_ref().unwrap().clone();
                upstream_conn = (upstream_rustls::ServerConnection::new(config).unwrap(), ());
                &mut upstream_conn
            }
        };
        let outcome = handshake(conn, &mut socket).and_then(|()| {
            receive_all(conn, &mut socket, &body)?;
            send_all(conn, &mut socket, &body)?;
            conn.close();
            flush(conn, &mut socket).map_err(|error| format!("io: {error}"))
        });
        match outcome {
            Ok(()) => {
                let (kind, version, peer) = conn.report();
                println!(
                    "served ok {kind} {version} {}",
                    peer.map_or_else(|| "-".into(), |leaf| hex(&leaf))
                );
            }
            Err(refusal) => println!("served refused {refusal}"),
        }
        io::stdout().flush().unwrap();
        close(socket);
    }
}

/// Ends writing on `socket` and reads until the peer's stream ends, so that closing it never
/// resets a stream the peer has not finished reading.
fn close(mut socket: TcpStream) {
    let _ = socket.shutdown(Shutdown::Write);
    let mut sink = [0; 4096];
    while matches!(socket.read(&mut sink), Ok(n) if n > 0) {}
}

/// A server process and where it listens.
struct Server {
    child: Child,
    lines: BufReader<ChildStdout>,
    address: SocketAddr,
}

impl Server {
    fn spawn(imp: Imp, scenario: &Scenario) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .env("HTLS_SERVER", format!("{}:{}", imp.name(), scenario.name))
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap());
        let port: u16 = Self::line(&mut lines)
            .strip_prefix("listening ")
            .unwrap()
            .parse()
            .unwrap();
        Self {
            child,
            lines,
            address: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    /// The next line the server reports: a fact it states once it has it, or the end of its output
    /// if its process ended.
    fn line(lines: &mut BufReader<ChildStdout>) -> String {
        let mut line = String::new();
        lines.read_line(&mut line).unwrap();
        line.trim().to_owned()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The client's configuration for a scenario, kept across its connections so that a later one
/// resumes: the session store is the configuration's.
enum ClientConfig {
    Hyper(Box<hyper_tls::ClientConfig>),
    Upstream(std::sync::Arc<upstream_rustls::ClientConfig>),
}

/// Runs one scenario with the client on `client` and the server process on `server`; a line of
/// what it saw.
fn run(client: Imp, server: Imp, scenario: &Scenario) -> String {
    let mut process = Server::spawn(server, scenario);
    let address = process.address;
    let mut config = match client {
        Imp::Hyper => ClientConfig::Hyper(Box::new(hyper::client(scenario))),
        Imp::Upstream => ClientConfig::Upstream(upstream::client(scenario)),
    };
    let body = body();
    let mut seen = Vec::new();
    for (index, expect) in scenario.connections.iter().enumerate() {
        let began = Instant::now();
        let mut socket = TcpStream::connect(address).unwrap();
        socket.set_read_timeout(Some(GUARD)).unwrap();
        socket.set_nodelay(true).unwrap();
        let name = hyper_tls::pki_types::ServerName::try_from(scenario.name_asked).unwrap();
        let mut hyper_conn;
        let mut upstream_conn;
        let conn: &mut dyn Conn = match &mut config {
            ClientConfig::Hyper(config) => {
                let config = &mut **config;
                hyper_conn = (
                    hyper_tls::ClientConnection::new(config, name).unwrap(),
                    config,
                );
                &mut hyper_conn
            }
            ClientConfig::Upstream(config) => {
                upstream_conn = (
                    upstream_rustls::ClientConnection::new(config.clone(), name).unwrap(),
                    (),
                );
                &mut upstream_conn
            }
        };
        // The client sends its body, reads the server's back, and reads until the server's stream
        // ends before it closes its own: closing with unread bytes would reset the server's.
        let outcome = handshake(conn, &mut socket).and_then(|()| {
            send_all(conn, &mut socket, &body)?;
            receive_all(conn, &mut socket, &body)?;
            drain(conn, &mut socket)?;
            conn.close();
            flush(conn, &mut socket).map_err(|error| format!("io: {error}"))
        });
        let report = outcome.as_ref().ok().map(|()| conn.report());
        // Whichever side refused, neither closes with bytes unread, which would reset the other's
        // stream and could lose the alert in it: each ends its writing and reads to the end.
        close(socket);
        let served = Server::line(&mut process.lines);
        let context = format!(
            "{} client, {} server, {}, connection {index}",
            client.name(),
            server.name(),
            scenario.name
        );
        match *expect {
            Expect::Done { kind, version } => {
                let (our_kind, our_version, peer) =
                    report.unwrap_or_else(|| panic!("{context}: {outcome:?}"));
                assert_eq!(
                    (our_kind.as_str(), our_version.as_str()),
                    (kind, version),
                    "{context}"
                );
                assert_eq!(
                    peer,
                    Some(
                        hyper_tls::pki_types::CertificateDer::pem_slice_iter(
                            scenario.server_chain.file("end.fullchain")
                        )
                        .next()
                        .unwrap()
                        .unwrap()
                        .as_ref()
                        .to_vec()
                    ),
                    "{context}: the server's certificate"
                );
                let client_leaf = scenario.client_chain.map_or_else(
                    || "-".to_owned(),
                    |ca| {
                        hex(hyper_tls::pki_types::CertificateDer::pem_slice_iter(
                            ca.file("client.fullchain"),
                        )
                        .next()
                        .unwrap()
                        .unwrap()
                        .as_ref())
                    },
                );
                assert_eq!(
                    served,
                    format!("served ok {kind} {version} {client_leaf}"),
                    "{context}: the server's report"
                );
            }
            Expect::Refused {
                client: client_refusal,
                server: server_refusal,
            } => {
                assert_eq!(outcome, Err(text(client_refusal)), "{context}");
                assert_eq!(
                    served,
                    format!("served refused {}", text(server_refusal)),
                    "{context}: the server's report"
                );
            }
        }
        seen.push(format!(
            "{:?} in {:?}",
            expect_label(expect),
            began.elapsed()
        ));
    }
    format!(
        "{} client, {} server: {}",
        client.name(),
        server.name(),
        seen.join(", ")
    )
}

/// A refusal as the drivers report it.
fn text(refusal: Refusal) -> String {
    match refusal {
        Refusal::UnknownIssuer => "UnknownIssuer".into(),
        Refusal::NotValidForName => "NotValidForName".into(),
        Refusal::NoCertificates => "NoCertificates".into(),
        Refusal::Alert(alert) => format!("Alert({alert})"),
    }
}

fn expect_label(expect: &Expect) -> String {
    match expect {
        Expect::Done { kind, version } => format!("{kind} {version}"),
        Expect::Refused { client, server } => format!("refused ({client:?} / {server:?})"),
    }
}

fn main() {
    if let Ok(server) = std::env::var("HTLS_SERVER") {
        let (imp, name) = server.split_once(':').unwrap();
        server_process(Imp::parse(imp), scenario(name));
        return;
    }
    let only = std::env::args()
        .skip(1)
        .find(|argument| !argument.starts_with('-'));
    let pairs = [
        (Imp::Hyper, Imp::Hyper),
        (Imp::Hyper, Imp::Upstream),
        (Imp::Upstream, Imp::Hyper),
    ];
    for scenario in SCENARIOS {
        if only
            .as_deref()
            .is_some_and(|only| !scenario.name.contains(only))
        {
            continue;
        }
        for (client, server) in pairs {
            let began = Instant::now();
            let report = run(client, server, scenario);
            println!(
                "e2e {}: ok in {:?}: {report}",
                scenario.name,
                began.elapsed()
            );
        }
    }
}
