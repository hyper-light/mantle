//! The S3 listener (docs/design/node.md §4.1–§4.5): stock S3 clients' HTTP/1.1, on hyper-rt's TCP,
//! through mantle's own codec (`mantle_http`), each request routed, authenticated and handed to
//! the node's object host in-process ([`crate::door`]), with no loopback QUIC hop.
//!
//! The listener accepts on the node's shard under its connection budget, past which a connection
//! is answered `503 SlowDown` and closed from a reserve of its own (never closed with no answer,
//! which a stock client can take for a part done), and runs each connection as a task of its own,
//! so the runtime wakes the one whose socket or channel moved. A connection whose client says
//! nothing for a stock client's patience is let go, its slot with it. A connection serves its
//! requests one at a time, in order (RFC 9112 §9.3), each through [`conn`]: its head parsed, its
//! route and signature checked ([`request`]), its operation handed over, its body read only as the
//! host's driver wants it, and the host's reply written as S3's response ([`respond`]).
//!
//! Plain HTTP on a loopback address, TLS elsewhere with the endpoint's P-256 certificate (node.md
//! §1.5; research/84), TLS 1.2 beside 1.3. A connection's TLS configuration is its own, as
//! hyper-tls's connections hold none (vendor/hyper-tls/VENDORED.md §2): it is built at the
//! connection's start around the one certified key every connection signs with.

mod conn;
mod io;
mod request;
mod respond;

use std::net::SocketAddr;

use hyper_rt::RtError;
use hyper_rt::sync::Notify;
use hyper_rt::tcp::{ConnectionBudget, TcpListener, TcpStream};
use hyper_tls::crypto::CryptoProvider;
use hyper_tls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use hyper_tls::server::{ClientHello, NoServerSessionStorage, ResolvesServerCert};
use hyper_tls::sign::CertifiedKey;
use hyper_tls::{ServerConfig, version};

use crate::door::Submitter;
use crate::process::AccessKey;

pub use conn::PIECE;

/// The connections a listener serves at once: what stock clients open to an endpoint by default,
/// the AWS CLI's `max_concurrent_requests` and botocore's `max_pool_connections`, ten each (the
/// AWS CLI's S3 configuration reference; botocore's `Config`). A cell's floor serves one such
/// client; a connection past it is answered `503 SlowDown` from [`REFUSING`].
pub const CONNECTIONS: u64 = 10;

/// The connections past [`CONNECTIONS`] the listener answers `503 SlowDown`, `Connection: close`,
/// rather than closes: as many as one more stock client opens at once (research/72). A connection
/// closed with no answer is what the AWS CLI's transfers take worst: a ranged GET whose every
/// attempt met one is dropped, and the copy ends exit 0 with the object short, where a `503` it
/// retries and then fails loudly (research/72, measured 2026-11-02). Past both budgets a
/// connection is closed as it comes, the one refusal left that costs nothing to make.
pub const REFUSING: u64 = CONNECTIONS;

/// The connection tasks a listener runs at once, those it serves and those it answers refused:
/// the node gives each a task, a timer for its wait on its client (`conn`) and a socket.
pub const TASKS: u64 = CONNECTIONS + REFUSING;

/// The connections the kernel queues for the listener before it accepts them: the budget, as no
/// more are served at once.
const BACKLOG: u32 = 10;

/// The application protocol the endpoint answers when a client offers ALPN: HTTP/1.1, the only
/// version Amazon S3's endpoints answer (node.md §4.1).
const ALPN: &[u8] = b"http/1.1";

/// What the listener serves, from the node's stated S3 keys (`crate::process::S3Keys`).
pub struct Settings {
    /// Where it listens.
    pub listen: SocketAddr,
    /// The region its requests are signed for.
    pub region: String,
    /// The DNS names its clients reach it by, lowercase: a request to one names its bucket in its
    /// path, and one to a name under it names its bucket by the host (`mantle_s3::route`).
    pub domains: Vec<String>,
    /// The cell's access keys.
    pub access: Vec<AccessKey>,
    /// The endpoint's certificate and its PKCS#8 key, DER, for TLS; none serves plain HTTP.
    pub tls: Option<(Vec<u8>, Vec<u8>)>,
    /// This node's id, which each response names as its host.
    pub node: u64,
}

/// Why the listener could not start or ended.
#[derive(Debug, thiserror::Error)]
pub enum ListenError {
    #[error("the socket: {0:?}")]
    Socket(RtError),
    #[error("the endpoint's key: {0}")]
    Key(hyper_tls::Error),
}

/// What every connection reads and none changes: the region, the keys, the endpoint's certified
/// key, and the signal a connection's end gives an accept that waits for a descriptor.
pub(crate) struct Endpoint {
    pub(crate) region: String,
    pub(crate) access: Vec<AccessKey>,
    /// How requests name buckets (`mantle_s3::route`): by path at the endpoint's names, its
    /// addresses and `localhost`, as the operator's profile addresses them (research/84), and by
    /// the host under one of its names; any other host names nothing.
    pub(crate) addressing: mantle_s3::route::Endpoint,
    pub(crate) certified: Option<&'static CertifiedKey>,
    /// This node's id, as `x-amz-id-2` names it.
    pub(crate) host: String,
    /// A connection ended: its descriptor is free.
    pub(crate) closed: Notify,
}

impl Endpoint {
    /// The secret of access key `id`, and the account it acts for.
    pub(crate) fn key(&self, id: &str) -> Option<&AccessKey> {
        self.access.iter().find(|k| k.id == id)
    }
}

/// The endpoint's one certificate, whatever the client asks.
#[derive(Debug)]
struct Certified(&'static CertifiedKey);

impl ResolvesServerCert for Certified {
    fn resolve(&self, _: ClientHello<'_>) -> Option<&CertifiedKey> {
        Some(self.0)
    }
}

/// aws-lc's provider, hyper-tls's default: TLS 1.3's suites, and TLS 1.2's ECDHE suites, of which
/// the endpoint's P-256 key takes the ECDHE-ECDSA AES-GCM ones Schannel and Secure Transport offer
/// (research/84).
fn provider() -> &'static CryptoProvider {
    &hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER
}

/// A connection's TLS configuration around `certified`: TLS 1.3 and 1.2, no client certificate,
/// HTTP/1.1 by ALPN. It keeps no sessions and sends no tickets, since a connection's configuration
/// ends with it, and compresses no certificate (RFC 8879): the chain is one leaf, and a
/// compressor's window is megabytes a connection would hold for it.
pub(crate) fn tls_config(
    certified: &'static CertifiedKey,
) -> Result<ServerConfig, hyper_tls::Error> {
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&version::TLS13, &version::TLS12])?
        .with_no_client_auth()
        .with_cert_resolver(Box::new(Certified(certified)));
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.session_storage = Box::new(NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    config.cert_compressors = Vec::new();
    Ok(config)
}

/// The S3 listener, bound.
pub struct Listener {
    socket: TcpListener,
    /// The connections served at once ([`CONNECTIONS`]).
    budget: ConnectionBudget,
    /// The connections past the budget answered `503 SlowDown` at once ([`REFUSING`]).
    refusing: ConnectionBudget,
    endpoint: &'static Endpoint,
    door: Submitter,
}

impl Listener {
    /// Binds `settings.listen` under the connection budget, the endpoint's key loaded once for
    /// every connection's handshake. The endpoint lives as long as the process: every connection
    /// task reads it, so it is made once and never freed.
    pub fn bind(settings: Settings, door: Submitter) -> Result<Self, ListenError> {
        let certified = match settings.tls {
            Some((certificate, key)) => {
                let key = provider()
                    .key_provider
                    .load_private_key(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)))
                    .map_err(ListenError::Key)?;
                let certified = CertifiedKey::new(vec![CertificateDer::from(certificate)], key);
                Some(&*Box::leak(Box::new(certified)))
            }
            None => None,
        };
        let budget = ConnectionBudget::new(CONNECTIONS).map_err(ListenError::Socket)?;
        let refusing = ConnectionBudget::new(REFUSING).map_err(ListenError::Socket)?;
        // The listener takes each connection's slot itself (`start`), so a connection past the
        // budget is answered rather than closed by the accept.
        let socket = TcpListener::bind(settings.listen, BACKLOG).map_err(ListenError::Socket)?;
        let endpoint = Box::leak(Box::new(Endpoint {
            region: settings.region,
            access: settings.access,
            addressing: mantle_s3::route::Endpoint {
                domains: settings.domains,
                cname: false,
            },
            certified,
            host: format!("{:016x}", settings.node),
            closed: Notify::new().map_err(ListenError::Socket)?,
        }));
        Ok(Self {
            socket,
            budget,
            refusing,
            endpoint,
            door,
        })
    }

    /// Where it listens.
    pub fn local_addr(&self) -> Result<SocketAddr, ListenError> {
        self.socket.local_addr().map_err(ListenError::Socket)
    }

    /// Accepts until the socket fails: each connection a task of its own on this shard. A
    /// process out of descriptors or socket buffers takes no connection until one of its own
    /// closes, as hyper-rt's `tcp::serve` waits; with none of its own open, nothing would free
    /// one, and the failure ends the listener.
    pub async fn serve(&self) -> ListenError {
        loop {
            match self.socket.accept().await {
                Ok(stream) => self.start(stream),
                Err(e) if exhausted(&e) && (self.budget.open() > 0 || self.refusing.open() > 0) => {
                    if let Err(e) = self.endpoint.closed.notified().await {
                        return ListenError::Socket(e);
                    }
                }
                Err(e) => return ListenError::Socket(e),
            }
        }
    }

    /// A connection's task, served within the budget or answered `503 SlowDown` past it. One past
    /// both budgets, or that the shard has no room for, is closed as it comes, with its slot.
    fn start(&self, stream: TcpStream) {
        let admitted = match self.budget.try_take() {
            Some(slot) => (slot, conn::Admitted::Served),
            None => match self.refusing.try_take() {
                Some(slot) => (slot, conn::Admitted::Refused),
                None => return,
            },
        };
        let io = match self.endpoint.certified {
            None => io::Io::Plain(stream),
            Some(certified) => match tls_config(certified)
                .map_err(io::IoError::Tls)
                .and_then(|config| io::Tls::new(stream, config))
            {
                Ok(tls) => io::Io::Tls(Box::new(tls)),
                Err(_) => return,
            },
        };
        let _ = hyper_rt::futures::spawn_detached(conn::serve(
            io,
            self.endpoint,
            self.door.clone(),
            admitted,
        ));
    }
}

/// Whether `error` is the process out of descriptors or socket buffers (hyper-rt `tcp::serve`).
fn exhausted(error: &RtError) -> bool {
    let RtError::DriverRefused {
        code: Some(code), ..
    } = error
    else {
        return false;
    };
    #[cfg(unix)]
    {
        use rustix::io::Errno;
        [Errno::MFILE, Errno::NFILE, Errno::NOBUFS, Errno::NOMEM]
            .contains(&Errno::from_raw_os_error(*code))
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Networking::WinSock::{WSAEMFILE, WSAENOBUFS};
        matches!(*code, WSAEMFILE | WSAENOBUFS)
    }
}
