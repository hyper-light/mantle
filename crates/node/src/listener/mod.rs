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
//! connection's start around the one certified key every connection signs with and the endpoint's
//! one store of sessions ([`sessions`]), which a client resumes from without early data
//! (research/94).

mod conn;
mod io;
mod request;
mod respond;
mod sessions;

use std::net::SocketAddr;
use std::sync::Mutex;

use hyper_rt::RtError;
use hyper_rt::sync::Notify;
use hyper_rt::tcp::{ConnectionBudget, TcpListener, TcpStream};
use hyper_tls::crypto::CryptoProvider;
use hyper_tls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use hyper_tls::server::{ClientHello, ResolvesServerCert};
use hyper_tls::sign::CertifiedKey;
use hyper_tls::{ServerConfig, version};

use crate::door::Submitter;
use crate::process::AccessKey;

pub use conn::PIECE;
use sessions::Sessions;

/// The connections one stock client opens to an endpoint at once by default, the AWS CLI's
/// `max_concurrent_requests` and botocore's `max_pool_connections`, ten each (the AWS CLI's S3
/// configuration reference; botocore's `Config`).
pub(crate) const STOCK_CLIENT: u64 = 10;

/// The part one stock client cuts an upload into, and the size past which it cuts one: the AWS
/// CLI's default `multipart_chunksize` and `multipart_threshold`, `8MB` each, a megabyte 1024²
/// octets to it (the AWS CLI's S3 configuration reference; research/73 [10]), as boto3's
/// transfers default. A stock client's connections each carry one at once.
pub(crate) const STOCK_PART: u64 = 8 << 20;

/// What one connection holds with a request in flight, the host holding the pieces of its body:
/// measured by a counting allocator on the listener's thread with the whole body taken
/// (`tests/listener_memory.rs`), 382,028 bytes at the most, over TLS 1.3 at one connection on
/// Windows x86_64 (Jupiter, 2026-11-16); a connection over TLS adds 381,964 from one connection to
/// four and one over plain HTTP 346,132, the same on macOS arm64 and Windows x86_64 at every count,
/// whose single-connection counts hold 80 bytes more there. Mostly pieces of 64 KiB (`PIECE`): the
/// four of the test's body, and the connection's own buffer, which each read reuses; TLS 1.3 adds
/// its session's buffers. Counted with only what a connection reads ahead taken, the count read
/// however far each connection had read: 300,208 over TLS, and on one CPU 101 KB less at one
/// connection (CI run 43762797164).
pub const CONNECTION_BYTES: u64 = 382_028;

/// What the listener holds for each connection it serves beside the connection's own bytes, all
/// reserved when it binds: the slots of the sessions one handshake leaves its client
/// (`sessions::TICKETS`, a TLS 1.3 ticket each or a TLS 1.2 session), 384 octets each, 768, and
/// the connection's share of the store's index, hashbrown's table for twice the slots' ids. Its
/// buckets are the power of two at or past 8/7 of the ids it is made for, so fewer than 2 × 8/7 of
/// them, 64/7 a connection, each its 40-octet id and slot and one control octet: under 375
/// octets a connection, the table's own group of control octets included at every count. 1,143
/// in all, counted by the allocator as the store is made for every count of connections to 64 and
/// either side of each doubling of its table to 2^17 buckets
/// (`sessions::tests::the_store_is_reserved_whole_within_its_stated_bytes_and_never_grows`).
pub const SESSION_BYTES: u64 = 1_143;

/// The connections past those it serves ([`Settings::connections`]) the listener answers `503
/// SlowDown`, `Connection: close`, rather than closes: as many as one more stock client opens at
/// once (research/72). A connection closed with no answer is what the AWS CLI's transfers take
/// worst: a ranged GET whose every attempt met one is dropped, and the copy ends exit 0 with the
/// object short, where a `503` it retries and then fails loudly (research/72, measured
/// 2026-11-02). Past both budgets a connection is closed as it comes, the one refusal left that
/// costs nothing to make.
pub const REFUSING: u64 = STOCK_CLIENT;

/// The connection tasks a listener serving `connections` runs at once, those it serves and those
/// it answers refused: the node gives each a task, a timer for its wait on its client (`conn`) and
/// a socket.
pub fn tasks(connections: u64) -> u64 {
    connections.saturating_add(REFUSING)
}

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
    /// The connections it serves at once: as many as the node runs operations at most
    /// (`gateway.objects`), each carrying one request at a time, so one past them would only wait
    /// at the door; each charged what it holds ([`CONNECTION_BYTES`]) in what an operation costs
    /// the node (`derive::operations`). The kernel queues as many before the listener accepts
    /// them, as no more are served at once.
    pub connections: u64,
}

/// Why the listener could not start or ended.
#[derive(Debug, thiserror::Error)]
pub enum ListenError {
    #[error("the socket: {0:?}")]
    Socket(RtError),
    #[error("the endpoint's key: {0}")]
    Key(hyper_tls::Error),
    #[error("the sessions of {0} connections, past what this machine addresses")]
    Sessions(u64),
}

/// What every connection reads: the region, the keys, the endpoint's certified key, the sessions
/// its clients resume, and the signal a connection's end gives an accept that waits for a
/// descriptor. Only the sessions change, each within one call of a handshake.
pub(crate) struct Endpoint {
    pub(crate) region: String,
    pub(crate) access: Vec<AccessKey>,
    /// How requests name buckets (`mantle_s3::route`): by path at the endpoint's names, its
    /// addresses and `localhost`, as the operator's profile addresses them (research/84), and by
    /// the host under one of its names; any other host names nothing.
    pub(crate) addressing: mantle_s3::route::Endpoint,
    pub(crate) certified: Option<&'static CertifiedKey>,
    /// The TLS sessions every connection's handshake keeps and resumes, [`sessions::TICKETS`] for
    /// each connection served; none over plain HTTP.
    sessions: Mutex<Sessions>,
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

    /// The cell's account: the one its keys act for, which its principals belong to
    /// (docs/design/principals.md). `mantle init` gives a cell one account and every node its key.
    pub(crate) fn account(&self) -> Option<&str> {
        self.access.first().map(|k| k.account.as_str())
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
/// HTTP/1.1 by ALPN, and no compressed certificate (RFC 8879): the chain is one leaf, and a
/// compressor's window is megabytes a connection would hold for it.
///
/// Its sessions are the endpoint's (`sessions`): a TLS 1.3 handshake gives [`sessions::TICKETS`]
/// tickets, each the key of a session kept there, and a TLS 1.2 handshake a session ID. hyper-tls's
/// ticketer stays its default, which makes no ticket: a TLS 1.3 ticket is then a key into the store
/// (`server/tls13.rs`, `emit_ticket`), and a TLS 1.2 client is offered no RFC 5077 ticket. No early
/// data: a client "MUST NOT send unsafe methods (or methods whose safety is not known) in early
/// data" (RFC 8470 §4), and RFC 9001 calls disabling 0-RTT "the most effective defense against
/// replay attack" (research/25 §5); a ticket then says the client may send none (RFC 8446 §4.6.1),
/// and early data a client sends anyway is skipped unread (§4.2.10).
pub(crate) fn tls_config(
    certified: &'static CertifiedKey,
    sessions: &'static Mutex<Sessions>,
) -> Result<ServerConfig, hyper_tls::Error> {
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&version::TLS13, &version::TLS12])?
        .with_no_client_auth()
        .with_cert_resolver(Box::new(Certified(certified)));
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.session_storage = Box::new(sessions::Kept::new(sessions));
    config.send_tls13_tickets = sessions::TICKETS;
    config.max_early_data_size = 0;
    config.cert_compressors = Vec::new();
    Ok(config)
}

/// The S3 listener, bound.
pub struct Listener {
    socket: TcpListener,
    /// The connections served at once ([`Settings::connections`]).
    budget: ConnectionBudget,
    /// The connections past the budget answered `503 SlowDown` at once ([`REFUSING`]).
    refusing: ConnectionBudget,
    endpoint: &'static Endpoint,
    door: Submitter,
}

impl Listener {
    /// Binds `settings.listen` under the connection budget, the endpoint's key loaded once for
    /// every connection's handshake and the slots of its sessions reserved, `sessions::TICKETS`
    /// for each connection it serves ([`SESSION_BYTES`]). The endpoint lives as long as the
    /// process: every connection task reads it, so it is made once and never freed.
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
        let slots = match certified {
            Some(_) => usize::try_from(settings.connections)
                .ok()
                .and_then(|n| n.checked_mul(sessions::TICKETS))
                .ok_or(ListenError::Sessions(settings.connections))?,
            None => 0,
        };
        let budget = ConnectionBudget::new(settings.connections).map_err(ListenError::Socket)?;
        let refusing = ConnectionBudget::new(REFUSING).map_err(ListenError::Socket)?;
        // The listener takes each connection's slot itself (`start`), so a connection past the
        // budget is answered rather than closed by the accept.
        let backlog = u32::try_from(settings.connections).unwrap_or(u32::MAX);
        let socket = TcpListener::bind(settings.listen, backlog).map_err(ListenError::Socket)?;
        let endpoint = Box::leak(Box::new(Endpoint {
            region: settings.region,
            access: settings.access,
            addressing: mantle_s3::route::Endpoint {
                domains: settings.domains,
                cname: false,
            },
            certified,
            sessions: Mutex::new(Sessions::new(slots)),
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
            Some(certified) => match tls_config(certified, &self.endpoint.sessions)
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
