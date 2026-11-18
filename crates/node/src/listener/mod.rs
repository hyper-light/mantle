//! The S3 listener (docs/design/node.md §4.1–§4.5): stock S3 clients' HTTP/1.1, on hyper-rt's TCP,
//! through mantle's own codec (`mantle_http`), each request routed, authenticated and handed to
//! the node's object host in-process ([`crate::door`]), with no loopback QUIC hop.
//!
//! The listener accepts on the node's shard under its connection budget, past which a connection
//! is answered `503 SlowDown` and closed from a reserve of its own (never closed with no answer,
//! which a stock client can take for a part done), and runs each connection as a task of its own,
//! so the runtime wakes the one whose socket or channel moved: on the node's other shard with the
//! fewest connections open, where the node has others, and on its own where it has none or the
//! one chosen refuses it ([`shards`]). A connection whose client says
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
mod shards;

use std::net::SocketAddr;
use std::sync::Mutex;

use hyper_rt::sync::Notify;
use hyper_rt::tcp::{ConnectionBudget, ConnectionSlot, OwnedStream, TcpListener, TcpStream};
use hyper_rt::{RtError, SlotHolder};
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
/// (`tests/listener_memory.rs`). 382,052 bytes at the most, over TLS 1.3 at one connection on
/// Windows x86_64: the 382,028 counted there (Jupiter, 2026-11-16) and the 24 a TLS connection has
/// held since it resumes sessions, its way into the endpoint's store (`sessions::Kept`, boxed in its
/// TLS configuration), an 8-octet reference and a 16-octet `Option<u64>` on every 64-bit target.
/// Counted on macOS arm64 (2026-11-17), a connection over TLS adds 381,988 from one connection to
/// four, the 381,964 macOS arm64 and Windows x86_64 both counted before and those 24, and one over
/// plain HTTP 346,132 as before. One connection over TLS holds 382,036 there: 80 bytes under
/// Windows x86_64 as before, and 64 more once, as std's `Mutex` around the store boxes its pthread
/// mutex at the first lock on macOS, where on Windows and Linux it is an atomic and allocates
/// nothing. Mostly pieces of 64 KiB (`PIECE`): the four of the test's body, and the connection's
/// own buffer, which each read reuses; TLS 1.3 adds its session's buffers. Counted with only what a
/// connection reads ahead taken, the count read however far each connection had read: 300,208 over
/// TLS, and on one CPU 101 KB less at one connection (CI run 43762797164).
pub const CONNECTION_BYTES: u64 = 382_052;

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

/// The most plaintext one TLS record carries: 2^14 octets (RFC 8446 §5.1; RFC 5246 §6.2.1). A
/// connection encrypts a response's bytes a record of it at a time (`io::Tls::new`).
pub(crate) const RECORD_PLAINTEXT: usize = 1 << 14;

/// One TLS record at its largest on the wire: its 5-octet header (RFC 8446 §5.1), and
/// [`RECORD_PLAINTEXT`] with the most its protection adds, 256 octets under TLS 1.3 (RFC 8446
/// §5.2) and 2,048 under TLS 1.2 (RFC 5246 §6.2.3), the larger taken: hyper-tls's `MAX_WIRE_SIZE`
/// (`msgs/message`), which its deframer's buffer is held to.
pub const TLS_RECORD: usize = 5 + RECORD_PLAINTEXT + 2048;

/// The most one request holds above its connection at rest, whose read buffer ([`PIECE`]), head
/// buffer and response head buffers are kept across its requests: one head
/// ([`mantle_http::MAX_HEAD`], S3's 8 KB) for what the door makes of a request's head and answers
/// it with (its route, principal and operation, the metadata a version keeps, its signature's
/// string to sign, the host's reply and the channels between the two; its fields are read where
/// the head holds them, no table of them made, so none of this grows with their count), one body
/// segment ([`PIECE`]), the piece a put's body is copied into, and one TLS record
/// ([`TLS_RECORD`]). A put whose body is longer than one segment holds [`streamed_bytes`]. Pinned
/// by `tests/listener_memory.rs`
/// (`a_request_holds_no_more_than_its_stated_bound_above_its_connections_rest`), a head padded to
/// its 8 KB with the most fields it carries among its cases.
pub const REQUEST_BYTES: usize = mantle_http::MAX_HEAD + PIECE + TLS_RECORD;

/// The most pieces a put's body holds in the host's queue: the cap on the depth the node learns
/// (`door::Depth`). The bytes the node's memory division gives each connection
/// ([`CONNECTION_BYTES`], `derive`'s charge for each of the listener's connections) past what a
/// connection holds of a request beside its queue, its own read buffer ([`PIECE`]), one head
/// ([`mantle_http::MAX_HEAD`]) and one TLS record ([`TLS_RECORD`]), in whole pieces, and at least
/// one, the least that moves a body. Four at today's terms, the four pieces of body the charge was
/// counted holding.
pub fn most_pieces() -> usize {
    let beside = PIECE
        .saturating_add(mantle_http::MAX_HEAD)
        .saturating_add(TLS_RECORD);
    usize::try_from(CONNECTION_BYTES)
        .unwrap_or(usize::MAX)
        .saturating_sub(beside)
        .checked_div(PIECE)
        .unwrap_or(0)
        .max(1)
}

/// The most a put whose body is longer than one segment holds above its connection at rest: one
/// head and one TLS record, as [`REQUEST_BYTES`], and the pieces of its body in the host's queue,
/// at most the queue's depth of them, which the node learns and caps at [`most_pieces`]. The
/// connection waits for the queue's room before it copies a piece
/// (`hyper_rt::sync::Sender::reserve`), so it holds no piece the queue has no room for, and the
/// one it copies is one of the queue's. At a depth of one this is [`REQUEST_BYTES`]. A sum past a
/// `usize` is past any memory, so it saturates there. Pinned at the cap by
/// `tests/listener_memory.rs`
/// (`a_request_holds_no_more_than_its_stated_bound_above_its_connections_rest`).
pub fn streamed_bytes() -> usize {
    most_pieces()
        .saturating_mul(PIECE)
        .saturating_add(mantle_http::MAX_HEAD)
        .saturating_add(TLS_RECORD)
}

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
    #[error("a shard's lane: {0:?}")]
    Lane(RtError),
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
    /// The node's other shards, each connection handed to one ([`Listener::spread`]); none serves
    /// every connection on the listener's own.
    lanes: Vec<shards::Lane<Handed>>,
}

/// A connection handed to another shard: its socket, its place in that shard's lane, and its slot
/// in the listener's budgets as it was admitted.
type Handed = (
    OwnedStream,
    ConnectionSlot,
    (ConnectionSlot, conn::Admitted),
);

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
            lanes: Vec::new(),
        })
    }

    /// Hands every connection from now on to the shards `holders` name, each through a lane that
    /// holds as many as the listener holds at once ([`tasks`]), so none is ever refused for want of
    /// room; a lane that refuses one anyway, its shard gone, hands it back, and it is served on the
    /// listener's own shard ([`shards`]). Refused when a lane cannot be made, its shard gone or its
    /// control queue full.
    pub fn spread(&mut self, holders: &[SlotHolder]) -> Result<(), ListenError> {
        let capacity = tasks(self.budget.max());
        let mut lanes = Vec::new();
        lanes.try_reserve_exact(holders.len()).map_err(|_| {
            ListenError::Lane(RtError::Capacity {
                what: "the listener's lanes",
                bound: holders.len(),
            })
        })?;
        for holder in holders {
            let endpoint = self.endpoint;
            let door = self.door.clone();
            let start = move |(socket, placed, admitted): Handed| {
                if let Ok(stream) = TcpStream::from_parts(socket, Some(placed)) {
                    begin(stream, endpoint, &door, admitted);
                }
            };
            lanes.push(shards::Lane::new(*holder, capacity, start).map_err(ListenError::Lane)?);
        }
        self.lanes = lanes;
        Ok(())
    }

    /// The connections open on each lane's shard, in the lanes' order.
    #[cfg(test)]
    fn lanes_open(&self) -> Vec<u64> {
        self.lanes.iter().map(shards::Lane::open).collect()
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

    /// A connection's task, served within the budget or answered `503 SlowDown` past it: on the
    /// shard whose lane has the fewest connections open ([`shards::least`]), or on this one where
    /// the listener has no lane or the lane refuses it. One past both budgets, or that the shard
    /// has no room for, is closed as it comes, with its slot.
    fn start(&self, stream: TcpStream) {
        let admitted = match self.budget.try_take() {
            Some(slot) => (slot, conn::Admitted::Served),
            None => match self.refusing.try_take() {
                Some(slot) => (slot, conn::Admitted::Refused),
                None => return,
            },
        };
        let Some((lane, placed)) =
            shards::least(&self.lanes).and_then(|lane| Some((lane, lane.place()?)))
        else {
            return begin(stream, self.endpoint, &self.door, admitted);
        };
        // The listener's socket counts no connection of its own (`bind`): the stream carries no
        // slot, and its place in the lane rides in its stead.
        let (socket, _) = stream.into_parts();
        // Refused, it is served here, its place in the lane given back.
        if let Err((socket, _, admitted)) = lane.hand((socket, placed, admitted))
            && let Ok(stream) = TcpStream::from_parts(socket, None)
        {
            begin(stream, self.endpoint, &self.door, admitted);
        }
    }
}

/// Starts a connection's task on this shard, its TLS session made here: a connection whose TLS
/// cannot be set up is closed, with its slot.
fn begin(
    stream: TcpStream,
    endpoint: &'static Endpoint,
    door: &Submitter,
    admitted: (ConnectionSlot, conn::Admitted),
) {
    let io = match endpoint.certified {
        None => io::Io::Plain(stream),
        Some(certified) => match tls_config(certified, &endpoint.sessions)
            .map_err(io::IoError::Tls)
            .and_then(|config| io::Tls::new(stream, config))
        {
            Ok(tls) => io::Io::Tls(Box::new(tls)),
            Err(_) => return,
        },
    };
    let _ = hyper_rt::futures::spawn_detached(conn::serve(io, endpoint, door.clone(), admitted));
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

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::sync::mpsc::sync_channel;
    use std::task::Waker;

    use hyper_rt::combine::race2;

    use super::*;

    /// The most pieces a body's queue holds are what the charge for a connection holds past its
    /// read buffer, a head and a TLS record, at least one; a put at that depth holds no more above
    /// its connection's rest than the charge.
    #[test]
    fn a_bodys_queue_holds_what_the_charge_for_a_connection_holds() {
        let beside = PIECE + mantle_http::MAX_HEAD + TLS_RECORD;
        let charge = usize::try_from(CONNECTION_BYTES).unwrap();
        assert_eq!(most_pieces(), (charge - beside) / PIECE);
        assert_eq!(most_pieces(), 4);
        assert_eq!(
            streamed_bytes(),
            mantle_http::MAX_HEAD + most_pieces() * PIECE + TLS_RECORD
        );
        assert!(streamed_bytes() + PIECE <= charge);
    }

    /// A listener spread over its runtime's second shard hands that shard the connection it
    /// accepts on the first: the connection's request is answered, and while its client holds it
    /// open its stream holds a place in the second shard's lane, which only the second shard's
    /// taker gives a stream it adopts.
    #[test]
    fn a_spread_listener_serves_its_connection_on_the_other_shard() {
        // The launcher, the listener's task and a connection's on each shard.
        let rt = shards::tests::started(2, 3);
        let (first, second) = (rt.shard_ids()[0], rt.shard_ids()[1]);
        let holder = rt.holder_of(second).unwrap();
        let (bound, at) = sync_channel(1);
        let (ask, mut asked) = hyper_rt::sync::channel::<()>(1).unwrap();
        let (told, heard) = sync_channel(1);
        let launch = async move {
            let serving = async move {
                let (_doorway, door) = crate::door::doorway(1, Waker::noop().clone()).unwrap();
                let mut listener = Listener::bind(
                    Settings {
                        listen: SocketAddr::from(([127, 0, 0, 1], 0)),
                        region: "us-east-1".into(),
                        domains: Vec::new(),
                        access: Vec::new(),
                        tls: None,
                        node: 1,
                        connections: 1,
                    },
                    door,
                )
                .unwrap();
                listener.spread(&[holder]).unwrap();
                bound.try_send(listener.local_addr().unwrap()).unwrap();
                let probe = async {
                    while asked.recv().await.is_ok() {
                        told.try_send(listener.lanes_open()).unwrap();
                    }
                };
                let _ = race2(listener.serve(), probe).await;
            };
            hyper_rt::futures::spawn_detached(serving).unwrap();
        };
        rt.spawn_on(first, launch).unwrap();
        let at = at.recv().unwrap();
        let mut client = std::net::TcpStream::connect(at).unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            .unwrap();
        let mut answer = Vec::new();
        let mut read = [0u8; 512];
        while !answer.windows(4).any(|end| end == b"\r\n\r\n") {
            let n = client.read(&mut read).unwrap();
            assert!(n > 0, "closed before its answer: {answer:?}");
            answer.extend_from_slice(&read[..n]);
        }
        assert!(answer.starts_with(b"HTTP/1.1 "), "{answer:?}");
        ask.try_send(()).unwrap();
        assert_eq!(heard.recv().unwrap(), vec![1]);
        drop(client);
        drop(ask);
        rt.shutdown().unwrap();
    }
}
