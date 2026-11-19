//! Every bound of a transport, a node's or a client's, derived from what its owner states
//! (docs/design/node.md §2.6, §3.3; CLAUDE.md §2): its peers, its ranges and their settings, the
//! memory its transport may hold. Times the path has not yet measured start from QUIC's own initial
//! estimates (RFC 9002 §6.2.2) and move once it has.

use std::time::Duration;

use hyper_rt_transport::{Io, MAX_BATCH};
use hyper_transport::{
    Acknowledgement, AdmissionLimits, AskQueue, Classes, Limits, MIN_DATAGRAM, RECEIVE_CHUNK,
    StreamCredit, class_reserve, frame_grant, initial_window, state_charge_for,
};

use crate::classes::{Bounds, NodeClasses};

/// RFC 9002 §6.2.2: before a path has a sample, its smoothed round trip is `kInitialRtt`, 333 ms,
/// and its variation half that.
const INITIAL_RTT: Duration = Duration::from_millis(333);
/// RFC 9002 §6.1.2: `kGranularity`, the timer granularity a probe timeout adds at least.
const GRANULARITY: Duration = Duration::from_millis(1);
/// RFC 9000 §18.2: `max_ack_delay`'s default, 25 ms, which a probe timeout adds once a connection
/// is established; every node's endpoint states it.
pub(crate) const MAX_ACK_DELAY: Duration = Duration::from_millis(25);
/// RFC 9000 §10.1: an idle timeout of at least three probe timeouts, so a connection whose packets
/// a loss delays is not taken for idle.
const IDLE_PTOS: u32 = 3;
/// How long a request whose client says nothing of its own patience is waited on without its work
/// moving: the stock S3 clients', 60 s between bytes, botocore's `read_timeout` and the AWS CLI's
/// `--cli-read-timeout` by default (research/77 [9, 10], D2). A node working longer for such a
/// request works for a client that has gone.
pub const STOCK_PATIENCE: Duration = Duration::from_secs(60);
/// The connections one peer may hold at once: its current one and the one that replaces it, for
/// the handshake's round trip in which both carry traffic (hyper-datagram's `epochs_per_peer` of
/// two, for the same overlap).
pub const CONNECTIONS_PER_PEER: usize = 2;

/// The exchanges a pooled client's connection is granted at its handshake and keeps between its
/// requests: one, as mantle-client asks one object exchange at a time ([`crate::object`]'s
/// requests, `mantle_client`'s `stated`), and as hyper-transport's pool keeps one a connection
/// outside its account (Breakwater's demand speculation, its `pool::KEPT`). The rest a client
/// opens at once comes from the pool ([`Stated::pool`]).
pub const CLIENT_EXCHANGES: u32 = 1;

/// What a node or a client states of itself for its transport's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stated {
    /// The peers it keeps connections to that replicate, place chunks or ask ranges: a node's
    /// other nodes and gateways, a client's nodes.
    pub peers: usize,
    /// The peers its ranges' messages go to over its lanes: a node's other nodes, the cell's,
    /// which its ranges' members are on (mantle-node's `Node::send` sends a range's message to a
    /// node of the cell alone); none for a client, which nothing replicates to. A gateway among
    /// [`Self::peers`] is sent no frame of a lane.
    pub replicas: usize,
    /// The clients of the native protocol it admits at once (a node's `gateway.clients`), each a
    /// pooled role: its connection holds [`CLIENT_EXCHANGES`] of its own and draws the rest from
    /// the pool (hyper-transport's `StreamCredit::Pooled`, Breakwater's credit). None for a client.
    pub clients: usize,
    /// The exchanges the clients may hold at once past those they keep, all together: the
    /// operations the node runs at most (`gateway.objects`). The node sizes the pool to what its
    /// admission learns within it (`Endpoint::set_stream_pool`).
    pub pool: usize,
    /// The ranges it hosts.
    pub ranges: usize,
    /// The appends a range's leader keeps out to one follower (`Settings::max_inflight_msgs`).
    pub inflight: usize,
    /// The Raft lanes each peer may open to it, at least one (node.md §3.2): a node's shards that
    /// run its ranges, at most one a range (§2.9); a client's one.
    pub lanes: usize,
    /// The chunk exchanges its volumes take at once, which its peers may open to it: each volume's
    /// writer queue and its reads at the device and waiting.
    pub transfers: usize,
    /// The chunk exchanges it asks of its peers at once (a node's `chunk_asks`): opened on its
    /// peers' credit, each a place in its own table of exchanges, none a stream it grants.
    pub asked: usize,
    /// The bytes the node's transport may hold at once, its budget's capacity.
    pub transport_bytes: u64,
    /// The largest message of each class (`crate::classes::Bounds`).
    pub messages: Bounds,
    /// The longest head of a message it sends or receives: a node's, of every kind it serves; a
    /// client's, an object's request or reply ([`crate::object::MAX_HEAD`]).
    pub max_head: usize,
}

/// Why the bounds could not be derived: a statement this machine cannot hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the stated bounds overflow: {0}")]
pub struct Unfit(pub &'static str);

/// A path's measured probe timeout, the current retransmission timeout a backoff doubles from
/// (RFC 6298 §5.5): hyper-quic's, with the peer's whole `max_ack_delay` (RFC 9002 §6.2.1), since
/// hyper-quic adds the delay its ACK frequency asks for, which may be shorter.
pub fn measured_pto(path: &hyper_transport::PathFacts) -> Duration {
    path.probe_timeout.saturating_add(MAX_ACK_DELAY)
}

/// A backoff's first delay from a measured probe timeout `pto` (node.md §5, retries and
/// reconnects): `pto`, at most the idle timeout, as every delay [`backed_off`] gives after it is.
/// A round trip sampled across a hold of its endpoint (its shard held in one poll, its process
/// stopped or swapped out) measures the hold, and a path's first sample makes its probe timeout
/// three times it (RFC 9002 §5.3: `smoothed_rtt` the sample, `rttvar` half of it; §6.2.1). Taken
/// whole, a redial's pause drawn from it outlasted the reconnect its node stated, the idle timeout
/// and the backoff the doubling had capped: on CI run 44369369345 (linux-aarch64-1cpu) a node
/// restarted into a shard held 218 s stated a reconnect of 6.144 s, and its links stayed unkeyed
/// after one dial each to the end of a 600 s wait, its loop turning all the while.
pub fn backoff_from(pto: Duration) -> Duration {
    pto.min(idle_timeout())
}

/// RFC 9002 §6.2.1's probe timeout before any sample: `smoothed_rtt + max(4·rttvar, kGranularity)
/// + max_ack_delay`, with `rttvar` half the initial round trip (§6.2.2).
pub fn initial_pto() -> Duration {
    let variation = INITIAL_RTT.checked_div(2).unwrap_or(INITIAL_RTT);
    INITIAL_RTT
        .saturating_add(variation.saturating_mul(4).max(GRANULARITY))
        .saturating_add(MAX_ACK_DELAY)
}

/// The idle timeout: three probe timeouts at the initial estimates (RFC 9000 §10.1).
pub fn idle_timeout() -> Duration {
    initial_pto().saturating_mul(IDLE_PTOS)
}

/// A backoff's next delay (node.md §5, retries and reconnects): doubled (RFC 6298 §5.5), at most
/// the idle timeout.
pub fn backed_off(delay: Duration) -> Duration {
    delay.saturating_mul(2).min(idle_timeout())
}

/// One pause of a backoff of `delay` (node.md §5): focal's equal jitter, drawn uniformly between
/// half of `delay` and the whole of it, by `local`'s `attempt`-th draw (hyper-timing's, the one
/// a node's campaign delay takes: independent across nodes and attempts, deterministic for a
/// simulation's seed), so the peers that lost one node do not ask it again in step.
pub fn pause(delay: Duration, local: u64, attempt: u32) -> Duration {
    let half = delay.checked_div(2).unwrap_or(delay);
    half.saturating_add(hyper_timing::election_delay(
        delay.saturating_sub(half),
        local,
        attempt,
    ))
}

/// One pause of a full-jitter backoff of `cap`: drawn uniformly between none and the whole of
/// `cap`, by `local`'s `attempt`-th draw as [`pause`] draws, the backoff botocore's standard retry
/// mode waits before asking a throttled request again (`ExponentialBackoff.delay_amount`:
/// "t_i = rand(0, 1) * min(2 ** attempt, MAX_BACKOFF)"; research/77 [5]).
pub fn full_jitter(cap: Duration, local: u64, attempt: u32) -> Duration {
    hyper_timing::election_delay(cap, local, attempt)
}

/// What a node of `stated` keeps of the transport's shape: its peers and their connections,
/// the streams each connection takes and the exchanges all of them hold, a lane's window, and the
/// largest head a message carries.
struct Shape {
    /// Every identity: the peers and the clients.
    peers: usize,
    connections: usize,
    /// Of those, the peers' connections, each of which may hold `streams`.
    peer_connections: usize,
    streams: u32,
    exchanges: usize,
    lane_window: usize,
    max_head: u32,
}

fn shape(stated: &Stated) -> Result<Shape, Unfit> {
    let peers = stated
        .peers
        .max(1)
        .checked_add(stated.clients)
        .ok_or(Unfit("identities"))?;
    let connections = peers
        .checked_mul(CONNECTIONS_PER_PEER)
        .ok_or(Unfit("connections"))?;
    let peer_connections = stated
        .peers
        .max(1)
        .checked_mul(CONNECTIONS_PER_PEER)
        .ok_or(Unfit("connections"))?;
    let client_connections = connections
        .checked_sub(peer_connections)
        .ok_or(Unfit("connections"))?;
    // A peer node opens at most a snapshot for each range it leads to this one, Raft holding
    // replication to a member while its snapshot is out (hyper-raft's `report_snapshot`), and a
    // command for each range besides; every range may be led there. Its chunk exchanges are at
    // most what the node's volumes take: what this node asks of it opens on its own credit.
    let streams = stated
        .ranges
        .max(1)
        .checked_mul(2)
        .and_then(|n| n.checked_add(stated.transfers))
        .and_then(|n| u32::try_from(n).ok())
        .ok_or(Unfit("streams"))?;
    // The peers' connections each their streams; a client's each the exchange it keeps, and the
    // pool the clients draw the rest from.
    let exchanges = usize::try_from(streams)
        .ok()
        .and_then(|s| s.checked_mul(peer_connections))
        .zip(usize::try_from(CLIENT_EXCHANGES).ok())
        .and_then(|(peers, kept)| {
            peers
                .checked_add(client_connections.checked_mul(kept)?)?
                .checked_add(stated.pool)?
                .checked_add(stated.asked)
        })
        .ok_or(Unfit("exchanges"))?;
    // A lane carries every range of a shard to one peer: each range's appends in flight and one
    // control message too large for a datagram.
    let lane_window = stated
        .ranges
        .max(1)
        .checked_mul(stated.inflight.checked_add(1).ok_or(Unfit("inflight"))?)
        .ok_or(Unfit("lane window"))?;
    let max_head = u32::try_from(stated.max_head).map_err(|_| Unfit("head"))?;
    Ok(Shape {
        peers,
        connections,
        peer_connections,
        streams,
        exchanges,
        lane_window,
        max_head,
    })
}

/// The least bytes the node's transport may hold for `stated` (its `transport_bytes` aside):
/// what it holds besides QUIC's windows, and each share of the rest at QUIC's initial window,
/// below which [`limits`] refuses (docs/design/node.md §1.4). Its windows' ceiling is below a
/// receive chunk, so it holds none, and each datagram it receives is copied into a buffer of its
/// own (hyper-transport's `receive`); [`chunked_transport`] is what spares them.
pub fn least_transport(stated: &Stated) -> Result<u64, Unfit> {
    transport_at(stated, initial_window(MIN_DATAGRAM))
}

/// The bytes the node's transport holds for `stated` (its `transport_bytes` aside) with each
/// share at a receive chunk, [`RECEIVE_CHUNK`]: its want past its floor (docs/design/node.md
/// §1.4, §3.3). Each connection's window may grow to a chunk, which hyper-transport's auto-tuning
/// takes as a whole window is read within two round trips (Chromium QUIC's
/// `QuicFlowController::MaybeIncreaseMaxWindowSize`; RFC 9000 §4.2 lets a receiver raise its
/// limits as data is consumed), and each peer's pool holds a chunk for the window's unread data
/// ([`buffering`]), so a datagram it receives is cut from a chunk, not copied into a buffer of its
/// own (hyper-transport's `receive`). Past [`least_transport`] by a chunk less QUIC's initial
/// window for each share.
pub fn chunked_transport(stated: &Stated) -> Result<u64, Unfit> {
    let chunk = u64::try_from(RECEIVE_CHUNK).map_err(|_| Unfit("a receive chunk"))?;
    transport_at(stated, chunk)
}

/// The bytes the node's transport holds for `stated` (its `transport_bytes` aside) with each
/// share at `window`: what it holds besides QUIC's windows ([`held`]) and a share each for a
/// peer's two connections' windows and its pool of chunks ([`buffering`]), so its windows'
/// ceiling is `window`. Below [`least_transport`]'s window, QUIC's initial one, [`limits`]
/// refuses it; at [`chunked_transport`]'s, a receive chunk, each pool holds a chunk; between, a
/// window grows past its first and its datagrams are copied.
pub fn transport_at(stated: &Stated, window: u64) -> Result<u64, Unfit> {
    let shape = shape(stated)?;
    let (held, shares) = held(stated, &shape)?;
    shares
        .checked_mul(window)
        .and_then(|windows| windows.checked_add(held))
        .ok_or(Unfit("the transport's bytes"))
}

/// The transport's limits for a node of `stated`.
pub fn limits(stated: &Stated) -> Result<Limits, Unfit> {
    let shape = shape(stated)?;
    let (window_ceiling, receive_chunks) = buffering(stated, &shape)?;
    let Shape {
        peers,
        connections,
        streams,
        exchanges,
        lane_window,
        max_head,
        ..
    } = shape;
    let idle = idle_timeout();
    Ok(Limits {
        admission: AdmissionLimits {
            pending: peers,
            identities: peers,
            connections,
            per_identity: CONNECTIONS_PER_PEER,
        },
        streams_per_connection: streams,
        // A node's clients draw their exchanges from a pool its admission sizes; a peer's
        // connection is raised to the streams per connection once its role is known.
        stream_credit: if stated.clients == 0 {
            StreamCredit::Fixed
        } else {
            StreamCredit::Pooled {
                initial: CLIENT_EXCHANGES,
            }
        },
        ask_queue: AskQueue::Table,
        exchanges,
        // A lane a peer for each shard that runs ranges (node.md §3.2's lane a shard).
        lanes_per_peer: lanes(stated)?,
        lane_window,
        // A command's head names its range; a reply's, its status and the leader; a chunk's, its
        // volume, its key and the CRC-32C or the range asked; an object's, the operation and its
        // names' lengths, which ride in its body. A snapshot's exchange carries its frame in the
        // body and no head.
        max_head,
        window_ceiling,
        // An exchange that makes no progress for as long as a connection may stay silent is
        // judged stalled.
        serve_period: idle,
        idle_timeout: idle,
        // Below the idle timeout, so a node's connection to a peer, and the plane's keys from it
        // (node.md §3.4), outlive a quiet spell.
        keep_alive: Some(idle.checked_div(2).unwrap_or(idle)),
        // One response a peer at a time.
        max_responses: peers,
        receive_chunks,
        // A handshake from an address not yet proven is answered with a Retry once half the
        // pending places are taken (RFC 9000 §8.1.2), as before hyper-transport could count
        // the handshakes' bytes instead.
        retry_bytes: None,
        // As before hyper-raft b5edd11 and c0d6ca8: acknowledgements decimated, no erasure code.
        acknowledgement: Acknowledgement::Decimated,
        fec_window: None,
    })
}

/// The lanes each peer may open ([`Stated::lanes`]), at least one.
fn lanes(stated: &Stated) -> Result<u32, Unfit> {
    u32::try_from(stated.lanes.max(1)).map_err(|_| Unfit("lanes"))
}

/// The bytes the transport and its owner hold of messages at once, at most: every exchange's head
/// both ways (an outgoing message's prefix and head, an incoming head), the one body piece the
/// owner reads at a time (a receive chunk: the node's and the client's `PIECE`), and, for a node
/// that keeps appends out (`Stated::inflight`), for each lane it may open, its window queued out
/// to each node its ranges replicate to (`Stated::replicas`) and one frame read in from each peer.
fn messages(
    stated: &Stated,
    exchanges: usize,
    max_head: u32,
    lane_window: usize,
) -> Result<u64, Unfit> {
    let unfit = || Unfit("the messages' bytes");
    // An outgoing message's reservation, its prefix and head (`frame_grant`), and an incoming head.
    let head = frame_grant(u64::from(max_head))
        .checked_add(u64::from(max_head))
        .ok_or_else(unfit)?;
    let heads = u64::try_from(exchanges)
        .ok()
        .and_then(|e| e.checked_mul(head))
        .ok_or_else(unfit)?;
    let frames = if stated.inflight == 0 {
        0
    } else {
        let frame = frame_grant(stated.messages.replication);
        let lanes = u64::from(lanes(stated)?);
        // Queued out, a lane's window to each node the ranges replicate to; read in, a frame a
        // lane from any peer, which may open its lanes whatever its role.
        u64::try_from(stated.replicas)
            .ok()
            .zip(u64::try_from(lane_window).ok())
            .and_then(|(replicas, window)| replicas.checked_mul(window))
            .zip(u64::try_from(stated.peers.max(1)).ok())
            .and_then(|(out, peers)| {
                out.checked_add(peers)?
                    .checked_mul(lanes)?
                    .checked_mul(frame)
            })
            .ok_or_else(unfit)?
    };
    let piece = u64::try_from(RECEIVE_CHUNK).map_err(|_| unfit())?;
    heads
        .checked_add(piece)
        .and_then(|b| b.checked_add(frames))
        .ok_or_else(unfit)
}

/// What QUIC buffers for peers, within the transport's bytes less the messages' and the receive
/// ring's growth (node.md §3.3): a connection's receive window, at most the ceiling returned, and
/// the receive chunks datagrams are cut from, at most the count returned. Both are charged to the
/// transport's budget, which the messages and the ring share, so they are sized to leave those
/// bytes free: reading what QUIC buffered is what frees it, and a read refused for the bytes the
/// buffering holds never comes (CI run 39694528095: a client's receive chunks held its whole
/// budget, and the piece a 1 MiB reply is read into was refused). Each peer holds at most two
/// connections' windows, each with the class reserve its first window carries, and a full window
/// of its unread data packed into whole chunks; each handshake admission lets wait holds a first
/// window.
fn buffering(stated: &Stated, shape: &Shape) -> Result<(u64, usize), Unfit> {
    let unfit = || Unfit("the transport's bytes, less the messages', hold no window");
    let (held, shares) = held(stated, shape)?;
    let free = stated.transport_bytes.checked_sub(held).ok_or_else(unfit)?;
    let window_ceiling = free.checked_div(shares).ok_or_else(unfit)?;
    if window_ceiling < initial_window(MIN_DATAGRAM) {
        return Err(unfit());
    }
    let peers = u64::try_from(shape.peers).map_err(|_| unfit())?;
    let chunk = u64::try_from(RECEIVE_CHUNK).map_err(|_| unfit())?;
    let receive_chunks = window_ceiling
        .checked_div(chunk)
        .and_then(|c| c.checked_mul(peers))
        .and_then(|c| usize::try_from(c).ok())
        .ok_or_else(unfit)?;
    Ok((window_ceiling, receive_chunks))
}

/// What the transport's bytes hold besides QUIC's windows, and the shares the rest is cut into
/// ([`buffering`]): the messages' bytes, each connection's reserve and each waiting handshake's
/// first window with its reserve, the connections' state ([`connections_state`]), and the
/// driver's receive ring at its most past the buffer it is made with ([`ring_growth`]); and a
/// share each for a peer's two connections' windows and its pool of chunks.
fn held(stated: &Stated, shape: &Shape) -> Result<(u64, u64), Unfit> {
    let unfit = || Unfit("the transport's bytes, less the messages', hold no window");
    let peers = u64::try_from(shape.peers).map_err(|_| unfit())?;
    let connections = u64::try_from(shape.connections).map_err(|_| unfit())?;
    let reserve = class_reserve(<NodeClasses as Classes>::RANKS.saturating_sub(1));
    // Each connection's reserve, and each waiting handshake's first window with its reserve.
    let fixed = connections
        .checked_mul(reserve)
        .and_then(|r| {
            let first = initial_window(MIN_DATAGRAM).checked_add(reserve)?;
            r.checked_add(peers.checked_mul(first)?)
        })
        .ok_or_else(unfit)?;
    // The ring's growth, which its driver charges to the budget as the ring grows, held apart at
    // its most: grown, it takes no room a window, a head or a piece was sized for. Uncounted, on
    // Linux it took the heads' room, the likely cause of finished jobs' reply heads refused there.
    // So is the connections' state, which the endpoint charges as each connection is made.
    let held = messages(stated, shape.exchanges, shape.max_head, shape.lane_window)?
        .checked_add(fixed)
        .and_then(|held| held.checked_add(connections_state(stated).ok()?))
        .and_then(|held| held.checked_add(ring_growth(stated).ok()?))
        .ok_or_else(unfit)?;
    // A share each for a peer's two connections' windows and its pool of chunks.
    let shares = connections.checked_add(peers).ok_or_else(unfit)?;
    Ok((held, shares))
}

/// A transport endpoint as it opens, its TLS configurations with the node's certificate, key and
/// authority included, beside the tables its limits size: measured, 59,325 bytes under the
/// transport tests' 326-byte certificates, and 713 for each peer its directory names, at one, two
/// and four peers (mantle-node's `tests/transport_memory.rs`); and 550 more at the longest
/// certificate a node carries ([`CERTIFICATE_COMPRESSION_BYTES`]): two endpoints opened 534 more
/// under the longest issued now, 403 bytes (macOS arm64), and from 2050 its two octets more take
/// 16 at the most an octet of certificate took, 8, from 346 bytes to 403.
const ENDPOINT_BYTES: u64 = 59_875;
const ENDPOINT_PEER_BYTES: u64 = 713;

/// A slot of an endpoint's table of receive chunks, beside the chunk its budget charges:
/// measured, 72 bytes.
const CHUNK_SLOT_BYTES: u64 = 72;

/// A QUIC connection and its TLS session once established, a side's: measured, 122,012 bytes at
/// the most any window took under the transport tests' 326-byte certificates, and 95 for each
/// stream it may hold; and 1,322 more at the longest certificate a node carries
/// ([`CERTIFICATE_COMPRESSION_BYTES`]): a connection's two sides together took 1,270 more under
/// the longest issued now, 403 bytes (mantle-node's `tests/transport_memory.rs`, macOS arm64), and
/// from 2050 its two octets more take 52 at the most an octet of certificate took, 26, from 346
/// bytes to 403.
const CONNECTION_BYTES: u64 = 123_334;
const STREAM_BYTES: u64 = 95;

/// An exchange in use, a side's, beside what its budget charges: its stream's state and the
/// transport's record of it, measured, 2,151 bytes at the most.
const EXCHANGE_BYTES: u64 = 2_151;

/// What a server's handshake takes, once a certificate, to compress the certificate (RFC 8879):
/// hyper-tls's brotli at quality 11 with a window the length of the certificate's Certificate
/// message (1 KiB, WBITS 10, for every message up to 1,008 bytes), given back as the handshake goes
/// on. Within the window the state grows with the message and not with what it says, so it is
/// measured at the longest certificate a node carries: each field at its longest as `mantle init`
/// issues it, the cell's authority's 28-character name, a name's id and a grant at twenty digits,
/// Ed25519's 32-byte key and 64-byte signature and a 16-octet serial, valid from a GeneralizedTime,
/// as every time from 2050 is (RFC 5280 §4.1.2.5), to `NO_EXPIRY`: 405 bytes, a 414-byte message.
/// 722,238 bytes, the same for each of 64 drawn anew (mantle-node's `tests/transport_memory.rs`,
/// macOS arm64); 722,215 at the longest issued before 2050, 403 bytes, and 721,001 at 326.
pub const CERTIFICATE_COMPRESSION_BYTES: u64 = 722_238;

/// The most bytes a transport of `stated` holds beside its budget (`transport_bytes`): what the
/// budget does not charge.
/// - The endpoint: as it opens, a slot of each exchange in use, a slot of each receive chunk, and,
///   where it `serves` handshakes, the compression of its certificate as a server's first handshake
///   makes it.
/// - Each connection its admission takes: QUIC's and TLS's state with a stream's for each it may
///   hold, and what QUIC keeps to send, at most what its peer's window takes, `window_ceiling`
///   (every node of a cell states the same).
/// - The sockets: the driver's ring of one buffer and its listener's socket, lent
///   (hyper-rt-transport's `least_bytes`), a lent socket for each connection where the platform
///   gives a connection one of its own, receiving into that ring (`Batched::lent_bytes`), and a
///   node's plane's at what hyper-rt states one holds (`Io::held_bound`). The ring grows past one
///   buffer only within the transport's budget, which charges it and is sized to hold it at its
///   most ([`ring_growth`]).
///
/// QUIC's datagrams are none: no node or client sends them, and no endpoint offers to take them
/// (hyper-transport's `Limits::transport`; hyper-quic's default took up to 1,250,000 bytes of them a
/// connection). What aws-lc keeps of keys is in its own heap, which a counting allocator does not
/// see.
pub fn beside(stated: &Stated, serves: bool) -> Option<u64> {
    let limits = limits(stated).ok()?;
    let count = |n: usize| u64::try_from(n).ok();
    let peers = count(stated.peers.max(1).checked_add(stated.clients)?)?;
    let connections = count(limits.admission.connections)?;
    let compression = if serves {
        CERTIFICATE_COMPRESSION_BYTES
    } else {
        0
    };
    let endpoint = peers
        .checked_mul(ENDPOINT_PEER_BYTES)?
        .checked_add(ENDPOINT_BYTES)?
        .checked_add(compression)?
        .checked_add(count(limits.exchanges)?.checked_mul(EXCHANGE_BYTES)?)?
        .checked_add(count(limits.receive_chunks)?.checked_mul(CHUNK_SLOT_BYTES)?)?;
    let shape = shape(stated).ok()?;
    let peer_connections = count(shape.peer_connections)?;
    let client_connections = connections.checked_sub(peer_connections)?;
    // Each connection's QUIC and TLS state and what QUIC keeps to send; a peer's its streams, a
    // client's the one it keeps, and the pool's streams wherever the clients hold them.
    let connection = CONNECTION_BYTES.checked_add(limits.window_ceiling)?;
    let streams = u64::from(limits.streams_per_connection)
        .checked_mul(peer_connections)?
        .checked_add(u64::from(CLIENT_EXCHANGES).checked_mul(client_connections)?)?
        .checked_add(count(stated.pool)?)?
        .checked_mul(STREAM_BYTES)?;
    let io = io(stated);
    let sockets = count(hyper_rt_transport::least_bytes(io))?
        .checked_add(count(io.held_bound())?)?
        .checked_add(connections.checked_mul(count(hyper_rt::udp::Batched::lent_bytes())?)?)?;
    endpoint
        .checked_add(connections.checked_mul(connection)?)?
        .checked_add(streams)?
        .checked_add(sockets)
}

/// How the node's sockets batch: a system call takes up to one datagram train from each peer, what
/// arrives at once when every peer's driver sent one transmit (hyper-transport makes one
/// connection's batch at a time, its `SEGMENTS_A_TRANSMIT`), within `UIO_MAXIOV` (`MAX_BATCH`). On
/// Linux the receive ring holds that many 64 KiB buffers (hyper-rt `Io`), one elsewhere.
pub fn io(stated: &Stated) -> Io {
    Io {
        batch: stated
            .peers
            .saturating_add(stated.clients)
            .clamp(1, MAX_BATCH),
    }
}

/// The most the driver's receive ring grows past the buffer it is made with, bytes, as the
/// platform's driver grows it, which it charges to the transport's budget (hyper-rt-transport's
/// `Driver::size_ring`, through `Endpoint::reserve_own`) and the budget's sizing holds apart
/// ([`held`]). On Linux a receive call takes up to a batch of messages (`recvmmsg(2)`), each a
/// datagram train the kernel coalesced into one buffer of up to 64 KiB (`UDP_GRO`, udp(7);
/// `RECEIVE_BYTES`), and the ring holds a buffer for each message its drains took, up to the
/// batch ([`io`]): a train from each peer and client, within `UIO_MAXIOV`. Elsewhere a call takes
/// one datagram, and the ring holds one buffer and never grows (hyper-rt's `Ring::bytes_for`).
pub fn ring_growth(stated: &Stated) -> Result<u64, Unfit> {
    let io = io(stated);
    let most = hyper_rt::udp::Ring::bytes_for(io, io.batch);
    let first = hyper_rt::udp::Ring::bytes_for(io, 1);
    most.checked_sub(first)
        .and_then(|grown| u64::try_from(grown).ok())
        .ok_or(Unfit("the receive ring"))
}

/// The most the endpoint charges its connections' state, bytes, which it reserves from the
/// transport's budget as each connection is made, a dial or an accepted handshake, and the
/// budget's sizing holds apart ([`held`]): each connection at hyper-transport's `state_charge`,
/// its own structures and its exchange tables at the streams a connection takes, the least the
/// endpoint charges one and all it charges before its owner observes what its connections hold
/// (`Endpoint::observe`, which no node or client calls); at the most connections the endpoint
/// holds at once, those its admission holds and the handshakes it lets wait beside them
/// (`AdmissionLimits`'s `connections` and `pending`, the count hyper-transport's
/// `Endpoint::receive_demand` takes). Uncounted, a node's 64 clients took about 1.3 MB of the room
/// its heads and frames were sized for.
pub fn connections_state(stated: &Stated) -> Result<u64, Unfit> {
    let shape = shape(stated)?;
    let each = state_charge_for::<NodeClasses>(shape.streams);
    shape
        .connections
        .checked_add(shape.peers)
        .and_then(|held| u64::try_from(held).ok())
        .and_then(|held| held.checked_mul(each))
        .ok_or(Unfit("the connections' state"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stated() -> Stated {
        Stated {
            peers: 2,
            replicas: 2,
            clients: 0,
            pool: 0,
            ranges: 3,
            inflight: 4,
            lanes: 1,
            transfers: 5,
            asked: 0,
            transport_bytes: 64 << 20,
            messages: Bounds {
                replication: 1 << 16,
                request: 1 << 16,
                transfer: 1 << 20,
                bulk: 1 << 24,
            },
            max_head: crate::object::MAX_HEAD,
        }
    }

    /// A peer may open a lane for each shard that runs the node's ranges, at least one, and the
    /// messages' bytes count each lane's frames; the exchanges, which a lane to a shard running
    /// ranges holds (mantle-node's `Node::spread_to`), are at least four a range, so never fewer
    /// than the ranges on one shard.
    #[test]
    fn a_peer_opens_a_lane_a_shard_and_the_exchanges_hold_every_range() {
        assert_eq!(limits(&stated()).unwrap().lanes_per_peer, 1);
        let none = Stated {
            lanes: 0,
            ..stated()
        };
        assert_eq!(limits(&none).unwrap().lanes_per_peer, 1);
        let three = Stated {
            lanes: 3,
            ..stated()
        };
        assert_eq!(limits(&three).unwrap().lanes_per_peer, 3);
        assert!(least_transport(&three).unwrap() > least_transport(&stated()).unwrap());
        for ranges in [1, 5, 64] {
            for peers in [1, 2, 9] {
                let s = Stated {
                    ranges,
                    peers,
                    ..stated()
                };
                let s = Stated {
                    transport_bytes: least_transport(&s).unwrap(),
                    ..s
                };
                assert!(limits(&s).unwrap().exchanges >= 4 * ranges);
            }
        }
    }

    /// A node queues lane frames out to the nodes its ranges replicate to alone, a range's
    /// messages going to a node of the cell alone (mantle-node's `Node::send`): a gateway among
    /// its peers is charged its connections and a frame a lane read in, none queued out.
    #[test]
    fn a_gateway_among_the_peers_is_held_no_lane_frames() {
        let frames = |s: &Stated| {
            let at = |s: &Stated| {
                let shape = shape(s).unwrap();
                messages(s, shape.exchanges, shape.max_head, shape.lane_window).unwrap()
            };
            at(s) - at(&Stated { inflight: 0, ..*s })
        };
        let nodes = stated();
        let read = frame_grant(nodes.messages.replication) * u64::from(lanes(&nodes).unwrap());
        let gateways = Stated {
            peers: nodes.peers + 2,
            ..nodes
        };
        assert!(frames(&nodes) > 2 * read);
        assert_eq!(frames(&gateways), frames(&nodes) + 2 * read);
        let replicas = Stated {
            replicas: gateways.peers,
            ..gateways
        };
        assert_eq!(frames(&replicas), 2 * frames(&nodes));
        assert_eq!(
            frames(&Stated {
                replicas: 0,
                ..nodes
            }),
            2 * read
        );
    }

    #[test]
    fn the_initial_probe_timeout_is_rfc_9002_s() {
        // 333 ms + max(4 × 166.5 ms, 1 ms) + 25 ms.
        assert_eq!(initial_pto(), Duration::from_micros(1_024_000));
        assert_eq!(idle_timeout(), Duration::from_micros(3_072_000));
    }

    #[test]
    fn the_least_transport_gives_windows_and_a_byte_less_gives_none() {
        for peers in [1, 2, 9] {
            let least = least_transport(&Stated { peers, ..stated() }).unwrap();
            let at = |transport_bytes| {
                limits(&Stated {
                    peers,
                    transport_bytes,
                    ..stated()
                })
            };
            let limits = at(least).unwrap();
            assert_eq!(limits.window_ceiling, initial_window(MIN_DATAGRAM));
            assert!(at(least - 1).is_err());
        }
    }

    /// The transport's want past its floor gives each share a receive chunk: each connection's
    /// window may grow to a chunk and each peer's pool holds one, a chunk for each full window,
    /// where at the floor's windows, QUIC's initial window, the pool holds none and each datagram
    /// is copied into a buffer of its own. It is the floor and a chunk less the initial window for
    /// each share, every class held beside the windows as at the floor; a byte less holds no chunk.
    #[test]
    fn the_transports_want_gives_each_window_and_pool_a_receive_chunk() {
        let chunk = RECEIVE_CHUNK as u64;
        let initial = initial_window(MIN_DATAGRAM);
        for (peers, clients) in [(1, 0), (2, 0), (9, 0), (2, 64), (2, 1_000)] {
            let at = |transport_bytes| Stated {
                peers,
                clients,
                pool: clients,
                transport_bytes,
                ..stated()
            };
            let least = least_transport(&at(0)).unwrap();
            let chunked = chunked_transport(&at(0)).unwrap();
            let identities = (peers.max(1) + clients) as u64;
            let shares = identities * (CONNECTIONS_PER_PEER as u64 + 1);
            assert_eq!(chunked - least, shares * (chunk - initial));
            let floor = limits(&at(least)).unwrap();
            assert_eq!((floor.window_ceiling, floor.receive_chunks), (initial, 0));
            let want = limits(&at(chunked)).unwrap();
            assert_eq!(
                want.window_ceiling, chunk,
                "{peers} peers, {clients} clients"
            );
            assert_eq!(want.receive_chunks as u64, identities);
            let (held, _) = held(&at(chunked), &shape(&at(chunked)).unwrap()).unwrap();
            let connections = identities * CONNECTIONS_PER_PEER as u64;
            assert_eq!(
                held + connections * want.window_ceiling + want.receive_chunks as u64 * chunk,
                chunked
            );
            assert_eq!(limits(&at(chunked - 1)).unwrap().receive_chunks, 0);
        }
    }

    #[test]
    fn a_backoff_doubles_to_the_idle_timeout_and_each_pause_is_within_its_upper_half() {
        let mut delay = Duration::from_millis(3);
        let mut delays = Vec::new();
        for _ in 0..16 {
            delays.push(delay);
            delay = backed_off(delay);
        }
        assert_eq!(delays[1], Duration::from_millis(6));
        assert_eq!(*delays.last().unwrap(), idle_timeout());
        for (attempt, delay) in delays.iter().enumerate() {
            for local in [1, 2, u64::MAX] {
                let pause = pause(*delay, local, u32::try_from(attempt).unwrap());
                assert!(
                    pause >= *delay / 2 && pause <= *delay,
                    "{pause:?} for {delay:?}"
                );
            }
        }
        // Two peers' pauses of one delay differ: they do not ask again in step.
        assert_ne!(pause(delays[4], 1, 0), pause(delays[4], 2, 0));
    }

    /// A backoff begun from a measured probe timeout is that timeout, at most the idle timeout:
    /// one measured across a hold of minutes gives pauses no longer than the idle timeout, where
    /// pauses drawn from it whole ran for minutes.
    #[test]
    fn a_backoff_from_a_measured_probe_timeout_is_at_most_the_idle_timeout() {
        let measured = Duration::from_millis(40);
        assert_eq!(backoff_from(measured), measured);
        // A first round trip sampled across a 218 s hold: its probe timeout three times it.
        let held = Duration::from_secs(218).saturating_mul(3);
        assert_eq!(backoff_from(held), idle_timeout());
        for attempt in 0..16 {
            assert!(pause(backoff_from(held), 7, attempt) <= idle_timeout());
        }
    }

    #[test]
    fn the_limits_follow_what_the_node_states() {
        let limits = limits(&stated()).unwrap();
        assert_eq!(limits.admission.connections, 4);
        // Six for the ranges' snapshots and commands, five for chunks.
        assert_eq!(limits.streams_per_connection, 11);
        assert_eq!(limits.exchanges, 44);
        assert_eq!(
            limits.max_head,
            u32::try_from(crate::object::MAX_HEAD).unwrap()
        );
        assert_eq!(limits.lane_window, 15);
        assert!(limits.keep_alive.unwrap() < limits.idle_timeout);
    }

    /// The most QUIC's buffering holds for `stated` under `limits`: each connection's window with
    /// its reserve, each waiting handshake's first window, and the receive chunks.
    fn buffered(stated: &Stated, limits: &Limits) -> u64 {
        let peers = stated.peers.max(1) as u64;
        let connections = peers * CONNECTIONS_PER_PEER as u64;
        let reserve = class_reserve(<NodeClasses as Classes>::RANKS - 1);
        connections * (limits.window_ceiling + reserve)
            + peers * (initial_window(MIN_DATAGRAM) + reserve)
            + limits.receive_chunks as u64 * RECEIVE_CHUNK as u64
    }

    #[test]
    fn quics_buffering_leaves_the_messages_their_bytes() {
        let client = Stated {
            peers: 3,
            replicas: 0,
            ranges: 1,
            inflight: 0,
            lanes: 1,
            transfers: 1,
            asked: 0,
            transport_bytes: 3 * 4 * RECEIVE_CHUNK as u64,
            ..stated()
        };
        // A storage node of a cell's process tests: Raft messages of about a kilobyte, a writer
        // queue's transfers.
        let node = Stated {
            peers: 7,
            replicas: 7,
            clients: 0,
            pool: 0,
            ranges: 5,
            inflight: 2,
            lanes: 1,
            transfers: 130,
            asked: 0,
            transport_bytes: 7 * 4 * RECEIVE_CHUNK as u64,
            messages: Bounds {
                replication: 1_200,
                ..stated().messages
            },
            max_head: stated().max_head,
        };
        for stated in [stated(), client, node] {
            let limits = limits(&stated).unwrap();
            let messages = messages(
                &stated,
                limits.exchanges,
                limits.max_head,
                limits.lane_window,
            )
            .unwrap();
            // The connections' state and the receive ring at its most are held apart beside them.
            let held = messages
                + buffered(&stated, &limits)
                + connections_state(&stated).unwrap()
                + ring_growth(&stated).unwrap();
            assert!(held <= stated.transport_bytes, "{stated:?}: {held}");
            // Nothing is left over but what whole windows and whole chunks cannot use.
            let peers = stated.peers.max(1) as u64;
            let shares = peers * (CONNECTIONS_PER_PEER as u64 + 1);
            let pool_short = limits.window_ceiling % RECEIVE_CHUNK as u64 * peers;
            assert!(
                stated.transport_bytes - held < shares + pool_short,
                "{stated:?}"
            );
            assert_eq!(
                limits.receive_chunks as u64,
                limits.window_ceiling / RECEIVE_CHUNK as u64 * peers
            );
        }
        // A budget the messages leave no window of is refused, not shared out.
        let starved = Stated {
            transport_bytes: 2 * RECEIVE_CHUNK as u64,
            ..stated()
        };
        assert!(limits(&starved).is_err());
    }

    /// What a node asks of its peers takes places in its own table of exchanges, never streams it
    /// grants: a peer may open on its connection only what the node's volumes and ranges take.
    #[test]
    fn what_a_node_asks_grants_its_peers_nothing() {
        let quiet = limits(&stated()).unwrap();
        let asking = limits(&Stated {
            asked: 1_000,
            ..stated()
        })
        .unwrap();
        assert_eq!(asking.streams_per_connection, quiet.streams_per_connection);
        assert_eq!(asking.exchanges, quiet.exchanges + 1_000);
    }

    /// A node's clients are pooled: each connection of a client's holds the exchange it keeps,
    /// the pool the rest, so a client adds what its connections and that exchange hold, not a
    /// peer's streams; a peer's connections keep theirs. The bytes they cost grow by the same each
    /// client.
    #[test]
    fn a_nodes_clients_are_pooled_and_each_costs_its_own_connections() {
        let with = |clients: usize, pool: usize| Stated {
            clients,
            pool,
            ..stated()
        };
        let none = limits(&with(0, 0)).unwrap();
        assert_eq!(none.stream_credit, StreamCredit::Fixed);
        let some = limits(&with(10, 7)).unwrap();
        assert_eq!(
            some.stream_credit,
            StreamCredit::Pooled {
                initial: CLIENT_EXCHANGES
            }
        );
        assert_eq!(some.streams_per_connection, none.streams_per_connection);
        assert_eq!(some.admission.identities, 12);
        assert_eq!(some.admission.connections, 24);
        // The peers' 4 connections at 11 streams, the clients' 20 at the one they keep, and the
        // pool's 7.
        assert_eq!(some.exchanges, 44 + 20 + 7);
        assert_eq!(some.lane_window, none.lane_window);
        let cost = |clients| {
            let s = with(clients, clients);
            let least = least_transport(&s).unwrap();
            least
                + beside(
                    &Stated {
                        transport_bytes: least,
                        ..s
                    },
                    true,
                )
                .unwrap()
        };
        let each = cost(2) - cost(1);
        assert_eq!(cost(3) - cost(2), each);
        assert_eq!(cost(101) - cost(1), 100 * each);
        // A peer costs its connections' streams and lanes besides: a client never more.
        let peer = |peers| {
            let s = Stated {
                peers,
                ..with(0, 0)
            };
            let least = least_transport(&s).unwrap();
            least
                + beside(
                    &Stated {
                        transport_bytes: least,
                        ..s
                    },
                    true,
                )
                .unwrap()
        };
        assert!(each < peer(3) - peer(2), "{each}");
    }

    #[test]
    fn a_call_batches_a_train_from_each_peer_within_the_kernels_bound() {
        assert_eq!(io(&stated()).batch, 2);
        let alone = Stated {
            peers: 0,
            ..stated()
        };
        assert_eq!(io(&alone).batch, 1);
        let crowd = Stated {
            peers: MAX_BATCH + 1,
            ..stated()
        };
        assert_eq!(io(&crowd).batch, MAX_BATCH);
    }

    /// The driver's receive ring at its most, as the platform's driver grows it (on Linux a
    /// 64 KiB buffer for each message of a batch, a train from each peer and client within
    /// `UIO_MAXIOV`; one buffer elsewhere), is held apart from every class's room: the messages,
    /// the reserves, the windows at their ceiling and the receive chunks leave it whole, at the
    /// floor and past it, and the floor counts it. Uncounted, on Linux a grown ring took the room
    /// a finished job's reply head was sized for, and its client gave the upload up `Stalled`.
    #[test]
    fn the_budget_holds_the_receive_ring_at_its_most_beside_every_class() {
        let reserve = class_reserve(<NodeClasses as Classes>::RANKS - 1);
        for (peers, clients) in [(1, 0), (2, 0), (9, 0), (2, 64), (2, 1_000), (2, 5_000)] {
            let at = |transport_bytes| Stated {
                peers,
                clients,
                pool: clients,
                transport_bytes,
                ..stated()
            };
            let batch = io(&at(0)).batch;
            let buffers = if cfg!(target_os = "linux") { batch } else { 1 };
            let ring = ring_growth(&at(0)).unwrap();
            assert_eq!(
                ring,
                (buffers as u64 - 1) * hyper_rt_transport::RECEIVE_BYTES as u64,
                "{peers} peers, {clients} clients"
            );
            let least = least_transport(&at(0)).unwrap();
            for transport_bytes in [least, 8 * least] {
                let stated = at(transport_bytes);
                let shape = shape(&stated).unwrap();
                let limits = limits(&stated).unwrap();
                let messages =
                    messages(&stated, shape.exchanges, shape.max_head, shape.lane_window).unwrap();
                let (connections, identities) = (shape.connections as u64, shape.peers as u64);
                let fixed =
                    connections * reserve + identities * (initial_window(MIN_DATAGRAM) + reserve);
                let state = connections_state(&stated).unwrap();
                let (held, shares) = held(&stated, &shape).unwrap();
                assert_eq!(
                    held,
                    messages + fixed + state + ring,
                    "the ring is held apart"
                );
                // Every class at its most: each connection's window at the ceiling, each peer's
                // receive chunks, the messages, the reserves and the ring.
                let windows = connections * limits.window_ceiling
                    + limits.receive_chunks as u64 * RECEIVE_CHUNK as u64;
                assert!(
                    held + windows <= transport_bytes,
                    "{peers} peers, {clients} clients: {} past {transport_bytes}",
                    held + windows
                );
                if transport_bytes == least {
                    assert_eq!(least, held + shares * initial_window(MIN_DATAGRAM));
                }
            }
        }
    }

    /// The connections' state, which the endpoint charges as each connection is made, a dial or
    /// an accepted handshake (hyper-transport's `state_charge`, all it charges one before an
    /// observation, which no node or client makes), is held apart from every class's room at the
    /// most connections the endpoint holds at once: those its admission holds and the handshakes
    /// it lets wait beside them. The messages, the reserves, the windows at their ceiling, the
    /// receive chunks and the ring leave it whole, at the floor and past it; the floor counts it,
    /// and a budget that leaves it less is refused. Uncounted, a node of 64 clients gave its
    /// connections about 1.3 MB of the room its heads and frames were sized for.
    #[test]
    fn the_budget_holds_the_connections_state_at_its_most_beside_every_class() {
        let reserve = class_reserve(<NodeClasses as Classes>::RANKS - 1);
        for (peers, clients) in [(1, 0), (2, 0), (9, 0), (2, 64), (2, 1_000), (2, 5_000)] {
            let at = |transport_bytes| Stated {
                peers,
                clients,
                pool: clients,
                transport_bytes,
                ..stated()
            };
            let least = least_transport(&at(0)).unwrap();
            let state = connections_state(&at(0)).unwrap();
            // Each connection admission holds and each handshake it lets wait, at what the
            // endpoint charges a connection's state under these limits.
            let floor = limits(&at(least)).unwrap();
            let most = (floor.admission.connections + floor.admission.pending) as u64;
            let identities = (peers.max(1) + clients) as u64;
            assert_eq!(most, identities * (CONNECTIONS_PER_PEER as u64 + 1));
            let each = hyper_transport::state_charge::<NodeClasses>(&floor);
            assert_eq!(
                hyper_transport::connection_charge::<NodeClasses>(&floor),
                initial_window(MIN_DATAGRAM) + reserve + each,
                "the state is what a connection is charged past its first window"
            );
            assert_eq!(state, most * each, "{peers} peers, {clients} clients");
            for transport_bytes in [least, 8 * least] {
                let stated = at(transport_bytes);
                let shape = shape(&stated).unwrap();
                let limits = limits(&stated).unwrap();
                let messages =
                    messages(&stated, shape.exchanges, shape.max_head, shape.lane_window).unwrap();
                let (connections, identities) = (shape.connections as u64, shape.peers as u64);
                let fixed =
                    connections * reserve + identities * (initial_window(MIN_DATAGRAM) + reserve);
                let ring = ring_growth(&stated).unwrap();
                let (held, shares) = held(&stated, &shape).unwrap();
                assert_eq!(
                    held,
                    messages + fixed + state + ring,
                    "the connections' state is held apart"
                );
                // Every class at its most beside it: each connection's window at the ceiling,
                // each peer's receive chunks, the messages, the reserves, the ring and the state.
                let windows = connections * limits.window_ceiling
                    + limits.receive_chunks as u64 * RECEIVE_CHUNK as u64;
                assert!(
                    held + windows <= transport_bytes,
                    "{peers} peers, {clients} clients: {} past {transport_bytes}",
                    held + windows
                );
                if transport_bytes == least {
                    assert_eq!(least, held + shares * initial_window(MIN_DATAGRAM));
                }
            }
            assert!(
                limits(&at(least - state)).is_err(),
                "{peers} peers, {clients} clients: a budget the state short is refused"
            );
        }
    }
}
