//! Every bound of a transport, a node's or a client's, derived from what its owner states
//! (docs/design/node.md §2.6, §3.3; CLAUDE.md §2): its peers, its ranges and their settings, the
//! memory its transport may hold. Times the path has not yet measured start from QUIC's own initial
//! estimates (RFC 9002 §6.2.2) and move once it has.

use std::time::Duration;

use hyper_rt_transport::{Io, MAX_BATCH};
use hyper_transport::{
    AdmissionLimits, AskQueue, Classes, Limits, MIN_DATAGRAM, RECEIVE_CHUNK, StreamCredit,
    class_reserve, frame_grant, initial_window,
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
/// The connections one peer may hold at once: its current one and the one that replaces it, for
/// the handshake's round trip in which both carry traffic (hyper-datagram's `epochs_per_peer` of
/// two, for the same overlap).
pub const CONNECTIONS_PER_PEER: usize = 2;

/// What a node or a client states of itself for its transport's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stated {
    /// The peers it keeps connections to: a node's other nodes, gateways and clients, a client's
    /// nodes.
    pub peers: usize,
    /// The ranges it hosts.
    pub ranges: usize,
    /// The appends a range's leader keeps out to one follower (`Settings::max_inflight_msgs`).
    pub inflight: usize,
    /// The chunk exchanges the node keeps open at once: those its volumes take (each volume's
    /// writer queue and its reads at the device and waiting) and those it asks of its peers.
    pub transfers: usize,
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

/// What a node of `stated` keeps of the transport's shape: its peers and their connections,
/// the streams each connection takes and the exchanges all of them hold, a lane's window, and the
/// largest head a message carries.
struct Shape {
    peers: usize,
    connections: usize,
    streams: u32,
    exchanges: usize,
    lane_window: usize,
    max_head: u32,
}

fn shape(stated: &Stated) -> Result<Shape, Unfit> {
    let peers = stated.peers.max(1);
    let connections = peers
        .checked_mul(CONNECTIONS_PER_PEER)
        .ok_or(Unfit("connections"))?;
    // A peer node opens at most a snapshot for each range it leads to this one, Raft holding
    // replication to a member while its snapshot is out (hyper-raft's `report_snapshot`), and a
    // command for each range besides; every range may be led there. Its chunk exchanges are at
    // most the node's transfers.
    let streams = stated
        .ranges
        .max(1)
        .checked_mul(2)
        .and_then(|n| n.checked_add(stated.transfers))
        .and_then(|n| u32::try_from(n).ok())
        .ok_or(Unfit("streams"))?;
    let exchanges = usize::try_from(streams)
        .ok()
        .and_then(|s| s.checked_mul(connections))
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
        streams,
        exchanges,
        lane_window,
        max_head,
    })
}

/// The least bytes the node's transport may hold for `stated` (its `transport_bytes` aside):
/// what it holds besides QUIC's windows, and each share of the rest at QUIC's initial window,
/// below which [`limits`] refuses (docs/design/node.md §1.4). Receive chunks past it spare a
/// datagram its copy, which its connection's window already charges (hyper-transport's
/// `receive`), and wait for their knee.
pub fn least_transport(stated: &Stated) -> Result<u64, Unfit> {
    let shape = shape(stated)?;
    let (held, shares) = held(stated, &shape)?;
    shares
        .checked_mul(initial_window(MIN_DATAGRAM))
        .and_then(|windows| windows.checked_add(held))
        .ok_or(Unfit("the transport's least bytes"))
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
        stream_credit: StreamCredit::Fixed,
        ask_queue: AskQueue::Table,
        exchanges,
        // One lane a peer: the node runs its ranges on one shard (node.md §3.2's lane a shard).
        lanes_per_peer: 1,
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
    })
}

/// The bytes the transport and its owner hold of messages at once, at most: every exchange's head
/// both ways (an outgoing message's prefix and head, an incoming head), the one body piece the
/// owner reads at a time (a receive chunk: the node's and the client's `PIECE`), and, for a node
/// that keeps appends out (`Stated::inflight`), each peer lane's frames, its window queued out and
/// one read in.
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
        u64::try_from(stated.peers.max(1))
            .ok()
            .zip(u64::try_from(lane_window.checked_add(1).ok_or_else(unfit)?).ok())
            .and_then(|(peers, window)| peers.checked_mul(window)?.checked_mul(frame))
            .ok_or_else(unfit)?
    };
    let piece = u64::try_from(RECEIVE_CHUNK).map_err(|_| unfit())?;
    heads
        .checked_add(piece)
        .and_then(|b| b.checked_add(frames))
        .ok_or_else(unfit)
}

/// What QUIC buffers for peers, within the transport's bytes less the messages' (node.md §3.3): a
/// connection's receive window, at most the ceiling returned, and the receive chunks datagrams are
/// cut from, at most the count returned. Both are charged to the transport's budget, which the
/// messages share, so they are sized to leave the messages' bytes free: reading what QUIC buffered
/// is what frees it, and a read refused for the bytes the buffering holds never comes (CI run
/// 39694528095: a client's receive chunks held its whole budget, and the piece a 1 MiB reply is
/// read into was refused). Each peer holds at most two connections' windows, each with the class
/// reserve its first window carries, and a full window of its unread data packed into whole
/// chunks; each handshake admission lets wait holds a first window.
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
/// first window with its reserve; and a share each for a peer's two connections' windows and
/// its pool of chunks.
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
    let held = messages(stated, shape.exchanges, shape.max_head, shape.lane_window)?
        .checked_add(fixed)
        .ok_or_else(unfit)?;
    // A share each for a peer's two connections' windows and its pool of chunks.
    let shares = connections.checked_add(peers).ok_or_else(unfit)?;
    Ok((held, shares))
}

/// A transport endpoint as it opens, its TLS configurations with the node's certificate, key and
/// authority included, beside the tables its limits size: measured, 59,325 bytes, and 713 for
/// each peer its directory names, at one, two and four peers (mantle-node's
/// `tests/transport_memory.rs`).
const ENDPOINT_BYTES: u64 = 59_325;
const ENDPOINT_PEER_BYTES: u64 = 713;

/// A slot of an endpoint's table of receive chunks, beside the chunk its budget charges:
/// measured, 72 bytes.
const CHUNK_SLOT_BYTES: u64 = 72;

/// A QUIC connection and its TLS session once established, a side's: measured, 122,012 bytes at
/// the most any window took, and 95 for each stream it may hold.
const CONNECTION_BYTES: u64 = 122_012;
const STREAM_BYTES: u64 = 95;

/// An exchange in use, a side's, beside what its budget charges: its stream's state and the
/// transport's record of it, measured, 2,151 bytes at the most.
const EXCHANGE_BYTES: u64 = 2_151;

/// What a server's handshake takes, once a certificate, to compress the certificate (RFC 8879):
/// hyper-tls's brotli at quality 11 and a 4 MiB window, measured, 34,267,200 bytes, given back as
/// the handshake goes on.
pub const CERTIFICATE_COMPRESSION_BYTES: u64 = 34_267_200;

/// The most bytes a transport of `stated` holds beside its budget (`transport_bytes`): what the
/// budget does not charge.
/// - The endpoint: as it opens, a slot of each exchange in use, a slot of each receive chunk, and,
///   where it `serves` handshakes, the compression of its certificate as a server's first handshake
///   makes it.
/// - Each connection its admission takes: QUIC's and TLS's state with a stream's for each it may
///   hold, and what QUIC keeps to send, at most what its peer's window takes, `window_ceiling`
///   (every node of a cell states the same).
/// - The sockets: the listener's, one for each connection where the platform gives a connection a
///   socket of its own, and a node's plane's, each at what hyper-rt states one holds
///   (`Io::held_bound`).
///
/// QUIC's datagrams are none: no node or client sends them, though hyper-quic's default takes up to
/// 1,250,000 bytes of them a connection. What aws-lc keeps of keys is in its own heap, which a
/// counting allocator does not see.
pub fn beside(stated: &Stated, serves: bool) -> Option<u64> {
    let limits = limits(stated).ok()?;
    let count = |n: usize| u64::try_from(n).ok();
    let peers = count(stated.peers.max(1))?;
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
    let connection = u64::from(limits.streams_per_connection)
        .checked_mul(STREAM_BYTES)?
        .checked_add(CONNECTION_BYTES)?
        .checked_add(limits.window_ceiling)?;
    let sockets = connections
        .checked_add(2)?
        .checked_mul(count(io(stated).held_bound())?)?;
    endpoint
        .checked_add(connections.checked_mul(connection)?)?
        .checked_add(sockets)
}

/// How the node's sockets batch: a system call takes up to one datagram train from each peer, what
/// arrives at once when every peer's driver sent one transmit (hyper-transport makes one
/// connection's batch at a time, its `SEGMENTS_A_TRANSMIT`), within `UIO_MAXIOV` (`MAX_BATCH`). On
/// Linux the receive ring holds that many 64 KiB buffers (hyper-rt `Io`), one elsewhere.
pub fn io(stated: &Stated) -> Io {
    Io {
        batch: stated.peers.clamp(1, MAX_BATCH),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stated() -> Stated {
        Stated {
            peers: 2,
            ranges: 3,
            inflight: 4,
            transfers: 5,
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
            ranges: 1,
            inflight: 0,
            transfers: 1,
            transport_bytes: 3 * 4 * RECEIVE_CHUNK as u64,
            ..stated()
        };
        // A storage node of a cell's process tests: Raft messages of about a kilobyte, a writer
        // queue's transfers.
        let node = Stated {
            peers: 7,
            ranges: 5,
            inflight: 2,
            transfers: 130,
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
            let held = messages + buffered(&stated, &limits);
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
}
