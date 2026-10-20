//! Every bound of the node's transport, derived from what the node states (docs/design/node.md §2.6,
//! §3.3; CLAUDE.md §2): its peers, its ranges and their settings, the memory its transport may hold.
//! Times the path has not yet measured start from QUIC's own initial estimates (RFC 9002 §6.2.2)
//! and move once it has.

use std::time::Duration;

use hyper_datagram::PlaneLimits;
use hyper_rt_transport::{Io, MAX_BATCH};
use hyper_transport::{AdmissionLimits, AskQueue, Limits, RECEIVE_CHUNK, StreamCredit};

use crate::classes::Bounds;
use crate::frame;

/// RFC 9002 §6.2.2: before a path has a sample, its smoothed round trip is `kInitialRtt`, 333 ms,
/// and its variation half that.
const INITIAL_RTT: Duration = Duration::from_millis(333);
/// RFC 9002 §6.1.2: `kGranularity`, the timer granularity a probe timeout adds at least.
const GRANULARITY: Duration = Duration::from_millis(1);
/// RFC 9000 §18.2: `max_ack_delay`'s default, 25 ms, which a probe timeout adds once a connection
/// is established.
const MAX_ACK_DELAY: Duration = Duration::from_millis(25);
/// RFC 9000 §10.1: an idle timeout of at least three probe timeouts, so a connection whose packets
/// a loss delays is not taken for idle.
const IDLE_PTOS: u32 = 3;
/// The connections one peer may hold at once: its current one and the one that replaces it, for
/// the handshake's round trip in which both carry traffic (hyper-datagram's `epochs_per_peer` of
/// two, for the same overlap).
const CONNECTIONS_PER_PEER: usize = 2;

/// What the node states of itself for its transport's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stated {
    /// The other nodes it keeps connections to.
    pub peers: usize,
    /// The ranges it hosts.
    pub ranges: usize,
    /// The appends a range's leader keeps out to one follower (`Settings::max_inflight_msgs`).
    pub inflight: usize,
    /// The bytes the node's transport may hold at once, its budget's capacity.
    pub transport_bytes: u64,
    /// The largest message of each class (`crate::classes::Bounds`).
    pub messages: Bounds,
}

/// Why the bounds could not be derived: a statement this machine cannot hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the node's stated bounds overflow: {0}")]
pub struct Unfit(&'static str);

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

/// The transport's limits for a node of `stated`.
pub fn limits(stated: &Stated) -> Result<Limits, Unfit> {
    let peers = stated.peers.max(1);
    let connections = peers
        .checked_mul(CONNECTIONS_PER_PEER)
        .ok_or(Unfit("connections"))?;
    // A peer node opens at most a snapshot for each range it leads to this one, Raft holding
    // replication to a member while its snapshot is out (hyper-raft's `report_snapshot`), and a
    // command for each range besides; every range may be led there.
    let streams = stated
        .ranges
        .max(1)
        .checked_mul(2)
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
    let peers_u64 = u64::try_from(peers).map_err(|_| Unfit("peers"))?;
    let window_ceiling = stated
        .transport_bytes
        .checked_div(peers_u64)
        .ok_or(Unfit("window"))?;
    let chunk = u64::try_from(RECEIVE_CHUNK).map_err(|_| Unfit("chunk"))?;
    let receive_chunks = window_ceiling
        .div_ceil(chunk)
        .checked_mul(peers_u64)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or(Unfit("receive chunks"))?;
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
        // A snapshot's exchange carries its frame in the body, a frame's header ahead of it.
        max_head: u32::try_from(frame::HEADER_BYTES).map_err(|_| Unfit("head"))?,
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
    })
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

/// The plane's bounds: a key for each peer, the epochs of its current connection and the one
/// replacing it, and a replay window that widens to what one peer's lane may hold in flight.
pub fn plane(stated: &Stated) -> Result<PlaneLimits, Unfit> {
    let window = stated
        .ranges
        .max(1)
        .checked_mul(stated.inflight.checked_add(1).ok_or(Unfit("inflight"))?)
        .ok_or(Unfit("plane window"))?;
    Ok(PlaneLimits {
        max_peers: stated.peers.max(1),
        epochs_per_peer: CONNECTIONS_PER_PEER,
        window_limit: window.max(hyper_datagram::DEFAULT_WIDTH),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stated() -> Stated {
        Stated {
            peers: 2,
            ranges: 3,
            inflight: 4,
            transport_bytes: 64 << 20,
            messages: Bounds {
                replication: 1 << 16,
                request: 1 << 16,
                bulk: 1 << 24,
            },
        }
    }

    #[test]
    fn the_initial_probe_timeout_is_rfc_9002_s() {
        // 333 ms + max(4 × 166.5 ms, 1 ms) + 25 ms.
        assert_eq!(initial_pto(), Duration::from_micros(1_024_000));
        assert_eq!(idle_timeout(), Duration::from_micros(3_072_000));
    }

    #[test]
    fn the_limits_follow_what_the_node_states() {
        let limits = limits(&stated()).unwrap();
        assert_eq!(limits.admission.connections, 4);
        assert_eq!(limits.streams_per_connection, 6);
        assert_eq!(limits.exchanges, 24);
        assert_eq!(limits.lane_window, 15);
        assert_eq!(limits.window_ceiling, 32 << 20);
        assert_eq!(
            limits.receive_chunks,
            usize::try_from((32u64 << 20).div_ceil(RECEIVE_CHUNK as u64) * 2).unwrap()
        );
        assert!(limits.keep_alive.unwrap() < limits.idle_timeout);
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
