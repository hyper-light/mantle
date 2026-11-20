//! Why this node answered its puts `SlowDown` (node.md §4.5), each cause counted since the node
//! opened. A client's `503` names its path alone, `Unplaced` or `Patience` (`door::Slowed`); these
//! counts name which cause of each path answered it, so a failed run's node says what refused its
//! puts where its client saw only the status (CI gates run 44667404838's three `Unplaced` PUTs,
//! each judged by code path for want of them).

use std::sync::atomic::{AtomicU64, Ordering};

use hyper_transport::Refusal;
use mantle_meta::record::VolumeStanding;
use mantle_meta::root::Fact;

use super::ChunkOutcome;
use crate::chunk;

/// What answered a put `SlowDown`: the block placement left short, the refusal of its last chunk
/// with no volume left, or its client's patience passed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// Placement offered fewer volumes than the block's chunks, before any went, some active
    /// volume's node held away by the root range (repair.md §2.1): the active volumes, away ones
    /// among them, still too few.
    ShortAway,
    /// As `ShortAway`, no active volume's node held away, and some volume leaving or gone.
    ShortInactive,
    /// As `ShortAway`, every volume active and its node seen: the cell narrower than its layout.
    ShortNarrow,
    /// The block's last chunk went to a volume the table no longer held.
    Untabled,
    /// The block's last chunk refused by its volume, by the volume's status (node.md §5.2).
    Volume(chunk::Status),
    /// The block's last chunk refused by the transport toward its node for a cause of the peer's:
    /// not the node named, the exchange unknown, the bytes damaged on the way, QUIC refused, or
    /// unknown.
    Peer(Refusal),
    /// The block's last chunk lost with its connection, closed under it or none up, asked again
    /// of the same volume a beat later and lost or refused again: by the transport's last word.
    Lost(Refusal),
    /// The put held past its client's patience while a chunk of it waited for room.
    PatienceRoom,
    /// The put held past its client's patience otherwise: an ask of its own out.
    PatienceHeld,
}

/// The volume statuses that refuse a chunk for a cause of the volume, sending it on
/// (`host::taken`), and `Full`, a last refusal that fails a block `Unplaced` where another of its
/// chunks was refused otherwise: each counted under its name.
const VOLUME: [(chunk::Status, &str); 13] = [
    (chunk::Status::Full, "full"),
    (chunk::Status::Fenced, "fenced"),
    (chunk::Status::Closed, "closing"),
    (chunk::Status::NoVolume, "no_volume"),
    (chunk::Status::Checksum, "checksum"),
    (chunk::Status::TooLarge, "too_large"),
    (chunk::Status::NotFound, "not_found"),
    (chunk::Status::Range, "range"),
    (chunk::Status::Corrupt, "corrupt"),
    (chunk::Status::Exists, "exists"),
    (chunk::Status::Malformed, "malformed"),
    (chunk::Status::Failed, "failed"),
    (chunk::Status::Read, "read"),
];

/// The transport's refusals of a peer's that send a chunk on (`host::taken`).
const PEER: [(Refusal, &str); 5] = [
    (Refusal::Identity, "identity"),
    (Refusal::UnknownExchange, "unknown_exchange"),
    (Refusal::Corrupt, "damaged"),
    (Refusal::Quic, "quic"),
    (Refusal::Unknown, "unknown"),
];

/// The transport's refusals that lose a chunk with its connection (`host::Taken::Lost`).
const LOST: [(Refusal, &str); 2] = [
    (Refusal::Closed, "closed"),
    (Refusal::NotConnected, "not_connected"),
];

/// The causes counted on their own, before the volume's, the peer's and the lost ones.
const OWN: [(Cause, &str); 6] = [
    (Cause::ShortAway, "unplaced_short_away"),
    (Cause::ShortInactive, "unplaced_short_inactive"),
    (Cause::ShortNarrow, "unplaced_short_narrow"),
    (Cause::Untabled, "unplaced_untabled"),
    (Cause::PatienceRoom, "patience_room"),
    (Cause::PatienceHeld, "patience_held"),
];

/// One count for each cause: its own, then each volume status, each peer refusal and each lost
/// one, and one for a refusal none of those names, which the host's classes never send here.
pub const CAUSES: usize = OWN.len() + VOLUME.len() + PEER.len() + LOST.len() + 1;

impl Cause {
    /// The cause of a block's failing `Unplaced` at `outcome`, its last chunk's: none where the
    /// outcome never fails one so (stored, no room now, or unsendable as it was sent).
    pub fn of(outcome: &ChunkOutcome) -> Option<Self> {
        match outcome {
            // Stored, a get's bytes, or a get's peer suspected: never a put's last refusal.
            ChunkOutcome::Stored | ChunkOutcome::Read(_) | ChunkOutcome::Suspected => None,
            ChunkOutcome::Refused(status) => Some(Self::Volume(*status)),
            ChunkOutcome::Lost(refusal @ (Refusal::Closed | Refusal::NotConnected)) => {
                Some(Self::Lost(*refusal))
            }
            ChunkOutcome::Lost(refusal) => Some(Self::Peer(*refusal)),
        }
    }

    /// The cause where placement offered fewer volumes than a block's chunks, by the presence and
    /// standing of the table's volumes (`facts`) as placement read them.
    pub fn short(facts: &[Fact]) -> Self {
        let active = |f: &&Fact| f.standing == VolumeStanding::Active;
        if facts.iter().filter(active).any(|f| !f.presence.present) {
            Self::ShortAway
        } else if facts.iter().any(|f| f.standing != VolumeStanding::Active) {
            Self::ShortInactive
        } else {
            Self::ShortNarrow
        }
    }

    /// Its count's place among the counts ([`CAUSES`]).
    pub(crate) fn slot(self) -> usize {
        let own = OWN.len();
        let volume = own.saturating_add(VOLUME.len());
        let peer = volume.saturating_add(PEER.len());
        let lost = peer.saturating_add(LOST.len());
        let found = match self {
            Self::Volume(status) => VOLUME
                .iter()
                .position(|(s, _)| *s == status)
                .map(|at| own.saturating_add(at)),
            Self::Peer(refusal) => PEER
                .iter()
                .position(|(r, _)| *r == refusal)
                .map(|at| volume.saturating_add(at)),
            Self::Lost(refusal) => LOST
                .iter()
                .position(|(r, _)| *r == refusal)
                .map(|at| peer.saturating_add(at)),
            cause => OWN.iter().position(|(c, _)| *c == cause),
        };
        found.unwrap_or(lost)
    }
}

/// Each count's name in a node's `counts` answer, in [`Refusals::counts`]' order.
pub fn names() -> impl Iterator<Item = (&'static str, &'static str)> {
    OWN.iter()
        .map(|(_, name)| ("", *name))
        .chain(VOLUME.iter().map(|(_, name)| ("unplaced_volume_", *name)))
        .chain(PEER.iter().map(|(_, name)| ("unplaced_peer_", *name)))
        .chain(LOST.iter().map(|(_, name)| ("unplaced_lost_", *name)))
        .chain(std::iter::once(("unplaced_", "other")))
}

/// The puts this node answered `SlowDown`, a count for each [`Cause`], since it opened: one fixed
/// array, each count an atomic add on a refusal's path alone, shared by every host of the node as
/// its gateway's settings are (one `Arc` made with the node).
#[derive(Debug)]
pub struct Refusals {
    counts: [AtomicU64; CAUSES],
}

impl Default for Refusals {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Refusals {
    /// A put answered `SlowDown` for `cause`. Saturating: a count past `u64` says as much.
    pub fn count(&self, cause: Cause) {
        if let Some(count) = self.counts.get(cause.slot()) {
            let _ = count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            });
        }
    }

    /// Each cause's count, in [`names`]' order.
    pub fn counts(&self) -> [u64; CAUSES] {
        std::array::from_fn(|at| {
            self.counts
                .get(at)
                .map_or(0, |count| count.load(Ordering::Relaxed))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every cause has a count of its own, and each count a name.
    #[test]
    fn each_cause_has_a_count_of_its_own_and_a_name() {
        let mut causes = OWN.map(|(cause, _)| cause).to_vec();
        causes.extend(VOLUME.map(|(status, _)| Cause::Volume(status)));
        causes.extend(PEER.map(|(refusal, _)| Cause::Peer(refusal)));
        causes.extend(LOST.map(|(refusal, _)| Cause::Lost(refusal)));
        let refusals = Refusals::default();
        for (at, cause) in causes.iter().enumerate() {
            assert_eq!(cause.slot(), at, "{cause:?}");
            refusals.count(*cause);
        }
        let counts = refusals.counts();
        assert_eq!(counts[..causes.len()], vec![1; causes.len()][..]);
        assert_eq!(counts[CAUSES - 1], 0);
        assert_eq!(names().count(), CAUSES);
        // A refusal the host never sends here is counted, not lost.
        refusals.count(Cause::Peer(Refusal::Budget));
        assert_eq!(refusals.counts()[CAUSES - 1], 1);
    }
}
