//! What a replica fails with.

use hyper_log::LogError;
use mantle_meta::engine::EngineError;
use mantle_meta::error::MetaError;
use mantle_meta::record::RecordError;

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    /// The core refused the call or dropped the message, changing nothing.
    #[error("raft: {0}")]
    Refused(focal_raft::Error),
    /// The replica's state no longer adds up, or a committed entry breaks the state machine:
    /// it stops and recovers from its durable state or its peers.
    #[error("the replica stopped: {0}")]
    Stopped(String),
    /// The member's log failed to make a write durable, a failed write or flush that fenced
    /// it, or refused one for a reason no retry clears: what the failed write carried may be
    /// on the device or not, so the member takes no call that could acknowledge anything
    /// again. Every call answers this until the node reopens the log, from what its device
    /// kept, and opens the member afresh; the group goes on without it meanwhile
    /// (docs/design/replica.md §3, audit S04).
    #[error("the replica is fenced: {0}")]
    Fenced(String),
    /// The group's records on this member's log are damaged (docs/design/raft-log.md §6),
    /// or its log lost entries its engine had applied: the member does not open under its
    /// identity, which may have voted in terms the log no longer shows. It is quarantined,
    /// and rebuilt from its peers under a new identity ([`crate::Replica::rebuild`]).
    #[error("the member's log is damaged; it is rebuilt from its peers under a new identity")]
    Damaged,
    /// The replica waits for room in its log to make a ready durable: until the group or its
    /// neighbours on the log compact, it takes no call but `drive` and `compact`.
    #[error("the replica waits for room in its log")]
    Stalled,
    /// A message came while a ready's update flushes, and the messages held until it is done
    /// would pass their bound: it is refused, as the network may drop it, and the sender's
    /// retries cover it.
    #[error(
        "messages held while a ready flushes would take {bytes} bytes; the replica holds {max}"
    )]
    MessagesHeld { bytes: u64, max: u64 },
    /// The member's log may lack entries it acknowledged, so it takes no part in elections
    /// until it holds them again (docs/design/raft-log.md §6).
    #[error("the member's log may lack entries it acknowledged; it does not campaign")]
    Uncertain,
    /// An entry more than the range's bound, which every member's log holds in one frame.
    #[error("an entry of {len} bytes; the range takes {max} at most")]
    EntryTooLarge { len: usize, max: u64 },
    /// Reads waiting for the next round of confirmation would hold more bytes of contexts
    /// than the range's entry bound: the read is to be asked again once a round is confirmed.
    #[error("reads waiting would hold {bytes} bytes of contexts; the range takes {max}")]
    ReadsWaiting { bytes: u64, max: u64 },
    /// Settings a member's log cannot hold to.
    #[error("the range's settings do not fit this member's log: {0}")]
    Config(&'static str),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Record(#[from] RecordError),
}

impl From<focal_raft::Error> for ReplicaError {
    fn from(e: focal_raft::Error) -> Self {
        if e.is_fatal() {
            ReplicaError::Stopped(e.to_string())
        } else {
            ReplicaError::Refused(e)
        }
    }
}
