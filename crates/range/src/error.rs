//! What a replica fails with.

use hyper_log::LogError;
use mantle_meta::engine::EngineError;
use mantle_meta::error::MetaError;
use mantle_meta::record::RecordError;

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    /// The core refused the call or dropped the message, changing nothing.
    #[error("raft: {0}")]
    Refused(hyper_raft::Error),
    /// The replica's state no longer adds up as it opens: it does not open.
    #[error("the replica stopped: {0}")]
    Stopped(String),
    /// The member's log failed to make a write durable, or refused one for a reason no retry
    /// clears; or the core or the engine failed in a way that leaves its state unknown, or a
    /// committed entry broke the range's state machine. What the member holds may be on the
    /// device or not, so it takes no call that could acknowledge anything again. Every call
    /// answers this until the node reopens the log, from what its device kept, and opens the
    /// member afresh; the group goes on without it meanwhile (docs/design/replica.md §3, audit
    /// S04).
    #[error("the replica is fenced: {0}")]
    Fenced(String),
    /// The group's records on this member's log are damaged (docs/design/raft-log.md §6): the
    /// member does not open under its identity, which may have voted in terms the log no longer
    /// shows. It is quarantined, and rebuilt from its peers under a new identity
    /// ([`crate::Replica::rebuild`]).
    #[error("the member's log is damaged; it is rebuilt from its peers under a new identity")]
    Damaged,
    /// A write waits for room in the member's log: until the group or its neighbours on the log
    /// compact, it takes no part in the group (docs/design/replica.md §3).
    #[error("the replica waits for room in its log")]
    Stalled,
    /// The member's log may lack entries it acknowledged, and the others are no quorum of each
    /// half of its configuration without it: it does not campaign (docs/design/replica.md §4).
    #[error("the member's log may lack entries it acknowledged; it does not campaign")]
    Uncertain,
    /// An entry more than the range's bound, which every member's log holds in one frame.
    #[error("an entry of {len} bytes; the range takes {max} at most")]
    EntryTooLarge { len: usize, max: u64 },
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

impl From<hyper_raft::Error> for ReplicaError {
    fn from(e: hyper_raft::Error) -> Self {
        if e.is_fatal() {
            ReplicaError::Stopped(e.to_string())
        } else {
            ReplicaError::Refused(e)
        }
    }
}
