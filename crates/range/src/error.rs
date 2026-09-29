//! What a replica fails with.

use mantle_log::LogError;
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
    /// The replica waits for room in its log to make a ready durable: until the group or its
    /// neighbours on the log compact, it takes no call but `drive` and `compact`.
    #[error("the replica waits for room in its log")]
    Stalled,
    /// The member's log may lack entries it acknowledged, so it takes no part in elections
    /// until it holds them again (docs/design/raft-log.md §6).
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

impl From<focal_raft::Error> for ReplicaError {
    fn from(e: focal_raft::Error) -> Self {
        if e.is_fatal() {
            ReplicaError::Stopped(e.to_string())
        } else {
            ReplicaError::Refused(e)
        }
    }
}
