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
