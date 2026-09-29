//! What the log fails with.

use mantle_disk::DiskError;

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error(transparent)]
    Disk(#[from] DiskError),
    /// A write or flush failed; nothing more is taken until the log is reopened.
    #[error("the log is fenced after a failed write or flush")]
    Fenced,
    #[error("the log is closed")]
    Closed,
    /// The queue holds all the writer takes; the submitter retries.
    #[error("the log's queue is full")]
    Busy,
    /// Every segment holds live records and the file is at its quota: groups must compact.
    #[error("the log is full of live records")]
    Full,
    #[error("a submission of {0} bytes is more than one frame holds")]
    TooLarge(usize),
    #[error("the log holds {0} groups already")]
    TooManyGroups(usize),
    /// The group would retain more entries than its bound: it compacts first.
    #[error("group {0:032x} would retain more than its bound")]
    Backlog(u128),
    #[error("group {group:032x}: {reason}")]
    Invalid { group: u128, reason: &'static str },
    #[error("group {group:032x} holds no entry {index}")]
    Unavailable { group: u128, index: u64 },
    #[error("group {group:032x} compacted entries before {first}")]
    Compacted { group: u128, first: u64 },
    /// An entry read back does not verify as the one asked for.
    #[error("entry {index} of group {group:032x} does not verify")]
    Corrupt { group: u128, index: u64 },
    /// State the log acknowledged is damaged: the replicas on this log recover from peers.
    #[error("the log's acknowledged state is damaged: {0}")]
    Damaged(&'static str),
    /// The file holds no log, another log, or another geometry.
    #[error("the file is not this log: {0}")]
    Foreign(&'static str),
    #[error("the log's configuration is invalid: {0}")]
    Config(&'static str),
}
