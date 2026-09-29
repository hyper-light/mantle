//! A range replica (docs/design/replica.md): focal-raft's core driven over the device's
//! shared Raft log (docs/design/raft-log.md) and the range's engine.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

mod conf;
mod error;
mod image;
mod replica;
mod store;

pub use error::ReplicaError;
pub use focal_raft::proto::{ConfChangeV2, ConfState, Message, MessageType};
pub use replica::{Applied, Drive, Range, Replica, Settings};

/// Whether a row is one a member keeps for itself rather than one its range replicates: the
/// point of the last snapshot it installed. Members of one range hold the same rows but
/// these.
pub fn member_local(key: &[u8]) -> bool {
    key == conf::INSTALLED
}
