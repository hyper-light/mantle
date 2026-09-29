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
