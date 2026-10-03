//! A range replica (docs/design/replica.md): hyper-raft's durable shell, its core inside, over
//! the device's shared Raft log (docs/design/raft-log.md) and the range's engine.
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
mod machine;
pub mod membership;
mod replica;

pub use error::ReplicaError;
pub use hyper_durable::{Driven, GroupStore, LogStore};
pub use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Entry,
    Message, MessageType,
};
pub use machine::Applied;
pub use replica::{Output, Range, Replica, Settings, claim, remove};
