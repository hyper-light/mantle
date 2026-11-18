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
mod stored;

pub use error::ReplicaError;
pub use hyper_durable::{Driven, GroupStore, LogStore};
pub use hyper_raft::Elections;
pub use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Entry,
    Message, MessageType,
};
pub use machine::{Applied, RangeMachine};
pub use replica::{
    Output, Range, Replica, Settings, Shell, claim, limits, open_shell, propose_encoded,
    propose_entry, ready_bytes, ready_held, remove,
};
pub use stored::{Stored, holds_rows};

/// hyper-measure's counting allocator under the unit tests, which count what an encoder allocates
/// (Lean doors step 7: a message whose length is known first is made at it).
#[cfg(test)]
#[global_allocator]
static ALLOCATOR: hyper_measure::alloc::Counting = hyper_measure::alloc::Counting;

/// The allocations and reallocations `f` makes on this thread.
#[cfg(test)]
fn counted<T>(f: impl FnOnce() -> T) -> (T, (u64, u64)) {
    hyper_measure::alloc::begin();
    let out = f();
    let counts = hyper_measure::alloc::end();
    (out, (counts.allocations, counts.reallocations))
}
