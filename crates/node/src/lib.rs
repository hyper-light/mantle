//! A node (docs/design/node.md): its ranges' replicas on its shards, replicating over hyper-raft's
//! transport, QUIC for everything stateful and a sealed UDP datagram plane for consensus control
//! (node.md §3).
//!
//! - [`classes`]: the kinds of message QUIC carries, their classes and who may send each.
//! - [`directory`]: the peers a connection may name, by certificate.
//! - [`frame`]: a range's Raft message as it travels.
//! - [`layout`]: what the node keeps beside its log: its run.

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

pub mod classes;
pub mod directory;
pub mod frame;
pub mod layout;
