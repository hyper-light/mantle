//! A node (docs/design/node.md): its ranges' replicas on its shards, replicating over hyper-raft's
//! transport, QUIC for everything stateful and a sealed UDP datagram plane for consensus control
//! (node.md §3).
//!
//! - [`bounds`]: every bound of the transport, derived from what the node states.
//! - [`classes`]: the kinds of message QUIC carries, their classes and who may send each.
//! - [`client`]: a client of the cell's ranges, asking their leaders to apply entries.
//! - [`command`]: a command for a range as a client asks it and a node answers it.
//! - [`directory`]: the peers a connection may name, by certificate.
//! - [`frame`]: a range's Raft message as it travels.
//! - [`layout`]: what the node keeps beside its log: its run.
//! - [`node`]: the owner: the ranges, their log, the transport, one turn at a time.
//! - [`process`]: a node as a process, `mantle node`: its configuration and its run.
//! - [`read`]: a read a range's leader answers by ReadIndex.

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

pub mod bounds;
pub mod classes;
pub mod client;
pub mod command;
pub mod directory;
pub mod frame;
pub mod layout;
pub mod node;
pub mod process;
pub mod read;
