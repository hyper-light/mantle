//! The metadata service (docs/design/metadata.md): buckets, the versions of every object,
//! multipart uploads, and which files, blocks and chunks hold an object's bytes, kept in
//! ranges replicated by Raft.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cast_possible_truncation
    )
)]

pub mod apply;
pub mod block;
pub mod bucket;
pub mod clock;
pub mod coordinator;
pub mod engine;
pub mod error;
pub mod file;
pub mod key;
pub mod name;
pub mod overlay;
pub mod reclaim;
pub mod record;
pub mod session;
pub mod wire;
