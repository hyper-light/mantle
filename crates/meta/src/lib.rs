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

pub mod engine;
pub mod key;
pub mod record;
