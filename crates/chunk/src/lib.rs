//! The chunk store: one log-structured volume per device (docs/design/chunk-store.md).
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

pub mod codec;
pub mod frame;
pub mod key;
pub mod record;
pub mod superblock;

pub use key::ChunkKey;
