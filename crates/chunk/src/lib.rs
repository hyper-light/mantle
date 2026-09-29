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

mod clean;
mod error;
pub mod frame;
pub mod index;
pub mod key;
pub mod layout;
mod log;
mod read;
pub mod record;
mod recover;
mod scrub;
pub mod superblock;
mod volume;
mod writer;

pub use clean::CleanReport;
pub use error::ChunkError;
pub use key::ChunkKey;
pub use layout::{Config, Limits, Reads};
pub use read::ReadStats;
pub use recover::RecoveryReport;
pub use volume::{ChunkStat, Usage, Volume};
