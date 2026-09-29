//! Local storage devices: what the device under a path is, what it measures as, and how to
//! move bytes to it with alignment and durability the device and OS actually guarantee.
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

use std::path::PathBuf;

pub mod block;
pub mod buf;
pub mod calibrate;
pub mod commit;
pub mod file;
pub mod histogram;
pub mod identity;
pub mod measure;
pub mod probe;
pub mod rounds;
#[cfg(any(test, feature = "sim"))]
pub mod sim;

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("{op} {}: {source}", path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("direct transfer at offset {offset} of {len} bytes is not aligned to {align}")]
    Misaligned {
        offset: u64,
        len: usize,
        align: usize,
    },
    #[error("{} ended at offset {offset}: {missing} bytes short", path.display())]
    ShortRead {
        path: PathBuf,
        offset: u64,
        missing: usize,
    },
    #[error(transparent)]
    Buf(#[from] buf::BufError),
}
