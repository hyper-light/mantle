//! The S3 protocol as mantle's gateway speaks it: request authentication, body framing and
//! wire formats (docs/research/05).
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::disallowed_methods
    )
)]

pub mod body;
pub mod checksum;
pub mod chunked;
pub mod conditional;
pub mod list;
pub mod range;
pub mod response;
pub mod route;
pub mod sigv4;
pub mod time;
pub mod xml;
