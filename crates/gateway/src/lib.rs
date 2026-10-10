//! The gateway's object path (docs/design/gateway.md): a PUT's body sealed, cut into blocks,
//! coded into chunks and written with its Block, File and Name rows, and a GET's range read
//! back, each as a state machine that names its requests and does no I/O.
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

pub mod complete;
pub mod get;
pub mod layout;
pub mod put;

/// `n` over `d`, rounded up. The divisor's type rules out the zero that makes
/// `u64::div_ceil` panic, and a zero numerator divides to zero.
pub(crate) fn div_ceil(n: u64, d: std::num::NonZeroU64) -> u64 {
    std::num::NonZeroU64::new(n).map_or(0, |n| n.div_ceil(d).get())
}
