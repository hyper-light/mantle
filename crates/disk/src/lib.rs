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

pub mod buf;
