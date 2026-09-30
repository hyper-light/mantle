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
