//! Impossible numeric queue shapes refuse without unwinding or losing arena owners.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]
use hyper_rt::error::RtError;
use hyper_rt::sync::{SyncError, channel_with};

#[test]
fn impossible_value_ticket_shapes_are_typed_refusals_then_a_healthy_channel_works() {
    for capacity in [usize::MAX, usize::MAX - 1] {
        let outcome = std::panic::catch_unwind(|| channel_with::<u8>(capacity, 1));
        assert!(
            matches!(outcome, Ok(Err(RtError::BadConfig { .. }))),
            "impossible std ticket arithmetic is refused before construction"
        );
    }
    let (tx, mut rx) = channel_with::<u8>(1, 1).unwrap();
    tx.try_send(42).unwrap();
    assert_eq!(rx.try_recv(), Ok(Some(42)));
    drop(tx);
    assert_eq!(rx.try_recv(), Err(SyncError::Closed(())));
}
