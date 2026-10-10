//! Independent std waiter queue numeric shapes, no Room or new refusal variants.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]
use hyper_rt::error::RtError;
use hyper_rt::sync::{Semaphore, channel_with};
#[test]
fn impossible_channel_waiter_queue_shapes_refuse_then_healthy_queue_works() {
    for waiters in [usize::MAX, usize::MAX - 1] {
        let outcome = std::panic::catch_unwind(|| channel_with::<u8>(1, waiters));
        assert!(matches!(outcome, Ok(Err(RtError::BadConfig { .. }))));
    }
    let (tx, mut rx) = channel_with::<u8>(1, 1).unwrap();
    tx.try_send(42).unwrap();
    assert_eq!(rx.try_recv(), Ok(Some(42)));
}
#[test]
fn impossible_semaphore_waiter_queue_shapes_refuse_then_healthy_permit_works() {
    for waiters in [usize::MAX, usize::MAX - 1] {
        let outcome = std::panic::catch_unwind(|| Semaphore::new(0, waiters));
        assert!(matches!(outcome, Ok(Err(RtError::BadConfig { .. }))));
    }
    let semaphore = Semaphore::new(1, 1).unwrap();
    let permit = semaphore.try_acquire().unwrap();
    assert_eq!(semaphore.available(), 0);
    drop(permit);
    assert_eq!(semaphore.available(), 1);
}
