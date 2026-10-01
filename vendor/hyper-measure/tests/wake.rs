//! A counting waker counts every wake of it and of its clones, and tells its tag each time.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_macros)]

use hyper_measure::wake;

#[test]
fn a_waker_counts_its_wakes_and_its_clones_count_with_it() {
    let (tell, heard) = std::sync::mpsc::sync_channel(4);
    let (waker, slot) = wake::waker(7, tell);
    let clone = waker.clone();
    waker.wake_by_ref();
    clone.wake();
    assert_eq!(slot.wakes(), 2);
    assert_eq!(heard.try_recv().ok(), Some(7));
    assert_eq!(heard.try_recv().ok(), Some(7));
    drop(waker);
    assert!(heard.try_recv().is_err());
}
