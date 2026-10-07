//! docs/runtime.md §8: every synchronization cell is freed by its primitive's last handle. A binary of its
//! own, so no other test's live primitives are counted.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(clippy::unwrap_used, clippy::disallowed_macros)]

use hyper_rt::sync;

#[test]
fn cells_are_given_back() {
    let before = sync::cell::live();
    {
        let (tx, rx) = sync::channel::<u8>(1).unwrap();
        let (once, done) = sync::oneshot::<u8>().unwrap();
        let (mut watch, receiver) = sync::watch(0, 2).unwrap();
        let other = watch.subscribe().unwrap();
        let semaphore = sync::Semaphore::new(1, 1).unwrap();
        let notify = sync::Notify::new().unwrap();
        assert!(sync::cell::live() > before);
        drop((
            tx, rx, once, done, watch, receiver, other, semaphore, notify,
        ));
    }
    assert_eq!(
        sync::cell::live(),
        before,
        "every cell freed by its last handle"
    );
}
