//! The OS probes a measurement reads beside the allocator's counts allocate nothing, so no window is
//! charged for its own probes: the process's account (`usage::this`) and its faults (`faults::read`).
//! On Linux the account was three `/proc` files read through `format!` and `read_to_string`, 7
//! allocations and 18 reallocations a read, which `hyper-rt`'s `tests/service_driverless_receipts.rs`
//! counted as the runtime's. One test in its own binary, so that no other test's allocations share the
//! process count.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_macros)]

use hyper_measure::{alloc, faults, usage};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

#[test]
fn reading_the_process_account_and_its_faults_allocates_nothing() {
    assert!(alloc::installed());
    // A first read of each before the count, so that what a platform sets up once is not counted.
    let first = (usage::this(), faults::read());
    alloc::begin_process();
    let account = std::hint::black_box(usage::this());
    let faults = std::hint::black_box(faults::read());
    let counts = alloc::end_process();
    assert_eq!(
        counts.allocations, 0,
        "{counts:?} {account:?} {faults:?} {first:?}"
    );
    assert_eq!(counts.reallocations, 0, "{counts:?}");
    assert_eq!(counts.bytes, 0, "{counts:?}");
}
