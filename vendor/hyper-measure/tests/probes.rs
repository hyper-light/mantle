//! The OS probes a measurement reads beside the allocator's counts allocate nothing, so no window is
//! charged for its own probes: the process's account (`usage::this`) and its faults (`faults::read`).
//! On Linux the account was three `/proc` files read through `format!` and `read_to_string`, 7
//! allocations and 18 reallocations a read, which `hyper-rt`'s `tests/service_driverless_receipts.rs`
//! counted as the runtime's. Counted on the thread that reads them, which is where the probes run:
//! the process's count took libtest's own thread's allocations made beside the test for the probes'
//! (one of 48 to 160 bytes, never freed in the window, in 2 to 3 of 800 runs translated by Rosetta
//! under eight-way contention, every one with the reading thread's count zero), and failed CI's
//! translated job on a tree whose probes had not changed.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_macros)]

use hyper_measure::{alloc, faults, usage};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

#[test]
fn reading_the_process_account_and_its_faults_allocates_nothing() {
    assert!(alloc::installed());
    // A first read of each before the count, so that what a platform sets up once is not counted.
    let first = (usage::this(), faults::read());
    alloc::begin();
    let account = std::hint::black_box(usage::this());
    let faults = std::hint::black_box(faults::read());
    let counts = alloc::end();
    assert_eq!(
        counts.allocations, 0,
        "{counts:?} {account:?} {faults:?} {first:?}"
    );
    assert_eq!(counts.reallocations, 0, "{counts:?}");
    assert_eq!(counts.bytes, 0, "{counts:?}");
}
