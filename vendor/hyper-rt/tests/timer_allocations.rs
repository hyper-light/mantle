//! AC-0.4, §4.3: reserving a bounded wheel must not issue one allocation per wheel bucket.
//! Count the actual allocator calls, then fill, renew and expire timers without allocating.
//! Each thread counts its own allocator calls (hyper-measure's counting allocator, whose own tests
//! check that another thread's allocations are not attributed to this one: slates' version of that
//! check is not carried, ORIGIN.md).

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::string_slice,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::missing_panics_doc
)]
use hyper_measure::alloc::{self, Counting};
use hyper_rt::timer::Wheel;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Allocator calls (allocations and reallocations) on this thread while `work` runs.
fn calls_during<T>(work: impl FnOnce() -> T) -> (T, u64) {
    alloc::begin();
    let out = work();
    let counts = alloc::end();
    (out, counts.calls())
}

/// AC-0.4: do reserve CI's reported capacity, fill a smaller non-power-of-two wheel,
/// renew every timer, refuse stale cancellation and capacity overflow, then expire it.
/// Expect bounded boot allocations and no allocator calls while using the reserved slots.
#[test]
fn reservation_cost_is_independent_of_capacity_and_renewals_allocate_nothing() {
    let (small, small_calls) = calls_during(|| Wheel::new(1, 65, 0));
    let (large, large_calls) = calls_during(|| Wheel::new(1, 1_617_130, 0));
    eprintln!("wheel allocations: 65 timers = {small_calls}; 1617130 timers = {large_calls}");
    assert_eq!(
        large_calls, small_calls,
        "reservation must not scale with possible leases"
    );
    drop((small, large));

    let capacity = 129;
    let mut wheel = Wheel::new(1, capacity, 0);
    let mut fired = Vec::with_capacity(capacity);
    let ids = || (0..capacity).map(|id| u32::try_from(id).unwrap());
    alloc::begin();
    for id in ids() {
        wheel.arm(id, 10, u64::from(id)).unwrap();
    }
    assert!(
        wheel
            .arm(u32::try_from(capacity).unwrap(), 10, u64::MAX)
            .is_err(),
        "past the capacity"
    );
    for id in ids() {
        wheel.disarm(id).unwrap();
        wheel.arm(id, 20, u64::from(id)).unwrap();
        assert!(
            wheel.arm(id, 20, 0).is_err(),
            "an armed timer is not armed twice"
        );
    }
    wheel.advance(10, &mut fired);
    assert!(fired.is_empty());
    wheel.advance(20, &mut fired);
    let calls = alloc::end().calls();
    assert_eq!(
        calls, 0,
        "insertion, renewal and expiry stay in reserved storage"
    );
    let mut words: Vec<u64> = fired.iter().map(|(_, word)| *word).collect();
    words.sort_unstable();
    assert_eq!(
        words,
        (0..u64::try_from(capacity).unwrap()).collect::<Vec<_>>()
    );
}
