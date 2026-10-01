//! The counts are what the allocator was asked, and the faults are what the
//! OS charged.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_macros)]

use hyper_measure::{alloc, faults, stats};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

#[test]
fn an_allocation_a_reallocation_and_a_free_are_counted_with_their_bytes() {
    assert!(alloc::installed());
    alloc::begin();
    let mut bytes: Vec<u8> = Vec::with_capacity(100);
    bytes.reserve_exact(300);
    let peak_before_free = alloc::read().peak;
    drop(bytes);
    let counts = alloc::end();
    assert_eq!(counts.allocations, 1);
    assert_eq!(counts.reallocations, 1);
    assert_eq!(counts.frees, 1);
    // 100 asked for, then grown by 200 to 300.
    assert_eq!(counts.bytes, 300);
    assert_eq!(peak_before_free, 300);
    assert_eq!(counts.peak, 300);
    assert_eq!(counts.live, 0);
}

#[test]
fn nothing_is_counted_while_paused_or_before_counting_begins() {
    let early: Vec<u64> = Vec::with_capacity(8);
    alloc::begin();
    alloc::pause();
    let paused: Vec<u64> = Vec::with_capacity(8);
    alloc::resume();
    let counted: Vec<u64> = Vec::with_capacity(8);
    let counts = alloc::end();
    drop((early, paused, counted));
    assert_eq!(counts.allocations, 1);
    assert_eq!(counts.bytes, 64);
}

#[test]
fn touching_fresh_pages_is_charged_as_faults() {
    let before = faults::read().expect("the OS counts faults");
    // Sixty-four MiB the process never touched: each page faults in once
    // when first written.
    let mut fresh: Vec<u8> = Vec::with_capacity(64 << 20);
    fresh.resize(64 << 20, 1);
    let after = faults::read().expect("the OS counts faults");
    let charged = after.since(&before);
    assert!(charged.total() > 0, "{charged:?}");
    #[cfg(target_vendor = "apple")]
    assert!(
        charged.task.expect("task_info on macOS").faults > 0,
        "{charged:?}"
    );
    drop(fresh);
}

#[test]
fn the_band_holds_the_ratio_of_every_two_halves() {
    let same = [10.0; 8];
    assert_eq!(stats::band(&same), Some((1.0, 1.0)));
    let runs = [9.0, 10.0, 11.0, 10.0, 12.0, 8.0];
    let (low, high) = stats::band(&runs).unwrap();
    assert!(low < 1.0 && high > 1.0 && (low * high - 1.0).abs() < 1e-9);
    assert_eq!(stats::band(&runs[..3]), None);
    let summary = stats::Summary::of(&runs).unwrap();
    assert_eq!(
        (summary.median, summary.min, summary.max),
        (10.0, 8.0, 12.0)
    );
}

#[test]
fn work_set_aside_is_in_the_total_and_apart() {
    alloc::begin();
    let measured: Vec<u8> = Vec::with_capacity(16);
    alloc::aside();
    let harness: Vec<u8> = Vec::with_capacity(48);
    alloc::back();
    let total = alloc::end();
    let aside = alloc::read_aside();
    drop((measured, harness));
    assert_eq!((total.allocations, total.bytes), (2, 64));
    assert_eq!((aside.allocations, aside.bytes), (1, 48));
    let core = total.less(&aside);
    assert_eq!((core.allocations, core.bytes), (1, 16));
}
