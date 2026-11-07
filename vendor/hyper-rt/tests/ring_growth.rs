//! What a receive ring's growth costs: the buffers it adds, and the pages it touches making them.
//! A ring grows when a drain wanted more buffers than it holds and shrinks after a spell that
//! wanted fewer (`Ring::wants`), so a transfer's bursts grow it again and again: mantle's ledger
//! counted 15 allocations and 1 MB an operation on a native PUT of 8 MiB's servers
//! (`Ring::resize`, docs/research/80 "The sites"). Each buffer was zeroed as it was made, every one
//! of its pages touched and so faulted in, though the kernel writes what it receives into it and
//! nothing reads past that. Counted by hyper-measure's allocator and the OS's fault count, in a
//! process of its own.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_measure::{alloc, faults};
use hyper_rt::udp::{Io, RECEIVE_BYTES, Ring};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The buffers the ring grows to: Linux's bound for a batch of this size.
const BATCH: usize = 64;

/// A ring grown from one buffer to its bound allocates each buffer it adds, and touches none of
/// their pages: what the allocator writes of its own beside each, a page a buffer at most. Before,
/// zeroing each touched its sixteen pages.
#[test]
fn a_ring_grows_without_touching_its_buffers() {
    if !cfg!(target_os = "linux") {
        // Elsewhere a ring holds one buffer and never grows.
        return;
    }
    let mut ring = Ring::new(Io { batch: BATCH }).unwrap();
    let added = (BATCH - 1) as u64;
    let before = faults::read().unwrap();
    alloc::begin();
    ring.resize(BATCH).unwrap();
    let counts = alloc::end();
    let after = faults::read().unwrap();
    let touched = after.since(&before).minor;
    assert!(
        alloc::installed(),
        "the counting allocator is not installed"
    );
    eprintln!(
        "grown to {} buffers: {counts:?}, {touched} minor faults",
        ring.buffers()
    );
    assert_eq!(ring.buffers(), BATCH);
    assert_eq!(
        (counts.allocations, counts.bytes),
        (added, added * RECEIVE_BYTES as u64),
        "a buffer each added"
    );
    assert!(
        touched <= added,
        "{touched} pages touched growing {added} buffers of {RECEIVE_BYTES} bytes"
    );
}
