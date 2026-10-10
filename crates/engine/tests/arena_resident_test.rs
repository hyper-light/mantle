//! RocksDB's `MmapTest.AllocateLazyZeroed` and `ArenaTest.UnmappedAllocation`
//! (memory/arena_test.cc): a large arena block is not resident until written. RocksDB counts
//! minor page faults with `getrusage`; here the claim is measured as the process's resident set,
//! read with `ps` (a real process; no `unsafe` needed), before and after writing. A resident set
//! moves with every allocation in the process, so both run in this binary's one test, one after
//! the other, with no other test beside them. Unix only, where `ps -o rss=` exists.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]
#![cfg(unix)]

use mantle_engine::memory::arena::{Addr, Arena, MIN_BLOCK_SIZE};

/// The only test in this process, so nothing else allocates while it measures.
#[test]
fn arena_blocks_are_resident_only_once_written() {
    allocate_lazy_zeroed();
    unmapped_allocation();
}

/// The process's resident set in bytes, from `ps -o rss=` (kibibytes).
fn resident_bytes() -> usize {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout)
        .unwrap()
        .trim()
        .parse::<usize>()
        .unwrap()
        * 1024
}

/// `MmapTest.AllocateLazyZeroed`, on an arena block of the C++'s length: it reads as zeros,
/// and writing half of it, then the other half, makes about that much of it resident.
fn allocate_lazy_zeroed() {
    // Doesn't have to be page aligned
    const LEN: usize = 1_234_567; // in bytes
    const COUNT: usize = LEN / 8; // in u64 objects
    // One allocation of LEN, a block of its own (above a quarter block). The allocator may
    // hand back memory an earlier allocation left resident; as `UnmappedAllocation` does, try
    // fresh blocks until one is not.
    let mut arena = Arena::new(MIN_BLOCK_SIZE, None).unwrap();
    let words = |arena: &Arena, arr: Addr, from: usize, to: usize| {
        (from..to)
            .map(|i| {
                arena
                    .store()
                    .word_at(arr, i)
                    .unwrap()
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .collect::<Vec<u64>>()
    };
    for attempt in 0.. {
        let before = resident_bytes();
        let arr = arena.allocate_aligned(LEN).unwrap();

        // Access half of the allocation
        assert!(words(&arena, arr, 0, COUNT / 2).iter().all(|&w| w == 0));
        for i in 0..COUNT / 2 {
            arena
                .write(arr.offset_by(i * 8).unwrap(), &(i as u64).to_le_bytes())
                .unwrap();
        }
        let half = resident_bytes();
        // Appropriate residency (maybe more)
        if half.saturating_sub(before) < LEN / 2 * 3 / 4 {
            assert!(attempt < 1000, "no fresh block in 1000 allocations");
            continue;
        }

        // Access rest of the allocation
        assert!(words(&arena, arr, COUNT / 2, COUNT).iter().all(|&w| w == 0));
        for i in COUNT / 2..COUNT {
            arena
                .write(arr.offset_by(i * 8).unwrap(), &(i as u64).to_le_bytes())
                .unwrap();
        }
        let all = resident_bytes();
        assert!(
            all.saturating_sub(half) >= LEN / 2 * 3 / 4,
            "after half {half}, after all {all}"
        );

        // Verify data
        let got = words(&arena, arr, 0, COUNT);
        assert!(got.iter().enumerate().all(|(i, &w)| w == i as u64));
        break;
    }
}

/// `UnmappedAllocation`: a large block is not resident until written, so the arena neither
/// wastes memory nor spends time initializing it.
fn unmapped_allocation() {
    // This block size value is smaller than the smallest x86 huge page size,
    // so should not be fulfilled by a transparent huge page mapping.
    const BLOCK_SIZE: usize = 1 << 20;
    let mut arena = Arena::new(BLOCK_SIZE, None).unwrap();

    // The allocator might give us back recycled memory for a while, but
    // shouldn't last forever.
    let pattern: Vec<u8> = (0..BLOCK_SIZE).map(|j| (j & 255) as u8).collect();
    for i in 0.. {
        let p = arena.allocate(BLOCK_SIZE).unwrap();

        let before = resident_bytes();
        // Overwrite the whole allocation
        arena.write(p, &pattern).unwrap();
        let after = resident_bytes();
        if after.saturating_sub(before) >= BLOCK_SIZE * 3 / 4 {
            // Most of the access made pages resident => GOOD
            break;
        }
        // Should have succeeded after enough tries
        assert!(i < 1000);
    }
}
