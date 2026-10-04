//! RocksDB's memory/arena_test.cc, test for test: its 6 test definitions, as tests of the
//! memtable node store's accounting (memory/arena.rs, docs/research/24 §3.1 P2).
//!
//! The arena keeps RocksDB's accounting exactly (inline block, block size, a block of its own
//! for an allocation above a quarter block, aligned allocations from the front), so the
//! accounting tests keep their literals. RocksDB runs each twice, with and without a 2 MiB
//! huge-page size; the port does not allocate huge pages (`MAP_HUGETLB` is Linux-only and the
//! memtable's blocks are ordinary allocations), so each runs once, without.
//!
//! `MmapTest.AllocateLazyZeroed` and `UnmappedAllocation` measure the process's resident set,
//! which another test's allocations would move, so they run alone in their own process:
//! tests/arena_resident_test.rs.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

#[allow(dead_code)]
#[path = "support/random.rs"]
mod random;

use mantle_engine::memory::arena::{ALIGN_UNIT, Addr, Arena, INLINE_SIZE, MIN_BLOCK_SIZE};
use random::Random;

#[test]
fn empty() {
    let _arena0 = Arena::new(MIN_BLOCK_SIZE, None).unwrap();
}

/// The value returned by `MemoryAllocatedBytes` may be greater than the requested memory; the
/// C++ bounds it by `expected * 1.1`.
fn check_memory_allocated(allocated: usize, expected: usize) -> bool {
    let max_expected = expected + expected / 10;
    allocated >= expected && allocated <= max_expected
}

#[test]
fn memory_allocated_bytes() {
    const N: usize = 17;
    let bsz = 32 * 1024; // block size

    let mut arena = Arena::new(bsz, None).unwrap();

    // requested size > quarter of a block:
    //   allocate requested size separately
    let req_sz = 12 * 1024;
    for _ in 0..N {
        arena.allocate(req_sz).unwrap();
    }
    let mut expected_memory_allocated = req_sz * N + INLINE_SIZE;
    assert!(check_memory_allocated(
        arena.memory_allocated_bytes(),
        expected_memory_allocated
    ));

    arena.allocate(INLINE_SIZE - 1).unwrap();

    // requested size < quarter of a block:
    //   allocate a block with the default size, then try to use unused part
    //   of the block. So one new block will be allocated for the first
    //   Allocate(99) call. All the remaining calls won't lead to new allocation.
    let req_sz = 99;
    for _ in 0..N {
        arena.allocate(req_sz).unwrap();
    }
    expected_memory_allocated += bsz;
    assert!(check_memory_allocated(
        arena.memory_allocated_bytes(),
        expected_memory_allocated
    ));

    // requested size > size of a block:
    //   allocate requested size separately
    let mut expected_memory_allocated = arena.memory_allocated_bytes();
    let req_sz = 8 * 1024 * 1024;
    for _ in 0..N {
        arena.allocate(req_sz).unwrap();
    }
    expected_memory_allocated += req_sz * N;
    assert!(check_memory_allocated(
        arena.memory_allocated_bytes(),
        expected_memory_allocated
    ));
}

/// Make sure we didn't count the allocate but not used memory space in
/// `ApproximateMemoryUsage`.
#[test]
fn approximate_memory_usage() {
    const BLOCK_SIZE: usize = 4096;
    const ENTRY_SIZE: usize = BLOCK_SIZE / 8;
    let mut arena = Arena::new(BLOCK_SIZE, None).unwrap();
    assert_eq!(0, arena.approximate_memory_usage());

    // allocate inline bytes
    assert!(arena.is_in_inline_block());
    arena.allocate_aligned(ALIGN_UNIT).unwrap();
    assert!(arena.is_in_inline_block());
    arena
        .allocate_aligned(INLINE_SIZE / 2 - (2 * ALIGN_UNIT))
        .unwrap();
    assert!(arena.is_in_inline_block());
    arena.allocate_aligned(INLINE_SIZE / 2).unwrap();
    assert!(arena.is_in_inline_block());
    assert_eq!(arena.approximate_memory_usage(), INLINE_SIZE - ALIGN_UNIT);
    assert!(check_memory_allocated(
        arena.memory_allocated_bytes(),
        INLINE_SIZE
    ));

    let num_blocks = BLOCK_SIZE / ENTRY_SIZE;

    // first allocation
    arena.allocate_aligned(ENTRY_SIZE).unwrap();
    assert!(!arena.is_in_inline_block());
    let mem_usage = arena.memory_allocated_bytes();
    assert!(check_memory_allocated(mem_usage, BLOCK_SIZE + INLINE_SIZE));
    let mut usage = arena.approximate_memory_usage();
    assert!(usage < mem_usage);
    for _ in 1..num_blocks {
        arena.allocate_aligned(ENTRY_SIZE).unwrap();
        assert_eq!(mem_usage, arena.memory_allocated_bytes());
        assert_eq!(arena.approximate_memory_usage(), usage + ENTRY_SIZE);
        assert!(!arena.is_in_inline_block());
        usage = arena.approximate_memory_usage();
    }
    assert!(usage > mem_usage);
}

#[test]
fn simple() {
    let mut allocated: Vec<(usize, Addr)> = Vec::new();
    let mut arena = Arena::new(MIN_BLOCK_SIZE, None).unwrap();
    const N: usize = 100_000;
    let mut bytes = 0usize;
    let mut rnd = Random::new(301);
    for i in 0..N {
        let mut s = if i % (N / 10) == 0 {
            i
        } else if rnd.one_in(4000) {
            rnd.uniform(6000) as usize
        } else if rnd.one_in(10) {
            rnd.uniform(100) as usize
        } else {
            rnd.uniform(20) as usize
        };
        if s == 0 {
            // Our arena disallows size 0 allocations.
            s = 1;
        }
        let r = if rnd.one_in(10) {
            arena.allocate_aligned(s).unwrap()
        } else {
            arena.allocate(s).unwrap()
        };

        // Fill the "i"th allocation with a known bit pattern
        arena.write(r, &vec![(i % 256) as u8; s]).unwrap();
        bytes += s;
        allocated.push((s, r));
        assert!(arena.approximate_memory_usage() >= bytes);
        if i > N / 10 {
            assert!(arena.approximate_memory_usage() as f64 <= bytes as f64 * 1.10);
        }
    }
    for (i, &(num_bytes, p)) in allocated.iter().enumerate() {
        // Check the "i"th allocation for the known bit pattern
        let mut out = Vec::new();
        arena.store().read_bytes(p, num_bytes, &mut out).unwrap();
        assert!(out.iter().all(|&b| usize::from(b) == i % 256));
    }
}
