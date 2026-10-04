//! RocksDB's memory/arena_test.cc, test for test: its 6 test definitions, as tests of the
//! memtable node store's accounting (memory/arena.rs, docs/research/24 §3.1 P2).
//!
//! The arena keeps RocksDB's accounting exactly (inline block, block size, a block of its own
//! for an allocation above a quarter block, aligned allocations from the front), so the
//! accounting tests keep their literals. RocksDB runs each twice, with and without a 2 MiB
//! huge-page size; the port does not allocate huge pages (`MAP_HUGETLB` is Linux-only and the
//! memtable's blocks are ordinary allocations), so each runs once, without.
//!
//! `MmapTest.AllocateLazyZeroed` and `UnmappedAllocation` test that a large allocation is not
//! touched until written: RocksDB counts minor page faults with `getrusage`. The port's blocks
//! are allocated zeroed by the allocator (`calloc`, through zerocopy's `new_box_zeroed`), which
//! for a large block maps fresh zero pages without writing them; the tests measure that claim
//! as the process's resident set, read with `ps` (a real process; no `unsafe` needed), before
//! and after writing. `AllocateLazyZeroed` tests RocksDB's `MemMapping` (port/mmap.h), which
//! the port does not have before P9; its claim is tested on the arena's block of the same
//! length, the allocation the memtable makes. Both run on Unix, where `ps -o rss=` exists.
//!
//! Every test holds one lock, so the resident-set measurements see no other test's memory.
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

use std::sync::Mutex;

use mantle_engine::memory::arena::{ALIGN_UNIT, Addr, Arena, INLINE_SIZE, MIN_BLOCK_SIZE};
use random::Random;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn empty() {
    let _g = serial();
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
    let _g = serial();
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
    let _g = serial();
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
    let _g = serial();
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

/// The process's resident set in bytes, from `ps -o rss=` (kibibytes).
#[cfg(unix)]
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
#[cfg(unix)]
#[test]
fn allocate_lazy_zeroed() {
    let _g = serial();
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
#[cfg(unix)]
#[test]
fn unmapped_allocation() {
    let _g = serial();
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
