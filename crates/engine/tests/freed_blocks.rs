//! glibc's mmap threshold kept at its initial value (`port::mmap::return_freed_blocks`): a freed
//! large block's memory goes back to the OS when it is freed, even after a larger block was freed,
//! which raises a threshold left to adjust itself past it (mallopt(3), `M_MMAP_THRESHOLD`). Left to
//! adjust, the threshold would have the block come from glibc's heap and stay mapped there once
//! freed. One test, so no other test's allocations share the process.
#![cfg(all(target_os = "linux", target_env = "gnu"))]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

/// Whether any of the process's mappings holds `address` (proc(5), `/proc/self/maps`).
fn mapped(address: usize) -> bool {
    std::fs::read_to_string("/proc/self/maps")
        .unwrap()
        .lines()
        .any(|line| {
            let range = line.split_whitespace().next().unwrap();
            let (from, to) = range.split_once('-').unwrap();
            let from = usize::from_str_radix(from, 16).unwrap();
            let to = usize::from_str_radix(to, 16).unwrap();
            from <= address && address < to
        })
}

#[test]
fn a_freed_large_block_goes_back_to_the_os_once_the_threshold_is_kept() {
    mantle_engine::port::mmap::return_freed_blocks();
    // A larger block freed first: a threshold left to adjust itself rises to its size.
    drop(std::hint::black_box(vec![1u8; 8 << 20]));
    let block = std::hint::black_box(vec![1u8; 4 << 20]);
    let address = block.as_ptr() as usize;
    assert!(mapped(address), "the block is not mapped");
    drop(block);
    assert!(
        !mapped(address),
        "the freed block's memory stayed mapped, kept in glibc's heap"
    );
}
