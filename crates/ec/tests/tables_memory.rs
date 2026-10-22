//! The tables the coding library builds at its first use, once a process, counted exactly by
//! hyper-measure's counting allocator: a process of its own, so nothing before has built them.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_measure::alloc;
use mantle_ec::{Code, TABLE_BYTES};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

#[test]
fn the_librarys_tables_are_their_stated_bytes() {
    let code = Code::new(2, 1).unwrap();
    let block = [1u8; 128];
    alloc::begin();
    let chunks = code.encode(&block).unwrap();
    let decoded = code
        .decode(&[(1, &chunks[1][..]), (2, &chunks[2][..])], 128)
        .unwrap();
    let first = alloc::end();
    assert_eq!(decoded, block);
    // What stays held is the tables and the calls' answers, which the test still holds.
    let answers = chunks.iter().map(Vec::capacity).sum::<usize>()
        + chunks.capacity() * std::mem::size_of::<Vec<u8>>()
        + decoded.capacity();
    let held = u64::try_from(first.live).unwrap() - answers as u64;
    assert_eq!(held, TABLE_BYTES);
}
