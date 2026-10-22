//! What a branch cursor holds, counted by hyper-measure's counting allocator against what
//! `branch::cursor_bytes` states: cursors placed in a branch of the range's densest rows and in
//! one of its largest, each from a store whose pools hold nothing yet, so each makes its own.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_measure::alloc;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{self, Builder, Op};
use mantle_engine::store::{Config, Store};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The range's bounds on a row's key and value (mantle_meta's `key::largest_bytes` and
/// `wire::largest_row_bytes`).
const KEY: usize = 2_132;
const VALUE: usize = 18_699;

fn key(len: usize, i: u64) -> Vec<u8> {
    let mut k = vec![b'p'; len.saturating_sub(8)];
    k.extend_from_slice(&i.to_be_bytes()[8 - len.min(8)..]);
    k
}

/// Cursors placed across a branch of `rows` rows of `key_len`-byte keys and `value`-byte values:
/// the most one held, and what the statement says.
fn cursors(key_len: usize, value: usize, rows: u64, open: u64) -> (u64, u64) {
    let page = branch::least_page(4096, key_len, value).unwrap();
    let config = Config {
        page_size: page,
        extent_pages: 2,
        max_extents: 1 << 20,
    };
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    let mut store = Store::create(file, config).unwrap();
    let v = vec![b'v'; value];
    let mut b = Builder::new(&mut store, Keys::Exactly(rows)).unwrap();
    for i in 0..rows {
        b.add(&mut store, &key(key_len, i), Op::Put, &v).unwrap();
    }
    let built = b.finish(&mut store).unwrap();
    let mut most = 0u64;
    let mut held = Vec::new();
    for c in 0..open {
        alloc::begin();
        let cursor = built
            .seek(&mut store, &key(key_len, c * rows / open))
            .unwrap();
        most = most.max(u64::try_from(alloc::read().peak).unwrap());
        alloc::end();
        held.push(cursor);
    }
    for cursor in held {
        cursor.give_back(&mut store);
    }
    let stated = branch::cursor_bytes(page, 2, 1).unwrap();
    (most, stated)
}

#[test]
fn a_cursor_holds_no_more_than_it_states() {
    for (name, key_len, value, rows) in [
        ("densest", 8usize, 0usize, 100_000u64),
        ("longest", KEY, 0, 4_000),
        ("largest", KEY, VALUE, 1_000),
    ] {
        let (held, stated) = cursors(key_len, value, rows, 8);
        eprintln!("{name}: a cursor held {held} of {stated} stated");
        assert!(held <= stated, "{name}: a cursor held {held} past {stated}");
    }
}
