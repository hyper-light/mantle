//! The engine's per-entry paths allocate nothing once their buffers have grown, counted exactly
//! by hyper-measure's counting allocator: a branch builder adding entries, and a merge stepping
//! over branches, as a compaction does both for every key. Each was three allocations a key
//! before its buffers were kept (2026-10-07: 1.45 allocations and 0.90 reallocations per put
//! at 3 M, now 0.087 and 0.000, benches/shard_db.rs).
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
use mantle_engine::branch::merge::Merge;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::store::{Config, Store};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 4096,
};
const ENTRIES: u64 = 4_000;

fn key(n: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&n.to_be_bytes());
    k
}

/// A store on a real file: the simulated device allocates as it keeps what is written, and
/// would be counted with the engine.
fn store(dir: &tempfile::TempDir) -> Store<DeviceFile> {
    let align = Alignment::new(4096).unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        align,
    )
    .unwrap();
    Store::create(file, CONFIG).unwrap()
}

/// A branch of every `step`-th key from `first`, the values distinct.
fn build(s: &mut Store<DeviceFile>, first: u64, step: u64) -> Branch {
    let mut b = Builder::new(s, Keys::Exactly(ENTRIES)).unwrap();
    for i in 0..ENTRIES {
        let n = first + i * step;
        b.add(s, &key(n), Op::Put, &n.to_le_bytes()).unwrap();
    }
    b.finish(s).unwrap()
}

#[test]
fn a_builder_adds_entries_without_allocating_once_its_pages_have_grown() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(&dir);
    // Extent buffers come from the store's pool: a first branch's written runs fill it.
    build(&mut s, 0, 1);
    let mut b = Builder::new(&mut s, Keys::Exactly(ENTRIES)).unwrap();
    let value = [7u8; 40];
    for n in 0..ENTRIES / 2 {
        b.add(&mut s, &key(n), Op::Put, &value).unwrap();
    }
    alloc::begin();
    for n in ENTRIES / 2..ENTRIES {
        b.add(&mut s, &key(n), Op::Put, &value).unwrap();
    }
    let counts = alloc::end();
    // No allocation at all. The one reallocation is the branch's own list of extents doubling
    // as it reaches its fifth: the list is the branch's output, grown geometrically.
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 1),
        "{counts:?}"
    );
    let branch = b.finish(&mut s).unwrap();
    assert!(branch.extents.len() > 4, "{}", branch.extents.len());
}

#[test]
fn a_merge_steps_over_branches_without_allocating() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(&dir);
    let older = build(&mut s, 0, 2);
    let newer = build(&mut s, 1, 3);
    let mut m = Merge::new(&mut s, [&newer, &older], b"", None).unwrap();
    // Past several leaves of both branches: the store's page pool has grown to the most its
    // cursors hold at once.
    for _ in 0..1_000 {
        m.next(&mut s).unwrap();
    }
    alloc::begin();
    let mut steps = 0u64;
    while m.entry().is_some() {
        m.next(&mut s).unwrap();
        steps += 1;
    }
    let counts = alloc::end();
    assert!(steps > ENTRIES, "{steps}");
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "{counts:?}"
    );
    m.give_back(&mut s);
}

#[test]
fn a_point_read_allocates_nothing_once_its_value_buffer_has_grown() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(&dir);
    let branch = build(&mut s, 0, 1);
    let mut value = Vec::new();
    assert_eq!(
        branch.get(&mut s, &key(1), &mut value).unwrap(),
        Some(Op::Put)
    );
    alloc::begin();
    for n in 0..ENTRIES {
        branch.get(&mut s, &key(n), &mut value).unwrap();
    }
    let counts = alloc::end();
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "{counts:?}"
    );
}
