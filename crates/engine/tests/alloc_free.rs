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
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::{Consolidation, TrunkConfig, ViewChoice};

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
    // A first branch of the same shape fills the store's pools: extent buffers from its written
    // runs, and the builder's working lists grown to a branch this size.
    let value = [7u8; 40];
    let mut warm = Builder::new(&mut s, Keys::Exactly(ENTRIES)).unwrap();
    for n in 0..ENTRIES {
        warm.add(&mut s, &key(n), Op::Put, &value).unwrap();
    }
    let warm = warm.finish(&mut s).unwrap();
    // Its extents freed by a checkpoint, as a compaction's inputs are: the next branch reuses
    // them, so the store's own tables do not grow (a store that grows its file grows them).
    for &e in &warm.extents {
        s.release(e).unwrap();
    }
    s.checkpoint(None, 1).unwrap();
    let mut b = Builder::new(&mut s, Keys::Exactly(ENTRIES)).unwrap();
    for n in 0..ENTRIES / 2 {
        b.add(&mut s, &key(n), Op::Put, &value).unwrap();
    }
    alloc::begin();
    for n in ENTRIES / 2..ENTRIES {
        b.add(&mut s, &key(n), Op::Put, &value).unwrap();
    }
    let counts = alloc::end();
    // No allocation and no reallocation: the builder's working lists (its extents, its pages'
    // entry counts) come from the store's pool, grown by the first branch, and the branch is
    // given exactly sized copies when it is sealed.
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "{counts:?}"
    );
    let branch = b.finish(&mut s).unwrap();
    assert!(branch.extents.len() > 4, "{}", branch.extents.len());
    assert!(branch.counts.len() > 32, "{}", branch.counts.len());
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
        m.next(&mut s, [&newer, &older]).unwrap();
    }
    alloc::begin();
    let mut steps = 0u64;
    while m.entry().is_some() {
        m.next(&mut s, [&newer, &older]).unwrap();
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

#[test]
fn a_shard_scans_without_allocating_once_its_buffers_have_grown() {
    let dir = tempfile::tempdir().unwrap();
    let align = Alignment::new(4096).unwrap();
    let file = DeviceFile::open(
        &dir.path().join("shard"),
        true,
        CachingRequest::Buffered,
        align,
    )
    .unwrap();
    let mut db = ShardDb::create(
        file,
        CONFIG,
        4 * 1024,
        TrunkConfig {
            fanout: 3,
            leaf_entries: 96,
        },
    )
    .unwrap();
    // Views built whenever they can be, not as measured costs choose: the test does not depend
    // on timing.
    db.set_view_choice(ViewChoice::Rebuild);
    // Its bundles kept for their views: seeks consolidate none (`Consolidation`).
    db.set_consolidation(Consolidation::Never);
    // Random keys over many memtables: the trunk has pivot bundles and in-flight branches, and
    // a memtable is packing, so a scan merges both memtables and several trunk sources.
    let mut x = 0x2545_f491_4f6c_dd1du64;
    let mut next_key = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % 20_000
    };
    for _ in 0..12_000 {
        let n = next_key();
        db.put(&key(n), &n.to_le_bytes()).unwrap();
    }
    let mut rows = Rows::new();
    let mut next = Vec::new();
    // The same scans twice, open and bounded: the first grows every buffer and pool the second
    // needs, so the second allocates nothing.
    let scans = |db: &mut ShardDb<DeviceFile>, rows: &mut Rows, next: &mut Vec<u8>| {
        for i in 0..2_000u64 {
            let from = key((i * 7_919) % 20_000);
            let end = key((i * 7_919) % 20_000 + 40);
            rows.clear();
            let bound = if i % 2 == 0 { None } else { Some(&end[..]) };
            db.scan(&from, bound, 10, rows, next).unwrap();
        }
    };
    scans(&mut db, &mut rows, &mut next);
    alloc::begin();
    scans(&mut db, &mut rows, &mut next);
    let counts = alloc::end();
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "{counts:?}"
    );
    // The debts paid in idle slices, which build the bundles' REMIX views: scans then walk views,
    // and allocate nothing either once their walks' buffers have grown.
    while db.owed() {
        db.idle_step(64).unwrap();
    }
    let (_, t, _) = db.stats();
    assert!(
        db.views() > 0,
        "no view built: {t:?} shape {:?}",
        db.shape()
    );
    scans(&mut db, &mut rows, &mut next);
    alloc::begin();
    scans(&mut db, &mut rows, &mut next);
    let counts = alloc::end();
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "through views: {counts:?}"
    );
}

#[test]
fn a_memtable_cycle_allocates_nothing_once_its_buffers_have_grown() {
    use mantle_engine::memtable::hashed::{HashMem, Walk};
    // A memtable's whole cycle: filled with seals between (sorts and merges), then scanned at
    // random keys with writes between, so each walk's merging becomes a merged range, writes
    // drop ranges and later walks make new ones from the freed nodes; then cleared for the next.
    // Two cycles grow every buffer, the pool of them and the slab to their sizes; a third
    // allocates and reallocates nothing.
    let cycle = |m: &mut HashMem, walk: &mut Walk| {
        m.clear();
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for i in 0..20_000u64 {
            m.insert(&key(next() % 5_000), Op::Put, &i.to_le_bytes())
                .unwrap();
            if i % 1_000 == 999 {
                m.seal();
            }
        }
        m.seal();
        let (mut walked, mut merged_only) = (0usize, 0usize);
        for i in 0..4_000u64 {
            m.walk_from_into(&key(next() % 5_000), walk).unwrap();
            walked += m.walk_some(walk, 10, |_, _, _| Ok(())).unwrap();
            if walk.run_work() == (0, 0) {
                merged_only += 1;
            }
            m.adopt(walk);
            if i % 3 == 0 {
                m.insert(&key(next() % 5_000), Op::Put, b"w").unwrap();
            }
        }
        (walked, merged_only)
    };
    let mut m = HashMem::new(1 << 22).unwrap();
    let mut walk = Walk::default();
    let warm = cycle(&mut m, &mut walk);
    cycle(&mut m, &mut walk);
    alloc::begin();
    let measured = cycle(&mut m, &mut walk);
    let counts = alloc::end();
    // The same cycle each time, and some of its walks ran in merged ranges alone.
    assert_eq!(warm, measured);
    assert!(measured.1 > 0, "{measured:?}");
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "{counts:?}"
    );
}
