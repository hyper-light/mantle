//! What an engine at a range's floor holds, counted by hyper-measure's counting allocator against
//! what `mantle_engine` states: its configured bytes (`shard_db::bounded_bytes`), its store's pools
//! at the most of each kind it had out at once (`shard_db::pooled_bytes`, the peaks measured) and
//! its branches' metadata as held (`ShardDb::memory`). Filled with paced puts and a scan of ten
//! rows every 97, at the range's densest rows and its longest keys, with and without its largest
//! values; the peaks and the metadata are printed, the measured concurrency node.md §1.4 names.
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
use mantle_engine::shard_db::{self, ShardDb};
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The range's bounds on a row's key and value (mantle_meta's `key::largest_bytes` and
/// `wire::largest_row_bytes`), which size the floor's page and memtable.
const KEY: usize = 2_132;
const VALUE: usize = 18_699;

/// Fills an engine at the floor with `rows` rows of `key_bytes`-byte keys and `value_bytes`-byte
/// values, settles it, and checks what it holds against what it states.
fn fill(key_bytes: usize, value_bytes: usize, rows: u64) {
    let page = mantle_engine::branch::least_page(4096, KEY, VALUE).unwrap();
    let memtable = mantle_engine::memtable::hashed::entry_bytes(KEY, VALUE).unwrap();
    let least = mantle_engine::memtable::hashed::entry_bytes(2, 0).unwrap();
    let config = Config {
        page_size: page,
        extent_pages: 2,
        max_extents: 1 << 20,
    };
    let trunk = TrunkConfig {
        fanout: 8,
        leaf_entries: (memtable / least) as u64,
    };
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    let value = vec![7u8; value_bytes];
    let mut key = vec![0u8; key_bytes];
    alloc::begin();
    let mut db = ShardDb::create(file, config, memtable, trunk).unwrap();
    let mut out = mantle_engine::rows::Rows::new();
    let mut next = Vec::new();
    let mut from = vec![0u8; key_bytes];
    let at = key_bytes.saturating_sub(8);
    for n in 0..rows {
        let bytes = n.to_be_bytes();
        key[at..].copy_from_slice(&bytes[8 - (key_bytes - at)..]);
        // Each put pays its shares of the work owed, as a range's puts do.
        db.put(&key, &value).unwrap();
        // Seeks of ten rows as the fill goes, every source on their paths opened.
        if n % 97 == 0 {
            let bytes = (n / 2).to_be_bytes();
            from[at..].copy_from_slice(&bytes[8 - (key_bytes - at)..]);
            out.clear();
            db.scan(&from, None, 10, &mut out, &mut next).unwrap();
        }
    }
    db.flush().unwrap();
    db.maintain(u64::MAX).unwrap();
    let held = u64::try_from(alloc::read().live).unwrap();
    let peaks = db.pool_peaks();
    let memory = db.memory();
    // A node takes a flush while it holds fewer than `fanout` live in-flight bundles, and a flush
    // lands at most `fanout + 1`; the last before a split does not wait, and the split takes what
    // it lands: so at most `3 fanout + 1` (trunk/upkeep.rs `takes_flush`).
    let inflight_most = db.stats().1.inflight_most;
    assert!(
        inflight_most <= 3 * trunk.fanout as u64 + 1,
        "{key_bytes}-byte keys: a node held {inflight_most} live in-flight bundles"
    );
    let metadata = (memory.filters
        + memory.indexes
        + memory.counts
        + memory.ranges
        + memory.pivots
        + memory.branches) as u64;
    let bounded = shard_db::bounded_bytes(&config, memtable).unwrap();
    // Builders' lists stated for branches of a leaf's entries, what a packing and a leaf's settle
    // write; a pivot's flush writes what its in-flight bundles hold, which the trunk does not
    // bound yet (node.md §1.4).
    let pooled =
        shard_db::pooled_bytes(&config, peaks, key_bytes, trunk.leaf_entries, 1, 8).unwrap();
    let stated = bounded + pooled + metadata;
    eprintln!(
        "keys {key_bytes} values {value_bytes} rows {rows}: held {held} of {stated} stated (configured {bounded}, pooled {pooled} at {peaks:?}, metadata {metadata} {memory:?}, inflight_most {inflight_most})"
    );
    assert!(
        held <= stated,
        "{key_bytes}-byte keys: held {held} past {stated}"
    );
    alloc::end();
    drop(db);
}

#[test]
fn an_engine_holds_no_more_than_it_states() {
    for rows in [40_000u64, 320_000, 1_280_000] {
        fill(8, 0, rows);
    }
    for rows in [2_000u64, 32_000] {
        fill(KEY, 0, rows);
    }
    fill(KEY, VALUE, 3_200);
}
