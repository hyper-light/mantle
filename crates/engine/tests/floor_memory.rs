//! What an engine at a range's floor holds in memory that its configuration bounds, counted
//! exactly by hyper-measure's counting allocator (`shard_db::bounded_bytes`).
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

#[test]
fn an_engine_made_at_the_floor_holds_its_bounded_bytes() {
    let page = mantle_engine::branch::least_page(4096, KEY, VALUE).unwrap();
    let memtable = mantle_engine::memtable::hashed::entry_bytes(KEY, VALUE).unwrap();
    let config = Config {
        page_size: page,
        extent_pages: 2,
        max_extents: 1 << 16,
    };
    let trunk = TrunkConfig {
        fanout: 8,
        leaf_entries: 2_084,
    };
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    alloc::begin();
    let db = ShardDb::create(file, config, memtable, trunk).unwrap();
    let made = alloc::end();
    let most = shard_db::bounded_bytes(&config, memtable).unwrap();
    let peak = u64::try_from(made.peak).unwrap();
    eprintln!("made at {peak} bytes of {most}");
    assert!(peak <= most, "an engine made at {peak} bytes, past {most}");
    drop(db);
}
