//! A range at its memory share (docs/research/69): an engine whose branches' resident metadata is
//! at or past its limit refuses a put that would pack its memtable, typed, and keeps the memtable;
//! takes deletes, which pack it as before; and takes puts again once its metadata is under the
//! limit. At the range's floor (its page sized by the range's longest key and largest row), filled
//! with 8-byte keys without values, the densest rows.
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
use mantle_engine::error::Error;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

/// The range's bounds on a row's key and value (mantle_meta's `key::largest_bytes` and
/// `wire::largest_row_bytes`), which size the floor's page and memtable.
const KEY: usize = 2_132;
const VALUE: usize = 18_699;

fn key(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

#[test]
fn a_range_at_its_metadata_share_refuses_puts_takes_deletes_and_takes_puts_again() {
    let page = mantle_engine::branch::least_page(4096, KEY, VALUE).unwrap();
    let memtable = mantle_engine::memtable::hashed::entry_bytes(KEY, VALUE).unwrap();
    let least = mantle_engine::memtable::hashed::entry_bytes(8, 0).unwrap();
    // A memtable holds at most this many of the rows put here: a put past it packs it.
    let memtable_rows = (memtable / least) as u64;
    let config = Config {
        page_size: page,
        extent_pages: 2,
        max_extents: 1 << 20,
    };
    let trunk = TrunkConfig {
        fanout: 8,
        leaf_entries: memtable_rows,
    };
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    let mut db = ShardDb::create(file, config, memtable, trunk).unwrap();
    let filled = 40 * memtable_rows;
    for n in 0..filled {
        db.put(&key(n), &[]).unwrap();
    }
    db.maintain(u64::MAX).unwrap();
    let held = db.memory().total() as u64;
    assert!(held > 0);

    // A limit at what the branches hold: a put that would pack the memtable is refused, at most a
    // memtable's rows after.
    db.set_metadata_limit(Some(held));
    let mut n = filled;
    let refused = loop {
        match db.put(&key(n), &[]) {
            Ok(()) => n += 1,
            Err(error) => break error,
        }
        assert!(
            n <= filled + memtable_rows,
            "{} puts past the limit, a memtable holding {memtable_rows}",
            n - filled
        );
    };
    assert!(
        matches!(
            refused,
            Error::LimitExceeded {
                what: "a range's resident metadata",
                limit
            } if limit == held
        ),
        "{refused:?}"
    );
    // The memtable is kept: every put taken is found, the refused one is not.
    let mut value = Vec::new();
    for m in [filled, n - 1] {
        assert!(db.get(&key(m), &mut value).unwrap(), "row {m}");
    }
    assert!(!db.get(&key(n), &mut value).unwrap());
    // Refused again while its branches have not changed.
    assert!(matches!(
        db.put(&key(n), &[]),
        Err(Error::LimitExceeded { .. })
    ));

    // Deletes are taken, packing the memtable as before.
    for m in 0..n {
        db.delete(&key(m)).unwrap();
    }
    db.maintain(u64::MAX).unwrap();
    for m in [0, filled, n - 1] {
        assert!(!db.get(&key(m), &mut value).unwrap(), "row {m}");
    }

    // Under a limit past what it holds, puts are taken again, packing memtables: past a
    // memtable's rows, so the limit is looked at again where one fills.
    let now = db.memory().total() as u64;
    eprintln!(
        "metadata {held} at the limit, refused after {} puts, {now} after deleting every row",
        n - filled
    );
    db.set_metadata_limit(Some(now + held));
    for m in n..n + 2 * memtable_rows {
        db.put(&key(m), &[]).unwrap();
    }
    db.maintain(u64::MAX).unwrap();
    for m in [n, n + 2 * memtable_rows - 1] {
        assert!(db.get(&key(m), &mut value).unwrap(), "row {m}");
    }
}
