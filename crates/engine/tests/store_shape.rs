//! A store's shape as its superblocks name it (`Store::shape`): what a node derives its engine's
//! configuration from once the store exists, since a store opens only at the page and extent
//! sizes it was created with and admitting at least the extents it holds.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::io::Write;
use std::path::Path;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::error::Error;
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 8192,
    extent_pages: 3,
    max_extents: 64,
};

fn file(path: &Path, fresh: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        fresh,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap()
}

#[test]
fn a_stores_shape_is_what_its_superblocks_name_and_it_opens_only_at_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(file(&path, true), CONFIG).unwrap();
    for _ in 0..5 {
        store.allocate_extent().unwrap();
    }
    store.checkpoint(None, 7).unwrap();
    drop(store);
    let shape = Store::shape(&file(&path, false), CONFIG.page_size).unwrap();
    assert_eq!((shape.page_size, shape.extent_pages), (8192, 3));
    assert!(shape.extents >= 6, "{shape:?}");

    // Read at the page its first copy names, whatever page its reader expects.
    for expected in [4096, 12_288, 65_536] {
        assert_eq!(Store::shape(&file(&path, false), expected).unwrap(), shape);
    }

    // It opens admitting its extents, and not below them.
    let below = Config {
        max_extents: shape.extents - 1,
        ..CONFIG
    };
    assert!(Store::open(file(&path, false), below).is_err());
    let at = Config {
        max_extents: shape.extents,
        ..CONFIG
    };
    Store::open(file(&path, false), at).unwrap();

    // Its first copy torn: the other, read at the page its reader expects.
    let mut torn = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    torn.write_all(&[0xa5; 4096]).unwrap();
    torn.sync_all().unwrap();
    drop(torn);
    let other = Store::shape(&file(&path, false), CONFIG.page_size).unwrap();
    assert_eq!((other.page_size, other.extent_pages), (8192, 3));
    assert!(other.extents >= 1 && other.extents <= shape.extents);
    // At another page, neither copy reads.
    assert!(matches!(
        Store::shape(&file(&path, false), 4096),
        Err(Error::Corruption { .. })
    ));
}
