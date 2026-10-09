//! Written-page cache validity when a queue payload is refused or a write/flush fails.
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
use hyper_block::sim::{Fault, SimFile};
use mantle_engine::error::Error;
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 64,
};

fn written() -> (Store<SimFile>, u64) {
    let file = SimFile::new(
        Alignment::new(4096).unwrap(),
        Alignment::new(512).unwrap(),
        11,
    )
    .unwrap();
    let mut store = Store::create(file, CONFIG).unwrap();
    store.set_cache(CONFIG.extent_pages as usize);
    let extent = store.allocate_extent().unwrap();
    let address = store.address(extent, 0).unwrap();
    store
        .write_page(address, b"last successful contents")
        .unwrap();
    store.checkpoint(Some(address), 1).unwrap();
    (store, address)
}

#[test]
fn a_rejected_queue_payload_preserves_the_last_successful_page() {
    let (mut store, address) = written();
    let refused = vec![b'x'; store.page_capacity() + 1];
    let mut run = store.run().unwrap();
    assert!(matches!(
        store.queue_page(&mut run, address, &refused),
        Err(Error::InvalidArgument { .. })
    ));
    let mut out = Vec::new();
    store.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"last successful contents");
    let value = store.with_page(address, |p| Ok(p.to_vec())).unwrap();
    assert_eq!(value, b"last successful contents");
    let mut span = store.span_sequential().unwrap();
    assert!(store.ready(&mut span, address).unwrap());
    out.clear();
    store.read_page_ahead(&mut span, address, &mut out).unwrap();
    assert_eq!(out, b"last successful contents");
    store.give_span(span);
    store.give_run(run);
}

/// A fault on the medium remains observable: a failed write/flush cannot hide it in the cache.
fn failed_write_does_not_serve_cached_bytes(fault: Fault) {
    let (store, address) = written();
    let file = store.into_file().0;
    let write = fault == Fault::WriteError;
    file.inject(fault).unwrap();
    file.inject(Fault::ReadError {
        offset: address * CONFIG.page_size as u64,
        len: CONFIG.page_size as u64,
    })
    .unwrap();
    let (mut store, _) = Store::open(file, CONFIG).unwrap();
    store.set_cache(CONFIG.extent_pages as usize);
    let mut run = store.run().unwrap();
    if write {
        store
            .queue_page(&mut run, address, b"failed write bytes")
            .unwrap();
        assert!(matches!(store.write_run(&mut run), Err(Error::Io { .. })));
    } else {
        store
            .queue_page(&mut run, address, b"unflushed bytes")
            .unwrap();
        store.write_run(&mut run).unwrap();
        assert!(matches!(
            store.checkpoint(Some(address), 2),
            Err(Error::Io { .. })
        ));
    }
    assert!(matches!(
        store.read_page(address, &mut Vec::new()),
        Err(Error::Io { .. })
    ));
    assert!(matches!(
        store.with_page(address, |_| Ok(())),
        Err(Error::Io { .. })
    ));
    let mut span = store.span_sequential().unwrap();
    assert!(store.ready(&mut span, address).unwrap());
    assert!(matches!(
        store.read_page_ahead(&mut span, address, &mut Vec::new()),
        Err(Error::Io { .. })
    ));
    store.give_span(span);
    store.give_run(run);
}

#[test]
fn a_failed_write_does_not_serve_cached_bytes() {
    failed_write_does_not_serve_cached_bytes(Fault::WriteError);
}

#[test]
fn a_failed_flush_does_not_serve_cached_bytes() {
    failed_write_does_not_serve_cached_bytes(Fault::SyncError);
}
