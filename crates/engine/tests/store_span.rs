//! The store's spans (`Store::read_page_ahead`) on hyper-block's simulated device: a compaction's
//! span reads an extent a call, a scan's reads a page and then doubles while it runs on, each
//! page exactly as `read_page` gives it; a span stops at the file's end and finds a page
//! corrupted on the medium.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Fault, SimFile};
use mantle_engine::error::Error;
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 64,
};

fn sim() -> SimFile {
    let align = Alignment::new(4096).unwrap();
    SimFile::new(align, Alignment::new(512).unwrap(), 11).unwrap()
}

fn payload(address: u64) -> Vec<u8> {
    let mut v = format!("page {address} ").into_bytes();
    v.resize(64 + (address as usize * 29) % 700, address as u8);
    v
}

/// A store with two extents written whole and checkpointed: their page addresses in order.
fn written() -> (Store<SimFile>, Vec<u64>) {
    let mut store = Store::create(sim(), CONFIG).unwrap();
    let mut addresses = Vec::new();
    let mut run = store.run().unwrap();
    for _ in 0..2 {
        let extent = store.allocate_extent().unwrap();
        for n in 0..CONFIG.extent_pages {
            let a = store.address(extent, n).unwrap();
            store.queue_page(&mut run, a, &payload(a)).unwrap();
            addresses.push(a);
        }
    }
    store.write_run(&mut run).unwrap();
    store.checkpoint(None, 1).unwrap();
    (store, addresses)
}

#[test]
fn a_compaction_reads_an_extent_a_call_and_every_page_as_read_page_gives_it() {
    let (mut store, addresses) = written();
    let mut span = store.span_sequential().unwrap();
    let before = store.io_stats();
    for &a in &addresses {
        let mut out = Vec::new();
        store.read_page_ahead(&mut span, a, &mut out).unwrap();
        assert_eq!(out, payload(a));
    }
    let after = store.io_stats();
    assert_eq!(after.reads - before.reads, 2);
    assert_eq!(after.pages_read - before.pages_read, 8);

    // Back into the first extent from its middle: one call, its last two pages.
    let mut out = Vec::new();
    store
        .read_page_ahead(&mut span, addresses[2], &mut out)
        .unwrap();
    store
        .read_page_ahead(&mut span, addresses[3], &mut out)
        .unwrap();
    assert_eq!(out, [payload(addresses[2]), payload(addresses[3])].concat());
    let last = store.io_stats();
    assert_eq!(last.reads - after.reads, 1);
    assert_eq!(last.pages_read - after.pages_read, 2);
}

#[test]
fn a_scan_reads_one_page_then_doubles_while_it_runs_on() {
    let (mut store, addresses) = written();
    let mut span = store.span().unwrap();
    // (pages the read takes, or 0 when the span holds the page), page by page over two extents of
    // four: 1, then 2, then the extent's last 1 (the extent bounds it), then the next extent's 4.
    let want = [1, 2, 0, 1, 4, 0, 0, 0];
    for (&a, &pages) in addresses.iter().zip(&want) {
        let before = store.io_stats();
        let mut out = Vec::new();
        store.read_page_ahead(&mut span, a, &mut out).unwrap();
        assert_eq!(out, payload(a));
        let after = store.io_stats();
        assert_eq!(after.pages_read - before.pages_read, pages, "page {a}");
        assert_eq!(after.reads - before.reads, u64::from(pages > 0), "page {a}");
    }
    // A seek back starts again at a page.
    let before = store.io_stats();
    store
        .read_page_ahead(&mut span, addresses[1], &mut Vec::new())
        .unwrap();
    assert_eq!(store.io_stats().pages_read - before.pages_read, 1);
}

#[test]
fn a_span_stops_at_the_files_end() {
    let (mut store, addresses) = written();
    // A new extent past every page the file holds (the allocator reuses freed extents first, so
    // it takes extents until one lies past every extent written), two of its four pages written.
    let highest = addresses
        .iter()
        .map(|&a| a / u64::from(CONFIG.extent_pages))
        .chain(store.map_extents().iter().copied())
        .max()
        .unwrap();
    let extent = loop {
        let e = store.allocate_extent().unwrap();
        if e > highest {
            break e;
        }
    };
    let pages: Vec<u64> = (0..2).map(|n| store.address(extent, n).unwrap()).collect();
    let mut run = store.run().unwrap();
    for &a in &pages {
        store.queue_page(&mut run, a, &payload(a)).unwrap();
    }
    store.write_run(&mut run).unwrap();
    let mut span = store.span_sequential().unwrap();
    let before = store.io_stats();
    let mut out = Vec::new();
    for &a in &pages {
        store.read_page_ahead(&mut span, a, &mut out).unwrap();
    }
    assert_eq!(out, [payload(pages[0]), payload(pages[1])].concat());
    let after = store.io_stats();
    assert_eq!(after.reads - before.reads, 1);
    assert_eq!(after.pages_read - before.pages_read, 2);
    // The page past the file's end is refused, typed.
    let past = store.address(extent, 2).unwrap();
    let err = store
        .read_page_ahead(&mut span, past, &mut Vec::new())
        .unwrap_err();
    assert!(matches!(err, Error::Corruption { .. }), "{err}");
}

#[test]
fn a_page_flipped_on_the_medium_fails_alone_through_a_span() {
    let (store, addresses) = written();
    let file = finished_file(store.into_file()).0;
    let offset = addresses[1] * CONFIG.page_size as u64 + 100;
    file.inject(Fault::BitFlip {
        offset,
        bit: 5,
        stored: true,
    })
    .unwrap();
    let (mut store, _) = Store::open(file, CONFIG).unwrap();
    let mut span = store.span().unwrap();
    let mut out = Vec::new();
    store
        .read_page_ahead(&mut span, addresses[0], &mut out)
        .unwrap();
    let err = store
        .read_page_ahead(&mut span, addresses[1], &mut out)
        .unwrap_err();
    assert!(matches!(err, Error::Corruption { .. }), "{err}");
    store
        .read_page_ahead(&mut span, addresses[2], &mut out)
        .unwrap();
    assert_eq!(out, [payload(addresses[0]), payload(addresses[2])].concat());
}
