//! A scan's reads and the page cache (docs/research/2026-10-07-engine-p999-review.md §3): the
//! page a seek lands on is demand, admitted and then served from memory; the pages a scan runs on
//! through, and a compaction's, are only looked at, so a long scan does not fill the cache.
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
use hyper_block::sim::SimFile;
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 32,
    max_extents: 4096,
};

fn payload(address: u64) -> Vec<u8> {
    (0..100u64).map(|i| (address * 7 + i) as u8).collect()
}

/// A store holding two extents of written pages and an empty cache of `cache` pages.
fn written(cache: usize) -> (Store<SimFile>, Vec<u64>) {
    let align = Alignment::new(4096).unwrap();
    let mut store = Store::create(
        SimFile::new(align, Alignment::new(512).unwrap(), 5).unwrap(),
        CONFIG,
    )
    .unwrap();
    let mut run = store.run().unwrap();
    let mut addresses = Vec::new();
    for _ in 0..2 {
        let extent = store.allocate_extent().unwrap();
        for n in 0..CONFIG.extent_pages {
            let a = store.address(extent, n).unwrap();
            store.queue_page(&mut run, a, &payload(a)).unwrap();
            addresses.push(a);
        }
    }
    store.write_run(&mut run).unwrap();
    store.drain().unwrap();
    // Every write settled (a compaction's read of them all), then the cache emptied: the cache
    // takes pages as their writes land.
    let mut span = store.span_sequential().unwrap();
    for &a in &addresses {
        store
            .read_page_ahead(&mut span, a, &mut Vec::new())
            .unwrap();
    }
    store.give_span(span);
    store.set_cache(cache);
    (store, addresses)
}

#[test]
fn a_seeks_page_is_admitted_and_read_from_memory_the_next_time() {
    let (mut store, addresses) = written(16);
    let target = addresses[40];
    let mut out = Vec::new();
    let mut span = store.span().unwrap();
    store.read_page_ahead(&mut span, target, &mut out).unwrap();
    store.give_span(span);
    assert_eq!(out, payload(target));
    let before = store.io_stats();
    // A second seek to it, from a new scan: from the cache, no read.
    out.clear();
    let mut span = store.span().unwrap();
    store.read_page_ahead(&mut span, target, &mut out).unwrap();
    store.give_span(span);
    assert_eq!(out, payload(target));
    let after = store.io_stats();
    assert_eq!(after.reads, before.reads, "{after:?}");
    assert_eq!(after.span_cache_hits, before.span_cache_hits + 1);
}

/// How many of `addresses` a point read finds in the cache: those read without a read call.
fn cached(store: &mut Store<SimFile>, addresses: &[u64]) -> usize {
    let mut found = 0;
    for &a in addresses {
        let before = store.io_stats().reads;
        let mut out = Vec::new();
        store.read_page(a, &mut out).unwrap();
        assert_eq!(out, payload(a));
        if store.io_stats().reads == before {
            found += 1;
        }
    }
    found
}

#[test]
fn a_long_scan_admits_only_the_page_it_started_at() {
    let (mut store, addresses) = written(64);
    let mut span = store.span().unwrap();
    for &a in &addresses {
        let mut out = Vec::new();
        store.read_page_ahead(&mut span, a, &mut out).unwrap();
        assert_eq!(out, payload(a));
    }
    store.give_span(span);
    // Run on through every page from the first: the first alone was admitted.
    assert_eq!(cached(&mut store, &addresses), 1);
    // A compaction's reads admit nothing.
    let (mut store, addresses) = written(64);
    let mut span = store.span_sequential().unwrap();
    for &a in &addresses {
        store
            .read_page_ahead(&mut span, a, &mut Vec::new())
            .unwrap();
    }
    store.give_span(span);
    assert_eq!(cached(&mut store, &addresses), 0);
}
