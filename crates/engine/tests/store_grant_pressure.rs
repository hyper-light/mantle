//! Whole-grant refusals preserve allocator and cached data; attachment replacement waits
//! until every existing demand-read owner has returned.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[path = "support/shared_sim.rs"]
mod shared_sim;

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use hyper_block::buf::Alignment;
use hyper_block::issuer::Issuer;
use hyper_block::sim::{Crash, Fault};
use mantle_engine::Error;
use mantle_engine::store::{Config, Store};
use shared_sim::SharedSim;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 16,
};

fn store(limit: u64) -> (Store<SharedSim>, SharedSim) {
    let file = SharedSim::new(
        Alignment::new(CONFIG.page_size).unwrap(),
        Alignment::new(512).unwrap(),
        301,
    )
    .unwrap();
    let observed = file.clone();
    let store = Store::create(
        file,
        Config {
            max_extents: limit,
            ..CONFIG
        },
    )
    .unwrap();
    (store, observed)
}

#[test]
fn an_insufficient_grant_leaves_the_remaining_extent_available() {
    // Extent zero and the first durable map use two of the three allowed extents.
    let (mut store, _) = store(3);
    let refs = store.refs().to_vec();
    let generation = store.generation();
    assert!(matches!(store.grant(2), Err(Error::LimitExceeded { .. })));
    assert_eq!(store.refs(), refs);
    assert_eq!(store.generation(), generation);
    let extent = store.allocate_extent().unwrap();
    let address = store.address(extent, 0).unwrap();
    store.write_page(address, b"the remaining extent").unwrap();
    let mut out = Vec::new();
    store.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"the remaining extent");
}

#[test]
fn pending_extents_remain_protected_and_durable_free_extents_are_granted_atomically() {
    let (mut store, observed) = store(6);
    let extent = store.allocate_extent().unwrap();
    let address = store.address(extent, 0).unwrap();
    store.write_page(address, b"old durable root").unwrap();
    store.checkpoint(Some(address), 1).unwrap();
    store.release(extent).unwrap();
    let refs = store.refs().to_vec();
    assert!(matches!(store.grant(4), Err(Error::LimitExceeded { .. })));
    assert_eq!(store.refs(), refs);
    let other = store.allocate_extent().unwrap();
    assert_ne!(other, extent, "the old durable root cannot be overwritten");
    store.grant_back(&[other]).unwrap();
    store.checkpoint(None, 2).unwrap();
    let refs = store.refs().to_vec();
    assert!(matches!(store.grant(5), Err(Error::LimitExceeded { .. })));
    assert_eq!(store.refs(), refs);
    let grant = store.grant(2).unwrap();
    assert!(grant.contains(&extent));
    for &unused in &grant {
        if unused != extent {
            store.grant_back(&[unused]).unwrap();
        }
    }
    store.write_page(address, b"new durable root").unwrap();
    store.checkpoint(Some(address), 3).unwrap();
    let config = store.config();
    let expected_refs = store.refs().to_vec();
    let (file, landed) = finished_file(store.into_file());
    landed.unwrap();
    observed.crash(Crash::LoseAll).unwrap();
    let (mut reopened, recovered) = Store::open(file, config).unwrap();
    assert_eq!(recovered.applied, 3);
    assert_eq!(recovered.root, Some(address));
    assert_eq!(reopened.refs(), expected_refs);
    let mut out = Vec::new();
    reopened.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"new durable root");
}

#[test]
fn a_refused_grant_preserves_cached_pages_but_an_accepted_grant_invalidates_them() {
    let (mut store, observed) = store(6);
    store.set_cache(usize::try_from(CONFIG.extent_pages).unwrap());
    let extent = store.allocate_extent().unwrap();
    let address = store.address(extent, 0).unwrap();
    store.write_page(address, b"cached old bytes").unwrap();
    store.checkpoint(Some(address), 1).unwrap();
    store.release(extent).unwrap();
    store.checkpoint(None, 2).unwrap();
    observed
        .inject(Fault::ReadError {
            offset: address * u64::try_from(CONFIG.page_size).unwrap(),
            len: u64::try_from(CONFIG.page_size).unwrap(),
        })
        .unwrap();
    let refs = store.refs().to_vec();
    assert!(matches!(store.grant(5), Err(Error::LimitExceeded { .. })));
    assert_eq!(store.refs(), refs);
    let mut out = Vec::new();
    store.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"cached old bytes");
    let grant = store.grant(2).unwrap();
    assert!(grant.contains(&extent));
    out.clear();
    assert!(matches!(
        store.read_page(address, &mut out),
        Err(Error::Io { .. })
    ));
    observed.clear_faults().unwrap();
    store.write_page(address, b"new granted bytes").unwrap();
    store.read_page(address, &mut out).unwrap();
    assert_eq!(out, b"new granted bytes");
}

#[test]
fn an_invalid_return_cannot_release_an_earlier_valid_grant_member() {
    let (mut store, _) = store(CONFIG.max_extents);
    let grant = store.grant(2).unwrap();
    store.retain(grant[1]).unwrap();
    let refs = store.refs().to_vec();
    for invalid in [
        vec![grant[0], grant[1]],
        vec![grant[0], grant[0]],
        vec![grant[0], 0],
        vec![grant[0], u64::MAX],
    ] {
        assert!(matches!(
            store.grant_back(&invalid),
            Err(Error::InvalidArgument { .. })
        ));
        assert_eq!(store.refs(), refs);
    }
    store.release(grant[1]).unwrap();
    store.grant_back(&grant).unwrap();
    let mut reused = store.grant(2).unwrap();
    let mut expected = grant;
    reused.sort_unstable();
    expected.sort_unstable();
    assert_eq!(reused, expected);
}

#[test]
fn attachment_replacement_refuses_until_the_existing_span_and_read_are_returned() {
    let dir = tempfile::tempdir().unwrap();
    let (mut store, _) = store(CONFIG.max_extents);
    let extent = store.allocate_extent().unwrap();
    let address = store.address(extent, 0).unwrap();
    store.write_page(address, b"same attachment page").unwrap();
    // One owned demand plus the dispatcher; no held second device callback is needed.
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let mut span = store.span_sequential().unwrap();
    store.prefetch(&mut span, address).unwrap();
    let refs = store.refs().to_vec();
    let generation = store.generation();
    let end = store.end();
    assert!(matches!(
        store.attach(&issuer, 1),
        Err(Error::InvalidArgument { .. })
    ));
    assert_eq!(store.refs(), refs);
    assert_eq!(store.generation(), generation);
    assert_eq!(store.end(), end);
    let mut out = Vec::new();
    store.read_page_ahead(&mut span, address, &mut out).unwrap();
    assert_eq!(out, b"same attachment page");
    // A landed span still owns a page from the old attachment's identity.
    assert!(matches!(
        store.attach(&issuer, 1),
        Err(Error::InvalidArgument { .. })
    ));
    store.give_span(span);
    store.drain().unwrap();
    store.attach(&issuer, 1).unwrap();
    let mut next = store.span_sequential().unwrap();
    store.prefetch(&mut next, address).unwrap();
    out.clear();
    store.read_page_ahead(&mut next, address, &mut out).unwrap();
    assert_eq!(out, b"same attachment page");
    store.give_span(next);
    let (_, landed) = finished_file(store.into_file());
    landed.unwrap();
}
