//! A refused branch size leaves the store usable for the next writer.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::error::Error;
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 4096,
};
const ENTRIES: u64 = 512;

fn key(n: u64) -> Vec<u8> {
    let mut key = b"a/shared/prefix/".to_vec();
    key.extend_from_slice(&n.to_be_bytes());
    key
}

fn build(store: &mut Store<DeviceFile>) -> Branch {
    let mut builder = Builder::new(store, Keys::Exactly(ENTRIES)).unwrap();
    for n in 0..ENTRIES {
        builder
            .add(store, &key(n), Op::Put, &n.to_le_bytes())
            .unwrap();
    }
    builder.finish(store).unwrap()
}

fn refused_then_reused(hint: Keys) {
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    let mut store = Store::create(file, CONFIG).unwrap();
    let warm = build(&mut store);
    let mut value = Vec::new();
    for _ in 0..2 {
        assert!(matches!(
            Builder::new(&mut store, hint),
            Err(Error::LimitExceeded { .. })
        ));
        let branch = build(&mut store);
        let mut encoded = Vec::new();
        branch.encode(&mut encoded).unwrap();
        let (back, _) = Branch::decode(&mut store, &encoded).unwrap();
        for n in 0..ENTRIES {
            value.clear();
            assert_eq!(
                back.get(&mut store, &key(n), &mut value).unwrap(),
                Some(Op::Put)
            );
            assert_eq!(value, n.to_le_bytes());
            value.clear();
            assert_eq!(
                warm.get(&mut store, &key(n), &mut value).unwrap(),
                Some(Op::Put)
            );
            assert_eq!(value, n.to_le_bytes());
        }
    }
}

#[test]
fn an_unrepresentable_exact_size_is_refused_and_the_next_builder_round_trips() {
    refused_then_reused(Keys::Exactly(u64::MAX));
}

#[test]
fn an_unrepresentable_upper_bound_is_refused_and_the_next_builder_round_trips() {
    refused_then_reused(Keys::AtMost(u64::MAX));
}
