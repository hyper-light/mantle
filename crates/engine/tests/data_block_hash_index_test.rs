//! table/block_based/data_block_hash_index_test.cc, its tests of the hash index alone, ported test
//! for test: `DataBlockHashTestSmall`, `DataBlockHashTest`, `InitializeRejectsCorruptNumBuckets`,
//! `InitializeRejectsUnsupportedSize`, `DataBlockHashTestCollision`, `DataBlockHashTestLarge` and
//! `RestartIndexExceedMax`. The tests that build and read whole blocks come with the block reader.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::HashMap;

use mantle_engine::table::block_based::data_block_hash_index::{
    COLLISION, DataBlockHashIndex, DataBlockHashIndexBuilder,
    MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX, NO_ENTRY,
};

/// `SearchForOffset` [R data_block_hash_index_test.cc:26-41]: a collision may hold the key; an
/// empty bucket does not; otherwise the bucket must name `restart_point`.
fn search_for_offset(
    index: &DataBlockHashIndex,
    data: &[u8],
    key: &[u8],
    restart_point: u8,
) -> bool {
    match index.lookup(data, key).unwrap() {
        COLLISION => true,
        NO_ENTRY => false,
        entry => entry == restart_point,
    }
}

/// Builds `keys` into a hash index after `prefix`, checks the size estimate and where the map
/// starts, and returns the block and its index.
fn build(builder: &mut DataBlockHashIndexBuilder, prefix: &[u8]) -> (Vec<u8>, DataBlockHashIndex) {
    let estimated = builder.estimate_size() + prefix.len();
    let mut buffer = prefix.to_vec();
    builder.finish(&mut buffer);
    assert_eq!(buffer.len(), estimated);
    let index = DataBlockHashIndex::initialize(&buffer).unwrap();
    // The hash map starts where the block's content ended.
    assert_eq!(usize::from(index.map_offset()), prefix.len());
    (buffer, index)
}

fn key(i: u8) -> Vec<u8> {
    format!("key{i}").into_bytes()
}

#[test]
fn data_block_hash_test_small() {
    let mut builder = DataBlockHashIndexBuilder::default();
    builder.initialize(0.75);
    for j in 0..5u8 {
        for i in 0..2 + j {
            builder.add(&key(i), usize::from(i));
        }
        let (buffer, index) = build(&mut builder, b"fake");
        for i in 0..2 {
            assert!(search_for_offset(&index, &buffer, &key(i), i));
        }
        builder.reset();
    }
}

#[test]
fn data_block_hash_test() {
    let mut builder = DataBlockHashIndexBuilder::default();
    builder.initialize(0.75);
    for i in 0..100 {
        builder.add(&key(i), usize::from(i));
    }
    let (buffer, index) = build(&mut builder, b"fake content");
    for i in 0..100 {
        assert!(search_for_offset(&index, &buffer, &key(i), i));
    }
}

#[test]
fn initialize_rejects_corrupt_num_buckets() {
    let mut builder = DataBlockHashIndexBuilder::default();
    builder.initialize(0.75);
    for i in 0..10 {
        builder.add(&key(i), usize::from(i));
    }
    let mut buffer = b"fake content".to_vec();
    builder.finish(&mut buffer);
    assert!(DataBlockHashIndex::initialize(&buffer).is_some());
    // NUM_BUCKETS past the buffer, then zero.
    let at = buffer.len() - 2;
    let past = (buffer.len() + 1000) as u16;
    buffer[at..].copy_from_slice(&past.to_le_bytes());
    assert!(DataBlockHashIndex::initialize(&buffer).is_none());
    buffer[at..].copy_from_slice(&0u16.to_le_bytes());
    assert!(DataBlockHashIndex::initialize(&buffer).is_none());
}

#[test]
fn initialize_rejects_unsupported_size() {
    for size in [
        0,
        1,
        MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX,
        MAX_BLOCK_SIZE_SUPPORTED_BY_HASH_INDEX + 1,
    ] {
        assert!(DataBlockHashIndex::initialize(&vec![b'x'; size]).is_none());
    }
}

#[test]
fn data_block_hash_test_collision() {
    let mut builder = DataBlockHashIndexBuilder::default();
    builder.initialize(0.75);
    for i in 0..100 {
        builder.add(&key(i), usize::from(i));
    }
    let (buffer, index) = build(&mut builder, b"some other fake content to take up space");
    for i in 0..100 {
        assert!(search_for_offset(&index, &buffer, &key(i), i));
    }
}

#[test]
fn data_block_hash_test_large() {
    let mut builder = DataBlockHashIndexBuilder::default();
    builder.initialize(0.75);
    let mut m = HashMap::new();
    // Half the keys left out.
    for i in (0..100).step_by(2) {
        builder.add(&key(i), usize::from(i));
        m.insert(key(i), i);
    }
    let (buffer, index) = build(&mut builder, b"filling stuff");
    for i in 0..100 {
        // A key left out may land in a bucket by chance: false positives are allowed.
        if let Some(&restart) = m.get(&key(i)) {
            assert_eq!(restart, i);
            assert!(search_for_offset(&index, &buffer, &key(i), i));
        }
    }
}

#[test]
fn restart_index_exceed_max() {
    let mut builder = DataBlockHashIndexBuilder::default();
    builder.initialize(0.75);
    for i in 0..=253u8 {
        builder.add(&key(i), usize::from(i));
    }
    assert!(builder.valid());
    builder.reset();
    for i in 0..=254u8 {
        builder.add(&key(i), usize::from(i));
    }
    assert!(!builder.valid());
    builder.reset();
    assert!(builder.valid());
}
