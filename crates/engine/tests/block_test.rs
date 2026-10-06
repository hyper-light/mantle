//! Ports of `table/block_based/block_test.cc` [R block_test.cc] for the reader: whole-block
//! scans and seeks over data and index blocks, interpolation search at the shared-prefix
//! boundary, per-entry checksums, and blocks with corrupt entries or footers. Not ported: the
//! read-amplification bitmap tests (the bitmap is not ported), `ApproximateMemory` (the port
//! reports memory with its statistics), user-defined timestamps (P16), and the tests that corrupt
//! a value in memory through a sync point, which are `block.rs`'s own unit tests. Then the
//! port's own: the seek-for-get end-of-block fix and widths RocksDB does not write.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

#[allow(dead_code)]
#[path = "support/random.rs"]
mod random;

use std::collections::BTreeSet;

use mantle_engine::db::dbformat::{
    DISABLE_GLOBAL_SEQUENCE_NUMBER, MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK, ValueType,
    append_internal_key_footer,
};
use mantle_engine::db::kv_checksum;
use mantle_engine::error::Error;
use mantle_engine::table::block_based::block::{
    Block, BlockIter, BlockSearchType, IndexIterOptions,
};
use mantle_engine::table::block_based::block_builder::{BlockBuilder, BlockBuilderOptions};
use mantle_engine::table::block_based::data_block_footer::{DataBlockFooter, DataBlockIndexType};
use mantle_engine::table::format::{BLOCK_TRAILER_SIZE, BlockHandle, IndexValue};
use mantle_engine::util::comparator::Comparator;
use random::Random;

/// `Random::RandomString`: printable bytes.
fn random_string(rnd: &mut Random, len: usize) -> Vec<u8> {
    (0..len).map(|_| b' ' + rnd.uniform(95) as u8).collect()
}

fn internal(user: &[u8], seq: u64, t: ValueType) -> Vec<u8> {
    let mut k = user.to_vec();
    append_internal_key_footer(&mut k, seq, t).unwrap();
    k
}

/// `GenerateInternalKey`.
fn generate_internal_key(
    primary: i32,
    secondary: i32,
    padding: usize,
    rnd: &mut Random,
) -> Vec<u8> {
    let mut k = format!("{primary:6}{secondary:4}").into_bytes();
    k.extend(random_string(rnd, padding));
    internal(&k, 0, ValueType::Value)
}

/// `GenerateRandomKVs`: sorted internal keys and 100-byte values.
fn generate_random_kvs(
    from: i32,
    len: i32,
    step: usize,
    padding: usize,
    keys_share_prefix: i32,
) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut rnd = Random::new(302);
    let (mut keys, mut values) = (Vec::new(), Vec::new());
    for i in (from..from + len).step_by(step) {
        for j in 0..keys_share_prefix {
            keys.push(generate_internal_key(i, j, padding, &mut rnd));
            values.push(random_string(&mut rnd, 100));
        }
    }
    (keys, values)
}

fn index_type(hash: bool) -> DataBlockIndexType {
    if hash {
        DataBlockIndexType::BinaryAndHash
    } else {
        DataBlockIndexType::BinarySearch
    }
}

fn build(options: BlockBuilderOptions, keys: &[Vec<u8>], values: &[Vec<u8>]) -> Vec<u8> {
    let mut b = BlockBuilder::new(options).unwrap();
    for (k, v) in keys.iter().zip(values) {
        b.add(k, v, None, false).unwrap();
    }
    b.finish().unwrap().to_vec()
}

fn data_options(interval: u32, delta: bool, hash: bool, separated: bool) -> BlockBuilderOptions {
    BlockBuilderOptions {
        restart_interval: interval,
        use_delta_encoding: delta,
        index_type: index_type(hash),
        use_separated_kv_storage: separated,
        ..BlockBuilderOptions::default()
    }
}

/// The parameters `BlockTest` runs under, less user-defined timestamps.
fn block_test_params() -> Vec<(bool, bool, u32, bool)> {
    let mut p = Vec::new();
    for delta in [false, true] {
        for hash in [false, true] {
            for interval in [1, 8, 16] {
                for separated in [false, true] {
                    p.push((delta, hash, interval, separated));
                }
            }
        }
    }
    p
}

#[test]
fn simple_test() {
    for (delta, hash, interval, separated) in block_test_params() {
        let mut rnd = Random::new(301);
        let records = 20;
        let (keys, values) = generate_random_kvs(0, records, 1, 0, 1);
        let block = Block::new(
            build(
                data_options(interval, delta, hash, separated),
                &keys,
                &values,
            ),
            interval,
        );
        let mut it = block.new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER);
        let mut count = 0;
        it.seek_to_first();
        while it.valid() {
            assert_eq!(it.key(), keys[count].as_slice());
            assert_eq!(it.value(), values[count].as_slice());
            count += 1;
            it.next();
        }
        assert_eq!(count, keys.len());
        let mut it = block.new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER);
        for _ in 0..records {
            let index = rnd.uniform(records as u32) as usize;
            it.seek(&keys[index]);
            assert!(it.valid());
            assert_eq!(it.value(), values[index].as_slice());
        }
    }
}

/// `CheckBlockContents`.
fn check_block_contents(
    block: &[u8],
    max_key: i32,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    interval: u32,
) {
    let block = Block::new(block.to_vec(), interval);
    let mut it = block.new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER);
    for (k, v) in keys.iter().zip(values) {
        it.seek(k);
        it.status().unwrap();
        assert!(it.valid());
        assert_eq!(it.value(), v.as_slice());
    }
    let mut none = Random::new(1);
    for i in (1..max_key - 1).step_by(2) {
        it.seek(&generate_internal_key(i, 0, 0, &mut none));
        assert!(it.valid(), "seek past key {i}");
    }
}

#[test]
fn simple_index_hash() {
    let max_key = 100_000;
    let (keys, values) = generate_random_kvs(0, max_key, 2, 8, 1);
    for (delta, hash, interval, separated) in block_test_params() {
        let block = build(
            data_options(interval, delta, hash, separated),
            &keys,
            &values,
        );
        check_block_contents(&block, max_key, &keys, &values, interval);
    }
}

#[test]
fn index_hash_with_shared_prefix() {
    let max_key = 100_000;
    let (keys, values) = generate_random_kvs(0, max_key, 2, 10, 5);
    for (delta, hash, interval, separated) in block_test_params() {
        let block = build(
            data_options(interval, delta, hash, separated),
            &keys,
            &values,
        );
        check_block_contents(&block, max_key, &keys, &values, interval);
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyDistribution {
    Uniform,
    NonUniform,
}

struct IndexEntries {
    separators: Vec<Vec<u8>>,
    handles: Vec<BlockHandle>,
    first_keys: Vec<Vec<u8>>,
}

/// `GenerateRandomIndexEntries`: `len` blocks' first keys, separators and consecutive handles.
fn generate_random_index_entries(
    len: usize,
    key_length: usize,
    prefix_length: usize,
    distribution: KeyDistribution,
) -> IndexEntries {
    let mut rnd = Random::new(42);
    let prefix = vec![b'x'; prefix_length];
    let mut keys = BTreeSet::new();
    let cluster_prefix_len = key_length.saturating_sub(5);
    let mut cluster1 = prefix.clone();
    cluster1.extend(random_string(&mut rnd, cluster_prefix_len));
    let mut cluster2 = prefix.clone();
    cluster2.extend(random_string(&mut rnd, cluster_prefix_len));
    while keys.len() < len * 2 {
        let mut key = if distribution == KeyDistribution::NonUniform {
            let remaining = key_length - cluster_prefix_len;
            let mut k = if keys.len() % 2 == 0 {
                cluster1.clone()
            } else {
                cluster2.clone()
            };
            k.extend(random_string(&mut rnd, remaining.max(1)));
            k
        } else {
            let mut base = keys.len() as u64 * 1000 + u64::from(rnd.uniform(100));
            let mut bytes = vec![0u8; key_length];
            for j in (0..key_length).rev() {
                if base == 0 {
                    break;
                }
                bytes[j] = (base & 0xFF) as u8;
                base >>= 8;
            }
            let mut k = prefix.clone();
            k.extend(bytes);
            k
        };
        append_internal_key_footer(&mut key, 0, ValueType::Value).unwrap();
        keys.insert(key);
    }
    let mut out = IndexEntries {
        separators: Vec::new(),
        handles: Vec::new(),
        first_keys: Vec::new(),
    };
    let mut offset = 0u64;
    let mut it = keys.into_iter();
    while let (Some(first), Some(separator)) = (it.next(), it.next()) {
        out.first_keys.push(first);
        out.separators.push(separator);
        let size = u64::from(rnd.uniform(1024 * 16));
        out.handles.push(BlockHandle { offset, size });
        offset += size + BLOCK_TRAILER_SIZE;
    }
    out
}

/// `AddIndexBlockEntry`.
fn add_index_entry(
    b: &mut BlockBuilder,
    key: &[u8],
    handle: BlockHandle,
    previous: Option<&BlockHandle>,
    include_first_key: bool,
    first_internal_key: &[u8],
) {
    let entry = IndexValue {
        handle,
        first_internal_key,
    };
    let mut full = Vec::new();
    entry.encode_to(&mut full, include_first_key, None).unwrap();
    let mut delta = Vec::new();
    if let Some(previous) = previous {
        entry
            .encode_to(&mut delta, include_first_key, Some(previous))
            .unwrap();
    }
    b.add(key, &full, Some(&delta), false).unwrap();
}

fn user_key(k: &[u8]) -> &[u8] {
    &k[..k.len() - 8]
}

#[test]
fn index_value_encoding_test() {
    let mut cases = 0;
    for key_includes_seq in [false, true] {
        for value_delta in [false, true] {
            for include_first_key in [false, true] {
                for separated in [false, true] {
                    for search in [
                        BlockSearchType::Binary,
                        BlockSearchType::Interpolation,
                        BlockSearchType::Auto,
                    ] {
                        for records in [1usize, 100] {
                            for interval in [1u32, 16] {
                                for key_length in [1usize, 8, 12] {
                                    for (prefix, distribution) in [
                                        (0, KeyDistribution::Uniform),
                                        (0, KeyDistribution::NonUniform),
                                        (50, KeyDistribution::Uniform),
                                        (50, KeyDistribution::NonUniform),
                                    ] {
                                        index_value_case(
                                            key_includes_seq,
                                            value_delta,
                                            include_first_key,
                                            separated,
                                            search,
                                            records.min(1 << key_length),
                                            interval,
                                            key_length,
                                            prefix,
                                            distribution,
                                        );
                                        cases += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(cases, 2304);
}

#[allow(clippy::too_many_arguments)]
fn index_value_case(
    key_includes_seq: bool,
    value_delta: bool,
    include_first_key: bool,
    separated: bool,
    search: BlockSearchType,
    records: usize,
    interval: u32,
    key_length: usize,
    prefix: usize,
    distribution: KeyDistribution,
) {
    let mut rnd = Random::new(301);
    let entries = generate_random_index_entries(records, key_length, prefix, distribution);
    let mut b = BlockBuilder::new(BlockBuilderOptions {
        restart_interval: interval,
        use_delta_encoding: true,
        use_value_delta_encoding: value_delta,
        is_user_key: !key_includes_seq,
        use_separated_kv_storage: separated,
        uniform_cv_threshold: Some(0.2),
        ..BlockBuilderOptions::default()
    })
    .unwrap();
    let mut last = None;
    for i in 0..records {
        let sep = &entries.separators[i];
        let key = if key_includes_seq {
            sep.as_slice()
        } else {
            user_key(sep)
        };
        let previous = if value_delta && i > 0 {
            last.as_ref()
        } else {
            None
        };
        add_index_entry(
            &mut b,
            key,
            entries.handles[i],
            previous,
            include_first_key,
            &entries.first_keys[i],
        );
        last = Some(entries.handles[i]);
    }
    let bytes = b.finish().unwrap().to_vec();
    let builder_uniform = b.is_uniform();
    let block = Block::new(bytes, interval);
    let options = IndexIterOptions {
        have_first_key: include_first_key,
        key_includes_seq,
        value_is_full: !value_delta,
        search,
    };
    let expect = |it: &BlockIter<'_>, index: usize| {
        let sep = &entries.separators[index];
        let want: &[u8] = if key_includes_seq { sep } else { user_key(sep) };
        assert_eq!(it.key(), want);
        let v = it.index_value().unwrap();
        assert_eq!(v.handle, entries.handles[index]);
        let first: &[u8] = if include_first_key {
            &entries.first_keys[index]
        } else {
            &[]
        };
        assert_eq!(v.first_internal_key, first);
    };
    let mut it = block.new_index_iterator(
        Comparator::Bytewise,
        DISABLE_GLOBAL_SEQUENCE_NUMBER,
        options,
    );
    it.seek_to_first();
    for index in 0..records {
        assert!(it.valid());
        expect(&it, index);
        it.next();
    }
    let expect_uniform = block.num_restarts() >= 3 && distribution == KeyDistribution::Uniform;
    assert_eq!(block.is_uniform(), expect_uniform);
    assert_eq!(builder_uniform, expect_uniform);
    let mut it = block.new_index_iterator(
        Comparator::Bytewise,
        DISABLE_GLOBAL_SEQUENCE_NUMBER,
        options,
    );
    for _ in 0..records * 2 {
        let index = rnd.uniform(records as u32) as usize;
        it.seek(&entries.separators[index]);
        assert!(it.valid());
        expect(&it, index);
    }
}

fn prefix_boundary_block(keys: &[Vec<u8>], is_user_key: bool) -> Block {
    const BLOCK_SIZE: u64 = 50;
    let handles: Vec<BlockHandle> = (0..keys.len() as u64)
        .map(|i| BlockHandle {
            offset: i * (BLOCK_SIZE + BLOCK_TRAILER_SIZE),
            size: BLOCK_SIZE,
        })
        .collect();
    let mut b = BlockBuilder::new(BlockBuilderOptions {
        restart_interval: 1,
        use_value_delta_encoding: true,
        is_user_key,
        ..BlockBuilderOptions::default()
    })
    .unwrap();
    for (i, k) in keys.iter().enumerate() {
        let previous = if i > 0 { Some(&handles[i - 1]) } else { None };
        add_index_entry(&mut b, k, handles[i], previous, false, &[]);
    }
    Block::new(b.finish().unwrap().to_vec(), 0)
}

fn interpolating(key_includes_seq: bool) -> IndexIterOptions {
    IndexIterOptions {
        have_first_key: false,
        key_includes_seq,
        value_is_full: false,
        search: BlockSearchType::Interpolation,
    }
}

#[test]
fn interpolation_search_prefix_boundary() {
    let keys: Vec<Vec<u8>> = (0..20)
        .map(|i| format!("ABCDEFGHIJ{i:03}").into_bytes())
        .collect();
    let block = prefix_boundary_block(&keys, true);
    let target = |u: &str| {
        let mut t = u.as_bytes().to_vec();
        append_internal_key_footer(&mut t, MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK).unwrap();
        t
    };
    let mut it = block.new_index_iterator(
        Comparator::Bytewise,
        DISABLE_GLOBAL_SEQUENCE_NUMBER,
        interpolating(false),
    );
    for (t, want) in [
        ("AAAAAA", Some(0)),
        ("", Some(0)),
        ("ABCDEFGHZZ", None),
        ("ABCDEFGHIJ", Some(0)),
        ("ABCDEFG", Some(0)),
    ] {
        it.seek(&target(t));
        it.status().unwrap();
        match want {
            Some(i) => {
                assert!(it.valid(), "{t}");
                assert_eq!(it.key(), keys[i].as_slice(), "{t}");
            }
            None => assert!(!it.valid(), "{t}"),
        }
    }
}

#[test]
fn interpolation_search_prefix_boundary2() {
    let keys: Vec<Vec<u8>> = (0..20u64)
        .map(|i| internal(b"ABCDEFGHIJ", 20 - i, ValueType::Value))
        .collect();
    let block = prefix_boundary_block(&keys, false);
    let mut it = block.new_index_iterator(
        Comparator::Bytewise,
        DISABLE_GLOBAL_SEQUENCE_NUMBER,
        interpolating(true),
    );
    for (i, k) in keys.iter().enumerate() {
        it.seek(&internal(b"ABCDEFGHIJ", 20 - i as u64, ValueType::Value));
        assert!(it.valid());
        assert_eq!(it.key(), k.as_slice());
    }
    let max = |u: &[u8]| internal(u, MAX_SEQUENCE_NUMBER, ValueType::Value);
    for (t, want) in [
        (max(b"AAAAAA"), Some(0)),
        (max(b""), Some(0)),
        (max(b"ABCDEFGHZZ"), None),
        (max(b"ABCDEFGHIJ"), Some(0)),
        (max(b"ABCDEFG"), Some(0)),
        (max(b"ABCDEFGHIJ\x01"), None),
    ] {
        it.seek(&t);
        it.status().unwrap();
        match want {
            Some(i) => {
                assert!(it.valid());
                assert_eq!(it.key(), keys[i].as_slice());
            }
            None => assert!(!it.valid()),
        }
    }
}

#[test]
fn empty_block() {
    let bytes = build(data_options(16, true, false, false), &[], &[]);
    let mut block = Block::new(bytes, 16);
    block
        .initialize_data_block_protection_info(8, Comparator::Bytewise)
        .unwrap();
    let mut it = block.new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER);
    let mut rnd = Random::new(33);
    it.seek_to_first();
    assert!(!it.valid());
    it.status().unwrap();
    it.seek_for_get(&generate_internal_key(1, 1, 10, &mut rnd));
    assert!(!it.valid());
    it.status().unwrap();
    it.seek_to_last();
    assert!(!it.valid());
    it.status().unwrap();
    it.seek(&generate_internal_key(1, 1, 10, &mut rnd));
    assert!(!it.valid());
    it.status().unwrap();
    it.seek_for_prev(&generate_internal_key(1, 1, 10, &mut rnd));
    assert!(!it.valid());
    it.status().unwrap();
}

fn is_corruption(r: Result<(), Error>) -> bool {
    matches!(r, Err(Error::Corruption { .. }))
}

#[test]
fn initialize_protection_info_on_a_corrupt_block() {
    let mut data = Block::new(b"1".to_vec(), 0);
    data.initialize_data_block_protection_info(8, Comparator::Bytewise)
        .unwrap();
    assert!(is_corruption(
        data.new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER)
            .status()
    ));
    let mut index = Block::new(b"1".to_vec(), 0);
    index
        .initialize_index_block_protection_info(8, Comparator::Bytewise, true, false)
        .unwrap();
    let options = IndexIterOptions {
        have_first_key: false,
        key_includes_seq: true,
        value_is_full: true,
        search: BlockSearchType::Binary,
    };
    assert!(is_corruption(
        index
            .new_index_iterator(
                Comparator::Bytewise,
                DISABLE_GLOBAL_SEQUENCE_NUMBER,
                options
            )
            .status()
    ));
    let mut meta = Block::new(b"1".to_vec(), 0);
    meta.initialize_meta_index_block_protection_info(8).unwrap();
    assert!(is_corruption(meta.new_meta_iterator().status()));
}

#[test]
fn corrupt_hash_index_num_buckets_no_over_read() {
    let mut body = vec![b'x'; 500];
    body.extend_from_slice(&60_000u16.to_le_bytes());
    DataBlockFooter {
        num_restarts: 1,
        index_type: DataBlockIndexType::BinaryAndHash,
        separated_kv: true,
        is_uniform: false,
        values_section_offset: u32::MAX,
    }
    .encode_to(&mut body)
    .unwrap();
    let block = Block::new(body, 0);
    assert!(is_corruption(
        block
            .new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER)
            .status()
    ));
}

/// What a move reads: the key, and the value as `value` renders it.
type Read = fn(&BlockIter<'_>) -> Vec<u8>;

fn plain(it: &BlockIter<'_>) -> Vec<u8> {
    it.value().to_vec()
}

/// An index entry's value in full: its handle, then its first key where the block carries them.
fn full_index_value(have_first_key: bool) -> Read {
    if have_first_key {
        |it| encode_full(it, true)
    } else {
        |it| encode_full(it, false)
    }
}

fn encode_full(it: &BlockIter<'_>, have_first_key: bool) -> Vec<u8> {
    let mut v = Vec::new();
    it.index_value()
        .unwrap()
        .encode_to(&mut v, have_first_key, None)
        .unwrap();
    v
}

/// The entries the iterator visits from where it is, forward or back.
fn walk(it: &mut BlockIter<'_>, forward: bool, read: Read) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut seen = Vec::new();
    while it.valid() {
        seen.push((it.key().to_vec(), read(it)));
        if forward {
            it.next();
        } else {
            it.prev();
        }
    }
    it.status().unwrap();
    seen
}

/// Checks that the block holds one checksum of `width` bytes per entry, each `ProtectKV` of its
/// entry's key and value as stored, then that every move reads, verifies and returns the entries
/// it should: `TestSeekToFirst`, `TestSeekToLast`, `TestSeek` and `TestSeekForPrev`, which count
/// verifications through a sync point where the port compares the entries returned (a failed
/// verification ends the iterator with corruption, which `walk` refuses).
fn check_protected<'b>(
    block: &'b Block,
    width: u8,
    entries: &[(Vec<u8>, Vec<u8>)],
    new_iter: impl Fn(&'b Block) -> BlockIter<'b>,
    read: Read,
    seek_for_prev: bool,
) {
    let sums = block.kv_checksum();
    let w = usize::from(width);
    assert_eq!(sums.len(), entries.len() * w);
    if width > 0 {
        let mut it = new_iter(block);
        it.seek_to_first();
        for i in 0..entries.len() {
            let sum = kv_checksum::protect_kv(it.key(), it.raw_value());
            assert!(
                kv_checksum::verify(sum, width, &sums[i * w..(i + 1) * w]),
                "entry {i}"
            );
            it.next();
        }
    }
    let mid = entries.len() / 2;
    let mut it = new_iter(block);
    it.seek_to_first();
    assert_eq!(walk(&mut it, true, read), entries);
    it.seek_to_last();
    let backward: Vec<_> = entries.iter().rev().cloned().collect();
    assert_eq!(walk(&mut it, false, read), backward);
    it.seek(&entries[mid].0);
    assert_eq!(walk(&mut it, true, read), entries[mid..]);
    if seek_for_prev {
        it.seek_for_prev(&entries[mid].0);
        let back: Vec<_> = entries[..=mid].iter().rev().cloned().collect();
        assert_eq!(walk(&mut it, false, read), back);
    }
}

#[test]
fn data_block_checksum_construction_and_verification() {
    for hash in [false, true] {
        for width in [0u8, 1, 2, 4, 8] {
            for interval in [1u32, 2, 3, 8, 16] {
                for delta in [false, true] {
                    for intervals in [1, 16] {
                        let records = intervals * interval as i32;
                        let (keys, values) = generate_random_kvs(0, records + 1, 1, 24, 1);
                        let n = records as usize;
                        let options = data_options(interval, delta, hash, false);
                        let mut block =
                            Block::new(build(options, &keys[..n], &values[..n]), interval);
                        block
                            .initialize_data_block_protection_info(width, Comparator::Bytewise)
                            .unwrap();
                        let entries: Vec<_> = keys[..n]
                            .iter()
                            .cloned()
                            .zip(values[..n].iter().cloned())
                            .collect();
                        // The stored checksums cover the entries as written, not only as read.
                        for (i, (k, v)) in entries.iter().enumerate() {
                            let w = usize::from(width);
                            let sum = kv_checksum::protect_kv(k, v);
                            assert!(
                                width == 0
                                    || kv_checksum::verify(
                                        sum,
                                        width,
                                        &block.kv_checksum()[i * w..(i + 1) * w]
                                    )
                            );
                        }
                        check_protected(
                            &block,
                            width,
                            &entries,
                            |b| {
                                b.new_data_iterator(
                                    Comparator::Bytewise,
                                    DISABLE_GLOBAL_SEQUENCE_NUMBER,
                                )
                            },
                            plain,
                            true,
                        );
                    }
                }
            }
        }
    }
}

/// `first` with the global sequence number `seqno` in place, as the iterator returns it.
fn with_seqno(first: &[u8], seqno: u64) -> Vec<u8> {
    if seqno == DISABLE_GLOBAL_SEQUENCE_NUMBER {
        return first.to_vec();
    }
    let user = first.len() - 8;
    let mut k = first[..user].to_vec();
    k.extend_from_slice(&((seqno << 8) | u64::from(first[user])).to_le_bytes());
    k
}

#[test]
fn index_block_checksum_construction_and_verification() {
    for hash in [false, true] {
        for width in [0u8, 1, 2, 4, 8] {
            for interval in [1u32, 3, 8, 16] {
                for value_delta in [true, false] {
                    for include_first_key in [true, false] {
                        for intervals in [1usize, 16] {
                            for seqno in [DISABLE_GLOBAL_SEQUENCE_NUMBER, 10_001] {
                                index_checksum_case(
                                    hash,
                                    width,
                                    interval,
                                    value_delta,
                                    include_first_key,
                                    intervals,
                                    seqno,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

fn index_checksum_case(
    hash: bool,
    width: u8,
    interval: u32,
    value_delta: bool,
    include_first_key: bool,
    intervals: usize,
    seqno: u64,
) {
    let records = intervals * interval as usize;
    let e = generate_random_index_entries(records, 12, 0, KeyDistribution::Uniform);
    let mut b = BlockBuilder::new(BlockBuilderOptions {
        restart_interval: interval,
        use_value_delta_encoding: value_delta,
        index_type: index_type(hash),
        ..BlockBuilderOptions::default()
    })
    .unwrap();
    let mut last = None;
    let mut entries = Vec::new();
    for i in 0..records {
        let previous = if value_delta && i > 0 {
            last.as_ref()
        } else {
            None
        };
        add_index_entry(
            &mut b,
            &e.separators[i],
            e.handles[i],
            previous,
            include_first_key,
            &e.first_keys[i],
        );
        last = Some(e.handles[i]);
        let first = with_seqno(&e.first_keys[i], seqno);
        let mut full = Vec::new();
        IndexValue {
            handle: e.handles[i],
            first_internal_key: &first,
        }
        .encode_to(&mut full, include_first_key, None)
        .unwrap();
        entries.push((e.separators[i].clone(), full));
    }
    let mut block = Block::new(b.finish().unwrap().to_vec(), interval);
    block
        .initialize_index_block_protection_info(
            width,
            Comparator::Bytewise,
            !value_delta,
            include_first_key,
        )
        .unwrap();
    let options = IndexIterOptions {
        have_first_key: include_first_key,
        key_includes_seq: true,
        value_is_full: !value_delta,
        search: BlockSearchType::Binary,
    };
    check_protected(
        &block,
        width,
        &entries,
        |b| b.new_index_iterator(Comparator::Bytewise, seqno, options),
        full_index_value(include_first_key),
        false,
    );
}

#[test]
fn meta_index_block_checksum_construction_and_verification() {
    for width in [0u8, 1, 2, 4, 8] {
        for records in [1i32, 16] {
            let (keys, values) = generate_random_kvs(0, records + 1, 1, 24, 1);
            let n = records as usize;
            let options = BlockBuilderOptions {
                restart_interval: 1,
                ..BlockBuilderOptions::default()
            };
            let mut block = Block::new(build(options, &keys[..n], &values[..n]), 1);
            block
                .initialize_meta_index_block_protection_info(width)
                .unwrap();
            let entries: Vec<_> = keys[..n]
                .iter()
                .cloned()
                .zip(values[..n].iter().cloned())
                .collect();
            check_protected(
                &block,
                width,
                &entries,
                |b| b.new_meta_iterator(),
                plain,
                true,
            );
        }
    }
}

fn meta_block(separated: bool) -> Vec<u8> {
    let mut b = BlockBuilder::new(BlockBuilderOptions {
        restart_interval: 1,
        data_block_hash_table_util_ratio: 0.0,
        is_user_key: true,
        use_separated_kv_storage: separated,
        ..BlockBuilderOptions::default()
    })
    .unwrap();
    for i in 1..=4 {
        b.add(
            format!("key00{i}").as_bytes(),
            format!("val0{i}").as_bytes(),
            None,
            false,
        )
        .unwrap();
    }
    b.finish().unwrap().to_vec()
}

/// The restart array's start, the keys' end and the first entry's offset of `meta_block`.
fn meta_layout(data: &[u8], separated: bool) -> (u32, u32, usize) {
    let footer = if separated { 8 } else { 4 };
    let packed = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
    let restarts = (packed & DataBlockFooter::MAX_NUM_RESTARTS) as usize;
    let restarts_start = data.len() - footer - restarts * 4;
    let key_end = if separated {
        u32::from_le_bytes(data[data.len() - 8..data.len() - 4].try_into().unwrap())
    } else {
        restarts_start as u32
    };
    let first = u32::from_le_bytes(data[restarts_start..restarts_start + 4].try_into().unwrap());
    (restarts_start as u32, key_end, first as usize)
}

#[test]
fn corrupted_key_length_past_key_end() {
    for separated in [false, true] {
        let mut data = meta_block(separated);
        let (_, key_end, first) = meta_layout(&data, separated);
        data[first + 1] = key_end as u8;
        let block = Block::new(data, 1);
        let mut it = block.new_meta_iterator();
        it.seek_to_first();
        assert!(!it.valid());
        assert!(is_corruption(it.status()));
    }
}

#[test]
fn corrupted_value_length_past_value_end() {
    for separated in [false, true] {
        let mut data = meta_block(separated);
        let (value_end, _, first) = meta_layout(&data, separated);
        data[first + 2] = value_end as u8;
        let block = Block::new(data, 1);
        let mut it = block.new_meta_iterator();
        it.seek_to_first();
        assert!(!it.valid());
        assert!(is_corruption(it.status()));
    }
}

#[test]
fn separated_kv_invalid_values_section_offset() {
    let mut b = BlockBuilder::new(BlockBuilderOptions {
        restart_interval: 16,
        index_type: DataBlockIndexType::BinaryAndHash,
        use_separated_kv_storage: true,
        ..BlockBuilderOptions::default()
    })
    .unwrap();
    for i in 0..5u64 {
        b.add(
            &internal(format!("key{i}").as_bytes(), 100 - i, ValueType::Value),
            format!("value{i}").as_bytes(),
            None,
            false,
        )
        .unwrap();
    }
    let mut data = b.finish().unwrap().to_vec();
    let mut input = data.as_slice();
    let mut footer = DataBlockFooter::decode_from(&mut input).unwrap();
    assert!(footer.separated_kv);
    footer.values_section_offset = data.len() as u32;
    let mut encoded = Vec::new();
    footer.encode_to(&mut encoded).unwrap();
    let at = data.len() - encoded.len();
    data[at..].copy_from_slice(&encoded);
    let block = Block::new(data, 0);
    assert_eq!(block.size(), 0);
    let it = block.new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER);
    assert!(!it.valid());
    assert!(is_corruption(it.status()));
}

/// RocksDB's `SeekForGetImpl` decides it ran off the block's end by comparing the position with
/// the restart array's start, which with separated keys and values it never reaches: the keys
/// end at the values section. It then compares the last key it read with the target, and for a
/// target past every key answers that the key is in neither this block nor the next, which is
/// wrong (the contract of `SeekForGet`, block.cc:215-239). The port tests validity instead, and answers
/// that the next block may hold it.
#[test]
fn seek_for_get_past_the_last_key_of_a_separated_block() {
    let keys: Vec<Vec<u8>> = ["a", "b", "c"]
        .iter()
        .map(|u| internal(u.as_bytes(), 1, ValueType::Value))
        .collect();
    let values = vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()];
    let block = Block::new(
        build(data_options(16, true, true, true), &keys, &values),
        16,
    );
    let mut it = block.new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER);
    assert!(it.seek_for_get(&internal(b"zzz", 7, ValueType::Value)));
    assert!(!it.valid());
    it.status().unwrap();
    assert!(it.seek_for_get(&internal(b"b", 7, ValueType::Value)));
    assert_eq!(it.value(), b"2");
}

/// A width RocksDB never writes (it accepts 0, 1, 2, 4 and 8 bytes) is refused as unsupported
/// rather than left to undefined behaviour in `Encode`.
#[test]
fn unsupported_protection_width() {
    let mut block = Block::new(
        build(
            data_options(16, true, false, false),
            &[internal(b"a", 1, ValueType::Value)],
            &[b"v".to_vec()],
        ),
        16,
    );
    assert!(matches!(
        block.initialize_data_block_protection_info(3, Comparator::Bytewise),
        Err(Error::Unsupported { .. })
    ));
}
