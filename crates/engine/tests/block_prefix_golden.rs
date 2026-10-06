//! The port's hash-search index reading against RocksDB 11.8.1's (`tests/golden/p4_prefix_gen.cc`):
//! each of the 160 indexes RocksDB wrote is read by the port's `BlockPrefixIndex` from RocksDB's
//! prefix blocks, and every seek and step through it must land where RocksDB's did, with the same
//! key and handle, or the same verdict that the prefix is absent.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::db::dbformat::DISABLE_GLOBAL_SEQUENCE_NUMBER;
use mantle_engine::table::block_based::block::{
    Block, BlockIter, BlockSearchType, IndexIterOptions,
};
use mantle_engine::table::block_based::block_prefix_index::BlockPrefixIndex;
use mantle_engine::util::comparator::Comparator;
use mantle_engine::util::slice_transform::SliceTransform;

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
}

fn result(it: &BlockIter<'_>) -> String {
    if it.prefix_absent() {
        return "A".into();
    }
    if it.status().is_err() {
        return "E".into();
    }
    if !it.valid() {
        return "I".into();
    }
    let v = it.index_value().unwrap();
    format!("V{}/{},{}", hex(it.key()), v.handle.offset, v.handle.size)
}

#[test]
fn hash_index_seeks_match_rocksdb() {
    let golden = include_str!("golden/p4_prefix.txt");
    let (mut indexes, mut moves, mut absent) = (0, 0, 0);
    for line in golden.lines() {
        let f: Vec<&str> = line.split(' ').collect();
        let comparator = if f[0] == "b" {
            Comparator::Bytewise
        } else {
            Comparator::ReverseBytewise
        };
        let (kind, len) = f[2].split_once('.').unwrap();
        let len: usize = len.parse().unwrap();
        let extractor = match kind {
            "0" => SliceTransform::Fixed(len),
            "1" => SliceTransform::Capped(len),
            _ => SliceTransform::Noop,
        };
        let prefix_index = BlockPrefixIndex::create(extractor, &unhex(f[6]), &unhex(f[7])).unwrap();
        let block = Block::new(unhex(f[5]), 1);
        let mut it = block.new_index_iterator(
            comparator,
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
            IndexIterOptions {
                have_first_key: false,
                key_includes_seq: f[4] == "1",
                value_is_full: f[3] == "1",
                search: BlockSearchType::Binary,
                prefix_index: Some(&prefix_index),
            },
        );
        for (m, step) in f[8..].iter().enumerate() {
            let (op, want) = step.split_once('=').unwrap();
            match op.split_once(':') {
                Some(("S", target)) => it.seek(&unhex(target)),
                _ => it.next(),
            }
            assert_eq!(result(&it), want, "index {indexes} move {m} {op}");
            absent += usize::from(want == "A");
            moves += 1;
        }
        indexes += 1;
    }
    assert_eq!(indexes, 160);
    eprintln!("{indexes} indexes, {moves} moves, {absent} absent prefixes");
}

/// Prefix metadata RocksDB would trust: a run of no blocks (whose end RocksDB underflows), a
/// prefix past the prefix block, prefix bytes no run names, and an unreadable field.
#[test]
fn corrupt_prefix_metadata_is_refused() {
    let meta = |fields: &[u32]| {
        let mut m = Vec::new();
        for &f in fields {
            mantle_engine::util::coding::put_varint32(&mut m, f);
        }
        m
    };
    let fixed = SliceTransform::Fixed(2);
    assert!(BlockPrefixIndex::create(fixed, b"ab", &meta(&[2, 0, 1])).is_ok());
    for (prefixes, metadata) in [
        (&b"ab"[..], meta(&[2, 0, 0])),
        (&b"ab"[..], meta(&[3, 0, 1])),
        (&b"abcd"[..], meta(&[2, 0, 1])),
        (&b"ab"[..], vec![0x80]),
        (&b"ab"[..], meta(&[2, 0x7FFF_FFFF, 1])),
    ] {
        assert!(
            matches!(
                BlockPrefixIndex::create(fixed, prefixes, &metadata),
                Err(mantle_engine::error::Error::Corruption { .. })
            ),
            "{prefixes:?} {metadata:?}"
        );
    }
}

/// A target outside the extractor's domain, where RocksDB's `Transform` reads past the key,
/// is answered by a total-order seek: the first entry at or after it.
#[test]
fn a_target_outside_the_domain_seeks_in_total_order() {
    let line = include_str!("golden/p4_prefix.txt")
        .lines()
        .find(|l| {
            l.split(' ')
                .nth(2)
                .is_some_and(|p| p.starts_with("0.") && p != "0.1")
        })
        .unwrap();
    let f: Vec<&str> = line.split(' ').collect();
    let len: usize = f[2].split_once('.').unwrap().1.parse().unwrap();
    let comparator = if f[0] == "b" {
        Comparator::Bytewise
    } else {
        Comparator::ReverseBytewise
    };
    let prefix_index =
        BlockPrefixIndex::create(SliceTransform::Fixed(len), &unhex(f[6]), &unhex(f[7])).unwrap();
    let block = Block::new(unhex(f[5]), 1);
    let options = IndexIterOptions {
        have_first_key: false,
        key_includes_seq: f[4] == "1",
        value_is_full: f[3] == "1",
        search: BlockSearchType::Binary,
        prefix_index: None,
    };
    let mut total = block.new_index_iterator(comparator, DISABLE_GLOBAL_SEQUENCE_NUMBER, options);
    let mut hashed = block.new_index_iterator(
        comparator,
        DISABLE_GLOBAL_SEQUENCE_NUMBER,
        IndexIterOptions {
            prefix_index: Some(&prefix_index),
            ..options
        },
    );
    // A user key one byte shorter than the prefix, with an internal key's trailer.
    let mut target = b"abcd"[..len - 1].to_vec();
    target.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]);
    total.seek(&target);
    hashed.seek(&target);
    assert!(!hashed.prefix_absent());
    assert_eq!(result(&hashed), result(&total));
}
