//! RocksDB's db/dbformat_test.cc, test for test: its 12 test definitions (`IterKeySwapTest` is
//! one `TEST_P` over 64 parameter sets) with the same literals.
//!
//! `IterKeySwapTest` sets some keys "pinned" (pointing at memory the key does not own). The
//! port's `IterKey` always owns its bytes (docs/research/24 §4.6: pinning is ownership), so a
//! pinned key is set by copying and the test checks the same observable contents.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::cmp::Ordering;

use mantle_engine::db::dbformat::{
    InternalKey, InternalKeyComparator, IterKey, MAX_SEQUENCE_NUMBER, ParsedInternalKey,
    RangeTombstone, VALUE_TYPE_FOR_SEEK, ValueType, append_internal_key,
    find_short_internal_key_successor, find_shortest_internal_key_separator,
    pad_internal_key_with_min_timestamp, parse_internal_key,
    replace_internal_key_with_min_timestamp, strip_timestamp_from_internal_key,
    update_internal_key,
};
use mantle_engine::util::comparator::Comparator;
use mantle_engine::version::{
    ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH, ROCKSDB_VERSION_INT, make_version_int, version_ge,
};

fn ikey(user_key: &[u8], seq: u64, vt: ValueType) -> Vec<u8> {
    let mut encoded = Vec::new();
    append_internal_key(&mut encoded, &ParsedInternalKey::new(user_key, seq, vt)).unwrap();
    encoded
}

fn shorten(s: &[u8], l: &[u8]) -> Vec<u8> {
    find_shortest_internal_key_separator(Comparator::Bytewise, s, l).unwrap()
}

fn short_successor(s: &[u8]) -> Vec<u8> {
    find_short_internal_key_successor(Comparator::Bytewise, s).unwrap()
}

fn test_key(key: &[u8], seq: u64, vt: ValueType) {
    let encoded = ikey(key, seq, vt);
    let decoded = parse_internal_key(&encoded).unwrap();
    assert_eq!(key, decoded.user_key);
    assert_eq!(seq, decoded.sequence);
    assert_eq!(vt, decoded.value_type);

    assert!(parse_internal_key(b"bar").is_err());
}

#[test]
fn internal_key_encode_decode() {
    let keys: [&[u8]; 4] = [b"", b"k", b"hello", b"longggggggggggggggggggggg"];
    let seq: [u64; 12] = [
        1,
        2,
        3,
        (1 << 8) - 1,
        1 << 8,
        (1 << 8) + 1,
        (1 << 16) - 1,
        1 << 16,
        (1 << 16) + 1,
        (1 << 32) - 1,
        1 << 32,
        (1 << 32) + 1,
    ];
    for k in keys {
        for s in seq {
            test_key(k, s, ValueType::Value);
            test_key(b"hello", 1, ValueType::Deletion);
        }
    }
}

#[test]
fn internal_key_short_separator() {
    use ValueType::{Deletion, Value};
    // When user keys are same
    assert_eq!(
        ikey(b"foo", 100, Value),
        shorten(&ikey(b"foo", 100, Value), &ikey(b"foo", 99, Value))
    );
    assert_eq!(
        ikey(b"foo", 100, Value),
        shorten(&ikey(b"foo", 100, Value), &ikey(b"foo", 101, Value))
    );
    assert_eq!(
        ikey(b"foo", 100, Value),
        shorten(&ikey(b"foo", 100, Value), &ikey(b"foo", 100, Value))
    );
    assert_eq!(
        ikey(b"foo", 100, Value),
        shorten(&ikey(b"foo", 100, Value), &ikey(b"foo", 100, Deletion))
    );

    // When user keys are misordered
    assert_eq!(
        ikey(b"foo", 100, Value),
        shorten(&ikey(b"foo", 100, Value), &ikey(b"bar", 99, Value))
    );

    // When user keys are different, but correctly ordered
    assert_eq!(
        ikey(b"g", MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
        shorten(&ikey(b"foo", 100, Value), &ikey(b"hello", 200, Value))
    );

    assert_eq!(
        ikey(b"ABC2", MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
        shorten(
            &ikey(b"ABC1AAAAA", 100, Value),
            &ikey(b"ABC2ABB", 200, Value)
        )
    );

    assert_eq!(
        ikey(b"AAA2", MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
        shorten(&ikey(b"AAA1AAA", 100, Value), &ikey(b"AAA2AA", 200, Value))
    );

    assert_eq!(
        ikey(b"AAA2", MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
        shorten(&ikey(b"AAA1AAA", 100, Value), &ikey(b"AAA4", 200, Value))
    );

    assert_eq!(
        ikey(b"AAA1B", MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
        shorten(&ikey(b"AAA1AAA", 100, Value), &ikey(b"AAA2", 200, Value))
    );

    assert_eq!(
        ikey(b"AAA2", MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
        shorten(&ikey(b"AAA1AAA", 100, Value), &ikey(b"AAA2A", 200, Value))
    );

    assert_eq!(
        ikey(b"AAA1", 100, Value),
        shorten(&ikey(b"AAA1", 100, Value), &ikey(b"AAA2", 200, Value))
    );

    // When start user key is prefix of limit user key
    assert_eq!(
        ikey(b"foo", 100, Value),
        shorten(&ikey(b"foo", 100, Value), &ikey(b"foobar", 200, Value))
    );

    // When limit user key is prefix of start user key
    assert_eq!(
        ikey(b"foobar", 100, Value),
        shorten(&ikey(b"foobar", 100, Value), &ikey(b"foo", 200, Value))
    );
}

#[test]
fn internal_key_shortest_successor() {
    assert_eq!(
        ikey(b"g", MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
        short_successor(&ikey(b"foo", 100, ValueType::Value))
    );
    assert_eq!(
        ikey(b"\xff\xff", 100, ValueType::Value),
        short_successor(&ikey(b"\xff\xff", 100, ValueType::Value))
    );
}

#[test]
fn iter_key_operation() {
    let mut k = IterKey::new();
    let p = b"abcdefghijklmnopqrstuvwxyz";
    let q = b"0123456789";

    assert_eq!(k.user_key(), b"");

    k.trim_append(0, &p[..3]).unwrap();
    assert_eq!(k.user_key(), b"abc");

    k.trim_append(1, &p[..3]).unwrap();
    assert_eq!(k.user_key(), b"aabc");

    k.trim_append(0, &p[..26]).unwrap();
    assert_eq!(k.user_key(), b"abcdefghijklmnopqrstuvwxyz");

    k.trim_append(26, &q[..10]).unwrap();
    assert_eq!(k.user_key(), b"abcdefghijklmnopqrstuvwxyz0123456789");

    k.trim_append(36, &q[..1]).unwrap();
    assert_eq!(k.user_key(), b"abcdefghijklmnopqrstuvwxyz01234567890");

    k.trim_append(26, &q[..1]).unwrap();
    assert_eq!(k.user_key(), b"abcdefghijklmnopqrstuvwxyz0");

    // Size going up, memory allocation is triggered
    k.trim_append(27, &p[..26]).unwrap();
    assert_eq!(
        k.user_key(),
        b"abcdefghijklmnopqrstuvwxyz0abcdefghijklmnopqrstuvwxyz"
    );
}

/// `IterKeySwapTest::PopulateKey`: a key of `key_len` bytes of `fill`, optionally moved to the
/// timestamp-padding path; returns the key the IterKey then holds.
fn populate_key(
    k: &mut IterKey,
    key_len: usize,
    fill: u8,
    copy: bool,
    use_secondary: bool,
) -> Vec<u8> {
    let base = vec![fill; key_len];
    if !copy {
        // Pinned in RocksDB; the port copies (see the file header).
        k.set_user_key(&base);
        return base;
    }
    k.set_user_key(&base);
    if use_secondary {
        let ts_sz = 8;
        // Keep 1 byte from the existing key, append the rest + timestamp.
        let suffix = &base[1..];
        k.trim_append_with_timestamp(1, suffix, ts_sz).unwrap();
        let mut expected = base.clone();
        expected.extend(std::iter::repeat_n(0u8, ts_sz));
        return expected;
    }
    base
}

#[test]
fn iter_key_swap_test_swap_and_destroy() {
    for a_key_len in [10, 50] {
        for a_copy in [false, true] {
            for a_use_secondary in [false, true] {
                for b_key_len in [10, 50] {
                    for b_copy in [false, true] {
                        for b_use_secondary in [false, true] {
                            let mut a = IterKey::new();
                            let expected_a =
                                populate_key(&mut a, a_key_len, b'a', a_copy, a_use_secondary);
                            assert_eq!(a.user_key(), expected_a);
                            let expected_b;
                            {
                                let mut b = IterKey::new();
                                expected_b =
                                    populate_key(&mut b, b_key_len, b'b', b_copy, b_use_secondary);
                                assert_eq!(b.user_key(), expected_b);

                                a.swap(&mut b);

                                // After swap: a has b's old data, b has a's old data.
                                assert_eq!(a.user_key(), expected_b);
                                assert_eq!(b.user_key(), expected_a);
                            } // b destroyed here -- must not corrupt a's data

                            // a must still hold valid data after b's destruction.
                            assert_eq!(a.user_key(), expected_b);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn iter_key_with_timestamp_operation() {
    let mut k = IterKey::new();
    k.set_user_key(b"");
    let p = b"abcdefghijklmnopqrstuvwxyz";
    let q = b"0123456789";

    assert_eq!(k.user_key(), b"");

    let ts_sz = 8;
    let min_timestamp = vec![0u8; ts_sz];
    let with_ts = |s: &[u8]| [s, &min_timestamp].concat();
    k.trim_append_with_timestamp(0, &p[..3], ts_sz).unwrap();
    assert_eq!(k.user_key(), with_ts(b"abc"));

    k.trim_append_with_timestamp(1, &p[..3], ts_sz).unwrap();
    assert_eq!(k.user_key(), with_ts(b"aabc"));

    k.trim_append_with_timestamp(0, &p[..26], ts_sz).unwrap();
    assert_eq!(k.user_key(), with_ts(b"abcdefghijklmnopqrstuvwxyz"));

    k.trim_append_with_timestamp(26, &q[..10], ts_sz).unwrap();
    assert_eq!(
        k.user_key(),
        with_ts(b"abcdefghijklmnopqrstuvwxyz0123456789")
    );

    k.trim_append_with_timestamp(36, &q[..1], ts_sz).unwrap();
    assert_eq!(
        k.user_key(),
        with_ts(b"abcdefghijklmnopqrstuvwxyz01234567890")
    );

    k.trim_append_with_timestamp(26, &q[..1], ts_sz).unwrap();
    assert_eq!(k.user_key(), with_ts(b"abcdefghijklmnopqrstuvwxyz0"));

    k.trim_append_with_timestamp(27, &p[..26], ts_sz).unwrap();
    assert_eq!(
        k.user_key(),
        with_ts(b"abcdefghijklmnopqrstuvwxyz0abcdefghijklmnopqrstuvwxyz")
    );
    // IterKey holds an internal key, the last 8 bytes hold the key footer, the
    // timestamp is expected to be added before the key footer.
    let key_without_ts = b"keywithoutts";
    k.set_internal_key(&[&key_without_ts[..], &min_timestamp, b"internal"].concat());

    assert_eq!(
        k.internal_key(),
        [&key_without_ts[..], &min_timestamp, b"internal"].concat()
    );
    let mid = |a: &[u8], b: &[u8]| [a, &min_timestamp, b].concat();
    k.trim_append_with_timestamp(0, &p[..10], ts_sz).unwrap();
    assert_eq!(k.internal_key(), mid(b"ab", b"cdefghij"));

    k.trim_append_with_timestamp(1, &p[..8], ts_sz).unwrap();
    assert_eq!(k.internal_key(), mid(b"a", b"abcdefgh"));

    k.trim_append_with_timestamp(9, &p[..3], ts_sz).unwrap();
    assert_eq!(k.internal_key(), mid(b"aabc", b"defghabc"));

    k.trim_append_with_timestamp(10, &q[..10], ts_sz).unwrap();
    assert_eq!(k.internal_key(), mid(b"aabcdefgha01", b"23456789"));

    k.trim_append_with_timestamp(20, &q[..1], ts_sz).unwrap();
    assert_eq!(k.internal_key(), mid(b"aabcdefgha012", b"34567890"));

    k.trim_append_with_timestamp(21, &p[..26], ts_sz).unwrap();
    assert_eq!(
        k.internal_key(),
        mid(b"aabcdefgha01234567890abcdefghijklmnopqr", b"stuvwxyz")
    );
}

#[test]
fn update_internal_key_test() {
    let user_key = b"abcdefghijklmnopqrstuvwxyz";
    let new_seq = 0x123456;
    let new_val_type = ValueType::Deletion;

    let mut ikey_buf = Vec::new();
    append_internal_key(
        &mut ikey_buf,
        &ParsedInternalKey::new(user_key, 100, ValueType::Value),
    )
    .unwrap();
    let ikey_size = ikey_buf.len();
    update_internal_key(&mut ikey_buf, new_seq, new_val_type).unwrap();
    assert_eq!(ikey_size, ikey_buf.len());

    let decoded = parse_internal_key(&ikey_buf).unwrap();
    assert_eq!(user_key, decoded.user_key);
    assert_eq!(new_seq, decoded.sequence);
    assert_eq!(new_val_type, decoded.value_type);
}

#[test]
fn range_tombstone_serialize_end_key() {
    let t = RangeTombstone::new(b"a", b"b", 2);
    let k = InternalKey::new(b"b", 3, ValueType::Value).unwrap();
    let cmp = InternalKeyComparator::new(Comparator::Bytewise);
    assert_eq!(
        cmp.compare(t.serialize_end_key().unwrap().encode(), k.encode()),
        Ordering::Less
    );
}

#[test]
fn pad_internal_key_with_min_timestamp_test() {
    let orig_user_key = b"foo";
    let orig_internal_key = ikey(orig_user_key, 100, ValueType::Value);
    let ts_sz = 8;

    let mut key_buf = Vec::new();
    pad_internal_key_with_min_timestamp(&mut key_buf, &orig_internal_key, ts_sz).unwrap();
    let key_with_timestamp = parse_internal_key(&key_buf).unwrap();

    let min_timestamp = vec![0u8; ts_sz];
    assert_eq!(
        [&orig_user_key[..], &min_timestamp].concat(),
        key_with_timestamp.user_key
    );
    assert_eq!(100, key_with_timestamp.sequence);
    assert_eq!(ValueType::Value, key_with_timestamp.value_type);
}

#[test]
fn strip_timestamp_from_internal_key_test() {
    let ts_sz = 8;
    let timestamp = vec![0u8; ts_sz];
    let orig_user_key = [&b"foo"[..], &timestamp].concat();
    let orig_internal_key = ikey(&orig_user_key, 100, ValueType::Value);

    let mut key_buf = Vec::new();
    strip_timestamp_from_internal_key(&mut key_buf, &orig_internal_key, ts_sz).unwrap();
    let key_without_timestamp = parse_internal_key(&key_buf).unwrap();

    assert_eq!(b"foo", key_without_timestamp.user_key);
    assert_eq!(100, key_without_timestamp.sequence);
    assert_eq!(ValueType::Value, key_without_timestamp.value_type);
}

#[test]
fn replace_internal_key_with_min_timestamp_test() {
    let ts_sz = 8;
    let orig_user_key = [&b"foo"[..], &[1u8; 8]].concat();
    let orig_internal_key = ikey(&orig_user_key, 100, ValueType::Value);

    let mut key_buf = Vec::new();
    replace_internal_key_with_min_timestamp(&mut key_buf, &orig_internal_key, ts_sz).unwrap();
    let new_key = parse_internal_key(&key_buf).unwrap();

    let min_timestamp = vec![0u8; ts_sz];
    let ukey_diff_offset = new_key
        .user_key
        .iter()
        .zip(&orig_user_key)
        .take_while(|(a, b)| a == b)
        .count();
    assert_eq!(
        min_timestamp,
        &new_key.user_key[ukey_diff_offset..ukey_diff_offset + ts_sz]
    );
    assert_eq!(orig_user_key.len(), new_key.user_key.len());
    assert_eq!(100, new_key.sequence);
    assert_eq!(ValueType::Value, new_key.value_type);
}

/// `RocksdbVersionTest.Version`: the C++ checks its preprocessor macros, several with
/// `static_assert`; here the same comparisons run on the constants and functions.
#[test]
fn rocksdb_version_test_version() {
    let (ma, mi, pa) = (
        i64::from(ROCKSDB_MAJOR),
        i64::from(ROCKSDB_MINOR),
        i64::from(ROCKSDB_PATCH),
    );
    assert!(ma > 0);
    assert!(mi >= 0);
    assert!(pa >= 0);
    assert!(ma < 1000);
    assert!(mi < 1000);
    assert!(pa < 1000);
    assert_eq!(make_version_int(123, 456, 789), 123_456_789);
    const { assert!(ROCKSDB_VERSION_INT > 9_999_999) };
    const { assert!(ROCKSDB_VERSION_INT < 99_999_999) };
    assert!(version_ge(9, 8, 7));
    assert!(version_ge(ma, mi, pa));
    assert!(version_ge(ma, mi, pa - 1));
    assert!(version_ge(ma, mi, pa - 100));
    assert!(version_ge(ma, mi - 1, pa + 1));
    assert!(version_ge(ma - 1, mi + 1, pa + 1));
    assert!(!version_ge(ma, mi, pa + 1));
    assert!(!version_ge(ma, mi, pa + 100));
    assert!(!version_ge(ma, mi + 1, pa - 1));
    assert!(!version_ge(ma + 1, mi - 1, pa - 1));
    // More typical usage
    assert!(version_ge(ma, mi, pa));
    assert!(!version_ge(ma, mi, pa + 1));
}
