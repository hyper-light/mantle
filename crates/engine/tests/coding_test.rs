//! RocksDB's util/coding_test.cc, test for test: its 19 `TEST(Coding, …)` cases with the same
//! literals. Where RocksDB checks for `nullptr` or `false`, these check for an error; where it
//! reads from a raw pointer, these read from the slice after it.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use mantle_engine::util::coding::{
    decode_fixed16, decode_fixed32, decode_fixed64, get_length_prefixed_slice, get_varint32_ptr,
    get_varint64_ptr, put_fixed16, put_fixed32, put_fixed64, put_length_prefixed_slice,
    put_varint32, put_varint64, varint_length,
};
use mantle_engine::util::prefix_varint::{
    MAX_PREFIX_VARINT32_LENGTH, decode_prefix_varint32, decode_prefix_varint64,
    encode_prefix_varint32, encode_prefix_varint64, encode_prefix_varint64_min_bytes,
    get_prefix_varint32, get_prefix_varint32_ptr, get_prefix_varint64, get_prefix_varint64_ptr,
    prefix_varint32_addl_byte_count, prefix_varint32_length, prefix_varint64_addl_byte_count,
    prefix_varint64_length, put_prefix_varint32, put_prefix_varint64,
};

#[test]
fn fixed16() {
    let mut s = Vec::new();
    for v in 0..0xFFFFu16 {
        put_fixed16(&mut s, v);
    }
    let mut p: &[u8] = &s;
    for v in 0..0xFFFFu16 {
        let actual = decode_fixed16(p).unwrap();
        assert_eq!(v, actual);
        p = &p[2..];
    }
}

#[test]
fn fixed32() {
    let mut s = Vec::new();
    for v in 0..100_000u32 {
        put_fixed32(&mut s, v);
    }
    let mut p: &[u8] = &s;
    for v in 0..100_000u32 {
        let actual = decode_fixed32(p).unwrap();
        assert_eq!(v, actual);
        p = &p[4..];
    }
}

#[test]
fn fixed64() {
    let mut s = Vec::new();
    for power in 0..=63 {
        let v = 1u64 << power;
        put_fixed64(&mut s, v.wrapping_sub(1));
        put_fixed64(&mut s, v);
        put_fixed64(&mut s, v + 1);
    }
    let mut p: &[u8] = &s;
    for power in 0..=63 {
        let v = 1u64 << power;
        assert_eq!(v.wrapping_sub(1), decode_fixed64(p).unwrap());
        p = &p[8..];
        assert_eq!(v, decode_fixed64(p).unwrap());
        p = &p[8..];
        assert_eq!(v + 1, decode_fixed64(p).unwrap());
        p = &p[8..];
    }
}

/// The encoders write little-endian.
#[test]
fn encoding_output() {
    let mut dst = Vec::new();
    put_fixed32(&mut dst, 0x0403_0201);
    assert_eq!(4, dst.len());
    assert_eq!([0x01, 0x02, 0x03, 0x04], dst[..]);

    dst.clear();
    put_fixed64(&mut dst, 0x0807_0605_0403_0201);
    assert_eq!(8, dst.len());
    assert_eq!([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08], dst[..]);
}

#[test]
fn varint32() {
    let mut s = Vec::new();
    for i in 0..(32 * 32u32) {
        let v = (i / 32) << (i % 32);
        put_varint32(&mut s, v);
    }
    let mut p: &[u8] = &s;
    for i in 0..(32 * 32u32) {
        let expected = (i / 32) << (i % 32);
        let (actual, n) = get_varint32_ptr(p).unwrap();
        assert_eq!(expected, actual);
        assert_eq!(varint_length(u64::from(actual)), n);
        p = &p[n..];
    }
    assert!(p.is_empty());
}

#[test]
fn varint64() {
    let mut values = vec![0u64, 100, !0u64, !0u64 - 1];
    for k in 0..64 {
        // Values near powers of two.
        let power = 1u64 << k;
        values.push(power);
        values.push(power - 1);
        values.push(power + 1);
    }

    let mut s = Vec::new();
    for &v in &values {
        put_varint64(&mut s, v);
    }

    let mut p: &[u8] = &s;
    for &v in &values {
        assert!(!p.is_empty());
        let (actual, n) = get_varint64_ptr(p).unwrap();
        assert_eq!(v, actual);
        assert_eq!(varint_length(actual), n);
        p = &p[n..];
    }
    assert!(p.is_empty());
}

#[test]
fn varint32_overflow() {
    let input = b"\x81\x82\x83\x84\x85\x11";
    assert!(get_varint32_ptr(input).is_err());
}

#[test]
fn varint32_truncation() {
    let large_value = (1u32 << 31) + 100;
    let mut s = Vec::new();
    put_varint32(&mut s, large_value);
    for len in 0..s.len() - 1 {
        assert!(get_varint32_ptr(&s[..len]).is_err());
    }
    let (result, _) = get_varint32_ptr(&s).unwrap();
    assert_eq!(large_value, result);
}

#[test]
fn varint64_overflow() {
    let input = b"\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11";
    assert!(get_varint64_ptr(input).is_err());
}

#[test]
fn varint64_truncation() {
    let large_value = (1u64 << 63) + 100;
    let mut s = Vec::new();
    put_varint64(&mut s, large_value);
    for len in 0..s.len() - 1 {
        assert!(get_varint64_ptr(&s[..len]).is_err());
    }
    let (result, _) = get_varint64_ptr(&s).unwrap();
    assert_eq!(large_value, result);
}

#[test]
fn strings() {
    let mut s = Vec::new();
    put_length_prefixed_slice(&mut s, b"").unwrap();
    put_length_prefixed_slice(&mut s, b"foo").unwrap();
    put_length_prefixed_slice(&mut s, b"bar").unwrap();
    put_length_prefixed_slice(&mut s, &[b'x'; 200]).unwrap();

    let mut input: &[u8] = &s;
    assert_eq!(b"", get_length_prefixed_slice(&mut input).unwrap());
    assert_eq!(b"foo", get_length_prefixed_slice(&mut input).unwrap());
    assert_eq!(b"bar", get_length_prefixed_slice(&mut input).unwrap());
    assert_eq!(
        &[b'x'; 200][..],
        get_length_prefixed_slice(&mut input).unwrap()
    );
    assert!(input.is_empty());
}

struct Case<T> {
    value: T,
    length: usize,
    encoded: &'static [u8],
}

const PREFIX_VARINT32_TEST_CASES: &[Case<u32>] = &[
    Case {
        value: 0,
        length: 1,
        encoded: &[0x01],
    },
    Case {
        value: 1,
        length: 1,
        encoded: &[0x03],
    },
    Case {
        value: 127,
        length: 1,
        encoded: &[0xFF],
    },
    Case {
        value: 128,
        length: 2,
        encoded: &[0x02, 0x02],
    },
    Case {
        value: 255,
        length: 2,
        encoded: &[0xFE, 0x03],
    },
    Case {
        value: 16383,
        length: 2,
        encoded: &[0xFE, 0xFF],
    },
    Case {
        value: 16384,
        length: 3,
        encoded: &[0x04, 0x00, 0x02],
    },
    Case {
        value: (1 << 21) - 1,
        length: 3,
        encoded: &[0xFC, 0xFF, 0xFF],
    },
    Case {
        value: 1 << 21,
        length: 4,
        encoded: &[0x08, 0x00, 0x00, 0x02],
    },
    Case {
        value: (1 << 28) - 1,
        length: 4,
        encoded: &[0xF8, 0xFF, 0xFF, 0xFF],
    },
    Case {
        value: 1 << 28,
        length: 5,
        encoded: &[0x10, 0x00, 0x00, 0x00, 0x02],
    },
    Case {
        value: !0,
        length: 5,
        encoded: &[0xF0, 0xFF, 0xFF, 0xFF, 0x1F],
    },
];

const PREFIX_VARINT64_TEST_CASES: &[Case<u64>] = &[
    Case {
        value: 0,
        length: 1,
        encoded: &[0x01],
    },
    Case {
        value: 1,
        length: 1,
        encoded: &[0x03],
    },
    Case {
        value: 127,
        length: 1,
        encoded: &[0xFF],
    },
    Case {
        value: 128,
        length: 2,
        encoded: &[0x02, 0x02],
    },
    Case {
        value: 16383,
        length: 2,
        encoded: &[0xFE, 0xFF],
    },
    Case {
        value: 1 << 21,
        length: 4,
        encoded: &[0x08, 0x00, 0x00, 0x02],
    },
    Case {
        value: 1 << 28,
        length: 5,
        encoded: &[0x10, 0x00, 0x00, 0x00, 0x02],
    },
    Case {
        value: 1 << 35,
        length: 6,
        encoded: &[0x20, 0x00, 0x00, 0x00, 0x00, 0x02],
    },
    Case {
        value: 1 << 42,
        length: 7,
        encoded: &[0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02],
    },
    Case {
        value: 1 << 49,
        length: 8,
        encoded: &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02],
    },
    Case {
        value: (1 << 56) - 1,
        length: 8,
        encoded: &[0x80, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
    },
    Case {
        value: 1 << 56,
        length: 9,
        encoded: &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
    },
    Case {
        value: !0,
        length: 9,
        encoded: &[0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
    },
];

/// RocksDB's `PrefixVarintTraits<T>`: the prefix varint API of one width.
trait Traits: Copy + PartialEq + std::fmt::Debug + Default {
    fn length(self) -> usize;
    fn encode(self) -> Vec<u8>;
    fn put(dst: &mut Vec<u8>, v: Self);
    fn get_ptr(src: &[u8]) -> Option<(Self, usize)>;
    fn get(input: &mut &[u8]) -> Option<Self>;
    fn addl_byte_count(first: u8) -> Option<usize>;
    fn decode(first: u8, addl: &[u8]) -> Option<Self>;
}

impl Traits for u32 {
    fn length(self) -> usize {
        prefix_varint32_length(self)
    }
    fn encode(self) -> Vec<u8> {
        encode_prefix_varint32(self).as_bytes().to_vec()
    }
    fn put(dst: &mut Vec<u8>, v: Self) {
        put_prefix_varint32(dst, v);
    }
    fn get_ptr(src: &[u8]) -> Option<(Self, usize)> {
        get_prefix_varint32_ptr(src).ok()
    }
    fn get(input: &mut &[u8]) -> Option<Self> {
        get_prefix_varint32(input).ok()
    }
    fn addl_byte_count(first: u8) -> Option<usize> {
        prefix_varint32_addl_byte_count(first).ok()
    }
    fn decode(first: u8, addl: &[u8]) -> Option<Self> {
        decode_prefix_varint32(first, addl).ok()
    }
}

impl Traits for u64 {
    fn length(self) -> usize {
        prefix_varint64_length(self)
    }
    fn encode(self) -> Vec<u8> {
        encode_prefix_varint64(self).as_bytes().to_vec()
    }
    fn put(dst: &mut Vec<u8>, v: Self) {
        put_prefix_varint64(dst, v);
    }
    fn get_ptr(src: &[u8]) -> Option<(Self, usize)> {
        get_prefix_varint64_ptr(src).ok()
    }
    fn get(input: &mut &[u8]) -> Option<Self> {
        get_prefix_varint64(input).ok()
    }
    fn addl_byte_count(first: u8) -> Option<usize> {
        Some(prefix_varint64_addl_byte_count(first))
    }
    fn decode(first: u8, addl: &[u8]) -> Option<Self> {
        decode_prefix_varint64(first, addl).ok()
    }
}

fn assert_prefix_varint_round_trip<T: Traits>(cases: &[Case<T>]) {
    let mut encoded_values = Vec::new();
    for tc in cases {
        assert_eq!(tc.length, tc.value.length());
        assert_eq!(tc.encoded, &tc.value.encode()[..]);
        T::put(&mut encoded_values, tc.value);
    }

    let mut p: &[u8] = &encoded_values;
    for tc in cases {
        let (actual, n) = T::get_ptr(p).unwrap();
        assert_eq!(tc.value, actual);
        assert_eq!(tc.length, n);
        p = &p[n..];
    }
    assert!(p.is_empty());

    let mut input: &[u8] = &encoded_values;
    for tc in cases {
        assert_eq!(Some(tc.value), T::get(&mut input));
    }
    assert!(input.is_empty());
}

fn assert_prefix_varint_disk_read_api<T: Traits>(cases: &[Case<T>]) {
    for tc in cases {
        let encoded = tc.value.encode();
        assert_eq!(tc.length, encoded.len());

        let addl_byte_count = T::addl_byte_count(encoded[0]).unwrap();
        assert_eq!(tc.length - 1, addl_byte_count);

        assert_eq!(
            Some(tc.value),
            T::decode(encoded[0], &encoded[1..1 + addl_byte_count])
        );
        if addl_byte_count > 0 {
            assert_eq!(None, T::decode(encoded[0], &encoded[1..addl_byte_count]));
        }
    }
}

fn assert_prefix_varint_truncation<T: Traits>(value: T) {
    let encoded = value.encode();
    for len in 0..encoded.len() - 1 {
        assert!(T::get_ptr(&encoded[..len]).is_none());
    }
    let (actual, _) = T::get_ptr(&encoded).unwrap();
    assert_eq!(value, actual);
}

/// RocksDB's `EncodeInvalidPrefixVarint32Overflow`: a 5-byte form whose payload is 2^32.
fn encode_invalid_prefix_varint32_overflow() -> Vec<u8> {
    let encoded: u64 = (1 << 37) | 0x10;
    encoded.to_le_bytes()[..MAX_PREFIX_VARINT32_LENGTH].to_vec()
}

#[test]
fn prefix_varint32() {
    for tc in PREFIX_VARINT32_TEST_CASES {
        assert_eq!(tc.length, varint_length(u64::from(tc.value)));
    }
    assert_prefix_varint_round_trip(PREFIX_VARINT32_TEST_CASES);
}

#[test]
fn prefix_varint32_disk_read_api() {
    assert_prefix_varint_disk_read_api(PREFIX_VARINT32_TEST_CASES);
    // RocksDB's kInvalidPrefixVarint32AddlByteCount: an error here.
    for first in [0x00u8, 0x20, 0x40, 0x80] {
        assert!(prefix_varint32_addl_byte_count(first).is_err());
    }
}

#[test]
fn prefix_varint64() {
    for tc in PREFIX_VARINT64_TEST_CASES {
        let expected_length = if tc.value < (1u64 << 56) {
            varint_length(tc.value)
        } else {
            9
        };
        assert_eq!(tc.length, expected_length);
    }
    assert_prefix_varint_round_trip(PREFIX_VARINT64_TEST_CASES);
}

#[test]
fn prefix_varint64_disk_read_api() {
    assert_prefix_varint_disk_read_api(PREFIX_VARINT64_TEST_CASES);
    assert_eq!(8, prefix_varint64_addl_byte_count(b'\0'));
    assert_eq!(7, prefix_varint64_addl_byte_count(b'\x80'));
}

#[test]
fn prefix_varint32_overflow() {
    let input = encode_invalid_prefix_varint32_overflow();
    assert!(decode_prefix_varint32(input[0], &input[1..]).is_err());
    assert!(get_prefix_varint32_ptr(&input).is_err());
}

#[test]
fn prefix_varint32_truncation() {
    assert_prefix_varint_truncation(!0u32);
}

#[test]
fn prefix_varint64_truncation() {
    assert_prefix_varint_truncation(!0u64);
}

/// RocksDB's `TestPrefixVarint64MinimumBytes<kMinimumBytes>`.
fn test_prefix_varint64_minimum_bytes(minimum_bytes: usize, values: &[u64]) {
    for &value in values {
        let encoded = encode_prefix_varint64_min_bytes(value, minimum_bytes).unwrap();
        let buf = encoded.as_bytes();
        let natural_len = prefix_varint64_length(value);
        assert_eq!(
            buf.len(),
            natural_len.max(minimum_bytes),
            "value={value} minimum_bytes={minimum_bytes}"
        );

        // The split-decode API.
        let addl = prefix_varint64_addl_byte_count(buf[0]);
        assert_eq!(addl + 1, buf.len());
        assert_eq!(
            value,
            decode_prefix_varint64(buf[0], &buf[1..1 + addl]).unwrap()
        );

        // The pointer API.
        assert_eq!((value, buf.len()), get_prefix_varint64_ptr(buf).unwrap());

        // The slice API.
        let mut slice: &[u8] = buf;
        assert_eq!(value, get_prefix_varint64(&mut slice).unwrap());
        assert!(slice.is_empty());
    }
}

#[test]
fn prefix_varint64_improper_encoding() {
    // Values spanning each natural encoding length.
    let values = [
        0,
        1,
        63,
        127, // 1-byte natural
        128,
        255,
        16383, // 2-byte natural
        16384,
        (1u64 << 21) - 1, // 3-byte natural
        1 << 21,
        (1 << 28) - 1, // 4-byte natural
        1 << 28,
        (1 << 35) - 1, // 5-byte natural
        1 << 35,
        (1 << 42) - 1, // 6-byte natural
        1 << 42,
        (1 << 49) - 1, // 7-byte natural
        1 << 49,
        (1 << 56) - 1, // 8-byte natural
        1 << 56,
        !0, // 9-byte natural
    ];
    for minimum_bytes in 2..=8 {
        test_prefix_varint64_minimum_bytes(minimum_bytes, &values);
    }

    let check_bytes = |value: u64, minimum_bytes: usize, expected: &[u8]| {
        let encoded = encode_prefix_varint64_min_bytes(value, minimum_bytes).unwrap();
        assert_eq!(expected, encoded.as_bytes());
    };
    // value=0 with kMinimumBytes=2: (0 << 2) | (1 << 1) = 0x02
    check_bytes(0, 2, &[0x02, 0x00]);
    // value=0 with kMinimumBytes=3: (0 << 3) | (1 << 2) = 0x04
    check_bytes(0, 3, &[0x04, 0x00, 0x00]);
    // value=1 with kMinimumBytes=2: (1 << 2) | (1 << 1) = 0x06
    check_bytes(1, 2, &[0x06, 0x00]);
    // value=127 with kMinimumBytes=2: (127 << 2) | (1 << 1) = 0x01FE
    check_bytes(127, 2, &[0xFE, 0x01]);
    // value=0 with kMinimumBytes=8: (0 << 8) | (1 << 7) = 0x80
    check_bytes(0, 8, &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    // value=1 with kMinimumBytes=8: (1 << 8) | (1 << 7) = 0x0180
    check_bytes(1, 8, &[0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

    // RocksDB rejects kMinimumBytes >= 9 at compile time; the port refuses it at run time.
    assert!(encode_prefix_varint64_min_bytes(0, 9).is_err());
}
