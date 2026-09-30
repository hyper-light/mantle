//! RocksDB's util/crc32c_test.cc, test for test: its 8 `TEST(CRC, …)` cases with the same
//! literals, including folly's 3-way CRC vectors over the same FNV-filled buffer. RocksDB's
//! `Random` supplies the combine tests' strings; any bytes serve, since those tests compare two
//! ways of computing one CRC, and SplitMix64 supplies them here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]

mod common;

use common::SplitMix64;
use mantle_engine::util::crc32c::{self, crc32c_combine, unmask, value};

const BUFFER_SIZE: usize = 512 * 1024 * 8;

/// folly's `fnv64_buf`, as the test copies it: each byte is xored in as a *signed* char.
fn fnv64_buf(buf: &[u8]) -> u64 {
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for &b in buf {
        hash = hash.wrapping_add(
            (hash << 1)
                .wrapping_add(hash << 4)
                .wrapping_add(hash << 5)
                .wrapping_add(hash << 7)
                .wrapping_add(hash << 8)
                .wrapping_add(hash << 40),
        );
        hash ^= i64::from(b as i8) as u64;
    }
    hash
}

/// The test's `main`: a zero word, then each word the FNV hash of the word before it.
fn buffer() -> Vec<u8> {
    let mut buf = vec![0u8; BUFFER_SIZE];
    for i in (8..BUFFER_SIZE).step_by(8) {
        let h = fnv64_buf(&buf[i - 8..i]);
        buf[i..i + 8].copy_from_slice(&h.to_le_bytes());
    }
    buf
}

struct ExpectedResult {
    offset: usize,
    length: usize,
    crc32c: u32,
}

const EXPECTED_RESULTS: &[ExpectedResult] = &[
    // Zero-byte input
    ExpectedResult {
        offset: 0,
        length: 0,
        crc32c: !0,
    },
    // Small aligned inputs to test special cases in SIMD implementations
    ExpectedResult {
        offset: 8,
        length: 1,
        crc32c: 1_543_413_366,
    },
    ExpectedResult {
        offset: 8,
        length: 2,
        crc32c: 523_493_126,
    },
    ExpectedResult {
        offset: 8,
        length: 3,
        crc32c: 1_560_427_360,
    },
    ExpectedResult {
        offset: 8,
        length: 4,
        crc32c: 3_422_504_776,
    },
    ExpectedResult {
        offset: 8,
        length: 5,
        crc32c: 447_841_138,
    },
    ExpectedResult {
        offset: 8,
        length: 6,
        crc32c: 3_910_050_499,
    },
    ExpectedResult {
        offset: 8,
        length: 7,
        crc32c: 3_346_241_981,
    },
    // Small unaligned inputs
    ExpectedResult {
        offset: 9,
        length: 1,
        crc32c: 3_855_826_643,
    },
    ExpectedResult {
        offset: 10,
        length: 2,
        crc32c: 560_880_875,
    },
    ExpectedResult {
        offset: 11,
        length: 3,
        crc32c: 1_479_707_779,
    },
    ExpectedResult {
        offset: 12,
        length: 4,
        crc32c: 2_237_687_071,
    },
    ExpectedResult {
        offset: 13,
        length: 5,
        crc32c: 4_063_855_784,
    },
    ExpectedResult {
        offset: 14,
        length: 6,
        crc32c: 2_553_454_047,
    },
    ExpectedResult {
        offset: 15,
        length: 7,
        crc32c: 1_349_220_140,
    },
    // Larger inputs to test leftover chunks at the end of aligned blocks
    ExpectedResult {
        offset: 8,
        length: 8,
        crc32c: 627_613_930,
    },
    ExpectedResult {
        offset: 8,
        length: 9,
        crc32c: 2_105_929_409,
    },
    ExpectedResult {
        offset: 8,
        length: 10,
        crc32c: 2_447_068_514,
    },
    ExpectedResult {
        offset: 8,
        length: 11,
        crc32c: 863_807_079,
    },
    ExpectedResult {
        offset: 8,
        length: 12,
        crc32c: 292_050_879,
    },
    ExpectedResult {
        offset: 8,
        length: 13,
        crc32c: 1_411_837_737,
    },
    ExpectedResult {
        offset: 8,
        length: 14,
        crc32c: 2_614_515_001,
    },
    ExpectedResult {
        offset: 8,
        length: 15,
        crc32c: 3_579_076_296,
    },
    ExpectedResult {
        offset: 8,
        length: 16,
        crc32c: 2_897_079_161,
    },
    ExpectedResult {
        offset: 8,
        length: 17,
        crc32c: 675_168_386,
    },
    // Much larger inputs
    ExpectedResult {
        offset: 0,
        length: BUFFER_SIZE,
        crc32c: 2_096_790_750,
    },
    ExpectedResult {
        offset: 1,
        length: BUFFER_SIZE / 2,
        crc32c: 3_854_797_577,
    },
];

#[test]
fn standard_results() {
    // Original Fast_CRC32 tests, from RFC 3720 section B.4.
    let mut buf = [0u8; 32];
    assert_eq!(0x8a91_36aa, value(&buf));

    buf.fill(0xff);
    assert_eq!(0x62a8_ab43, value(&buf));

    for (i, b) in buf.iter_mut().enumerate() {
        *b = i as u8;
    }
    assert_eq!(0x46dd_794e, value(&buf));

    for (i, b) in buf.iter_mut().enumerate() {
        *b = 31 - i as u8;
    }
    assert_eq!(0x113f_db5c, value(&buf));

    let data: [u8; 48] = [
        0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00,
        0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00,
    ];
    assert_eq!(0xd996_3a56, value(&data));

    let buffer = buffer();
    // 3-way CRC-32C tests ported from folly. Test 1: single computation.
    for expected in EXPECTED_RESULTS {
        let result = value(&buffer[expected.offset..expected.offset + expected.length]);
        assert_eq!(!expected.crc32c, result);
    }
    // Test 2: stitching two computations.
    for expected in EXPECTED_RESULTS {
        let partial_length = expected.length / 2;
        let start = expected.offset;
        let partial_checksum = value(&buffer[start..start + partial_length]);
        let result = crc32c::extend(
            partial_checksum,
            &buffer[start + partial_length..start + expected.length],
        );
        assert_eq!(!expected.crc32c, result);
    }
}

#[test]
fn values() {
    assert_ne!(value(b"a"), value(b"foo"));
}

#[test]
fn extend() {
    assert_eq!(
        value(b"hello world"),
        crc32c::extend(value(b"hello "), b"world")
    );
}

#[test]
fn mask() {
    let crc = value(b"foo");
    assert_ne!(crc, crc32c::mask(crc));
    assert_ne!(crc, crc32c::mask(crc32c::mask(crc)));
    assert_eq!(crc, unmask(crc32c::mask(crc)));
    assert_eq!(crc, unmask(unmask(crc32c::mask(crc32c::mask(crc)))));
}

#[test]
fn crc32c_combine_basic_test() {
    let crc1 = value(b"hello ");
    let crc2 = value(b"world");
    let crc3 = value(b"hello world");
    assert_eq!(crc3, crc32c_combine(crc1, crc2, 5));
}

#[test]
fn crc32c_combine_order_matters_test() {
    let crc1 = value(b"hello ");
    let crc2 = value(b"world");
    let crc3 = value(b"hello world");
    assert_ne!(crc3, crc32c_combine(crc2, crc1, 6));
}

#[test]
fn crc32c_combine_full_cover_test() {
    let scale = 4 * 1024;
    let mut rnd = SplitMix64(301);
    let size_1 = 1024 * 1024;
    let s1 = rnd.bytes(size_1);
    let crc1 = value(&s1);
    for size_2 in 0..scale {
        let s2 = rnd.bytes(size_2);
        let crc2 = value(&s2);
        let crc1_2 = crc32c::extend(crc1, &s2);
        assert_eq!(crc1_2, crc32c_combine(crc1, crc2, size_2));
    }
}

#[test]
fn crc32c_combine_big_size_test() {
    let mut rnd = SplitMix64(301);
    let size_1 = 1024 * 1024;
    let s1 = rnd.bytes(size_1);
    let crc1 = value(&s1);
    let size_2 = 16 * 1024 * 1024 - 1;
    let s2 = rnd.bytes(size_2);
    let crc2 = value(&s2);
    let crc1_2 = crc32c::extend(crc1, &s2);
    assert_eq!(crc1_2, crc32c_combine(crc1, crc2, size_2));
}
