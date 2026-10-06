//! table/table_test.cc's `FooterTests` and `LegacyFormatRejectionTests`, ported test for test,
//! with the port's own tests of the footer preconditions RocksDB only asserts and of
//! `IndexValue`'s delta encoding.
//!
//! Differences from the C++ harness: RocksDB draws `FooterTests`' sizes from a thread-local
//! generator seeded per thread; here they come from `Random` seeded by `test::RandomSeed`, so every
//! run is the same run. `FooterTests`' plain-table half waits for phase P20, until which plain
//! tables are refused (docs/research/24 §1.9); the refusal is tested instead.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[path = "support/random.rs"]
#[allow(dead_code)]
mod random;

use mantle_engine::table::format::{
    BLOCK_BASED_TABLE_MAGIC_NUMBER, BLOCK_TRAILER_SIZE, BlockHandle, ChecksumType,
    FOOTER_ENCODED_LENGTH, FOOTER_VERSION0_ENCODED_LENGTH, Footer, IndexValue,
    LATEST_BBT_FORMAT_VERSION, LEGACY_BLOCK_BASED_TABLE_MAGIC_NUMBER,
    MIN_SUPPORTED_BBT_FORMAT_VERSION, PLAIN_TABLE_MAGIC_NUMBER, build_footer,
    format_version_uses_context_checksum, format_version_uses_index_handle_in_footer,
};
use mantle_engine::{Error, Malformed};
use random::{Random, random_seed};

/// `GetSupportedChecksums` [R test_util/testutil.cc]: every checksum type.
const CHECKSUMS: [ChecksumType; 5] = [
    ChecksumType::NoChecksum,
    ChecksumType::Crc32c,
    ChecksumType::XxHash,
    ChecksumType::XxHash64,
    ChecksumType::Xxh3,
];

fn put_fixed32(dst: &mut [u8], value: u32) {
    dst[..4].copy_from_slice(&value.to_le_bytes());
}

fn put_fixed64(dst: &mut [u8], value: u64) {
    dst[..8].copy_from_slice(&value.to_le_bytes());
}

/// `TEST(TableTest, FooterTests)`, its block-based half: every checksum type at every format
/// version written, a footer built and decoded again; from format_version 6 the footer's checksum
/// carries its offset, and a metaindex of 4 GiB or more cannot be written.
#[test]
fn footer_tests() {
    let mut r = Random::new(random_seed());
    let data_size = (1u64 << r.uniform(40)) + u64::from(r.uniform(100));
    let index_size = u64::from(r.uniform(1_000_000_000));
    let metaindex_size = u64::from(r.uniform(1_000_000));
    let index = BlockHandle::new(data_size + 5, index_size);
    let meta_index = BlockHandle::new(data_size + index_size + 2 * 5, metaindex_size);
    let footer_offset = data_size + metaindex_size + index_size + 3 * 5;
    let base_context_checksum = 123_456_789u32;
    for t in CHECKSUMS {
        for fv in MIN_SUPPORTED_BBT_FORMAT_VERSION..=LATEST_BBT_FORMAT_VERSION {
            let maybe_bcc = if format_version_uses_context_checksum(fv) {
                base_context_checksum
            } else {
                0
            };
            let footer = build_footer(fv, footer_offset, t, meta_index, index, maybe_bcc).unwrap();
            let decoded = Footer::decode_from(&footer, footer_offset, None).unwrap();
            assert_eq!(decoded.table_magic_number, BLOCK_BASED_TABLE_MAGIC_NUMBER);
            assert_eq!(decoded.checksum_type, t);
            assert_eq!(decoded.metaindex_handle, meta_index);
            if format_version_uses_index_handle_in_footer(fv) {
                assert_eq!(decoded.index_handle, index);
            }
            assert_eq!(decoded.format_version, fv);
            assert_eq!(decoded.block_trailer_size, BLOCK_TRAILER_SIZE);
            if format_version_uses_context_checksum(fv) {
                assert_eq!(decoded.base_context_checksum, base_context_checksum);
                // A bad offset fails the footer's checksum.
                assert!(Footer::decode_from(&footer, footer_offset - 1, None).is_err());
            } else {
                assert_eq!(decoded.base_context_checksum, 0);
            }
            // A metaindex of 4 GiB or more fails only the new footer.
            let big_metaindex_size = 0x1_0000_0007u64;
            let big_footer_offset = data_size + big_metaindex_size + index_size + 3 * 5;
            let big_metaindex =
                BlockHandle::new(data_size + index_size + 2 * 5, big_metaindex_size);
            assert_eq!(
                build_footer(fv, big_footer_offset, t, big_metaindex, index, maybe_bcc).is_ok(),
                !format_version_uses_context_checksum(fv)
            );
        }
    }
}

/// `TEST(TableTest, LegacyFormatRejectionTests)`: LevelDB's magic number, and format versions 0
/// and 1 under the block-based magic, are refused.
#[test]
fn legacy_format_rejection_tests() {
    // The legacy block-based magic number: not supported, with how to upgrade.
    let mut fake = [0u8; FOOTER_VERSION0_ENCODED_LENGTH];
    let at = fake.len() - 8;
    put_fixed64(&mut fake[at..], LEGACY_BLOCK_BASED_TABLE_MAGIC_NUMBER);
    match Footer::decode_from(&fake, 0, None) {
        Err(Error::Unsupported { feature, .. }) => {
            assert!(feature.contains("format_version 2"), "{feature}");
            assert!(feature.contains("full compaction"), "{feature}");
        }
        other => panic!("{other:?}"),
    }
    // format_version 1 and 0 with the new magic number: corruption naming the format version.
    for version in [1u32, 0] {
        let mut fake = [0u8; FOOTER_ENCODED_LENGTH];
        fake[0] = ChecksumType::Crc32c.to_byte();
        let part3 = fake.len() - 12;
        put_fixed32(&mut fake[part3..], version);
        put_fixed64(&mut fake[part3 + 4..], BLOCK_BASED_TABLE_MAGIC_NUMBER);
        assert_eq!(
            Footer::decode_from(&fake, 0, None),
            Err(Error::Corruption {
                what: "footer format_version",
                why: Malformed::UnknownVersion(u64::from(version)),
            })
        );
    }
}

/// Plain tables are refused until P20, where `FooterTests`' plain half is ported; and a footer
/// whose magic number is not the one enforced is corruption.
#[test]
fn plain_tables_wait_for_their_phase_and_magic_is_enforced() {
    let mut fake = [0u8; FOOTER_ENCODED_LENGTH];
    let at = fake.len() - 8;
    put_fixed64(&mut fake[at..], PLAIN_TABLE_MAGIC_NUMBER);
    assert!(matches!(
        Footer::decode_from(&fake, 0, None),
        Err(Error::Unsupported { .. })
    ));
    let footer = build_footer(
        5,
        100,
        ChecksumType::Crc32c,
        BlockHandle::new(10, 85),
        BlockHandle::new(1, 2),
        0,
    )
    .unwrap();
    assert!(Footer::decode_from(&footer, 100, Some(BLOCK_BASED_TABLE_MAGIC_NUMBER)).is_ok());
    assert_eq!(
        Footer::decode_from(&footer, 100, Some(PLAIN_TABLE_MAGIC_NUMBER)),
        Err(Error::Corruption {
            what: "table magic number",
            why: Malformed::BadMagic,
        })
    );
}

/// The preconditions RocksDB's `FooterBuilder::Build` only asserts are refused: a base context
/// checksum where none belongs, none where one must be, and a metaindex that does not end at the
/// footer; and a footer shorter than the shortest is corruption, as is a format_version 6 footer
/// whose metaindex would start before the file.
#[test]
fn footer_preconditions_are_errors_not_asserts() {
    let meta = BlockHandle::new(100, 50);
    let footer_offset = 155;
    assert!(matches!(
        build_footer(
            5,
            footer_offset,
            ChecksumType::Crc32c,
            meta,
            BlockHandle::NULL,
            7
        ),
        Err(Error::InvalidArgument { .. })
    ));
    assert!(matches!(
        build_footer(
            6,
            footer_offset,
            ChecksumType::Crc32c,
            meta,
            BlockHandle::NULL,
            0
        ),
        Err(Error::InvalidArgument { .. })
    ));
    assert!(matches!(
        build_footer(
            6,
            footer_offset + 1,
            ChecksumType::Crc32c,
            meta,
            BlockHandle::NULL,
            7
        ),
        Err(Error::InvalidArgument { .. })
    ));
    assert!(matches!(
        build_footer(
            8,
            footer_offset,
            ChecksumType::Crc32c,
            meta,
            BlockHandle::NULL,
            7
        ),
        Err(Error::Unsupported { .. })
    ));
    assert_eq!(
        Footer::decode_from(&[0u8; FOOTER_VERSION0_ENCODED_LENGTH - 1], 0, None),
        Err(Error::Corruption {
            what: "table footer",
            why: Malformed::Truncated,
        })
    );
    // A metaindex as large as the footer's offset would start before the file.
    let footer =
        build_footer(6, 155, ChecksumType::NoChecksum, meta, BlockHandle::NULL, 7).unwrap();
    let decoded = Footer::decode_from(&footer, 155, None).unwrap();
    assert_eq!(decoded.metaindex_handle, meta);
    let mut lying = footer;
    // Part 2's metaindex size field, at checksum type (1) + extended magic (4) + checksum (4) +
    // base context checksum (4); with no checksum the stored value is the context modifier only,
    // which does not cover the size.
    put_fixed32(&mut lying[13..], 1_000);
    assert_eq!(
        Footer::decode_from(&lying, 155, None),
        Err(Error::Corruption {
            what: "footer metaindex size",
            why: Malformed::OutOfRange,
        })
    );
}

/// A handle encodes as two varints and decodes back; one cut short is corruption.
#[test]
fn block_handles_round_trip() {
    for (offset, size) in [(0, 0), (1, 2), (u64::MAX, u64::MAX), (1 << 40, 4096)] {
        let h = BlockHandle::new(offset, size);
        let mut v = Vec::new();
        h.encode_to(&mut v);
        assert_eq!(h.encoded().as_slice(), v.as_slice());
        let mut input = v.as_slice();
        assert_eq!(BlockHandle::decode_from(&mut input).unwrap(), h);
        assert!(input.is_empty());
        let mut short = &v[..v.len() - 1];
        assert!(BlockHandle::decode_from(&mut short).is_err());
    }
    assert!(BlockHandle::NULL.is_null());
}

/// An index value with a previous handle stores only its size's difference, its block just past
/// the previous one and its trailer; a value whose block is not the next one cannot be written,
/// and a stored difference that would make a negative size is corruption.
#[test]
fn index_values_delta_encode_against_the_previous_block() {
    let previous = BlockHandle::new(1000, 300);
    let next = BlockHandle::new(1000 + 300 + BLOCK_TRAILER_SIZE, 280);
    let key = b"first key";
    for have_first_key in [false, true] {
        for prev in [None, Some(&previous)] {
            let v = IndexValue {
                handle: next,
                first_internal_key: if have_first_key { key } else { &[] },
            };
            let mut dst = Vec::new();
            v.encode_to(&mut dst, have_first_key, prev).unwrap();
            let mut input = dst.as_slice();
            assert_eq!(
                IndexValue::decode_from(&mut input, have_first_key, prev).unwrap(),
                v
            );
            assert!(input.is_empty());
        }
    }
    let elsewhere = IndexValue {
        handle: BlockHandle::new(5, 280),
        first_internal_key: &[],
    };
    assert!(matches!(
        elsewhere.encode_to(&mut Vec::new(), false, Some(&previous)),
        Err(Error::InvalidArgument { .. })
    ));
    // A difference of −301 from a size of 300.
    let mut input: &[u8] = &[0xd9, 0x04];
    assert_eq!(
        IndexValue::decode_from(&mut input, false, Some(&previous)),
        Err(Error::Corruption {
            what: "delta-encoded index value",
            why: Malformed::OutOfRange,
        })
    );
}
