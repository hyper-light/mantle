//! Table blocks' compression framing (`util::block_compression`) against the reference codecs:
//! the reference's blocks of the corpus (`tests/golden/{snappy,lz4,deflate}`, and ZSTD frames
//! from the reference library, the `zstd` oracle), framed as RocksDB frames a block, decode to
//! their input; the port's own blocks are framed as RocksDB's are and decode the same way. Then
//! the builder's choice to keep, reject or bypass a block, and the reader's refusals: a stated
//! size above the bound before any allocation, a size that disagrees with the codec's output, and
//! types RocksDB reserves or the port does not read.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[allow(dead_code)]
#[path = "support/corpus.rs"]
mod corpus;

use std::path::Path;

use mantle_engine::error::Error;
use mantle_engine::util::block_compression::{
    Compressed, CompressionOptions, CompressionType, compress_block, decompress_block,
    uncompressed_size,
};
use mantle_engine::util::coding::put_varint32;

fn golden(codec: &str) -> std::path::PathBuf {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden")).join(codec)
}

/// `body` after RocksDB's varint32 size prefix (`compress_format_version` 2).
fn framed(len: usize, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint32(&mut out, u32::try_from(len).unwrap());
    out.extend_from_slice(body);
    out
}

#[test]
fn the_references_blocks_decode_framed_as_rocksdb_frames_them() {
    let mut blocks = 0;
    for (name, input) in corpus::corpus() {
        let sz = std::fs::read(golden("snappy").join(format!("{name}.sz"))).unwrap();
        let lz4 = std::fs::read(golden("lz4").join(format!("{name}.lz4"))).unwrap();
        let zz = std::fs::read(golden("deflate").join(format!("{name}.zz"))).unwrap();
        let zstd = zstd::bulk::compress(&input, 3).unwrap();
        for (kind, block) in [
            (CompressionType::Snappy, sz),
            (CompressionType::Lz4, framed(input.len(), &lz4)),
            (CompressionType::Lz4hc, framed(input.len(), &lz4)),
            (CompressionType::Zlib, framed(input.len(), &zz)),
            (CompressionType::Zstd, framed(input.len(), &zstd)),
        ] {
            let out = decompress_block(kind, &block, &[], input.len()).unwrap();
            assert!(out == input, "{kind:?} {name}");
            blocks += 1;
        }
    }
    assert_eq!(blocks, 61 * 5);
}

#[test]
fn zstd_blocks_with_a_dictionary_decode() {
    let dict: Vec<u8> = corpus::corpus()
        .into_iter()
        .filter(|(n, _)| n.starts_with("text-"))
        .flat_map(|(_, d)| d)
        .take(16 * 1024)
        .collect();
    for (name, input) in corpus::corpus() {
        let mut c = zstd::bulk::Compressor::with_dictionary(3, &dict).unwrap();
        let frame = c.compress(&input).unwrap();
        let out = decompress_block(
            CompressionType::Zstd,
            &framed(input.len(), &frame),
            &dict,
            input.len(),
        )
        .unwrap();
        assert!(out == input, "{name}");
    }
}

#[test]
fn the_ports_blocks_are_framed_as_rocksdbs_and_decode() {
    let opts = CompressionOptions {
        max_compressed_bytes_per_kb: 1023,
        ..CompressionOptions::default()
    };
    let checked = CompressionOptions {
        checksum: true,
        level: Some(19),
        ..opts
    };
    let (mut kept, mut rejected, mut bypassed) = (0, 0, 0);
    for (name, input) in corpus::corpus() {
        for (kind, o, dict) in [
            (CompressionType::Snappy, opts, &b""[..]),
            (CompressionType::Lz4, opts, &b""[..]),
            (
                CompressionType::Lz4,
                opts,
                &b"the quick brown fox jumps over the lazy dog "[..],
            ),
            (CompressionType::Zlib, opts, &b""[..]),
            (CompressionType::Zstd, opts, &b""[..]),
            (CompressionType::Zstd, checked, &b""[..]),
        ] {
            match compress_block(kind, &input, dict, &o).unwrap() {
                Compressed::Kept(k, block) => {
                    assert_eq!(k, kind);
                    assert!(block.len() <= input.len() * 1023 / 1024, "{kind:?} {name}");
                    let (stated, body) = uncompressed_size(kind, &block).unwrap();
                    assert_eq!(stated, input.len() as u64);
                    if kind != CompressionType::Snappy {
                        // The varint32 prefix, then the codec's bytes.
                        assert_eq!(body.len() + varint_len(input.len()), block.len());
                    }
                    if kind == CompressionType::Zstd {
                        // The reference decodes the port's frame.
                        assert!(zstd::bulk::decompress(body, input.len()).unwrap() == input);
                    }
                    let out = decompress_block(kind, &block, dict, input.len()).unwrap();
                    assert!(out == input, "{kind:?} {name}");
                    kept += 1;
                }
                Compressed::Rejected => rejected += 1,
                Compressed::Bypassed => bypassed += 1,
            }
        }
    }
    eprintln!("{kept} kept, {rejected} rejected, {bypassed} bypassed");
    assert!(kept > 0 && rejected > 0 && bypassed > 0);
}

fn varint_len(n: usize) -> usize {
    let mut v = Vec::new();
    put_varint32(&mut v, n as u32);
    v.len()
}

/// The builder keeps a block only within `max_compressed_bytes_per_kb` of each 1024 bytes,
/// counting anything above 1023 as 1023, and bypasses compression when the bound leaves no room
/// past a size prefix's five bytes.
#[test]
fn keeps_rejects_and_bypasses_as_the_table_builder_does() {
    let same = vec![0x5Au8; 4096];
    let random: Vec<u8> = {
        let mut rng = corpus::Rng(3);
        (0..4096).map(|_| rng.next() as u8).collect()
    };
    let opts = CompressionOptions::default();
    assert!(matches!(
        compress_block(CompressionType::Zstd, &same, &[], &opts).unwrap(),
        Compressed::Kept(CompressionType::Zstd, _)
    ));
    for kind in [
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Zlib,
        CompressionType::Zstd,
    ] {
        assert_eq!(
            compress_block(kind, &random, &[], &opts).unwrap(),
            Compressed::Rejected,
            "{kind:?}"
        );
    }
    assert_eq!(
        compress_block(CompressionType::NoCompression, &same, &[], &opts).unwrap(),
        Compressed::Bypassed
    );
    // 6 bytes at 7/8 bound to 5: no room past a prefix.
    assert_eq!(
        compress_block(CompressionType::Lz4, &same[..6], &[], &opts).unwrap(),
        Compressed::Bypassed
    );
    // A limit of 1024 or more is 1023: a block that does not shrink is never kept.
    let lax = CompressionOptions {
        max_compressed_bytes_per_kb: 4096,
        ..opts
    };
    assert_eq!(
        compress_block(CompressionType::Lz4, &random, &[], &lax).unwrap(),
        Compressed::Rejected
    );
}

#[test]
fn a_stated_size_above_the_bound_is_refused_before_allocating() {
    // A prefix stating 2^63 bytes: RocksDB would allocate it.
    let mut block = Vec::new();
    mantle_engine::util::coding::put_varint64(&mut block, 1 << 63);
    block.extend_from_slice(b"anything");
    for kind in [
        CompressionType::Lz4,
        CompressionType::Zlib,
        CompressionType::Zstd,
    ] {
        assert!(matches!(
            decompress_block(kind, &block, &[], 1 << 20),
            Err(Error::LimitExceeded { .. })
        ));
    }
    let same = vec![7u8; 1 << 16];
    let Compressed::Kept(_, block) = compress_block(
        CompressionType::Snappy,
        &same,
        &[],
        &CompressionOptions::default(),
    )
    .unwrap() else {
        panic!("not kept")
    };
    assert!(matches!(
        decompress_block(CompressionType::Snappy, &block, &[], (1 << 16) - 1),
        Err(Error::LimitExceeded { .. })
    ));
}

#[test]
fn a_size_that_disagrees_with_the_codec_is_corruption() {
    let input = b"the quick brown fox jumps over the lazy dog, the quick brown fox".repeat(8);
    for kind in [
        CompressionType::Lz4,
        CompressionType::Zlib,
        CompressionType::Zstd,
    ] {
        let Compressed::Kept(_, block) =
            compress_block(kind, &input, &[], &CompressionOptions::default()).unwrap()
        else {
            panic!("{kind:?} not kept")
        };
        let (_, body) = uncompressed_size(kind, &block).unwrap();
        for stated in [input.len() - 1, input.len() + 1] {
            let wrong = framed(stated, body);
            assert!(
                matches!(
                    decompress_block(kind, &wrong, &[], 1 << 20),
                    Err(Error::Corruption { .. })
                ),
                "{kind:?} stated {stated}"
            );
        }
    }
}

#[test]
fn compression_types_as_rocksdb_names_them() {
    for byte in 0u8..=0x07 {
        assert_eq!(CompressionType::from_u8(byte).unwrap().as_u8(), byte);
    }
    for byte in 0x80u8..=0xFE {
        assert_eq!(
            CompressionType::from_u8(byte).unwrap(),
            CompressionType::Custom(byte)
        );
    }
    for byte in (0x08u8..=0x7F).chain([0xFF]) {
        assert!(matches!(
            CompressionType::from_u8(byte),
            Err(Error::Corruption { .. })
        ));
    }
    for kind in [
        CompressionType::BZip2,
        CompressionType::Xpress,
        CompressionType::Custom(0x80),
    ] {
        assert!(matches!(
            decompress_block(kind, b"\x05hello", &[], 16),
            Err(Error::Unsupported { .. })
        ));
    }
}
