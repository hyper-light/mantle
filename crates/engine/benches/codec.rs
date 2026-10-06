//! Table blocks' ZSTD against the reference library (zstd 1.5.7 through the `zstd` crate, the
//! tests' oracle): compress and decompress of block-sized inputs as RocksDB compresses a table's
//! blocks, at its default level 3.
//!
//! `cargo bench -p mantle-engine --bench codec` takes blocks of 4 KiB (RocksDB's default
//! `block_size`) and 16 KiB from the corpus's text (`tests/support/corpus.rs`), and for each
//! size runs 59 rounds (Wilks 95/95), the two sides alternating which goes first. A round
//! compresses then decompresses every block. The reference reuses one compression context, one
//! decompression context and one output buffer, as RocksDB's working areas do. The port is
//! called as the table code calls it (`util::block_compression`). Both decoders read the
//! reference's frames; each encoder's output size is printed beside its time. It prints
//! `side op block_size round ns_per_block allocations output_bytes` per round.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros
)]

#[allow(dead_code)]
#[path = "../tests/support/corpus.rs"]
mod corpus;

use std::time::Instant;

use hyper_measure::alloc;
use mantle_engine::util::block_compression::{
    Compressed, CompressionOptions, CompressionType, compress_block, decompress_block,
};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Rounds a size (Wilks: the extremes of 59 bound the 95th percentile at 95%).
const ROUNDS: usize = 59;

/// Blocks of `size` bytes cut from the corpus's text inputs, in order.
fn blocks(size: usize) -> Vec<Vec<u8>> {
    let text: Vec<u8> = corpus::corpus()
        .into_iter()
        .filter(|(n, _)| n.starts_with("text-"))
        .flat_map(|(_, d)| d)
        .collect();
    text.chunks_exact(size).map(<[u8]>::to_vec).collect()
}

fn main() {
    let opts = CompressionOptions::default();
    for size in [4096usize, 16384] {
        let input = blocks(size);
        let mut cctx = zstd::bulk::Compressor::new(3).unwrap();
        let mut dctx = zstd::bulk::Decompressor::new().unwrap();
        let mut out = Vec::with_capacity(zstd::zstd_safe::compress_bound(size) + 8);
        let mut plain = vec![0u8; size];
        // The port's frames, framed as a table stores them, for its decompression.
        let ours: Vec<Vec<u8>> = input
            .iter()
            .map(
                |b| match compress_block(CompressionType::Zstd, b, &[], &opts).unwrap() {
                    Compressed::Kept(_, c) => c,
                    other => panic!("block not kept: {other:?}"),
                },
            )
            .collect();
        let theirs: Vec<Vec<u8>> = input
            .iter()
            .map(|b| zstd::bulk::compress(b, 3).unwrap())
            .collect();
        // The reference's frames as a table stores them, so both decoders read the same frames.
        let theirs_framed: Vec<Vec<u8>> = theirs
            .iter()
            .zip(&input)
            .map(|(f, b)| {
                let mut v = Vec::new();
                mantle_engine::util::coding::put_varint32(&mut v, b.len() as u32);
                v.extend_from_slice(f);
                v
            })
            .collect();
        let ours_bytes: usize = ours.iter().map(Vec::len).sum();
        let theirs_bytes: usize = theirs.iter().map(Vec::len).sum();
        let n = input.len() as f64;
        for round in 0..ROUNDS {
            let reference_first = round % 2 == 0;
            for side in if reference_first { [0, 1] } else { [1, 0] } {
                if side == 0 {
                    alloc::begin();
                    let t = Instant::now();
                    for b in &input {
                        out.clear();
                        cctx.compress_to_buffer(b, &mut out).unwrap();
                    }
                    let ns = t.elapsed().as_nanos() as f64 / n;
                    let a = alloc::end();
                    println!(
                        "ref compress {size} {round} {ns:.0} {} {theirs_bytes}",
                        a.allocations
                    );
                    alloc::begin();
                    let t = Instant::now();
                    for c in &theirs {
                        dctx.decompress_to_buffer(c, &mut plain).unwrap();
                    }
                    let ns = t.elapsed().as_nanos() as f64 / n;
                    let a = alloc::end();
                    println!(
                        "ref decompress {size} {round} {ns:.0} {} {theirs_bytes}",
                        a.allocations
                    );
                } else {
                    alloc::begin();
                    let t = Instant::now();
                    for b in &input {
                        std::hint::black_box(
                            compress_block(CompressionType::Zstd, b, &[], &opts).unwrap(),
                        );
                    }
                    let ns = t.elapsed().as_nanos() as f64 / n;
                    let a = alloc::end();
                    println!(
                        "port compress {size} {round} {ns:.0} {} {ours_bytes}",
                        a.allocations
                    );
                    alloc::begin();
                    let t = Instant::now();
                    for c in &theirs_framed {
                        std::hint::black_box(
                            decompress_block(CompressionType::Zstd, c, &[], size).unwrap(),
                        );
                    }
                    let ns = t.elapsed().as_nanos() as f64 / n;
                    let a = alloc::end();
                    println!(
                        "port decompress {size} {round} {ns:.0} {} {theirs_bytes}",
                        a.allocations
                    );
                }
            }
        }
    }
}
