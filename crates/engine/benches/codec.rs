//! Table blocks' ZSTD against the reference library (zstd 1.5.7 through the `zstd` crate, the
//! tests' oracle): compress and decompress of block-sized inputs as RocksDB compresses a table's
//! blocks, at its default level 3.
//!
//! `cargo bench -p mantle-engine --bench codec` takes blocks of 4 KiB (RocksDB's default
//! `block_size`) and 16 KiB from three corpora, and for each runs 59 rounds (Wilks 95/95), the
//! two sides alternating which goes first:
//! - `objstore`: data blocks as RocksDB writes them (the port's `BlockBuilder`, byte for byte
//!   RocksDB's), of keys and values shaped as Meta's object-storage metadata in ZippyDB: keys of
//!   48-53 bytes for 60% and 90-91 bytes otherwise, values averaging 43 bytes with 90% under 34
//!   and 1% over 400 (Cao et al., "Characterizing, Modeling, and Benchmarking RocksDB Key-Value
//!   Workloads at Facebook", FAST '20, §5, Table 2, Figure 8(c, d)).
//! - `assoc`: data blocks of social-graph associations as UDB's `Assoc` column family holds
//!   them: keys of about 27 bytes, values of about 50 (same paper, Table 2 and §5).
//! - `text`: the corpus's text (`tests/support/corpus.rs`), repetitive prose, kept for continuity.
//!
//! A round compresses then decompresses every block, after both decoders and the reference have
//! read every frame back to its block. The reference reuses one compression context, one
//! decompression context and one output buffer, as RocksDB's working areas do; the port reuses
//! one `Workspace` for both, called as the table code calls it (`util::block_compression`), and
//! returns each compressed block in a buffer of its own, as a table builder takes it. Both
//! decoders read the reference's frames; each encoder's output size is printed beside its time. It prints
//! `side op corpus block_size round ns_per_block allocations output_bytes` per round, and each
//! corpus's input bytes once as `input corpus block_size bytes blocks`. `-- rounds [port|both]
//! [corpus]` runs that many rounds instead, `port` only the port's side (to profile it over a run
//! long enough to sample), and one corpus only.
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
    Compressed, CompressionOptions, CompressionType, Workspace, compress_block,
    compress_block_with, decompress_block_into,
};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Rounds a size by default (Wilks: the extremes of 59 bound the 95th percentile at 95%).
const ROUNDS: usize = 59;

/// Blocks of `size` bytes cut from the corpus's text inputs, in order.
fn text_blocks(size: usize) -> Vec<Vec<u8>> {
    let text: Vec<u8> = corpus::corpus()
        .into_iter()
        .filter(|(n, _)| n.starts_with("text-"))
        .flat_map(|(_, d)| d)
        .collect();
    text.chunks_exact(size).map(<[u8]>::to_vec).collect()
}

/// Words the generated keys' path components and metadata are drawn from.
const WORDS: [&str; 16] = [
    "photos", "video", "thumb", "2026", "10", "06", "agent", "run", "trace", "ckpt", "shard",
    "batch", "model", "eval", "logs", "data",
];

/// A key of exactly `len` bytes: a tenant, a bucket and a path, as an object store names
/// objects, cut or padded with the object's sequence number.
fn object_key(rng: &mut corpus::Rng, tenant: u64, seq: u64, len: usize) -> Vec<u8> {
    let mut k = format!("{tenant:08x}/b{}/", rng.below(8)).into_bytes();
    for _ in 0..1 + rng.below(4) {
        k.extend_from_slice(WORDS[rng.below(WORDS.len() as u64) as usize].as_bytes());
        k.push(b'/');
    }
    let tail = format!("{seq:020}");
    k.extend_from_slice(tail.as_bytes());
    if k.len() > len {
        // Keep the unique tail.
        k.drain(..k.len() - len);
    } else {
        while k.len() < len {
            k.insert(9, b'x');
        }
    }
    k
}

/// A value of `len` bytes as object metadata holds: where the object's data lies (volume,
/// offset, length, checksum), then, for the longer ones, user metadata as text.
fn object_value(rng: &mut corpus::Rng, offset: u64, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    v.extend_from_slice(&rng.below(64).to_le_bytes()[..2]);
    v.extend_from_slice(&offset.to_le_bytes()[..6]);
    v.extend_from_slice(&(rng.below(1 << 24) as u32).to_le_bytes());
    v.extend_from_slice(&(rng.next() as u32).to_le_bytes());
    while v.len() < len {
        let w = WORDS[rng.below(WORDS.len() as u64) as usize];
        v.extend_from_slice(format!("\"{w}\":\"{}\",", rng.below(1000)).as_bytes());
    }
    v.truncate(len);
    v
}

/// Data blocks of `block_size` as RocksDB's table builder cuts them (restart interval 16, cut
/// at the block size as `FlushBlockBySizePolicy` cuts), from `entries` sorted keys and their
/// values, each key with its 8-byte internal trailer.
fn data_blocks(block_size: usize, entries: &mut [(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    use mantle_engine::table::block_based::block_builder::{BlockBuilder, BlockBuilderOptions};
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut b = BlockBuilder::new(BlockBuilderOptions {
        restart_interval: 16,
        capacity: block_size,
        ..BlockBuilderOptions::default()
    })
    .unwrap();
    let limit = (block_size * 90).div_ceil(100);
    let mut out = Vec::new();
    for (key, value) in entries.iter() {
        let mut ikey = key.clone();
        ikey.extend_from_slice(&((1u64 << 8) | 1).to_le_bytes());
        if !b.is_empty() {
            let cur = b.current_size_estimate();
            if cur >= block_size
                || (b.estimate_size_after_kv(&ikey, value) > block_size && cur > limit)
            {
                out.push(b.finish().unwrap().to_vec());
                b.reset();
            }
        }
        b.add(&ikey, value, None, false).unwrap();
    }
    out.push(b.finish().unwrap().to_vec());
    out
}

/// Object metadata as ZippyDB's object-storage shard holds it (FAST '20 §5, Figure 8(c, d)).
fn objstore_blocks(block_size: usize) -> Vec<Vec<u8>> {
    let mut rng = corpus::Rng(0x6f626a);
    let mut entries = Vec::new();
    let mut offset = 0u64;
    for seq in 0..60_000u64 {
        let tenant = rng.below(256);
        let key_len = if rng.below(100) < 60 {
            48 + rng.below(6) as usize
        } else {
            90 + rng.below(2) as usize
        };
        let pick = rng.below(100);
        let value_len = if pick < 90 {
            16 + rng.below(18) as usize
        } else if pick < 99 {
            34 + rng.below(366) as usize
        } else {
            400 + rng.below(1600) as usize
        };
        offset += rng.below(1 << 20);
        entries.push((
            object_key(&mut rng, tenant, seq, key_len),
            object_value(&mut rng, offset, value_len),
        ));
    }
    data_blocks(block_size, &mut entries)
}

/// Social-graph associations as UDB's `Assoc` column family holds them (FAST '20 Table 2, §5):
/// a 4-byte table index, two object ids and a type, then a small value.
fn assoc_blocks(block_size: usize) -> Vec<Vec<u8>> {
    let mut rng = corpus::Rng(0x6173736f63);
    let mut entries = Vec::new();
    for _ in 0..60_000u64 {
        let mut k = Vec::with_capacity(27);
        k.extend_from_slice(&7u32.to_be_bytes());
        k.extend_from_slice(&(rng.below(1 << 30)).to_be_bytes());
        k.extend_from_slice(&rng.below(16).to_be_bytes()[5..]);
        k.extend_from_slice(&rng.next().to_be_bytes());
        let value_len = 10 + rng.below(81) as usize;
        let mut v = Vec::with_capacity(value_len);
        v.extend_from_slice(&rng.below(1 << 40).to_le_bytes()[..5]);
        while v.len() < value_len {
            v.push(b"0123456789abcdef"[rng.below(16) as usize]);
        }
        entries.push((k, v));
    }
    data_blocks(block_size, &mut entries)
}

fn main() {
    let args: Vec<String> = std::env::args().filter(|a| !a.starts_with('-')).collect();
    let rounds: usize = args.get(1).map_or(ROUNDS, |a| a.parse().unwrap());
    let port_only = args.get(2).is_some_and(|a| a == "port");
    let only = args.get(3).cloned();
    let opts = CompressionOptions::default();
    for name in ["objstore", "assoc", "text"] {
        if only.as_deref().is_some_and(|o| o != name) {
            continue;
        }
        for size in [4096usize, 16384] {
            let input = match name {
                "objstore" => objstore_blocks(size),
                "assoc" => assoc_blocks(size),
                _ => text_blocks(size),
            };
            run(name, size, &input, rounds, port_only, &opts);
        }
    }
}

fn run(
    name: &str,
    size: usize,
    input: &[Vec<u8>],
    rounds: usize,
    port_only: bool,
    opts: &CompressionOptions,
) {
    let input_bytes: usize = input.iter().map(Vec::len).sum();
    println!("input {name} {size} {input_bytes} {}", input.len());
    let largest = input.iter().map(Vec::len).max().unwrap_or(0);
    let mut cctx = zstd::bulk::Compressor::new(3).unwrap();
    let mut dctx = zstd::bulk::Decompressor::new().unwrap();
    let mut out = Vec::with_capacity(zstd::zstd_safe::compress_bound(largest) + 8);
    let mut plain = vec![0u8; largest];
    // The port's working area and output buffer, kept across blocks as the reference's are.
    let mut workspace = Workspace::default();
    let mut port_out = Vec::with_capacity(largest);
    let ours: Vec<Vec<u8>> = input
        .iter()
        .map(
            |b| match compress_block(CompressionType::Zstd, b, &[], opts).unwrap() {
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
        .zip(input)
        .map(|(f, b)| {
            let mut v = Vec::new();
            mantle_engine::util::coding::put_varint32(&mut v, b.len() as u32);
            v.extend_from_slice(f);
            v
        })
        .collect();
    // Both decoders must give back every block.
    for ((c, f), b) in theirs.iter().zip(&theirs_framed).zip(input) {
        let n = dctx.decompress_to_buffer(c, &mut plain).unwrap();
        assert!(plain[..n] == b[..]);
        decompress_block_into(
            CompressionType::Zstd,
            f,
            &[],
            b.len(),
            &mut workspace,
            &mut port_out,
        )
        .unwrap();
        assert!(port_out == *b);
    }
    for (c, b) in ours.iter().zip(input) {
        let (_, body) =
            mantle_engine::util::block_compression::uncompressed_size(CompressionType::Zstd, c)
                .unwrap();
        let n = dctx.decompress_to_buffer(body, &mut plain).unwrap();
        assert!(
            plain[..n] == b[..],
            "the reference reads the port's frame back"
        );
    }
    let ours_bytes: usize = ours.iter().map(Vec::len).sum();
    let theirs_bytes: usize = theirs.iter().map(Vec::len).sum();
    let n = input.len() as f64;
    for round in 0..rounds {
        let reference_first = round % 2 == 0;
        for side in if reference_first { [0, 1] } else { [1, 0] } {
            if port_only && side == 0 {
                continue;
            }
            if side == 0 {
                alloc::begin();
                let t = Instant::now();
                for b in input {
                    out.clear();
                    cctx.compress_to_buffer(b, &mut out).unwrap();
                }
                let ns = t.elapsed().as_nanos() as f64 / n;
                let a = alloc::end();
                println!(
                    "ref compress {name} {size} {round} {ns:.0} {} {theirs_bytes}",
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
                    "ref decompress {name} {size} {round} {ns:.0} {} {theirs_bytes}",
                    a.allocations
                );
            } else {
                alloc::begin();
                let t = Instant::now();
                for b in input {
                    std::hint::black_box(
                        compress_block_with(CompressionType::Zstd, b, &[], opts, &mut workspace)
                            .unwrap(),
                    );
                }
                let ns = t.elapsed().as_nanos() as f64 / n;
                let a = alloc::end();
                println!(
                    "port compress {name} {size} {round} {ns:.0} {} {ours_bytes}",
                    a.allocations
                );
                alloc::begin();
                let t = Instant::now();
                for (c, b) in theirs_framed.iter().zip(input) {
                    decompress_block_into(
                        CompressionType::Zstd,
                        c,
                        &[],
                        b.len(),
                        &mut workspace,
                        &mut port_out,
                    )
                    .unwrap();
                }
                let ns = t.elapsed().as_nanos() as f64 / n;
                let a = alloc::end();
                println!(
                    "port decompress {name} {size} {round} {ns:.0} {} {theirs_bytes}",
                    a.allocations
                );
            }
        }
    }
}
