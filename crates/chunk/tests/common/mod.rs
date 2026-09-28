#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;

use mantle_chunk::{ChunkKey, Config, Limits};
use mantle_disk::buf::Alignment;
use mantle_disk::sim::SimFile;

/// A small compact volume: 256 KiB segments, 4 KiB checksum blocks, a short log so tests
/// reach checkpoints and wrap-around quickly.
pub fn config() -> Config {
    Config {
        segment_size: 256 << 10,
        checksum_shift: 12,
        max_fragments: 2000,
        compact: true,
        scrub_period: None,
        limits: Limits {
            batch_requests: 64,
            batch_bytes: 1 << 20,
            fragments_per_chunk: 64,
        },
    }
}

pub const SIZE: u64 = 8 << 20;

pub fn sim(seed: u64) -> Arc<SimFile> {
    Arc::new(
        SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            seed,
        )
        .unwrap(),
    )
}

pub fn key(n: u64) -> ChunkKey {
    ChunkKey {
        block: u128::from(n) * 0x9E37_79B9_7F4A_7C15,
        epoch: 1,
        index: (n % 9) as u16,
    }
}

/// Deterministic bytes that differ per key and length.
pub fn data(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}
