//! The engine's own xxHash (`util::xxhash`) against `twox-hash`, which implements the same
//! released algorithms, over every length from 0 to 4,200 (every length class of each algorithm
//! and its boundaries, and XXH3's long path across several blocks) and seeds that set and clear
//! every byte; the golden vectors of `tests/golden.rs` check the same functions against RocksDB's
//! C++.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::hash::Hasher as _;

use mantle_engine::util::xxhash::{
    xxh3_64bits, xxh3_64bits_with_seed, xxh3_128bits_with_seed, xxh32, xxh32_with_last_byte, xxh64,
    xxh64_with_last_byte,
};
use twox_hash::{XxHash3_64, XxHash3_128, XxHash32, XxHash64};

/// Every length to past XXH3's third long block (1,024 bytes a block with the default secret).
const LENGTHS: usize = 4_200;
const SEEDS: [u64; 6] = [
    0,
    1,
    0x9E37_79B9_7F4A_7C15,
    u64::MAX,
    0x0000_0000_FFFF_FFFF,
    0xFFFF_FFFF_0000_0000,
];

/// Bytes that change at every position: a SplitMix64 stream.
fn input() -> Vec<u8> {
    let mut x = 0x0123_4567_89AB_CDEFu64;
    (0..LENGTHS)
        .map(|_| {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            (z ^ (z >> 31)) as u8
        })
        .collect()
}

#[test]
fn xxh32_and_xxh64_match_the_oracle() {
    let data = input();
    for len in 0..=LENGTHS {
        let d = &data[..len];
        for seed in SEEDS {
            assert_eq!(
                xxh32(d, seed as u32),
                XxHash32::oneshot(seed as u32, d),
                "{len} {seed}"
            );
            assert_eq!(xxh64(d, seed), XxHash64::oneshot(seed, d), "{len} {seed}");
        }
    }
}

#[test]
fn xxh3_matches_the_oracle() {
    let data = input();
    for len in 0..=LENGTHS {
        let d = &data[..len];
        assert_eq!(xxh3_64bits(d), XxHash3_64::oneshot(d), "{len}");
        for seed in SEEDS {
            assert_eq!(
                xxh3_64bits_with_seed(d, seed),
                XxHash3_64::oneshot_with_seed(seed, d),
                "{len} {seed}"
            );
            assert_eq!(
                xxh3_128bits_with_seed(d, seed),
                XxHash3_128::oneshot_with_seed(seed, d),
                "{len} {seed}"
            );
        }
    }
}

/// The streamed `data ‖ last_byte` forms the block checksums use, at every length, against the
/// oracle's streaming state.
#[test]
fn last_byte_forms_match_the_oracle_streamed() {
    let data = input();
    for len in 0..=LENGTHS {
        let d = &data[..len];
        for last in [0x00u8, 0x5A, 0xFF] {
            for seed in SEEDS {
                let mut h32 = XxHash32::with_seed(seed as u32);
                h32.write(d);
                h32.write(&[last]);
                assert_eq!(xxh32_with_last_byte(d, last, seed as u32), h32.finish_32());
                let mut h64 = XxHash64::with_seed(seed);
                h64.write(d);
                h64.write(&[last]);
                assert_eq!(xxh64_with_last_byte(d, last, seed), h64.finish(), "{len}");
            }
        }
    }
}
