//! The released xxHash algorithms RocksDB vendors as `util/xxhash.h` (xxHash 0.8.1, symbols
//! prefixed `ROCKSDB_`) [R util/xxhash.h:11-13, :474-476]: XXH32, XXH64, XXH3-64 and XXH3-128,
//! computed by `twox-hash`, which implements the same released algorithms (docs/research/24
//! §1.2, §2.4). They key the block checksums (`table::format`) and `Hash128`/`Hash2x64`
//! (`util::hash`). `Hash64`, which keys filters, is not one of these: it is the XXH3 preview
//! in `util::xxph3`.

use std::hash::Hasher as _;

use twox_hash::{XxHash3_64, XxHash3_128, XxHash32, XxHash64};

/// `XXH32(data, len, seed)`.
pub fn xxh32(data: &[u8], seed: u32) -> u32 {
    XxHash32::oneshot(seed, data)
}

/// `XXH64(data, len, seed)`.
pub fn xxh64(data: &[u8], seed: u64) -> u64 {
    XxHash64::oneshot(seed, data)
}

/// `XXH3_64bits(data, len)`.
pub fn xxh3_64bits(data: &[u8]) -> u64 {
    XxHash3_64::oneshot(data)
}

/// `XXH3_64bits_withSeed(data, len, seed)`.
pub fn xxh3_64bits_with_seed(data: &[u8], seed: u64) -> u64 {
    XxHash3_64::oneshot_with_seed(seed, data)
}

/// `XXH3_128bits_withSeed(data, len, seed)`, as `high64 << 64 | low64`.
pub fn xxh3_128bits_with_seed(data: &[u8], seed: u64) -> u128 {
    XxHash3_128::oneshot_with_seed(seed, data)
}

/// XXH32 over `data ‖ last_byte` without joining them: RocksDB's streamed
/// `XXH32_update(data); XXH32_update(&last_byte, 1)` [R table/format.cc:652-660].
pub fn xxh32_with_last_byte(data: &[u8], last_byte: u8, seed: u32) -> u32 {
    let mut state = XxHash32::with_seed(seed);
    state.write(data);
    state.write(&[last_byte]);
    state.finish_32()
}

/// XXH64 over `data ‖ last_byte`, streamed as [`xxh32_with_last_byte`]
/// [R table/format.cc:662-670].
pub fn xxh64_with_last_byte(data: &[u8], last_byte: u8, seed: u64) -> u64 {
    let mut state = XxHash64::with_seed(seed);
    state.write(data);
    state.write(&[last_byte]);
    state.finish()
}
