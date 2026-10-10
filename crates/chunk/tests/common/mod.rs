#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

pub mod device;

use std::sync::OnceLock;

use hyper_block::issuer::Issuer;
use mantle_chunk::{ChunkError, ChunkKey, Config, Limits, Reads, Volume};

use device::SimDevice;

/// The issuer of the one device every volume of a test binary is on, as a device's volumes
/// share one, keeping four transfers in flight, as a device measured there.
pub fn issuer() -> &'static Issuer {
    static ISSUER: OnceLock<Issuer> = OnceLock::new();
    ISSUER.get_or_init(|| Issuer::start(std::path::Path::new("test device"), 4).unwrap())
}

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
        prewrite: false,
        reads: Reads::default(),
    }
}

pub const SIZE: u64 = 8 << 20;

/// Puts, retrying while the volume answers `Busy`, as a client backs off while the cleaner
/// frees space. The bound only stops a hang.
pub fn put_retrying(v: &Volume<SimDevice>, k: ChunkKey, data: &[u8]) -> Result<(), ChunkError> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match v.put(k, data) {
            Err(ChunkError::Busy) if std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            other => return other,
        }
    }
}

/// A simulated device whose crashes and faults replay from `seed`.
pub fn sim(seed: u64) -> SimDevice {
    device::sim(seed)
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

/// The index frames of `volume` the device durably holds, as (byte offset in the file, kind,
/// LSN), in the order they sit on the device. A frame starts with its magic, version and
/// kind, then its LSN and the volume's ID (frame.rs).
pub fn frames(file: &SimDevice, volume: u128) -> Vec<(u64, u16, u64)> {
    let image = file.durable_image().unwrap();
    image
        .as_chunks::<4096>()
        .0
        .iter()
        .enumerate()
        .filter(|(_, b)| {
            &b[0..4] == b"MNIX" && u128::from_le_bytes(b[16..32].try_into().unwrap()) == volume
        })
        .map(|(i, b)| {
            let kind = u16::from_le_bytes([b[6], b[7]]);
            let lsn = u64::from_le_bytes(b[8..16].try_into().unwrap());
            (i as u64 * 4096, kind, lsn)
        })
        .collect()
}

/// The frame with the highest LSN.
pub fn last_frame(file: &SimDevice, volume: u128) -> (u64, u16, u64) {
    frames(file, volume)
        .into_iter()
        .max_by_key(|&(_, _, lsn)| lsn)
        .expect("no frame")
}

/// Damages the frame at `at` for good: a stored bit of its CRC-32C, which every frame has,
/// where its records may be too short to reach a given offset and bytes past them are padding
/// no checksum covers.
pub fn damage_frame(file: &SimDevice, at: u64) {
    file.inject(hyper_block::sim::Fault::BitFlip {
        offset: at + 44,
        bit: 3,
        stored: true,
    })
    .unwrap();
}

/// Asserts the frame at `at` is a confirmation: a batch frame (kind 1) of no records, the
/// count 36 bytes in (frame.rs). Damage to it may lose nothing answered.
pub fn assert_confirmation(file: &SimDevice, at: u64) {
    let image = file.durable_image().unwrap();
    let at = at as usize;
    let kind = u16::from_le_bytes([image[at + 6], image[at + 7]]);
    let count = u32::from_le_bytes(image[at + 36..at + 40].try_into().unwrap());
    assert_eq!((kind, count), (1, 0), "not a confirmation frame");
}
