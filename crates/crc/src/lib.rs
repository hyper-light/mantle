//! The checksums mantle stores and verifies.
//!
//! CRC-32C (Castagnoli, iSCSI; RFC 3720 §12.1) protects every on-disk record and network
//! payload (docs/research/03 §14). CRC-64/NVME is one of the full-object checksums S3
//! clients send (docs/research/05). Both come from `crc-fast`, which computes them with
//! carry-less multiplication (PCLMULQDQ on x86_64, PMULL on aarch64) selected at run
//! time; on the development machine it computes CRC-32C at ~100 GB/s against ~32 GB/s for
//! the `crc32c` crate's CRC instructions (1 MiB buffers, Apple M5 Max). Only the two
//! predefined algorithms are used: `crc-fast` panics only for custom CRC parameters.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

use crc_fast::{CrcAlgorithm, Digest};

/// CRC-32C of `data`.
pub fn crc32c(data: &[u8]) -> u32 {
    crc_fast::crc32_iscsi(data)
}

/// The CRC-32C of `a ‖ b`, given `crc32c(a)`, `crc32c(b)` and `b.len()`.
pub fn crc32c_combine(crc_a: u32, crc_b: u32, len_b: u64) -> u32 {
    let combined =
        crc_fast::checksum_combine(CrcAlgorithm::Crc32Iscsi, crc_a.into(), crc_b.into(), len_b);
    // A 32-bit CRC's combination fits in 32 bits.
    u32::try_from(combined).unwrap_or(0)
}

/// An incremental CRC-32C.
#[derive(Clone)]
pub struct Crc32c(Digest);

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32c {
    pub fn new() -> Self {
        Self(Digest::new(CrcAlgorithm::Crc32Iscsi))
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    pub fn finish(&self) -> u32 {
        u32::try_from(self.0.finalize()).unwrap_or(0)
    }
}

/// CRC-64/NVME of `data`.
pub fn crc64nvme(data: &[u8]) -> u64 {
    crc_fast::checksum(CrcAlgorithm::Crc64Nvme, data)
}

/// An incremental CRC-64/NVME.
#[derive(Clone)]
pub struct Crc64Nvme(Digest);

impl Default for Crc64Nvme {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc64Nvme {
    pub fn new() -> Self {
        Self(Digest::new(CrcAlgorithm::Crc64Nvme))
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    pub fn finish(&self) -> u64 {
        self.0.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The catalogue check values: the CRC of the ASCII string "123456789" (Williams, "A
    /// Painless Guide to CRC Error Detection Algorithms"; the Rocksoft model parameters
    /// crc-fast implements). CRC-32C also matches RFC 3720 Appendix B.4's vectors below.
    #[test]
    fn check_values() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc64nvme(b"123456789"), 0xAE8B_1486_0A79_9888);
    }

    /// RFC 3720, Appendix B.4: 32 bytes of zeros, of 0xFF, and incrementing.
    #[test]
    fn rfc3720_vectors() {
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
        let inc: Vec<u8> = (0u8..32).collect();
        assert_eq!(crc32c(&inc), 0x46DD_794E);
        let dec: Vec<u8> = (0u8..32).rev().collect();
        assert_eq!(crc32c(&dec), 0x113F_DB5C);
    }

    proptest! {
        #[test]
        fn combine_equals_whole(a in proptest::collection::vec(any::<u8>(), 0..5000),
                                b in proptest::collection::vec(any::<u8>(), 0..5000)) {
            let whole: Vec<u8> = a.iter().chain(b.iter()).copied().collect();
            prop_assert_eq!(crc32c_combine(crc32c(&a), crc32c(&b), b.len() as u64), crc32c(&whole));
        }

        #[test]
        fn incremental_equals_one_shot(data in proptest::collection::vec(any::<u8>(), 0..20000),
                                       cut in 0usize..20000) {
            let cut = cut.min(data.len());
            let mut c = Crc32c::new();
            c.update(&data[..cut]);
            c.update(&data[cut..]);
            prop_assert_eq!(c.finish(), crc32c(&data));
            let mut n = Crc64Nvme::new();
            n.update(&data[..cut]);
            n.update(&data[cut..]);
            prop_assert_eq!(n.finish(), crc64nvme(&data));
        }
    }
}
