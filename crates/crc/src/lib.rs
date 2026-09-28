//! The checksums mantle stores and verifies.
//!
//! CRC-32C (Castagnoli, iSCSI; RFC 3720 §12.1) protects every on-disk record and network
//! payload (docs/research/03 §14). CRC-64/NVME is one of the full-object checksums S3
//! clients send (docs/research/05). Both come from `crc-fast`, which computes them with
//! carry-less multiplication (PCLMULQDQ on x86_64, PMULL on aarch64) selected at run
//! time; on the development machine it computes CRC-32C at ~100 GB/s against ~32 GB/s for
//! the `crc32c` crate's CRC instructions (1 MiB buffers, Apple M5 Max). Only the two
//! predefined algorithms are used: `crc-fast` panics only for custom CRC parameters.
//!
//! Combining CRC-32Cs is done here, with zlib's method (crc32.c, `crc32_combine64`):
//! the CRC of `a ‖ b` is `crc(a) · x^(8·|b|) ^ crc(b)` modulo the polynomial, and the power of
//! x comes from a table of x^(2^k). `crc-fast`'s combination rebuilt that operator on every
//! call, which made combining a record's 64 KiB block CRCs cost nine times as much as
//! computing them (profile of the chunk writer, 8 MiB records); [`Crc32cShift`] builds it once
//! for a fixed length, so each combination is a single multiplication.
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

/// The CRC-32C polynomial 0x1EDC6F41, bit-reflected, with x^32 implied.
const POLY: u32 = 0x82F6_3B78;

/// a(x) · b(x) modulo the polynomial, both bit-reflected (zlib's `multmodp`); `a` is never
/// zero here, since powers of x are never zero modulo a polynomial with a constant term.
const fn multmodp(a: u32, mut b: u32) -> u32 {
    let mut m: u32 = 1 << 31;
    let mut p: u32 = 0;
    while m != 0 {
        if a & m != 0 {
            p ^= b;
            if a & (m.wrapping_sub(1)) == 0 {
                break;
            }
        }
        m >>= 1;
        b = if b & 1 != 0 { (b >> 1) ^ POLY } else { b >> 1 };
    }
    p
}

/// x^(2^k) modulo the polynomial, for k in 0..32.
const X2N: [u32; 32] = {
    let mut table = [0u32; 32];
    let mut p: u32 = 1 << 30; // x^1
    let mut rest: &mut [u32] = &mut table;
    while let Some((slot, tail)) = rest.split_first_mut() {
        *slot = p;
        p = multmodp(p, p);
        rest = tail;
    }
    table
};

/// x^(8·len) modulo the polynomial: the operator that shifts a CRC past `len` bytes.
fn shift_operator(len: u64) -> u32 {
    let mut p: u32 = 1 << 31; // x^0
    let mut n = len;
    let mut k: usize = 3; // 8 = 2^3
    while n != 0 {
        if n & 1 != 0 {
            p = multmodp(X2N.get(k & 31).copied().unwrap_or(1 << 31), p);
        }
        n >>= 1;
        k = k.wrapping_add(1);
    }
    p
}

/// The CRC-32C of `a ‖ b`, given `crc32c(a)`, `crc32c(b)` and `b.len()`.
pub fn crc32c_combine(crc_a: u32, crc_b: u32, len_b: u64) -> u32 {
    multmodp(shift_operator(len_b), crc_a) ^ crc_b
}

/// Combines CRC-32Cs across a fixed length: built once, each combination is one
/// multiplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Crc32cShift(u32);

impl Crc32cShift {
    /// For appending `len` bytes.
    pub fn new(len: u64) -> Self {
        Self(shift_operator(len))
    }

    /// The CRC-32C of `a ‖ b`, given `crc32c(a)` and `crc32c(b)`, where `b` is `len` bytes.
    pub fn combine(self, crc_a: u32, crc_b: u32) -> u32 {
        multmodp(self.0, crc_a) ^ crc_b
    }
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

    #[test]
    fn combining_with_nothing_changes_nothing() {
        let c = crc32c(b"123456789");
        assert_eq!(crc32c_combine(c, crc32c(&[]), 0), c);
        assert_eq!(crc32c_combine(crc32c(&[]), c, 9), c);
        assert_eq!(
            crc32c_combine(crc32c(b"1234"), crc32c(b"56789"), 5),
            0xE306_9283
        );
    }

    proptest! {
        #[test]
        fn combine_equals_whole(a in proptest::collection::vec(any::<u8>(), 0..5000),
                                b in proptest::collection::vec(any::<u8>(), 0..5000)) {
            let whole: Vec<u8> = a.iter().chain(b.iter()).copied().collect();
            prop_assert_eq!(crc32c_combine(crc32c(&a), crc32c(&b), b.len() as u64), crc32c(&whole));
        }

        #[test]
        fn a_fixed_shift_combines_like_the_general_one(blocks in proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 1000), 1..20)) {
            let shift = Crc32cShift::new(1000);
            let mut acc = crc32c(&blocks[0]);
            for b in &blocks[1..] {
                acc = shift.combine(acc, crc32c(b));
            }
            let whole: Vec<u8> = blocks.concat();
            prop_assert_eq!(acc, crc32c(&whole));
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
