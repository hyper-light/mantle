//! The bitstreams of Zstandard (RFC 8878): a forward reader for FSE table descriptions
//! (§4.1.1), and the backward reader every FSE- and Huffman-coded stream is read with
//! (§3.1.1.3.2.1.2, §4.2.2).
//!
//! A writer appends each value at the stream's bit position, least significant bit first, so
//! the stream is one little-endian bit string; it ends with a 1 bit and zero padding to the byte.
//! The backward reader starts below that 1 bit and takes each value from the top: `n` bits read
//! are the bits `[pos - n, pos)`, the lowest at `pos - n`. Reading past the start yields zero
//! bits and leaves `pos` negative, which is how the Huffman weights' decoder (§4.2.1.2) learns
//! the stream ended.

use super::Corrupt;

/// Reads `bits` (at most 57) from `bytes` starting at bit `at`, little-endian; bits outside the
/// slice read as zero.
fn bits_at(bytes: &[u8], at: u64, bits: u32) -> u64 {
    if bits == 0 {
        return 0;
    }
    let byte = usize::try_from(at / 8).unwrap_or(usize::MAX);
    let shift = u32::try_from(at % 8).unwrap_or(0);
    let mut word = [0u8; 8];
    if let Some(src) = bytes.get(byte..) {
        let n = src.len().min(8);
        if let (Some(dst), Some(src)) = (word.get_mut(..n), src.get(..n)) {
            dst.copy_from_slice(src);
        }
    }
    let value = u64::from_le_bytes(word) >> shift;
    value
        & u64::MAX
            .checked_shr(64u32.saturating_sub(bits))
            .unwrap_or(0)
}

/// A forward reader over a little-endian bit string (§4.1.1's table descriptions).
pub(super) struct Forward<'a> {
    bytes: &'a [u8],
    pos: u64,
}

impl<'a> Forward<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// The next `bits` (at most 32) without consuming them; past the end reads zero.
    pub(super) fn peek(&self, bits: u32) -> u32 {
        u32::try_from(bits_at(self.bytes, self.pos, bits)).unwrap_or(u32::MAX)
    }

    /// Consumes `bits`, refusing to pass the end of the bytes.
    pub(super) fn skip(&mut self, bits: u32) -> Result<(), Corrupt> {
        let pos = self
            .pos
            .checked_add(u64::from(bits))
            .ok_or(Corrupt::Truncated)?;
        let len = u64::try_from(self.bytes.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(8);
        if pos > len {
            return Err(Corrupt::Truncated);
        }
        self.pos = pos;
        Ok(())
    }

    /// The bytes consumed, the last partly used one included (§4.1.1: "the bitstream consumes a
    /// round number of bytes").
    pub(super) fn bytes_used(&self) -> usize {
        usize::try_from(self.pos.div_ceil(8)).unwrap_or(usize::MAX)
    }
}

/// A backward reader over one stream (§3.1.1.3.2.1.2, §4.2.2).
pub(super) struct Backward<'a> {
    bytes: &'a [u8],
    /// Bits not yet read, below the padding; negative once a read went past the start.
    pos: i64,
}

impl<'a> Backward<'a> {
    /// A reader positioned below the stream's final 1 bit. A stream that is empty or whose last
    /// byte is zero has no such bit and is corrupt.
    pub(super) fn new(bytes: &'a [u8]) -> Result<Self, Corrupt> {
        let last = *bytes.last().ok_or(Corrupt::Bitstream)?;
        if last == 0 {
            return Err(Corrupt::Bitstream);
        }
        let len = i64::try_from(bytes.len()).map_err(|_| Corrupt::Bitstream)?;
        // The highest set bit of the last byte is the end marker.
        let marker = i64::from(7u32.saturating_sub(last.leading_zeros()));
        let pos = len
            .checked_sub(1)
            .and_then(|b| b.checked_mul(8))
            .and_then(|b| b.checked_add(marker))
            .ok_or(Corrupt::Bitstream)?;
        Ok(Self { bytes, pos })
    }

    /// The `bits` (at most 32) below the position without consuming them; bits below the start
    /// read as zero.
    pub(super) fn peek(&self, bits: u32) -> u32 {
        if bits == 0 {
            return 0;
        }
        let start = self.pos.saturating_sub(i64::from(bits));
        let value = if start >= 0 {
            bits_at(self.bytes, start.unsigned_abs(), bits)
        } else {
            // Fewer than `bits` remain: the missing low bits are zero.
            let have = u32::try_from(self.pos.max(0)).unwrap_or(0);
            let missing = bits.saturating_sub(have);
            bits_at(self.bytes, 0, have) << missing
        };
        u32::try_from(value).unwrap_or(u32::MAX)
    }

    /// Reads and consumes `bits` (at most 32).
    pub(super) fn read(&mut self, bits: u32) -> u32 {
        let value = self.peek(bits);
        self.pos = self.pos.saturating_sub(i64::from(bits));
        value
    }

    /// Consumes `bits` already peeked.
    pub(super) fn skip(&mut self, bits: u32) {
        self.pos = self.pos.saturating_sub(i64::from(bits));
    }

    /// Whether every bit was read and none past the start: a stream must be consumed exactly
    /// (§3.1.1.3.2.1.2, §4.2.2).
    pub(super) fn finished(&self) -> bool {
        self.pos == 0
    }

    /// Whether a read went past the start (§4.2.1.2's end condition).
    pub(super) fn overflowed(&self) -> bool {
        self.pos < 0
    }
}
