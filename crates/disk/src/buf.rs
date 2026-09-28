//! Buffers whose address and length meet a device's direct-I/O alignment.
//!
//! Direct I/O transfers straight between the device and user memory, so the buffer address,
//! the file offset and the length must each be a multiple of the alignment the device and
//! file system require (Linux open(2), NOTES on O_DIRECT; Windows "File Buffering",
//! FILE_FLAG_NO_BUFFERING). The buffer is carved out of an ordinary allocation that is
//! `align - 1` bytes longer than needed, so no `unsafe` allocation is involved.

use std::num::NonZeroUsize;

/// The largest alignment accepted. Direct-I/O alignments are logical block sizes and page
/// sizes (512 B to 64 KiB on every platform mantle targets); the bound caps the slack a
/// buffer can waste.
pub const MAX_ALIGNMENT: usize = 1 << 20;

/// The largest single buffer. A buffer holds one I/O; the largest I/O mantle issues is well
/// below this, and the bound turns a corrupt length into a refusal instead of an allocation.
pub const MAX_BUFFER: usize = 1 << 30;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BufError {
    #[error("alignment {0} is not a power of two in 1..={MAX_ALIGNMENT}")]
    Alignment(usize),
    #[error("buffer of {0} bytes exceeds the {MAX_BUFFER}-byte bound")]
    TooLarge(usize),
    #[error("{requested} bytes do not fit in the {available} bytes left")]
    Full { requested: usize, available: usize },
    #[error("length {len} exceeds capacity {capacity}")]
    Length { len: usize, capacity: usize },
}

/// A power-of-two alignment in bytes, at most [`MAX_ALIGNMENT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Alignment(NonZeroUsize);

impl Alignment {
    /// One byte: no constraint.
    pub const BYTE: Self = Self(NonZeroUsize::MIN);

    pub fn new(bytes: usize) -> Result<Self, BufError> {
        match NonZeroUsize::new(bytes) {
            Some(n) if n.is_power_of_two() && bytes <= MAX_ALIGNMENT => Ok(Self(n)),
            _ => Err(BufError::Alignment(bytes)),
        }
    }

    pub fn get(self) -> usize {
        self.0.get()
    }

    fn mask(self) -> usize {
        // A power of two is at least 1, so this cannot wrap.
        self.0.get().wrapping_sub(1)
    }

    pub fn is_aligned(self, n: usize) -> bool {
        n & self.mask() == 0
    }

    pub fn is_aligned_u64(self, n: u64) -> bool {
        // usize is at most 64 bits on every supported target, so the widening is lossless.
        n & (self.mask() as u64) == 0
    }

    /// `n` rounded down to a multiple of the alignment.
    pub fn down(self, n: usize) -> usize {
        n & !self.mask()
    }

    pub fn down_u64(self, n: u64) -> u64 {
        n & !(self.mask() as u64)
    }

    /// `n` rounded up to a multiple of the alignment, or `None` past `usize::MAX`.
    pub fn up(self, n: usize) -> Option<usize> {
        n.checked_add(self.mask()).map(|m| m & !self.mask())
    }

    pub fn up_u64(self, n: u64) -> Option<u64> {
        let mask = self.mask() as u64;
        n.checked_add(mask).map(|m| m & !mask)
    }

    /// The larger of two alignments; both are powers of two, so it satisfies each.
    pub fn max(self, other: Self) -> Self {
        if other.0 > self.0 { other } else { self }
    }
}

/// A fixed-capacity byte buffer whose first byte and capacity are aligned.
///
/// `len` bytes are in use; the capacity is a multiple of the alignment. [`AlignedBuf::padded`]
/// yields the in-use bytes extended with zeros to the next alignment boundary, which is what a
/// direct write of a record that does not end on a boundary transfers.
pub struct AlignedBuf {
    storage: Vec<u8>,
    start: usize,
    capacity: usize,
    len: usize,
    align: Alignment,
}

impl std::fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuf")
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .field("align", &self.align.get())
            .finish()
    }
}

impl AlignedBuf {
    /// A zero-filled buffer of at least `capacity` bytes, rounded up to the alignment.
    pub fn zeroed(capacity: usize, align: Alignment) -> Result<Self, BufError> {
        let capacity = align.up(capacity).ok_or(BufError::TooLarge(capacity))?;
        if capacity > MAX_BUFFER {
            return Err(BufError::TooLarge(capacity));
        }
        // capacity <= 2^30 and mask < 2^20, so the sum cannot overflow.
        let total = capacity
            .checked_add(align.mask())
            .ok_or(BufError::TooLarge(capacity))?;
        let storage = vec![0u8; total];
        let misalignment = storage.as_ptr().addr() & align.mask();
        let start = align.get().wrapping_sub(misalignment) & align.mask();
        Ok(Self {
            storage,
            start,
            capacity,
            len: 0,
            align,
        })
    }

    pub fn alignment(&self) -> Alignment {
        self.align
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn remaining(&self) -> usize {
        // len <= capacity is an invariant every mutator keeps.
        self.capacity.saturating_sub(self.len)
    }

    /// The in-use bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.window(self.len)
    }

    /// The whole aligned capacity, for reading into.
    pub fn as_mut_capacity(&mut self) -> &mut [u8] {
        let (start, end) = (self.start, self.start.saturating_add(self.capacity));
        self.storage.get_mut(start..end).unwrap_or_default()
    }

    /// The in-use bytes, mutable.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let (start, end) = (self.start, self.start.saturating_add(self.len));
        self.storage.get_mut(start..end).unwrap_or_default()
    }

    /// The in-use bytes followed by zeros up to the next alignment boundary.
    pub fn padded(&mut self) -> Result<&[u8], BufError> {
        let padded = self
            .align
            .up(self.len)
            .ok_or(BufError::TooLarge(self.len))?;
        let (start, len) = (self.start, self.len);
        let tail = self
            .storage
            .get_mut(start.saturating_add(len)..start.saturating_add(padded))
            .ok_or(BufError::Length {
                len: padded,
                capacity: self.capacity,
            })?;
        tail.fill(0);
        Ok(self.window(padded))
    }

    /// Declares the first `len` bytes in use, e.g. after a read filled them.
    pub fn set_len(&mut self, len: usize) -> Result<(), BufError> {
        if len > self.capacity {
            return Err(BufError::Length {
                len,
                capacity: self.capacity,
            });
        }
        self.len = len;
        Ok(())
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufError> {
        let available = self.remaining();
        if bytes.len() > available {
            return Err(BufError::Full {
                requested: bytes.len(),
                available,
            });
        }
        let from = self.start.saturating_add(self.len);
        let to = from.saturating_add(bytes.len());
        let dst = self.storage.get_mut(from..to).ok_or(BufError::Full {
            requested: bytes.len(),
            available,
        })?;
        dst.copy_from_slice(bytes);
        self.len = self.len.saturating_add(bytes.len());
        Ok(())
    }

    fn window(&self, len: usize) -> &[u8] {
        self.storage
            .get(self.start..self.start.saturating_add(len))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn alignment_rejects_non_powers_of_two_and_oversize() {
        assert!(Alignment::new(0).is_err());
        assert!(Alignment::new(3).is_err());
        assert!(Alignment::new(MAX_ALIGNMENT * 2).is_err());
        assert_eq!(Alignment::new(4096).unwrap().get(), 4096);
    }

    #[test]
    fn rounding() {
        let a = Alignment::new(512).unwrap();
        assert_eq!(a.up(0), Some(0));
        assert_eq!(a.up(1), Some(512));
        assert_eq!(a.up(512), Some(512));
        assert_eq!(a.up(usize::MAX), None);
        assert_eq!(a.down(1023), 512);
        assert_eq!(a.up_u64(u64::MAX - 10), None);
        assert!(a.is_aligned_u64(1 << 40));
    }

    #[test]
    fn padded_zeroes_the_tail_even_after_reuse() {
        let mut buf = AlignedBuf::zeroed(8192, Alignment::new(4096).unwrap()).unwrap();
        buf.extend_from_slice(&[0xAB; 5000]).unwrap();
        buf.clear();
        buf.extend_from_slice(&[1, 2, 3]).unwrap();
        let padded = buf.padded().unwrap();
        assert_eq!(padded.len(), 4096);
        assert_eq!(&padded[..3], &[1, 2, 3]);
        assert!(padded[3..].iter().all(|&b| b == 0));
    }

    #[test]
    fn overflow_is_refused() {
        let mut buf = AlignedBuf::zeroed(512, Alignment::new(512).unwrap()).unwrap();
        buf.extend_from_slice(&[0; 500]).unwrap();
        assert_eq!(
            buf.extend_from_slice(&[0; 13]),
            Err(BufError::Full {
                requested: 13,
                available: 12
            })
        );
        assert!(buf.set_len(513).is_err());
        assert!(AlignedBuf::zeroed(MAX_BUFFER + 1, Alignment::BYTE).is_err());
    }

    proptest! {
        #[test]
        fn start_and_capacity_are_aligned(cap in 0usize..300_000, shift in 0u32..17) {
            let align = Alignment::new(1 << shift).unwrap();
            let mut buf = AlignedBuf::zeroed(cap, align).unwrap();
            prop_assert!(buf.capacity() >= cap);
            prop_assert!(align.is_aligned(buf.capacity()));
            let region = buf.as_mut_capacity();
            prop_assert_eq!(region.len() % align.get(), 0);
            prop_assert_eq!(region.as_ptr().addr() % align.get(), 0);
        }
    }
}
