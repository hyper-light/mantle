//! Reed–Solomon erasure coding of a block into chunks (docs/research/04).
//!
//! A block of `len` bytes becomes `data` chunks that hold its bytes in order, chunk `i` being
//! bytes `[i·c, (i+1)·c)` with the last padded with zeros, and `parity` chunks computed from
//! them, all `c` bytes long. Data chunks are contiguous pieces of the block rather than
//! stripes, so a read of a range is usually served by one chunk directly, and
//! reconstruction reads are the exception (Tectonic §6.4; docs/research/04 §0). Any `data`
//! of the `data + parity` chunks rebuild the others.
//!
//! The code is systematic Reed–Solomon over GF(2^16), computed with the Lin–Chung–Han FFT
//! in O(n log n) by `reed-solomon-simd` (docs/research/04 A2, B1.1). Its symbols are two
//! bytes, so chunks are an even number of bytes, never zero. The library returns errors
//! for every invalid input its high-level API accepts; it is still called behind an unwind
//! boundary, as every dependency is (CLAUDE.md §1).
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cast_possible_truncation
    )
)]

pub mod durability;

use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EcError {
    #[error("RS({data},{parity}) is not a code this library supports")]
    Unsupported { data: usize, parity: usize },
    #[error("{present} chunks present; rebuilding needs {needed}")]
    TooFewChunks { present: usize, needed: usize },
    #[error("chunk {index} is {len} bytes; this block's chunks are {expected}")]
    ChunkLength {
        index: usize,
        len: usize,
        expected: usize,
    },
    #[error("chunk index {index} is outside a code of {width} chunks")]
    ChunkIndex { index: usize, width: usize },
    #[error("chunk {0} is given twice")]
    Duplicate(usize),
    #[error("a block of {0} bytes is too large to encode")]
    TooLarge(usize),
    #[error("the Reed–Solomon library failed: {0}")]
    Library(String),
}

impl From<reed_solomon_simd::Error> for EcError {
    fn from(e: reed_solomon_simd::Error) -> Self {
        Self::Library(e.to_string())
    }
}

/// The codes mantle stores blocks in, as `(data, parity)`: each is tested for every loss it
/// tolerates. RS(6,3) through RS(9,6) are the profiles docs/research/04 §R1.2 names for
/// clusters of 9 to 15 failure domains; the narrower ones serve fewer domains.
pub const CODES: [(usize, usize); 7] = [(2, 1), (3, 2), (4, 2), (6, 3), (8, 4), (10, 4), (9, 6)];

/// A systematic Reed–Solomon code of `data` data chunks and `parity` parity chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Code {
    data: usize,
    parity: usize,
}

impl Code {
    pub fn new(data: usize, parity: usize) -> Result<Self, EcError> {
        if data == 0 || parity == 0 || !ReedSolomonEncoder::supports(data, parity) {
            return Err(EcError::Unsupported { data, parity });
        }
        Ok(Self { data, parity })
    }

    pub fn data(&self) -> usize {
        self.data
    }

    pub fn parity(&self) -> usize {
        self.parity
    }

    /// Chunks in all: data and parity.
    pub fn width(&self) -> usize {
        self.data.saturating_add(self.parity)
    }

    /// The length of each chunk of a block of `len` bytes: the block spread over the data
    /// chunks, rounded up to whole two-byte symbols, and at least one symbol.
    pub fn chunk_len(&self, len: usize) -> Result<usize, EcError> {
        // `new` refuses zero data chunks, so the division is defined; checked all the same,
        // since `div_ceil` panics on a zero divisor.
        let whole = len.checked_div(self.data).ok_or(EcError::Unsupported {
            data: self.data,
            parity: self.parity,
        })?;
        let part = usize::from(len.checked_rem(self.data).is_some_and(|r| r > 0));
        whole
            .checked_add(part)
            .and_then(|c| c.checked_next_multiple_of(2))
            .map(|c| c.max(2))
            .ok_or(EcError::TooLarge(len))
    }

    /// The span of a block of `len` bytes that data chunk `i` holds: `[i·c, (i+1)·c)` clipped
    /// to the block. The chunk is those bytes and then zeros to `c`, so a caller holding the
    /// block takes every chunk the block fills as a slice of it, and copies only a last one it
    /// does not fill.
    pub fn data_span(&self, len: usize, i: usize) -> Result<Range<usize>, EcError> {
        if i >= self.data {
            return Err(EcError::ChunkIndex {
                index: i,
                width: self.width(),
            });
        }
        let c = self.chunk_len(len)?;
        let start = i.saturating_mul(c).min(len);
        Ok(start..start.saturating_add(c).min(len))
    }

    /// The parity chunks of `block`, computed from its data chunks where they lie in it: only a
    /// last chunk the block does not fill is copied, to pad it (audit P08).
    pub fn parity_of(&self, block: &[u8]) -> Result<Vec<Vec<u8>>, EcError> {
        let c = self.chunk_len(block.len())?;
        guarded(|| {
            let mut encoder = ReedSolomonEncoder::new(self.data, self.parity, c)?;
            let mut padded = Vec::new();
            for i in 0..self.data {
                let bytes = block
                    .get(self.data_span(block.len(), i)?)
                    .unwrap_or_default();
                if bytes.len() == c {
                    encoder.add_original_shard(bytes)?;
                } else {
                    padded.clear();
                    padded.extend_from_slice(bytes);
                    padded.resize(c, 0);
                    encoder.add_original_shard(&padded)?;
                }
            }
            let result = encoder.encode()?;
            (0..self.parity)
                .map(|i| {
                    result
                        .recovery(i)
                        .map(<[u8]>::to_vec)
                        .ok_or_else(|| EcError::Library(format!("no parity chunk {i}")))
                })
                .collect()
        })
    }

    /// All chunks of `block`, each its own copy, in index order: its data chunks, then the
    /// parity chunks. A caller holding the block in a shared buffer takes the data chunks as
    /// slices of it instead (`data_span`) and only the parity from here (`parity_of`).
    pub fn encode(&self, block: &[u8]) -> Result<Vec<Vec<u8>>, EcError> {
        let c = self.chunk_len(block.len())?;
        // The reservations are fallible: a block too large to hold twice is refused rather
        // than aborting the process.
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        chunks
            .try_reserve_exact(self.width())
            .map_err(|_| EcError::TooLarge(block.len()))?;
        for i in 0..self.data {
            let mut chunk = Vec::new();
            chunk
                .try_reserve_exact(c)
                .map_err(|_| EcError::TooLarge(block.len()))?;
            chunk.extend_from_slice(
                block
                    .get(self.data_span(block.len(), i)?)
                    .unwrap_or_default(),
            );
            chunk.resize(c, 0);
            chunks.push(chunk);
        }
        chunks.extend(self.parity_of(block)?);
        Ok(chunks)
    }

    /// The chunks at `wanted`, rebuilt from the chunks in `present`, given as (index, bytes).
    /// A wanted data chunk is copied from where it is present or restored; the data chunks
    /// are gathered to encode again only when a parity chunk is wanted (audit P08).
    pub fn rebuild(
        &self,
        present: &[(usize, &[u8])],
        wanted: &[usize],
    ) -> Result<Vec<Vec<u8>>, EcError> {
        let c = self.check(present)?;
        let width = self.width();
        if let Some(&index) = wanted.iter().find(|&&w| w >= width) {
            return Err(EcError::ChunkIndex { index, width });
        }
        let parity_wanted = wanted.iter().any(|&w| w >= self.data);
        guarded(|| {
            let mut out: Vec<Option<Vec<u8>>> = vec![None; wanted.len()];
            let mut encoder = if parity_wanted {
                Some(ReedSolomonEncoder::new(self.data, self.parity, c)?)
            } else {
                None
            };
            self.each_data(present, c, &mut |i, chunk| {
                for (slot, _) in out.iter_mut().zip(wanted).filter(|(_, w)| **w == i) {
                    *slot = Some(chunk.to_vec());
                }
                if let Some(encoder) = encoder.as_mut() {
                    encoder.add_original_shard(chunk)?;
                }
                Ok(())
            })?;
            if let Some(mut encoder) = encoder {
                let result = encoder.encode()?;
                for (slot, &w) in out.iter_mut().zip(wanted) {
                    if let Some(p) = w.checked_sub(self.data) {
                        *slot = result.recovery(p).map(<[u8]>::to_vec);
                    }
                }
            }
            out.into_iter()
                .zip(wanted)
                .map(|(chunk, &index)| chunk.ok_or(EcError::ChunkIndex { index, width }))
                .collect()
        })
    }

    /// The block of `len` bytes from any `data` of its chunks, given as (index, bytes). Each
    /// byte of the block is copied once, from a chunk present or restored; before, the data
    /// chunks were copied out and then into the block (audit P08).
    pub fn decode(&self, present: &[(usize, &[u8])], len: usize) -> Result<Vec<u8>, EcError> {
        let c = self.check(present)?;
        if self.chunk_len(len)? != c {
            return Err(EcError::ChunkLength {
                index: present.first().map_or(0, |p| p.0),
                len: c,
                expected: self.chunk_len(len)?,
            });
        }
        // `len` is the caller's record of the block's length: a length too large to allocate
        // is refused rather than aborting the process.
        let mut block = Vec::new();
        block
            .try_reserve_exact(len)
            .map_err(|_| EcError::TooLarge(len))?;
        self.each_data(present, c, &mut |_, chunk| {
            let take = len.saturating_sub(block.len()).min(chunk.len());
            block.extend_from_slice(chunk.get(..take).unwrap_or_default());
            Ok(())
        })?;
        Ok(block)
    }

    /// Checks that `present` names distinct chunks of this code, all of one length, and
    /// enough of them to rebuild the rest; returns the chunk length.
    fn check(&self, present: &[(usize, &[u8])]) -> Result<usize, EcError> {
        let width = self.width();
        let mut seen = vec![false; width];
        let expected = present.first().map_or(0, |p| p.1.len());
        for &(index, bytes) in present {
            let slot = seen
                .get_mut(index)
                .ok_or(EcError::ChunkIndex { index, width })?;
            if *slot {
                return Err(EcError::Duplicate(index));
            }
            *slot = true;
            if bytes.len() != expected || expected == 0 || !expected.is_multiple_of(2) {
                return Err(EcError::ChunkLength {
                    index,
                    len: bytes.len(),
                    expected,
                });
            }
        }
        if present.len() < self.data {
            return Err(EcError::TooFewChunks {
                present: present.len(),
                needed: self.data,
            });
        }
        Ok(expected)
    }

    /// Calls `each` with every data chunk in index order: the one in `present` where it is
    /// there, and the decoder's restoration where it is not, never a copy.
    fn each_data(
        &self,
        present: &[(usize, &[u8])],
        c: usize,
        each: &mut Each<'_>,
    ) -> Result<(), EcError> {
        let mut data: Vec<Option<&[u8]>> = vec![None; self.data];
        for &(index, bytes) in present {
            if let Some(slot) = data.get_mut(index) {
                *slot = Some(bytes);
            }
        }
        if data.iter().all(Option::is_some) {
            for (i, chunk) in data.iter().enumerate() {
                each(i, chunk.unwrap_or_default())?;
            }
            return Ok(());
        }
        guarded(|| {
            let mut decoder = ReedSolomonDecoder::new(self.data, self.parity, c)?;
            for &(index, bytes) in present {
                match index.checked_sub(self.data) {
                    None => decoder.add_original_shard(index, bytes)?,
                    Some(p) => decoder.add_recovery_shard(p, bytes)?,
                }
            }
            let result = decoder.decode()?;
            for (i, chunk) in data.iter().enumerate() {
                let chunk = match chunk {
                    Some(chunk) => chunk,
                    None => result
                        .restored_original(i)
                        .ok_or_else(|| EcError::Library(format!("data chunk {i} not restored")))?,
                };
                each(i, chunk)?;
            }
            Ok(())
        })
    }
}

/// What `each_data` calls with every data chunk: its index and bytes.
type Each<'a> = dyn FnMut(usize, &[u8]) -> Result<(), EcError> + 'a;

/// Runs `f`, turning a panic inside the library into an error.
fn guarded<T>(f: impl FnOnce() -> Result<T, EcError>) -> Result<T, EcError> {
    catch_unwind(AssertUnwindSafe(f))
        .unwrap_or_else(|_| Err(EcError::Library("the library panicked".into())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn block(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    /// Every subset of `k` elements of `0..n`, in lexicographic order.
    fn subsets(n: usize, k: usize) -> Vec<Vec<usize>> {
        let mut out = Vec::new();
        let mut pick: Vec<usize> = (0..k).collect();
        loop {
            out.push(pick.clone());
            let Some(i) = (0..k).rev().find(|&i| pick[i] != i + n - k) else {
                return out;
            };
            pick[i] += 1;
            for j in i + 1..k {
                pick[j] = pick[j - 1] + 1;
            }
        }
    }

    /// Every combination of lost chunks within each code's tolerance is rebuilt exactly,
    /// and the block reads back exactly from what is left.
    #[test]
    fn every_loss_within_tolerance_is_rebuilt() {
        for (data, parity) in CODES {
            let code = Code::new(data, parity).unwrap();
            // A length that leaves the last data chunk partly padded.
            let len = code.chunk_len(1000).unwrap() * data - 3;
            let original = block(len, (data * 100 + parity) as u64);
            let chunks = code.encode(&original).unwrap();
            assert_eq!(chunks.len(), data + parity);
            for lost_count in 1..=parity {
                for lost in subsets(data + parity, lost_count) {
                    let present: Vec<(usize, &[u8])> = (0..data + parity)
                        .filter(|i| !lost.contains(i))
                        .map(|i| (i, chunks[i].as_slice()))
                        .collect();
                    let rebuilt = code.rebuild(&present, &lost).unwrap();
                    for (w, chunk) in lost.iter().zip(&rebuilt) {
                        assert_eq!(chunk, &chunks[*w], "RS({data},{parity}) lost {lost:?}");
                    }
                    assert_eq!(code.decode(&present, len).unwrap(), original);
                }
            }
        }
    }

    /// A block's data chunks are its spans padded with zeros, and its parity what `encode`
    /// gives, so a caller holding the block shares its bytes rather than copying them; a
    /// chunk rebuilt from every data chunk present, and a parity chunk rebuilt from them, is
    /// the chunk encoded.
    #[test]
    fn a_blocks_chunks_are_its_spans_and_its_parity() {
        for (data, parity) in CODES {
            let code = Code::new(data, parity).unwrap();
            let c = code.chunk_len(1000).unwrap();
            for len in [c * data, c * data - 3, 1, 0] {
                let original = block(len, len as u64);
                let chunks = code.encode(&original).unwrap();
                let c = code.chunk_len(len).unwrap();
                for (i, chunk) in chunks.iter().enumerate().take(data) {
                    let span = &original[code.data_span(len, i).unwrap()];
                    assert_eq!(&chunk[..span.len()], span);
                    assert!(chunk[span.len()..].iter().all(|&b| b == 0));
                    assert_eq!(chunk.len(), c);
                }
                assert_eq!(code.parity_of(&original).unwrap(), chunks[data..]);
                let present: Vec<(usize, &[u8])> =
                    (0..data).map(|i| (i, chunks[i].as_slice())).collect();
                let wanted = [data + parity - 1, 0, data + parity - 1];
                let rebuilt = code.rebuild(&present, &wanted).unwrap();
                let want = [&chunks[wanted[0]], &chunks[0], &chunks[wanted[0]]];
                assert!(rebuilt.iter().eq(want));
            }
            assert!(code.data_span(10, data).is_err());
            assert!(code.rebuild(&[], &[data + parity]).is_err());
        }
    }

    #[test]
    fn one_loss_too_many_is_refused() {
        let code = Code::new(4, 2).unwrap();
        let chunks = code.encode(&block(100, 1)).unwrap();
        let present: Vec<(usize, &[u8])> = (3..6).map(|i| (i, chunks[i].as_slice())).collect();
        assert_eq!(
            code.decode(&present, 100),
            Err(EcError::TooFewChunks {
                present: 3,
                needed: 4
            })
        );
    }

    #[test]
    fn inconsistent_chunks_are_refused() {
        let code = Code::new(2, 1).unwrap();
        let chunks = code.encode(&block(100, 2)).unwrap();
        let short = &chunks[1][..10];
        assert!(matches!(
            code.decode(&[(0, &chunks[0]), (1, short)], 100),
            Err(EcError::ChunkLength { index: 1, .. })
        ));
        assert_eq!(
            code.decode(&[(0, &chunks[0]), (0, &chunks[0])], 100),
            Err(EcError::Duplicate(0))
        );
        assert_eq!(
            code.decode(&[(0, &chunks[0]), (7, &chunks[1])], 100),
            Err(EcError::ChunkIndex { index: 7, width: 3 })
        );
        assert!(Code::new(0, 2).is_err());
        assert!(Code::new(2, 0).is_err());
    }

    #[test]
    fn an_empty_block_encodes_and_decodes() {
        let code = Code::new(3, 2).unwrap();
        let chunks = code.encode(&[]).unwrap();
        assert!(chunks.iter().all(|c| c.len() == 2));
        let present: Vec<(usize, &[u8])> = (2..5).map(|i| (i, chunks[i].as_slice())).collect();
        assert_eq!(code.decode(&present, 0).unwrap(), Vec::<u8>::new());
    }

    proptest! {
        #[test]
        fn any_block_survives_any_tolerable_loss(
            data in 1usize..=16, parity in 1usize..=6, len in 0usize..4000,
            seed in any::<u64>(), picks in proptest::collection::vec(any::<usize>(), 0..6)) {
            let code = Code::new(data, parity).unwrap();
            let original = block(len, seed);
            let chunks = code.encode(&original).unwrap();
            let mut lost: Vec<usize> = picks.iter().map(|p| p % (data + parity)).collect();
            lost.sort_unstable();
            lost.dedup();
            lost.truncate(parity);
            let present: Vec<(usize, &[u8])> = (0..data + parity)
                .filter(|i| !lost.contains(i))
                .map(|i| (i, chunks[i].as_slice()))
                .collect();
            prop_assert_eq!(code.decode(&present, len).unwrap(), original);
            let rebuilt = code.rebuild(&present, &lost).unwrap();
            for (w, chunk) in lost.iter().zip(&rebuilt) {
                prop_assert_eq!(chunk, &chunks[*w]);
            }
        }
    }
}
