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
        len.div_ceil(self.data)
            .checked_next_multiple_of(2)
            .map(|c| c.max(2))
            .ok_or(EcError::TooLarge(len))
    }

    /// All chunks of `block`, in index order: its data chunks, then the parity chunks.
    pub fn encode(&self, block: &[u8]) -> Result<Vec<Vec<u8>>, EcError> {
        let c = self.chunk_len(block.len())?;
        let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(self.width());
        for i in 0..self.data {
            let start = i.saturating_mul(c).min(block.len());
            let end = start.saturating_add(c).min(block.len());
            let mut chunk = Vec::with_capacity(c);
            chunk.extend_from_slice(block.get(start..end).unwrap_or_default());
            chunk.resize(c, 0);
            chunks.push(chunk);
        }
        let parity = self.parity_of(&chunks, c)?;
        chunks.extend(parity);
        Ok(chunks)
    }

    /// The parity chunks of `data`, which are the code's data chunks, each `c` bytes.
    fn parity_of(&self, data: &[Vec<u8>], c: usize) -> Result<Vec<Vec<u8>>, EcError> {
        guarded(|| {
            let mut encoder = ReedSolomonEncoder::new(self.data, self.parity, c)?;
            for chunk in data {
                encoder.add_original_shard(chunk)?;
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

    /// The chunks at `wanted`, rebuilt from the chunks in `present`, given as (index, bytes).
    pub fn rebuild(
        &self,
        present: &[(usize, &[u8])],
        wanted: &[usize],
    ) -> Result<Vec<Vec<u8>>, EcError> {
        let c = self.check(present)?;
        let data = self.data_chunks(present, c)?;
        let parity = if wanted.iter().any(|&w| w >= self.data) {
            self.parity_of(&data, c)?
        } else {
            Vec::new()
        };
        wanted
            .iter()
            .map(|&w| {
                let chunk = match w.checked_sub(self.data) {
                    None => data.get(w),
                    Some(p) => parity.get(p),
                };
                chunk.cloned().ok_or(EcError::ChunkIndex {
                    index: w,
                    width: self.width(),
                })
            })
            .collect()
    }

    /// The block of `len` bytes from any `data` of its chunks, given as (index, bytes).
    pub fn decode(&self, present: &[(usize, &[u8])], len: usize) -> Result<Vec<u8>, EcError> {
        let c = self.check(present)?;
        if self.chunk_len(len)? != c {
            return Err(EcError::ChunkLength {
                index: present.first().map_or(0, |p| p.0),
                len: c,
                expected: self.chunk_len(len)?,
            });
        }
        let data = self.data_chunks(present, c)?;
        let mut block = Vec::with_capacity(len);
        for chunk in &data {
            let take = len.saturating_sub(block.len()).min(chunk.len());
            block.extend_from_slice(chunk.get(..take).unwrap_or_default());
        }
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

    /// Every data chunk, from `present` where it is there and restored from the others where
    /// it is not.
    fn data_chunks(&self, present: &[(usize, &[u8])], c: usize) -> Result<Vec<Vec<u8>>, EcError> {
        let mut data: Vec<Option<Vec<u8>>> = vec![None; self.data];
        for &(index, bytes) in present {
            if let Some(slot) = data.get_mut(index) {
                *slot = Some(bytes.to_vec());
            }
        }
        if data.iter().all(Option::is_some) {
            return Ok(data.into_iter().flatten().collect());
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
            data.iter()
                .enumerate()
                .map(|(i, chunk)| match chunk {
                    Some(chunk) => Ok(chunk.clone()),
                    None => result
                        .restored_original(i)
                        .map(<[u8]>::to_vec)
                        .ok_or_else(|| EcError::Library(format!("data chunk {i} not restored"))),
                })
                .collect()
        })
    }
}

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
        for (data, parity) in [(2, 1), (3, 2), (4, 2), (6, 3), (8, 4), (10, 4), (9, 6)] {
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
