//! How an object's bytes lie on the way down (docs/design/gateway.md §1): the plaintext sealed
//! in 64 KiB segments, the sealed stream cut into blocks of whole segments, and each block
//! stored in its scheme's chunks.

use std::ops::Range;

use mantle_ec::durability::Scheme;
use mantle_s3::seal;

/// Chunks of up to 8 MiB: Tectonic's typical chunk, nine of them to an RS(9,6) block of
/// 72 MiB (docs/research/01 §1.14).
pub const CHUNK: u64 = 8 << 20;

/// A sealed segment's stored bytes: 64 KiB of plaintext and its tag.
pub const SEALED: u64 = (seal::SEGMENT as u64) + (seal::TAG as u64);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    #[error("a length past what the layout can address")]
    Overflow,
    #[error("a range outside the object")]
    Range,
    /// A scheme of no chunks, or of more data or parity chunks than a block's header counts.
    #[error("a scheme a block header cannot describe")]
    Width,
    #[error(transparent)]
    Code(#[from] mantle_ec::EcError),
}

/// Where a file's stored bytes lie: its blocks' scheme, and how many sealed segments a block
/// holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    scheme: Scheme,
    per_block: u64,
}

/// The part of a block a range of plaintext needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Piece {
    /// The block's place in the file.
    pub block: u64,
    /// The file's segments of the range the block holds.
    pub segments: Range<u64>,
    /// Their stored bytes, as offsets in the block.
    pub stored: Range<u64>,
}

impl Layout {
    /// The layout of files stored in `scheme`: a block holds as many whole segments as its
    /// data chunks hold at the chunk size, at least one.
    pub fn new(scheme: Scheme) -> Result<Self, LayoutError> {
        let described = match scheme {
            Scheme::Copies(n) => n.checked_sub(1).is_some_and(|p| u8::try_from(p).is_ok()),
            Scheme::Rs(code) => {
                u8::try_from(code.data()).is_ok() && u8::try_from(code.parity()).is_ok()
            }
        };
        if !described {
            return Err(LayoutError::Width);
        }
        let needed = u64::try_from(scheme.needed()).map_err(|_| LayoutError::Overflow)?;
        let per_block = needed
            .checked_mul(CHUNK)
            .ok_or(LayoutError::Overflow)?
            .checked_div(SEALED)
            .ok_or(LayoutError::Overflow)?
            .max(1);
        Ok(Self { scheme, per_block })
    }

    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// Segments a block holds, the last block excepted.
    pub fn segments_per_block(&self) -> u64 {
        self.per_block
    }

    /// Blocks a file of `plain` plaintext bytes is stored in.
    pub fn blocks(&self, plain: u64) -> u64 {
        seal::segments(plain).div_ceil(self.per_block)
    }

    /// The stored length of segment `s` of a file of `plain` bytes: a whole sealed segment,
    /// but for the last, whatever the file holds after its whole segments, sealed.
    pub fn segment_len(&self, plain: u64, s: u64) -> Result<u64, LayoutError> {
        let last = seal::segments(plain)
            .checked_sub(1)
            .ok_or(LayoutError::Range)?;
        if s < last {
            return Ok(SEALED);
        }
        if s > last {
            return Err(LayoutError::Range);
        }
        let before = last.checked_mul(SEALED).ok_or(LayoutError::Overflow)?;
        seal::sealed_len(plain)
            .and_then(|all| all.checked_sub(before))
            .ok_or(LayoutError::Overflow)
    }

    /// The segments block `block` of a file of `plain` bytes holds.
    pub fn block_segments(&self, plain: u64, block: u64) -> Result<Range<u64>, LayoutError> {
        let segments = seal::segments(plain);
        let first = block
            .checked_mul(self.per_block)
            .ok_or(LayoutError::Overflow)?;
        if first >= segments {
            return Err(LayoutError::Range);
        }
        let end = first
            .checked_add(self.per_block)
            .ok_or(LayoutError::Overflow)?
            .min(segments);
        Ok(first..end)
    }

    /// The stored length of block `block` of a file of `plain` bytes.
    pub fn block_len(&self, plain: u64, block: u64) -> Result<u64, LayoutError> {
        let segments = self.block_segments(plain, block)?;
        self.stored(plain, block, segments).map(|r| r.end)
    }

    /// The stored bytes of `segments`, which block `block` holds, as offsets in the block.
    fn stored(
        &self,
        plain: u64,
        block: u64,
        segments: Range<u64>,
    ) -> Result<Range<u64>, LayoutError> {
        let first = block
            .checked_mul(self.per_block)
            .ok_or(LayoutError::Overflow)?;
        let last = segments.end.checked_sub(1).ok_or(LayoutError::Range)?;
        let offset = |s: u64| {
            s.checked_sub(first)
                .and_then(|i| i.checked_mul(SEALED))
                .ok_or(LayoutError::Range)
        };
        let start = offset(segments.start)?;
        let end = offset(last)?
            .checked_add(self.segment_len(plain, last)?)
            .ok_or(LayoutError::Overflow)?;
        Ok(start..end)
    }

    /// The length of each chunk a block of `len` stored bytes is kept in: the block itself for
    /// copies, and the code's chunk length for a code.
    pub fn chunk_len(&self, len: u64) -> Result<u64, LayoutError> {
        match self.scheme {
            Scheme::Copies(_) => Ok(len),
            Scheme::Rs(code) => {
                let len = usize::try_from(len).map_err(|_| LayoutError::Overflow)?;
                u64::try_from(code.chunk_len(len)?).map_err(|_| LayoutError::Overflow)
            }
        }
    }

    /// The pieces of blocks that hold plaintext bytes `[from, to)` of a file of `plain` bytes,
    /// in order: the segments that cover the range, grouped by block, and their stored bytes.
    /// An empty range needs nothing.
    pub fn pieces(&self, plain: u64, from: u64, to: u64) -> Result<Vec<Piece>, LayoutError> {
        if from > to || to > plain {
            return Err(LayoutError::Range);
        }
        if from == to {
            return Ok(Vec::new());
        }
        let segment = u64::try_from(seal::SEGMENT).map_err(|_| LayoutError::Overflow)?;
        let first = from.checked_div(segment).ok_or(LayoutError::Overflow)?;
        let end = to.div_ceil(segment).min(seal::segments(plain));
        let mut pieces = Vec::new();
        let mut at = first;
        while at < end {
            let block = at
                .checked_div(self.per_block)
                .ok_or(LayoutError::Overflow)?;
            let holds = self.block_segments(plain, block)?;
            let segments = at..holds.end.min(end);
            let stored = self.stored(plain, block, segments.clone())?;
            at = segments.end;
            pieces.push(Piece {
                block,
                segments,
                stored,
            });
        }
        Ok(pieces)
    }

    /// Where stored bytes `stored` of a block of `len` bytes lie in its chunks: the whole range
    /// in any copy, or, for a code, each data chunk's part of it, in order, as (chunk index,
    /// offsets in the chunk).
    pub fn spans(
        &self,
        len: u64,
        stored: Range<u64>,
    ) -> Result<Vec<(usize, Range<u64>)>, LayoutError> {
        if stored.start > stored.end || stored.end > len {
            return Err(LayoutError::Range);
        }
        let Scheme::Rs(_) = self.scheme else {
            return Ok(vec![(0, stored)]);
        };
        let chunk = self.chunk_len(len)?;
        let mut spans = Vec::new();
        let mut at = stored.start;
        while at < stored.end {
            let index = at.checked_div(chunk).ok_or(LayoutError::Overflow)?;
            let base = index.checked_mul(chunk).ok_or(LayoutError::Overflow)?;
            let end = base
                .checked_add(chunk)
                .ok_or(LayoutError::Overflow)?
                .min(stored.end);
            let offset = at.checked_sub(base).ok_or(LayoutError::Overflow)?;
            let upto = end.checked_sub(base).ok_or(LayoutError::Overflow)?;
            spans.push((
                usize::try_from(index).map_err(|_| LayoutError::Overflow)?,
                offset..upto,
            ));
            at = end;
        }
        Ok(spans)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The largest body one request carries fits a file's extents under every layout: the
    /// layout with the smallest blocks, one copy, holds 127 segments a block, so 5 GiB takes
    /// 646 blocks of the 10,000 a file holds (audit B08).
    #[test]
    fn the_largest_upload_fits_a_file_under_every_layout() {
        let smallest = Layout::new(Scheme::Copies(1)).unwrap();
        assert_eq!(smallest.segments_per_block(), 127);
        let blocks = smallest.blocks(mantle_s3::body::MAX_UPLOAD);
        assert_eq!(blocks, 646);
        assert!(blocks <= mantle_meta::file::MAX_EXTENTS as u64);
        for scheme in [
            Scheme::Copies(2),
            Scheme::Copies(3),
            Scheme::Rs(mantle_ec::Code::new(6, 3).unwrap()),
            Scheme::Rs(mantle_ec::Code::new(9, 6).unwrap()),
        ] {
            let layout = Layout::new(scheme).unwrap();
            assert!(layout.segments_per_block() >= smallest.segments_per_block());
        }
    }
    use mantle_ec::Code;
    use proptest::prelude::*;

    fn schemes() -> Vec<Layout> {
        let mut out = vec![Layout::new(Scheme::Copies(3)).unwrap()];
        for (data, parity) in mantle_ec::CODES {
            out.push(Layout::new(Scheme::Rs(Code::new(data, parity).unwrap())).unwrap());
        }
        out
    }

    /// A block holds whole segments within its data chunks at the chunk size: RS(9,6)'s nine
    /// 8 MiB chunks hold 1,151 segments, about the 72 MiB block Tectonic stores in it.
    #[test]
    fn a_block_holds_whole_segments_within_its_chunks() {
        let rs96 = Layout::new(Scheme::Rs(Code::new(9, 6).unwrap())).unwrap();
        assert_eq!(rs96.segments_per_block(), 9 * CHUNK / SEALED);
        assert_eq!(rs96.segments_per_block(), 1151);
        let copies = Layout::new(Scheme::Copies(3)).unwrap();
        assert_eq!(copies.segments_per_block(), 127);
        assert_eq!(Layout::new(Scheme::Copies(0)), Err(LayoutError::Width));
        assert_eq!(Layout::new(Scheme::Copies(257)), Err(LayoutError::Width));
        assert!(Layout::new(Scheme::Copies(256)).is_ok());
        for l in schemes() {
            let full = l.segments_per_block() * SEALED;
            let chunk = l.chunk_len(full).unwrap();
            assert!(chunk <= CHUNK, "{l:?}: {chunk}");
        }
        // An empty file is one empty segment, sealed: its tag.
        assert_eq!(copies.blocks(0), 1);
        assert_eq!(copies.block_len(0, 0).unwrap(), 16);
        assert_eq!(copies.pieces(0, 0, 0).unwrap(), []);
    }

    proptest! {
        /// The blocks hold the sealed file exactly, each within its scheme's chunks, and the
        /// pieces of a range are its segments, each in the block that holds it.
        #[test]
        fn blocks_and_pieces_cover_the_sealed_file(
            which in 0usize..8,
            plain in prop_oneof![0u64..300_000, 0u64..40_000_000],
            a in any::<u64>(),
            b in any::<u64>(),
        ) {
            let l = schemes()[which];
            let blocks = l.blocks(plain);
            let mut total = 0u64;
            for block in 0..blocks {
                let len = l.block_len(plain, block).unwrap();
                prop_assert!(len > 0 && len <= l.segments_per_block() * SEALED);
                total += len;
            }
            prop_assert_eq!(Some(total), seal::sealed_len(plain));
            prop_assert!(l.block_len(plain, blocks).is_err());

            let (from, to) = if plain == 0 { (0, 0) } else {
                let (x, y) = (a % (plain + 1), b % (plain + 1));
                (x.min(y), x.max(y))
            };
            let pieces = l.pieces(plain, from, to).unwrap();
            if from == to {
                prop_assert!(pieces.is_empty());
                return Ok(());
            }
            let segment = seal::SEGMENT as u64;
            let wanted = (from / segment)..to.div_ceil(segment);
            let mut next = wanted.start;
            for p in &pieces {
                prop_assert_eq!(p.segments.start, next);
                next = p.segments.end;
                let holds = l.block_segments(plain, p.block).unwrap();
                prop_assert!(holds.start <= p.segments.start && p.segments.end <= holds.end);
                let lens: u64 = p.segments.clone().map(|s| l.segment_len(plain, s).unwrap()).sum();
                prop_assert_eq!(p.stored.end - p.stored.start, lens);
                prop_assert!(p.stored.end <= l.block_len(plain, p.block).unwrap());
                // Each chunk's part of the piece, in order, covers it exactly.
                let len = l.block_len(plain, p.block).unwrap();
                let spans = l.spans(len, p.stored.clone()).unwrap();
                let chunk = l.chunk_len(len).unwrap();
                let covered: u64 = spans.iter().map(|(_, r)| r.end - r.start).sum();
                prop_assert_eq!(covered, p.stored.end - p.stored.start);
                for (index, r) in &spans {
                    prop_assert!(r.end <= chunk && (*index as u64) < l.scheme().needed() as u64);
                }
            }
            prop_assert_eq!(next, wanted.end);
        }
    }

    #[test]
    fn ranges_outside_the_object_are_refused() {
        let l = Layout::new(Scheme::Copies(1)).unwrap();
        assert_eq!(l.pieces(10, 5, 11), Err(LayoutError::Range));
        assert_eq!(l.pieces(10, 6, 5), Err(LayoutError::Range));
        assert_eq!(l.segment_len(10, 1), Err(LayoutError::Range));
        assert_eq!(l.spans(10, 0..11), Err(LayoutError::Range));
    }
}
