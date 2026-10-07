//! A bundle's maplet (docs/research/38): a map from a key to the branches of its bundle that may
//! hold it, with one-sided error, in store pages read one a lookup.
//!
//! A maplet is a quotient filter over its keys' 32-bit hashes with each slot widened by a value,
//! the age of the branch holding the key in its bundle (the maplet construction, Bender et al.,
//! arXiv 2510.05518 §3). A hash's top `bucket_bits` pick its bucket and the next bits its
//! remainder; a slot is the remainder and the value in one byte, as the vector quotient filter's
//! 8-bit slots are (Pandey et al., SIGMOD '21, §6.1). Buckets run [`BUCKETS`] to a block, a block
//! being its buckets' counts in unary (each slot a 0, each bucket's end a 1, VQF §3.2), then its
//! slots. Bundles are immutable, so a maplet is built once from its keys' hashes in order and each
//! key has one place: a lookup reads one block, finds its bucket's run with one select, and
//! compares the run's slots, under one on average. A two-choice block (VQF §3.1), which dynamic
//! inserts need, reads two blocks and selects twice in each (benches/maplet.rs: about 330
//! instructions a lookup).
//!
//! Blocks are laid end to end in pages. An index in memory gives each block's page, offset and
//! header length in one word, about a bit a key, so a lookup reads its index word and then its
//! block's line: one miss a bundle where filters take one a branch, with no chain of reads
//! through a page table (benches/maplet.rs measured the chain at about 130 cycles in L2).

use crate::error::{Error, Malformed};
use crate::fst::bits::select_broadword;

/// Derived: buckets a block holds. A block's headers are its buckets and its slots in bits, read
/// as one 128-bit word: at most 128, so a block holds at most `128 − BUCKETS` slots. At the
/// loads a maplet is built for, at most one key a bucket on average, 32 buckets hold about 32 keys
/// and more than 96 next to never; a build that meets one is refused.
pub const BUCKETS: u32 = 32;
/// Derived: the most slots a block holds, its headers within a 128-bit word.
pub const MOST_SLOTS: u32 = 128 - BUCKETS;
/// Format: a block's index word: its page in the high 16 bits, its headers' bytes less one in
/// the next 4 (at most 16), its offset in the page in the low 12 (a page is at most 4 KiB).
const OFFSET_BITS: u32 = 12;
const HEADER_BITS: u32 = 4;
const PAGE_SHIFT: u32 = OFFSET_BITS + HEADER_BITS;

fn corrupt() -> Error {
    Error::Corruption {
        what: "a maplet page",
        why: Malformed::OutOfRange,
    }
}

/// The bits `n` distinct values need: `ceil(log2 n)`, 0 for 1 or none.
pub fn ceil_log2(n: u64) -> u32 {
    if n <= 1 {
        0
    } else {
        u64::BITS.saturating_sub(n.wrapping_sub(1).leading_zeros())
    }
}

/// A key's maplet hash: the top 32 bits of its xxh3 hash, what a branch keeps for each key.
pub fn hash32(hash: u64) -> u32 {
    u32::try_from(hash >> 32).unwrap_or(0)
}

/// A maplet's dimensions and its blocks' index, as its owner keeps them beside its pages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shape {
    pub bucket_bits: u32,
    pub value_bits: u32,
    pub entries: u64,
    pub pages: u32,
    /// Each block's page, header length and offset ([`PAGE_SHIFT`]).
    pub index: Vec<u32>,
}

/// Where a key's lookup goes: its page, its block's offset and header length there, its bucket
/// in the block and its remainder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Probe {
    pub page: u32,
    offset: u16,
    header: u8,
    bucket: u32,
    rem: u8,
}

impl Shape {
    fn remainder_bits(&self) -> u32 {
        8u32.saturating_sub(self.value_bits)
    }

    /// A hash's bucket and its remainder.
    fn split(&self, h: u32) -> (u32, u8) {
        let bucket = h
            .checked_shr(32u32.saturating_sub(self.bucket_bits))
            .unwrap_or(0);
        let r = self.remainder_bits();
        let rem = h
            .checked_shl(self.bucket_bits)
            .unwrap_or(0)
            .checked_shr(32u32.saturating_sub(r))
            .unwrap_or(0);
        (bucket, u8::try_from(rem).unwrap_or(0))
    }

    fn blocks(&self) -> u32 {
        1u32.checked_shl(self.bucket_bits.saturating_sub(BUCKETS.trailing_zeros()))
            .unwrap_or(u32::MAX)
    }

    /// Where a key's lookup goes, from its xxh3 hash: worked out once, from the index alone.
    pub fn probe(&self, hash: u64) -> Result<Probe, Error> {
        let (bucket, rem) = self.split(hash32(hash));
        let word = usize::try_from(bucket / BUCKETS)
            .ok()
            .and_then(|b| self.index.get(b))
            .copied()
            .ok_or_else(corrupt)?;
        Ok(Probe {
            page: word >> PAGE_SHIFT,
            offset: u16::try_from(word & ((1 << OFFSET_BITS) - 1)).unwrap_or(0),
            header: u8::try_from((word >> OFFSET_BITS) & ((1 << HEADER_BITS) - 1))
                .unwrap_or(0)
                .wrapping_add(1),
            bucket: bucket % BUCKETS,
            rem,
        })
    }

    /// The bytes of memory its index takes.
    pub fn index_bytes(&self) -> usize {
        self.index.len().saturating_mul(4)
    }
}

/// The `k`-th set bit (from 0) of a 128-bit word.
fn select128(m: u128, k: u32) -> u32 {
    let low = u64::try_from(m & u128::from(u64::MAX)).unwrap_or(0);
    let ones = low.count_ones();
    let (w, k, base) = if k < ones {
        (low, k, 0)
    } else {
        (
            u64::try_from(m >> 64).unwrap_or(0),
            k.wrapping_sub(ones),
            64,
        )
    };
    u32::try_from(select_broadword(w, usize::try_from(k).unwrap_or(0)))
        .unwrap_or(0)
        .wrapping_add(base)
}

/// The 16 bytes at `at` of `page` as a little-endian word, zeros past its end.
fn word128(page: &[u8], at: usize) -> u128 {
    match page.get(at..).and_then(<[u8]>::first_chunk::<16>) {
        Some(b) => u128::from_le_bytes(*b),
        None => {
            let mut b = [0u8; 16];
            for (d, s) in b.iter_mut().zip(page.get(at..).unwrap_or_default()) {
                *d = *s;
            }
            u128::from_le_bytes(b)
        }
    }
}

/// The values under which `page`, the probe's page, may hold the probed key, as a mask: bit `v`
/// for value `v`. A key held under value `v` always sets bit `v`.
pub fn lookup(page: &[u8], shape: &Shape, probe: &Probe) -> Result<u64, Error> {
    let offset = usize::from(probe.offset);
    // The block's headers: its buckets' ends and its slots' zeros, within one 128-bit word.
    let meta = word128(page, offset);
    let y = probe.bucket;
    let end_bit = select128(meta, y);
    let below = meta
        & 1u128
            .checked_shl(end_bit)
            .map_or(u128::MAX, |m| m.wrapping_sub(1));
    // The run starts above the last one below its end; the ones below that are the y buckets'
    // ends before it, the rest its earlier slots.
    let start_bit = u128::BITS.wrapping_sub(below.leading_zeros());
    let start = usize::try_from(start_bit.saturating_sub(y)).unwrap_or(usize::MAX);
    let end = usize::try_from(end_bit.saturating_sub(y)).unwrap_or(usize::MAX);
    let base = offset.wrapping_add(usize::from(probe.header));
    let run = page
        .get(base.saturating_add(start)..base.saturating_add(end))
        .ok_or_else(corrupt)?;
    let shift = shape.value_bits;
    let value_mask = 1u8.checked_shl(shift).map_or(0xFF, |m| m.wrapping_sub(1));
    let mut mask = 0u64;
    for &s in run {
        if s.checked_shr(shift).unwrap_or(0) == probe.rem {
            mask |= 1u64.checked_shl(u32::from(s & value_mask)).unwrap_or(0);
        }
    }
    Ok(mask)
}

/// A built maplet held in memory: its shape and its pages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Maplet {
    pub shape: Shape,
    pub pages: Vec<Vec<u8>>,
}

impl Maplet {
    /// The values under which the key of xxh3 hash `hash` may be held, as a mask.
    pub fn route(&self, hash: u64) -> Result<u64, Error> {
        let probe = self.shape.probe(hash)?;
        let page = usize::try_from(probe.page)
            .ok()
            .and_then(|p| self.pages.get(p))
            .ok_or_else(corrupt)?;
        lookup(page, &self.shape, &probe)
    }

    /// The bytes of memory it takes: its pages and its index.
    pub fn bytes(&self) -> usize {
        self.pages
            .iter()
            .map(Vec::len)
            .fold(self.shape.index_bytes(), usize::saturating_add)
    }
}

/// The bucket bits for `entries` keys: at most one key a bucket on average, a block at least.
pub fn bucket_bits(entries: u64) -> u32 {
    ceil_log2(entries).clamp(BUCKETS.trailing_zeros(), 32)
}

/// A build: keys' 32-bit hashes with their values, in ascending hash order, a block at a time
/// into a page; each page handed on when its next block would not fit. Its shape, with the
/// blocks' index, comes back from [`Self::close`].
#[derive(Debug)]
pub struct Builder {
    bucket_bits: u32,
    value_bits: u32,
    payload: usize,
    page: Vec<u8>,
    pages: u32,
    index: Vec<u32>,
    /// The block being gathered: its number and its slots with their buckets.
    block: u32,
    slots: Vec<(u32, u8)>,
    last: Option<u32>,
    entries: u64,
}

impl Builder {
    /// A build of a maplet of `bucket_bits` (from [`bucket_bits`]) and values `value_bits` wide,
    /// in pages of `payload` bytes, at most `1 << OFFSET_BITS`.
    pub fn new(bucket_bits: u32, value_bits: u32, payload: usize) -> Result<Self, Error> {
        if value_bits > 7
            || bucket_bits > 32
            || bucket_bits < BUCKETS.trailing_zeros()
            || payload > 1 << OFFSET_BITS
        {
            return Err(Error::InvalidArgument {
                what: "a maplet's values over 7 bits, its buckets outside 32 to 2^32, or pages over 4 KiB",
            });
        }
        Ok(Self {
            bucket_bits,
            value_bits,
            payload,
            page: Vec::with_capacity(payload),
            pages: 0,
            index: Vec::new(),
            block: 0,
            slots: Vec::new(),
            last: None,
            entries: 0,
        })
    }

    fn shape(&self) -> Shape {
        Shape {
            bucket_bits: self.bucket_bits,
            value_bits: self.value_bits,
            entries: self.entries,
            pages: self.pages,
            index: Vec::new(),
        }
    }

    /// Adds the next key, its hash at or above the last's; `emit` takes each finished page.
    pub fn add(
        &mut self,
        h: u32,
        value: u8,
        emit: &mut impl FnMut(&[u8]) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if self.last.is_some_and(|l| h < l) {
            return Err(Error::InvalidArgument {
                what: "maplet hashes out of order",
            });
        }
        if u32::from(value).checked_shr(self.value_bits).unwrap_or(0) != 0 {
            return Err(Error::InvalidArgument {
                what: "a maplet value wider than its shape",
            });
        }
        self.last = Some(h);
        let (bucket, rem) = self.shape().split(h);
        let block = bucket / BUCKETS;
        // Each round seals a block: at most the maplet's blocks.
        while self.block < block {
            self.seal(emit)?;
        }
        if u32::try_from(self.slots.len()).unwrap_or(u32::MAX) >= MOST_SLOTS {
            return Err(Error::LimitExceeded {
                what: "keys in a maplet block's buckets",
                limit: u64::from(MOST_SLOTS),
            });
        }
        let slot = rem.checked_shl(self.value_bits).unwrap_or(0) | value;
        self.slots.push((bucket % BUCKETS, slot));
        self.entries = self.entries.saturating_add(1);
        Ok(())
    }

    /// Encodes the gathered block into the page (handing the page on first if it would not
    /// fit), indexes it, and starts the next.
    fn seal(&mut self, emit: &mut impl FnMut(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        let mut meta = 0u128;
        let mut bit = 0u32;
        let mut at = 0usize;
        for y in 0..BUCKETS {
            // Each slot of bucket y a 0, then its end a 1.
            while self.slots.get(at).is_some_and(|&(b, _)| b == y) {
                bit = bit.saturating_add(1);
                at = at.saturating_add(1);
            }
            meta |= 1u128.checked_shl(bit).unwrap_or(0);
            bit = bit.saturating_add(1);
        }
        let header = usize::try_from(bit.div_ceil(8)).unwrap_or(usize::MAX);
        let size = header.saturating_add(self.slots.len());
        if self.page.len().saturating_add(size) > self.payload && !self.page.is_empty() {
            self.flush(emit)?;
        }
        if size > self.payload {
            return Err(Error::LimitExceeded {
                what: "a maplet block larger than a page",
                limit: u64::try_from(self.payload).unwrap_or(u64::MAX),
            });
        }
        let limit = |what| Error::LimitExceeded {
            what,
            limit: 1 << (u32::BITS - PAGE_SHIFT),
        };
        let page = self.pages;
        if page >> (u32::BITS - PAGE_SHIFT) != 0 {
            return Err(limit("a maplet's pages"));
        }
        let offset =
            u32::try_from(self.page.len()).map_err(|_| limit("a maplet page's offsets"))?;
        let h =
            u32::try_from(header.wrapping_sub(1)).map_err(|_| limit("a maplet block's headers"))?;
        self.index
            .push((page << PAGE_SHIFT) | (h << OFFSET_BITS) | offset);
        self.page
            .extend_from_slice(meta.to_le_bytes().get(..header).unwrap_or_default());
        self.page.extend(self.slots.iter().map(|&(_, s)| s));
        self.slots.clear();
        self.block = self.block.saturating_add(1);
        Ok(())
    }

    /// Writes the page of the blocks gathered.
    fn flush(&mut self, emit: &mut impl FnMut(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        if self.page.is_empty() {
            return Ok(());
        }
        self.page.resize(self.payload, 0);
        emit(&self.page)?;
        self.page.clear();
        self.pages = self.pages.saturating_add(1);
        Ok(())
    }

    /// Seals the blocks left and writes the last page: the maplet is whole.
    pub fn close(
        mut self,
        emit: &mut impl FnMut(&[u8]) -> Result<(), Error>,
    ) -> Result<Shape, Error> {
        let blocks = self.shape().blocks();
        // Each round seals a block: at most the maplet's blocks.
        while self.block < blocks {
            self.seal(emit)?;
        }
        self.flush(emit)?;
        let mut shape = self.shape();
        shape.index = std::mem::take(&mut self.index);
        Ok(shape)
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    const PAYLOAD: usize = 4076;

    fn hashes(n: usize, seed: u64) -> Vec<u64> {
        let mut x = seed | 1;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            })
            .collect()
    }

    fn build(pairs: &[(u64, u8)], value_bits: u32) -> (Shape, Vec<Vec<u8>>) {
        let mut sorted: Vec<(u32, u8)> = pairs.iter().map(|&(h, v)| (hash32(h), v)).collect();
        sorted.sort_unstable();
        let mut pages = Vec::new();
        let mut emit = |p: &[u8]| {
            pages.push(p.to_vec());
            Ok(())
        };
        let mut b = Builder::new(bucket_bits(sorted.len() as u64), value_bits, PAYLOAD).unwrap();
        for &(h, v) in &sorted {
            b.add(h, v, &mut emit).unwrap();
        }
        let shape = b.close(&mut emit).unwrap();
        (shape, pages)
    }

    fn query(shape: &Shape, pages: &[Vec<u8>], hash: u64) -> u64 {
        let p = shape.probe(hash).unwrap();
        lookup(&pages[p.page as usize], shape, &p).unwrap()
    }

    #[test]
    fn every_key_finds_its_branches_and_others_match_within_the_bound() {
        for (n, branches) in [
            (1usize, 1u8),
            (100, 1),
            (10_000, 3),
            (300_000, 8),
            (300_000, 1),
        ] {
            let vb = ceil_log2(u64::from(branches));
            let pairs: Vec<(u64, u8)> = hashes(n, 0x2545_f491 ^ n as u64)
                .into_iter()
                .enumerate()
                .map(|(i, h)| (h, (i % branches as usize) as u8))
                .collect();
            let (shape, pages) = build(&pairs, vb);
            assert_eq!(shape.pages as usize, pages.len());
            assert_eq!(shape.entries, n as u64);
            for &(h, v) in &pairs {
                assert_ne!(query(&shape, &pages, h) & (1 << v), 0, "n {n}: a key lost");
            }
            // A query compares against its bucket's keys, the load n / 2^b on average, each
            // matching with chance 2^-r. A fixed seed: an exact count.
            let absent = hashes(100_000, 0x9e37_79b9 ^ n as u64);
            let hits = absent
                .iter()
                .filter(|&&h| query(&shape, &pages, h) != 0)
                .count();
            let r = 8 - vb;
            let load = n as f64 / (1u64 << shape.bucket_bits) as f64;
            let expected = 100_000.0 * load / (1u64 << r) as f64;
            println!("n {n} branches {branches}: {hits} false matches, expected {expected:.0}");
            // Small counts spread by about their square root.
            assert!(
                hits as f64 <= expected * 1.15 + 3.0 * expected.sqrt() + 4.0,
                "n {n}: {hits} > {expected:.0}"
            );
        }
    }

    #[test]
    fn every_page_fits_and_the_index_names_every_block_in_order() {
        let pairs: Vec<(u64, u8)> = hashes(200_000, 5).into_iter().map(|h| (h, 0)).collect();
        let (shape, pages) = build(&pairs, 0);
        assert!(pages.iter().all(|p| p.len() == PAYLOAD));
        assert_eq!(shape.index.len() as u32, shape.blocks());
        // Blocks run in order: each after the last, in its page or a later one.
        let place = |w: u32| (w >> PAGE_SHIFT, w & ((1 << OFFSET_BITS) - 1));
        for pair in shape.index.windows(2) {
            assert!(place(pair[0]) < place(pair[1]), "{:?}", pair);
        }
        assert!(
            shape.index_bytes() * 8 <= 200_000 * 2,
            "index over two bits a key"
        );
    }

    #[test]
    fn out_of_order_or_too_wide_values_are_refused() {
        let mut b = Builder::new(10, 1, PAYLOAD).unwrap();
        let mut emit = |_: &[u8]| Ok(());
        b.add(5, 0, &mut emit).unwrap();
        assert!(b.add(4, 0, &mut emit).is_err());
        assert!(b.add(6, 2, &mut emit).is_err(), "value wider than one bit");
        assert!(Builder::new(10, 8, PAYLOAD).is_err());
        assert!(Builder::new(10, 0, 8192).is_err(), "pages over 4 KiB");
    }
}
