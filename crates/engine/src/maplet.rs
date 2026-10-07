//! A bundle's maplet (docs/research/38): a map from a key to the branches of its bundle that may
//! hold it, with one-sided error, in store pages read one a lookup.
//!
//! A maplet over `n` keys keeps each key's fingerprint, the top `hash_bits` of its xxh3 hash, with
//! a value: the age of the branch holding it in its bundle. Fingerprints are sorted; their top
//! `bucket_bits` pick a bucket, the rest are the remainder. Buckets run in pages, `2^k` a page,
//! `k` the most that lets every page fit, found from a counting pass, so a build streams its
//! sorted entries twice and never holds them, and a merge of maplets reads theirs twice
//! (SplinterDB's routing filter is built so: research/34 §1).
//!
//! A page is blocks of [`BLOCK_BUCKETS`] buckets after a table giving each block's offset and
//! entry count (u16 each). A block is its buckets' headers in unary (each entry a 0, each
//! bucket's end a 1, research/34's "unary bucket encoding") followed by its entries' remainder
//! and value, packed, so a lookup reads its block's table entry and the block: two cache lines
//! where headers and entries kept apart took three (benches/maplet.rs). It ORs in the value of
//! each entry of its bucket whose remainder matches: a superset of the branches holding the key,
//! never missing one.

use crate::error::{Error, Malformed};
use crate::fst::bits::select_in_word;

/// Cited: remainder bits at the largest bundle. A lookup compares against a bucket's entries, on
/// average the load `n / 2^b <= 1`, so its false-positive rate is about `load · 2^−r`: at `r = 7`,
/// at most 0.78%, the Bloom filters' 0.8% a probe (research/38 §3; Broder and Mitzenmacher 2004
/// §2.1 for the filters'), now once a bundle rather than once a branch.
pub const REMAINDER_BITS: u32 = 7;
/// Derived: buckets a block holds. At a load of one, 64 buckets' headers and entries of 10 bits
/// take about 600 bits, a cache line, and a lookup scans its block's headers in about two words.
pub const BLOCK_BUCKETS: u64 = 64;
/// Format: a block's table entry, its byte offset and its entries, u16 each.
const TABLE_BYTES: usize = 4;

fn corrupt(why: Malformed) -> Error {
    Error::Corruption {
        what: "a maplet page",
        why,
    }
}

/// The bits `n` distinct values need an index of: `ceil(log2 n)`, 0 for 1 or none.
pub fn ceil_log2(n: u64) -> u32 {
    if n <= 1 {
        0
    } else {
        u64::BITS.saturating_sub(n.wrapping_sub(1).leading_zeros())
    }
}

/// A store's fingerprint width, for bundles of at most `most` entries: their bucket bits and
/// [`REMAINDER_BITS`], at most 64. Every maplet a store builds uses it, so any of them merge.
pub fn hash_bits(most: u64) -> u32 {
    ceil_log2(most)
        .saturating_add(REMAINDER_BITS)
        .clamp(1, u64::BITS)
}

/// A maplet's dimensions, as its owner records them beside its pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shape {
    pub hash_bits: u32,
    pub bucket_bits: u32,
    pub value_bits: u32,
    /// `k`: a page holds `2^k` buckets.
    pub page_bucket_bits: u32,
    pub pages: u32,
    pub entries: u64,
}

impl Shape {
    fn remainder_bits(&self) -> u32 {
        self.hash_bits.saturating_sub(self.bucket_bits)
    }

    fn entry_bits(&self) -> u32 {
        self.remainder_bits().saturating_add(self.value_bits)
    }

    fn page_buckets(&self) -> u64 {
        1u64.checked_shl(self.page_bucket_bits).unwrap_or(0)
    }

    fn blocks(&self) -> u64 {
        self.page_buckets().div_ceil(BLOCK_BUCKETS).max(1)
    }

    /// The bytes a block of `count` entries takes: its headers then its entries, each whole
    /// bytes.
    fn block_bytes(&self, count: u64) -> u64 {
        let headers = count.saturating_add(BLOCK_BUCKETS).div_ceil(8);
        headers.saturating_add(
            count
                .saturating_mul(u64::from(self.entry_bits()))
                .div_ceil(8),
        )
    }

    /// A key's fingerprint, from its xxh3 hash.
    pub fn fingerprint(&self, hash: u64) -> u64 {
        hash.checked_shr(u64::BITS.saturating_sub(self.hash_bits))
            .unwrap_or(0)
    }

    fn bucket(&self, fp: u64) -> u64 {
        fp.checked_shr(self.remainder_bits()).unwrap_or(0)
    }

    /// The page a key's lookup reads.
    pub fn page_of(&self, hash: u64) -> u32 {
        let page = self
            .bucket(self.fingerprint(hash))
            .checked_shr(self.page_bucket_bits)
            .unwrap_or(0);
        u32::try_from(page).unwrap_or(u32::MAX)
    }
}

/// The first pass of a build: the entries a block, from which [`Self::shape`] picks the pages'
/// size.
#[derive(Debug)]
pub struct Plan {
    hash_bits: u32,
    bucket_bits: u32,
    value_bits: u32,
    blocks: Vec<u16>,
    entries: u64,
    last: Option<u64>,
}

impl Plan {
    /// A plan for `entries` fingerprints of `hash_bits`, their values `value_bits` wide.
    pub fn new(hash_bits: u32, value_bits: u32, entries: u64) -> Result<Self, Error> {
        if hash_bits == 0 || hash_bits > u64::BITS || value_bits > 6 {
            return Err(Error::InvalidArgument {
                what: "a maplet of no fingerprint bits, over 64, or values over 6 bits",
            });
        }
        let bucket_bits = ceil_log2(entries).min(hash_bits);
        let buckets = 1u64.checked_shl(bucket_bits).unwrap_or(u64::MAX);
        let blocks =
            usize::try_from(buckets.div_ceil(BLOCK_BUCKETS)).map_err(|_| Error::LimitExceeded {
                what: "a maplet's buckets",
                limit: usize::MAX as u64,
            })?;
        Ok(Self {
            hash_bits,
            bucket_bits,
            value_bits,
            blocks: vec![0; blocks.max(1)],
            entries: 0,
            last: None,
        })
    }

    /// Counts the next fingerprint, in ascending order.
    pub fn count(&mut self, fp: u64) -> Result<(), Error> {
        if self.last.is_some_and(|l| fp < l) {
            return Err(Error::InvalidArgument {
                what: "maplet fingerprints out of order",
            });
        }
        self.last = Some(fp);
        let shape = self.bare();
        let block = usize::try_from(shape.bucket(fp) / BLOCK_BUCKETS).unwrap_or(usize::MAX);
        let c = self.blocks.get_mut(block).ok_or(Error::InvalidArgument {
            what: "a maplet fingerprint wider than its plan",
        })?;
        *c = c.checked_add(1).ok_or(Error::LimitExceeded {
            what: "a maplet block's entries",
            limit: u64::from(u16::MAX),
        })?;
        self.entries = self.entries.saturating_add(1);
        Ok(())
    }

    fn bare(&self) -> Shape {
        Shape {
            hash_bits: self.hash_bits,
            bucket_bits: self.bucket_bits,
            value_bits: self.value_bits,
            page_bucket_bits: 0,
            pages: 0,
            entries: self.entries,
        }
    }

    /// The bytes a page of these blocks takes in `shape`.
    fn page_bytes(shape: &Shape, blocks: &[u16]) -> u64 {
        let table = u64::try_from(blocks.len().saturating_mul(TABLE_BYTES)).unwrap_or(u64::MAX);
        blocks.iter().fold(table, |sum, &c| {
            sum.saturating_add(shape.block_bytes(u64::from(c)))
        })
    }

    /// The shape for pages of `payload` bytes: the most buckets a page that lets every page fit.
    pub fn shape(&self, payload: usize) -> Result<Shape, Error> {
        let payload = u64::try_from(payload).unwrap_or(u64::MAX);
        let least = ceil_log2(BLOCK_BUCKETS).min(self.bucket_bits);
        // Each round tries a page half the last one's buckets: at most the bucket bits of rounds.
        for k in (least..=self.bucket_bits).rev() {
            let shape = Shape {
                page_bucket_bits: k,
                ..self.bare()
            };
            let per = usize::try_from(shape.blocks()).unwrap_or(usize::MAX);
            let fits = self.blocks.chunks(per).all(|page| {
                let bytes = Self::page_bytes(&shape, page);
                bytes <= payload && bytes <= u64::from(u16::MAX)
            });
            if fits {
                let pages = self.blocks.len().div_ceil(per);
                return Ok(Shape {
                    pages: u32::try_from(pages).map_err(|_| Error::LimitExceeded {
                        what: "a maplet's pages",
                        limit: u64::from(u32::MAX),
                    })?,
                    ..shape
                });
            }
        }
        Err(Error::LimitExceeded {
            what: "a maplet block larger than a page",
            limit: payload,
        })
    }
}

/// Reads the 64 bits at bit `at` of `bytes`, little-endian, zeros past their end.
fn word_at(bytes: &[u8], at: u64) -> u64 {
    let byte = usize::try_from(at / 8).unwrap_or(usize::MAX);
    let shift = u32::try_from(at % 8).unwrap_or(0);
    let wide = match bytes.get(byte..).and_then(<[u8]>::first_chunk::<16>) {
        Some(b) => u128::from_le_bytes(*b),
        None => {
            let mut b = [0u8; 16];
            let tail = bytes.get(byte..).unwrap_or_default();
            for (d, s) in b.iter_mut().zip(tail) {
                *d = *s;
            }
            u128::from_le_bytes(b)
        }
    };
    u64::try_from((wide >> shift) & u128::from(u64::MAX)).unwrap_or(0)
}

/// The low `width` bits (at most 64) at bit `at` of `bytes`.
fn get_bits(bytes: &[u8], at: u64, width: u32) -> u64 {
    let w = word_at(bytes, at);
    if width >= 64 {
        w
    } else {
        w & (1u64 << width).wrapping_sub(1)
    }
}

/// ORs `value`'s low `width` bits in at bit `at` of `bytes`, which were zero there.
fn put_bits(bytes: &mut [u8], at: u64, width: u32, value: u64) -> Option<()> {
    if width == 0 {
        return Some(());
    }
    let byte = usize::try_from(at / 8).ok()?;
    let shift = u32::try_from(at % 8).ok()?;
    let n = usize::try_from(shift.saturating_add(width).div_ceil(8)).ok()?;
    let mask = if width >= 64 {
        u64::MAX
    } else {
        (1u64 << width).wrapping_sub(1)
    };
    let v = u128::from(value & mask) << shift;
    for (i, b) in bytes
        .get_mut(byte..byte.checked_add(n)?)?
        .iter_mut()
        .enumerate()
    {
        let part = v.checked_shr(u32::try_from(i).ok()?.checked_mul(8)?)?;
        *b |= u8::try_from(part & 0xFF).ok()?;
    }
    Some(())
}

/// A block's place in its page: the bit its headers start at, the bit its entries start at, and
/// its entries.
#[derive(Clone, Copy, Debug)]
struct Block {
    headers: u64,
    entries: u64,
    count: u64,
}

/// Block `i` of `page`, from the page's table.
fn block(page: &[u8], i: u64) -> Result<Block, Error> {
    let at = usize::try_from(i)
        .ok()
        .and_then(|i| i.checked_mul(TABLE_BYTES))
        .ok_or(corrupt(Malformed::OutOfRange))?;
    let t = page
        .get(at..)
        .and_then(<[u8]>::first_chunk::<4>)
        .ok_or(corrupt(Malformed::OutOfRange))?;
    let offset = u64::from(u16::from_le_bytes([t[0], t[1]]));
    let count = u64::from(u16::from_le_bytes([t[2], t[3]]));
    let headers = offset.saturating_mul(8);
    let entries = headers.saturating_add(
        count
            .saturating_add(BLOCK_BUCKETS)
            .div_ceil(8)
            .saturating_mul(8),
    );
    Ok(Block {
        headers,
        entries,
        count,
    })
}

/// The second pass of a build: entries in ascending fingerprint order, written a page at a time
/// into a buffer handed on before the next begins.
#[derive(Debug)]
pub struct Writer {
    shape: Shape,
    blocks: Vec<u16>,
    page: Vec<u8>,
    payload: usize,
    /// The page being written; its block being written, that block's entries written and the
    /// buckets whose ends it has written.
    index: u64,
    block: u64,
    filled: u64,
    ended: u64,
    at: Block,
}

impl Writer {
    /// A writer of `plan`'s maplet in `shape`, pages of `payload` bytes.
    pub fn new(plan: &Plan, shape: Shape, payload: usize) -> Self {
        let mut w = Self {
            shape,
            blocks: plan.blocks.clone(),
            page: vec![0; payload],
            payload,
            index: 0,
            block: 0,
            filled: 0,
            ended: 0,
            at: Block {
                headers: 0,
                entries: 0,
                count: 0,
            },
        };
        w.open();
        w
    }

    fn per(&self) -> u64 {
        self.shape.blocks()
    }

    /// Starts page `index`: its table written from the plan's counts, its first block open.
    fn open(&mut self) {
        self.page.clear();
        self.page.resize(self.payload, 0);
        let per = self.per();
        let first = self.index.saturating_mul(per);
        let mut offset = per.saturating_mul(TABLE_BYTES as u64);
        for b in 0..per {
            let count = usize::try_from(first.saturating_add(b))
                .ok()
                .and_then(|i| self.blocks.get(i))
                .copied()
                .unwrap_or(0);
            let slot = usize::try_from(b)
                .unwrap_or(usize::MAX)
                .saturating_mul(TABLE_BYTES);
            if let (Ok(o), Some(t)) = (
                u16::try_from(offset),
                self.page.get_mut(slot..slot.saturating_add(TABLE_BYTES)),
            ) {
                let mut e = [0u8; TABLE_BYTES];
                e[..2].copy_from_slice(&o.to_le_bytes());
                e[2..].copy_from_slice(&count.to_le_bytes());
                t.copy_from_slice(&e);
            }
            offset = offset.saturating_add(self.shape.block_bytes(u64::from(count)));
        }
        self.block = 0;
        self.enter();
    }

    /// Opens block `self.block` of the page.
    fn enter(&mut self) {
        self.filled = 0;
        self.ended = 0;
        if let Ok(b) = block(&self.page, self.block) {
            self.at = b;
        }
    }

    /// Ends the open block's buckets up to `local`, exclusive.
    fn end_to(&mut self, local: u64) -> Option<()> {
        // Each round ends a bucket: at most a block's buckets.
        while self.ended < local {
            let bit = self
                .at
                .headers
                .checked_add(self.filled)?
                .checked_add(self.ended)?;
            put_bits(&mut self.page, bit, 1, 1)?;
            self.ended = self.ended.checked_add(1)?;
        }
        Some(())
    }

    /// Ends the open block and every block before `block`.
    fn move_to(&mut self, block: u64) -> Option<()> {
        // Each round ends a block: at most a page's blocks.
        while self.block < block {
            self.end_to(BLOCK_BUCKETS)?;
            if self.filled != self.at.count {
                return None;
            }
            self.block = self.block.checked_add(1)?;
            self.enter();
        }
        Some(())
    }

    /// Adds the next entry; `emit` takes each page as it is finished.
    pub fn add(
        &mut self,
        fp: u64,
        value: u8,
        emit: &mut impl FnMut(&[u8]) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let bad = || Error::InvalidArgument {
            what: "a maplet entry outside its plan",
        };
        let bucket = self.shape.bucket(fp);
        let page = bucket.checked_shr(self.shape.page_bucket_bits).unwrap_or(0);
        // Each round finishes a page: at most the maplet's pages.
        while self.index < page {
            self.finish(emit)?;
        }
        if page != self.index {
            return Err(bad());
        }
        let local = bucket & self.shape.page_buckets().wrapping_sub(1);
        self.move_to(local / BLOCK_BUCKETS).ok_or_else(bad)?;
        if self.filled >= self.at.count {
            return Err(bad());
        }
        self.end_to(local % BLOCK_BUCKETS).ok_or_else(bad)?;
        let r = self.shape.remainder_bits();
        let rem = fp & 1u64.checked_shl(r).map_or(u64::MAX, |m| m.wrapping_sub(1));
        let entry = rem | u64::from(value).checked_shl(r).unwrap_or(0);
        let at = self.at.entries.saturating_add(
            self.filled
                .saturating_mul(u64::from(self.shape.entry_bits())),
        );
        put_bits(&mut self.page, at, self.shape.entry_bits(), entry).ok_or_else(bad)?;
        // The entry's 0 in the headers is already there: the page started zeroed.
        self.filled = self.filled.saturating_add(1);
        Ok(())
    }

    /// Ends the page being written and hands it on.
    fn finish(&mut self, emit: &mut impl FnMut(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        let short = || Error::InvalidArgument {
            what: "maplet entries other than their plan's",
        };
        self.move_to(self.per().saturating_sub(1))
            .ok_or_else(short)?;
        self.end_to(BLOCK_BUCKETS).ok_or_else(short)?;
        if self.filled != self.at.count {
            return Err(short());
        }
        emit(&self.page)?;
        self.index = self.index.saturating_add(1);
        self.open();
        Ok(())
    }

    /// Writes the pages left; the maplet is whole.
    pub fn close(mut self, emit: &mut impl FnMut(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        // Each round finishes a page: at most the maplet's pages.
        while self.index < u64::from(self.shape.pages) {
            self.finish(emit)?;
        }
        Ok(())
    }
}

/// The bit at which bucket `local`'s headers start within its block, and the block's entries
/// before it.
fn bucket_start(page: &[u8], b: &Block, local: u64) -> (u64, u64) {
    let mut pos = b.headers;
    let mut skip = local;
    // Each round reads a word of the block's headers: at most its buckets and entries in bits.
    for _ in 0..=b.count.saturating_add(BLOCK_BUCKETS) / 64 {
        if skip == 0 {
            break;
        }
        let w = word_at(page, pos);
        let ones = u64::from(w.count_ones());
        if ones >= skip {
            let at = select_in_word(w, usize::try_from(skip.wrapping_sub(1)).unwrap_or(0));
            pos = pos
                .saturating_add(u64::try_from(at).unwrap_or(0))
                .saturating_add(1);
            skip = 0;
        } else {
            skip = skip.saturating_sub(ones);
            pos = pos.saturating_add(64);
        }
    }
    // Each header bit before the bucket is an entry or an earlier bucket's end.
    (pos, pos.saturating_sub(b.headers).saturating_sub(local))
}

/// The values of `page`'s entries whose fingerprint is `hash`'s, as a mask: bit `v` for value
/// `v`. A key held under value `v` always sets bit `v`.
pub fn lookup(page: &[u8], shape: &Shape, hash: u64) -> Result<u64, Error> {
    let bad = || corrupt(Malformed::OutOfRange);
    let fp = shape.fingerprint(hash);
    let local = shape.bucket(fp) & shape.page_buckets().wrapping_sub(1);
    let b = block(page, local / BLOCK_BUCKETS)?;
    let r = shape.remainder_bits();
    let rem_mask = 1u64.checked_shl(r).map_or(u64::MAX, |m| m.wrapping_sub(1));
    let rem = fp & rem_mask;
    let (mut pos, mut index) = bucket_start(page, &b, local % BLOCK_BUCKETS);
    let width = shape.entry_bits();
    let mut mask = 0u64;
    // Each round reads an entry of the bucket: at most the block's entries.
    for _ in 0..b.count {
        if word_at(page, pos) & 1 == 1 || index >= b.count {
            break;
        }
        let at = b
            .entries
            .saturating_add(index.saturating_mul(u64::from(width)));
        let entry = get_bits(page, at, width);
        let found = entry & rem_mask;
        if found == rem {
            let value = u32::try_from(entry.checked_shr(r).unwrap_or(0)).map_err(|_| bad())?;
            mask |= 1u64.checked_shl(value).ok_or_else(bad)?;
        } else if found > rem {
            break;
        }
        pos = pos.saturating_add(1);
        index = index.saturating_add(1);
    }
    Ok(mask)
}

/// `page`'s entries in order, as (fingerprint, value), page number `index` of `shape`: what a
/// merge reads back without the keys.
pub fn entries<'a>(
    page: &'a [u8],
    shape: &'a Shape,
    index: u32,
) -> impl Iterator<Item = Result<(u64, u8), Error>> + 'a {
    let r = shape.remainder_bits();
    let width = shape.entry_bits();
    let rem_mask = 1u64.checked_shl(r).map_or(u64::MAX, |m| m.wrapping_sub(1));
    let first = u64::from(index)
        .checked_shl(shape.page_bucket_bits)
        .unwrap_or(0);
    let per = shape.blocks();
    // The block being read, its header bit, the bucket it is at and the entry.
    let (mut blk, mut cur, mut pos, mut bucket, mut i) = (0u64, None::<Block>, 0u64, 0u64, 0u64);
    std::iter::from_fn(move || {
        // Each round passes a header bit or a block: at most the page's bits.
        loop {
            let b = match cur {
                Some(b) => b,
                None => {
                    if blk >= per {
                        return None;
                    }
                    let b = match block(page, blk) {
                        Ok(b) => b,
                        Err(e) => return Some(Err(e)),
                    };
                    cur = Some(b);
                    pos = b.headers;
                    bucket = first.saturating_add(blk.saturating_mul(BLOCK_BUCKETS));
                    i = 0;
                    b
                }
            };
            if i >= b.count {
                blk = blk.saturating_add(1);
                cur = None;
                continue;
            }
            if word_at(page, pos) & 1 == 1 {
                bucket = bucket.saturating_add(1);
                pos = pos.saturating_add(1);
                continue;
            }
            let entry = get_bits(
                page,
                b.entries.saturating_add(i.saturating_mul(u64::from(width))),
                width,
            );
            let fp = bucket.checked_shl(r).unwrap_or(0) | (entry & rem_mask);
            pos = pos.saturating_add(1);
            i = i.saturating_add(1);
            return Some(
                u8::try_from(entry.checked_shr(r).unwrap_or(0))
                    .map(|v| (fp, v))
                    .map_err(|_| corrupt(Malformed::OutOfRange)),
            );
        }
    })
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    const PAYLOAD: usize = 4076;

    fn hashes(n: usize, seed: u64) -> Vec<u64> {
        let mut x = seed;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            })
            .collect()
    }

    /// Builds a maplet of (hash, value) pairs into in-memory pages.
    fn build(pairs: &[(u64, u8)], hash_bits: u32, value_bits: u32) -> (Shape, Vec<Vec<u8>>) {
        let mut entries: Vec<(u64, u8)> = Vec::new();
        let probe = Plan::new(hash_bits, value_bits, pairs.len() as u64).unwrap();
        let bare = probe.bare();
        for &(h, v) in pairs {
            entries.push((bare.fingerprint(h), v));
        }
        entries.sort_unstable();
        let mut plan = Plan::new(hash_bits, value_bits, pairs.len() as u64).unwrap();
        for &(fp, _) in &entries {
            plan.count(fp).unwrap();
        }
        let shape = plan.shape(PAYLOAD).unwrap();
        let mut pages = Vec::new();
        let mut emit = |p: &[u8]| {
            pages.push(p.to_vec());
            Ok(())
        };
        let mut w = Writer::new(&plan, shape, PAYLOAD);
        for &(fp, v) in &entries {
            w.add(fp, v, &mut emit).unwrap();
        }
        w.close(&mut emit).unwrap();
        (shape, pages)
    }

    fn query(shape: &Shape, pages: &[Vec<u8>], hash: u64) -> u64 {
        lookup(&pages[shape.page_of(hash) as usize], shape, hash).unwrap()
    }

    #[test]
    fn every_key_finds_its_branches_and_others_rarely_match() {
        for (n, branches) in [(1usize, 1u8), (100, 1), (10_000, 3), (300_000, 8)] {
            let hb = hash_bits(n as u64);
            let vb = ceil_log2(u64::from(branches));
            let hs = hashes(n, 0x2545_f491_4f6c_dd1d ^ n as u64);
            let pairs: Vec<(u64, u8)> = hs
                .iter()
                .enumerate()
                .map(|(i, &h)| (h, (i % branches as usize) as u8))
                .collect();
            let (shape, pages) = build(&pairs, hb, vb);
            assert_eq!(shape.pages as usize, pages.len());
            for &(h, v) in &pairs {
                assert_ne!(query(&shape, &pages, h) & (1 << v), 0, "n {n}: a key lost");
            }
            // Keys not in it: a query matches an entry of its bucket with chance 2^-r, and a
            // bucket holds the load n / 2^b on average, so matches stay near load · 2^-r; a
            // quarter over it at most (a fixed seed: an exact count).
            let absent = hashes(100_000, 0x9e37_79b9_7f4a_7c15 ^ n as u64);
            let hits = absent
                .iter()
                .filter(|&&h| query(&shape, &pages, h) != 0)
                .count();
            let load = n as f64 / (1u64 << shape.bucket_bits) as f64;
            let r = shape.hash_bits - shape.bucket_bits;
            let expected = 100_000.0 * load / (1u64 << r) as f64;
            println!("n {n}: {hits} false matches, expected {expected:.0}");
            assert!(
                hits as f64 <= expected * 1.25 + 4.0,
                "n {n}: {hits} false matches of 100000, expected {expected:.0}"
            );
        }
    }

    #[test]
    fn pages_read_back_their_entries_in_order() {
        let hs = hashes(50_000, 7);
        let pairs: Vec<(u64, u8)> = hs.iter().map(|&h| (h, (h >> 61) as u8)).collect();
        let hb = hash_bits(400_000);
        let (shape, pages) = build(&pairs, hb, 3);
        let mut want: Vec<(u64, u8)> = pairs
            .iter()
            .map(|&(h, v)| (shape.fingerprint(h), v))
            .collect();
        want.sort_unstable();
        let mut got = Vec::new();
        for (i, p) in pages.iter().enumerate() {
            assert!(p.len() <= PAYLOAD);
            got.extend(entries(p, &shape, i as u32).map(Result::unwrap));
        }
        assert_eq!(got, want);
    }

    #[test]
    fn merged_maplets_answer_for_both() {
        // A bundle's maplet and a new branch's, merged from their pages alone.
        let hb = hash_bits(1_000_000);
        let a: Vec<(u64, u8)> = hashes(40_000, 11)
            .into_iter()
            .map(|h| (h, (h >> 62) as u8))
            .collect();
        let b: Vec<(u64, u8)> = hashes(30_000, 12).into_iter().map(|h| (h, 4)).collect();
        let (sa, pa) = build(&a, hb, 2);
        let (sb, pb) = build(&b, hb, 0);
        let read = |s: &Shape, ps: &[Vec<u8>], value: Option<u8>| -> Vec<(u64, u8)> {
            ps.iter()
                .enumerate()
                .flat_map(|(i, p)| {
                    entries(p, s, i as u32)
                        .map(Result::unwrap)
                        .collect::<Vec<_>>()
                })
                .map(|(fp, v)| (fp, value.unwrap_or(v)))
                .collect()
        };
        let mut merged = read(&sa, &pa, None);
        merged.extend(read(&sb, &pb, Some(4)));
        merged.sort_unstable();
        let mut plan = Plan::new(hb, 3, merged.len() as u64).unwrap();
        for &(fp, _) in &merged {
            plan.count(fp).unwrap();
        }
        let shape = plan.shape(PAYLOAD).unwrap();
        let mut pages = Vec::new();
        let mut emit = |p: &[u8]| {
            pages.push(p.to_vec());
            Ok(())
        };
        let mut w = Writer::new(&plan, shape, PAYLOAD);
        for &(fp, v) in &merged {
            w.add(fp, v, &mut emit).unwrap();
        }
        w.close(&mut emit).unwrap();
        for &(h, v) in a.iter().chain(&b) {
            assert_ne!(query(&shape, &pages, h) & (1 << v), 0);
        }
    }

    #[test]
    fn out_of_order_or_extra_entries_are_refused() {
        let mut plan = Plan::new(20, 0, 2).unwrap();
        plan.count(5).unwrap();
        assert!(plan.count(4).is_err());
        let mut plan = Plan::new(20, 0, 1).unwrap();
        plan.count(5).unwrap();
        let shape = plan.shape(PAYLOAD).unwrap();
        let mut w = Writer::new(&plan, shape, PAYLOAD);
        let mut emit = |_: &[u8]| Ok(());
        w.add(5, 0, &mut emit).unwrap();
        assert!(w.add(6, 0, &mut emit).is_err(), "more entries than planned");
    }
}
