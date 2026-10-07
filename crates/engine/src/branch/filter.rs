//! A branch's filter: a blocked Bloom filter (Putze, Sanders and Singler, "Cache-, hash- and
//! space-efficient Bloom filters", WEA 2007) over the branch's keys, so a point read skips a
//! branch that cannot hold its key without reading a page. Every probe of a key falls in one
//! 512-bit block, one cache line, so a check costs one cache miss.
//!
//! Step E5's maplets (docs/design/engine-structure.md §4) route a key to its pivot's branches
//! with one lookup in less space; this filter is the step before them, measured against RocksDB
//! (benches/shard_db.rs).

use crate::util::xxhash::xxh3_64bits;

/// Cited: bits a key. A Bloom filter of `b` bits a key and `k = b·ln 2` probes has a
/// false-positive rate of about `0.6185^b`, 0.8% at 10 (Broder and Mitzenmacher, "Network
/// applications of Bloom filters: a survey", Internet Mathematics 1(4), 2004, §2.1); 10 is
/// RocksDB's default `bits_per_key`.
pub const BITS_PER_KEY: usize = 10;
/// Derived: probes a key, `round(BITS_PER_KEY · ln 2)` = 7, the rate's minimum.
const PROBES: u32 = 7;
/// A block's 64-bit words: 512 bits, a cache line.
const BLOCK_WORDS: usize = 8;

/// A key's filter hash: computed once a lookup, checked against every branch on its path.
pub fn hash(key: &[u8]) -> u64 {
    xxh3_64bits(key)
}

/// The filter: its blocks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filter {
    blocks: Vec<[u64; BLOCK_WORDS]>,
}

/// The keys a filter is made for, before they are added: known exactly (a memtable packed), or
/// at most (a compaction, whose duplicates merge).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keys {
    Exactly(u64),
    AtMost(u64),
}

/// Blocks for `keys` keys at [`BITS_PER_KEY`] bits each, at least one.
fn blocks_for(keys: u64) -> usize {
    usize::try_from(keys)
        .unwrap_or(usize::MAX)
        .saturating_mul(BITS_PER_KEY)
        .div_ceil(BLOCK_WORDS.saturating_mul(64))
        .max(1)
}

/// The block for a hash: its high 32 bits scaled into the block count (Lemire, "Fast random
/// integer generation in an interval", ACM TOMACS 29(1), 2019).
fn block_of(h: u64, blocks: usize) -> usize {
    let n = u64::try_from(blocks).unwrap_or(u64::MAX);
    usize::try_from(((h >> 32).wrapping_mul(n)) >> 32).unwrap_or(0)
}

/// The probes' bit positions within a block, from the hash's low 32 bits remixed by the golden
/// ratio each probe, its top 9 bits a position (RocksDB's FastLocalBloom derives its probes so).
fn probes(h: u64) -> impl Iterator<Item = (usize, u64)> {
    let mut x = u32::try_from(h & 0xFFFF_FFFF).unwrap_or(0);
    (0..PROBES).map(move |_| {
        x = x.wrapping_mul(0x9E37_79B9);
        let bit = x >> 23;
        (usize::try_from(bit >> 6).unwrap_or(0), 1u64 << (bit & 63))
    })
}

impl Filter {
    /// An empty filter for `keys`: sized for them when known exactly; when only bounded, a
    /// power of two of blocks for the bound, which [`Self::fit`] halves once the count is known.
    pub fn new(keys: Keys) -> Self {
        let count = match keys {
            Keys::Exactly(n) => blocks_for(n),
            Keys::AtMost(n) => blocks_for(n).checked_next_power_of_two().unwrap_or(1),
        };
        Self {
            blocks: vec![[0u64; BLOCK_WORDS]; count],
        }
    }

    /// Adds a key of hash `h`.
    pub fn insert(&mut self, h: u64) {
        let count = self.blocks.len();
        if let Some(block) = self.blocks.get_mut(block_of(h, count)) {
            for (word, mask) in probes(h) {
                if let Some(w) = block.get_mut(word) {
                    *w |= mask;
                }
            }
        }
    }

    /// Halves the filter while half its blocks still give `keys` keys [`BITS_PER_KEY`] bits
    /// each: block pairs `2i, 2i + 1` OR into block `i`. A key's block at `n / 2` blocks is its
    /// block at `n` halved (`⌊⌊h·n / 2^32⌋ / 2⌋ = ⌊h·(n/2) / 2^32⌋`) and its probes within a
    /// block do not depend on `n`, so every key added is still found.
    pub fn fit(&mut self, keys: u64) {
        let need = blocks_for(keys);
        while self.blocks.len().is_multiple_of(2) && self.blocks.len() / 2 >= need {
            let half: Vec<[u64; BLOCK_WORDS]> = self
                .blocks
                .as_chunks::<2>()
                .0
                .iter()
                .map(|[lo, hi]| {
                    let mut b = *lo;
                    for (w, h) in b.iter_mut().zip(hi) {
                        *w |= h;
                    }
                    b
                })
                .collect();
            self.blocks = half;
        }
    }

    /// A filter over keys of `hashes`, at [`BITS_PER_KEY`] bits each, at least one block.
    pub fn build(hashes: &[u64]) -> Self {
        let mut f = Self::new(Keys::Exactly(
            u64::try_from(hashes.len()).unwrap_or(u64::MAX),
        ));
        for &h in hashes {
            f.insert(h);
        }
        f
    }

    /// Whether a key of hash `h` may be in the branch: false only when it is not.
    pub fn may_contain(&self, h: u64) -> bool {
        let Some(block) = self.blocks.get(block_of(h, self.blocks.len())) else {
            return true;
        };
        probes(h).all(|(word, mask)| block.get(word).is_some_and(|w| w & mask != 0))
    }

    /// The filter as bytes: each block's words, little-endian.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.blocks
            .iter()
            .flat_map(|b| b.iter().flat_map(|w| w.to_le_bytes()))
            .collect()
    }

    /// A filter from [`Self::to_bytes`]'s bytes: whole blocks, at least one.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let block = BLOCK_WORDS.checked_mul(8)?;
        if bytes.is_empty() || !bytes.len().is_multiple_of(block) {
            return None;
        }
        let blocks = bytes
            .chunks_exact(block)
            .map(|chunk| {
                let mut b = [0u64; BLOCK_WORDS];
                for (w, c) in b.iter_mut().zip(chunk.as_chunks::<8>().0) {
                    *w = u64::from_le_bytes(*c);
                }
                b
            })
            .collect();
        Some(Self { blocks })
    }

    /// The filter's bytes in memory.
    pub fn bytes(&self) -> usize {
        self.blocks
            .len()
            .saturating_mul(BLOCK_WORDS.saturating_mul(8))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_key_built_in_is_found_and_the_false_positive_rate_is_near_the_bound() {
        let keys: Vec<Vec<u8>> = (0..100_000u32)
            .map(|i| format!("key-{i}").into_bytes())
            .collect();
        let hashes: Vec<u64> = keys.iter().map(|k| hash(k)).collect();
        let f = Filter::build(&hashes);
        assert!(hashes.iter().all(|&h| f.may_contain(h)));
        let false_positives = (0..100_000u32)
            .filter(|i| f.may_contain(hash(format!("other-{i}").as_bytes())))
            .count();
        // The bound for 10 bits a key is 0.8% unblocked; blocking costs a little. Over 100,000
        // absent keys, 2% is far past any rate a working filter gives.
        assert!(false_positives < 2_000, "{false_positives}");
        assert_eq!(
            f.bytes(),
            100_000usize.saturating_mul(10).div_ceil(512) * 64
        );
    }

    #[test]
    fn a_bounded_filter_fitted_to_its_keys_finds_every_one_at_no_fewer_bits() {
        // Bounded at 400,000 keys: 7,813 blocks, rounded up to 8,192. The 30,000 keys added
        // need 586, so it halves three times, to 1,024: 512 would not hold them.
        let mut f = Filter::new(Keys::AtMost(400_000));
        assert_eq!(f.bytes(), 8_192 * 64);
        let hashes: Vec<u64> = (0..30_000u32)
            .map(|i| hash(format!("key-{i}").as_bytes()))
            .collect();
        for &h in &hashes {
            f.insert(h);
        }
        f.fit(30_000);
        assert_eq!(f.bytes(), 1_024 * 64);
        assert!(hashes.iter().all(|&h| f.may_contain(h)));
        let false_positives = (0..100_000u32)
            .filter(|i| f.may_contain(hash(format!("other-{i}").as_bytes())))
            .count();
        // 17 bits a key here, against 10 in the first test's bound of 2,000.
        assert!(false_positives < 2_000, "{false_positives}");
        // Fitted to more keys than half holds, it stays.
        let mut g = Filter::new(Keys::AtMost(1_000));
        g.fit(1_000);
        assert_eq!(g.bytes(), 32 * 64);
    }
}
