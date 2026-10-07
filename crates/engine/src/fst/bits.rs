//! A bit vector with rank and select as the Fast Succinct Trie uses them (research/35 §1; Zhang et
//! al. SIGMOD 2018 §2.6): rank from one level of lookup table, a running count of set bits at
//! the start of every block of `B` bits, then a popcount within the block; select from a table
//! sampling the position of every `S`th set bit, then a forward scan by popcount.

use crate::error::{Error, Malformed};

fn corrupt() -> Error {
    Error::Corruption {
        what: "a succinct trie's bit vector",
        why: Malformed::OutOfRange,
    }
}

/// Bits a select sample covers: one sample every 64 set bits, 9–17% of a dense vector, 1–2% of
/// the trie (research/35 §1).
pub const SELECT_SAMPLE: usize = 64;

/// A bit vector, its bits in 64-bit words, least significant first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bits {
    words: Vec<u64>,
    len: usize,
    /// Words a rank block holds (`B / 64`), and the set bits before each block.
    block_words: usize,
    ranks: Vec<u32>,
    /// The position of every `SELECT_SAMPLE`th set bit, the first included.
    selects: Vec<u32>,
    ones: usize,
}

impl Bits {
    /// The vector of `bits`, with rank blocks of `block_bits` (a multiple of 64): 64 for LOUDS-
    /// Dense, 512 for LOUDS-Sparse.
    pub fn new(bits: &[bool], block_bits: usize) -> Result<Self, Error> {
        let block_words =
            block_bits
                .checked_div(64)
                .filter(|&w| w > 0)
                .ok_or(Error::InvalidArgument {
                    what: "a rank block that is not a whole number of words",
                })?;
        let mut words = vec![0u64; bits.len().div_ceil(64)];
        for (i, _) in bits.iter().enumerate().filter(|(_, b)| **b) {
            if let Some(w) = words.get_mut(i / 64) {
                *w |= 1u64 << (i % 64);
            }
        }
        let mut v = Self {
            words,
            len: bits.len(),
            block_words,
            ranks: Vec::new(),
            selects: Vec::new(),
            ones: 0,
        };
        v.index()?;
        Ok(v)
    }

    fn index(&mut self) -> Result<(), Error> {
        let mut ones = 0usize;
        for (i, w) in self.words.iter().enumerate() {
            if i.checked_rem(self.block_words) == Some(0) {
                self.ranks.push(u32::try_from(ones).map_err(|_| corrupt())?);
            }
            let mut bits = *w;
            while bits != 0 {
                if ones.is_multiple_of(SELECT_SAMPLE) {
                    let pos = i
                        .checked_mul(64)
                        .and_then(|p| p.checked_add(bits.trailing_zeros() as usize))
                        .ok_or(corrupt())?;
                    self.selects
                        .push(u32::try_from(pos).map_err(|_| corrupt())?);
                }
                ones = ones.saturating_add(1);
                bits &= bits.wrapping_sub(1);
            }
        }
        self.ones = ones;
        Ok(())
    }

    /// The bits.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the vector holds no bits.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The set bits.
    pub fn ones(&self) -> usize {
        self.ones
    }

    /// Bit `i`.
    pub fn get(&self, i: usize) -> bool {
        i < self.len
            && self
                .words
                .get(i / 64)
                .is_some_and(|w| w >> (i % 64) & 1 == 1)
    }

    /// The set bits in `[0, i]`, as the paper counts rank (`rank1(i)` includes position i).
    pub fn rank1(&self, i: usize) -> usize {
        if self.len == 0 {
            return 0;
        }
        let i = i.min(self.len.saturating_sub(1));
        let word = i / 64;
        let block = word.checked_div(self.block_words).unwrap_or(0);
        let mut r = self.ranks.get(block).map_or(0, |&r| r as usize);
        for w in block.saturating_mul(self.block_words)..word {
            r = r.saturating_add(self.words.get(w).map_or(0, |w| w.count_ones() as usize));
        }
        let mask = if i % 64 == 63 {
            u64::MAX
        } else {
            (1u64 << ((i % 64).saturating_add(1))).wrapping_sub(1)
        };
        r.saturating_add(
            self.words
                .get(word)
                .map_or(0, |w| (w & mask).count_ones() as usize),
        )
    }

    /// The position of the `k`th set bit, counting from 1 as the paper does; none past the last.
    pub fn select1(&self, k: usize) -> Option<usize> {
        if k == 0 || k > self.ones {
            return None;
        }
        let before = k.checked_sub(1)?;
        let sample = before / SELECT_SAMPLE;
        let start = *self.selects.get(sample)? as usize;
        // Set bits before `start`, and those still to pass from it.
        let mut left = before % SELECT_SAMPLE;
        let mut word = start / 64;
        let mut bits = self.words.get(word)? & (u64::MAX << (start % 64));
        // At most the vector's words.
        for _ in 0..=self.words.len() {
            let n = bits.count_ones() as usize;
            if left < n {
                for _ in 0..left {
                    bits &= bits.wrapping_sub(1);
                }
                return word
                    .checked_mul(64)
                    .and_then(|p| p.checked_add(bits.trailing_zeros() as usize));
            }
            left = left.saturating_sub(n);
            word = word.checked_add(1)?;
            bits = *self.words.get(word)?;
        }
        None
    }

    /// Bytes held, for the trie's memory accounting.
    pub fn bytes(&self) -> usize {
        self.words
            .len()
            .saturating_mul(8)
            .saturating_add(self.ranks.len().saturating_mul(4))
            .saturating_add(self.selects.len().saturating_mul(4))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naive_rank(bits: &[bool], i: usize) -> usize {
        bits[..=i.min(bits.len() - 1)]
            .iter()
            .filter(|&&b| b)
            .count()
    }

    fn naive_select(bits: &[bool], k: usize) -> Option<usize> {
        bits.iter()
            .enumerate()
            .filter(|(_, b)| **b)
            .nth(k.checked_sub(1)?)
            .map(|(i, _)| i)
    }

    #[test]
    fn rank_and_select_agree_with_counting_on_every_position() {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for len in [1usize, 63, 64, 65, 511, 512, 513, 2_000, 9_999] {
            for density in [1u64, 3, 17, 50, 97] {
                let bits: Vec<bool> = (0..len)
                    .map(|_| {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        x % 100 < density
                    })
                    .collect();
                for block in [64, 512] {
                    let v = Bits::new(&bits, block).unwrap();
                    assert_eq!(v.ones(), bits.iter().filter(|&&b| b).count());
                    for i in 0..len {
                        assert_eq!(v.get(i), bits[i]);
                        assert_eq!(
                            v.rank1(i),
                            naive_rank(&bits, i),
                            "len {len} block {block} i {i}"
                        );
                    }
                    for k in 0..=v.ones() + 1 {
                        assert_eq!(v.select1(k), naive_select(&bits, k), "len {len} k {k}");
                    }
                }
            }
        }
    }
}
