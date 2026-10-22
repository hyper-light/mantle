//! A bit vector with rank as the Fast Succinct Trie uses it (research/35 §1; Zhang et al. SIGMOD
//! 2018 §2.6): one level of lookup table, a running count of set bits at the start of every block
//! of `B` bits, then a popcount within the block. The paper's select, on S-LOUDS alone, is a
//! stored node start in the trie instead (research/62 §1).

use crate::error::{Error, Malformed};

fn corrupt() -> Error {
    Error::Corruption {
        what: "a succinct trie's bit vector",
        why: Malformed::OutOfRange,
    }
}

/// A bit vector, its bits in 64-bit words, least significant first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bits {
    words: Vec<u64>,
    len: usize,
    /// Words a rank block holds (`B / 64`), and the set bits before each block.
    block_words: usize,
    ranks: Vec<u32>,
    ones: usize,
}

/// A bit vector as a builder fills it: appended to, and set in place.
#[derive(Clone, Debug, Default)]
pub struct Grow {
    words: Vec<u64>,
    len: usize,
}

impl Grow {
    /// Appends `b`. Every bit past the length is clear (a word is added clear, and only `set`
    /// writes, within the length), so a clear bit is appended by the length alone.
    pub fn push(&mut self, b: bool) {
        let i = self.len;
        if i.is_multiple_of(64) {
            self.words.push(0);
        }
        self.len = i.saturating_add(1);
        if b && let Some(w) = self.words.last_mut() {
            *w |= 1u64 << (i % 64);
        }
    }

    /// Sets bit `i`, within the length, to `b`.
    pub fn set(&mut self, i: usize, b: bool) {
        if i >= self.len {
            return;
        }
        if let Some(w) = self.words.get_mut(i / 64) {
            if b {
                *w |= 1u64 << (i % 64);
            } else {
                *w &= !(1u64 << (i % 64));
            }
        }
    }

    /// Bit `i`.
    pub fn get(&self, i: usize) -> bool {
        i < self.len
            && self
                .words
                .get(i / 64)
                .is_some_and(|w| w >> (i % 64) & 1 == 1)
    }

    /// The last bit, if any.
    pub fn last(&self) -> Option<bool> {
        self.len.checked_sub(1).map(|i| self.get(i))
    }

    /// Sets the last bit, if any, to `b`.
    pub fn set_last(&mut self, b: bool) {
        if let Some(i) = self.len.checked_sub(1) {
            self.set(i, b);
        }
    }

    /// Empties it, keeping its words' room.
    pub fn clear(&mut self) {
        self.words.clear();
        self.len = 0;
    }

    /// Grows to `len` bits, the new ones clear.
    pub fn grow_to(&mut self, len: usize) {
        if len > self.len {
            self.words.resize(len.div_ceil(64), 0);
            self.len = len;
        }
    }

    /// The bits.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no bit is held.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Bits {
    /// The vector of `bits`, with rank blocks of `block_bits` (a multiple of 64): 64 for LOUDS-
    /// Dense, 512 for LOUDS-Sparse.
    pub fn new(bits: &[bool], block_bits: usize) -> Result<Self, Error> {
        let mut g = Grow::default();
        for &b in bits {
            g.push(b);
        }
        Self::from_grow(g, block_bits)
    }

    /// A copy of the vector a builder fills, its words copied whole: the builder keeps its
    /// buffers for its next trie.
    pub fn copy_of(bits: &Grow, block_bits: usize) -> Result<Self, Error> {
        Self::from_grow(bits.clone(), block_bits)
    }

    /// The vector a builder filled, its words taken as they are.
    pub fn from_grow(bits: Grow, block_bits: usize) -> Result<Self, Error> {
        let block_words =
            block_bits
                .checked_div(64)
                .filter(|&w| w > 0)
                .ok_or(Error::InvalidArgument {
                    what: "a rank block that is not a whole number of words",
                })?;
        let mut v = Self {
            words: bits.words,
            len: bits.len,
            block_words,
            ranks: Vec::new(),
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
            ones = ones.saturating_add(w.count_ones() as usize);
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

    /// The first set bit at or past `i`, by words: a node's end in LOUDS-Sparse is usually in
    /// the word its start is.
    pub fn next_one(&self, i: usize) -> Option<usize> {
        if i >= self.len {
            return None;
        }
        let mut word = i / 64;
        let mut bits = self.words.get(word)? & (u64::MAX << (i % 64));
        loop {
            if bits != 0 {
                let p = word
                    .checked_mul(64)?
                    .checked_add(bits.trailing_zeros() as usize)?;
                return if p < self.len { Some(p) } else { None };
            }
            word = word.checked_add(1)?;
            bits = *self.words.get(word)?;
        }
    }

    /// The last set bit at or before `i`, by words.
    pub fn prev_one(&self, i: usize) -> Option<usize> {
        let i = i.min(self.len.checked_sub(1)?);
        let mut word = i / 64;
        let keep = 63usize.saturating_sub(i % 64);
        let mut bits = self.words.get(word)? & (u64::MAX >> keep);
        loop {
            if bits != 0 {
                return word
                    .checked_mul(64)?
                    .checked_add(63usize.saturating_sub(bits.leading_zeros() as usize));
            }
            word = word.checked_sub(1)?;
            bits = *self.words.get(word)?;
        }
    }

    /// Appends the vector to `out`: its length in bits, its rank block in words, its words. The
    /// rank table is rebuilt when it is read.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&u64::try_from(self.len).unwrap_or(u64::MAX).to_le_bytes());
        out.extend_from_slice(
            &u64::try_from(self.block_words)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for w in &self.words {
            out.extend_from_slice(&w.to_le_bytes());
        }
    }

    /// A vector [`Self::encode`] wrote at the start of `bytes`, and the bytes it took.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
        let word = |at: usize| -> Result<u64, Error> {
            bytes
                .get(at..)
                .and_then(<[u8]>::first_chunk::<8>)
                .map(|b| u64::from_le_bytes(*b))
                .ok_or(Error::Corruption {
                    what: "a succinct trie's bit vector",
                    why: Malformed::Truncated,
                })
        };
        let len = usize::try_from(word(0)?).map_err(|_| corrupt())?;
        let block_words = usize::try_from(word(8)?).map_err(|_| corrupt())?;
        if block_words == 0 {
            return Err(corrupt());
        }
        let n = len.div_ceil(64);
        let mut words = Vec::with_capacity(n.min(bytes.len() / 8));
        let mut at = 16usize;
        for _ in 0..n {
            words.push(word(at)?);
            at = at.checked_add(8).ok_or(corrupt())?;
        }
        // Bits past the length are zero, as `new` leaves them.
        if let (Some(last), r) = (words.last(), len % 64)
            && r != 0
            && last >> r != 0
        {
            return Err(corrupt());
        }
        let mut v = Self {
            words,
            len,
            block_words,
            ranks: Vec::new(),
            ones: 0,
        };
        v.index()?;
        Ok((v, at))
    }

    /// Bytes held, for the trie's memory accounting.
    pub fn bytes(&self) -> usize {
        self.words
            .len()
            .saturating_mul(8)
            .saturating_add(self.ranks.len().saturating_mul(4))
    }

    /// The bytes its buffers hold in memory: its words' and its rank samples' capacities.
    pub fn held_bytes(&self) -> usize {
        self.words
            .capacity()
            .saturating_mul(8)
            .saturating_add(self.ranks.capacity().saturating_mul(4))
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

    #[test]
    fn rank_and_scans_agree_with_counting_on_every_position() {
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
                    for i in 0..len {
                        let back = (0..=i).rev().find(|&j| bits[j]);
                        assert_eq!(v.prev_one(i), back, "len {len} prev from {i}");
                        let want = (i..len).find(|&j| bits[j]);
                        assert_eq!(v.next_one(i), want, "len {len} next from {i}");
                    }
                }
            }
        }
    }
}
