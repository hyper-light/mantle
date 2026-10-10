//! The fill oracle as a bitset: a key's presence is one bit, and the keys after one are found by
//! scanning words, so the oracle takes `num / 8` bytes at any scale, where a byte a key and eight
//! more a present key grew with the run (a benchmark is bounded as the engine is).

pub struct Present {
    bits: Vec<u64>,
    count: usize,
}

impl Present {
    /// No key of `[0, num)` present yet; refused when its words cannot be reserved.
    pub fn new(num: u64) -> Result<Self, std::collections::TryReserveError> {
        let words = usize::try_from(num.div_ceil(64)).unwrap_or(usize::MAX);
        let mut bits = Vec::new();
        bits.try_reserve_exact(words)?;
        bits.resize(words, 0);
        Ok(Self { bits, count: 0 })
    }

    pub fn insert(&mut self, key: u64) {
        if let Some(word) = self.bits.get_mut((key / 64) as usize) {
            let bit = 1u64 << (key % 64);
            if *word & bit == 0 {
                *word |= bit;
                self.count += 1;
            }
        }
    }

    pub fn holds(&self, key: u64) -> bool {
        self.bits
            .get((key / 64) as usize)
            .is_some_and(|word| (word >> (key % 64)) & 1 == 1)
    }

    /// The keys present.
    pub fn len(&self) -> usize {
        self.count
    }

    /// The first `limit` present keys at or after `from`, ascending, into `out`, which is
    /// cleared first.
    pub fn after(&self, from: u64, limit: usize, out: &mut Vec<u64>) {
        out.clear();
        let mut word = (from / 64) as usize;
        let Some(&first) = self.bits.get(word) else {
            return;
        };
        let mut bits = first & (u64::MAX << (from % 64));
        while out.len() < limit {
            while bits == 0 {
                word += 1;
                match self.bits.get(word) {
                    Some(&w) => bits = w,
                    None => return,
                }
            }
            out.push(word as u64 * 64 + u64::from(bits.trailing_zeros()));
            bits &= bits - 1;
        }
    }

    /// Every present key, ascending.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.bits.iter().enumerate().flat_map(|(at, &word)| {
            let mut bits = word;
            std::iter::from_fn(move || {
                if bits == 0 {
                    return None;
                }
                let bit = bits.trailing_zeros();
                bits &= bits - 1;
                Some(at as u64 * 64 + u64::from(bit))
            })
        })
    }
}
