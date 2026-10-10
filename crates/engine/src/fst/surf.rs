//! A range filter of a sorted run's keys (SuRF, Zhang et al. SIGMOD 2018 §3; research/35 §2): a
//! succinct trie of each key cut to the prefix that tells it from its neighbours, with the key's
//! next `n` bits after the cut as its value (SuRF-Real). It answers whether a range may hold a key
//! with no false negative.
//!
//! An entry, prefix `p` and suffix `s`, stands for the keys that start with `p` and whose next `n`
//! bits, zero past the key's end, are `s`: in byte order, one interval, from `p` followed by `s`'s
//! bits (trailing zero bytes dropped, which sort first) to before `p` followed by `s + 1`. Every key
//! is in its entry's interval, so a range meeting no interval holds no key. The entries a range
//! `[from, end)` may meet are the stored prefixes of `from`, which the trie alone cannot order
//! against it, and the least stored prefix at or past `from`, whose interval starts lowest of all
//! the rest; each is checked against the range by its bits.

use std::cmp::Ordering;
use std::ops::ControlFlow;

use super::packed::MAX_WIDTH;
use super::trie::{Trie, TrieBuilder, Width};
use crate::error::{Error, Malformed};

/// Bytes a suffix of at most 32 bits spans.
const SUFFIX_BYTES: usize = 4;

/// The `n` bits of `key` after its first `at` bytes, zero past its end, most significant first.
fn suffix_of(key: &[u8], at: usize, n: u32) -> u32 {
    let bytes = n.div_ceil(8) as usize;
    let mut v = 0u64;
    for i in 0..bytes {
        let b = at
            .checked_add(i)
            .and_then(|j| key.get(j))
            .copied()
            .unwrap_or(0);
        v = (v << 8) | u64::from(b);
    }
    let spare = u32::try_from(bytes.saturating_mul(8))
        .unwrap_or(0)
        .saturating_sub(n);
    u32::try_from(v >> spare).unwrap_or(u32::MAX)
}

/// `s`'s `n` bits as bytes, left aligned, trailing zero bytes dropped: what follows the prefix in
/// the least key of an entry's interval.
fn suffix_bytes(s: u32, n: u32) -> ([u8; SUFFIX_BYTES], usize) {
    let bytes = n.div_ceil(8) as usize;
    let spare = u32::try_from(bytes.saturating_mul(8))
        .unwrap_or(0)
        .saturating_sub(n);
    // `s` left aligned in its bytes, those at the top of a word; no bytes, no bits.
    let top = u32::try_from(64usize.saturating_sub(bytes.saturating_mul(8))).unwrap_or(64);
    let aligned = u64::from(s)
        .checked_shl(spare)
        .and_then(|v| v.checked_shl(top))
        .unwrap_or(0);
    let all = aligned.to_be_bytes();
    let mut out = [0u8; SUFFIX_BYTES];
    out.copy_from_slice(all.get(..SUFFIX_BYTES).unwrap_or(&[0; SUFFIX_BYTES]));
    let mut len = bytes;
    while len > 0 && out.get(len.saturating_sub(1)) == Some(&0) {
        len = len.saturating_sub(1);
    }
    (out, len)
}

/// Whether the least key of the entry `prefix`, `s` is below `end` (no end: always).
fn starts_below(prefix: &[u8], s: u32, n: u32, end: Option<&[u8]>) -> bool {
    let Some(end) = end else { return true };
    let (bytes, len) = suffix_bytes(s, n);
    let lo = prefix.iter().chain(bytes.get(..len).unwrap_or(&[]));
    lo.cmp(end.iter()) == Ordering::Less
}

/// A run's range filter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Surf {
    trie: Trie,
    /// Suffix bits an entry keeps.
    suffix: u32,
}

impl Surf {
    /// Whether `[from, end)` (no end: unbounded) may hold a key: false only when it holds none.
    /// `scratch` holds the least entry's prefix, so a check allocates nothing once it has grown.
    pub fn may_hold(&self, from: &[u8], end: Option<&[u8]>, scratch: &mut Vec<u8>) -> bool {
        if end.is_some_and(|e| e <= from) {
            return false;
        }
        let n = self.suffix;
        let found = self.trie.seek(from, scratch, |len, s| {
            // A stored prefix of `from`: its keys at or past `from` are those whose bits after
            // the prefix are at least `from`'s.
            let p = from.get(..len).unwrap_or(&[]);
            if s >= suffix_of(from, len, n) && starts_below(p, s, n, end) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        match found {
            ControlFlow::Break(()) => true,
            ControlFlow::Continue(None) => false,
            ControlFlow::Continue(Some(s)) => starts_below(scratch, s, n, end),
        }
    }

    /// The suffix bits an entry keeps.
    pub fn suffix_bits(&self) -> u32 {
        self.suffix
    }

    /// The keys filtered.
    pub fn len(&self) -> usize {
        self.trie.len()
    }

    /// Whether it filters no key.
    pub fn is_empty(&self) -> bool {
        self.trie.is_empty()
    }

    /// Bytes held.
    pub fn bytes(&self) -> usize {
        self.trie.bytes()
    }

    /// Appends the filter to `out`: its suffix bits, then its trie.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(u8::try_from(self.suffix).unwrap_or(u8::MAX));
        self.trie.encode(out);
    }

    /// A filter [`Self::encode`] wrote at the start of `bytes`, and the bytes it took.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
        let corrupt = |why| Error::Corruption {
            what: "a range filter",
            why,
        };
        let suffix = u32::from(*bytes.first().ok_or(corrupt(Malformed::Truncated))?);
        if suffix > MAX_WIDTH {
            return Err(corrupt(Malformed::OutOfRange));
        }
        let (trie, used) = Trie::decode(bytes.get(1..).unwrap_or(&[]))?;
        Ok((
            Self { trie, suffix },
            used.checked_add(1).ok_or(corrupt(Malformed::TooLarge))?,
        ))
    }
}

/// Builds a [`Surf`] from keys in strictly ascending order, one held at a time: a key's cut
/// needs the next key, so each is added to the trie when the next arrives. Reset, it keeps its
/// buffers, so one used again for a run no larger allocates nothing as keys are added.
#[derive(Clone, Debug, Default)]
pub struct SurfBuilder {
    trie: TrieBuilder,
    suffix: u32,
    /// The key not yet added, and the bytes it shares with the key before it.
    last: Vec<u8>,
    shared_before: usize,
    pending: bool,
}

impl SurfBuilder {
    /// A builder keeping `suffix` bits a key.
    pub fn new(suffix: u32) -> Result<Self, Error> {
        let mut b = Self::default();
        b.reset(suffix)?;
        Ok(b)
    }

    /// Empties the builder for another run, keeping `suffix` bits a key, its buffers kept.
    pub fn reset(&mut self, suffix: u32) -> Result<(), Error> {
        self.trie.reset(Width::Bits(suffix))?;
        self.suffix = suffix;
        self.last.clear();
        self.shared_before = 0;
        self.pending = false;
        Ok(())
    }

    /// Adds `key`, greater than every key added.
    pub fn add(&mut self, key: &[u8]) -> Result<(), Error> {
        let mut shared = 0usize;
        if self.pending {
            if key <= self.last.as_slice() {
                return Err(Error::InvalidArgument {
                    what: "a range filter's keys not strictly ascending",
                });
            }
            shared = self
                .last
                .iter()
                .zip(key)
                .take_while(|(a, b)| a == b)
                .count();
            self.cut_last(shared)?;
        }
        self.last.clear();
        self.last.extend_from_slice(key);
        self.shared_before = shared;
        self.pending = true;
        Ok(())
    }

    /// Adds the held key to the trie, cut one byte past what it shares with either neighbour
    /// (`shared_after` with the next).
    fn cut_last(&mut self, shared_after: usize) -> Result<(), Error> {
        let len = self
            .shared_before
            .max(shared_after)
            .saturating_add(1)
            .min(self.last.len());
        let s = suffix_of(&self.last, len, self.suffix);
        self.trie.add(self.last.get(..len).unwrap_or(&[]), s)
    }

    /// The filter of the keys added.
    pub fn finish(&mut self) -> Result<Surf, Error> {
        if self.pending {
            self.cut_last(0)?;
            self.pending = false;
        }
        Ok(Surf {
            trie: self.trie.finish()?,
            suffix: self.suffix,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn rng(x: &mut u64) -> u64 {
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        *x
    }

    /// Keys from a small alphabet with 0x00 and 0xFF in it, lengths 0 to 6.
    fn key(x: &mut u64) -> Vec<u8> {
        let len = (rng(x) % 7) as usize;
        (0..len)
            .map(|_| [0x00, 0x01, b'a', b'b', 0x7f, 0xfe, 0xff][(rng(x) % 7) as usize])
            .collect()
    }

    fn surf(set: &BTreeSet<Vec<u8>>, suffix: u32) -> Surf {
        let mut b = SurfBuilder::new(suffix).unwrap();
        for k in set {
            b.add(k).unwrap();
        }
        b.finish().unwrap()
    }

    #[test]
    fn suffix_bits_and_bytes_agree() {
        assert_eq!(suffix_of(b"ab", 0, 8), u32::from(b'a'));
        assert_eq!(suffix_of(b"ab", 1, 4), u32::from(b'b') >> 4);
        assert_eq!(suffix_of(b"ab", 1, 12), u32::from(b'b') << 4);
        assert_eq!(suffix_of(b"ab", 2, 32), 0);
        assert_eq!(suffix_bytes(u32::from(b'b') << 4, 12), ([b'b', 0, 0, 0], 1));
        assert_eq!(suffix_bytes(0x0_1, 12), ([0, 0x10, 0, 0], 2));
        assert_eq!(suffix_bytes(0, 8), ([0; 4], 0));
    }

    #[test]
    fn a_range_holding_a_key_is_never_ruled_out() {
        let mut x = 0x2545_f491_4f6c_dd1du64;
        for case in 0..300 {
            let n = (rng(&mut x) % 200) as usize;
            let set: BTreeSet<Vec<u8>> = (0..n).map(|_| key(&mut x)).collect();
            let mut scratch = Vec::new();
            for suffix in [0u32, 1, 4, 8, 12, 32] {
                let f = surf(&set, suffix);
                assert_eq!(f.len(), set.len());
                let mut out = Vec::new();
                f.encode(&mut out);
                assert_eq!(Surf::decode(&out).unwrap(), (f.clone(), out.len()));
                for k in &set {
                    let mut after = k.clone();
                    after.push(0);
                    assert!(
                        f.may_hold(k, Some(&after), &mut scratch),
                        "case {case} {k:?}"
                    );
                }
                for _ in 0..300 {
                    let (a, b) = (key(&mut x), key(&mut x));
                    let (from, end) = if a <= b { (a, b) } else { (b, a) };
                    let bounded = !rng(&mut x).is_multiple_of(4);
                    let holds = if bounded {
                        set.range(from.clone()..end.clone()).next().is_some()
                    } else {
                        set.range(from.clone()..).next().is_some()
                    };
                    let end = if bounded { Some(end.as_slice()) } else { None };
                    let may = f.may_hold(&from, end, &mut scratch);
                    assert!(
                        may || !holds,
                        "case {case} suffix {suffix} [{from:?}, {end:?}) holds a key"
                    );
                }
            }
        }
    }

    #[test]
    fn suffix_bits_rule_out_ranges_the_prefixes_cannot() {
        // Keys sharing long prefixes, queried between them: the more suffix bits, the fewer
        // ranges let through that hold nothing, and with the whole key kept, none.
        let keys: BTreeSet<Vec<u8>> = (0u32..2_000)
            .map(|n| format!("user/{:08}/object", n * 7).into_bytes())
            .collect();
        let mut scratch = Vec::new();
        let mut through = Vec::new();
        for suffix in [0u32, 8, 16, 32] {
            let f = surf(&keys, suffix);
            let mut n = 0;
            for m in 0u32..14_000 {
                let from = format!("user/{m:08}/object").into_bytes();
                let mut end = from.clone();
                end.push(0);
                let holds = keys.contains(&from);
                let may = f.may_hold(&from, Some(&end), &mut scratch);
                assert!(may || !holds, "{m}");
                if may && !holds {
                    n += 1;
                }
            }
            through.push(n);
        }
        assert!(
            through.windows(2).all(|w| w[1] <= w[0]),
            "false positives by suffix bits {through:?}"
        );
        assert!(through[0] > through[3], "{through:?}");
    }
}
