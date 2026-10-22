//! Comparisons of keys, eight bytes at a time. A key is a byte string ordered as slices order
//! (bytewise, a prefix before its extensions); the trunk, its branches and the memtables compare
//! keys on every entry they move. A byte loop compares one byte a step, and a slice comparison
//! calls `memcmp` out of line; here two keys are read as words, their first difference found
//! from the words' exclusive or, and the comparison stays inline.

/// The length of the prefix `a` and `b` share. Words are read little-endian, so the first byte
/// that differs is the exclusive or's lowest set byte.
#[inline]
pub fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let mut at = 0usize;
    while let (Some(x), Some(y)) = (
        a.get(at..).and_then(<[u8]>::first_chunk::<8>),
        b.get(at..).and_then(<[u8]>::first_chunk::<8>),
    ) {
        let diff = u64::from_le_bytes(*x) ^ u64::from_le_bytes(*y);
        if diff != 0 {
            return at.saturating_add(usize::try_from(diff.trailing_zeros() / 8).unwrap_or(0));
        }
        at = at.saturating_add(8);
    }
    // Fewer than eight bytes left in the shorter key. When it has eight in all, the word that
    // ends it overlaps bytes already found equal, so its first difference is one past them.
    let shorter = a.len().min(b.len());
    let last = shorter.checked_sub(8).and_then(|from| {
        let x = a.get(from..shorter)?.first_chunk::<8>()?;
        let y = b.get(from..shorter)?.first_chunk::<8>()?;
        let diff = u64::from_le_bytes(*x) ^ u64::from_le_bytes(*y);
        Some(if diff == 0 {
            shorter
        } else {
            from.saturating_add(usize::try_from(diff.trailing_zeros() / 8).unwrap_or(0))
        })
    });
    match last {
        Some(at) => at,
        None => {
            let (a, b) = (a.get(at..).unwrap_or(&[]), b.get(at..).unwrap_or(&[]));
            at.saturating_add(a.iter().zip(b).take_while(|(x, y)| x == y).count())
        }
    }
}

/// Whether `a` orders below `b`: past their common prefix, a smaller byte, or `a` ending there
/// while `b` goes on.
#[inline]
pub fn less(a: &[u8], b: &[u8]) -> bool {
    let at = common_prefix(a, b);
    match (a.get(at), b.get(at)) {
        (Some(x), Some(y)) => x < y,
        (None, Some(_)) => true,
        _ => false,
    }
}

/// Whether `a` and `b` are the same key.
#[inline]
pub fn equal(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && common_prefix(a, b) == a.len()
}

/// A key read where it lies in two pieces, back to back: the bytes it shares with another key,
/// read from that key, and its own bytes past them, as a branch page stages a key after its
/// first (`branch::Page`). Nothing is copied to read it.
#[derive(Clone, Copy, Debug, Default)]
pub struct Pieces<'a> {
    pub head: &'a [u8],
    pub tail: &'a [u8],
}

impl<'a> Pieces<'a> {
    /// `key` in one piece.
    #[inline]
    pub const fn whole(key: &'a [u8]) -> Self {
        Self {
            head: key,
            tail: &[],
        }
    }

    /// The key's bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.head.len().saturating_add(self.tail.len())
    }

    /// Whether the key holds no byte.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.is_empty() && self.tail.is_empty()
    }

    /// The key's byte at `at`.
    #[inline]
    pub fn get(&self, at: usize) -> Option<u8> {
        match at.checked_sub(self.head.len()) {
            None => self.head.get(at).copied(),
            Some(past) => self.tail.get(past).copied(),
        }
    }

    /// The key's first `len` bytes, at most its own.
    #[inline]
    pub fn take(&self, len: usize) -> Self {
        match len.checked_sub(self.head.len()) {
            None => Self::whole(self.head.get(..len).unwrap_or(self.head)),
            Some(past) => Self {
                head: self.head,
                tail: self.tail.get(..past).unwrap_or(self.tail),
            },
        }
    }

    /// The key past its first `len` bytes.
    #[inline]
    pub fn skip(&self, len: usize) -> Self {
        match len.checked_sub(self.head.len()) {
            None => Self {
                head: self.head.get(len..).unwrap_or(&[]),
                tail: self.tail,
            },
            Some(past) => Self::whole(self.tail.get(past..).unwrap_or(&[])),
        }
    }

    /// The length of the prefix the key and `key` share.
    #[inline]
    pub fn common_prefix(&self, key: &[u8]) -> usize {
        let head = common_prefix(self.head, key);
        if head < self.head.len() {
            return head;
        }
        head.saturating_add(common_prefix(self.tail, key.get(head..).unwrap_or(&[])))
    }

    /// The key's bytes appended to `into`.
    #[inline]
    pub fn extend_into(&self, into: &mut Vec<u8>) {
        into.extend_from_slice(self.head);
        into.extend_from_slice(self.tail);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Keys from a small alphabet, so pairs share long prefixes as sorted keys do, of every
    /// length either side of a word's.
    fn key() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(
            prop_oneof![Just(0u8), Just(1), Just(0x7f), Just(0x80), Just(0xff)],
            0..40,
        )
    }

    proptest! {
        /// Against the byte loop and the slices' own order, on keys sharing prefixes of any
        /// length and on a key against its own extensions and truncations.
        #[test]
        fn word_comparisons_agree_with_bytes(a in key(), b in key(), cut in 0usize..40) {
            let bytes = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
            prop_assert_eq!(common_prefix(&a, &b), bytes);
            prop_assert_eq!(less(&a, &b), a < b);
            prop_assert_eq!(equal(&a, &b), a == b);
            let shorter = &a[..cut.min(a.len())];
            prop_assert_eq!(common_prefix(&a, shorter), shorter.len());
            prop_assert_eq!(less(shorter, &a), shorter < a.as_slice());
            prop_assert_eq!(less(&a, shorter), a.as_slice() < shorter);
            prop_assert_eq!(equal(&a, shorter), a.as_slice() == shorter);
        }
    }

    /// The first difference in each byte of a word, and past several words.
    #[test]
    fn a_difference_is_found_at_every_place() {
        for len in 0..33usize {
            let a: Vec<u8> = (0..len).map(|i| u8::try_from(i).unwrap()).collect();
            for at in 0..len {
                let mut b = a.clone();
                b[at] ^= 0x40;
                assert_eq!(common_prefix(&a, &b), at, "len {len} at {at}");
                assert_eq!(less(&a, &b), a < b, "len {len} at {at}");
            }
            assert_eq!(common_prefix(&a, &a), len);
            assert!(equal(&a, &a));
        }
    }
}
