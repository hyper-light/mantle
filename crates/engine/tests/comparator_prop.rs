//! The bytewise compare (`util::comparator::bytewise`) is the order of `[u8]` (`memcmp`, then
//! length) on every input: equal prefixes of every length around its 8-byte words, differing
//! bytes at every position, and every length relation.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::util::comparator::{Comparator, bytewise};
use proptest::prelude::*;

proptest! {
    #[test]
    fn bytewise_is_slice_order(a in proptest::collection::vec(any::<u8>(), 0..80),
                               b in proptest::collection::vec(any::<u8>(), 0..80)) {
        prop_assert_eq!(bytewise(&a, &b), a.cmp(&b));
        prop_assert_eq!(Comparator::ReverseBytewise.compare(&a, &b), b.cmp(&a));
    }

    #[test]
    fn shared_prefixes_then_a_difference(prefix in proptest::collection::vec(any::<u8>(), 0..40),
                                         x in proptest::collection::vec(any::<u8>(), 0..20),
                                         y in proptest::collection::vec(any::<u8>(), 0..20)) {
        let a = [prefix.as_slice(), &x].concat();
        let b = [prefix.as_slice(), &y].concat();
        prop_assert_eq!(bytewise(&a, &b), a.cmp(&b));
    }
}

#[test]
fn every_position_and_length_around_the_words() {
    for len in 0..40usize {
        let base: Vec<u8> = (0..u8::try_from(len).unwrap()).collect();
        assert_eq!(bytewise(&base, &base), base.cmp(&base));
        for at in 0..len {
            for delta in [1u8, 0x80, 0xFF] {
                let mut other = base.clone();
                other[at] = other[at].wrapping_add(delta);
                assert_eq!(
                    bytewise(&base, &other),
                    base.cmp(&other),
                    "len {len} at {at}"
                );
                assert_eq!(bytewise(&other, &base), other.cmp(&base));
            }
        }
        for cut in 0..len {
            assert_eq!(bytewise(&base[..cut], &base), base[..cut].cmp(&base));
            assert_eq!(
                bytewise(&base, &base[..cut]),
                base.as_slice().cmp(&base[..cut])
            );
        }
    }
}
