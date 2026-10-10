//! Round trips and robustness of util/coding and util/prefix_varint over arbitrary values and
//! bytes: every encoding decodes to its value and length, and no input makes a decoder panic or
//! claim more bytes than it was given.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use mantle_engine::util::coding::{
    decode_length_prefixed_slice, get_fixed16, get_fixed32, get_fixed64, get_length_prefixed_slice,
    get_varint32, get_varint32_ptr, get_varint64, get_varint64_ptr, get_varsignedint64,
    put_fixed16, put_fixed32, put_fixed64, put_length_prefixed_slice,
    put_length_prefixed_slice_parts, put_varint32, put_varint32_varint32_varint64, put_varint64,
    put_varsignedint64, varint_length,
};
use mantle_engine::util::prefix_varint::{
    get_prefix_varint32, get_prefix_varint32_ptr, get_prefix_varint64, get_prefix_varint64_ptr,
    prefix_varint64_length, put_prefix_varint32, put_prefix_varint64,
};
use proptest::prelude::*;

proptest! {
    #[test]
    fn fixed_round_trips(a: u16, b: u32, c: u64) {
        let mut s = Vec::new();
        put_fixed16(&mut s, a);
        put_fixed32(&mut s, b);
        put_fixed64(&mut s, c);
        prop_assert_eq!(s.len(), 14);
        let mut input: &[u8] = &s;
        prop_assert_eq!(get_fixed16(&mut input).unwrap(), a);
        prop_assert_eq!(get_fixed32(&mut input).unwrap(), b);
        prop_assert_eq!(get_fixed64(&mut input).unwrap(), c);
        prop_assert!(input.is_empty());
    }

    #[test]
    fn varints_round_trip(a: u32, b: u64, c: i64, d: u32, e: u32, f: u64) {
        let mut s = Vec::new();
        put_varint32(&mut s, a);
        put_varint64(&mut s, b);
        put_varsignedint64(&mut s, c);
        put_varint32_varint32_varint64(&mut s, d, e, f);
        let (_, n) = get_varint32_ptr(&s).unwrap();
        prop_assert_eq!(n, varint_length(u64::from(a)));
        let mut input: &[u8] = &s;
        prop_assert_eq!(get_varint32(&mut input).unwrap(), a);
        prop_assert_eq!(get_varint64(&mut input).unwrap(), b);
        prop_assert_eq!(get_varsignedint64(&mut input).unwrap(), c);
        prop_assert_eq!(get_varint32(&mut input).unwrap(), d);
        prop_assert_eq!(get_varint32(&mut input).unwrap(), e);
        prop_assert_eq!(get_varint64(&mut input).unwrap(), f);
        prop_assert!(input.is_empty());
    }

    #[test]
    fn prefix_varints_round_trip(a: u32, b: u64, shift in 0u32..64) {
        let b = b >> shift;
        let mut s = Vec::new();
        put_prefix_varint32(&mut s, a);
        put_prefix_varint64(&mut s, b);
        prop_assert_eq!(s.len(), varint_length(u64::from(a)) + prefix_varint64_length(b));
        let mut input: &[u8] = &s;
        prop_assert_eq!(get_prefix_varint32(&mut input).unwrap(), a);
        prop_assert_eq!(get_prefix_varint64(&mut input).unwrap(), b);
        prop_assert!(input.is_empty());
    }

    #[test]
    fn length_prefixed_slices_round_trip(
        parts in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..300), 0..6)
    ) {
        let mut s = Vec::new();
        for p in &parts {
            put_length_prefixed_slice(&mut s, p).unwrap();
        }
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        put_length_prefixed_slice_parts(&mut s, &refs).unwrap();
        let mut input: &[u8] = &s;
        for p in &parts {
            prop_assert_eq!(get_length_prefixed_slice(&mut input).unwrap(), &p[..]);
        }
        prop_assert_eq!(decode_length_prefixed_slice(input).unwrap(), &parts.concat()[..]);
        prop_assert_eq!(get_length_prefixed_slice(&mut input).unwrap(), &parts.concat()[..]);
        prop_assert!(input.is_empty());
    }

    #[test]
    fn decoders_take_no_more_than_they_are_given(bytes in proptest::collection::vec(any::<u8>(), 0..16)) {
        if let Ok((_, n)) = get_varint32_ptr(&bytes) { prop_assert!(n <= bytes.len() && n <= 5); }
        if let Ok((_, n)) = get_varint64_ptr(&bytes) { prop_assert!(n <= bytes.len() && n <= 10); }
        if let Ok((_, n)) = get_prefix_varint32_ptr(&bytes) { prop_assert!(n <= bytes.len() && n <= 5); }
        if let Ok((_, n)) = get_prefix_varint64_ptr(&bytes) { prop_assert!(n <= bytes.len() && n <= 9); }
        let mut input: &[u8] = &bytes;
        if let Ok(v) = get_length_prefixed_slice(&mut input) {
            prop_assert!(v.len() + input.len() < bytes.len() + 1);
        } else {
            prop_assert_eq!(input, &bytes[..]);
        }
    }
}
