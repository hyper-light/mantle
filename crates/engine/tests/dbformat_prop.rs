//! Properties of the internal-key functions over random inputs: the encoded order equals the
//! order of the parts (user key ascending under the user comparator, sequence descending, type
//! descending) for both comparators, every separator and successor stays in its range, and no
//! input makes a decoder panic or claim bytes it was not given.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::cmp::Ordering;

use mantle_engine::db::dbformat::{
    InternalKeyComparator, LookupKey, MAX_SEQUENCE_NUMBER, ParsedInternalKey, ValueType,
    append_internal_key, find_short_internal_key_successor, find_shortest_internal_key_separator,
    parse_internal_key,
};
use mantle_engine::util::comparator::Comparator;
use proptest::prelude::*;

const STORED: [ValueType; 8] = [
    ValueType::Deletion,
    ValueType::Value,
    ValueType::Merge,
    ValueType::SingleDeletion,
    ValueType::RangeDeletion,
    ValueType::BlobIndex,
    ValueType::WideColumnEntity,
    ValueType::ValuePreferredSeqno,
];

fn user_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        prop::sample::select(vec![0u8, 1, b'A', 0x7f, 0x80, 0xff]),
        0..8,
    )
}

fn parts() -> impl Strategy<Value = (Vec<u8>, u64, ValueType)> {
    (
        user_key(),
        prop_oneof![0..4u64, Just(MAX_SEQUENCE_NUMBER), 0..=MAX_SEQUENCE_NUMBER],
        prop::sample::select(STORED.to_vec()),
    )
}

fn comparator() -> impl Strategy<Value = Comparator> {
    prop::sample::select(vec![Comparator::Bytewise, Comparator::ReverseBytewise])
}

fn encode(p: &(Vec<u8>, u64, ValueType)) -> Vec<u8> {
    let mut out = Vec::new();
    append_internal_key(&mut out, &ParsedInternalKey::new(&p.0, p.1, p.2)).unwrap();
    out
}

proptest! {
    #[test]
    fn order_of_encodings_is_order_of_parts(a in parts(), b in parts(), c in comparator()) {
        let icmp = InternalKeyComparator::new(c);
        let model = c
            .compare(&a.0, &b.0)
            .then(b.1.cmp(&a.1))
            .then(b.2.cmp(&a.2));
        let (ea, eb) = (encode(&a), encode(&b));
        prop_assert_eq!(icmp.compare(&ea, &eb), model);
        let pa = ParsedInternalKey::new(&a.0, a.1, a.2);
        let pb = ParsedInternalKey::new(&b.0, b.1, b.2);
        prop_assert_eq!(icmp.compare_parsed(&pa, &pb), model);
        prop_assert_eq!(icmp.compare_with_parsed(&ea, &pb), model);
        let parsed = parse_internal_key(&ea).unwrap();
        prop_assert_eq!(parsed, pa);
    }

    #[test]
    fn separator_and_successor_stay_in_range(a in parts(), b in parts(), c in comparator()) {
        let icmp = InternalKeyComparator::new(c);
        let (ea, eb) = (encode(&a), encode(&b));
        let sep = find_shortest_internal_key_separator(c, &ea, &eb).unwrap();
        prop_assert_ne!(icmp.compare(&ea, &sep), Ordering::Greater);
        if icmp.compare(&ea, &eb) == Ordering::Less {
            prop_assert_eq!(icmp.compare(&sep, &eb), Ordering::Less);
        }
        prop_assert!(sep.len() <= ea.len());
        let succ = find_short_internal_key_successor(c, &ea).unwrap();
        prop_assert_ne!(icmp.compare(&ea, &succ), Ordering::Greater);
    }

    #[test]
    fn lookup_key_sorts_first_among_its_sequence(p in parts()) {
        let lk = LookupKey::new(&p.0, p.1).unwrap();
        let icmp = InternalKeyComparator::new(Comparator::Bytewise);
        prop_assert_eq!(lk.user_key(), &p.0[..]);
        prop_assert_ne!(icmp.compare(lk.internal_key(), &encode(&p)), Ordering::Greater);
    }

    #[test]
    fn decoders_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..24)) {
        if let Ok(p) = parse_internal_key(&bytes) {
            prop_assert_eq!(p.user_key.len() + 8, bytes.len());
            prop_assert!(p.value_type.is_extended_value_type());
        }
        let icmp = InternalKeyComparator::new(Comparator::Bytewise);
        prop_assert_eq!(icmp.compare(&bytes, &bytes), Ordering::Equal);
        let _ = find_shortest_internal_key_separator(Comparator::Bytewise, &bytes, &bytes);
        let _ = find_short_internal_key_successor(Comparator::ReverseBytewise, &bytes);
    }
}

#[test]
fn sequence_above_56_bits_is_refused() {
    let mut out = Vec::new();
    let too_big = ParsedInternalKey::new(b"k", MAX_SEQUENCE_NUMBER + 1, ValueType::Value);
    assert!(append_internal_key(&mut out, &too_big).is_err());
    assert!(out.is_empty());
    assert!(LookupKey::new(b"k", MAX_SEQUENCE_NUMBER + 1).is_err());
    // A batch-only type is not an internal-key type.
    let batch_only = ParsedInternalKey::new(b"k", 1, ValueType::LogData);
    assert!(append_internal_key(&mut out, &batch_only).is_err());
}
