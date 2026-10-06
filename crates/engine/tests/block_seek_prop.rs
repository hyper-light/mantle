//! Every seek in a data block lands on the first key at or after its target under
//! `InternalKeyComparator`, found here by a scan of the sorted keys. The keys are drawn from the
//! bytes 0x00, 0x01, 'a' and 0xFF, so user keys are prefixes of one another, repeat with several
//! sequence numbers, run short, and hold bytes a trailer holds (a key that shares bytes with a
//! shorter one shares part of its trailer); the targets are the keys themselves and other user
//! keys at other sequence numbers. This holds the prefix-tracked compare of the linear scan
//! (`Core::compare_tracked`) to the full compare it replaces; with the bytes 'a' to 'c' alone,
//! a skip that ignored where the previous key ended passed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::cmp::Ordering;

use mantle_engine::db::dbformat::{DISABLE_GLOBAL_SEQUENCE_NUMBER, InternalKeyComparator};
use mantle_engine::table::block_based::block::Block;
use mantle_engine::table::block_based::block_builder::{BlockBuilder, BlockBuilderOptions};
use mantle_engine::table::block_based::data_block_footer::DataBlockIndexType;
use mantle_engine::util::comparator::Comparator;
use proptest::prelude::*;

fn internal(user: &[u8], seq: u64) -> Vec<u8> {
    let mut k = user.to_vec();
    k.extend_from_slice(&((seq << 8) | 1).to_le_bytes());
    k
}

fn user_key() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(
        prop_oneof![Just(0x00u8), Just(0x01), Just(b'a'), Just(0xFF)],
        0..7,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]
    #[test]
    fn seeks_land_where_a_scan_does(
        users in proptest::collection::vec((user_key(), 0u64..600), 1..60),
        targets in proptest::collection::vec((user_key(), 0u64..600), 1..40),
        interval in prop_oneof![Just(1u32), Just(2), Just(3), Just(16)],
        hash in any::<bool>(),
    ) {
        let icmp = InternalKeyComparator::new(Comparator::Bytewise);
        let mut keys: Vec<Vec<u8>> = users.iter().map(|(u, s)| internal(u, *s)).collect();
        keys.sort_by(|a, b| icmp.compare(a, b));
        keys.dedup();
        let mut b = BlockBuilder::new(BlockBuilderOptions {
            restart_interval: interval,
            index_type: if hash { DataBlockIndexType::BinaryAndHash } else { DataBlockIndexType::BinarySearch },
            ..BlockBuilderOptions::default()
        }).unwrap();
        for k in &keys {
            b.add(k, b"v", None, false).unwrap();
        }
        let block = Block::new(b.finish().unwrap().to_vec(), interval);
        let mut it = block.new_data_iterator(Comparator::Bytewise, DISABLE_GLOBAL_SEQUENCE_NUMBER);
        let mut all: Vec<Vec<u8>> = targets.iter().map(|(u, s)| internal(u, *s)).collect();
        all.extend(keys.iter().cloned());
        for t in &all {
            it.seek(t);
            it.status().unwrap();
            let want = keys.iter().find(|k| icmp.compare(k, t) != Ordering::Less);
            match want {
                Some(k) => {
                    prop_assert!(it.valid(), "seek {:?} found nothing, want {:?}", t, k);
                    prop_assert_eq!(it.key(), k.as_slice());
                }
                None => prop_assert!(!it.valid()),
            }
        }
    }
}
