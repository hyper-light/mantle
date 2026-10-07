//! The key region in a process of its own (`docs/seal.md` §8): refused before it is made, bounded by
//! its stated count, a dropped key's slot taken again, and the count of keys held what a consumer
//! checks after dropping every key on a suspend notification.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::cognitive_complexity
)]

use hyper_seal::SealError;
use hyper_seal::keys::WrappingKey;

#[test]
fn the_region_bounds_the_keys_a_process_holds() {
    assert_eq!(WrappingKey::generate(0).err(), Some(SealError::Capacity));
    assert_eq!(hyper_seal::keys_held(), None);
    hyper_seal::lock_keys(3).unwrap();
    assert_eq!(hyper_seal::lock_keys(3), Ok(()));
    let (slots, held) = hyper_seal::keys_held().unwrap();
    assert_eq!(held, 0);
    // The region is a whole number of pages: at least the three asked for.
    assert!(slots >= 3);
    let mut keys = Vec::new();
    for _ in 0..slots {
        keys.push(WrappingKey::generate(0).unwrap());
    }
    assert_eq!(hyper_seal::keys_held(), Some((slots, slots)));
    assert_eq!(WrappingKey::generate(0).err(), Some(SealError::Capacity));
    keys.pop();
    assert_eq!(hyper_seal::keys_held(), Some((slots, slots - 1)));
    keys.push(WrappingKey::generate(0).unwrap());
    keys.clear();
    assert_eq!(hyper_seal::keys_held(), Some((slots, 0)));
}
