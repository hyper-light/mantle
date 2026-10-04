//! RocksDB's memtable/skiplist_test.cc, test for test: its 8 test definitions, with the same
//! literals.
//!
//! RocksDB's `SkipList<Key, Comparator>` (memtable/skiplist.h) is a second, pointer-linked
//! skiplist for one writer and many readers, used by `WriteBatchWithIndex` (P17) and not by the
//! memtable. The port has one skiplist (memtable/inlineskiplist.rs), single-writer by
//! construction, so these tests run against it: the claims are the same — ordered lookups,
//! seeks both ways, and readers on other threads seeing every key present when they began
//! while the writer inserts.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[path = "support/skiplist_harness.rs"]
mod harness;
#[allow(dead_code)]
#[path = "support/random.rs"]
mod random;

use std::collections::BTreeSet;

use harness::{ConcurrentTest, Key, decode, encode, new_list, run_concurrent};
use random::{Random, random_seed};

#[test]
fn empty() {
    let list = new_list();
    assert!(!list.contains(&encode(10)));

    let mut iter = list.iter();
    assert!(!iter.valid());
    iter.seek_to_first();
    assert!(!iter.valid());
    iter.seek(&encode(100));
    assert!(!iter.valid());
    iter.seek_for_prev(&encode(100));
    assert!(!iter.valid());
    iter.seek_to_last();
    assert!(!iter.valid());
}

#[test]
fn insert_and_lookup() {
    const N: usize = 2000;
    const R: u64 = 5000;
    let mut rnd = Random::new(1000);
    let mut keys: BTreeSet<Key> = BTreeSet::new();
    let mut list = new_list();
    for _ in 0..N {
        let key = u64::from(rnd.next()) % R;
        if keys.insert(key) {
            list.insert(&encode(key)).unwrap();
        }
    }

    for i in 0..R {
        assert_eq!(list.contains(&encode(i)), keys.contains(&i));
    }

    // Simple iterator tests
    {
        let mut iter = list.iter();
        assert!(!iter.valid());

        iter.seek(&encode(0));
        assert!(iter.valid());
        assert_eq!(*keys.first().unwrap(), decode(iter.key()));

        iter.seek_for_prev(&encode(R - 1));
        assert!(iter.valid());
        assert_eq!(*keys.last().unwrap(), decode(iter.key()));

        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(*keys.first().unwrap(), decode(iter.key()));

        iter.seek_to_last();
        assert!(iter.valid());
        assert_eq!(*keys.last().unwrap(), decode(iter.key()));
    }

    // Forward iteration test
    for i in 0..R {
        let mut iter = list.iter();
        iter.seek(&encode(i));

        // Compare against model iterator
        let mut model_iter = keys.range(i..);
        for _ in 0..3 {
            match model_iter.next() {
                None => {
                    assert!(!iter.valid());
                    break;
                }
                Some(&k) => {
                    assert!(iter.valid());
                    assert_eq!(k, decode(iter.key()));
                    iter.next();
                }
            }
        }
    }

    // Backward iteration test
    for i in 0..R {
        let mut iter = list.iter();
        iter.seek_for_prev(&encode(i));

        // Compare against model iterator
        let mut model_iter = keys.range(..=i).rev();
        for _ in 0..3 {
            match model_iter.next() {
                None => {
                    assert!(!iter.valid());
                    break;
                }
                Some(&k) => {
                    assert!(iter.valid());
                    assert_eq!(k, decode(iter.key()));
                    iter.prev();
                }
            }
        }
    }
}

/// `ConcurrentTest::K` of this file.
const K: u64 = 4;

#[test]
fn concurrent_without_threads() {
    ConcurrentTest::<K>::default().without_threads(random_seed());
}

fn run(run: u32) {
    run_concurrent::<K>(random_seed() + run * 100);
}

#[test]
fn concurrent1() {
    run(1);
}

#[test]
fn concurrent2() {
    run(2);
}

#[test]
fn concurrent3() {
    run(3);
}

#[test]
fn concurrent4() {
    run(4);
}

#[test]
fn concurrent5() {
    run(5);
}
