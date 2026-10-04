//! RocksDB's memtable/inlineskiplist_test.cc, test for test: 19 of its 26 test definitions, with
//! the same literals.
//!
//! Left out, with the feature they test (`InsertConcurrently`: several writers inserting by
//! compare-and-swap, which the port drops for one writer per memtable, docs/research/24 §4.1):
//! `ConcurrentInsertWithoutThreads`, `ConcurrentInsert1`–`3` and `ConcurrentInsertWithHint1`–`3`.
//!
//! Adapted: `ConcurrentMultiGet` inserts from four threads at once. Its claim — a `MultiGet`
//! sees every key inserted before it began, the inserter's own last key above all, and never
//! returns a key below the one asked for — is kept with one inserting thread (which checks its
//! own last key after every insert) and three reading threads sampling the shared ring.
//!
//! The C++ lists hold keys in arena memory the test fills through `AllocateKey`; the port's
//! `insert` copies the key into the list's arena. `TEST_Validate` is `validate`. The C++ seeds
//! `MultiGetRandomized` and `ConcurrentMultiGet` from the clock; here from `test::RandomSeed`,
//! so a failure replays.
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

use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use harness::{
    ConcurrentTest, Key, TestList, check_against, decode, encode, new_list, run_concurrent,
};
use mantle_engine::memtable::inlineskiplist::Splice;
use random::{Random, random_seed};

/// `InlineSkipTest`'s `keys_` and its `Insert`/`InsertWithHint`/`Validate`.
#[derive(Default)]
struct Fixture {
    keys: BTreeSet<Key>,
}

impl Fixture {
    fn insert(&mut self, list: &mut TestList, key: Key) {
        list.insert(&encode(key)).unwrap();
        self.keys.insert(key);
    }

    fn insert_with_hint(&mut self, list: &mut TestList, key: Key, hint: &mut Splice) -> bool {
        let res = list.insert_with_hint(&encode(key), hint).unwrap();
        self.keys.insert(key);
        res
    }

    fn validate(&self, list: &TestList) {
        // Check keys exist.
        for &key in &self.keys {
            assert!(list.contains(&encode(key)));
        }
        // Iterate over the list, make sure keys appears in order and no extra
        // keys exist.
        let mut iter = list.iter();
        assert!(!iter.valid());
        iter.seek(&encode(0));
        for &key in &self.keys {
            assert!(iter.valid());
            assert_eq!(key, decode(iter.key()));
            iter.next();
        }
        assert!(!iter.valid());
        // Validate the list is well-formed.
        list.view().validate().unwrap();
        check_against(list, &self.keys);
    }
}

#[test]
fn empty() {
    let list = new_list();
    let mut key: Key = 10;
    assert!(!list.contains(&encode(key)));

    let mut iter = list.iter();
    assert!(!iter.valid());
    iter.seek_to_first();
    assert!(!iter.valid());
    key = 100;
    iter.seek(&encode(key));
    assert!(!iter.valid());
    iter.seek_for_prev(&encode(key));
    assert!(!iter.valid());
    iter.seek_to_last();
    assert!(!iter.valid());
}

#[test]
fn insert_and_lookup() {
    const N: usize = 2000;
    const R: u64 = 5000;
    let mut rnd = Random::new(1000);
    let mut keys = BTreeSet::new();
    let mut list = new_list();
    for _ in 0..N {
        let key = u64::from(rnd.next()) % R;
        if keys.insert(key) {
            list.insert(&encode(key)).unwrap();
        }
    }

    for i in 0..R {
        if list.contains(&encode(i)) {
            assert!(keys.contains(&i));
        } else {
            assert!(!keys.contains(&i));
        }
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

#[test]
fn insert_with_hint_sequential() {
    const N: u64 = 100_000;
    let mut f = Fixture::default();
    let mut list = new_list();
    let mut hint = Splice::new();
    for i in 0..N {
        let key = i;
        f.insert_with_hint(&mut list, key, &mut hint);
    }
    f.validate(&list);
}

#[test]
fn insert_with_hint_multiple_hints() {
    const N: usize = 100_000;
    const S: u32 = 100;
    let mut rnd = Random::new(534);
    let mut f = Fixture::default();
    let mut list = new_list();
    let mut hints: Vec<Splice> = (0..S).map(|_| Splice::new()).collect();
    let mut last_key = [0u64; S as usize];
    for _ in 0..N {
        let s = u64::from(rnd.uniform(S));
        last_key[s as usize] += 1;
        let key = (s << 32) + last_key[s as usize];
        f.insert_with_hint(&mut list, key, &mut hints[s as usize]);
    }
    f.validate(&list);
}

#[test]
fn insert_with_hint_multiple_hints_random() {
    const N: usize = 100_000;
    const S: u32 = 100;
    let mut rnd = Random::new(534);
    let mut f = Fixture::default();
    let mut list = new_list();
    let mut hints: Vec<Splice> = (0..S).map(|_| Splice::new()).collect();
    for _ in 0..N {
        let s = u64::from(rnd.uniform(S));
        let key = (s << 32) + u64::from(rnd.next());
        f.insert_with_hint(&mut list, key, &mut hints[s as usize]);
    }
    f.validate(&list);
}

#[test]
fn insert_with_hint_compatible_with_insert_without_hint() {
    const N: usize = 100_000;
    const S1: usize = 100;
    const S2: usize = 100;
    let mut rnd = Random::new(534);
    let mut f = Fixture::default();
    let mut list = new_list();
    let mut used = HashSet::new();
    let mut with_hint = [0u64; S1];
    let mut without_hint = [0u64; S2];
    let mut hints: Vec<Splice> = (0..S1).map(|_| Splice::new()).collect();
    for slot in &mut with_hint {
        loop {
            let s = u64::from(rnd.next());
            if used.insert(s) {
                *slot = s;
                break;
            }
        }
    }
    for slot in &mut without_hint {
        loop {
            let s = u64::from(rnd.next());
            if used.insert(s) {
                *slot = s;
                break;
            }
        }
    }
    for _ in 0..N {
        let s = rnd.uniform((S1 + S2) as u32) as usize;
        if s < S1 {
            let key = (with_hint[s] << 32) + u64::from(rnd.next());
            f.insert_with_hint(&mut list, key, &mut hints[s]);
        } else {
            let key = (without_hint[s - S1] << 32) + u64::from(rnd.next());
            f.insert(&mut list, key);
        }
    }
    f.validate(&list);
}

/// `MultiGet` with a callback that records the first entry for each query and stops.
fn multi_get_first(list: &TestList, queries: &[Key]) -> Vec<Option<Key>> {
    let encoded: Vec<[u8; 8]> = queries.iter().map(|&k| encode(k)).collect();
    let refs: Vec<&[u8]> = encoded.iter().map(|k| &k[..]).collect();
    let mut found = vec![None; queries.len()];
    list.view().multi_get(&refs, |i, entry| {
        if found[i].is_none() {
            found[i] = Some(decode(entry));
        }
        false // stop after first match
    });
    found
}

#[test]
fn multi_get_basic() {
    const N: u64 = 1000;
    let mut list = new_list();

    // Insert keys 0, 2, 4, ..., 2*(N-1)
    for i in 0..N {
        list.insert(&encode(i * 2)).unwrap();
    }

    // Query keys: 1, 101, 201, ..., 901 (odd, so exact matches won't exist)
    let query_keys: Vec<Key> = (0..10).map(|i| 1 + i * 100).collect();
    let found = multi_get_first(&list, &query_keys);

    // Verify: each query should find the next even number >= query key
    for (i, &q) in query_keys.iter().enumerate() {
        let expected = (q + 1) & !1u64; // round up to next even
        if expected < N * 2 {
            assert_eq!(found[i], Some(expected), "Query key {q}");
        }
    }
}

#[test]
fn multi_get_exact_matches() {
    const N: u64 = 500;
    let mut list = new_list();

    for i in 0..N {
        list.insert(&encode(i * 10)).unwrap();
    }

    // Query for exact matches: 0, 100, 200, ..., 900
    let query_keys: Vec<Key> = (0..10).map(|i| i * 100).collect();
    let found = multi_get_first(&list, &query_keys);
    for (i, &q) in query_keys.iter().enumerate() {
        assert_eq!(found[i], Some(q), "Key {q}");
    }
}

#[test]
fn multi_get_empty() {
    let list = new_list();

    // MultiGet on empty list should not crash
    let key = encode(42);
    list.view().multi_get(&[&key[..]], |_, _| false);

    // Zero keys
    list.view().multi_get(&[], |_, _| false);
}

#[test]
fn multi_get_single_key() {
    let mut list = new_list();
    list.insert(&encode(100)).unwrap();

    // Query for the exact key
    let found = multi_get_first(&list, &[100]);
    assert_eq!(found[0], Some(100));
}

#[test]
fn multi_get_randomized() {
    let seed = random_seed();
    let mut rnd = Random::new(seed);

    const N: usize = 5000;
    const R: u64 = 10_000;
    let mut list = new_list();
    let mut inserted = BTreeSet::new();

    for _ in 0..N {
        let key = u64::from(rnd.next()) % R;
        if inserted.insert(key) {
            list.insert(&encode(key)).unwrap();
        }
    }

    // Generate sorted query keys
    let mut query_keys: Vec<Key> = (0..100).map(|_| u64::from(rnd.next()) % R).collect();
    query_keys.sort_unstable();

    let found = multi_get_first(&list, &query_keys);

    // Validate against std::set::lower_bound
    for (i, &q) in query_keys.iter().enumerate() {
        assert_eq!(
            found[i],
            inserted.range(q..).next().copied(),
            "seed={seed} Query {q}"
        );
    }
}

/// Reproduces a bug where duplicate keys in a MultiGet batch cause an assertion
/// failure when the callback walks forward (e.g., merge operands).
#[test]
fn multi_get_duplicate_keys_with_callback_walk() {
    let mut list = new_list();

    // Insert keys: 10, 20, 30, 40, 50, 60
    for i in 1..=6u64 {
        list.insert(&encode(i * 10)).unwrap();
    }

    // Callback that walks forward through multiple entries before stopping.
    #[derive(Default, Clone, Copy)]
    struct WalkingCallbackArg {
        stop_at: Key,
        first_key: Key,
        num_visited: usize,
    }

    // Query with duplicate keys: [20, 20, 50]
    let query_keys: [Key; 3] = [20, 20, 50];
    let encoded: Vec<[u8; 8]> = query_keys.iter().map(|&k| encode(k)).collect();
    let refs: Vec<&[u8]> = encoded.iter().map(|k| &k[..]).collect();
    let mut cb_data = [WalkingCallbackArg::default(); 3];
    for (i, cb) in cb_data.iter_mut().enumerate() {
        cb.stop_at = if i == 0 { 40 } else { 0 }; // first query walks to 40
    }

    list.view().multi_get(&refs, |i, entry| {
        let cb = &mut cb_data[i];
        let k = decode(entry);
        if cb.num_visited == 0 {
            cb.first_key = k;
        }
        cb.num_visited += 1;
        // Walk forward until we reach stop_at (simulates merge accumulation)
        k < cb.stop_at
    });

    // First query for 20: should find 20 and walk forward through 30 (stop at 40)
    assert_eq!(cb_data[0].first_key, 20);
    assert!(cb_data[0].num_visited >= 2);

    // Second query for 20: should also find 20 (duplicate key)
    assert_eq!(cb_data[1].first_key, 20);

    // Third query for 50: should find 50
    assert_eq!(cb_data[2].first_key, 50);
}

/// `ConcurrentTest::K` of this file.
const K: u64 = 8;

#[test]
fn concurrent_read_without_threads() {
    ConcurrentTest::<K>::default().without_threads(random_seed());
}

fn run_concurrent_read(run: u32) {
    run_concurrent::<K>(random_seed() + run * 100);
}

#[test]
fn concurrent_read1() {
    run_concurrent_read(1);
}

#[test]
fn concurrent_read2() {
    run_concurrent_read(2);
}

#[test]
fn concurrent_read3() {
    run_concurrent_read(3);
}

#[test]
fn concurrent_read4() {
    run_concurrent_read(4);
}

#[test]
fn concurrent_read5() {
    run_concurrent_read(5);
}

/// `ConcurrentMultiGet`, with one inserting thread (see the file header).
#[test]
fn concurrent_multi_get() {
    const TOTAL_KEYS: usize = 20_000;
    const READERS: usize = 3;
    const RING_SIZE: usize = 1024;
    const EXTRA_QUERY_KEYS: usize = 15;
    let seed = random_seed();

    let mut list = new_list();

    // Generate a sequence of unique keys and shuffle them
    let mut all_keys: Vec<Key> = (1..=TOTAL_KEYS as u64).collect();
    let mut rnd = Random::new(seed);
    for i in (1..TOTAL_KEYS).rev() {
        let j = rnd.next() as usize % (i + 1);
        all_keys.swap(i, j);
    }

    // Shared ring buffer for cross-thread visibility checks
    let shared_ring: Vec<AtomicU64> = (0..RING_SIZE).map(|_| AtomicU64::new(0)).collect();
    let ring_cursor = AtomicUsize::new(0);
    let inserted = AtomicUsize::new(0);

    // A MultiGet over `query_keys` (sorted, deduplicated): each result is its query key or,
    // for a key not yet visible, a larger one — never a smaller.
    let check =
        |view: mantle_engine::memtable::inlineskiplist::ListRef<'_, harness::TestComparator>,
         query_keys: &[Key],
         must_see: Option<Key>| {
            let encoded: Vec<[u8; 8]> = query_keys.iter().map(|&k| encode(k)).collect();
            let refs: Vec<&[u8]> = encoded.iter().map(|k| &k[..]).collect();
            let mut results = vec![0u64; query_keys.len()];
            view.multi_get(&refs, |i, entry| {
                results[i] = decode(entry);
                false // point lookup: stop after first entry
            });
            for (j, &q) in query_keys.iter().enumerate() {
                if Some(q) == must_see {
                    assert_eq!(results[j], q, "seed={seed}: read-after-write");
                }
                if results[j] != 0 {
                    assert!(results[j] >= q, "seed={seed}: result below query");
                }
            }
        };

    // Samples up to EXTRA_QUERY_KEYS recently published keys from the ring.
    let sample = |rnd: &mut Random, query_keys: &mut Vec<Key>| {
        let cursor = ring_cursor.load(Ordering::Acquire);
        for _ in 0..EXTRA_QUERY_KEYS {
            let idx = if cursor > 0 {
                rnd.next() as usize % cursor.min(RING_SIZE)
            } else {
                0
            };
            let slot = (cursor.wrapping_sub(1).wrapping_sub(idx)) % RING_SIZE;
            let shared_key = shared_ring[slot].load(Ordering::Acquire);
            if shared_key != 0 {
                query_keys.push(shared_key);
            }
        }
        query_keys.sort_unstable();
        query_keys.dedup();
    };

    let (mut inserter, view) = list.split();
    std::thread::scope(|s| {
        for t in 0..READERS {
            let sample = &sample;
            let check = &check;
            let inserted = &inserted;
            s.spawn(move || {
                let mut rnd = Random::new(seed + t as u32 + 2);
                while inserted.load(Ordering::Acquire) < TOTAL_KEYS {
                    let mut query_keys = Vec::new();
                    sample(&mut rnd, &mut query_keys);
                    check(view, &query_keys, None);
                }
            });
        }
        let mut rnd = Random::new(seed + 1);
        for &my_key in &all_keys {
            // Insert the next unique key
            assert!(inserter.insert(&encode(my_key)).unwrap());

            // Publish to shared ring buffer so other threads can query it
            let slot = ring_cursor.fetch_add(1, Ordering::Relaxed);
            shared_ring[slot % RING_SIZE].store(my_key, Ordering::Release);

            // Build a MultiGet batch: the just-inserted key + random shared keys
            let mut query_keys = vec![my_key];
            sample(&mut rnd, &mut query_keys);
            check(view, &query_keys, Some(my_key));
            inserted.fetch_add(1, Ordering::Release);
        }
    });
    assert_eq!(inserted.load(Ordering::Relaxed), TOTAL_KEYS);
}
