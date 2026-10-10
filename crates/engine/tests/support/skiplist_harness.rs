//! What memtable/inlineskiplist_test.cc and memtable/skiplist_test.cc share: a list of 8-byte
//! keys ordered as u64s (`TestComparator`) and the one-writer, many-readers `ConcurrentTest`.
#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

use mantle_engine::memtable::inlineskiplist::{InlineSkipList, KeyComparator, ListRef, StoredKey};
use mantle_engine::util::hash::hash;

use super::random::Random;

pub type Key = u64;

/// `Encode`: the key's bytes in memory order, little-endian here as on every target.
pub fn encode(key: Key) -> [u8; 8] {
    key.to_le_bytes()
}

/// `Decode`.
pub fn decode(stored: StoredKey<'_>) -> Key {
    stored.word(0).unwrap()
}

/// `TestComparator`: keys ordered as the u64s they encode.
#[derive(Debug, Clone, Copy)]
pub struct TestComparator;

impl KeyComparator for TestComparator {
    fn compare(&self, stored: StoredKey<'_>, key: &[u8]) -> Ordering {
        decode(stored).cmp(&u64::from_le_bytes(key.try_into().unwrap()))
    }
}

pub type TestList = InlineSkipList<TestComparator>;

pub fn new_list() -> TestList {
    TestList::new(TestComparator).unwrap()
}

/// `ConcurrentTest`: keys `<key:24, gen:32, hash:8>`, K keys; see the C++ for the argument.
pub struct ConcurrentTest<const K: u64> {
    /// `current_`: the last generation inserted for each key.
    current: Vec<AtomicU64>,
    list: TestList,
}

pub fn key(k: Key) -> u64 {
    k >> 40
}
pub fn gen_of(k: Key) -> u64 {
    (k >> 8) & 0xffff_ffff
}
fn hash_of(k: Key) -> u64 {
    k & 0xff
}

fn hash_numbers(k: u64, g: u64) -> u64 {
    let mut data = [0u8; 16];
    data[..8].copy_from_slice(&k.to_le_bytes());
    data[8..].copy_from_slice(&g.to_le_bytes());
    u64::from(hash(&data, 0))
}

pub fn make_key(k: u64, g: u64) -> Key {
    (k << 40) | (g << 8) | (hash_numbers(k, g) & 0xff)
}

fn is_valid_key(k: Key) -> bool {
    hash_of(k) == (hash_numbers(key(k), gen_of(k)) & 0xff)
}

fn random_target<const K: u64>(rnd: &mut Random) -> Key {
    match rnd.next() % 10 {
        // Seek to beginning
        0 => make_key(0, 0),
        // Seek to end
        1 => make_key(K, 0),
        // Seek to middle
        _ => make_key(u64::from(rnd.next()) % K, 0),
    }
}

impl<const K: u64> Default for ConcurrentTest<K> {
    fn default() -> Self {
        Self {
            current: (0..K).map(|_| AtomicU64::new(0)).collect(),
            list: new_list(),
        }
    }
}

impl<const K: u64> ConcurrentTest<K> {
    /// `WriteStep`, with the writing handle of a split list.
    pub fn write_step(
        current: &[AtomicU64],
        inserter: &mut mantle_engine::memtable::inlineskiplist::Inserter<'_, TestComparator>,
        rnd: &mut Random,
    ) {
        let k = u64::from(rnd.next()) % K;
        let g = current[k as usize].load(AtomicOrdering::Acquire) + 1;
        let new_key = make_key(k, g);
        assert!(inserter.insert(&encode(new_key)).unwrap());
        current[k as usize].store(g, AtomicOrdering::Release);
    }

    /// `ReadStep`: every key present when the read began is seen.
    pub fn read_step(current: &[AtomicU64], list: ListRef<'_, TestComparator>, rnd: &mut Random) {
        // Remember the initial committed state of the skiplist.
        let initial: Vec<u64> = current
            .iter()
            .map(|g| g.load(AtomicOrdering::Acquire))
            .collect();

        let mut pos = random_target::<K>(rnd);
        let mut iter = list.iter();
        iter.seek(&encode(pos));
        loop {
            let current_key = if iter.valid() {
                let c = decode(iter.key());
                assert!(is_valid_key(c), "{c}");
                c
            } else {
                make_key(K, 0)
            };
            assert!(pos <= current_key, "should not go backwards");

            // Verify that everything in [pos,current) was not present in
            // initial_state.
            while pos < current_key {
                assert!(key(pos) < K, "{pos}");

                // Note that generation 0 is never inserted, so it is ok if
                // <*,0,*> is missing.
                assert!(
                    gen_of(pos) == 0 || gen_of(pos) > initial[key(pos) as usize],
                    "key: {}; gen: {}; initgen: {}",
                    key(pos),
                    gen_of(pos),
                    initial[key(pos) as usize]
                );

                // Advance to next key in the valid key space
                if key(pos) < key(current_key) {
                    pos = make_key(key(pos) + 1, 0);
                } else {
                    pos = make_key(key(pos), gen_of(pos) + 1);
                }
            }

            if !iter.valid() {
                break;
            }

            if rnd.next() % 2 == 1 {
                iter.next();
                pos = make_key(key(pos), gen_of(pos) + 1);
            } else {
                let new_target = random_target::<K>(rnd);
                if new_target > pos {
                    pos = new_target;
                    iter.seek(&encode(new_target));
                }
            }
        }
    }

    /// `ConcurrentWithoutThreads`/`ConcurrentReadWithoutThreads`: alternate reads and writes.
    pub fn without_threads(&mut self, seed: u32) {
        let mut rnd = Random::new(seed);
        let (mut inserter, view) = self.list.split();
        for _ in 0..10_000 {
            Self::read_step(&self.current, view, &mut rnd);
            Self::write_step(&self.current, &mut inserter, &mut rnd);
        }
    }
}

/// `RunConcurrent`/`RunConcurrentRead`: N rounds of one reader thread reading while this
/// thread writes kSize keys. The reader signals it is running through a barrier (the C++'s
/// STARTING→RUNNING wait) and stops on the quit flag; the scope's join is the C++'s DONE wait.
pub fn run_concurrent<const K: u64>(seed: u32) {
    let mut rnd = Random::new(seed);
    const N: usize = 1000;
    const K_SIZE: usize = 1000;
    for _ in 0..N {
        let mut t = ConcurrentTest::<K>::default();
        let quit = AtomicBool::new(false);
        let running = Barrier::new(2);
        let current = &t.current;
        let (mut inserter, view) = t.list.split();
        std::thread::scope(|s| {
            s.spawn(|| {
                let mut rnd = Random::new(seed + 1);
                running.wait();
                while !quit.load(AtomicOrdering::Acquire) {
                    ConcurrentTest::<K>::read_step(current, view, &mut rnd);
                }
            });
            running.wait();
            for _ in 0..K_SIZE {
                ConcurrentTest::<K>::write_step(current, &mut inserter, &mut rnd);
            }
            quit.store(true, AtomicOrdering::Release);
        });
    }
}

/// The keys of a list in iteration order.
pub fn contents(list: &TestList) -> Vec<Key> {
    let mut out = Vec::new();
    let mut iter = list.iter();
    iter.seek_to_first();
    while iter.valid() {
        out.push(decode(iter.key()));
        iter.next();
    }
    out
}

/// A model set and the list, checked equal.
pub fn check_against(list: &TestList, keys: &BTreeSet<Key>) {
    assert_eq!(contents(list), keys.iter().copied().collect::<Vec<_>>());
}
