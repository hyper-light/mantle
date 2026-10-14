//! The memtable index's cost by load (research/54 §4): a `KeyIndex` of `B` buckets is filled from
//! empty to its last bucket, and in each sixteenth of load the bench times the inserts that fill
//! it, then gets of keys it holds and of keys it does not, at that load. Keys
//! stand in an array the index's entries name, so a tag that matches compares a key read at
//! random, as a memtable's probe reads its arena. Each table size runs `ROUNDS` times, a fresh
//! index each, and prints every round, so the spread between rounds is the measurement's own.
//!
//! `cargo bench -p mantle-engine --bench index_load -- [LOG2_BUCKETS ...]` prints
//! `buckets round sixteenths insert_ns hit_ns miss_ns`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros
)]

use std::hint::black_box;
use std::time::Instant;

use mantle_engine::memtable::index::KeyIndex;

const ROUNDS: usize = 5;

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn run(log2: u32, round: usize) {
    let buckets = 1usize << log2;
    let band = buckets / 16;
    let most = buckets;
    let mut index = KeyIndex::sized(buckets, 0);
    assert_eq!(index.buckets(), buckets);
    let mut keys: Vec<u64> = Vec::with_capacity(most);
    let mut rng = XorShift(0x9e37_79b9_7f4a_7c15 ^ (u64::from(log2) << 32) ^ round as u64);
    let probes = band.min(1 << 16);
    let mut misses: Vec<u64> = Vec::with_capacity(probes);
    let mut sixteenth = 0usize;
    while keys.len() + band <= most {
        // Fresh keys, made before the clock starts.
        let start = keys.len();
        for _ in 0..band {
            keys.push(rng.next());
        }
        let t = Instant::now();
        for e in start..keys.len() {
            let k = keys[e];
            let was = index.put(k, u32::try_from(e).unwrap(), |f| keys[f as usize] == k);
            assert!(was.is_some(), "a key refused before the table was full");
            black_box(was);
        }
        let insert = t.elapsed().as_nanos() as f64 / band as f64;
        sixteenth += 1;
        // Gets of held keys, drawn at random, and of keys never put.
        let picks: Vec<usize> = (0..probes)
            .map(|_| (rng.next() % keys.len() as u64) as usize)
            .collect();
        let t = Instant::now();
        let mut found = 0usize;
        for &e in &picks {
            let k = keys[e];
            found += usize::from(index.get(k, |f| keys[f as usize] == k).is_some());
        }
        let hit = t.elapsed().as_nanos() as f64 / probes as f64;
        assert_eq!(found, probes, "a held key not found");
        misses.clear();
        for _ in 0..probes {
            misses.push(rng.next());
        }
        let t = Instant::now();
        let mut absent = 0usize;
        for &k in &misses {
            absent += usize::from(index.get(k, |f| keys[f as usize] == k).is_none());
        }
        let miss = t.elapsed().as_nanos() as f64 / probes as f64;
        black_box(absent);
        println!("{buckets} {round} {sixteenth} {insert:.1} {hit:.1} {miss:.1}");
    }
}

fn main() {
    let args: Vec<u32> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let sizes = if args.is_empty() {
        vec![16, 20, 23]
    } else {
        args
    };
    for round in 0..ROUNDS {
        for &log2 in &sizes {
            run(log2, round);
        }
    }
}
