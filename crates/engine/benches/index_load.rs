//! The memtable index's cost by load (research/54 §4): a `KeyIndex` of `B` buckets takes keys from
//! empty until four times its buckets were offered, a key refused once both its groups are full,
//! and in each sixteenth of load the bench times the inserts that filled it and counts those
//! refused, then gets of keys it holds and of keys it does not, at that load. Keys
//! stand in an array the index's entries name, so a tag that matches compares a key read at
//! random, as a memtable's probe reads its arena. Each table size runs `ROUNDS` times, a fresh
//! index each, and prints every round, so the spread between rounds is the measurement's own.
//!
//! `cargo bench -p mantle-engine --bench index_load -- [LOG2_BUCKETS ...]` prints
//! `buckets round sixteenths insert_ns hit_ns miss_ns refused`, a band's insert time a key offered.
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
    let mut index = KeyIndex::sized(buckets, 0);
    assert_eq!(index.buckets(), buckets);
    let mut keys: Vec<u64> = Vec::with_capacity(buckets);
    let mut held: Vec<usize> = Vec::with_capacity(buckets);
    let mut offered = 0usize;
    let mut rng = XorShift(0x9e37_79b9_7f4a_7c15 ^ (u64::from(log2) << 32) ^ round as u64);
    let probes = band.min(1 << 16);
    let mut misses: Vec<u64> = Vec::with_capacity(probes);
    let mut sixteenth = 0usize;
    // A band: keys offered until a sixteenth more of the buckets is held, or four times the
    // buckets were offered in all.
    while offered < 4 * buckets && held.len() < buckets {
        let goal = (held.len() + band).min(buckets);
        let mut refused = 0usize;
        let mut ns = 0u128;
        let mut tries = 0usize;
        while held.len() < goal && offered < 4 * buckets {
            // Fresh keys, made before the clock starts.
            let start = keys.len();
            let n = goal - held.len();
            for _ in 0..n {
                keys.push(rng.next());
            }
            let t = Instant::now();
            let mut taken = Vec::with_capacity(n);
            for e in start..keys.len() {
                let k = keys[e];
                match index.put(k, u32::try_from(e).unwrap(), |f| keys[f as usize] == k) {
                    Some(was) => {
                        black_box(was);
                        taken.push(e);
                    }
                    None => refused += 1,
                }
            }
            ns += t.elapsed().as_nanos();
            tries += n;
            offered += n;
            held.extend(taken);
        }
        let insert = ns as f64 / tries.max(1) as f64;
        sixteenth += 1;
        // Gets of held keys, drawn at random, and of keys never put.
        let picks: Vec<usize> = (0..probes)
            .map(|_| held[(rng.next() % held.len() as u64) as usize])
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
        println!("{buckets} {round} {sixteenth} {insert:.1} {hit:.1} {miss:.1} {refused}");
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
