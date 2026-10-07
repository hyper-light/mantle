//! The engine's `util::incmap::IncMap` against std's `HashMap` (hashbrown) with the identity hasher
//! the record cache used, on the operations the caches make of it: lookups that hit and miss in a
//! table of `N` entries, and churn (an insert and a removal, as a cache's eviction and fill).
//! Keys are xxh3-like random words, as the record cache's key hashes are. Each figure is the best
//! of `ROUNDS` timed passes (a busy machine adds time, never removes it).
//! `cargo bench -p mantle-engine --bench incmap -- [N] [OPS]`.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros
)]

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::hint::black_box;
use std::time::Instant;

use mantle_engine::util::incmap::IncMap;

const ROUNDS: usize = 5;

#[derive(Default)]
struct Identity(u64);

impl Hasher for Identity {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

type Std = HashMap<u64, u64, BuildHasherDefault<Identity>>;

fn keys(n: usize, seed: u64) -> Vec<u64> {
    let mut x = seed;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        })
        .collect()
}

/// The best of `ROUNDS` passes of `f`, in ns an operation.
fn best(ops: usize, mut f: impl FnMut() -> u64) -> f64 {
    let mut most = f64::MAX;
    for _ in 0..ROUNDS {
        let t = Instant::now();
        black_box(f());
        most = most.min(t.elapsed().as_nanos() as f64 / ops as f64);
    }
    most
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let n: usize = args.first().map_or(1_000_000, |s| s.parse().unwrap());
    let ops: usize = args.get(1).map_or(4_000_000, |s| s.parse().unwrap());
    let held = keys(n, 0x2545_f491_4f6c_dd1d);
    let absent = keys(ops, 0x9e37_79b9_7f4a_7c15);
    let mut x = 1u64;
    let probes: Vec<u64> = (0..ops)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            held[(x % n as u64) as usize]
        })
        .collect();
    let mut inc = IncMap::new();
    let mut std = Std::default();
    for (i, &k) in held.iter().enumerate() {
        inc.insert(k, i as u64);
        std.insert(k, i as u64);
    }
    // Reads finish a migration the fill left running, as the caches' reads do.
    while inc.migrating() {
        inc.settle();
    }
    let hit_inc = best(ops, || probes.iter().map(|&k| inc.get(k).unwrap()).sum());
    let hit_std = best(ops, || probes.iter().map(|&k| *std.get(&k).unwrap()).sum());
    let miss_inc = best(ops, || {
        absent.iter().filter(|&&k| inc.contains_key(k)).count() as u64
    });
    let miss_std = best(ops, || {
        absent.iter().filter(|&&k| std.contains_key(&k)).count() as u64
    });
    // Churn: each operation inserts a new key and removes the oldest held, the size steady,
    // as a cache evicts and fills. Each pass starts from the full table.
    let oldest = |i: usize| if i < n { held[i] } else { absent[i - n] };
    let churn_inc = best(ops, || {
        let mut m = IncMap::new();
        for (i, &k) in held.iter().enumerate() {
            m.insert(k, i as u64);
        }
        let mut gone = 0;
        for (i, &k) in absent.iter().enumerate() {
            m.insert(k, i as u64);
            gone += m.remove(oldest(i)).is_some() as u64;
        }
        gone
    }) - hit_inc * n as f64 / ops as f64;
    let churn_std = best(ops, || {
        let mut m = Std::default();
        for (i, &k) in held.iter().enumerate() {
            m.insert(k, i as u64);
        }
        let mut gone = 0;
        for (i, &k) in absent.iter().enumerate() {
            m.insert(k, i as u64);
            gone += m.remove(&oldest(i)).is_some() as u64;
        }
        gone
    }) - hit_std * n as f64 / ops as f64;
    println!("incmap n {n} ops {ops}");
    println!("get hit   IncMap {hit_inc:.1} ns  std {hit_std:.1} ns");
    println!("get miss  IncMap {miss_inc:.1} ns  std {miss_std:.1} ns");
    println!(
        "churn     IncMap {churn_inc:.1} ns  std {churn_std:.1} ns (each pass's fill excluded, approximately)"
    );
}
