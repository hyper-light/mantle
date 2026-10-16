//! The index over kept memtables ([`KeptIndex`], research/51 §7) as a shard's workers extend it
//! through a fill: a list of one memtable's keys at a time (each key's 64-bit filter hash and its
//! arena offset, `PER` keys, as a packing walks them), added oldest first, `N` keys in all, as a
//! 100M fill's packings add theirs. Each key is drawn as db_bench's fillrandom draws them, a
//! number below `N` with replacement, so about 37% of the keys a list adds replace an older
//! list's entry (a memtable holds a key once: a list's draws are distinct among themselves); its
//! hash is the number mixed by SplitMix64's finalizer, a stand-in for xxh3's. Reports, per key
//! added: wall and process CPU nanoseconds, and instructions and cycles where the OS counts them;
//! the longest a list took, a worker's job; the keys held; and the index's bytes at the end. The
//! shard extends two indexes with every list, so a fill pays this twice.
//! `cargo bench -p mantle-engine --bench kept -- [N] [PER] [ROUNDS]`
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros
)]

use std::time::Instant;

use hyper_measure::usage;
use mantle_engine::kept::{KeptIndex, KeyList};

/// xorshift64: a fixed stream of distinct-looking 64-bit words.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// SplitMix64's finalizer: a number's stand-in hash, one-to-one.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let n: usize = args.first().map_or(64_000_000, |s| s.parse().unwrap());
    let per: usize = args.get(1).map_or(560_000, |s| s.parse().unwrap());
    let rounds: usize = args.get(2).map_or(1, |s| s.parse().unwrap());
    // The lists, built before any is timed: a memtable's keys, each drawn once in it, offsets
    // rising as an arena's do.
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let mut lists = Vec::new();
    let mut left = n;
    let mut number = 0u32;
    let mut seen = std::collections::HashSet::new();
    while left > 0 {
        let len = per.min(left);
        let mut list = KeyList::default();
        seen.clear();
        while list.len() < len {
            let key = rng.next() % n as u64;
            if seen.insert(key) {
                list.push(mix(key), (list.len() * 120) as u32);
            }
        }
        lists.push((number, list));
        number += 1;
        left -= len;
    }
    println!("kept draws {n} per {per} lists {}", lists.len());
    for round in 0..rounds {
        let mut index = KeptIndex::new();
        let mut longest = 0f64;
        let before = usage::this().ok();
        let start = Instant::now();
        for one in lists.chunks(1) {
            let t = Instant::now();
            index.extend(one, 0, false);
            longest = longest.max(t.elapsed().as_secs_f64());
        }
        let wall = start.elapsed().as_nanos() as f64;
        let account = usage::this()
            .ok()
            .zip(before)
            .map(|(now, before)| now.since(&before));
        let per_key = |count: Option<u64>| {
            count.map_or_else(
                || "unavailable".to_string(),
                |count| format!("{:.1}", count as f64 / n as f64),
            )
        };
        println!(
            "kept round {round} draws {n} held {} wall_ns/key {:.1} cpu_ns/key {} instructions/key {} cycles/key {} longest_list_ms {:.1} bytes {}",
            index.len(),
            wall / n as f64,
            per_key(account.map(|a| a.cpu_ns())),
            per_key(account.and_then(|a| a.instructions)),
            per_key(account.and_then(|a| a.cycles)),
            longest * 1e3,
            index.bytes(),
        );
    }
}
