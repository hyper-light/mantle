//! The memtable against RocksDB 11.8.1's (docs/research/24 §4.1, §5 R5).
//!
//! `cargo bench -p mantle-engine --bench memtable -- [N ...]` runs the workloads of
//! `crates/engine/benches/p2_memtable_bench.cc`, the C++ side built against RocksDB, over the
//! same keys: `fill_random` (Add at SplitMix64 keys), `get_random` (Get of every key in a
//! SplitMix64 permutation, after the fill) and `fill_seq` (Add in key order), 16-byte keys and
//! 100-byte values, five runs each. It prints `workload N run ns_per_op bytes_per_entry`, as the
//! C++ does; docs/measurements/2026-09-30-engine-memtable.md records the runs.
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

use std::time::Instant;

use mantle_engine::branch::Op;
use mantle_engine::db::dbformat::{
    InternalKeyComparator, LookupKey, MAX_SEQUENCE_NUMBER, ValueType,
};
use mantle_engine::db::memtable::{Found, MemTable, MemTableOptions, MergeContext};
use mantle_engine::memtable::btree::BTreeMem;
use mantle_engine::util::comparator::Comparator;

const KEY: usize = 16;
const VALUE: usize = 100;
const RUNS: usize = 5;

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

fn random_keys(n: usize) -> Vec<[u8; KEY]> {
    let mut r = SplitMix64(0x6d65_6d74_6162_6c65); // "memtable"
    (0..n)
        .map(|_| {
            let mut k = [0u8; KEY];
            k[..8].copy_from_slice(&r.next().to_le_bytes());
            k[8..].copy_from_slice(&r.next().to_le_bytes());
            k
        })
        .collect()
}

fn seq_keys(n: usize) -> Vec<[u8; KEY]> {
    (0..n as u64)
        .map(|i| {
            let mut k = [0u8; KEY];
            k[8..].copy_from_slice(&i.to_be_bytes());
            k
        })
        .collect()
}

fn permutation(n: usize) -> Vec<usize> {
    let mut r = SplitMix64(0x6765_7467_6574);
    let mut p: Vec<usize> = (0..n).collect();
    for i in (2..=n).rev() {
        p.swap(i - 1, (r.next() % i as u64) as usize);
    }
    p
}

fn new_mem() -> MemTable {
    MemTable::new(
        InternalKeyComparator::new(Comparator::Bytewise),
        MemTableOptions::new(1 << 30),
        MAX_SEQUENCE_NUMBER,
    )
    .unwrap()
}

fn fill(mem: &mut MemTable, keys: &[[u8; KEY]]) {
    let value = [b'v'; VALUE];
    for (seq, k) in (1..).zip(keys) {
        mem.add(seq, ValueType::Value, k, &value).unwrap();
    }
}

fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let sizes = if sizes.is_empty() {
        vec![100_000, 1_000_000]
    } else {
        sizes
    };
    for n in sizes {
        let rkeys = random_keys(n);
        let skeys = seq_keys(n);
        let perm = permutation(n);
        for run in 0..RUNS {
            let mut mem = new_mem();
            let t0 = Instant::now();
            fill(&mut mem, &rkeys);
            let ns = t0.elapsed().as_nanos() as f64 / n as f64;
            let bytes = mem.approximate_memory_usage() as f64 / n as f64;
            println!("fill_random {n} {run} {ns:.1} {bytes:.1}");

            let mut found = 0usize;
            let t1 = Instant::now();
            for &i in &perm {
                let lk = LookupKey::new(&rkeys[i], MAX_SEQUENCE_NUMBER).unwrap();
                let mut mc = MergeContext::new();
                let mut max_cov = 0;
                if let Some(hit) = mem.get(&lk, &mut mc, &mut max_cov).unwrap()
                    && matches!(hit.found, Found::Value(_))
                {
                    found += 1;
                }
            }
            let gns = t1.elapsed().as_nanos() as f64 / n as f64;
            assert_eq!(found, n);
            println!("get_random {n} {run} {gns:.1} {bytes:.1}");
            drop(mem);

            let mut mem = new_mem();
            let t0 = Instant::now();
            fill(&mut mem, &skeys);
            let ns = t0.elapsed().as_nanos() as f64 / n as f64;
            let bytes = mem.approximate_memory_usage() as f64 / n as f64;
            println!("fill_seq {n} {run} {ns:.1} {bytes:.1}");
            drop(mem);

            // The shard's B-tree memtable (step E3) on the same keys and values.
            let value = [b'v'; VALUE];
            let mut bt = BTreeMem::new(1 << 30).unwrap();
            let t0 = Instant::now();
            for k in &rkeys {
                bt.insert(k, Op::Put, &value).unwrap();
            }
            let ns = t0.elapsed().as_nanos() as f64 / n as f64;
            let bytes = bt.memory() as f64 / n as f64;
            println!("btree_fill_random {n} {run} {ns:.1} {bytes:.1}");
            let mut out = Vec::with_capacity(VALUE);
            let mut found = 0usize;
            let t1 = Instant::now();
            for &i in &perm {
                if bt.get(&rkeys[i], &mut out).unwrap() == Some(Op::Put) {
                    found += 1;
                }
            }
            let gns = t1.elapsed().as_nanos() as f64 / n as f64;
            assert_eq!(found, n);
            println!("btree_get_random {n} {run} {gns:.1} {bytes:.1}");
            drop(bt);
            let mut bt = BTreeMem::new(1 << 30).unwrap();
            let t0 = Instant::now();
            for k in &skeys {
                bt.insert(k, Op::Put, &value).unwrap();
            }
            let ns = t0.elapsed().as_nanos() as f64 / n as f64;
            let bytes = bt.memory() as f64 / n as f64;
            println!("btree_fill_seq {n} {run} {ns:.1} {bytes:.1}");
        }
    }
}
