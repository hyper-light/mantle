//! Data block reads against RocksDB 11.8.1's (`crates/engine/benches/p4_block_bench.cc`).
//!
//! `cargo bench -p mantle-engine --bench block_read -- N index key` builds the blocks the C++ side
//! builds, byte for byte, and runs its workloads once each: `scan` (every entry of every block),
//! `seek` (every key, in a SplitMix64 permutation, in the block that holds it) and `get` (the
//! same through `seek_for_get`), each handing one iterator's buffers to the next as the port's
//! table reader will. It prints `workload index key N ns_per_op allocations reallocations
//! minor_faults checksum` as the C++ does.
//! docs/measurements/2026-10-06-engine-block-read.md records the runs.
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

use hyper_measure::{alloc, faults};
use mantle_engine::db::dbformat::DISABLE_GLOBAL_SEQUENCE_NUMBER;
use mantle_engine::table::block_based::block::{Block, IterBuffers};
use mantle_engine::table::block_based::block_builder::{BlockBuilder, BlockBuilderOptions};
use mantle_engine::table::block_based::data_block_footer::DataBlockIndexType;
use mantle_engine::util::comparator::Comparator;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Starts counting a workload's allocations and faults.
fn mark() -> faults::Faults {
    alloc::begin();
    faults::read().unwrap()
}

/// The workload's allocations, reallocations and minor faults since `mark`.
fn counts(from: &faults::Faults) -> String {
    let a = alloc::end();
    let f = faults::read().unwrap().since(from);
    format!("{} {} {}", a.allocations, a.reallocations, f.minor)
}

const VALUE: usize = 100;
const BLOCK_SIZE: usize = 4096;
const RESTART: u32 = 16;
const DEVIATION: usize = 10;

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

fn key(i: u64, len: usize) -> Vec<u8> {
    let mut k = vec![b'k'; len - 8];
    k.extend_from_slice(&i.to_be_bytes());
    // PackSequenceAndType(1, kTypeValue), little-endian.
    k.extend_from_slice(&((1u64 << 8) | 1).to_le_bytes());
    k
}

fn main() {
    let args: Vec<String> = std::env::args().filter(|a| !a.starts_with('-')).collect();
    let n: usize = args.get(1).map_or(1_000_000, |a| a.parse().unwrap());
    let hash = args.get(2).is_some_and(|a| a == "1");
    let key_len: usize = args.get(3).map_or(16, |a| a.parse().unwrap());
    // Passes over each workload, for profiling one long enough to sample; times are per pass.
    let passes: usize = args.get(4).map_or(1, |a| a.parse().unwrap());
    // `scan` stops after the scan, to profile or compare it alone.
    let scan_only = args.get(5).is_some_and(|a| a == "scan");
    let mut rng = SplitMix64(0x62_6c6f_636b);
    let keys: Vec<Vec<u8>> = (0..n as u64).map(|i| key(i * 7, key_len)).collect();
    let mut value = vec![0u8; VALUE];
    let mut blocks = Vec::new();
    let mut where_ = vec![0u32; n];
    let options = BlockBuilderOptions {
        restart_interval: RESTART,
        index_type: if hash {
            DataBlockIndexType::BinaryAndHash
        } else {
            DataBlockIndexType::BinarySearch
        },
        ..BlockBuilderOptions::default()
    };
    let mut b = BlockBuilder::new(options).unwrap();
    let limit = (BLOCK_SIZE * (100 - DEVIATION)).div_ceil(100);
    let cut = |b: &mut BlockBuilder, blocks: &mut Vec<Block>| {
        blocks.push(Block::new(b.finish().unwrap().to_vec(), RESTART));
        b.reset();
    };
    for i in 0..n {
        for c in &mut value {
            *c = rng.next() as u8;
        }
        if !b.is_empty() {
            let cur = b.current_size_estimate();
            if cur >= BLOCK_SIZE
                || (b.estimate_size_after_kv(&keys[i], &value) > BLOCK_SIZE && cur > limit)
            {
                cut(&mut b, &mut blocks);
            }
        }
        b.add(&keys[i], &value, None, false).unwrap();
        where_[i] = blocks.len() as u32;
    }
    cut(&mut b, &mut blocks);
    let mut order: Vec<usize> = (0..n).collect();
    for i in (2..=n).rev() {
        order.swap(i - 1, (rng.next() % i as u64) as usize);
    }

    let mut sum = 0u64;
    let mut entries = 0usize;
    let m = mark();
    let t0 = Instant::now();
    let mut buffers = IterBuffers::default();
    for blk in std::iter::repeat_n(&blocks, passes).flatten() {
        let mut it = blk.new_data_iterator_in(
            Comparator::Bytewise,
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
            buffers,
        );
        it.seek_to_first();
        while it.valid() {
            sum += u64::from(it.key()[key_len - 1]) + u64::from(it.value()[0]);
            entries += 1;
            it.next();
        }
        buffers = it.into_buffers();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    let c = counts(&m);
    println!(
        "scan {} {key_len} {n} {:.2} {c} {sum}",
        u8::from(hash),
        ns / entries as f64
    );

    sum = 0;
    if scan_only {
        return;
    }
    let m = mark();
    let t0 = Instant::now();
    for &i in std::iter::repeat_n(&order, passes).flatten() {
        let mut it = blocks[where_[i] as usize].new_data_iterator_in(
            Comparator::Bytewise,
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
            buffers,
        );
        it.seek(&keys[i]);
        sum += u64::from(it.value()[0]);
        buffers = it.into_buffers();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    let c = counts(&m);
    println!(
        "seek {} {key_len} {n} {:.2} {c} {sum}",
        u8::from(hash),
        ns / (n * passes) as f64
    );

    sum = 0;
    let m = mark();
    let t0 = Instant::now();
    for &i in std::iter::repeat_n(&order, passes).flatten() {
        let mut it = blocks[where_[i] as usize].new_data_iterator_in(
            Comparator::Bytewise,
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
            buffers,
        );
        if it.seek_for_get(&keys[i]) {
            sum += u64::from(it.value()[0]);
        }
        buffers = it.into_buffers();
    }
    let ns = t0.elapsed().as_nanos() as f64;
    let c = counts(&m);
    println!(
        "get {} {key_len} {n} {:.2} {c} {sum}",
        u8::from(hash),
        ns / (n * passes) as f64
    );
}
