//! A compaction's merging, alone: inputs built once in a store on a buffered file, then merged
//! whole by [`Compaction`] as a maintenance worker merges a job, each case warmed with the same
//! inputs first. Two shapes, the trunk's two kinds of merge (research/50 §10):
//! - `consolidate`: a leaf's settle, one large branch and one newer of an eighth its entries,
//!   half of those overwriting keys the large one holds;
//! - `pivot`: a pivot's compaction of four in-flight branches of a quarter each, their keys
//!   drawn from one stream so each interleaves the others.
//!
//! `cargo bench -p mantle-engine --bench compaction -- DIR ENTRIES [ROUNDS] [--direct]`, the
//! last opening the store for direct I/O where the file system takes it. Sixteen-byte keys
//! and 100-byte values as `ranges`; reports, per input entry, wall and process CPU nanoseconds,
//! instructions and cycles, and the allocations a merge made.
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

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Instant;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_measure::{alloc, usage};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::merge::Compaction;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::store::{Config, Store};

#[path = "support/rocks_workload.rs"]
mod rocks_workload;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// A branch of `numbers`' keys, each holding `value`.
fn build(store: &mut Store<DeviceFile>, numbers: &BTreeSet<u64>, value: &[u8]) -> Branch {
    let mut b = Builder::new(store, Keys::Exactly(numbers.len() as u64)).unwrap();
    for &n in numbers {
        b.add(store, &rocks_workload::key(n), Op::Put, value)
            .unwrap();
    }
    b.finish(store).unwrap()
}

/// Merges `inputs` (newest first) whole; the entries written. The outputs' extents go back, so
/// rounds reuse the store's room.
fn merge(store: &mut Store<DeviceFile>, inputs: &[Branch]) -> u64 {
    let mut c = Compaction::new(store, inputs.iter(), b"", None, false, u64::MAX).unwrap();
    c.step(store, inputs.iter(), u64::MAX).unwrap();
    let parts = c.finish(store).unwrap();
    let mut written = 0;
    for (_, b) in parts {
        written += b.count;
        for &e in &b.extents {
            store.release(e).unwrap();
        }
    }
    written
}

fn main() -> Result<(), mantle_engine::Error> {
    let direct = std::env::args().any(|arg| arg == "--direct");
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench" && arg != "--direct")
        .collect();
    let dir = PathBuf::from(args.first().expect("DIR"));
    let entries: u64 = args.get(1).expect("ENTRIES").parse().unwrap();
    let rounds: u32 = args.get(2).map_or(3, |arg| arg.parse().unwrap());
    assert!(entries >= 8 && rounds > 0);
    let path = dir.join("compaction.store");
    let align = Alignment::new(4096).unwrap();
    let caching = if direct {
        CachingRequest::PreferDirect
    } else {
        CachingRequest::Buffered
    };
    let file = DeviceFile::open(&path, true, caching, align).unwrap();
    let mode = file.caching();
    let mut store = Store::create(
        file,
        Config {
            page_size: 4096,
            extent_pages: 32,
            max_extents: 1 << 24,
        },
    )
    .unwrap();
    store.set_cache((256 << 20) / store.page_size());
    let old = [b'o'; 100];
    let new = [b'n'; 100];
    // consolidate: even numbers below 2·entries, and an eighth as many newer keys, every other
    // one an even number the large branch holds.
    let large: BTreeSet<u64> = (0..entries).map(|n| n * 2).collect();
    let small: BTreeSet<u64> = (0..entries / 8).map(|n| n * 16 + (n % 2)).collect();
    let consolidate = vec![
        build(&mut store, &small, &new),
        build(&mut store, &large, &old),
    ];
    // pivot: four branches of a quarter each from one stream of numbers.
    let mut rng = rocks_workload::Rng::new(301, 1);
    let mut quarters = vec![BTreeSet::new(); 4];
    for q in &mut quarters {
        while (q.len() as u64) < entries / 4 {
            q.insert(rng.next() % (entries * 4));
        }
    }
    let pivot: Vec<Branch> = quarters
        .iter()
        .map(|q| build(&mut store, q, &new))
        .collect();
    println!(
        "fixture entries {entries} rounds {rounds} key_bytes 16 value_bytes 100 cache_mib 256 caching {mode:?}"
    );
    for (name, inputs) in [("consolidate", &consolidate), ("pivot", &pivot)] {
        let read: u64 = inputs.iter().map(|b| b.count).sum();
        let warmed = merge(&mut store, inputs);
        for round in 0..rounds {
            let before = usage::this().ok();
            alloc::begin_process();
            let start = Instant::now();
            let written = merge(&mut store, inputs);
            let wall = start.elapsed().as_nanos() as f64;
            let allocations = alloc::end_process();
            let account = usage::this()
                .ok()
                .zip(before)
                .map(|(now, before)| now.since(&before));
            assert_eq!(written, warmed);
            let per = |count: Option<u64>| {
                count.map_or_else(
                    || "unavailable".to_string(),
                    |count| format!("{:.1}", count as f64 / read as f64),
                )
            };
            println!(
                "{name} round {round} inputs {} read {read} written {written} wall_ns/entry {:.1} cpu_ns/entry {} instructions/entry {} cycles/entry {} allocations {} reallocations {}",
                inputs.len(),
                wall / read as f64,
                per(account.and_then(|a| a.user_ns.checked_add(a.system_ns))),
                per(account.and_then(|a| a.instructions)),
                per(account.and_then(|a| a.cycles)),
                allocations.allocations,
                allocations.reallocations,
            );
        }
    }
    Ok(())
}
