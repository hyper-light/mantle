//! A branch's point reads alone: a branch of `ENTRIES` sixteen-byte keys and 100-byte values,
//! every page in the store's cache, then `READS` gets of its keys in a random order through
//! [`Branch::get`]: the filter, the leaf index, the page from the cache and the search in it.
//! Sized to stay in the CPU's caches or to pass them, the two show what a get's search costs
//! itself and what it waits on memory for.
//!
//! `cargo bench -p mantle-engine --bench point_read -- DIR ENTRIES READS [ROUNDS]`. Reports, a get,
//! wall and process CPU nanoseconds, instructions, and the allocations the reads made.
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

use std::path::PathBuf;
use std::time::Instant;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_measure::{alloc, usage};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Builder, Op};
use mantle_engine::store::{Config, Store};

#[path = "support/rocks_workload.rs"]
mod rocks_workload;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

fn main() -> Result<(), mantle_engine::Error> {
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect();
    let dir = PathBuf::from(args.first().expect("DIR"));
    let entries: u64 = args.get(1).expect("ENTRIES").parse().unwrap();
    let reads: u64 = args.get(2).expect("READS").parse().unwrap();
    let rounds: u32 = args.get(3).map_or(3, |arg| arg.parse().unwrap());
    assert!(entries > 0 && reads > 0 && rounds > 0);
    let path = dir.join("point_read.store");
    let align = Alignment::new(4096).unwrap();
    let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align).unwrap();
    let mut store = Store::create(
        file,
        Config {
            page_size: 4096,
            extent_pages: 32,
            max_extents: 1 << 24,
        },
    )
    .unwrap();
    let value = [b'v'; 100];
    let mut b = Builder::new(&mut store, Keys::Exactly(entries)).unwrap();
    for n in 0..entries {
        b.add(&mut store, &rocks_workload::key(n), Op::Put, &value)
            .unwrap();
    }
    let branch = b.finish(&mut store).unwrap();
    // Every page cached: the reads below find each in memory.
    let pages = branch.counts.len() + branch.extents.len();
    store.set_cache(pages.max(1) * 2);
    let mut out = Vec::new();
    for n in 0..entries {
        assert_eq!(
            branch
                .get(&mut store, &rocks_workload::key(n), &mut out)
                .unwrap(),
            Some(Op::Put)
        );
    }
    println!(
        "fixture entries {entries} reads {reads} rounds {rounds} pages {}",
        branch.counts.len()
    );
    let mut rng = rocks_workload::Rng::new(301, 2);
    let keys: Vec<[u8; 16]> = (0..reads)
        .map(|_| rocks_workload::key(rng.next() % entries))
        .collect();
    for round in 0..rounds {
        let before_io = store.io_stats();
        let before = usage::this().ok();
        alloc::begin_process();
        let start = Instant::now();
        let mut found = 0u64;
        for k in &keys {
            if branch.get(&mut store, k, &mut out).unwrap().is_some() {
                found += 1;
            }
        }
        let wall = start.elapsed().as_nanos() as f64;
        let allocations = alloc::end_process();
        let account = usage::this()
            .ok()
            .zip(before)
            .map(|(now, before)| now.since(&before));
        let device = store.io_stats().device_reads - before_io.device_reads;
        assert_eq!(found, reads);
        let per = |count: Option<u64>| {
            count.map_or_else(
                || "unavailable".to_string(),
                |count| format!("{:.1}", count as f64 / reads as f64),
            )
        };
        println!(
            "round {round} wall_ns/get {:.1} cpu_ns/get {} instructions/get {} allocations {} device_reads {device}",
            wall / reads as f64,
            per(account.and_then(|a| a.user_ns.checked_add(a.system_ns))),
            per(account.and_then(|a| a.instructions)),
            allocations.allocations,
        );
    }
    Ok(())
}
