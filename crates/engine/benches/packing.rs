//! A memtable's packing, alone: the work a maintenance worker does to turn a full memtable into
//! the trunk's branches ([`pack_memtable`]): the arena's copy sorted, then walked in key order
//! into a branch's pages, each key's filter hash and offset listed for the index over kept
//! memtables. The memtable is filled once as `ranges` fills one (16-byte keys from the RocksDB
//! workload's seeded generator over 100M numbers, 100-byte values) until its arena's bound
//! refuses the next entry; each round packs its copy into a store on a buffered file, the
//! outputs' extents given back, after one round that warms the buffers.
//!
//! `cargo bench -p mantle-engine --bench packing -- DIR [MIB] [ROUNDS] [PIECES] [--direct]`: the
//! arena's bound in MiB (64, `ranges`'s write budget a memtable), the rounds (5), and the pieces
//! the root's pivot keys cut the memtable into (1: none cut; a fill packs into up to the root's
//! pivots). Reports per entry the wall and process CPU nanoseconds, and the allocations.
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
use mantle_engine::Error;
use mantle_engine::branch::{Op, filter};
use mantle_engine::memtable::hashed::HashMem;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::pool::{SortPack, pack_memtable};

#[path = "support/rocks_workload.rs"]
mod rocks_workload;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

fn main() -> Result<(), Error> {
    let direct = std::env::args().any(|arg| arg == "--direct");
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench" && arg != "--direct")
        .collect();
    let dir = PathBuf::from(args.first().expect("DIR"));
    let mib: usize = args.get(1).map_or(64, |arg| arg.parse().unwrap());
    let rounds: u32 = args.get(2).map_or(5, |arg| arg.parse().unwrap());
    let pieces: usize = args.get(3).map_or(1, |arg| arg.parse().unwrap());
    assert!(mib > 0 && rounds > 0 && pieces > 0);

    // The memtable, filled until its arena refuses the next entry, as a fill rotates it.
    let mut mem = HashMem::new(mib << 20)?;
    let mut rng = rocks_workload::Rng::new(301, 1);
    let value = [b'v'; 100];
    let mut keys: Vec<[u8; 16]> = Vec::new();
    loop {
        let key = rocks_workload::key(rng.next() % 100_000_000);
        match mem.insert_hashed(&key, filter::hash(&key), Op::Put, &value) {
            Ok(()) => keys.push(key),
            Err(Error::LimitExceeded { .. }) => break,
            Err(error) => return Err(error),
        }
    }
    let entries = mem.len();
    let mut arena = Vec::new();
    mem.copy_arena(&mut arena)?;
    // The root's pivot keys past its first: keys that cut the memtable's into equal shares.
    keys.sort_unstable();
    keys.dedup();
    let cuts: Vec<Vec<u8>> = (1..pieces)
        .map(|i| keys[i * keys.len() / pieces].to_vec())
        .collect();

    let path = dir.join("packing.store");
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
    )?;
    let mut pack = SortPack {
        entries: entries as u64,
        skip: mem.skip(),
        arena,
        cuts,
        ..SortPack::default()
    };
    println!(
        "fixture entries {entries} arena_mib {mib} pieces {pieces} key_bytes 16 value_bytes 100 caching {mode:?}"
    );
    // Round 0 warms the sorter's and the key list's buffers, as a worker's earlier packings do.
    for round in 0..=rounds {
        let before = usage::this().ok();
        alloc::begin_process();
        let start = Instant::now();
        let parts = pack_memtable(&mut store, &mut pack)?;
        let wall = start.elapsed().as_nanos() as f64;
        let allocations = alloc::end_process();
        let account = usage::this()
            .ok()
            .zip(before)
            .map(|(now, before)| now.since(&before));
        let written: u64 = parts.iter().map(|(_, b)| b.count).sum();
        assert_eq!(written, entries as u64);
        for (_, b) in &parts {
            for &e in &b.extents {
                store.release(e)?;
            }
        }
        if round == 0 {
            continue;
        }
        let per = |count: Option<u64>| {
            count.map_or_else(
                || "unavailable".to_string(),
                |count| format!("{:.1}", count as f64 / entries as f64),
            )
        };
        println!(
            "pack round {round} entries {entries} parts {} wall_ns/entry {:.1} cpu_ns/entry {} allocations {} reallocations {}",
            parts.len(),
            wall / entries as f64,
            per(account.and_then(|a| a.user_ns.checked_add(a.system_ns))),
            allocations.allocations,
            allocations.reallocations,
        );
    }
    Ok(())
}
