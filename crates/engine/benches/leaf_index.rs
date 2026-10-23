//! A branch's leaf index alone (`Branch::leaf_in`, the trie's `floor_with`; research/61 §2): the
//! step of a seek's placing that finds the leaf a key falls in. A branch of `KEYS` keys is built
//! with its leaf index as a seek finds it, in two shapes: the shard benchmark's 16-byte keys, a
//! big-endian number padded with '0' bytes as RocksDB's db_bench writes them (every other number,
//! so half the lookups fall between keys, as the seek benchmark's draws do), and object names under
//! a few thousand prefixes, an S3 listing's. Each shape is looked up `LOOKUPS` times at keys drawn
//! over twice its range, once to warm and once timed: nanoseconds and instructions a lookup, and
//! the index's bytes written and held in memory. A fourth argument names one shape to run alone, so
//! a profiler sees one shape's lookups.
//! `cargo bench -p mantle-engine --bench leaf_index -- DIR KEYS LOOKUPS [number|object]`
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_precision_loss
)]

use std::path::PathBuf;
use std::time::Instant;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_measure::usage;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::store::{Config, Store};

#[path = "support/rocks_workload.rs"]
mod rocks_workload;

/// The shard benchmark's key `n`.
fn number(n: u64) -> Vec<u8> {
    rocks_workload::key(n).to_vec()
}

/// The object name of key `n`: one of a few thousand prefixes, then the number, so names share
/// long runs, as a bucket's listing does.
fn object(n: u64) -> Vec<u8> {
    format!("tenant-{:04}/photos/2026/{:012}.jpg", n % 4099, n).into_bytes()
}

fn build(store: &mut Store<DeviceFile>, keys: u64, name: fn(u64) -> Vec<u8>) -> Branch {
    let mut all: Vec<Vec<u8>> = (0..keys).map(|n| name(n * 2)).collect();
    all.sort_unstable();
    let mut builder = Builder::new(store, Keys::Exactly(keys)).unwrap();
    for key in &all {
        builder.add(store, key, Op::Put, &[b'v'; 100]).unwrap();
    }
    builder.finish(store).unwrap()
}

/// The keys looked up: drawn over twice the branch's range, made before any is timed.
fn probes(keys: u64, count: u64, name: fn(u64) -> Vec<u8>) -> Vec<Vec<u8>> {
    let mut rng = rocks_workload::Rng::new(301, 3);
    (0..count).map(|_| name(rng.next() % (keys * 2))).collect()
}

/// Kept out of line, so a profiler can collect inside it alone.
#[inline(never)]
fn lookups(branch: &Branch, probes: &[Vec<u8>]) -> u64 {
    let mut sum = 0u64;
    for key in probes {
        // The keys' separators are never cut short here: every lookup is the index's alone.
        sum = sum.wrapping_add(u64::from(branch.leaf_in(key).unwrap().unwrap()));
    }
    std::hint::black_box(sum)
}

/// A key shape: its name and the key of a number.
type Shape = (&'static str, fn(u64) -> Vec<u8>);

fn main() {
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect();
    let dir = PathBuf::from(args.first().expect("DIR"));
    let keys: u64 = args.get(1).expect("KEYS").parse().unwrap();
    let count: u64 = args.get(2).expect("LOOKUPS").parse().unwrap();
    assert!(keys > 0 && count > 0);
    let only = args.get(3);
    let shapes: [Shape; 2] = [("number", number), ("object", object)];
    assert!(
        only.is_none_or(|o| shapes.iter().any(|(shape, _)| shape == o)),
        "SHAPE is number or object"
    );
    for (shape, name) in shapes {
        if only.is_some_and(|o| o != shape) {
            continue;
        }
        let path = dir.join(format!("leaf_index-{shape}.store"));
        let file = DeviceFile::open(
            &path,
            true,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .unwrap();
        let mut store = Store::create(
            file,
            Config {
                page_size: 4096,
                extent_pages: 32,
                max_extents: 1 << 24,
            },
        )
        .unwrap();
        let branch = build(&mut store, keys, name);
        let probes = probes(keys, count, name);
        lookups(&branch, &probes);
        let before = usage::this().ok();
        let start = Instant::now();
        lookups(&branch, &probes);
        let elapsed = start.elapsed();
        let account = usage::this()
            .ok()
            .zip(before)
            .map(|(now, before)| now.since(&before));
        let per = |n: Option<u64>| {
            n.map_or_else(
                || "unavailable".to_string(),
                |n| format!("{:.1}", n as f64 / count as f64),
            )
        };
        println!(
            "leaf_index shape {shape} keys {keys} lookups {count} pages {} index_bytes {} index_memory {} ns/op {:.1} cpu_ns/op {} instructions/op {}",
            branch.counts.len(),
            branch.index_bytes,
            branch.index.bytes(),
            elapsed.as_nanos() as f64 / count as f64,
            per(account.and_then(|a| a.user_ns.checked_add(a.system_ns))),
            per(account.and_then(|a| a.instructions)),
        );
        drop(store);
        std::fs::remove_file(&path).unwrap();
    }
}
