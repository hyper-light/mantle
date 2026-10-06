//! One shard's engine (step E4) against RocksDB 11.8.1's `db_bench` on the same workload:
//! `fillrandom` then `readrandom`, 16-byte keys and 100-byte values, keys drawn uniformly from
//! `[0, num)` as db_bench draws them, a 64 MiB memtable (db_bench's `write_buffer_size`), no
//! compression, no write-ahead log (the Raft log is the engine's, docs/design/engine-structure.md
//! §2; `--disable_wal=1` on RocksDB's side), buffered reads through the OS page cache on both.
//! `cargo bench -p mantle-engine --bench shard_db -- DIR [NUM] [FANOUT] [buffered|direct]` prints
//! `workload num ops_per_s micros_per_op`, and the trunk's shape.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::path::PathBuf;
use std::time::Instant;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// db_bench's key: the number big-endian in the first 8 bytes, zeros after.
fn key(n: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&n.to_be_bytes());
    k
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let dir = PathBuf::from(args.first().expect("DIR"));
    let num: u64 = args.get(1).map_or(1_000_000, |s| s.parse().unwrap());
    let fanout: usize = args.get(2).map_or(8, |s| s.parse().unwrap());
    let path = dir.join("shard_db.store");
    let _ = std::fs::remove_file(&path);
    let align = Alignment::new(4096).unwrap();
    // `direct` as the fourth argument: transfers bypass the OS page cache (F_NOCACHE on macOS,
    // O_DIRECT on Linux); buffered by default, as db_bench reads.
    let caching = match args.get(3).map(String::as_str) {
        Some("direct") => CachingRequest::PreferDirect,
        _ => CachingRequest::Buffered,
    };
    let file = DeviceFile::open(&path, true, caching, align).unwrap();
    let config = Config {
        page_size: 4096,
        extent_pages: 32,
        max_extents: 1 << 24,
    };
    let mem = 64 << 20;
    // A leaf of about a memtable's entries, as SplinterDB sizes leaves by the memtable.
    let leaf_entries = (mem / (16 + 100 + 3)) as u64;
    let mut db = ShardDb::create(
        file,
        config,
        mem,
        TrunkConfig {
            fanout,
            leaf_entries,
        },
    )
    .unwrap();
    let value = [b'v'; 100];
    let mut rng = Rng(301);
    // Each operation timed, into a vector sized before the run: percentiles from the sorted
    // samples, as db_bench's --histogram=1 times each of its operations.
    let mut lat: Vec<u64> = Vec::with_capacity(num as usize);
    let t = Instant::now();
    for _ in 0..num {
        let k = key(rng.next() % num);
        let o = Instant::now();
        db.put(&k, &value).unwrap();
        lat.push(o.elapsed().as_nanos() as u64);
    }
    let s = t.elapsed().as_secs_f64();
    report("fillrandom", &mut lat);
    let (f, t, io) = db.stats();
    let user = num * (16 + 100);
    println!(
        "fill flushes {} pack {:.2}s (max {:.0}ms) incorporate {:.2}s (max {:.0}ms)",
        f.flushes,
        f.pack_ns as f64 / 1e9,
        f.pack_max_ns as f64 / 1e6,
        f.incorporate_ns as f64 / 1e9,
        f.incorporate_max_ns as f64 / 1e6
    );
    println!(
        "fill trunk pivot_compactions {} leaf_compactions {} flushes {} splits {} entries_written {} ({:.2}x the puts)",
        t.pivot_compactions,
        t.leaf_compactions,
        t.flushes,
        t.splits,
        t.entries_written,
        t.entries_written as f64 / num as f64
    );
    println!(
        "fill io reads {} ({:.2}s) writes {} ({:.2}s) pages_written {} ({:.2} GB, write amplification {:.2}) syncs {}",
        io.reads,
        io.read_ns as f64 / 1e9,
        io.writes,
        io.write_ns as f64 / 1e9,
        io.pages_written,
        io.pages_written as f64 * 4096.0 / 1e9,
        io.pages_written as f64 * 4096.0 / user as f64,
        io.syncs
    );
    println!(
        "fillrandom {num} {:.0} {:.3}",
        num as f64 / s,
        s * 1e6 / num as f64
    );
    let mut out = Vec::new();
    let mut found = 0u64;
    lat.clear();
    let t = Instant::now();
    for _ in 0..num {
        let k = key(rng.next() % num);
        let o = Instant::now();
        if db.get(&k, &mut out).unwrap() {
            found += 1;
        }
        lat.push(o.elapsed().as_nanos() as u64);
    }
    let s = t.elapsed().as_secs_f64();
    report("readrandom", &mut lat);
    println!(
        "readrandom {num} {:.0} {:.3} found {found}",
        num as f64 / s,
        s * 1e6 / num as f64
    );
    let (h, n, l) = db.shape().unwrap();
    println!("shape height {h} nodes {n} leaves {l}");
    drop(db);
    std::fs::remove_file(&path).unwrap();
}

/// Prints a workload's latency percentiles in microseconds: p50, p99, p99.9, p99.99, max.
fn report(name: &str, lat: &mut [u64]) {
    lat.sort_unstable();
    let at =
        |q: f64| lat[((lat.len() as f64 * q).max(0.0) as usize).min(lat.len() - 1)] as f64 / 1000.0;
    println!(
        "{name} latency_us p50 {:.2} p99 {:.2} p99.9 {:.2} p99.99 {:.2} max {:.2}",
        at(0.50),
        at(0.99),
        at(0.999),
        at(0.9999),
        lat[lat.len() - 1] as f64 / 1000.0
    );
}
