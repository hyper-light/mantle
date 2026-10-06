//! One shard's engine (step E4) against RocksDB 11.8.1's `db_bench` on the same workload:
//! `fillrandom` then `readrandom`, 16-byte keys and 100-byte values, keys drawn uniformly from
//! `[0, num)` as db_bench draws them, a 64 MiB memtable (db_bench's `write_buffer_size`), no
//! compression, no write-ahead log (the Raft log is the engine's, docs/design/engine-structure.md
//! §2; `--disable_wal=1` on RocksDB's side), buffered reads through the OS page cache on both.
//! `cargo bench -p mantle-engine --bench shard_db -- DIR [NUM] [FANOUT]` prints
//! `workload num ops_per_s micros_per_op`, and the trunk's shape.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::path::PathBuf;
use std::time::Instant;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::{Config, Store};
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
    let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align).unwrap();
    let config = Config {
        page_size: 4096,
        extent_pages: 32,
        max_extents: 1 << 24,
    };
    let store = Store::create(file, config).unwrap();
    let mem = 64 << 20;
    // A leaf of about a memtable's entries, as SplinterDB sizes leaves by the memtable.
    let leaf_entries = (mem / (16 + 100 + 3)) as u64;
    let mut db = ShardDb::new(
        store,
        mem,
        TrunkConfig {
            fanout,
            leaf_entries,
        },
    )
    .unwrap();
    let value = [b'v'; 100];
    let mut rng = Rng(301);
    let t = Instant::now();
    for _ in 0..num {
        db.put(&key(rng.next() % num), &value).unwrap();
    }
    let s = t.elapsed().as_secs_f64();
    println!(
        "fillrandom {num} {:.0} {:.3}",
        num as f64 / s,
        s * 1e6 / num as f64
    );
    let mut out = Vec::new();
    let mut found = 0u64;
    let t = Instant::now();
    for _ in 0..num {
        if db.get(&key(rng.next() % num), &mut out).unwrap() {
            found += 1;
        }
    }
    let s = t.elapsed().as_secs_f64();
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
