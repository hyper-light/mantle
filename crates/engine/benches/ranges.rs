//! A node's ranges on hyper-rt's shards (step E2) against RocksDB 11.8.1's `db_bench` at the same
//! thread count: `THREADS` client threads, each doing `NUM` operations as each of db_bench's
//! `--threads` does, against `THREADS` ranges on `THREADS` shards, the keyspace `[0, NUM)` cut
//! evenly between them. 16-byte keys, 100-byte values, keys drawn uniformly as db_bench draws
//! them; the memtables' total is db_bench's (`write_buffer_size` 64 MiB, two of them), shared
//! between the ranges; no write-ahead log on either side (the Raft log is the engine's,
//! docs/design/engine-structure.md §2; `--disable_wal=1`); buffered reads through the OS's cache.
//! The runtime is configured from the machine's calibration, with the client threads' cores
//! reserved. `cargo bench -p mantle-engine --bench ranges -- DIR NUM THREADS [READS] [CACHE_MIB]
//! [ISSUER_DEPTH] [SEEKS] [SEEK_NEXTS]` prints each phase's throughput and every operation's
//! percentiles.
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
use std::time::{Duration, Instant};

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::machine::calibration::{Calibration, Policy};
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::ranges::{Client, Ranges, RangesConfig};
use mantle_engine::rows::Rows;
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

fn report(name: &str, ops: u64, s: f64, lat: &mut [u64]) {
    lat.sort_unstable();
    let at =
        |q: f64| lat[((lat.len() as f64 * q).max(0.0) as usize).min(lat.len() - 1)] as f64 / 1000.0;
    println!(
        "{name} {ops} ops {:.0} ops/s latency_us p50 {:.2} p99 {:.2} p99.9 {:.2} p99.99 {:.2} max {:.2}",
        ops as f64 / s,
        at(0.50),
        at(0.99),
        at(0.999),
        at(0.9999),
        lat[lat.len() - 1] as f64 / 1000.0
    );
}

/// Runs `op` `n` times on each of `threads` client threads, each with its own client and
/// generator, and reports the throughput and every operation's latency.
fn phase(
    name: &str,
    ranges: &Ranges,
    threads: u64,
    n: u64,
    op: impl Fn(&mut Client<'_>, &mut Rng, &mut Vec<u8>) -> bool + Sync,
) {
    let t = Instant::now();
    let (mut lat, found): (Vec<u64>, u64) = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|i| {
                let op = &op;
                s.spawn(move || {
                    let mut client = ranges.client().unwrap();
                    let mut rng = Rng(301 + i * 7919);
                    let mut lat = Vec::with_capacity(n as usize);
                    let mut buf = Vec::new();
                    let mut found = 0u64;
                    for _ in 0..n {
                        let o = Instant::now();
                        found += u64::from(op(&mut client, &mut rng, &mut buf));
                        lat.push(o.elapsed().as_nanos() as u64);
                    }
                    (lat, found)
                })
            })
            .collect();
        let mut all = Vec::with_capacity((threads * n) as usize);
        let mut found = 0;
        for h in handles {
            let (lat, f) = h.join().unwrap();
            all.extend(lat);
            found += f;
        }
        (all, found)
    });
    let s = t.elapsed().as_secs_f64();
    report(name, threads * n, s, &mut lat);
    println!("{name} found {found}");
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let dir = PathBuf::from(args.first().expect("DIR"));
    let num: u64 = args.get(1).map_or(1_000_000, |s| s.parse().unwrap());
    let threads: u64 = args.get(2).map_or(4, |s| s.parse().unwrap());
    let reads: u64 = args.get(3).map_or(num, |s| s.parse().unwrap());
    let cache_mib: usize = args.get(4).map_or(0, |s| s.parse().unwrap());
    let issuer_depth: usize = args.get(5).map_or(0, |s| s.parse().unwrap());
    let seeks: u64 = args.get(6).map_or(0, |s| s.parse().unwrap());
    let seek_nexts: usize = args.get(7).map_or(10, |s| s.parse().unwrap());
    // The client's and shards' spin window in ns as the ninth argument, the calibration's by
    // default: for measuring what the window costs and buys.
    let spin_override: Option<u64> = args.get(8).map(|s| s.parse().unwrap());

    let calibration = Calibration::measure(Duration::from_millis(40), threads as u16).unwrap();
    let constants = calibration
        .constants(&Policy {
            reserved_cores: threads as u16,
            lateness_tolerance_ns: None,
            latency_objective_ns: None,
        })
        .unwrap();
    // One range a shard: a shard's tasks are its range's.
    let mut rt = RuntimeConfig::from_calibration(&calibration, &constants, 1, 1);
    rt.shards = threads as u16;
    rt.pin = false;
    rt.cores.clear();
    if let Some(spin) = spin_override {
        rt.spin_ns = spin;
        rt.wake_tracking = None;
    }
    println!(
        "runtime shards {} step_budget_ns {} spin_ns {} batch {}",
        rt.shards, rt.step_budget_ns, rt.spin_ns, rt.batch
    );
    let runtime = Runtime::start(&rt).unwrap();

    let align = Alignment::new(4096).unwrap();
    let config = Config {
        page_size: 4096,
        extent_pages: 32,
        max_extents: 1 << 24,
    };
    // db_bench's 64 MiB memtable, shared between the ranges.
    let mem = (64usize << 20) / threads as usize;
    let leaf_entries = (mem / (16 + 100 + 3)) as u64;
    let issuer = (issuer_depth > 0)
        .then(|| Issuer::start_for(&dir, issuer_depth, issuer_depth * threads as usize).unwrap());
    let mut engines = Vec::new();
    let mut paths = Vec::new();
    for i in 0..threads {
        let path = dir.join(format!("range-{i}.store"));
        let _ = std::fs::remove_file(&path);
        let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align).unwrap();
        let mut db = ShardDb::create(
            file,
            config,
            mem,
            TrunkConfig {
                fanout: 8,
                leaf_entries,
            },
        )
        .unwrap();
        db.set_cache((cache_mib << 20) / 4096 / threads as usize);
        if let Some(issuer) = &issuer {
            db.attach(issuer, issuer_depth).unwrap();
        }
        let start = if i == 0 {
            Vec::new()
        } else {
            key(i * num / threads).to_vec()
        };
        engines.push((start, db));
        paths.push(path);
    }
    let ranges = Ranges::start(
        &runtime,
        engines,
        RangesConfig {
            clients: threads as usize + 1,
            slice_ns: rt.step_budget_ns,
            spin_ns: rt.spin_ns,
        },
    )
    .unwrap();

    let value = [b'v'; 100];
    phase("fillrandom", &ranges, threads, num, |c, rng, _| {
        c.put(&key(rng.next() % num), &value).unwrap();
        true
    });
    if reads > 0 {
        phase("readrandom", &ranges, threads, reads, |c, rng, buf| {
            c.get(&key(rng.next() % num), buf).unwrap()
        });
    }
    if seeks > 0 {
        phase("seekrandom", &ranges, threads, seeks, |c, rng, _| {
            // Each operation's buffers are its own here; a client that keeps them allocates
            // nothing per seek.
            let mut page = Rows::new();
            let mut next = Vec::new();
            c.scan(
                &key(rng.next() % num),
                None,
                seek_nexts,
                &mut page,
                &mut next,
            )
            .unwrap();
            !page.is_empty()
        });
    }
    let mut c = ranges.client().unwrap();
    for (i, (f, t, io)) in c.stats().unwrap().iter().enumerate() {
        println!(
            "range {i} flushes {} stalls {} ({:.2}s) leaf_compactions {} splits {} io writes {} reads {}",
            f.flushes,
            f.stalls,
            f.stall_ns as f64 / 1e9,
            t.leaf_compactions,
            t.splits,
            io.submitted,
            io.reads
        );
    }
    drop(c);
    ranges.stop().unwrap();
    runtime.shutdown().unwrap();
    drop(issuer);
    for p in paths {
        std::fs::remove_file(&p).unwrap();
    }
}
