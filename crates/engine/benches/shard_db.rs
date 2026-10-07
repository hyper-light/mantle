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
use hyper_block::issuer::Issuer;
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
    // The reads after the fill, `num` by default; 0 measures the fill alone.
    let reads: u64 = args.get(4).map_or(num, |s| s.parse().unwrap());
    // The page cache in MiB, none by default: with buffered I/O the OS's cache serves reads.
    let cache_mib: usize = args.get(5).map_or(0, |s| s.parse().unwrap());
    // `attribute` as the seventh argument: each put slower than `SLOW_NS` is recorded with what
    // the engine did inside it, and the puts at or past p99.9 and p99.99 are broken down by cause.
    let attribute = args.get(6).map(String::as_str) == Some("attribute");
    // The device issuer's depth as the eighth argument, none by default: the store's runs are
    // then handed to it, that many out at once, and puts go on while the device writes.
    let issuer_depth: usize = args.get(7).map_or(0, |s| s.parse().unwrap());
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
    db.set_cache((cache_mib << 20) / 4096);
    let issuer = (issuer_depth > 0).then(|| Issuer::start(&dir, issuer_depth).unwrap());
    if let Some(issuer) = &issuer {
        db.attach(issuer, issuer.depth()).unwrap();
    }
    let value = [b'v'; 100];
    let mut rng = Rng(301);
    // Each operation timed, into a vector sized before the run: percentiles from the sorted
    // samples, as db_bench's --histogram=1 times each of its operations.
    let mut lat: Vec<u64> = Vec::with_capacity(num as usize);
    let mut slow: Vec<Slow> = Vec::new();
    let t = Instant::now();
    for _ in 0..num {
        let k = key(rng.next() % num);
        let before = attribute.then(|| db.stats());
        let o = Instant::now();
        db.put(&k, &value).unwrap();
        let ns = o.elapsed().as_nanos() as u64;
        lat.push(ns);
        if let Some(b) = before
            && ns >= SLOW_NS
        {
            slow.push(Slow::of(ns, b, db.stats()));
        }
    }
    let s = t.elapsed().as_secs_f64();
    if attribute {
        attribute_tail(&lat, &mut slow);
    }
    report("fillrandom", &mut lat);
    let (f, t, io) = db.stats();
    let user = num * (16 + 100);
    println!(
        "fill flushes {} pack {:.2}s (max slice {:.2}ms) trunk {:.2}s (max slice {:.2}ms) stalls {} ({:.2}s, max {:.2}ms)",
        f.flushes,
        f.pack_ns as f64 / 1e9,
        f.pack_max_ns as f64 / 1e6,
        f.incorporate_ns as f64 / 1e9,
        f.incorporate_max_ns as f64 / 1e6,
        f.stalls,
        f.stall_ns as f64 / 1e9,
        f.stall_max_ns as f64 / 1e6
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
        "fill io submitted {} write_waits {} ({:.2}s) span_cache_hits {} reads {} pages_read {} ({:.2}s) writes {} ({:.2}s) pages_written {} ({:.2} GB, write amplification {:.2}) syncs {}",
        io.submitted,
        io.write_waits,
        io.write_wait_ns as f64 / 1e9,
        io.span_cache_hits,
        io.reads,
        io.pages_read,
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
    // The maintenance the fill left owed, paid as a shard pays it in idle time, before reads.
    let t = Instant::now();
    db.maintain(u64::MAX).unwrap();
    println!("maintain {:.2}s", t.elapsed().as_secs_f64());
    let mut out = Vec::new();
    let mut found = 0u64;
    lat.clear();
    let t = Instant::now();
    for _ in 0..reads {
        let k = key(rng.next() % num);
        let o = Instant::now();
        if db.get(&k, &mut out).unwrap() {
            found += 1;
        }
        lat.push(o.elapsed().as_nanos() as u64);
    }
    let s = t.elapsed().as_secs_f64();
    if reads > 0 {
        report("readrandom", &mut lat);
        println!(
            "readrandom {reads} {:.0} {:.3} found {found}",
            reads as f64 / s,
            s * 1e6 / reads as f64
        );
    }
    let (_, _, io2) = db.stats();
    println!(
        "read io reads {} ({:.2}s) cache hits {} misses {}",
        io2.reads - io.reads,
        (io2.read_ns - io.read_ns) as f64 / 1e9,
        io2.cache_hits,
        io2.cache_misses
    );
    let (h, n, l) = db.shape().unwrap();
    println!("shape height {h} nodes {n} leaves {l}");
    drop(db);
    std::fs::remove_file(&path).unwrap();
}

/// Diagnostic: a put at least this slow is recorded with what the engine did inside it. Below
/// the measured p99.9 (61-77 us uncached at 10 M) and above p99 (2-3 us), so every put of the
/// p99.9 tail is recorded and few others.
const SLOW_NS: u64 = 10_000;

type Stats = (
    mantle_engine::shard_db::FlushStats,
    mantle_engine::trunk::TrunkStats,
    mantle_engine::store::IoStats,
);

/// One slow put: its time, and the engine's work inside it.
struct Slow {
    ns: u64,
    write_ns: u64,
    writes: u64,
    read_ns: u64,
    reads: u64,
    pack_ns: u64,
    trunk_ns: u64,
    stall_ns: u64,
    plan_ns: u64,
    finish_ns: u64,
    pack_finish_ns: u64,
    rotated: bool,
}

impl Slow {
    fn of(ns: u64, (f0, t0, i0): Stats, (f1, t1, i1): Stats) -> Self {
        Self {
            ns,
            write_ns: i1.write_ns - i0.write_ns,
            writes: i1.writes - i0.writes,
            read_ns: i1.read_ns - i0.read_ns,
            reads: i1.reads - i0.reads,
            pack_ns: f1.pack_ns - f0.pack_ns,
            trunk_ns: f1.incorporate_ns - f0.incorporate_ns,
            stall_ns: f1.stall_ns - f0.stall_ns,
            plan_ns: t1.plan_ns - t0.plan_ns,
            finish_ns: t1.finish_ns - t0.finish_ns,
            pack_finish_ns: f1.pack_finish_ns - f0.pack_finish_ns,
            rotated: f1.flushes != f0.flushes || f1.stalls != f0.stalls,
        }
    }
}

/// The puts at or past p99.9 and p99.99, broken down: mean time in write calls and read calls
/// (inside pack or trunk work), in pack and trunk work besides their I/O, in stalls, and the
/// rest (the memtable, a rotation); and how many made a write call, a read call, or neither.
fn attribute_tail(lat: &[u64], slow: &mut [Slow]) {
    let mut sorted = lat.to_vec();
    sorted.sort_unstable();
    for (name, q) in [("p99.9", 0.999), ("p99.99", 0.9999)] {
        let floor = sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)];
        let tail: Vec<&Slow> = slow.iter().filter(|s| s.ns >= floor).collect();
        let n = tail.len().max(1) as f64;
        let mean =
            |f: &dyn Fn(&Slow) -> u64| tail.iter().map(|s| f(s)).sum::<u64>() as f64 / n / 1000.0;
        let io = |s: &Slow| s.write_ns + s.read_ns;
        let work = |s: &Slow| s.pack_ns + s.trunk_ns;
        println!(
            "tail {name} (>= {:.2} us, {} puts): mean {:.2} us = writes {:.2} + reads {:.2} + work besides I/O {:.2} + stalls {:.2} + rest {:.2}; with a write call {}, a read call {}, neither {}, a rotation {}",
            floor as f64 / 1000.0,
            tail.len(),
            mean(&|s| s.ns),
            mean(&|s| s.write_ns),
            mean(&|s| s.read_ns),
            mean(&|s| work(s).saturating_sub(io(s)).saturating_sub(s.stall_ns)),
            mean(&|s| s.stall_ns),
            mean(&|s| s.ns.saturating_sub(work(s).max(io(s)))),
            tail.iter().filter(|s| s.writes > 0).count(),
            tail.iter().filter(|s| s.reads > 0).count(),
            tail.iter()
                .filter(|s| s.writes == 0 && s.reads == 0)
                .count(),
            tail.iter().filter(|s| s.rotated).count(),
        );
        println!(
            "tail {name} work: planning compactions {:.2} us, finishing them {:.2} us, finishing packed memtables {:.2} us, merging and packing the rest {:.2} us; puts that planned {}, finished {}, finished a pack {}",
            mean(&|s| s.plan_ns),
            mean(&|s| s.finish_ns),
            mean(&|s| s.pack_finish_ns),
            mean(&|s| (s.pack_ns + s.trunk_ns).saturating_sub(
                s.plan_ns + s.finish_ns + s.pack_finish_ns + s.write_ns + s.read_ns
            )),
            tail.iter().filter(|s| s.plan_ns > 0).count(),
            tail.iter().filter(|s| s.finish_ns > 0).count(),
            tail.iter().filter(|s| s.pack_finish_ns > 0).count(),
        );
    }
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
