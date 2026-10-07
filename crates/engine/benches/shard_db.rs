//! One shard's engine (step E4) against RocksDB 11.8.1's `db_bench` on the same workload:
//! `fillrandom` then `readrandom`, 16-byte keys and 100-byte values, keys drawn uniformly from
//! `[0, num)` as db_bench draws them, a 64 MiB memtable (db_bench's `write_buffer_size`), no
//! compression, no write-ahead log (the Raft log is the engine's, docs/design/engine-structure.md
//! §2; `--disable_wal=1` on RocksDB's side), buffered reads through the OS page cache on both.
//! `cargo bench -p mantle-engine --bench shard_db -- DIR [NUM] [FANOUT] [buffered|direct] ...` prints
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
use hyper_measure::{alloc, faults};
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// Starts a phase's count of every thread's allocations (the issuer's too) and of the
/// process's page faults.
fn begin() -> faults::Faults {
    alloc::begin_process();
    faults::read().unwrap()
}

/// A phase's allocations, reallocations and page faults, each per operation.
fn costs(name: &str, ops: u64, from: &faults::Faults) {
    let a = alloc::end_process();
    let f = faults::read().unwrap().since(from);
    let line = format!(
        "{name} allocs/op {:.3} reallocs/op {:.3} bytes/op {:.1} faults/op {:.4} (allocs {} reallocs {} faults {})",
        a.allocations as f64 / ops as f64,
        a.reallocations as f64 / ops as f64,
        a.bytes as f64 / ops as f64,
        f.total() as f64 / ops as f64,
        a.allocations,
        a.reallocations,
        f.total()
    );
    println!("{line}");
}

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
    // The runs the store may have out at once as the ninth argument, the issuer's depth by
    // default: extent buffers spared so puts go on while a contended device lags.
    let batches: usize = args.get(8).map_or(issuer_depth, |s| s.parse().unwrap());
    // Seeks after the reads as the tenth argument, none by default, each reading the eleventh's
    // count of keys from a random start: db_bench's seekrandom with --seek_nexts.
    let seeks: u64 = args.get(9).map_or(0, |s| s.parse().unwrap());
    let seek_nexts: usize = args.get(10).map_or(10, |s| s.parse().unwrap());
    // Each seek bounded to the twelfth argument's count of keys past its start, unbounded at 0
    // (the default): db_bench's --max_scan_distance, which sets the iterator's upper bound.
    let seek_distance: u64 = args.get(11).map_or(0, |s| s.parse().unwrap());
    // A mixed phase last, as db_bench's mixgraph runs one thread (uniform keys, 100-byte values,
    // scans of the eleventh argument's keys): the thirteenth argument's operations, none by
    // default, the fourteenth's percent of them puts and the fifteenth's seeks, the rest gets.
    // Nothing is paid between them but what each put pays, so seeks meet bundles in flight.
    let mix: u64 = args.get(12).map_or(0, |s| s.parse().unwrap());
    let mix_put_pct: u64 = args.get(13).map_or(50, |s| s.parse().unwrap());
    let mix_seek_pct: u64 = args.get(14).map_or(50, |s| s.parse().unwrap());
    // Write memory for runs waiting for the issuer, in MiB, as the sixteenth argument, none by
    // default (`ShardDb::set_write_budget`).
    let write_budget_mib: usize = args.get(15).map_or(0, |s| s.parse().unwrap());
    // A memory budget for the cache and write memory together, in MiB, as the seventeenth
    // argument, divided by the shard's tuner (`ShardDb::set_memory`) in place of the fixed
    // cache and write budget; none by default.
    let memory_mib: usize = args.get(16).map_or(0, |s| s.parse().unwrap());
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
    // The engine's own timers only when attributing: a clock read each slice costs every put.
    db.set_timed(attribute);
    let issuer =
        (issuer_depth > 0).then(|| Issuer::start_for(&dir, issuer_depth, batches.max(1)).unwrap());
    if let Some(issuer) = &issuer {
        db.attach(issuer, batches.max(1)).unwrap();
    }
    db.set_write_budget(write_budget_mib << 20);
    if memory_mib > 0 {
        db.set_memory(memory_mib << 20);
    }
    let value = [b'v'; 100];
    let mut rng = Rng(301);
    // Each operation timed, into a vector sized before the run: percentiles from the sorted
    // samples, as db_bench's --histogram=1 times each of its operations.
    let mut lat: Vec<u64> = Vec::with_capacity(num as usize);
    let mut slow: Vec<Slow> = Vec::new();
    let mark = begin();
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
    costs("fillrandom", num, &mark);
    if attribute {
        attribute_tail(&lat, &mut slow);
    }
    report("fillrandom", &mut lat);
    let (f, t, io) = db.stats();
    let user = num * (16 + 100);
    println!(
        "fill flushes {} pack {:.2}s (max slice {:.2}ms, most entries a put {}) trunk {:.2}s (max slice {:.2}ms, most keys a put {}) stalls {} ({:.2}s, max {:.2}ms)",
        f.flushes,
        f.pack_ns as f64 / 1e9,
        f.pack_max_ns as f64 / 1e6,
        f.pack_share_most,
        f.incorporate_ns as f64 / 1e9,
        f.incorporate_max_ns as f64 / 1e6,
        f.trunk_share_most,
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
        "fill io evict_steps_most {} submitted {} queued {} (most {}) write_waits {} ({:.2}s) span_cache_hits {} reads {} pages_read {} ({:.2}s) writes {} ({:.2}s) pages_written {} ({:.2} GB, write amplification {:.2}) syncs {}",
        io.cache_evict_steps_most,
        io.submitted,
        io.runs_queued,
        io.runs_queued_most,
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
    let mark = begin();
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
    costs("readrandom", reads.max(1), &mark);
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
    if seeks > 0 {
        let mut page = mantle_engine::rows::Rows::new();
        let mut next = Vec::new();
        let mut keys_read = 0u64;
        lat.clear();
        let (_, _, io_seek) = db.stats();
        let mark = begin();
        let t = Instant::now();
        let (skipped_before, opened_before) = db.scan_filtered();
        for _ in 0..seeks {
            let start = rng.next() % num;
            let from = key(start);
            let end = key(start.saturating_add(seek_distance));
            let bound = (seek_distance > 0).then_some(&end[..]);
            page.clear();
            let o = Instant::now();
            db.scan(&from, bound, seek_nexts, &mut page, &mut next)
                .unwrap();
            lat.push(o.elapsed().as_nanos() as u64);
            keys_read += page.len() as u64;
        }
        let s = t.elapsed().as_secs_f64();
        costs("seekrandom", seeks, &mark);
        let (_, _, io3) = db.stats();
        println!(
            "seekrandom io reads/seek {:.2} pages/seek {:.2} cache hits/seek {:.2} span cache hits/seek {:.2}",
            (io3.reads - io_seek.reads) as f64 / seeks as f64,
            (io3.pages_read - io_seek.pages_read) as f64 / seeks as f64,
            (io3.cache_hits - io_seek.cache_hits) as f64 / seeks as f64,
            (io3.span_cache_hits - io_seek.span_cache_hits) as f64 / seeks as f64
        );
        let (skipped, opened) = db.scan_filtered();
        println!(
            "seekrandom distance {seek_distance} sources/seek skipped {:.2} opened {:.2}",
            (skipped - skipped_before) as f64 / seeks as f64,
            (opened - opened_before) as f64 / seeks as f64
        );
        report("seekrandom", &mut lat);
        println!(
            "seekrandom {seeks} {:.0} {:.3} nexts {seek_nexts} keys read {keys_read}",
            seeks as f64 / s,
            s * 1e6 / seeks as f64
        );
    }
    if mix > 0 {
        let mut page = mantle_engine::rows::Rows::new();
        let mut next = Vec::new();
        let mut out = Vec::new();
        let (mut puts, mut gets, mut seeks_done) = (Vec::new(), Vec::new(), Vec::new());
        // With `attribute`, each slow put and seek recorded with what the engine did inside it.
        let (mut slow_puts, mut slow_seeks) = (Vec::new(), Vec::new());
        let (skipped_before, opened_before) = db.scan_filtered();
        let mark = begin();
        let t = Instant::now();
        for _ in 0..mix {
            let n = rng.next();
            let start = n % num;
            let k = key(start);
            let kind = (n >> 32) % 100;
            let before = attribute.then(|| db.stats());
            let o = Instant::now();
            if kind < mix_put_pct {
                db.put(&k, &value).unwrap();
                let ns = o.elapsed().as_nanos() as u64;
                puts.push(ns);
                if let Some(b) = before
                    && ns >= SLOW_NS
                {
                    slow_puts.push(Slow::of(ns, b, db.stats()));
                }
            } else if kind < mix_put_pct + mix_seek_pct {
                let end = key(start.saturating_add(seek_distance));
                let bound = (seek_distance > 0).then_some(&end[..]);
                page.clear();
                db.scan(&k, bound, seek_nexts, &mut page, &mut next)
                    .unwrap();
                let ns = o.elapsed().as_nanos() as u64;
                seeks_done.push(ns);
                if let Some(b) = before
                    && ns >= SLOW_NS
                {
                    slow_seeks.push(Slow::of(ns, b, db.stats()));
                }
            } else {
                db.get(&k, &mut out).unwrap();
                gets.push(o.elapsed().as_nanos() as u64);
            }
        }
        let s = t.elapsed().as_secs_f64();
        costs("mixgraph", mix, &mark);
        let (skipped, opened) = db.scan_filtered();
        let n_seeks = seeks_done.len().max(1) as f64;
        println!(
            "mixgraph puts {} gets {} seeks {} distance {seek_distance} sources/seek skipped {:.2} opened {:.2} views {}",
            puts.len(),
            gets.len(),
            seeks_done.len(),
            (skipped - skipped_before) as f64 / n_seeks,
            (opened - opened_before) as f64 / n_seeks,
            db.views()
        );
        if attribute {
            println!("mixgraph puts' tail:");
            attribute_tail(&puts, &mut slow_puts);
            println!("mixgraph seeks' tail:");
            attribute_tail(&seeks_done, &mut slow_seeks);
        }
        for (name, lat) in [
            ("mixgraph put", &mut puts),
            ("mixgraph get", &mut gets),
            ("mixgraph seek", &mut seeks_done),
        ] {
            if !lat.is_empty() {
                report(name, lat);
            }
        }
        println!(
            "mixgraph {mix} {:.0} {:.3}",
            mix as f64 / s,
            s * 1e6 / mix as f64
        );
    }
    let (cache_bytes, write_bytes) = db.memory_split();
    println!(
        "memory split cache {:.1} MiB write {:.1} MiB",
        cache_bytes as f64 / (1 << 20) as f64,
        write_bytes as f64 / (1 << 20) as f64
    );
    let (h, n, l) = db.shape().unwrap();
    let (_, trunk, _) = db.stats();
    println!(
        "shape height {h} nodes {n} leaves {l} views built {} dropped {}",
        trunk.views_built, trunk.views_dropped
    );
    let m = db.memory();
    println!(
        "memory filters {} B ({:.2} B/key) indexes {} B ({:.3} B/key) counts {} B ranges {} B ({:.2} B/key) views {} B ({:.2} B/key)",
        m.filters,
        m.filters as f64 / num as f64,
        m.indexes,
        m.indexes as f64 / num as f64,
        m.counts,
        m.ranges,
        m.ranges as f64 / num as f64,
        m.views,
        m.views as f64 / num as f64
    );
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
    insert_ns: u64,
    rotate_ns: u64,
    forget_ns: u64,
    wait_ns: u64,
    queue_ns: u64,
    ahead_ns: u64,
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
            insert_ns: f1.insert_ns - f0.insert_ns,
            rotate_ns: f1.rotate_ns - f0.rotate_ns,
            forget_ns: f1.forget_ns - f0.forget_ns,
            wait_ns: i1.write_wait_ns - i0.write_wait_ns,
            queue_ns: i1.queue_ns - i0.queue_ns,
            ahead_ns: i1.ahead_ns - i0.ahead_ns,
            rotated: f1.flushes != f0.flushes || f1.stalls != f0.stalls,
        }
    }
}

/// The puts at or past p99.9 and p99.99, broken down: mean time in write calls and read calls
/// (inside pack or trunk work), in pack and trunk work besides their I/O, in stalls, and the
/// rest (the memtable, a rotation); and how many made a write call, a read call, or neither.
fn attribute_tail(lat: &[u64], slow: &mut [Slow]) {
    // The five slowest puts, each with what it did.
    slow.sort_unstable_by_key(|s| std::cmp::Reverse(s.ns));
    for s in slow.iter().take(5) {
        println!(
            "worst put {:.2} us: writes {} ({:.2} us) reads {} ({:.2} us) pack {:.2} trunk {:.2} (plan {:.2} finish {:.2} pack finish {:.2}) stall {:.2} insert {:.2} (rotate {:.2}) forget {:.2} | queueing pages {:.2} (waiting for a run {:.2}) reading ahead {:.2} rotated {}",
            s.ns as f64 / 1000.0,
            s.writes,
            s.write_ns as f64 / 1000.0,
            s.reads,
            s.read_ns as f64 / 1000.0,
            s.pack_ns as f64 / 1000.0,
            s.trunk_ns as f64 / 1000.0,
            s.plan_ns as f64 / 1000.0,
            s.finish_ns as f64 / 1000.0,
            s.pack_finish_ns as f64 / 1000.0,
            s.stall_ns as f64 / 1000.0,
            s.insert_ns as f64 / 1000.0,
            s.rotate_ns as f64 / 1000.0,
            s.forget_ns as f64 / 1000.0,
            s.queue_ns as f64 / 1000.0,
            s.wait_ns as f64 / 1000.0,
            s.ahead_ns as f64 / 1000.0,
            s.rotated,
        );
    }
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
        println!(
            "tail {name} pages: queueing written pages {:.2} us (waiting for a run {:.2}), reading pages ahead {:.2} us, the rest of the work (merging, building) {:.2} us",
            mean(&|s| s.queue_ns),
            mean(&|s| s.wait_ns),
            mean(&|s| s.ahead_ns),
            mean(&|s| (s.pack_ns + s.trunk_ns).saturating_sub(s.queue_ns + s.ahead_ns)),
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
