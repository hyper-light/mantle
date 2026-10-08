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
use hyper_measure::{alloc, faults, usage};
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
    // `attribute` as the seventh argument: each put at or past the running p99.5 is recorded with what
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
    // Reads skewed as db_bench's --read_random_exp_range draws them, as the eighteenth
    // argument, uniform at 0 (the default): a key's rank is exponential, then scattered by a
    // large prime so hot keys are not neighbours.
    let read_exp_range: f64 = args.get(17).map_or(0.0, |s| s.parse().unwrap());
    // A cache of hot records for point reads, in MiB, as the nineteenth argument, none by default
    // (`ShardDb::set_record_cache`).
    let record_mib: usize = args.get(18).map_or(0, |s| s.parse().unwrap());
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
    if record_mib > 0 {
        db.set_record_cache(record_mib << 20);
    }
    let value = [b'v'; 100];
    let mut rng = Rng(301);
    // Each operation timed, into a vector sized before the run: percentiles from the sorted
    // samples, as db_bench's --histogram=1 times each of its operations.
    // Touched before the run, a write a page (zeroed memory is mapped only once written), so a
    // sample's push takes no page fault inside the timing.
    let mut lat: Vec<u64> = vec![0; num as usize];
    lat.iter_mut().step_by(512).for_each(|x| *x = 1);
    lat.clear();
    let mut slow: Recorder<Slow> = Recorder::new(if attribute { num } else { 0 });
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
            && slow.wants(ns)
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
        "fill io evict_steps_most {} submitted {} queued {} (most {}) write_waits {} ({:.2}s) span_cache_hits {} reads {} pages_read {} ({:.2}s) writes {} ({:.2}s) pages_written {} ({:.2} GB, write amplification {:.2}) syncs {} prefetches {} prefetch_waits {} seals {:.2}s (most {:.2}ms)",
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
        io.syncs,
        io.prefetches,
        io.prefetch_waits,
        io.seal_ns as f64 / 1e9,
        io.seal_most_ns as f64 / 1e6
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
    // With `attribute`, each slow get recorded with what the engine did inside it, and the
    // page faults and context switches the process was charged meanwhile.
    let mut slow_gets: Recorder<(Slow, u64, u64)> =
        Recorder::new(if attribute { reads } else { 0 });
    for _ in 0..reads {
        let k = key(skewed(rng.next(), num, read_exp_range));
        let before = attribute.then(|| {
            (
                db.stats(),
                faults::read().unwrap(),
                faults::switches().unwrap(),
            )
        });
        let o = Instant::now();
        if db.get(&k, &mut out).unwrap() {
            found += 1;
        }
        let ns = o.elapsed().as_nanos() as u64;
        lat.push(ns);
        if let Some((b, f, w)) = before
            && slow_gets.wants(ns)
        {
            let f = faults::read().unwrap().since(&f).total();
            let w = faults::switches().unwrap() - w;
            slow_gets.push((Slow::of(ns, b, db.stats()), f, w));
        }
    }
    let s = t.elapsed().as_secs_f64();
    if attribute {
        let floor = slow_gets.floor;
        let dropped = slow_gets.dropped;
        let mut slow_gets = std::mem::take(&mut slow_gets.events);
        slow_gets.sort_unstable_by_key(|s| std::cmp::Reverse(s.0.ns));
        for (g, f, w) in slow_gets.iter().take(8) {
            println!(
                "worst get {:.2} us: reads {} ({:.2} us) rest {:.2} us faults {f} switches {w}",
                g.ns as f64 / 1000.0,
                g.reads,
                g.read_ns as f64 / 1000.0,
                g.ns.saturating_sub(g.read_ns) as f64 / 1000.0,
            );
        }
        let n = slow_gets.len();
        let switched = slow_gets.iter().filter(|s| s.2 > 0).count();
        let faulted = slow_gets.iter().filter(|s| s.1 > 0).count();
        let read = slow_gets.iter().filter(|s| s.0.reads > 0).count();
        println!(
            "slow gets (>= {:.2} us at the end, the running p99.5) {n} (dropped {dropped}): with a context switch {switched}, a page fault {faulted}, a read call {read}",
            floor as f64 / 1000.0
        );
    }
    costs("readrandom", reads.max(1), &mark);
    if reads > 0 {
        report("readrandom", &mut lat);
        println!(
            "readrandom {reads} {:.0} {:.3} found {found}",
            reads as f64 / s,
            s * 1e6 / reads as f64
        );
    }
    let ((c, w, r), (ci, ri)) = (db.memory_held(), db.index_bytes());
    let u = usage::this().ok();
    let mib = |b: Option<u64>| b.map_or(f64::NAN, |b| b as f64 / 1048576.0);
    println!(
        "memory held cache {:.1} MiB write {:.1} MiB records {:.1} MiB; indexes cache {:.1} MiB records {:.1} MiB; process footprint {:.1} MiB (peak {:.1})",
        c as f64 / 1048576.0,
        w as f64 / 1048576.0,
        r as f64 / 1048576.0,
        ci as f64 / 1048576.0,
        ri as f64 / 1048576.0,
        mib(u.as_ref().and_then(|u| u.footprint)),
        mib(u.as_ref().and_then(|u| u.peak_footprint)),
    );
    let (_, t2, io2) = db.stats();
    let plan = db.filter_plan();
    println!(
        "filters: {} branches, {:.2} bits a key in all, false positives a get now {:.5}, with rates by entries over visits {:.5} ({:.1}x fewer), or today's rate in {:.2} bits a key ({:.0}% of today's), over {} gets",
        plan.branches,
        plan.bits / num as f64,
        plan.now,
        plan.best,
        plan.now / plan.best.max(f64::MIN_POSITIVE),
        plan.bits_for_now / num as f64,
        100.0 * plan.bits_for_now / plan.bits.max(1.0),
        plan.gets
    );
    println!(
        "maplets built {} declined {} dropped {}",
        t2.maplets_built, t2.maplets_declined, t2.maplets_dropped
    );
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
        let ops = if attribute { mix } else { 0 };
        let (mut slow_puts, mut slow_seeks): (Recorder<Slow>, Recorder<Slow>) =
            (Recorder::new(ops), Recorder::new(ops));
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
                    && slow_puts.wants(ns)
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
                    && slow_seeks.wants(ns)
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
    let (cache_bytes, write_bytes, record_bytes) = db.memory_split();
    println!(
        "memory split cache {:.1} MiB write {:.1} MiB records {:.1} MiB",
        cache_bytes as f64 / (1 << 20) as f64,
        write_bytes as f64 / (1 << 20) as f64,
        record_bytes as f64 / (1 << 20) as f64
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

/// Diagnostic: which operations are recorded with what the engine did inside them. A fixed
/// threshold censored the tail it labelled once p99.9 fell below it, so the threshold is the
/// p99.5 of the operations so far, read from a log-linear histogram of every one (sixteen
/// buckets a power of two, so a bucket is within 1/16 of its values) every `REREAD` operations:
/// p99.9 and p99.99 lie above it, and their tails are recorded whole. The events go to a buffer
/// sized and touched before the run, a fiftieth of the operations (four times the half percent
/// the threshold passes, room for the threshold lagging a run's phases), and what does not fit
/// is counted, so coverage is reported rather than assumed.
struct Recorder<T> {
    buckets: Vec<u64>,
    seen: u64,
    floor: u64,
    events: Vec<T>,
    dropped: u64,
}

/// Operations between rereads of the recording threshold: frequent enough to follow a run's
/// phases, rare enough that the histogram's scan costs nothing per operation.
const REREAD: u64 = 4096;

impl<T: Default> Recorder<T> {
    fn new(ops: u64) -> Self {
        let cap = (ops / 50 + REREAD) as usize;
        let mut events = Vec::with_capacity(cap);
        // Touched now, so no recorded event takes a page fault.
        events.resize_with(cap, T::default);
        events.clear();
        Self {
            buckets: vec![0; 64 * 16],
            seen: 0,
            floor: 0,
            events,
            dropped: 0,
        }
    }

    fn bucket(ns: u64) -> usize {
        let e = 63 - (ns | 1).leading_zeros() as usize;
        let m = if e >= 4 {
            ((ns >> (e - 4)) & 15) as usize
        } else {
            0
        };
        e * 16 + m
    }

    fn lower(b: usize) -> u64 {
        let (e, m) = (b / 16, (b % 16) as u64);
        if e >= 4 { (16 + m) << (e - 4) } else { 1 << e }
    }

    /// Counts an operation of `ns`; whether it is to be recorded.
    fn wants(&mut self, ns: u64) -> bool {
        self.buckets[Self::bucket(ns)] += 1;
        self.seen += 1;
        if self.seen.is_multiple_of(REREAD) {
            let mut above = self.seen / 200;
            for (b, &n) in self.buckets.iter().enumerate().rev() {
                if n > above {
                    self.floor = Self::lower(b);
                    break;
                }
                above -= n;
            }
        }
        ns >= self.floor
    }

    fn push(&mut self, e: T) {
        if self.events.len() < self.events.capacity() {
            self.events.push(e);
        } else {
            self.dropped += 1;
        }
    }
}

type Stats = (
    mantle_engine::shard_db::FlushStats,
    mantle_engine::trunk::TrunkStats,
    mantle_engine::store::IoStats,
);

/// One slow put: its time, and the engine's work inside it.
#[derive(Default)]
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
    order_ns: u64,
    retire_ns: u64,
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
            order_ns: f1.order_ns - f0.order_ns,
            retire_ns: f1.retire_ns - f0.retire_ns,
            rotated: f1.rotations != f0.rotations,
        }
    }
}

/// The operations at or past p99.9 and p99.99, broken down: mean time in write calls and read
/// calls (inside pack or trunk work), in pack and trunk work besides their I/O, in stalls, and
/// the rest (the memtable, a rotation); and how many made a write call, a read call, or neither.
/// Each is reported for the whole tail and for a band about the percentile (a few slow
/// operations can set the tail's mean without setting the percentile), with the tail's
/// operations counted and those recorded, so a tail the recorder missed shows.
fn attribute_tail(lat: &[u64], rec: &mut Recorder<Slow>) {
    let slow = &mut rec.events;
    slow.sort_unstable_by_key(|s| std::cmp::Reverse(s.ns));
    for s in slow.iter().take(5) {
        println!(
            "worst {:.2} us: writes {} ({:.2} us) reads {} ({:.2} us) pack {:.2} trunk {:.2} (plan {:.2} finish {:.2} pack finish {:.2}) stall {:.2} insert {:.2} (of it rotate {:.2}, retire {:.2}) order {:.2} forget {:.2} | queueing pages {:.2} (waiting for a run {:.2}) reading ahead {:.2} rotated {}",
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
            s.retire_ns as f64 / 1000.0,
            s.order_ns as f64 / 1000.0,
            s.forget_ns as f64 / 1000.0,
            s.queue_ns as f64 / 1000.0,
            s.wait_ns as f64 / 1000.0,
            s.ahead_ns as f64 / 1000.0,
            s.rotated,
        );
    }
    let mut sorted = lat.to_vec();
    sorted.sort_unstable();
    let at = |q: f64| sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)];
    println!(
        "recorded {} operations at or past the running p99.5 (last {:.2} us), {} dropped past the buffer",
        slow.len(),
        rec.floor as f64 / 1000.0,
        rec.dropped
    );
    for (name, q) in [("p99.9", 0.999), ("p99.99", 0.9999)] {
        let floor = at(q);
        let expected = sorted.iter().filter(|&&ns| ns >= floor).count();
        let tail: Vec<&Slow> = slow.iter().filter(|s| s.ns >= floor).collect();
        // A band of half the tail's width either side of the percentile: p99.85 to p99.95 for p99.9.
        let band_lo = at(1.0 - (1.0 - q) * 1.5);
        let band_hi = at(1.0 - (1.0 - q) * 0.5);
        let band: Vec<&Slow> = slow
            .iter()
            .filter(|s| s.ns >= band_lo && s.ns <= band_hi)
            .collect();
        println!(
            "tail {name} >= {:.2} us: {expected} operations, {} recorded ({:.1}%)",
            floor as f64 / 1000.0,
            tail.len(),
            100.0 * tail.len() as f64 / expected.max(1) as f64
        );
        for (part, set) in [("whole tail", &tail), ("band", &band)] {
            breakdown(name, part, set, band_lo, band_hi);
        }
    }
}

/// The mean breakdown of `set`, the `part` of the `name` tail.
fn breakdown(name: &str, part: &str, set: &[&Slow], band_lo: u64, band_hi: u64) {
    let n = set.len().max(1) as f64;
    let mean = |f: &dyn Fn(&Slow) -> u64| set.iter().map(|s| f(s)).sum::<u64>() as f64 / n / 1000.0;
    let io = |s: &Slow| s.write_ns + s.read_ns;
    let work = |s: &Slow| s.pack_ns + s.trunk_ns;
    let range = if part == "band" {
        format!(
            " [{:.2}, {:.2}] us",
            band_lo as f64 / 1000.0,
            band_hi as f64 / 1000.0
        )
    } else {
        String::new()
    };
    println!(
        "tail {name} {part}{range} ({} ops): mean {:.2} us = writes {:.2} + reads {:.2} + work besides I/O {:.2} + stalls {:.2} + memtable order {:.2} + rest {:.2} (insert {:.2}, of it rotating {:.2} and retiring {:.2}); with a write call {}, a read call {}, neither {}, a rotation {}",
        set.len(),
        mean(&|s| s.ns),
        mean(&|s| s.write_ns),
        mean(&|s| s.read_ns),
        mean(&|s| work(s).saturating_sub(io(s)).saturating_sub(s.stall_ns)),
        mean(&|s| s.stall_ns),
        mean(&|s| s.order_ns),
        mean(&|s| s
            .ns
            .saturating_sub(work(s).max(io(s)))
            .saturating_sub(s.order_ns)),
        mean(&|s| s.insert_ns),
        mean(&|s| s.rotate_ns),
        mean(&|s| s.retire_ns),
        set.iter().filter(|s| s.writes > 0).count(),
        set.iter().filter(|s| s.reads > 0).count(),
        set.iter().filter(|s| s.writes == 0 && s.reads == 0).count(),
        set.iter().filter(|s| s.rotated).count(),
    );
    println!(
        "tail {name} {part} work: planning compactions {:.2} us, finishing them {:.2} us, finishing packed memtables {:.2} us, merging and packing the rest {:.2} us; queueing written pages {:.2} us (waiting for a run {:.2}), reading pages ahead {:.2} us",
        mean(&|s| s.plan_ns),
        mean(&|s| s.finish_ns),
        mean(&|s| s.pack_finish_ns),
        mean(&|s| (s.pack_ns + s.trunk_ns)
            .saturating_sub(s.plan_ns + s.finish_ns + s.pack_finish_ns + s.write_ns + s.read_ns)),
        mean(&|s| s.queue_ns),
        mean(&|s| s.wait_ns),
        mean(&|s| s.ahead_ns),
    );
}

/// db_bench's GetRandomKey: uniform at range 0, else `num · e^(−u·range)` for `u` uniform in
/// [0, 1), scattered by its prime 0x5bd1e995 modulo `num` (tools/db_bench_tool.cc).
fn skewed(r: u64, num: u64, range: f64) -> u64 {
    if range == 0.0 {
        return r % num;
    }
    const BIG: u64 = 1 << 62;
    let order = -((r % BIG) as f64) / BIG as f64 * range;
    let rank = (order.exp() * num as f64) as u64;
    rank.wrapping_mul(0x5bd1_e995) % num
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
