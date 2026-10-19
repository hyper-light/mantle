//! A matched Range API workload on hyper-rt: MT19937-64 fill/get/seek streams from
//! RocksDB 11.8.1, ASCII-zero 16-byte keys, repeated-v 100-byte values. One client,
//! range and shard by default; declared multi-thread runs give each client NUM operations.
//! `DIR [NUM] [THREADS] [READS] [CACHE_MIB] [ISSUER_DEPTH] [SEEKS] [SEEK_NEXTS]
//! [SPIN_NS] [WRITE_BUDGET_MIB] [--rocks-seed=301] [--runtime-record=PATH]`. Memtables total 64 MiB; cache and
//! write memory are divided equally across ranges. Buffered I/O, fanout 8, no WAL.
//!
//! This compares identical Range controls, not direct RocksDB timings: throughput/work
//! include exact membership/value/row checks. An operation's latency is the time since the one
//! before it ended, db_bench's measure (its `FinishedOps`: one clock read an operation), so it
//! holds the API call and the checks of the operation before. Postfill and
//! postquery flush+checkpoint barriers are timed and charged, including metadata/flushes.
//! Continuous process counts through
//! stop/shutdown include maintenance between phase snapshots and discard/cleanup work.
//! Cold setup and exact fresh reopen/reference verification are separately charged; reopen
//! occurs only after the query population and all native/physical retirement have ended.
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

use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[path = "support/rocks_workload.rs"]
mod rocks_workload;

#[path = "support/benchmark_recovery.rs"]
mod benchmark_recovery;

#[path = "support/present.rs"]
mod present;

use present::Present;

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_block::segmented::SegmentedFile;
use hyper_measure::{alloc, faults, usage};
use hyper_rt::machine::calibration::{Calibration, Policy};
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_disk::histogram::Histogram;
use mantle_engine::ranges::{RangeStats, Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::{Config, IntoFile};
use mantle_engine::trunk::TrunkConfig;

/// A range's store file: one file growing by its writes, or (`--segments`) segment files each
/// written whole before use (hyper-block's `SegmentedFile`, docs/research/48).
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "one file a range, made once: the size between the variants costs nothing"
)]
enum BenchFile {
    One(DeviceFile),
    Segments(SegmentedFile),
}

macro_rules! each {
    ($self:expr, $f:ident => $e:expr) => {
        match $self {
            BenchFile::One($f) => $e,
            BenchFile::Segments($f) => $e,
        }
    };
}

impl BlockFile for BenchFile {
    fn alignment(&self) -> Alignment {
        each!(self, f => f.alignment())
    }
    fn layout_block(&self) -> Alignment {
        each!(self, f => BlockFile::layout_block(f))
    }
    fn len(&self) -> Result<u64, DiskError> {
        each!(self, f => BlockFile::len(f))
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        each!(self, f => BlockFile::read_exact_at(f, buf, offset))
    }
    fn read_resident_at(&mut self, buf: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        each!(self, f => BlockFile::read_resident_at(f, buf, offset))
    }
    fn reads_resident(&self) -> bool {
        each!(self, f => BlockFile::reads_resident(f))
    }
    fn residency(&self) -> hyper_block::Residency {
        each!(self, f => BlockFile::residency(f))
    }
    fn set_residency_shadow(&mut self, on: bool) {
        each!(self, f => BlockFile::set_residency_shadow(f, on))
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        each!(self, f => BlockFile::write_all_at(f, buf, offset))
    }
    fn write_whole(&self, offset: u64, copies: u64, fill: &[u8]) -> Result<(), DiskError> {
        each!(self, f => BlockFile::write_whole(f, offset, copies, fill))
    }
    fn sync_data(&self) -> Result<(), DiskError> {
        each!(self, f => BlockFile::sync_data(f))
    }
    fn fills_new_space(&self) -> bool {
        each!(self, f => BlockFile::fills_new_space(f))
    }
    fn grows_by_segments(&self) -> bool {
        each!(self, f => BlockFile::grows_by_segments(f))
    }
    fn grow(&self, len: u64) -> Result<Option<u64>, DiskError> {
        each!(self, f => BlockFile::grow(f, len))
    }
    fn segment_end(&self, offset: u64) -> Result<Option<u64>, DiskError> {
        each!(self, f => BlockFile::segment_end(f, offset))
    }
    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        each!(self, f => BlockFile::write_durable_at(f, buf, offset))
    }
    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(match self {
            BenchFile::One(f) => BenchFile::One(BlockFile::try_clone(f)?),
            BenchFile::Segments(f) => BenchFile::Segments(BlockFile::try_clone(f)?),
        })
    }
}

/// The segments a range's file may have: the process's open-file limit over the handles each
/// segment takes, the shard's own, one for each of the issuer's workers and one for each
/// maintenance worker the host's cores allow.
fn segment_bound(issuer_depth: usize) -> usize {
    #[cfg(unix)]
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile)
        .current
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(usize::MAX);
    #[cfg(not(unix))]
    let limit = usize::MAX;
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    (limit / (1 + issuer_depth + cores)).max(2)
}

// The allocation count is a diagnostic: compiled in only with the `count-allocs` feature, so a
// performance run allocates through the system allocator with nothing counted.
#[cfg(feature = "count-allocs")]
#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// CLOCK_MONOTONIC in nanoseconds, for the phase bounds a profile is checked against: the
/// `profile-phases` diagnostic only.
#[cfg(all(feature = "profile-phases", unix))]
fn mono_ns() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    u64::try_from(t.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(t.tv_nsec).unwrap_or(0))
}

/// Where there is no CLOCK_MONOTONIC (Windows): the nanoseconds since this run's first phase
/// bound read its monotonic clock (`Instant`, QueryPerformanceCounter on Windows). The bounds
/// order the run's phases; they share no origin with an outside profiler's clock, which needs its
/// own offset mapping to be compared with them.
#[cfg(all(feature = "profile-phases", not(unix)))]
fn mono_ns() -> u64 {
    static FIRST: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    u64::try_from(FIRST.get_or_init(Instant::now).elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// The process's allocation counts with the `count-allocs` feature; without it, none: unmeasured.
fn allocs() -> Option<alloc::Counts> {
    #[cfg(feature = "count-allocs")]
    {
        Some(alloc::read_process())
    }
    #[cfg(not(feature = "count-allocs"))]
    {
        None
    }
}

/// The value every put writes: `--value-bytes=N` bytes of `v` (100 unless given), set once
/// before any workload runs. A 1B-key run takes a smaller value where 100 bytes a key would not
/// fit the disk (owner, 2026-10-11).
static VALUE: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

fn fill_value() -> &'static [u8] {
    VALUE.get().map_or(&[], Vec::as_slice)
}
type BenchError = Box<dyn std::error::Error + Send + Sync>;

fn require(holds: bool, what: &'static str) -> Result<(), BenchError> {
    if holds {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidInput, what).into())
    }
}

struct Mark {
    at: Instant,
    alloc: Option<alloc::Counts>,
    faults: faults::Faults,
    switches: Option<u64>,
    usage: Option<usage::Usage>,
    io: Option<StorageIo>,
}

impl Mark {
    fn now() -> Result<Self, BenchError> {
        Ok(Self {
            at: Instant::now(),
            alloc: allocs(),
            faults: faults::read()?,
            switches: faults::switches().ok(),
            usage: usage::this().ok(),
            io: StorageIo::read(),
        })
    }
}

/// The bytes the process has had the storage layer read and write for it, every thread's, page
/// cache hits not among them: Linux's `/proc/self/io` `read_bytes` and `write_bytes` (proc(5)),
/// so a phase's read and write amplification is measured, not the whole run's. Other systems say
/// none here.
#[derive(Clone, Copy)]
struct StorageIo {
    read: u64,
    write: u64,
}

impl StorageIo {
    #[cfg(target_os = "linux")]
    fn read() -> Option<Self> {
        let text = std::fs::read_to_string("/proc/self/io").ok()?;
        let field = |name: &str| {
            text.lines()
                .find_map(|l| l.strip_prefix(name))
                .and_then(|v| v.trim().parse::<u64>().ok())
        };
        Some(Self {
            read: field("read_bytes:")?,
            write: field("write_bytes:")?,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn read() -> Option<Self> {
        None
    }
}

fn costs(name: &str, ops: u64, from: &Mark, to: &Mark) {
    let a = to.alloc.zip(from.alloc).map(|(to, from)| to.less(&from));
    // An allocation count the build did not take is unavailable, never zero.
    let count = |n: Option<u64>, places: usize| {
        n.map_or_else(
            || "unavailable".to_string(),
            |n| format!("{:.places$}", n as f64 / ops as f64),
        )
    };
    let total = |n: Option<u64>| n.map_or_else(|| "unavailable".to_string(), |n| n.to_string());
    let f = to.faults.since(&from.faults);
    let process = to.usage.zip(from.usage).map(|(a, b)| a.since(&b));
    let per_op = |count: Option<u64>| {
        count.map_or_else(
            || "unavailable".to_string(),
            |count| format!("{:.3}", count as f64 / ops as f64),
        )
    };
    println!(
        "{name} elapsed {:.6}s allocs/op {} reallocs/op {} bytes/op {} faults/op {:.4} (allocs {} reallocs {} faults {})",
        to.at.duration_since(from.at).as_secs_f64(),
        count(a.map(|a| a.allocations), 3),
        count(a.map(|a| a.reallocations), 3),
        count(a.map(|a| a.bytes), 1),
        f.total() as f64 / ops as f64,
        total(a.map(|a| a.allocations)),
        total(a.map(|a| a.reallocations)),
        f.total(),
    );
    println!(
        "{name} process_cpu_ns/op {} user_ns/op {} system_ns/op {} instructions/op {} cycles/op {} switches/op {} scope whole-process-including-clients-shards-issuer-device-workers-and-oracle",
        per_op(process.map(|p| p.cpu_ns())),
        per_op(process.map(|p| p.user_ns)),
        per_op(process.map(|p| p.system_ns)),
        per_op(process.and_then(|p| p.instructions)),
        per_op(process.and_then(|p| p.cycles)),
        per_op(
            to.switches
                .zip(from.switches)
                .and_then(|(a, b)| a.checked_sub(b))
        ),
    );
    let io = to
        .io
        .zip(from.io)
        .and_then(|(a, b)| Some((a.read.checked_sub(b.read)?, a.write.checked_sub(b.write)?)));
    println!(
        "{name} storage read_bytes {} write_bytes {} read_bytes/op {} write_bytes/op {}",
        total(io.map(|(r, _)| r)),
        total(io.map(|(_, w)| w)),
        per_op(io.map(|(r, _)| r)),
        per_op(io.map(|(_, w)| w)),
    );
}

/// `--trickle`'s rounds: the puts made, each range's counters before and after, and the marks
/// around them.
struct Trickled {
    rounds: u64,
    puts: u64,
    before: Vec<RangeStats>,
    after: Vec<RangeStats>,
    start: Mark,
    done: Mark,
}

/// `--tombstones`: the keys drawn, the deletes of those the fill left absent and the gets after
/// each draw, their latencies, each range's counters before and after, and the marks around them.
struct Tombstoned {
    draws: u64,
    deletes: u64,
    found: u64,
    delete_lat: Histogram,
    get_lat: Histogram,
    before: Vec<RangeStats>,
    after: Vec<RangeStats>,
    start: Mark,
    done: Mark,
}

/// The settled queries of `--queries-again`: their gets and seeks and the marks around them.
struct Again {
    start: Mark,
    get: Option<Phase>,
    read: Mark,
    seek: Option<Phase>,
    done: Mark,
}

/// A timed query's own check of its answer, as splinter_bench.c and db_bench make theirs: a get
/// found is counted and its value's length checked; a seek's rows are counted, and it is found
/// when its first row is the key sought. Each answer is checked against the fill's oracle in full
/// after the last phase, untimed, by a replay of the same queries ([`Workload::checked`]), whose
/// counts every timed phase's must equal. Returns the found flag and the rows.
fn counted(
    work: Work,
    key: &[u8],
    hit: bool,
    value: &[u8],
    page: &Rows,
) -> Result<(bool, u64), BenchError> {
    match work {
        Work::Fill => Ok((hit, 0)),
        Work::Get => {
            if hit {
                require(
                    value.len() == fill_value().len(),
                    "get value length differs from the fill's",
                )?;
            }
            Ok((hit, 0))
        }
        Work::Seek(_) => Ok((
            page.get(0).is_some_and(|(first, _)| first == key),
            page.len() as u64,
        )),
    }
}

/// The replay of a query phase against the timed runs of it: each must have found as many keys
/// and scanned as many rows, the replay having checked every answer against the oracle. Prints
/// the replay's counts.
fn checked_against(
    verified: &Verified,
    first: &Option<Phase>,
    again: Option<&Phase>,
) -> Result<(), BenchError> {
    for phase in first.iter().chain(again) {
        require(
            phase.ops == verified.ops
                && phase.found == verified.found
                && phase.rows == verified.rows,
            "a timed phase's counts differ from the oracle-checked replay's",
        )?;
    }
    if let Some(phase) = first {
        println!(
            "{} verified untimed against the oracle: ops {} found {} rows {}",
            phase.name, verified.ops, verified.found, verified.rows
        );
    }
    Ok(())
}

/// What the untimed replay of a query phase found ([`Workload::verify_async`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Verified {
    ops: u64,
    found: u64,
    rows: u64,
}

struct Phase {
    name: &'static str,
    ops: u64,
    found: u64,
    rows: u64,
    lat: Histogram,
}

impl Phase {
    fn report(&mut self, from: &Mark, to: &Mark) {
        let seconds = to.at.duration_since(from.at).as_secs_f64();
        // A log-linear histogram's bucket bounds: never below the latency, at most 2^-5 above it.
        let at = |ppm: u32| self.lat.quantile(ppm) as f64 / 1000.0;
        println!(
            "{} {} {:.0} {:.3}",
            self.name,
            self.ops,
            self.ops as f64 / seconds,
            seconds * 1e6 / self.ops as f64
        );
        println!(
            "{} found {} rows {} latency_us p50 {:.2} p99 {:.2} p99.9 {:.2} p99.99 {:.2} max {:.2}",
            self.name,
            self.found,
            self.rows,
            at(500_000),
            at(990_000),
            at(999_000),
            at(999_900),
            self.lat.max() as f64 / 1000.0
        );
        costs(self.name, self.ops, from, to);
    }
}

#[derive(Clone, Copy)]
enum Work {
    Fill,
    Get,
    Seek(usize),
}

struct Workload {
    num: u64,
    threads: u64,
    seed: u64,
    present: Present,
}

impl Workload {
    /// `--trickle`'s round `round`: `n` draws over the key space from a stream of the round's
    /// own, each key the fill left present put again with the fill's value, so the oracle's keys
    /// and values stay as the fill left them; the puts made.
    async fn trickle_async(&self, ranges: &Ranges, n: u64, round: u64) -> Result<u64, BenchError> {
        let mut client = ranges.client()?;
        let mut rng = self.rng(4 + round, 0);
        let mut puts = 0u64;
        for _ in 0..n {
            let k = rng.next() % self.num;
            if self.present.holds(k) {
                client
                    .put_async(&rocks_workload::key(k), fill_value())
                    .await?;
                puts += 1;
            }
        }
        Ok(puts)
    }

    /// `--tombstones`: `n` draws over the key space from the stream `phase`, each key the fill left
    /// absent deleted, a delete's entry a sixth of a put's, so a memtable takes more keys than its
    /// index was made for; after each draw a get of a key from the queries' stream, checked
    /// against the fill's oracle, which the deletes leave as it was. Each operation's latency is
    /// the time since the one before it ended, as the phases measure it.
    async fn tombstones_async(
        &self,
        ranges: &Ranges,
        n: u64,
        phase: u64,
    ) -> Result<(u64, u64, Histogram, Histogram), BenchError> {
        let mut client = ranges.client()?;
        let mut draws = self.rng(phase, 0);
        let mut gets = self.rng(2, 0);
        let mut delete_lat = Histogram::new();
        let mut get_lat = Histogram::new();
        let mut value = Vec::new();
        let mut deletes = 0u64;
        let mut found = 0u64;
        let mut last = Instant::now();
        for _ in 0..n {
            let k = draws.next() % self.num;
            if !self.present.holds(k) {
                client.delete_async(&rocks_workload::key(k)).await?;
                let now = Instant::now();
                delete_lat.record(now.duration_since(last).as_nanos() as u64);
                last = now;
                deletes += 1;
            }
            let g = gets.next() % self.num;
            let hit = client
                .get_async(&rocks_workload::key(g), &mut value)
                .await?;
            let now = Instant::now();
            get_lat.record(now.duration_since(last).as_nanos() as u64);
            last = now;
            require(
                hit == self.present.holds(g),
                "found flag differs from the fill oracle",
            )?;
            if hit {
                require(
                    value.as_slice() == fill_value(),
                    "get value differs from the fill oracle",
                )?;
            }
            found += u64::from(hit);
        }
        Ok((deletes, found, delete_lat, get_lat))
    }

    /// A query's answer checked against the fill's oracle in full: its found flag, a get's value,
    /// and a seek's count of rows and every row's key and value. Returns the found flag and the
    /// rows.
    fn checked(
        &self,
        work: Work,
        start: u64,
        hit: bool,
        value: &[u8],
        page: &Rows,
        expected: &mut Vec<u64>,
    ) -> Result<(bool, u64), BenchError> {
        match work {
            Work::Fill => Ok((hit, 0)),
            Work::Get => {
                require(
                    hit == self.present.holds(start),
                    "found flag differs from the fill oracle",
                )?;
                if hit {
                    require(
                        value == fill_value(),
                        "get value differs from the fill oracle",
                    )?;
                }
                Ok((hit, 0))
            }
            Work::Seek(limit) => {
                self.present.after(start, limit, expected);
                require(
                    page.len() == expected.len(),
                    "scan row count differs from the fill oracle",
                )?;
                for ((key, value), &number) in page.iter().zip(expected.iter()) {
                    require(
                        key == rocks_workload::key(number),
                        "scan key differs from the fill oracle",
                    )?;
                    require(
                        value == fill_value(),
                        "scan value differs from the fill oracle",
                    )?;
                }
                let key = rocks_workload::key(start);
                let hit = page.get(0).is_some_and(|(first, _)| first == key);
                require(
                    hit == self.present.holds(start),
                    "found flag differs from the fill oracle",
                )?;
                Ok((hit, page.len() as u64))
            }
        }
    }

    /// The `n` queries of `work` each client thread makes, replayed untimed in the order its
    /// timed phase made them (the same streams), every answer checked against the fill's oracle in
    /// full ([`Self::checked`]).
    fn verify(&self, ranges: &Ranges, n: u64, work: Work) -> Result<Verified, BenchError> {
        let phase = match work {
            Work::Fill => {
                return Ok(Verified {
                    ops: 0,
                    found: 0,
                    rows: 0,
                });
            }
            Work::Get => 2,
            Work::Seek(_) => 3,
        };
        let mut total = Verified {
            ops: 0,
            found: 0,
            rows: 0,
        };
        for thread in 0..self.threads {
            let mut client = ranges.client()?;
            let mut rng = self.rng(phase, thread);
            let (mut value, mut page, mut next, mut expected) =
                (Vec::new(), Rows::new(), Vec::new(), Vec::new());
            for _ in 0..n {
                let start = rng.next() % self.num;
                let key = rocks_workload::key(start);
                page.clear();
                next.clear();
                let hit = match work {
                    Work::Fill => true,
                    Work::Get => client.get(&key, &mut value)?,
                    Work::Seek(limit) => {
                        client.scan(&key, None, limit, &mut page, &mut next)?;
                        false
                    }
                };
                let (hit, rows) = self.checked(work, start, hit, &value, &page, &mut expected)?;
                total.ops += 1;
                total.found += u64::from(hit);
                total.rows += rows;
            }
        }
        Ok(total)
    }

    /// [`Self::verify`] with the client task on the range's shard, as [`Self::phase_async`].
    async fn verify_async(
        &self,
        ranges: &Ranges,
        n: u64,
        work: Work,
    ) -> Result<Verified, BenchError> {
        let phase = match work {
            Work::Fill => {
                return Ok(Verified {
                    ops: 0,
                    found: 0,
                    rows: 0,
                });
            }
            Work::Get => 2,
            Work::Seek(_) => 3,
        };
        let mut client = ranges.client()?;
        let mut rng = self.rng(phase, 0);
        let (mut value, mut page, mut next, mut expected) =
            (Vec::new(), Rows::new(), Vec::new(), Vec::new());
        let mut total = Verified {
            ops: 0,
            found: 0,
            rows: 0,
        };
        for _ in 0..n {
            let start = rng.next() % self.num;
            let key = rocks_workload::key(start);
            page.clear();
            next.clear();
            let hit = match work {
                Work::Fill => true,
                Work::Get => client.get_async(&key, &mut value).await?,
                Work::Seek(limit) => {
                    client
                        .scan_async(&key, None, limit, &mut page, &mut next)
                        .await?;
                    false
                }
            };
            let (hit, rows) = self.checked(work, start, hit, &value, &page, &mut expected)?;
            total.ops += 1;
            total.found += u64::from(hit);
            total.rows += rows;
        }
        Ok(total)
    }

    fn rng(&self, phase: u64, thread: u64) -> rocks_workload::Rng {
        // db_bench_tool.cc RunBenchmark increments total_thread_count for every thread in
        // every phase; ThreadState seeds Random64 with seed + that count. Keep the helper's
        // phase selector (seek has no database-selection draw) separate from this offset.
        let offset = (phase - 1) * (self.threads - 1) + thread;
        rocks_workload::Rng::new(self.seed + offset, phase)
    }

    fn phase(&self, ranges: &Ranges, n: u64, work: Work) -> Result<Phase, BenchError> {
        let (name, phase) = match work {
            Work::Fill => ("fillrandom", 1),
            Work::Get => ("readrandom", 2),
            Work::Seek(_) => ("seekrandom", 3),
        };
        let (lat, found, rows) = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..self.threads)
                .map(|thread| {
                    scope.spawn(move || {
                        let mut client = ranges.client()?;
                        let mut rng = self.rng(phase, thread);
                        let mut lat = Histogram::new();
                        let mut last = Instant::now();
                        let mut value = Vec::new();
                        let mut page = Rows::new();
                        let mut next = Vec::new();
                        let mut found = 0;
                        let mut rows = 0;
                        for _ in 0..n {
                            let start = rng.next() % self.num;
                            let key = rocks_workload::key(start);
                            page.clear();
                            next.clear();
                            let hit = match work {
                                Work::Fill => {
                                    client.put(&key, fill_value())?;
                                    true
                                }
                                Work::Get => client.get(&key, &mut value)?,
                                Work::Seek(limit) => {
                                    client.scan(&key, None, limit, &mut page, &mut next)?;
                                    false
                                }
                            };
                            let now = Instant::now();
                            lat.record(now.duration_since(last).as_nanos() as u64);
                            last = now;
                            let (hit, scanned) = counted(work, &key, hit, &value, &page)?;
                            rows += scanned;
                            found += u64::from(hit);
                        }
                        Ok::<_, BenchError>((lat, found, rows))
                    })
                })
                .collect();
            let mut all = Histogram::new();
            let mut found = 0;
            let mut rows = 0;
            for handle in handles {
                let (lat, hits, scanned) = handle
                    .join()
                    .map_err(|_| io::Error::other("a benchmark client thread panicked"))??;
                all.merge(&lat);
                found += hits;
                rows += scanned;
            }
            Ok::<_, BenchError>((all, found, rows))
        })?;
        Ok(Phase {
            name,
            ops: self.threads * n,
            found,
            rows,
            lat,
        })
    }
    /// The same oracle and reusable buffers, with the client task on the range's shard.
    async fn phase_async(&self, ranges: &Ranges, n: u64, work: Work) -> Result<Phase, BenchError> {
        let (name, phase) = match work {
            Work::Fill => ("fillrandom", 1),
            Work::Get => ("readrandom", 2),
            Work::Seek(_) => ("seekrandom", 3),
        };
        // Its start and end as they happen, so a profiler can sample this phase alone.
        println!("phase {name} start");
        // With the `profile-phases` diagnostic: the phase's start on CLOCK_MONOTONIC, the clock
        // `perf record -k mono` stamps samples on, so a profile is checked against its phases.
        #[cfg(feature = "profile-phases")]
        println!("phasemono {name} start {}", mono_ns());
        let thread = 0;
        let mut client = ranges.client()?;
        let mut rng = self.rng(phase, thread);
        let mut lat = Histogram::new();
        let mut last = Instant::now();
        let mut value = Vec::new();
        let mut page = Rows::new();
        let mut next = Vec::new();
        let mut found = 0;
        let mut rows = 0;
        for _ in 0..n {
            let start = rng.next() % self.num;
            let key = rocks_workload::key(start);
            page.clear();
            next.clear();
            let hit = match work {
                Work::Fill => {
                    client.put_async(&key, fill_value()).await?;
                    true
                }
                Work::Get => client.get_async(&key, &mut value).await?,
                Work::Seek(limit) => {
                    client
                        .scan_async(&key, None, limit, &mut page, &mut next)
                        .await?;
                    false
                }
            };
            let now = Instant::now();
            lat.record(now.duration_since(last).as_nanos() as u64);
            last = now;
            let (hit, scanned) = counted(work, &key, hit, &value, &page)?;
            rows += scanned;
            found += u64::from(hit);
        }
        #[cfg(feature = "profile-phases")]
        println!("phasemono {name} end {}", mono_ns());
        println!("phase {name} end");
        println!("{name} inline {} of {n}", client.inlined());
        // Match the threaded driver's joined histogram aggregation.
        let mut all = Histogram::new();
        all.merge(&lat);
        Ok(Phase {
            name,
            ops: n,
            found,
            rows,
            lat: all,
        })
    }
}

fn stats(name: &str, counters: &[RangeStats]) {
    for (range, (f, _, _)) in counters.iter().enumerate() {
        println!(
            "{name}_shard range {range} rotations {} fed {} frozen_most {} order_moves {} idle_slices {} idle_units {} record_hits {} record_misses {} record_ghost_hits {} record_held {} record_bytes {} kept {} kept_bytes {} kept_hits {}",
            f.rotations,
            f.fed,
            f.frozen_most,
            f.order_moves,
            f.idle_slices,
            f.idle_units,
            f.record_hits,
            f.record_misses,
            f.record_ghost_hits,
            f.record_held,
            f.record_bytes,
            f.kept,
            f.kept_bytes,
            f.kept_hits
        );
    }
    for (range, (f, t, io)) in counters.iter().enumerate() {
        println!(
            "{name} range {range} flushes {} stalls {} waited_steps {} pivot_compactions {} leaf_compactions {} splits {} entries_written {} consolidations {} merge_units {} merge_ns {} io reads {} pages_read {} submitted {} pages_written {} syncs {} queued {} queued_most {} write_waits {} prefetches {} prefetch_waits {} cache_hits {} span_cache_hits {} device_reads {} resident_reads {} device_read_ns {} resident_probes {} resident_skips {} residency_epochs {} residency_changes {} residency_wrong {} scans {} scan_segments {} scan_sources {} scan_skipped {} scan_opened {} pivot_entries {} settle_entries_seeks {} settle_entries_branches {} settle_entries_size {} settle_inflight_entries {} whole_consolidations {} charged_leaf_sources {} charged_index_sources {} paid_leaves {} paid_index {} paid_below {} consolidations_started {} fullest_flushes {} parted_entries {}",
            f.flushes,
            f.stalls,
            f.waited_steps,
            t.pivot_compactions,
            t.leaf_compactions,
            t.splits,
            t.entries_written,
            t.consolidations,
            t.merge_units,
            t.merge_ns,
            io.reads,
            io.pages_read,
            io.submitted,
            io.pages_written,
            io.syncs,
            io.runs_queued,
            io.runs_queued_most,
            io.write_waits,
            io.prefetches,
            io.prefetch_waits,
            io.cache_hits,
            io.span_cache_hits,
            io.device_reads,
            io.resident_reads,
            io.device_read_ns,
            io.residency.probes,
            io.residency.skips,
            io.residency.epochs,
            io.residency.changes,
            io.residency.wrong,
            t.scans,
            t.scan_segments,
            t.scan_sources,
            t.scan_skipped,
            t.scan_opened,
            t.pivot_entries,
            t.settle_entries_seeks,
            t.settle_entries_branches,
            t.settle_entries_size,
            t.settle_inflight_entries,
            t.whole_consolidations,
            t.charged_leaf_sources,
            t.charged_index_sources,
            t.paid_leaves,
            t.paid_index,
            t.paid_below,
            t.consolidations_started,
            t.fullest_flushes,
            t.parted_entries
        );
        let mut shape = String::new();
        for (depth, l) in t.shape.iter().enumerate().filter(|(_, l)| l.nodes > 0) {
            shape.push_str(&format!(
                " | depth {depth} leaf {} nodes {} pivots {} branches {} most {} inflight {} entries {}",
                l.leaf, l.nodes, l.pivots, l.branches, l.most, l.inflight, l.entries
            ));
        }
        println!("{name}_shape range {range}{shape}");
        let m = io.cache_memory;
        println!(
            "{name}_waits range {range} demand_waits {} demand_wait_ns {} cache_bytes {} cache_limit_pages {} cache_refused {} cache_live {} cache_free {} cache_dead {} cache_spare {} cache_lent {} cache_slots {} loans {} loan_room {} loan_kept {} loan_missed {}",
            io.demand_waits,
            io.demand_wait_ns,
            io.cache_bytes,
            io.cache_limit_pages,
            io.cache_refused,
            m.live,
            m.free,
            m.dead,
            m.spare,
            m.lent,
            m.slots,
            m.loans,
            m.loan_room,
            m.loan_kept,
            m.loan_missed
        );
    }
}

/// The frozen record holds the ten measured numeric RuntimeConfig fields below;
/// pinning is off and cores empty in both modes. Frozen runs disable adaptive wake tracking.
fn runtime_config(
    threads: u16,
    spin: Option<u64>,
    record: Option<&PathBuf>,
) -> Result<RuntimeConfig, BenchError> {
    if let Some(path) = record.filter(|path| path.exists()) {
        let text = std::fs::read_to_string(path)?;
        let numbers = text
            .split_whitespace()
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()?;
        let [
            shards,
            tasks,
            timers,
            interests,
            rings,
            step,
            tick,
            batch,
            page,
            saved_spin,
        ] = numbers.as_slice()
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime record needs ten numeric fields",
            )
            .into());
        };
        require(
            *shards == u64::from(threads),
            "runtime record thread count differs",
        )?;
        require(
            [tasks, timers, interests, rings, step, tick, batch, page]
                .iter()
                .all(|&&n| n > 0),
            "runtime record has a zero bound",
        )?;
        require(
            spin.is_none_or(|spin| spin == *saved_spin),
            "spin override differs from runtime record",
        )?;
        return Ok(RuntimeConfig {
            shards: threads,
            tasks_per_shard: usize::try_from(*tasks)?,
            timers_per_shard: usize::try_from(*timers)?,
            interests_per_shard: usize::try_from(*interests)?,
            ring_entries: usize::try_from(*rings)?,
            step_budget_ns: *step,
            timer_tick_ns: *tick,
            batch: usize::try_from(*batch)?,
            pin: false,
            cores: Vec::new(),
            page_bytes: usize::try_from(*page)?,
            spin_ns: *saved_spin,
            wake_tracking: None,
            latency_objective_ns: None,
        });
    }
    let calibration = Calibration::measure(Duration::from_millis(40), threads)?;
    let constants = calibration
        .constants(&Policy {
            reserved_cores: threads,
            lateness_tolerance_ns: None,
            latency_objective_ns: None,
        })
        .map_err(|unmet| io::Error::other(format!("runtime calibration unmet: {unmet:?}")))?;
    let mut config = RuntimeConfig::from_calibration(&calibration, &constants, 1, 1);
    config.shards = threads;
    config.pin = false;
    config.cores.clear();
    if let Some(spin) = spin {
        config.spin_ns = spin;
        config.wake_tracking = None;
    }
    if let Some(path) = record {
        config.wake_tracking = None;
        std::fs::write(
            path,
            format!(
                "{} {} {} {} {} {} {} {} {} {}\n",
                config.shards,
                config.tasks_per_shard,
                config.timers_per_shard,
                config.interests_per_shard,
                config.ring_entries,
                config.step_budget_ns,
                config.timer_tick_ns,
                config.batch,
                config.page_bytes,
                config.spin_ns
            ),
        )?;
    }
    Ok(config)
}

fn main() -> Result<(), BenchError> {
    // Freed large blocks go back to the OS, as the node's process has them (docs/research/64 §6).
    mantle_engine::port::mmap::return_freed_blocks();
    let mut shard_client = false;
    // `--no-inline`: a client on its range's shard sends every operation through the range's
    // channel, as one from another shard does (`RangesConfig::inline`), for an A/B of the two.
    let mut no_inline = false;
    let mut seed = 301u64;
    let mut runtime_record = None;
    let mut value_bytes = 100usize;
    // `--shadow-probe`: every resident read made on a kept answer probes anyway, and the answers
    // the probe finds wrong are counted (docs/research/46-reclaim-epochs.md): for measurement.
    let mut shadow_probe = false;
    // `--segments`: each range's store kept as segment files, every segment written whole before
    // any of it is used (docs/research/48), for an A/B against one file growing by its writes.
    let mut segments = false;
    // `--consolidation=never|measured|always`: when seeks' rent buys a pivot's consolidation
    // (`Trunk::set_consolidation`; measured by default), for arms that isolate what it costs.
    let mut consolidation = mantle_engine::trunk::Consolidation::Measured;
    // `--machine-memory`: each range sized to its share of the host's memory
    // (`ShardDb::follow_machine`), its cache, write memory and records divided by measured
    // gains, in place of the fixed cache and write budget given on the command line.
    let mut machine_memory = false;
    // `--direct`: each range's files opened for direct I/O where the file system takes it
    // (hyper-block `CachingRequest::PreferDirect`), past the OS page cache, for an A/B against
    // buffered I/O.
    let mut caching = CachingRequest::Buffered;
    // `--queries-again`: once the queries' flush has left the trunk idle (every consolidation the
    // seeks bought done), the gets and seeks run again and report as `readrandom_settled` and
    // `seekrandom_settled`: the queries' rate on the layout they paid for, apart from the
    // consolidation the first seeks run beside (docs/research/50 §1, §8).
    let mut queries_again = false;
    // `--trickle=ROUNDS:PUTS`: after the queries, ROUNDS rounds of PUTS draws over the key space,
    // each key the fill left present put again with the fill's value, every round flushed and its
    // maintenance settled: a store written in bursts with idle time between, whose maintenance
    // `pretrickle` and `posttrickle` and the `trickle` costs count apart from the queries
    // (docs/research/53). The client on its range's shard only.
    let mut trickle: Option<(u64, u64)> = None;
    // `--tombstones=N`: after the queries, N draws over the key space, each key the fill left
    // absent deleted and a get after each draw (`Workload::tombstones_async`): a memtable taking
    // more keys than its index was made for, read as it fills (docs/research/54 §5). The client
    // on its range's shard only.
    let mut tombstones: Option<u64> = None;
    let mut args = Vec::new();
    for arg in std::env::args().skip(1) {
        if arg == "--shard-client" {
            shard_client = true;
        } else if arg == "--no-inline" {
            no_inline = true;
        } else if arg == "--shadow-probe" {
            shadow_probe = true;
        } else if arg == "--segments" {
            segments = true;
        } else if arg == "--machine-memory" {
            machine_memory = true;
        } else if arg == "--direct" {
            caching = CachingRequest::PreferDirect;
        } else if arg == "--queries-again" {
            queries_again = true;
        } else if let Some(value) = arg.strip_prefix("--tombstones=") {
            tombstones = Some(value.parse()?);
        } else if let Some(value) = arg.strip_prefix("--trickle=") {
            let (rounds, puts) = value
                .split_once(':')
                .ok_or_else(|| io::Error::other("--trickle= takes ROUNDS:PUTS"))?;
            trickle = Some((rounds.parse()?, puts.parse()?));
        } else if let Some(value) = arg.strip_prefix("--consolidation=") {
            consolidation = match value {
                "never" => mantle_engine::trunk::Consolidation::Never,
                "always" => mantle_engine::trunk::Consolidation::Always,
                "measured" => mantle_engine::trunk::Consolidation::Measured,
                _ => {
                    return Err(io::Error::other(
                        "--consolidation= takes never, measured or always",
                    )
                    .into());
                }
            };
        } else if let Some(value) = arg.strip_prefix("--rocks-seed=") {
            seed = value.parse()?;
        } else if let Some(path) = arg.strip_prefix("--runtime-record=") {
            runtime_record = Some(PathBuf::from(path));
        } else if let Some(n) = arg.strip_prefix("--value-bytes=") {
            value_bytes = n.parse()?;
        } else if arg != "--bench" {
            args.push(arg);
        }
    }
    if tombstones.is_some() && !shard_client {
        return Err(io::Error::other(
            "--tombstones= runs the client on its range's shard: --shard-client",
        )
        .into());
    }
    if trickle.is_some() && !shard_client {
        return Err(io::Error::other(
            "--trickle= runs the client on its range's shard: --shard-client",
        )
        .into());
    }
    VALUE
        .set(vec![b'v'; value_bytes])
        .map_err(|_| io::Error::other("the value was set twice"))?;
    let dir = PathBuf::from(
        args.first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "DIR required"))?,
    );
    let num: u64 = args
        .get(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(1_000_000);
    let threads: u64 = args.get(2).map(|s| s.parse()).transpose()?.unwrap_or(1);
    let reads: u64 = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(num);
    let cache_mib: usize = args.get(4).map(|s| s.parse()).transpose()?.unwrap_or(0);
    let issuer_depth: usize = args.get(5).map(|s| s.parse()).transpose()?.unwrap_or(0);
    let seeks: u64 = args.get(6).map(|s| s.parse()).transpose()?.unwrap_or(0);
    let seek_nexts: usize = args.get(7).map(|s| s.parse()).transpose()?.unwrap_or(10);
    let spin_override: Option<u64> = args.get(8).map(|s| s.parse()).transpose()?;
    let write_budget_mib: usize = args.get(9).map(|s| s.parse()).transpose()?.unwrap_or(64);
    require(
        issuer_depth > 0,
        "positive issuer depth required for runtime range I/O",
    )?;
    require(
        num > 0 && threads > 0 && threads <= num && seek_nexts > 0,
        "positive num/threads/seek_nexts and threads <= num required",
    )?;
    let thread_count = u16::try_from(threads)?;
    let overflow = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "benchmark workload overflows its count or budget",
        )
    };
    let total = threads
        .checked_mul(
            num.checked_add(reads)
                .and_then(|n| n.checked_add(seeks))
                .ok_or_else(overflow)?,
        )
        .ok_or_else(overflow)?;
    usize::try_from(total)?;
    // Each seek returns at most its limit and the whole keyspace; this bounds both the
    // per-client and combined row counters without arithmetic on the measured path.
    let rows_per_seek = num.min(u64::try_from(seek_nexts)?);
    threads
        .checked_mul(seeks)
        .and_then(|seeks| seeks.checked_mul(rows_per_seek))
        .ok_or_else(overflow)?;
    seed.checked_add(threads.checked_mul(3).ok_or_else(overflow)?)
        .ok_or_else(overflow)?;
    let cache_bytes = cache_mib.checked_mul(1 << 20).ok_or_else(overflow)?;
    let write_bytes = write_budget_mib.checked_mul(1 << 20).ok_or_else(overflow)?;
    let all_batches = mantle_engine::shard_db::issuer_batches(issuer_depth, issuer_depth)
        .checked_mul(usize::try_from(threads)?)
        .ok_or_else(overflow)?;
    #[cfg(feature = "count-allocs")]
    alloc::begin_process();
    let before_setup = Mark::now()?;
    std::fs::create_dir_all(&dir)?;
    let mut workload = Workload {
        num,
        threads,
        seed,
        present: Present::new(num)?,
    };
    // Exact oracle and its storage are setup work, outside measured operations.
    for thread in 0..threads {
        let mut rng = workload.rng(1, thread);
        for _ in 0..num {
            workload.present.insert(rng.next() % num);
        }
    }
    require(
        threads == 1,
        "matched shard-client harness requires one client/range/shard",
    )?;
    let mut rt = runtime_config(thread_count, spin_override, runtime_record.as_ref())?;
    // Both controls admit the same tasks: the existing range and its caller.
    rt.tasks_per_shard = usize::try_from(threads)?
        .checked_add(usize::try_from(threads)?)
        .ok_or_else(overflow)?;
    println!(
        "client_mode {} tasks_per_shard {}",
        if shard_client {
            "same-shard-async"
        } else {
            "thread-sync"
        },
        rt.tasks_per_shard
    );
    println!(
        "workload generator rocksdb-mt19937_64 seed {seed} key_padding ascii-0 values repeated-v value_bytes {} num {num} client_threads {threads} ranges {threads} shards {threads} reads_per_thread {reads} seeks_per_thread {seeks} seek_nexts {seek_nexts} issuer_depth {issuer_depth} runs_in_flight_per_range {issuer_depth} cache_mib {cache_mib} write_budget_mib {write_budget_mib} oracle_keys {} throughput counted-as-splinter_bench oracle-replay-untimed latency since-previous-completion barriers flush+durable-checkpoint",
        fill_value().len(),
        workload.present.len()
    );
    // The diagnostics compiled into this build: a performance run has none.
    let state = |on: bool| if on { "on" } else { "off" };
    println!(
        "harness diagnostics count_allocs {} profile_phases {}",
        state(cfg!(feature = "count-allocs")),
        state(cfg!(feature = "profile-phases"))
    );
    println!(
        "runtime shards {} step_budget_ns {} spin_ns {} batch {}",
        rt.shards, rt.step_budget_ns, rt.spin_ns, rt.batch
    );
    println!(
        "runtime record {:?} adaptive_wake_tracking {} config {:?}",
        runtime_record,
        rt.wake_tracking.is_some(),
        rt
    );
    let mut runtime = Runtime::start(&rt)?;
    let align = Alignment::new(4096)?;
    let config = Config {
        page_size: 4096,
        extent_pages: 32,
        max_extents: 1 << 24,
    };
    let mem = (64usize << 20) / threads as usize;
    let leaf_entries = (mem / (16 + 100 + 3)) as u64;
    let issuer = Issuer::start_for(&dir, issuer_depth, all_batches)?;
    println!(
        "workers client_roles {threads} owner_roles {threads} client_placement {} issuer {} device {}",
        if shard_client {
            "same-shard"
        } else {
            "external-thread"
        },
        1,
        issuer.depth()
    );
    let mut engines = Vec::new();
    let mut paths = Vec::new();
    for i in 0..threads {
        let path = dir.join(format!("range-{i}.store"));
        require(!path.exists(), "benchmark requires a fresh directory")?;
        let bound = segment_bound(issuer_depth);
        let file = if segments {
            BenchFile::Segments(SegmentedFile::create(&path, caching, align, bound)?)
        } else {
            BenchFile::One(DeviceFile::open(&path, true, caching, align)?)
        };
        let mut db = ShardDb::create(
            file,
            config,
            mem,
            TrunkConfig {
                fanout: 8,
                leaf_entries,
            },
        )?;
        if machine_memory {
            db.follow_machine(threads as usize)?;
        } else {
            db.set_cache(cache_bytes / 4096 / threads as usize);
            db.set_write_budget(write_bytes / threads as usize)?;
        }
        db.attach(&issuer, issuer_depth)?;
        db.set_residency_shadow(shadow_probe);
        db.set_consolidation(consolidation);
        let worker_path = path.clone();
        db.set_workers(move || {
            let file = if segments {
                SegmentedFile::open(&worker_path, caching, align, bound).map(BenchFile::Segments)
            } else {
                DeviceFile::open(&worker_path, false, caching, align).map(BenchFile::One)
            };
            file.map_err(|error| mantle_engine::Error::Io {
                op: "open a range benchmark worker",
                detail: error.to_string(),
            })
        })?;
        let start = if i == 0 {
            Vec::new()
        } else {
            rocks_workload::key(i * num / threads).to_vec()
        };
        engines.push((start, db));
        paths.push(path);
    }
    let ranges = Ranges::start(
        &mut runtime,
        engines,
        RangesConfig {
            clients: threads as usize + 1,
            slice_ns: rt.step_budget_ns,
            spin_ns: rt.spin_ns,
            inline: !no_inline,
        },
    )
    .map_err(|failure| match failure {
        mantle_engine::ranges::StartError::Failed(error) => error,
        mantle_engine::ranges::StartError::Refused { ranges, error } => {
            // This benchmark creates engines on its plain cold caller.
            drop(ranges);
            error
        }
    })?;
    let start = Mark::now()?;
    let (
        mut fill,
        filled,
        fill_stats,
        postfill,
        mut get,
        read,
        read_stats,
        mut seek,
        queried,
        query_stats,
        postquery,
        again,
        mut tombstoned,
        trickled,
        workload,
    ) = if shard_client {
        let (done, result) = std::sync::mpsc::sync_channel(1);
        let shard = runtime
            .shard_ids()
            .first()
            .copied()
            .ok_or_else(|| io::Error::other("a runtime with no shards"))?;
        runtime.spawn_on(shard, async move {
            let measured = async {
                let mut client = ranges.client()?;
                let fill = workload.phase_async(&ranges, num, Work::Fill).await?;
                let filled = Mark::now()?;
                client.flush_async().await?;
                client.checkpoint_async(threads * num).await?;
                let fill_stats = client.stats_async().await?;
                let postfill = Mark::now()?;
                let get = if reads > 0 {
                    Some(workload.phase_async(&ranges, reads, Work::Get).await?)
                } else {
                    None
                };
                let read = Mark::now()?;
                let read_stats = client.stats_async().await?;
                let seek = if seeks > 0 {
                    Some(
                        workload
                            .phase_async(&ranges, seeks, Work::Seek(seek_nexts))
                            .await?,
                    )
                } else {
                    None
                };
                let queried = Mark::now()?;
                client.flush_async().await?;
                client.checkpoint_async(threads * num).await?;
                let query_stats = client.stats_async().await?;
                let postquery = Mark::now()?;
                let again = if queries_again {
                    let start = Mark::now()?;
                    let get = if reads > 0 {
                        Some(workload.phase_async(&ranges, reads, Work::Get).await?)
                    } else {
                        None
                    };
                    let read = Mark::now()?;
                    let seek = if seeks > 0 {
                        Some(
                            workload
                                .phase_async(&ranges, seeks, Work::Seek(seek_nexts))
                                .await?,
                        )
                    } else {
                        None
                    };
                    let done = Mark::now()?;
                    Some(Again {
                        start,
                        get,
                        read,
                        seek,
                        done,
                    })
                } else {
                    None
                };
                let tombstoned = match tombstones {
                    Some(n) => {
                        let before = client.stats_async().await?;
                        let start = Mark::now()?;
                        // The stream after every one the trickle draws from.
                        let phase = 4 + trickle.map_or(0, |(rounds, _)| rounds);
                        let (deletes, found, delete_lat, get_lat) =
                            workload.tombstones_async(&ranges, n, phase).await?;
                        let done = Mark::now()?;
                        let after = client.stats_async().await?;
                        Some(Tombstoned {
                            draws: n,
                            deletes,
                            found,
                            delete_lat,
                            get_lat,
                            before,
                            after,
                            start,
                            done,
                        })
                    }
                    None => None,
                };
                let trickled = match trickle {
                    Some((rounds, draws)) => {
                        let before = client.stats_async().await?;
                        let start = Mark::now()?;
                        let mut puts = 0u64;
                        for round in 0..rounds {
                            puts += workload.trickle_async(&ranges, draws, round).await?;
                            client.flush_async().await?;
                            client.settled_async().await?;
                        }
                        let done = Mark::now()?;
                        let after = client.stats_async().await?;
                        Some(Trickled {
                            rounds,
                            puts,
                            before,
                            after,
                            start,
                            done,
                        })
                    }
                    None => None,
                };
                // Every timed query's answer checked against the oracle in full, untimed, by a
                // replay of the same queries on the store as the phases left it: the trickle
                // puts the fill's values again and the tombstones delete absent keys, so the
                // oracle's answers are unchanged, and each timed phase's counts must be the
                // replay's.
                if reads > 0 {
                    let verified = workload.verify_async(&ranges, reads, Work::Get).await?;
                    checked_against(&verified, &get, again.as_ref().and_then(|a| a.get.as_ref()))?;
                }
                if seeks > 0 {
                    let verified = workload
                        .verify_async(&ranges, seeks, Work::Seek(seek_nexts))
                        .await?;
                    checked_against(
                        &verified,
                        &seek,
                        again.as_ref().and_then(|a| a.seek.as_ref()),
                    )?;
                }
                drop(client);
                ranges.stop_async().await?;
                Ok::<_, BenchError>((
                    fill,
                    filled,
                    fill_stats,
                    postfill,
                    get,
                    read,
                    read_stats,
                    seek,
                    queried,
                    query_stats,
                    postquery,
                    again,
                    tombstoned,
                    trickled,
                    workload,
                ))
            }
            .await;
            drop(done.send(measured));
        })?;
        result
            .recv()
            .map_err(|_| io::Error::other("benchmark client task ended without a result"))??
    } else {
        let mut client = ranges.client()?;
        let fill = workload.phase(&ranges, num, Work::Fill)?;
        let filled = Mark::now()?;
        client.flush()?;
        client.checkpoint(threads * num)?;
        let fill_stats = client.stats()?;
        let postfill = Mark::now()?;
        let get = (reads > 0)
            .then(|| workload.phase(&ranges, reads, Work::Get))
            .transpose()?;
        let read = Mark::now()?;
        let read_stats = client.stats()?;
        let seek = (seeks > 0)
            .then(|| workload.phase(&ranges, seeks, Work::Seek(seek_nexts)))
            .transpose()?;
        let queried = Mark::now()?;
        client.flush()?;
        client.checkpoint(threads * num)?;
        let query_stats = client.stats()?;
        let postquery = Mark::now()?;
        let again = if queries_again {
            let start = Mark::now()?;
            let get = (reads > 0)
                .then(|| workload.phase(&ranges, reads, Work::Get))
                .transpose()?;
            let read = Mark::now()?;
            let seek = (seeks > 0)
                .then(|| workload.phase(&ranges, seeks, Work::Seek(seek_nexts)))
                .transpose()?;
            let done = Mark::now()?;
            Some(Again {
                start,
                get,
                read,
                seek,
                done,
            })
        } else {
            None
        };
        // Every timed query's answer checked against the oracle in full, untimed, as on the
        // shard-client path.
        if reads > 0 {
            let verified = workload.verify(&ranges, reads, Work::Get)?;
            checked_against(&verified, &get, again.as_ref().and_then(|a| a.get.as_ref()))?;
        }
        if seeks > 0 {
            let verified = workload.verify(&ranges, seeks, Work::Seek(seek_nexts))?;
            checked_against(
                &verified,
                &seek,
                again.as_ref().and_then(|a| a.seek.as_ref()),
            )?;
        }
        drop(client);
        ranges.stop()?;

        (
            fill,
            filled,
            fill_stats,
            postfill,
            get,
            read,
            read_stats,
            seek,
            queried,
            query_stats,
            postquery,
            again,
            None,
            None,
            workload,
        )
    };
    let runtime_stats = runtime.shutdown()?;
    drop(issuer);
    let stopped = Mark::now()?;
    // Cold reopen is after physical Stop/Runtime/Issuer retirement and never warms queries.
    for path in &paths {
        let file = if segments {
            BenchFile::Segments(SegmentedFile::open(
                path,
                caching,
                align,
                segment_bound(issuer_depth),
            )?)
        } else {
            BenchFile::One(DeviceFile::open(path, false, caching, align)?)
        };
        let (mut db, applied) = ShardDb::open(
            file,
            config,
            mem,
            TrunkConfig {
                fanout: 8,
                leaf_entries,
            },
        )?;
        require(applied == threads * num, "recovered applied index differs")?;
        benchmark_recovery::verify(&mut db, &workload.present, fill_value(), seek_nexts)?;
        match db.into_file() {
            IntoFile::Finished { file, result } => {
                result?;
                drop(file);
            }
            IntoFile::Refused { owner, error } => {
                // This is the plain cold benchmark caller; preserve the error, then clean up.
                drop(owner);
                return Err(error.into());
            }
        }
    }
    let recovered = Mark::now()?;
    #[cfg(feature = "count-allocs")]
    alloc::end_process();
    fill.report(&start, &filled);
    costs("postfillflush", threads * num, &filled, &postfill);
    costs("paid_fill", threads * num, &start, &postfill);
    if let Some(get) = get.as_mut() {
        get.report(&postfill, &read);
    }
    if let Some(seek) = seek.as_mut() {
        seek.report(&read, &queried);
    }
    let query_ops = (threads * (reads + seeks)).max(1);
    costs("postqueryflush", query_ops, &queried, &postquery);
    costs("paid_queries", query_ops, &postfill, &postquery);
    let stop_from = match again {
        Some(mut again) => {
            if let Some(get) = again.get.as_mut() {
                get.name = "readrandom_settled";
                get.report(&again.start, &again.read);
            }
            if let Some(seek) = again.seek.as_mut() {
                seek.name = "seekrandom_settled";
                seek.report(&again.read, &again.done);
            }
            again.done
        }
        None => postquery,
    };
    let stop_from = match tombstoned.as_mut() {
        Some(t) => {
            println!(
                "tombstones draws {} deletes {} found {}",
                t.draws, t.deletes, t.found
            );
            let at = |h: &Histogram, ppm: u32| h.quantile(ppm) as f64 / 1000.0;
            for (name, h) in [("delete", &t.delete_lat), ("get", &t.get_lat)] {
                println!(
                    "tombstones {name} latency_us p50 {:.2} p99 {:.2} p99.9 {:.2} p99.99 {:.2} max {:.2}",
                    at(h, 500_000),
                    at(h, 990_000),
                    at(h, 999_000),
                    at(h, 999_900),
                    h.max() as f64 / 1000.0
                );
            }
            costs(
                "tombstones",
                t.draws.saturating_add(t.deletes).max(1),
                &t.start,
                &t.done,
            );
            &t.done
        }
        None => &stop_from,
    };
    let stop_from = match &trickled {
        Some(t) => {
            println!("trickle rounds {} puts {}", t.rounds, t.puts);
            costs("trickle", t.puts.max(1), &t.start, &t.done);
            &t.done
        }
        None => stop_from,
    };
    costs("stop_shutdown", threads * num, stop_from, &stopped);
    costs("paid_total", total, &start, &stopped);
    costs("cold_setup", threads * num, &before_setup, &start);
    costs("recovery_verify", threads * num, &stopped, &recovered);
    costs("setup_to_close", total, &before_setup, &stopped);
    costs(
        "setup_to_verified_recovery",
        total,
        &before_setup,
        &recovered,
    );
    println!(
        "recovery verified_live_keys {} all_values_exact true required_crc_reads true applied {}",
        workload.present.len(),
        threads * num
    );
    stats("postfill", &fill_stats);
    stats("postread", &read_stats);
    stats("postquery", &query_stats);
    if let Some(t) = &tombstoned {
        stats("pretombstones", &t.before);
        stats("posttombstones", &t.after);
    }
    if let Some(t) = &trickled {
        stats("pretrickle", &t.before);
        stats("posttrickle", &t.after);
    }
    for (shard, counters) in runtime_stats.iter().enumerate() {
        println!(
            "runtime shard {shard} polls {} waits {} harvests {} long_steps {} blocked_steps {} preempted_steps {} foreign_wakes {} spin_hits {} spin_misses {} poller_wakes {} longest_step_ns {}",
            counters.polls,
            counters.waits,
            counters.harvests,
            counters.long_steps,
            counters.blocked_steps,
            counters.preempted_steps,
            counters.wakes_foreign,
            counters.spin_hits,
            counters.spin_misses,
            counters.poller_wakes,
            counters.longest_step_ns
        );
    }
    for path in paths {
        // A store kept as segments has `<path>.1` and on beside its base.
        let mut lengths = Vec::new();
        for k in 1usize.. {
            let mut name = path.as_os_str().to_owned();
            name.push(format!(".{k}"));
            let segment = PathBuf::from(name);
            if !segment.exists() {
                break;
            }
            lengths.push(std::fs::metadata(&segment)?.len());
            std::fs::remove_file(segment)?;
        }
        println!(
            "store_file bytes {} segments {} segment_bytes {} lengths {lengths:?}",
            std::fs::metadata(&path)?.len(),
            lengths.len(),
            lengths.iter().sum::<u64>()
        );
        std::fs::remove_file(path)?;
    }
    Ok(())
}
