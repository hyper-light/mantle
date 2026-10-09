//! A matched Range API workload on hyper-rt: MT19937-64 fill/get/seek streams from
//! RocksDB 11.8.1, ASCII-zero 16-byte keys, repeated-v 100-byte values. One client,
//! range and shard by default; declared multi-thread runs give each client NUM operations.
//! `DIR [NUM] [THREADS] [READS] [CACHE_MIB] [ISSUER_DEPTH] [SEEKS] [SEEK_NEXTS]
//! [SPIN_NS] [WRITE_BUDGET_MIB] [--rocks-seed=301] [--runtime-record=PATH]`. Memtables total 64 MiB; cache and
//! write memory are divided equally across ranges. Buffered I/O, fanout 8, no WAL.
//!
//! This compares identical Range controls, not direct RocksDB timings: throughput/work
//! include exact membership/value/row checks; latency is the API call alone. Postfill and
//! postquery flush+checkpoint barriers are timed and charged, including metadata/flushes.
//! Checkpoint does not finish optional views/maplets. Continuous process counts through
//! stop/shutdown include maintenance between phase snapshots and discard/cleanup work.
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

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_measure::{alloc, faults, usage};
use hyper_rt::machine::calibration::{Calibration, Policy};
use hyper_rt::{Runtime, RuntimeConfig};
use mantle_engine::ranges::{RangeStats, Ranges, RangesConfig};
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

const VALUE: [u8; 100] = [b'v'; 100];
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
    alloc: alloc::Counts,
    faults: faults::Faults,
    switches: Option<u64>,
    usage: Option<usage::Usage>,
}

impl Mark {
    fn now() -> Result<Self, BenchError> {
        Ok(Self {
            at: Instant::now(),
            alloc: alloc::read_process(),
            faults: faults::read()?,
            switches: faults::switches().ok(),
            usage: usage::this().ok(),
        })
    }
}

fn costs(name: &str, ops: u64, from: &Mark, to: &Mark) {
    let a = to.alloc.less(&from.alloc);
    let f = to.faults.since(&from.faults);
    let process = to.usage.zip(from.usage).map(|(a, b)| a.since(&b));
    let per_op = |count: Option<u64>| {
        count.map_or_else(
            || "unavailable".to_string(),
            |count| format!("{:.3}", count as f64 / ops as f64),
        )
    };
    println!(
        "{name} elapsed {:.6}s allocs/op {:.3} reallocs/op {:.3} bytes/op {:.1} faults/op {:.4} (allocs {} reallocs {} faults {})",
        to.at.duration_since(from.at).as_secs_f64(),
        a.allocations as f64 / ops as f64,
        a.reallocations as f64 / ops as f64,
        a.bytes as f64 / ops as f64,
        f.total() as f64 / ops as f64,
        a.allocations,
        a.reallocations,
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
}

struct Phase {
    name: &'static str,
    ops: u64,
    found: u64,
    rows: u64,
    lat: Vec<u64>,
}

impl Phase {
    fn report(&mut self, from: &Mark, to: &Mark) {
        let seconds = to.at.duration_since(from.at).as_secs_f64();
        self.lat.sort_unstable();
        let at = |q: f64| {
            self.lat[((self.lat.len() as f64 * q) as usize).min(self.lat.len() - 1)] as f64 / 1000.0
        };
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
            at(0.50),
            at(0.99),
            at(0.999),
            at(0.9999),
            at(1.0)
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
    present: Vec<bool>,
    ordered: Vec<u64>,
}

impl Workload {
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
                        let mut lat = Vec::with_capacity(n as usize);
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
                            let began = Instant::now();
                            let hit = match work {
                                Work::Fill => {
                                    client.put(&key, &VALUE)?;
                                    true
                                }
                                Work::Get => client.get(&key, &mut value)?,
                                Work::Seek(limit) => {
                                    client.scan(&key, None, limit, &mut page, &mut next)?;
                                    false
                                }
                            };
                            lat.push(began.elapsed().as_nanos() as u64);
                            let hit = match work {
                                Work::Fill => hit,
                                Work::Get => {
                                    require(
                                        hit == self.present[start as usize],
                                        "found flag differs from the fill oracle",
                                    )?;
                                    if hit {
                                        require(
                                            value.as_slice() == VALUE,
                                            "get value differs from the fill oracle",
                                        )?;
                                    }
                                    hit
                                }
                                Work::Seek(limit) => {
                                    let at = self.ordered.partition_point(|&key| key < start);
                                    let expected = &self.ordered
                                        [at..at.saturating_add(limit).min(self.ordered.len())];
                                    require(
                                        page.len() == expected.len(),
                                        "scan row count differs from the fill oracle",
                                    )?;
                                    for ((key, value), &number) in page.iter().zip(expected) {
                                        require(
                                            key == rocks_workload::key(number),
                                            "scan key differs from the fill oracle",
                                        )?;
                                        require(
                                            value == VALUE,
                                            "scan value differs from the fill oracle",
                                        )?;
                                    }
                                    rows += page.len() as u64;
                                    let hit = page.get(0).is_some_and(|(first, _)| first == key);
                                    require(
                                        hit == self.present[start as usize],
                                        "found flag differs from the fill oracle",
                                    )?;
                                    hit
                                }
                            };
                            found += u64::from(hit);
                        }
                        Ok::<_, BenchError>((lat, found, rows))
                    })
                })
                .collect();
            let mut all = Vec::with_capacity((self.threads * n) as usize);
            let mut found = 0;
            let mut rows = 0;
            for handle in handles {
                let (lat, hits, scanned) = handle
                    .join()
                    .map_err(|_| io::Error::other("a benchmark client thread panicked"))??;
                all.extend(lat);
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
        let thread = 0;
        let mut client = ranges.client()?;
        let mut rng = self.rng(phase, thread);
        let mut lat = Vec::with_capacity(n as usize);
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
            let began = Instant::now();
            let hit = match work {
                Work::Fill => {
                    client.put_async(&key, &VALUE).await?;
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
            lat.push(began.elapsed().as_nanos() as u64);
            let hit = match work {
                Work::Fill => hit,
                Work::Get => {
                    require(
                        hit == self.present[start as usize],
                        "found flag differs from the fill oracle",
                    )?;
                    if hit {
                        require(
                            value.as_slice() == VALUE,
                            "get value differs from the fill oracle",
                        )?;
                    }
                    hit
                }
                Work::Seek(limit) => {
                    let at = self.ordered.partition_point(|&key| key < start);
                    let expected =
                        &self.ordered[at..at.saturating_add(limit).min(self.ordered.len())];
                    require(
                        page.len() == expected.len(),
                        "scan row count differs from the fill oracle",
                    )?;
                    for ((key, value), &number) in page.iter().zip(expected) {
                        require(
                            key == rocks_workload::key(number),
                            "scan key differs from the fill oracle",
                        )?;
                        require(value == VALUE, "scan value differs from the fill oracle")?;
                    }
                    rows += page.len() as u64;
                    let hit = page.get(0).is_some_and(|(first, _)| first == key);
                    require(
                        hit == self.present[start as usize],
                        "found flag differs from the fill oracle",
                    )?;
                    hit
                }
            };
            found += u64::from(hit);
        }
        // Match the threaded driver's joined latency-vector aggregation and its paid copy.
        let mut all = Vec::with_capacity(n as usize);
        all.extend(lat);
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
    for (range, (f, t, io)) in counters.iter().enumerate() {
        println!(
            "{name} range {range} flushes {} stalls {} waited_steps {} pivot_compactions {} leaf_compactions {} splits {} entries_written {} views_built {} views_dropped {} maplets_built {} maplets_declined {} maplets_dropped {} io reads {} pages_read {} submitted {} pages_written {} syncs {} queued {} queued_most {} write_waits {} prefetches {} prefetch_waits {} cache_hits {} span_cache_hits {}",
            f.flushes,
            f.stalls,
            f.waited_steps,
            t.pivot_compactions,
            t.leaf_compactions,
            t.splits,
            t.entries_written,
            t.views_built,
            t.views_dropped,
            t.maplets_built,
            t.maplets_declined,
            t.maplets_dropped,
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
            io.span_cache_hits
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
    let mut shard_client = false;
    let mut seed = 301u64;
    let mut runtime_record = None;
    let mut args = Vec::new();
    for arg in std::env::args().skip(1) {
        if arg == "--shard-client" {
            shard_client = true;
        } else if let Some(value) = arg.strip_prefix("--rocks-seed=") {
            seed = value.parse()?;
        } else if let Some(path) = arg.strip_prefix("--runtime-record=") {
            runtime_record = Some(PathBuf::from(path));
        } else if arg != "--bench" {
            args.push(arg);
        }
    }
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
        num > 0 && threads > 0 && threads <= num && seek_nexts > 0,
        "positive num/threads/seek_nexts and threads <= num required",
    )?;
    let thread_count = u16::try_from(threads)?;
    let num_keys = usize::try_from(num)?;
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
    let all_batches = issuer_depth
        .checked_mul(usize::try_from(threads)?)
        .ok_or_else(overflow)?;
    std::fs::create_dir_all(&dir)?;
    let mut workload = Workload {
        num,
        threads,
        seed,
        present: vec![false; num_keys],
        ordered: Vec::new(),
    };
    // Exact oracle and its storage are setup work, outside measured operations.
    for thread in 0..threads {
        let mut rng = workload.rng(1, thread);
        for _ in 0..num {
            workload.present[(rng.next() % num) as usize] = true;
        }
    }
    workload.ordered = workload
        .present
        .iter()
        .enumerate()
        .filter_map(|(key, &present)| present.then_some(key as u64))
        .collect();
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
        "workload generator rocksdb-mt19937_64 seed {seed} key_padding ascii-0 values repeated-v num {num} client_threads {threads} ranges {threads} shards {threads} reads_per_thread {reads} seeks_per_thread {seeks} seek_nexts {seek_nexts} issuer_depth {issuer_depth} runs_in_flight_per_range {issuer_depth} cache_mib {cache_mib} write_budget_mib {write_budget_mib} oracle_keys {} throughput full-oracle latency api-only barriers flush+durable-checkpoint optional-accelerators-not-fully-drained",
        workload.ordered.len()
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
    let runtime = Runtime::start(&rt)?;
    let align = Alignment::new(4096)?;
    let config = Config {
        page_size: 4096,
        extent_pages: 32,
        max_extents: 1 << 24,
    };
    let mem = (64usize << 20) / threads as usize;
    let leaf_entries = (mem / (16 + 100 + 3)) as u64;
    let issuer = if issuer_depth > 0 {
        Some(Issuer::start_for(&dir, issuer_depth, all_batches)?)
    } else {
        None
    };
    println!(
        "workers client_roles {threads} owner_roles {threads} client_placement {} issuer {} device {}",
        if shard_client {
            "same-shard"
        } else {
            "external-thread"
        },
        usize::from(issuer.is_some()),
        issuer.as_ref().map_or(0, Issuer::depth)
    );
    let mut engines = Vec::new();
    let mut paths = Vec::new();
    for i in 0..threads {
        let path = dir.join(format!("range-{i}.store"));
        require(!path.exists(), "benchmark requires a fresh directory")?;
        let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align)?;
        let mut db = ShardDb::create(
            file,
            config,
            mem,
            TrunkConfig {
                fanout: 8,
                leaf_entries,
            },
        )?;
        db.set_cache(cache_bytes / 4096 / threads as usize);
        db.set_write_budget(write_bytes / threads as usize);
        if let Some(issuer) = &issuer {
            db.attach(issuer, issuer_depth)?;
        }
        let start = if i == 0 {
            Vec::new()
        } else {
            rocks_workload::key(i * num / threads).to_vec()
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
    )?;
    alloc::begin_process();
    let start = Mark::now()?;
    let (
        mut fill,
        filled,
        fill_stats,
        postfill,
        mut get,
        read,
        mut seek,
        queried,
        query_stats,
        postquery,
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
                drop(client);
                ranges.stop_async().await?;
                Ok::<_, BenchError>((
                    fill,
                    filled,
                    fill_stats,
                    postfill,
                    get,
                    read,
                    seek,
                    queried,
                    query_stats,
                    postquery,
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
        let seek = (seeks > 0)
            .then(|| workload.phase(&ranges, seeks, Work::Seek(seek_nexts)))
            .transpose()?;
        let queried = Mark::now()?;
        client.flush()?;
        client.checkpoint(threads * num)?;
        let query_stats = client.stats()?;
        let postquery = Mark::now()?;
        drop(client);
        ranges.stop()?;

        (
            fill,
            filled,
            fill_stats,
            postfill,
            get,
            read,
            seek,
            queried,
            query_stats,
            postquery,
        )
    };
    let runtime_stats = runtime.shutdown()?;
    drop(issuer);
    let stopped = Mark::now()?;
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
    costs("stop_shutdown", threads * num, &postquery, &stopped);
    costs("paid_total", total, &start, &stopped);
    stats("postfill", &fill_stats);
    stats("postquery", &query_stats);
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
        std::fs::remove_file(path)?;
    }
    Ok(())
}
