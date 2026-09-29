//! `mantle bench chunk`: the chunk store's write and read throughput and latency on a device,
//! next to what the device does through the same file layer.
//!
//! The device is calibrated first (`mantle_disk::calibrate`). A chunk volume is then formatted
//! in a scratch file on the same device and driven by closed-loop workers: each worker issues
//! a request, waits for its answer and issues the next, so the number of workers is the
//! number of requests in flight. A fill pass writes the whole volume once, because the first
//! write into a block a file system has not written before can cost more than later writes
//! (docs/measurements/2026-09-28-flush-cost-by-extent-state.md); its throughput is reported
//! on its own. Then, for each chunk size and worker count, the workers put chunks for the
//! step's duration or until they have written a third of the volume, read the same chunks
//! back in random order for the step's duration, and delete them, so every point starts from
//! the same state. Right after each read point, the same number of workers read the same
//! number of bytes from random places in the volume's segments directly through the file
//! layer. Background work of the file system and the drive stalls reads at times; running
//! the two back to back shows what mantle adds, apart from the environment. Each point runs
//! in rounds, as calibration's do (`calibrate::Rounds`), until the throughput of its puts,
//! reads and file-layer reads is each within ±5% at 95% confidence, or six rounds have run:
//! one pass measures the drive's recent history as much as the store
//! (docs/measurements/2026-09-28-chunk-store-benchmark.md, finding 7). The scratch file is
//! removed however the benchmark ends.

use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mantle_chunk::{ChunkError, ChunkKey, Config, Volume};
use mantle_disk::buf::{AlignedBuf, Alignment};
use mantle_disk::calibrate::{self, Calibration, Rounds};
use mantle_disk::file::{CachingRequest, DeviceFile};
use mantle_disk::histogram::Histogram;
use mantle_disk::measure::SplitMix64;

use crate::display;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Disk(mantle_disk::DiskError),
    Chunk(ChunkError),
    Log(String),
    /// A worker thread unwound; its measurements are lost.
    Worker,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Disk(e) => write!(f, "{e}"),
            Self::Chunk(e) => write!(f, "chunk store: {e}"),
            Self::Log(e) => write!(f, "raft log: {e}"),
            Self::Worker => write!(f, "a benchmark worker stopped unexpectedly"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

/// What to measure.
#[derive(Debug, Clone)]
pub struct Plan {
    /// Bytes of the scratch volume.
    pub volume: u64,
    /// Chunk sizes, in bytes.
    pub sizes: Vec<usize>,
    /// Requests in flight, one worker each; the depths calibration measures reads at.
    pub workers: Vec<usize>,
    /// How long each round of a put or read point runs.
    pub step: Duration,
    pub rounds: Rounds,
}

impl Plan {
    /// A 4 GiB volume, or a tenth of the free space if that is smaller; chunks from a small
    /// object's 4 KiB to an erasure-coded shard's 8 MiB.
    pub fn standard(available: Option<u64>, step: Duration) -> Self {
        let cap = 4u64 << 30;
        let volume = available.map_or(cap, |free| cap.min(free / 10));
        Self {
            volume,
            sizes: vec![4 << 10, 64 << 10, 1 << 20, 8 << 20],
            workers: vec![1, 4, 16, 64],
            step,
            rounds: Rounds::STANDARD,
        }
    }
}

/// Removes the scratch volume whatever happens to the benchmark.
pub(crate) struct Scratch(pub(crate) PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        // Nothing to report to: the file is ours and absent is the goal.
        let _ = std::fs::remove_file(&self.0);
    }
}

/// One measured point.
#[derive(Debug, Clone)]
struct Outcome {
    ops: u64,
    bytes: u64,
    elapsed: Duration,
    latency: Histogram,
    /// The volume refused a write for lack of space before the point's budget ran out.
    full: bool,
}

impl Outcome {
    fn ops_per_sec(&self) -> f64 {
        rate(self.ops, self.elapsed)
    }

    fn bytes_per_sec(&self) -> f64 {
        rate(self.bytes, self.elapsed)
    }
}

/// A point's rounds: each round's throughput, and every round's transfers together.
#[derive(Debug, Default)]
struct Series {
    ops_per_sec: Vec<f64>,
    bytes_per_sec: Vec<f64>,
    latency: Histogram,
    full: bool,
}

impl Series {
    fn add(&mut self, o: &Outcome) {
        self.ops_per_sec.push(o.ops_per_sec());
        self.bytes_per_sec.push(o.bytes_per_sec());
        self.latency.merge(&o.latency);
        self.full |= o.full;
    }

    /// Enough rounds by `rounds`, or none to judge: reads of a point that wrote nothing.
    fn enough(&self, rounds: &Rounds) -> bool {
        self.ops_per_sec.is_empty() || rounds.enough(&self.ops_per_sec)
    }
}

// Rates are floating point; u64 -> f64 rounds above 2^53, far beyond any count a bounded
// point produces.
pub(crate) fn rate(count: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    count as f64 / secs
}

/// What `mantle bench chunk` was asked to run.
#[derive(Debug, Clone)]
pub struct Options {
    pub step: Duration,
    /// Chunk sizes; the standard set when empty.
    pub sizes: Vec<usize>,
    /// Requests in flight; the standard set when empty.
    pub workers: Vec<usize>,
    /// Leave out the device measurement.
    pub skip_device: bool,
}

pub fn chunk(out: &mut impl Write, path: &Path, options: &Options) -> Result<(), Error> {
    let id = mantle_disk::probe::identify(path);
    let align = [id.logical_block, id.physical_block]
        .into_iter()
        .flatten()
        .filter_map(|b| usize::try_from(b).ok())
        .filter_map(|b| Alignment::new(b).ok())
        .fold(
            Alignment::new(4096).unwrap_or(Alignment::BYTE),
            Alignment::max,
        );
    let mut plan = Plan::standard(id.file_system.available_bytes, options.step);
    if !options.sizes.is_empty() {
        plan.sizes.clone_from(&options.sizes);
    }
    if !options.workers.is_empty() {
        plan.workers.clone_from(&options.workers);
    }
    writeln!(out, "{}", path.display())?;
    // The volume is formatted as mantle would format it here, which the device measurement
    // decides; without one it is not pre-written.
    let mut prewrite = false;
    if !options.skip_device {
        writeln!(out, "measuring the device (about 30 s)")?;
        out.flush()?;
        let device = calibrate::calibrate(
            path,
            align,
            id.file_system.available_bytes,
            &calibrate::Plan::standard(align),
        )
        .map_err(Error::Disk)?;
        report_device(out, &device)?;
        prewrite = device.first_write_penalty();
    }
    run(out, path, align, &plan, prewrite)
}

pub(crate) fn report_device(out: &mut impl Write, c: &Calibration) -> std::io::Result<()> {
    let best = |points: &[calibrate::Point]| {
        points
            .iter()
            .map(|p| p.bytes_per_sec)
            .fold(0.0f64, f64::max)
    };
    writeln!(
        out,
        "  durable write  {} ({} write, then a full flush)",
        display::quantile(c.durable_write.p50_ns),
        display::size(c.small)
    )?;
    writeln!(
        out,
        "  durable batch  {} ({} written, then a full flush)",
        display::rate(c.durable_sequential.bytes_per_sec),
        display::size(c.durable_large)
    )?;
    writeln!(out, "  first writes   {}", crate::disk::first_writes(c))?;
    writeln!(
        out,
        "  throughput     {} read, {} write ({} transfers, no flush)",
        display::rate(best(&c.sequential_read)),
        display::rate(best(&c.sequential_write)),
        display::size(c.large)
    )?;
    for p in &c.random_read {
        writeln!(
            out,
            "  {} reads   {:>3} in flight: {}/s, {} p50, {} p99",
            display::size(c.small),
            p.depth,
            display::count(p.ops_per_sec),
            display::quantile(p.p50_ns),
            display::quantile(p.p99_ns)
        )?;
    }
    Ok(())
}

fn run(
    out: &mut impl Write,
    dir: &Path,
    align: Alignment,
    plan: &Plan,
    prewrite: bool,
) -> Result<(), Error> {
    let path = dir.join(format!(".mantle-bench-{}", std::process::id()));
    let _scratch = Scratch(path.clone());
    let file =
        DeviceFile::open(&path, true, CachingRequest::PreferDirect, align).map_err(Error::Disk)?;
    let volume = align.down_u64(plan.volume);
    file.preallocate(volume).map_err(Error::Disk)?;
    // Room in the index for a third of the volume in the smallest chunks, twice over.
    let smallest = plan.sizes.iter().copied().min().unwrap_or(4096).max(1);
    let config = Config {
        max_fragments: volume
            .checked_div(u64::try_from(smallest).unwrap_or(u64::MAX))
            .unwrap_or(0)
            .saturating_mul(2)
            .checked_div(3)
            .unwrap_or(0)
            .max(1024),
        scrub_period: None,
        prewrite,
        ..Config::default()
    };
    let v = Volume::format(file, volume, config).map_err(Error::Chunk)?;
    let raw =
        DeviceFile::open(&path, false, CachingRequest::PreferDirect, align).map_err(Error::Disk)?;
    let span = v.data_span();
    writeln!(
        out,
        "chunk store on a {} volume in a scratch file (removed afterwards){}",
        display::capacity(volume),
        if prewrite {
            ", written once at format"
        } else {
            ""
        }
    )?;
    out.flush()?;

    let fill_size = 1 << 20;
    let (filled, keys) = puts(&v, fill_size, 8, Duration::MAX, u64::MAX, 0)?;
    writeln!(
        out,
        "  first pass     {} writing {} chunks into blocks never written before",
        display::rate(filled.bytes_per_sec()),
        display::size(fill_size)
    )?;
    deletes(&v, &keys)?;

    writeln!(
        out,
        "  {:<14} {:>9} {:>10} {:>12} {:>5} {:>6} {:>10} {:>10} {:>10}",
        "", "in flight", "ops/s", "throughput", "±", "rounds", "p50", "p99", "p99.9"
    )?;
    let budget = volume / 3;
    let mut point = 1u64;
    for &size in &plan.sizes {
        for &workers in &plan.workers {
            // More writers than the store's queue admits would be refused with Busy: the
            // queue holds what the writer takes in two batches (Limits::queue_bytes).
            let admitted = config
                .limits
                .queue_bytes()
                .checked_div(u64::try_from(size).unwrap_or(u64::MAX))
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(usize::MAX)
                .clamp(1, config.limits.queue_requests().max(1));
            let writers = workers.min(admitted);
            let (mut put, mut get, mut direct) =
                (Series::default(), Series::default(), Series::default());
            for _ in 0..plan.rounds.limit() {
                let (outcome, keys) = puts(&v, size, writers, plan.step, budget, point)?;
                put.add(&outcome);
                if !keys.is_empty() {
                    get.add(&gets(&v, &keys, size, workers, plan.step, point)?);
                    direct.add(&raw_reads(&raw, &span, size, workers, plan.step, point)?);
                }
                deletes(&v, &keys)?;
                point = point.saturating_add(1);
                if [&put, &get, &direct].iter().all(|s| s.enough(&plan.rounds)) {
                    break;
                }
            }
            row(out, &format!("put {}", display::size(size)), writers, &put)?;
            if !get.ops_per_sec.is_empty() {
                row(out, &format!("get {}", display::size(size)), workers, &get)?;
                row(out, "  file layer", workers, &direct)?;
            }
        }
    }
    v.close();
    Ok(())
}

/// A point's row: its mean throughput over the rounds, the half-width of that mean's 95%
/// confidence interval, and latency quantiles over every round's transfers.
fn row(out: &mut impl Write, name: &str, workers: usize, s: &Series) -> std::io::Result<()> {
    writeln!(
        out,
        "  {:<14} {:>9} {:>10} {:>12} {:>5} {:>6} {:>10} {:>10} {:>10}{}",
        name,
        workers,
        display::count(calibrate::mean(&s.ops_per_sec)),
        display::rate(calibrate::mean(&s.bytes_per_sec)),
        display::percent(calibrate::relative_interval(&s.ops_per_sec)),
        s.ops_per_sec.len(),
        display::nanos(s.latency.p50()),
        display::nanos(s.latency.p99()),
        display::nanos(s.latency.p999()),
        if s.full { "  (volume full)" } else { "" }
    )?;
    out.flush()
}

/// A key no other point or worker uses.
fn key(point: u64, worker: usize, n: u64) -> ChunkKey {
    ChunkKey {
        block: (u128::from(point) << 96)
            | (u128::from(u64::try_from(worker).unwrap_or(0)) << 64)
            | u128::from(n),
        epoch: 1,
        index: 0,
    }
}

/// `workers` closed-loop writers put `size`-byte chunks until `step` passes, `budget` bytes
/// are written, or the volume is full.
fn puts(
    v: &Volume<DeviceFile>,
    size: usize,
    workers: usize,
    step: Duration,
    budget: u64,
    point: u64,
) -> Result<(Outcome, Vec<ChunkKey>), Error> {
    let started = Instant::now();
    let deadline = started.checked_add(step);
    let written = AtomicU64::new(0);
    let full = AtomicBool::new(false);
    let failed: Mutex<Option<ChunkError>> = Mutex::new(None);
    let size64 = u64::try_from(size).unwrap_or(u64::MAX);
    let results: Vec<Option<(Histogram, Vec<ChunkKey>)>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let (written, full, failed) = (&written, &full, &failed);
                scope.spawn(move || {
                    let mut payload = vec![0u8; size];
                    SplitMix64::new(point ^ u64::try_from(worker).unwrap_or(0)).fill(&mut payload);
                    let mut latency = Histogram::new();
                    let mut keys = Vec::new();
                    let mut n = 0u64;
                    loop {
                        if deadline.is_some_and(|d| Instant::now() >= d)
                            || written.load(Ordering::Relaxed) >= budget
                            || full.load(Ordering::Relaxed)
                            || !failed.lock().is_ok_and(|f| f.is_none())
                        {
                            break;
                        }
                        let k = key(point, worker, n);
                        n = n.saturating_add(1);
                        let t = Instant::now();
                        // Busy means the store is cleaning to make room: a client backs off
                        // and puts the same chunk again, and the wait counts in its latency.
                        let result = loop {
                            match v.put(k, &payload) {
                                Err(ChunkError::Busy)
                                    if deadline.is_none_or(|d| Instant::now() < d) =>
                                {
                                    std::thread::yield_now();
                                }
                                other => break other,
                            }
                        };
                        match result {
                            Ok(()) => {
                                latency.record(nanos(t.elapsed()));
                                keys.push(k);
                                written.fetch_add(size64, Ordering::Relaxed);
                            }
                            Err(ChunkError::Full) => full.store(true, Ordering::Relaxed),
                            // Still busy when the step ended: nothing was written.
                            Err(ChunkError::Busy) => {}
                            Err(e) => {
                                if let Ok(mut f) = failed.lock() {
                                    f.get_or_insert(e);
                                }
                            }
                        }
                    }
                    (latency, keys)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().ok()).collect()
    });
    let elapsed = started.elapsed();
    if let Some(e) = failed.into_inner().ok().flatten() {
        return Err(Error::Chunk(e));
    }
    let mut latency = Histogram::new();
    let mut keys = Vec::new();
    for result in results {
        let (h, k) = result.ok_or(Error::Worker)?;
        latency.merge(&h);
        keys.extend(k);
    }
    let ops = latency.count();
    Ok((
        Outcome {
            ops,
            bytes: ops.saturating_mul(size64),
            elapsed,
            latency,
            full: full.into_inner(),
        },
        keys,
    ))
}

/// `workers` closed-loop readers read whole chunks of `keys`, chosen at random, until
/// `step` passes.
fn gets(
    v: &Volume<DeviceFile>,
    keys: &[ChunkKey],
    size: usize,
    workers: usize,
    step: Duration,
    point: u64,
) -> Result<Outcome, Error> {
    let started = Instant::now();
    let deadline = started.checked_add(step);
    let failed: Mutex<Option<ChunkError>> = Mutex::new(None);
    let size64 = u64::try_from(size).unwrap_or(u64::MAX);
    let count = u64::try_from(keys.len()).unwrap_or(u64::MAX);
    let results: Vec<Option<Histogram>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let failed = &failed;
                scope.spawn(move || {
                    let mut rng = SplitMix64::new(!point ^ u64::try_from(worker).unwrap_or(0));
                    let mut latency = Histogram::new();
                    let mut buf = Vec::new();
                    loop {
                        if deadline.is_some_and(|d| Instant::now() >= d)
                            || !failed.lock().is_ok_and(|f| f.is_none())
                        {
                            break;
                        }
                        let pick = usize::try_from(rng.below(count)).unwrap_or(0);
                        let Some(k) = keys.get(pick) else {
                            break;
                        };
                        let t = Instant::now();
                        match v.read_into(k, 0, size64, &mut buf) {
                            Ok(()) => latency.record(nanos(t.elapsed())),
                            Err(e) => {
                                if let Ok(mut f) = failed.lock() {
                                    f.get_or_insert(e);
                                }
                            }
                        }
                    }
                    latency
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().ok()).collect()
    });
    let elapsed = started.elapsed();
    if let Some(e) = failed.into_inner().ok().flatten() {
        return Err(Error::Chunk(e));
    }
    let mut latency = Histogram::new();
    for h in results {
        latency.merge(&h.ok_or(Error::Worker)?);
    }
    let ops = latency.count();
    Ok(Outcome {
        ops,
        bytes: ops.saturating_mul(size64),
        elapsed,
        latency,
        full: false,
    })
}

/// `workers` closed-loop readers read `size` bytes, rounded up to the file's alignment, at
/// random aligned offsets in `span` through the file layer, until `step` passes.
fn raw_reads(
    file: &DeviceFile,
    span: &std::ops::Range<u64>,
    size: usize,
    workers: usize,
    step: Duration,
    point: u64,
) -> Result<Outcome, Error> {
    let align = file.alignment();
    let len = align.up(size).unwrap_or(size);
    let len64 = u64::try_from(len).unwrap_or(u64::MAX);
    let block = u64::try_from(align.get()).unwrap_or(4096);
    let slots = span
        .end
        .saturating_sub(span.start)
        .saturating_sub(len64)
        .checked_div(block)
        .unwrap_or(0)
        .max(1);
    let started = Instant::now();
    let deadline = started.checked_add(step);
    let failed: Mutex<Option<mantle_disk::DiskError>> = Mutex::new(None);
    let results: Vec<Option<Histogram>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let failed = &failed;
                scope.spawn(move || {
                    let seed = point.rotate_left(17) ^ u64::try_from(worker).unwrap_or(0);
                    let mut rng = SplitMix64::new(seed);
                    let mut latency = Histogram::new();
                    let mut buf = match AlignedBuf::zeroed(len, align) {
                        Ok(buf) => buf,
                        Err(e) => {
                            if let Ok(mut f) = failed.lock() {
                                f.get_or_insert(e.into());
                            }
                            return latency;
                        }
                    };
                    if buf.set_len(len).is_err() {
                        return latency;
                    }
                    loop {
                        if deadline.is_some_and(|d| Instant::now() >= d)
                            || !failed.lock().is_ok_and(|f| f.is_none())
                        {
                            break;
                        }
                        let at = span
                            .start
                            .saturating_add(rng.below(slots).saturating_mul(block));
                        let t = Instant::now();
                        match file.read_exact_at(buf.as_mut_slice(), at) {
                            Ok(()) => latency.record(nanos(t.elapsed())),
                            Err(e) => {
                                if let Ok(mut f) = failed.lock() {
                                    f.get_or_insert(e);
                                }
                            }
                        }
                    }
                    latency
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().ok()).collect()
    });
    let elapsed = started.elapsed();
    if let Some(e) = failed.into_inner().ok().flatten() {
        return Err(Error::Disk(e));
    }
    let mut latency = Histogram::new();
    for h in results {
        latency.merge(&h.ok_or(Error::Worker)?);
    }
    let ops = latency.count();
    Ok(Outcome {
        ops,
        bytes: ops.saturating_mul(u64::try_from(size).unwrap_or(u64::MAX)),
        elapsed,
        latency,
        full: false,
    })
}

/// Deletes `keys` with enough concurrent requests that they share flushes.
fn deletes(v: &Volume<DeviceFile>, keys: &[ChunkKey]) -> Result<(), Error> {
    const WORKERS: usize = 64;
    let per = keys.len().div_ceil(WORKERS).max(1);
    let results: Vec<Option<Result<(), ChunkError>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = keys
            .chunks(per)
            .map(|part| scope.spawn(move || part.iter().try_for_each(|k| v.delete(*k))))
            .collect();
        handles.into_iter().map(|h| h.join().ok()).collect()
    });
    for result in results {
        result.ok_or(Error::Worker)?.map_err(Error::Chunk)?;
    }
    Ok(())
}

pub(crate) fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_small_benchmark_runs_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let align = Alignment::new(4096).unwrap();
        let plan = Plan {
            volume: 1 << 30,
            sizes: vec![4 << 10, 1 << 20],
            workers: vec![1, 4],
            step: Duration::from_millis(50),
            rounds: Rounds {
                min: 2,
                max: 2,
                precision: 0.05,
            },
        };
        let mut out = Vec::new();
        run(&mut out, dir.path(), align, &plan, false).unwrap();
        let text = String::from_utf8(out).unwrap();
        for name in [
            "first pass",
            "put 4 KiB",
            "get 4 KiB",
            "put 1 MiB",
            "get 1 MiB",
            "file layer",
        ] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "scratch volume left behind"
        );
    }
}
