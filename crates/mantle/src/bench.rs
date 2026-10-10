//! `mantle bench chunk`: the chunk store's write and read throughput and latency on a device,
//! next to what the device does through the same file layer.
//!
//! The device is calibrated first (`mantle_disk::calibrate`). A chunk volume is then formatted
//! in a scratch file on the same device and driven by clients, each a record on a driver thread
//! (`drive`): closed loop, each client issues a request when its last is answered, so the
//! number of clients is the number of requests in flight; open loop, with `--rate`, each
//! client's requests arrive at intended times drawn from a Poisson process, and a request's
//! latency runs from its intended start (docs/design/measurement.md §10). Puts go out without a
//! thread each and are answered through the client's waker; reads, which block, run on as many
//! threads as the store lets read at once, the bound calibration measured. Each row says the
//! process's threads, as the OS counts them, and the generator's lateness. A fill pass writes the whole volume once, because the first
//! write into a block a file system has not written before can cost more than later writes
//! (docs/measurements/2026-09-28-flush-cost-by-extent-state.md); its throughput is reported
//! on its own. Then, for each chunk size and worker count, the workers put chunks for the
//! step's duration or until they have written a third of the volume, read the same chunks
//! back in random order for the step's duration, and delete them, so every point starts from
//! the same state. Each client's first operation of a round goes out whatever the step, so a
//! round measures at least that much on a host too slow to finish one within it. Right after
//! each read point, the same number of workers read the same number of bytes from random
//! places in the volume's segments directly through the file layer. Background work of the
//! file system and the drive stalls reads at times; running the two back to back shows what
//! mantle adds, apart from the environment. For chunks
//! larger than the smallest size, the same readers then read ranges of the smallest size at
//! random places within the same chunks, which a range GET does: the store reads a range's
//! checksum blocks, not the chunk (docs/design/chunk-store.md §7).
//!
//! One pass measures the drive's recent history as much as the store
//! (docs/measurements/2026-09-28-chunk-store-benchmark.md, finding 7), and this machine's
//! drive answers a stream of full flushes by stalling every read for about a second at a time,
//! so a round's reads catch a stall or miss it (docs/measurements/2026-09-29-chunk-store-states.md).
//! Each point therefore runs in rounds, ten to thirty by default, and each operation's rounds
//! are judged as `mantle_disk::rounds` judges them: whether they are independent in time,
//! whether they fall in one state or two, and each state's median with its 95% interval. A
//! point stops before the limit once its puts, reads and file-layer reads are independent and
//! every state's interval is within ±5% of its median. The scratch file is removed however the
//! benchmark ends.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};

use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::{self, Issuer};
use hyper_block::scratch::Scratch;
use mantle_chunk::{ChunkError, ChunkKey, Config, ReadBuffers, Reads, Volume};
use mantle_disk::calibrate::{self, Calibration};
use mantle_disk::histogram::Histogram;
use mantle_disk::measure::SplitMix64;
use mantle_disk::rounds::{self, Order, Policy};

use crate::display;
use crate::drive;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Disk(hyper_block::DiskError),
    Chunk(ChunkError),
    Log(String),
    /// A worker thread unwound; its measurements are lost.
    Worker,
    /// The operating system could not start a worker thread; none worked.
    Spawn(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Disk(e) => write!(f, "{e}"),
            Self::Chunk(e) => write!(f, "chunk store: {e}"),
            Self::Log(e) => write!(f, "raft log: {e}"),
            Self::Worker => write!(f, "a benchmark worker stopped unexpectedly"),
            Self::Spawn(e) => write!(f, "starting a benchmark worker: {e}"),
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
    /// Clients: requests in flight in a closed loop, and at most in flight in an open one.
    pub workers: Vec<usize>,
    /// Requests a second in all, arriving open loop; `None` for a closed loop.
    pub rate: Option<f64>,
    /// How long each round of a put or read point runs.
    pub step: Duration,
    pub rounds: Policy,
    /// The scratch volume's segment size; a volume's default when `None`.
    pub segment_size: Option<u64>,
}

impl Plan {
    /// A 4 GiB volume, or a tenth of the free space if that is smaller; chunks from a small
    /// object's 4 KiB to an erasure-coded shard's 8 MiB.
    pub fn standard(available: Option<u64>, step: Duration, rounds: usize) -> Self {
        let cap = 4u64 << 30;
        let volume = available.map_or(cap, |free| cap.min(free / 10));
        Self {
            volume,
            sizes: vec![4 << 10, 64 << 10, 1 << 20, 8 << 20],
            workers: vec![1, 4, 16, 64],
            rate: None,
            step,
            rounds: Policy {
                max: rounds,
                ..Policy::STANDARD
            },
            segment_size: None,
        }
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
    /// The most threads the process ran, as the OS counts them.
    threads: usize,
    generator: drive::Generator,
}

impl Outcome {
    fn ops_per_sec(&self) -> f64 {
        rate(self.ops, self.elapsed)
    }

    fn bytes_per_sec(&self) -> f64 {
        rate(self.bytes, self.elapsed)
    }
}

/// A point's rounds: each round's throughput and transfers, in time order.
#[derive(Debug, Default)]
struct Series {
    ops_per_sec: Vec<f64>,
    latency: Vec<Histogram>,
    full: bool,
    threads: usize,
    generator: drive::Generator,
}

impl Series {
    fn add(&mut self, o: Outcome) {
        self.ops_per_sec.push(o.ops_per_sec());
        self.full |= o.full;
        self.threads = self.threads.max(o.threads);
        self.generator.merge(&o.generator);
        self.latency.push(o.latency);
    }

    /// Enough rounds by `policy`, or none to judge: reads of a point that wrote nothing.
    fn enough(&self, policy: &Policy) -> bool {
        self.ops_per_sec.is_empty() || policy.enough(&rounds::judge(&self.ops_per_sec))
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
    /// Clients; the standard set when empty.
    pub workers: Vec<usize>,
    /// Requests a second in all, arriving open loop; a closed loop when `None`.
    pub rate: Option<f64>,
    /// Rounds each point runs at most.
    pub rounds: usize,
    /// Leave out the device measurement.
    pub skip_device: bool,
    /// The scratch volume's segment size; a volume's default when `None`.
    pub segment_size: Option<usize>,
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
    let mut plan = Plan::standard(id.file_system.available_bytes, options.step, options.rounds);
    if !options.sizes.is_empty() {
        plan.sizes.clone_from(&options.sizes);
    }
    if !options.workers.is_empty() {
        plan.workers.clone_from(&options.workers);
    }
    plan.rate = options.rate;
    plan.segment_size = options.segment_size.and_then(|s| u64::try_from(s).ok());
    writeln!(out, "{}", path.display())?;
    // The volume is formatted and read as mantle would here, which the device measurement
    // decides; without one it is not pre-written and reads one at a time.
    let mut prewrite = false;
    let mut reads = Reads::default();
    let mut measured = None;
    if !options.skip_device {
        writeln!(out, "measuring the device (about 30 s)")?;
        out.flush()?;
        let device = calibrate::calibrate(
            path,
            align,
            id.file_system.available_bytes,
            &calibrate::Plan::standard(align, id.queue_depth),
        )
        .map_err(Error::Disk)?;
        report_device(out, &device)?;
        prewrite = device.first_write_penalty();
        if let (Some(random), Some(sequential)) = (
            device.random_read_saturation(),
            device.sequential_read_saturation(),
        ) {
            let bytes = u64::try_from(sequential.depth)
                .ok()
                .zip(u64::try_from(device.large).ok())
                .and_then(|(d, l)| d.checked_mul(l))
                .unwrap_or(u64::MAX);
            reads = Reads::measured(random.depth, bytes, device.read_gap().unwrap_or(0));
            measured = Some(random.depth);
        }
    }
    let depth = issuer::depth(id.queue_depth, measured);
    run(out, path, align, &plan, prewrite, reads, depth)
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
            "  {} reads   {:>3} in flight ({:.1} achieved): {}/s, {} p50, {} p99",
            display::size(c.small),
            p.depth,
            p.achieved,
            display::count(p.ops_per_sec),
            display::quantile(p.p50_ns),
            display::quantile(p.p99_ns)
        )?;
    }
    writeln!(
        out,
        "  measured with  {} workers, the deepest step, within the device's queue and the \
         process's thread budget",
        c.workers
    )?;
    if c.random_read_capped {
        writeln!(
            out,
            "  still faster at the deepest depth this machine measures: the device saturates deeper"
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
    reads: Reads,
    depth: usize,
) -> Result<(), Error> {
    let scratch = Scratch::create(dir, ".mantle-bench").map_err(Error::Disk)?;
    let path = scratch.path();
    // The device's issuer, which every write of the volume goes through, at the depth where
    // the device's throughput stopped growing (docs/design/node.md §1.2).
    let issuer = Issuer::start(path, depth).map_err(Error::Disk)?;
    let file =
        DeviceFile::open(path, false, CachingRequest::PreferDirect, align).map_err(Error::Disk)?;
    let volume = align.down_u64(plan.volume);
    file.preallocate(volume).map_err(Error::Disk)?;
    // Room in the index for a third of the volume in the smallest chunks, twice over.
    let smallest = plan.sizes.iter().copied().min().unwrap_or(4096).max(1);
    let defaults = Config::default();
    let config = Config {
        max_fragments: volume
            .checked_div(u64::try_from(smallest).unwrap_or(u64::MAX))
            .unwrap_or(0)
            .saturating_mul(2)
            .checked_div(3)
            .unwrap_or(0)
            .max(1024),
        segment_size: plan.segment_size.unwrap_or(defaults.segment_size),
        scrub_period: None,
        prewrite,
        reads,
        ..defaults
    };
    let v = Volume::format(&issuer, file, volume, config).map_err(Error::Chunk)?;
    // Every chunk must fit a segment's record.
    let largest = v.max_payload();
    for &size in &plan.sizes {
        let len = u64::try_from(size).unwrap_or(u64::MAX);
        if len > largest {
            return Err(Error::Chunk(ChunkError::TooLarge { len, max: largest }));
        }
    }
    let segments = v.usage().map_err(Error::Chunk)?.segments;
    let raw =
        DeviceFile::open(path, false, CachingRequest::PreferDirect, align).map_err(Error::Disk)?;
    let span = v.data_span();
    writeln!(
        out,
        "chunk store on a {} volume of {} segments of {} in a scratch file (removed \
         afterwards){}, writing {} at a time, reading {} at a time with {} more let wait, and \
         through at most {} of a record to reach its range",
        display::capacity(volume),
        segments,
        display::size(usize::try_from(config.segment_size).unwrap_or(usize::MAX)),
        if prewrite {
            ", written once at format"
        } else {
            ""
        },
        issuer.depth(),
        reads.depth,
        reads.waiting,
        display::capacity(reads.gap)
    )?;
    out.flush()?;

    // As many writers as the store's queue admits: what it takes in two batches
    // (Limits::queue_bytes), so none is refused with Busy.
    let admitted = |size: usize| {
        config
            .limits
            .queue_bytes()
            .checked_div(u64::try_from(size).unwrap_or(u64::MAX))
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(usize::MAX)
            .clamp(1, config.limits.queue_requests().max(1))
    };
    let fill_size = usize::try_from(largest).map_or(1 << 20, |l| l.min(1 << 20));
    let fill = Load {
        clients: admitted(fill_size),
        rate: None,
        step: Duration::MAX,
        point: 0,
    };
    let (filled, keys) = puts(&v, path, fill_size, fill, volume)?;
    writeln!(
        out,
        "  first pass     {} writing {} chunks into blocks never written before",
        display::rate(filled.bytes_per_sec()),
        display::size(fill_size)
    )?;
    let deleters = config.limits.queue_requests().max(1);
    deletes(&v, path, &keys, deleters)?;

    writeln!(
        out,
        "  each row is the median of a point's rounds, ± the wider side of its 95% interval (–\n  \
         when the rounds are too few or not independent); rounds in two states take a row each"
    )?;
    writeln!(
        out,
        "  {:<14} {:>9} {:>10} {:>12} {:>5} {:>6} {:>10} {:>10} {:>10} {:>8} {:>10}",
        "",
        if plan.rate.is_some() {
            "clients"
        } else {
            "in flight"
        },
        "ops/s",
        "throughput",
        "±",
        "rounds",
        "p50",
        "p99",
        "p99.9",
        "threads",
        "late p99"
    )?;
    let budget = volume / 3;
    let mut point = 1u64;
    for &size in &plan.sizes {
        for &workers in &plan.workers {
            // More writers than the store's queue admits would be refused with Busy.
            let writers = workers.min(admitted(size));
            // More readers than the store holds at the device and lets wait would be refused
            // with Busy; the file layer reads as many at once, for comparison.
            let readers = workers.min(reads.depth.saturating_add(reads.waiting));
            let (mut put, mut get, mut direct, mut range) = (
                Series::default(),
                Series::default(),
                Series::default(),
                Series::default(),
            );
            for _ in 0..plan.rounds.limit() {
                let load = |clients| Load {
                    clients,
                    rate: plan.rate,
                    step: plan.step,
                    point,
                };
                let (outcome, keys) = puts(&v, path, size, load(writers), budget)?;
                put.add(outcome);
                if !keys.is_empty() {
                    get.add(gets(&v, path, &keys, size, size, load(readers))?);
                    direct.add(raw_reads(&raw, &span, size, load(readers))?);
                    if size > smallest {
                        range.add(gets(&v, path, &keys, size, smallest, load(readers))?);
                    }
                }
                deletes(&v, path, &keys, deleters)?;
                point = point.saturating_add(1);
                if [&put, &get, &direct, &range]
                    .iter()
                    .all(|s| s.enough(&plan.rounds))
                {
                    break;
                }
            }
            rows(
                out,
                &format!("put {}", display::size(size)),
                writers,
                size,
                &put,
            )?;
            if !get.ops_per_sec.is_empty() {
                rows(
                    out,
                    &format!("get {}", display::size(size)),
                    readers,
                    size,
                    &get,
                )?;
                rows(out, "  file layer", readers, size, &direct)?;
            }
            if !range.ops_per_sec.is_empty() {
                let name = format!("  {} range", display::size(smallest));
                rows(out, &name, readers, smallest, &range)?;
            }
        }
    }
    // A restart: the volume closed and opened again, which replays its log since the last
    // checkpoint and searches the whole log for a frame past the end it found.
    v.close();
    let file =
        DeviceFile::open(path, false, CachingRequest::PreferDirect, align).map_err(Error::Disk)?;
    let started = Instant::now();
    let (v, report) = Volume::open(&issuer, file, config).map_err(Error::Chunk)?;
    writeln!(
        out,
        "  reopened       in {}, replaying {} index frames of a {} log",
        display::nanos(nanos(started.elapsed())),
        report.frames,
        display::capacity(v.log_bytes())
    )?;
    v.close();
    Ok(())
}

/// A point's rows, one for each state its rounds fall in: the state's median throughput, the
/// wider side of the median's 95% interval, the rounds in the state, and latency quantiles
/// over the state's transfers. The first row says when the rounds are ordered in time.
fn rows(
    out: &mut impl Write,
    name: &str,
    workers: usize,
    size: usize,
    s: &Series,
) -> std::io::Result<()> {
    let late = display::nanos(s.generator.lateness.p99());
    let judged = rounds::judge(&s.ops_per_sec);
    let two = judged.states.len() > 1;
    // Every transfer of these points moves `size` bytes, so bytes follow operations.
    let bytes = f64::from(u32::try_from(size).unwrap_or(u32::MAX));
    for (i, state) in judged.states.iter().enumerate() {
        let mut latency = Histogram::new();
        for h in state.rounds.iter().filter_map(|&r| s.latency.get(r)) {
            latency.merge(h);
        }
        let first = i == 0;
        // Of two states, how large a share of rounds each may hold, at 95%.
        let share = if two {
            let (lo, hi) = rounds::share(state.rounds.len(), judged.rounds);
            format!(
                "  {}–{} of rounds",
                display::percent(lo),
                display::percent(hi)
            )
        } else {
            String::new()
        };
        let order = match judged.order {
            Order::Persistent if first => "  rounds persist in a state",
            Order::Alternating if first => "  rounds alternate",
            _ => "",
        };
        writeln!(
            out,
            "  {:<14} {:>9} {:>10} {:>12} {:>5} {:>6} {:>10} {:>10} {:>10} {:>8} {:>10}{}{}{}",
            if first { name } else { "" },
            workers,
            display::count(state.median),
            display::rate(state.median * bytes),
            state
                .spread()
                .map_or_else(|| "–".to_owned(), display::percent),
            if two {
                format!("{}/{}", state.rounds.len(), judged.rounds)
            } else {
                judged.rounds.to_string()
            },
            display::nanos(latency.p50()),
            display::nanos(latency.p99()),
            display::nanos(latency.p999()),
            s.threads,
            late,
            share,
            order,
            if first && s.full {
                "  (volume full)"
            } else {
                ""
            }
        )?;
    }
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

/// The clients of a point and how they issue: how many, on how many threads, and at what rate
/// in all, `None` for a closed loop.
#[derive(Debug, Clone, Copy)]
struct Load {
    clients: usize,
    rate: Option<f64>,
    step: Duration,
    point: u64,
}

/// A client of the put drivers: the next key it puts, when its operation is meant to start,
/// when its last answer came, and the put it has out.
struct Putter {
    worker: usize,
    n: u64,
    intended: Instant,
    free: Instant,
    schedule: drive::Schedule,
    out: Option<(mantle_chunk::Answer, ChunkKey)>,
}

/// What every put driver of one point reads: each driver's payload among them, made before the
/// step starts so that making it takes none of the step.
struct Puts<'a> {
    v: &'a Volume<DeviceFile>,
    load: Load,
    drivers: usize,
    payloads: &'a [Vec<u8>],
    budget: u64,
    written: &'a AtomicU64,
    full: &'a AtomicBool,
    failed: &'a Mutex<Option<ChunkError>>,
    peak: &'a AtomicUsize,
}

impl Puts<'_> {
    /// Whether puts may go on: the budget not reached, the volume not full, none failed.
    fn open(&self) -> bool {
        self.written.load(Ordering::Relaxed) < self.budget
            && !self.full.load(Ordering::Relaxed)
            && self.failed.lock().is_ok_and(|f| f.is_none())
    }

    fn fail(&self, e: ChunkError) {
        if let Ok(mut f) = self.failed.lock() {
            f.get_or_insert(e);
        }
    }

    /// One driver: the clients `driver`, `driver + drivers`, ... of the point's, each putting
    /// its next chunk when its operation is meant to start and its last is answered, until
    /// `deadline` (none: until the budget or a full volume). Each client's first put goes out
    /// whatever the deadline, so a round measures at least that much however slow the host.
    fn run(
        &self,
        driver: usize,
        deadline: Option<Instant>,
    ) -> (Histogram, Vec<ChunkKey>, drive::Generator) {
        let cpu = drive::cpu();
        let Load {
            clients,
            rate,
            point,
            ..
        } = self.load;
        let timely = || deadline.is_none_or(|d| Instant::now() < d);
        let Some(payload) = self.payloads.get(driver) else {
            return (Histogram::new(), Vec::new(), drive::Generator::default());
        };
        let size64 = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let now = Instant::now();
        let mut putters: Vec<Putter> = (driver..clients)
            .step_by(self.drivers.max(1))
            .map(|worker| {
                let seed = point.rotate_left(13) ^ u64::try_from(worker).unwrap_or(0);
                let mut schedule = drive::Schedule::new(rate, clients, seed);
                Putter {
                    worker,
                    n: 0,
                    intended: schedule.next(now, now),
                    free: now,
                    schedule,
                    out: None,
                }
            })
            .collect();
        // One entry a client: each has at most one put out, so a push never finds it full.
        let (ready, woken) = sync_channel(putters.len());
        let wakers: Vec<_> = (0..putters.len())
            .map(|i| drive::waker(&ready, i))
            .collect();
        drop(ready);
        // The clients with nothing out, by when their next put is meant to start.
        let mut idle: BinaryHeap<Reverse<(Instant, usize)>> = putters
            .iter()
            .enumerate()
            .map(|(i, p)| Reverse((p.intended, i)))
            .collect();
        let (mut latency, mut keys) = (Histogram::new(), Vec::new());
        let mut generator = drive::Generator::default();
        let (mut out, mut sampled) = (0usize, false);
        // The clients whose first put has not gone out.
        let mut first = putters.len();
        loop {
            // Every client whose put is due goes out, until the store says it is busy; past the
            // deadline only a first put does, and a client with one out leaves the round.
            let mut busy = false;
            while self.open() {
                let now = Instant::now();
                let Some(&Reverse((at, i))) = idle.peek().filter(|r| r.0.0 <= now) else {
                    break;
                };
                let (Some(p), Some(waker)) = (putters.get_mut(i), wakers.get(i)) else {
                    break;
                };
                if p.n > 0 && !timely() {
                    idle.pop();
                    continue;
                }
                let k = key(point, p.worker, p.n);
                match self.v.put_waking(k, payload, waker.clone()) {
                    Ok(answer) => {
                        idle.pop();
                        if p.n == 0 {
                            first = first.saturating_sub(1);
                        }
                        p.n = p.n.saturating_add(1);
                        p.out = Some((answer, k));
                        out = out.saturating_add(1);
                        generator
                            .lateness
                            .record(nanos(now.saturating_duration_since(at.max(p.free))));
                    }
                    // The store is cleaning to make room: the put waits for an answer to free
                    // some, and the wait counts in its latency.
                    Err(ChunkError::Busy) => {
                        busy = true;
                        break;
                    }
                    Err(ChunkError::Full) => self.full.store(true, Ordering::Relaxed),
                    Err(e) => self.fail(e),
                }
            }
            if !sampled && (idle.is_empty() || busy) {
                // As many of this driver's clients are out as will be: the process runs the
                // most threads it will.
                self.peak
                    .fetch_max(drive::thread_count(), Ordering::Relaxed);
                sampled = true;
            }
            let going = self.open() && (first > 0 || timely());
            if out == 0 {
                if !going {
                    break;
                }
                if busy {
                    std::thread::yield_now();
                    continue;
                }
            }
            // An answer, or the next put's start, whichever comes first.
            let next = if going && !busy {
                idle.peek()
                    .map(|r| r.0.0.saturating_duration_since(Instant::now()))
            } else {
                None
            };
            let i = match next {
                Some(wait) => match woken.recv_timeout(wait) {
                    Ok(i) => i,
                    Err(_) => continue,
                },
                None => match woken.recv() {
                    Ok(i) => i,
                    Err(_) => break,
                },
            };
            let Some(p) = putters.get_mut(i) else {
                continue;
            };
            let Some((answer, k)) = p.out.take() else {
                continue;
            };
            let Some(result) = answer.poll() else {
                p.out = Some((answer, k));
                continue;
            };
            out = out.saturating_sub(1);
            let now = Instant::now();
            match result {
                Ok(()) => {
                    latency.record(nanos(now.saturating_duration_since(p.intended)));
                    keys.push(k);
                    self.written.fetch_add(size64, Ordering::Relaxed);
                }
                Err(ChunkError::Full) => self.full.store(true, Ordering::Relaxed),
                // Busy at the writer: the chunk was not written.
                Err(ChunkError::Busy) => {}
                Err(e) => self.fail(e),
            }
            p.free = now;
            p.intended = p.schedule.next(p.intended, now);
            idle.push(Reverse((p.intended, i)));
        }
        generator.cpu = drive::cpu().saturating_sub(cpu);
        (latency, keys, generator)
    }
}

/// `load.clients` writers put `size`-byte chunks until `step` passes, `budget` bytes are
/// written, or the volume is full: records on at most a granted core's worth of driver threads.
fn puts(
    v: &Volume<DeviceFile>,
    path: &Path,
    size: usize,
    load: Load,
    budget: u64,
) -> Result<(Outcome, Vec<ChunkKey>), Error> {
    let written = AtomicU64::new(0);
    let full = AtomicBool::new(false);
    let failed: Mutex<Option<ChunkError>> = Mutex::new(None);
    let peak = AtomicUsize::new(0);
    let size64 = u64::try_from(size).unwrap_or(u64::MAX);
    let drivers = drive::drivers(load.clients);
    let payloads: Vec<Vec<u8>> = (0..drivers)
        .map(|driver| {
            let mut payload = vec![0u8; size];
            SplitMix64::new(load.point ^ u64::try_from(driver).unwrap_or(0)).fill(&mut payload);
            payload
        })
        .collect();
    let shared = Puts {
        v,
        load,
        drivers,
        payloads: &payloads,
        budget,
        written: &written,
        full: &full,
        failed: &failed,
        peak: &peak,
    };
    let started = Instant::now();
    // A step too long to add to now has no deadline: the fill's, which ends at its budget.
    let deadline = started.checked_add(load.step);
    let results = std::thread::scope(|scope| {
        drive::start(scope, path, drivers, |driver| {
            let shared = &shared;
            move || shared.run(driver, deadline)
        })
        .map(drive::Started::join)
    })?;
    let elapsed = started.elapsed();
    if let Some(e) = failed.into_inner().ok().flatten() {
        return Err(Error::Chunk(e));
    }
    let mut latency = Histogram::new();
    let mut keys = Vec::new();
    let mut generator = drive::Generator::default();
    for result in results {
        let (h, k, g) = result.ok_or(Error::Worker)?;
        latency.merge(&h);
        keys.extend(k);
        generator.merge(&g);
    }
    let ops = latency.count();
    Ok((
        Outcome {
            ops,
            bytes: ops.saturating_mul(size64),
            elapsed,
            latency,
            full: full.into_inner(),
            threads: peak.into_inner(),
            generator,
        },
        keys,
    ))
}

/// `load.clients` readers, each a blocking thread that reads as its own client, until `step`
/// passes: `init` makes a thread's buffer, and `read` does one read into it, given a source of
/// randomness. Their number is the store's
/// own bound on reads at the device and waiting, which calibration measured, so they are a
/// pool sized by the device, not one thread per client (docs/design/node.md §1.2).
fn readers<S, E: Send>(
    path: &Path,
    load: Load,
    seed: u64,
    init: impl Fn() -> Result<S, E> + Sync,
    read: impl Fn(&mut S, &mut SplitMix64) -> Result<(), E> + Sync,
) -> Result<(Histogram, drive::Generator, usize, Option<E>, Duration), Error> {
    let started = Instant::now();
    let deadline = started.checked_add(load.step);
    let failed: Mutex<Option<E>> = Mutex::new(None);
    let peak = AtomicUsize::new(0);
    let results = std::thread::scope(|scope| {
        drive::start(scope, path, load.clients, |worker| {
            let (failed, peak, init, read) = (&failed, &peak, &init, &read);
            move || {
                let cpu = drive::cpu();
                let mut latency = Histogram::new();
                let mut generator = drive::Generator::default();
                let mut buf = match init() {
                    Ok(buf) => buf,
                    Err(e) => {
                        if let Ok(mut f) = failed.lock() {
                            f.get_or_insert(e);
                        }
                        return (latency, generator);
                    }
                };
                let mut rng = SplitMix64::new(seed ^ u64::try_from(worker).unwrap_or(0));
                let mut schedule = drive::Schedule::new(load.rate, load.clients, rng.next_u64());
                let now = Instant::now();
                let (mut intended, mut free) = (schedule.next(now, now), now);
                peak.fetch_max(drive::thread_count(), Ordering::Relaxed);
                // The first read goes out whatever the deadline, so a round measures at least
                // that much however slow the host.
                let mut read_once = false;
                while (!read_once || deadline.is_some_and(|d| Instant::now() < d))
                    && failed.lock().is_ok_and(|f| f.is_none())
                {
                    let now = Instant::now();
                    if intended > now {
                        // Waits for the read's arrival; an early return looks again.
                        std::thread::park_timeout(intended.saturating_duration_since(now));
                        continue;
                    }
                    generator
                        .lateness
                        .record(nanos(now.saturating_duration_since(intended.max(free))));
                    read_once = true;
                    match read(&mut buf, &mut rng) {
                        Ok(()) => {
                            free = Instant::now();
                            latency.record(nanos(free.saturating_duration_since(intended)));
                        }
                        Err(e) => {
                            if let Ok(mut f) = failed.lock() {
                                f.get_or_insert(e);
                            }
                            free = Instant::now();
                        }
                    }
                    intended = schedule.next(intended, free);
                }
                generator.cpu = drive::cpu().saturating_sub(cpu);
                (latency, generator)
            }
        })
        .map(drive::Started::join)
    })?;
    let elapsed = started.elapsed();
    let mut latency = Histogram::new();
    let mut generator = drive::Generator::default();
    for result in results {
        let (h, g) = result.ok_or(Error::Worker)?;
        latency.merge(&h);
        generator.merge(&g);
    }
    let failed = failed.into_inner().ok().flatten();
    Ok((latency, generator, peak.into_inner(), failed, elapsed))
}

/// Readers read `len` bytes of the `size`-byte chunks of `keys`, the chunk chosen at random
/// and the range at a random multiple of `len` within it, until `step` passes.
fn gets(
    v: &Volume<DeviceFile>,
    path: &Path,
    keys: &[ChunkKey],
    size: usize,
    len: usize,
    load: Load,
) -> Result<Outcome, Error> {
    let len64 = u64::try_from(len).unwrap_or(u64::MAX);
    let places = u64::try_from(size)
        .unwrap_or(0)
        .checked_div(len64)
        .unwrap_or(0)
        .max(1);
    let count = u64::try_from(keys.len()).unwrap_or(u64::MAX);
    let (latency, generator, threads, failed, elapsed) = readers(
        path,
        load,
        !load.point,
        // Each reader's output and its own read buffers.
        || Ok((Vec::new(), v.read_buffers())),
        |(buf, buffers): &mut (Vec<u8>, ReadBuffers), rng: &mut SplitMix64| {
            // In range: `pick < count`, the keys' number.
            let pick = usize::try_from(rng.below(count)).unwrap_or(0);
            let k = keys
                .get(pick)
                .ok_or(ChunkError::Internal("a key past the keys"))?;
            let from = rng.below(places).saturating_mul(len64);
            v.read_into(k, from, len64, buf, buffers)
        },
    )?;
    if let Some(e) = failed {
        return Err(Error::Chunk(e));
    }
    let ops = latency.count();
    Ok(Outcome {
        ops,
        bytes: ops.saturating_mul(len64),
        elapsed,
        latency,
        full: false,
        threads,
        generator,
    })
}

/// Readers read `size` bytes, rounded up to the file's alignment, at random aligned offsets in
/// `span` through the file layer, until `step` passes.
fn raw_reads(
    file: &DeviceFile,
    span: &std::ops::Range<u64>,
    size: usize,
    load: Load,
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
    let (latency, generator, threads, failed, elapsed) = readers(
        file.path(),
        load,
        load.point.rotate_left(17),
        || {
            let mut buf = AlignedBuf::zeroed(len, align)?;
            buf.set_len(len)?;
            Ok(buf)
        },
        |buf: &mut AlignedBuf, rng: &mut SplitMix64| {
            let at = span
                .start
                .saturating_add(rng.below(slots).saturating_mul(block));
            file.read_exact_at(buf.as_mut_slice(), at)
        },
    )?;
    if let Some(e) = failed {
        return Err(Error::Disk(e));
    }
    let ops = latency.count();
    Ok(Outcome {
        ops,
        bytes: ops.saturating_mul(u64::try_from(size).unwrap_or(u64::MAX)),
        elapsed,
        latency,
        full: false,
        threads,
        generator,
    })
}

/// Deletes `keys`, `clients` at a time, so that they share flushes: records on at most a
/// granted core's worth of driver threads.
fn deletes(
    v: &Volume<DeviceFile>,
    path: &Path,
    keys: &[ChunkKey],
    clients: usize,
) -> Result<(), Error> {
    let drivers = drive::drivers(clients);
    let per = keys.len().div_ceil(drivers).max(1);
    let parts: Vec<&[ChunkKey]> = keys.chunks(per).collect();
    let results = std::thread::scope(|scope| {
        drive::start(scope, path, parts.len(), |i| {
            let part = parts.get(i).copied().unwrap_or_default();
            let most = clients.div_ceil(drivers).max(1);
            move || delete_part(v, part, most)
        })
        .map(drive::Started::join)
    })?;
    for result in results {
        result.ok_or(Error::Worker)?.map_err(Error::Chunk)?;
    }
    Ok(())
}

/// Deletes `part` with at most `most` deletes out at once, waiting for an answer when the
/// store is busy.
fn delete_part(v: &Volume<DeviceFile>, part: &[ChunkKey], most: usize) -> Result<(), ChunkError> {
    let (ready, woken) = sync_channel(most);
    let wakers: Vec<_> = (0..most).map(|i| drive::waker(&ready, i)).collect();
    drop(ready);
    let mut slots: Vec<Option<mantle_chunk::Answer>> = (0..most).map(|_| None).collect();
    let mut free: Vec<usize> = (0..most).collect();
    let mut keys = part.iter();
    let mut next = keys.next();
    let mut out = 0usize;
    while next.is_some() || out > 0 {
        while let (Some(k), Some(&slot)) = (next, free.last()) {
            let Some(waker) = wakers.get(slot) else {
                break;
            };
            match v.delete_waking(*k, waker.clone()) {
                Ok(answer) => {
                    free.pop();
                    if let Some(s) = slots.get_mut(slot) {
                        *s = Some(answer);
                    }
                    out = out.saturating_add(1);
                    next = keys.next();
                }
                Err(ChunkError::Busy) if out > 0 => break,
                Err(ChunkError::Busy) => std::thread::yield_now(),
                Err(e) => return Err(e),
            }
        }
        if out == 0 {
            continue;
        }
        let Ok(slot) = woken.recv() else {
            return Err(ChunkError::Closed);
        };
        let Some(answer) = slots.get_mut(slot).and_then(Option::take) else {
            continue;
        };
        match answer.poll() {
            None => {
                if let Some(s) = slots.get_mut(slot) {
                    *s = Some(answer);
                }
            }
            Some(result) => {
                out = out.saturating_sub(1);
                free.push(slot);
                result?;
            }
        }
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
            rate: None,
            step: Duration::from_millis(50),
            rounds: Policy {
                min: 2,
                max: 2,
                precision: 0.05,
            },
            segment_size: None,
        };
        let mut out = Vec::new();
        // As a device measured at four in flight: four readers, none refused.
        run(
            &mut out,
            dir.path(),
            align,
            &plan,
            false,
            Reads::measured(4, 4 << 20, 0),
            4,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        for name in [
            "writing 4 at a time, reading 4 at a time with 4 more let wait",
            "first pass",
            "put 4 KiB",
            "get 4 KiB",
            "put 1 MiB",
            "get 1 MiB",
            "file layer",
            "4 KiB range",
            "reopened",
        ] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "scratch volume left behind"
        );
    }

    /// Do: the small benchmark with a step of zero, on a volume of a few small segments.
    /// Expect: the first pass writes, every put and get row is there and measures something,
    /// and the scratch volume is gone: a round measures each client's first operation however
    /// short its step, as on a host too slow to finish any operation within one.
    #[test]
    fn a_step_shorter_than_any_operation_still_measures_each_clients_first() {
        let dir = tempfile::tempdir().unwrap();
        // The log takes its least, 64 MiB; three segments follow.
        let plan = Plan {
            volume: 128 << 20,
            sizes: vec![4 << 10, 1 << 20],
            workers: vec![1, 4],
            rate: None,
            step: Duration::ZERO,
            rounds: Policy {
                min: 2,
                max: 2,
                precision: 0.05,
            },
            segment_size: Some(16 << 20),
        };
        let mut out = Vec::new();
        run(
            &mut out,
            dir.path(),
            Alignment::new(4096).unwrap(),
            &plan,
            false,
            Reads::measured(4, 4 << 20, 0),
            4,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        for name in [
            "first pass",
            "put 4 KiB",
            "get 4 KiB",
            "put 1 MiB",
            "get 1 MiB",
        ] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
        for line in text.lines().filter(|l| {
            l.starts_with("  first pass") || l.starts_with("  put ") || l.starts_with("  get ")
        }) {
            assert!(
                !line.contains(" 0 B/s"),
                "a row measured nothing: {line}\n{text}"
            );
        }
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "scratch volume left behind"
        );
    }

    /// Clients are records: puts at ten times a driver a core's clients run as many threads, as
    /// the OS counts them, as at one client a core, and in open loop as in closed. Run in a
    /// process of its own, so that no other test's threads are counted.
    #[test]
    fn clients_cost_no_threads() {
        const ALONE: &str = "MANTLE_TEST_THREADS_ALONE";
        if std::env::var_os(ALONE).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "bench::tests::clients_cost_no_threads",
                    "--test-threads=1",
                    "--nocapture",
                ])
                .env(ALONE, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let align = Alignment::new(4096).unwrap();
        let path = dir.path().join("volume");
        let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align).unwrap();
        let size = 1u64 << 30;
        file.preallocate(size).unwrap();
        // As `run` sizes its index: a third of the volume in 4 KiB chunks, twice over.
        let config = Config {
            max_fragments: size / 4096 * 2 / 3,
            scrub_period: None,
            ..Config::default()
        };
        // As a device measured at four in flight.
        let issuer = Issuer::start(&path, 4).unwrap();
        let v = Volume::format(&issuer, file, size, config).unwrap();
        let cores = drive::drivers(usize::MAX);
        let mut seen = Vec::new();
        for (point, (clients, rate)) in [
            (cores, None),
            (10 * cores, None),
            (10 * cores, Some(20_000.0)),
        ]
        .into_iter()
        .enumerate()
        {
            let load = Load {
                clients,
                rate,
                step: Duration::from_millis(200),
                point: point as u64 + 1,
            };
            let (outcome, keys) = puts(&v, &path, 4096, load, size / 4).unwrap();
            assert!(outcome.ops > 0, "{clients} clients wrote nothing");
            eprintln!(
                "{clients} clients, rate {rate:?}: {} puts, {} threads, late p99 {} ns",
                outcome.ops,
                outcome.threads,
                outcome.generator.lateness.p99()
            );
            seen.push(outcome.threads);
            deletes(&v, &path, &keys, config.limits.queue_requests()).unwrap();
        }
        assert!(seen.iter().all(|&t| t == seen[0]), "threads {seen:?}");
        v.close();
    }
}
