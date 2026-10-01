//! `mantle bench log`: the Raft log's appends per second and their latency on a device, next
//! to what one durable write costs there (docs/design/raft-log.md §8).
//!
//! A log is created in a scratch file on the device and driven by closed-loop replicas: each
//! appends one entry to its own group, waits until the log has made it durable, and appends
//! the next, as a replica does with each `Ready`. A replica is a record, not a thread: driver
//! threads, at most one a granted core, each hold their share of the replicas, submit for each
//! whose answer came, and hear of each answer through the replica's waker, so the process runs
//! as many threads at a thousand replicas as at ten (docs/design/measurement.md §10). Each row
//! says the threads the process ran, as the OS counts them, beside the threads it ran idle just
//! before the point began: what the operating system runs in every process of its own (Windows'
//! loader starts a pool of workers in each) is in both, so the point's own threads are their
//! difference. It also says the drivers' CPU time. Every replica compacts behind itself, so
//! the log frees and reclaims segments as a running node's does. For each entry size and
//! number of replicas the replicas append for the step's duration. Besides throughput and
//! latency, each row says how many appends one flush carried: group commit is what lets
//! many replicas share one device's flushes (06 §C.c). Each point ends with a restart: the
//! log reopened, timed, and every replica's kept entries read back from the file, as a
//! leader reads what it sends a follower that lags. The scratch file is removed however the
//! benchmark ends.

use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Class, Config, Entries, Entry, Log, LogError, Pending, Start, Update, Waits};
use mantle_disk::calibrate;
use mantle_disk::histogram::Histogram;

use crate::bench::{Error, nanos, rate, report_device};
use crate::display;
use crate::drive;
use hyper_block::scratch::Scratch;

/// Entries a replica keeps behind its last before it compacts, as its engine keeps a
/// window for followers that lag.
const KEEP: u64 = 64;

/// What `mantle bench log` was asked to run.
#[derive(Debug, Clone)]
pub struct Options {
    pub step: Duration,
    /// Entry sizes, in bytes: 128 B, 1 KiB and 16 KiB by default.
    pub sizes: Vec<usize>,
    /// Replicas appending at once: 1, 4, 16, 64 and 256 by default.
    pub replicas: Vec<usize>,
    pub skip_device: bool,
}

pub fn log(out: &mut impl Write, path: &Path, options: &Options) -> Result<(), Error> {
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
    let sizes = if options.sizes.is_empty() {
        vec![128, 1 << 10, 16 << 10]
    } else {
        options.sizes.clone()
    };
    let replicas = if options.replicas.is_empty() {
        vec![1, 4, 16, 64, 256]
    } else {
        options.replicas.clone()
    };
    writeln!(out, "{}", path.display())?;
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
    }
    let most = replicas.iter().copied().max().unwrap_or(1).max(1);
    writeln!(
        out,
        "raft log in a scratch file (removed afterwards), one entry an append"
    )?;
    writeln!(
        out,
        "  {:<12} {:>9} {:>8} {:>5} {:>8} {:>11} {:>12} {:>10} {:>10} {:>10} {:>11} {:>10} {:>20}",
        "",
        "replicas",
        "threads",
        "idle",
        "cpu",
        "appends/s",
        "throughput",
        "p50",
        "p99",
        "p99.9",
        "per flush",
        "reopen",
        "read back"
    )?;
    let mut point = 0u64;
    for &size in &sizes {
        for &count in &replicas {
            point = point.saturating_add(1);
            let scratch = Scratch::create(path, ".mantle-bench-log").map_err(Error::Disk)?;
            let file = DeviceFile::open(scratch.path(), false, CachingRequest::PreferDirect, align)
                .map_err(Error::Disk)?;
            let config = Config {
                segment_bytes: 16 << 20,
                max_segments: 64,
                max_groups: most,
                group_entries: 1 << 20,
                group_bytes: 1 << 30,
                group_cache: 1 << 16,
                queue_submissions: most.saturating_mul(2),
                waits: Waits::Measured,
            };
            let idle = drive::thread_count();
            let log = Log::create(file, config, u128::from(point)).map_err(log_error)?;
            let outcome = appends(&log, scratch.path(), size, count, options.step)?;
            let (frames, updates) = log.flushed();
            let per_flush = if frames == 0 {
                0.0
            } else {
                // Counts of a bounded run are far below 2^53.
                updates as f64 / frames as f64
            };
            drop(log);
            let file = DeviceFile::open(scratch.path(), false, CachingRequest::PreferDirect, align)
                .map_err(Error::Disk)?;
            let started = Instant::now();
            let (log, _) = Log::open(file, config, u128::from(point)).map_err(log_error)?;
            let reopen = started.elapsed();
            let started = Instant::now();
            let read = read_back(&log, count)?;
            let read_back = started.elapsed();
            writeln!(
                out,
                "  {:<12} {:>9} {:>8} {:>5} {:>8} {:>11} {:>12} {:>10} {:>10} {:>10} {:>11.1} {:>10} {:>20}",
                format!("append {}", display::size(size)),
                count,
                outcome.threads,
                idle,
                display::nanos(nanos(outcome.cpu)),
                display::count(rate(outcome.appends, outcome.elapsed)),
                display::rate(rate(outcome.bytes, outcome.elapsed)),
                display::nanos(outcome.latency.p50()),
                display::nanos(outcome.latency.p99()),
                display::nanos(outcome.latency.p999()),
                per_flush,
                display::nanos(nanos(reopen)),
                format!("{} in {}", read, display::nanos(nanos(read_back)))
            )?;
            out.flush()?;
            drop(log);
        }
    }
    Ok(())
}

fn log_error(e: LogError) -> Error {
    Error::Log(e.to_string())
}

/// Reads back from the file every entry the `count` replicas' groups keep; returns how many.
fn read_back(log: &Log<DeviceFile>, count: usize) -> Result<u64, Error> {
    let mut read = 0u64;
    for replica in 0..count {
        let group = u128::from(u64::try_from(replica).unwrap_or(u64::MAX));
        let Some(view) = log.view(group).map_err(log_error)? else {
            continue;
        };
        let (Some(first), Some(high)) = (view.start.index.checked_add(1), view.last.checked_add(1))
        else {
            continue;
        };
        if first < high {
            let entries = log
                .entries(group, first, high, u64::MAX)
                .map_err(log_error)?;
            read = read.saturating_add(u64::try_from(entries.len()).unwrap_or(u64::MAX));
        }
    }
    Ok(read)
}

struct Outcome {
    appends: u64,
    bytes: u64,
    elapsed: Duration,
    latency: Histogram,
    /// The most threads the process ran, sampled as each driver had all its replicas out.
    threads: usize,
    /// The drivers' CPU time together.
    cpu: Duration,
}

/// One replica: its group, the index it appended last, and the append it has out.
struct Replica {
    group: u128,
    last: u64,
    started: Instant,
    pending: Option<Pending>,
}

/// `count` closed-loop replicas append `size`-byte entries, each to its own group, until
/// `step` passes, on at most a granted core's worth of driver threads.
fn appends(
    log: &Log<DeviceFile>,
    path: &Path,
    size: usize,
    count: usize,
    step: Duration,
) -> Result<Outcome, Error> {
    let started = Instant::now();
    let deadline = started.checked_add(step);
    let failed: Mutex<Option<String>> = Mutex::new(None);
    let peak = AtomicUsize::new(0);
    let payload = vec![0x5a; size];
    let drivers = drive::drivers(count);
    let shared = Drivers {
        log,
        payload: &payload,
        drivers,
        count,
        deadline,
        failed: &failed,
        peak: &peak,
    };
    let results = std::thread::scope(|scope| {
        drive::start(scope, path, drivers, |driver| {
            let shared = &shared;
            move || shared.run(driver)
        })
        .map(drive::Started::join)
    })?;
    let elapsed = started.elapsed();
    if let Some(e) = failed.into_inner().ok().flatten() {
        return Err(Error::Log(e));
    }
    let mut latency = Histogram::new();
    let mut cpu = Duration::ZERO;
    for result in results {
        let (h, used) = result.ok_or(Error::Worker)?;
        latency.merge(&h);
        cpu = cpu.saturating_add(used);
    }
    let appends = latency.count();
    Ok(Outcome {
        appends,
        bytes: appends.saturating_mul(u64::try_from(size).unwrap_or(u64::MAX)),
        elapsed,
        latency,
        threads: peak.into_inner(),
        cpu,
    })
}

/// What every driver of one point reads.
struct Drivers<'a> {
    log: &'a Log<DeviceFile>,
    payload: &'a [u8],
    drivers: usize,
    count: usize,
    deadline: Option<Instant>,
    failed: &'a Mutex<Option<String>>,
    peak: &'a AtomicUsize,
}

impl Drivers<'_> {
    /// One driver: the replicas `driver`, `driver + drivers`, ... of `count`, each submitting
    /// its next append when its last is answered. Returns their latencies and the driver's
    /// CPU time.
    fn run(&self, driver: usize) -> (Histogram, Duration) {
        let Self {
            log,
            payload,
            drivers,
            count,
            deadline,
            failed,
            peak,
        } = *self;
        let cpu = drive::cpu();
        let mut replicas: Vec<Replica> = (driver..count)
            .step_by(drivers.max(1))
            .map(|r| Replica {
                group: u128::from(u64::try_from(r).unwrap_or(u64::MAX)),
                last: 0,
                started: Instant::now(),
                pending: None,
            })
            .collect();
        // One entry a replica: each has at most one append out, so a push never finds it full.
        let (ready, woken) = sync_channel(replicas.len());
        let wakers: Vec<_> = (0..replicas.len())
            .map(|i| drive::waker(&ready, i))
            .collect();
        drop(ready);
        let fail = |e: LogError| {
            if let Ok(mut f) = failed.lock() {
                f.get_or_insert(e.to_string());
            }
        };
        let going = || {
            deadline.is_some_and(|d| Instant::now() < d) && failed.lock().is_ok_and(|f| f.is_none())
        };
        let submit = |replica: &mut Replica, waker: &std::task::Waker| -> Result<(), LogError> {
            let next = replica.last.checked_add(1).ok_or(LogError::Busy)?;
            let mut update = Update {
                entries: Some(Entries {
                    first: next,
                    // The log takes the entry's bytes and keeps them while the entry is recent.
                    entries: vec![Entry {
                        term: 1,
                        bytes: payload.to_vec(),
                    }],
                }),
                ..Update::default()
            };
            if next > KEEP && next % KEEP == 0 {
                update.start = Some(Start {
                    index: next.saturating_sub(KEEP),
                    term: 1,
                });
            }
            replica.started = Instant::now();
            replica.pending =
                Some(log.submit_waking(replica.group, Class::Normal, update, waker.clone())?);
            Ok(())
        };
        let mut latency = Histogram::new();
        let mut out = 0usize;
        for (replica, waker) in replicas.iter_mut().zip(&wakers) {
            if !going() {
                break;
            }
            match submit(replica, waker) {
                Ok(()) => out = out.saturating_add(1),
                Err(e) => fail(e),
            }
        }
        // Every replica of this driver is out: the process runs the most threads it will.
        peak.fetch_max(drive::thread_count(), Ordering::Relaxed);
        // Each append out wakes its replica exactly once, answered or not, so this ends.
        while out > 0 {
            let Ok(i) = woken.recv() else {
                break;
            };
            let (Some(replica), Some(waker)) = (replicas.get_mut(i), wakers.get(i)) else {
                continue;
            };
            let Some(pending) = replica.pending.take() else {
                continue;
            };
            let Some(answer) = pending.poll() else {
                replica.pending = Some(pending);
                continue;
            };
            out = out.saturating_sub(1);
            match answer {
                Ok(()) => {
                    latency.record(nanos(replica.started.elapsed()));
                    replica.last = replica.last.saturating_add(1);
                    if going() {
                        match submit(replica, waker) {
                            Ok(()) => out = out.saturating_add(1),
                            Err(e) => fail(e),
                        }
                    }
                }
                Err(e) => fail(e),
            }
        }
        (latency, drive::cpu().saturating_sub(cpu))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small run reaches every step's answer, so a change the benchmark no longer fits is
    /// caught where the gates run it.
    #[test]
    fn a_small_run_reports() {
        let dir = tempfile::tempdir().unwrap();
        let mut out = Vec::new();
        log(
            &mut out,
            dir.path(),
            &Options {
                step: Duration::from_millis(20),
                sizes: vec![128],
                replicas: vec![1, 4],
                skip_device: true,
            },
        )
        .unwrap();
        assert!(!out.is_empty());
    }
}
