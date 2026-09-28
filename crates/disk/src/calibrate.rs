//! Measuring what a device actually does, through the path mantle will use.
//!
//! The OS's description of a device is a claim (a virtio disk on flash reports itself
//! rotational; docs/research notes and the Linux probe test show it). Calibration writes a
//! scratch file under the data directory and measures random small reads and sequential large
//! transfers across queue depths, and the latency of a durable write, with the same direct-I/O
//! file layer the store uses. Runs on a loaded machine vary by 2-4x (docs/measurements), so
//! every point is measured in several rounds and the median round is reported.
//!
//! The scratch file's size is bounded by the plan and by a tenth of the free space, and it is
//! removed however calibration ends.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::DiskError;
use crate::buf::Alignment;
use crate::file::{Caching, CachingRequest, DeviceFile};
use crate::measure::{self, Job, JobResult, Pattern};

/// What to measure and how hard.
#[derive(Debug, Clone)]
pub struct Plan {
    /// Bytes of scratch file the jobs touch.
    pub span: u64,
    /// Rounds per point; the median round is reported.
    pub rounds: usize,
    /// Wall-clock budget of one job.
    pub step: Duration,
    /// Queue depths for random reads.
    pub random_depths: Vec<usize>,
    /// Queue depths for sequential transfers.
    pub sequential_depths: Vec<usize>,
    /// Small transfer: the alignment unit, 4 KiB on flash (Haas and Leis, PVLDB 2023, §2.2).
    pub small: usize,
    /// Large transfer.
    pub large: usize,
    /// Durable small writes measured (each one write plus a full flush).
    pub durable_writes: u64,
    /// Bytes of each durable sequential write: written, then flushed, one at a time. The
    /// chunk store's largest group commit, so the rate is the ceiling for batched writes.
    pub durable_large: usize,
}

impl Plan {
    /// About twenty seconds on a fast SSD: three rounds of half-second jobs.
    pub fn standard(align: Alignment) -> Self {
        let small = align
            .max(Alignment::new(4096).unwrap_or(Alignment::BYTE))
            .get();
        Self {
            span: 256 << 20,
            rounds: 3,
            step: Duration::from_millis(500),
            random_depths: vec![1, 4, 16, 64],
            sequential_depths: vec![1, 4],
            small,
            large: 1 << 20,
            durable_writes: 64,
            durable_large: 32 << 20,
        }
    }
}

/// One measured operating point: the median of the plan's rounds by throughput.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub depth: usize,
    pub ops_per_sec: f64,
    pub bytes_per_sec: f64,
    pub p50_ns: u64,
    pub p99_ns: u64,
}

#[derive(Debug, Clone)]
pub struct Calibration {
    pub caching: Caching,
    pub small: usize,
    pub large: usize,
    pub random_read: Vec<Point>,
    pub sequential_read: Vec<Point>,
    pub sequential_write: Vec<Point>,
    /// A small write followed by the platform's full flush, one at a time.
    pub durable_write: Point,
    /// `durable_large` bytes written sequentially, then a full flush, one at a time.
    pub durable_sequential: Point,
    pub durable_large: usize,
    pub elapsed: Duration,
}

impl Calibration {
    /// The smallest measured random-read depth that reaches 90% of the best throughput.
    /// Past the knee a deeper queue buys no throughput and only waits longer (Little's law,
    /// L = λW: with λ saturated, raising L raises W); the 10% is the margin for run-to-run
    /// noise.
    pub fn random_read_knee(&self) -> Option<Point> {
        knee(&self.random_read)
    }
}

fn knee(points: &[Point]) -> Option<Point> {
    let best = points.iter().map(|p| p.ops_per_sec).fold(0.0f64, f64::max);
    points.iter().find(|p| p.ops_per_sec >= best * 0.9).copied()
}

/// Removes the scratch file whatever happens to the calibration.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        // Nothing to report to: the file is ours and absent is the goal.
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Measures the device under `dir`. `available` is the free space the caller knows of; the
/// scratch file never exceeds a tenth of it.
pub fn calibrate(
    dir: &Path,
    align: Alignment,
    available: Option<u64>,
    plan: &Plan,
) -> Result<Calibration, DiskError> {
    let started = Instant::now();
    let span = match available {
        Some(free) => plan.span.min(free / 10),
        None => plan.span,
    };
    let span = align.down_u64(span);
    let needed = u64::try_from(plan.large.max(plan.small)).unwrap_or(u64::MAX);
    if span < needed {
        return Err(DiskError::Io {
            op: "calibrate",
            path: dir.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "too little free space for a calibration file",
            ),
        });
    }
    let path = dir.join(format!(".mantle-calibrate-{}", std::process::id()));
    let _scratch = Scratch(path.clone());
    let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align)?;
    file.preallocate(span)?;

    // Write the whole span first, so reads measure the medium rather than holes, and so
    // later writes overwrite written blocks (docs/measurements/2026-09-28).
    let fill = Job {
        pattern: Pattern::SequentialWrite,
        block: plan.large,
        depth: 1,
        span,
        budget: Duration::from_secs(600),
        max_ops: span
            .checked_div(u64::try_from(plan.large).unwrap_or(u64::MAX))
            .unwrap_or(0),
        sync_each: false,
        seed: 1,
    };
    measure::run(&file, &fill)?;
    file.sync_data()?;

    let job = |pattern, block, depth, seed| Job {
        pattern,
        block,
        depth,
        span,
        budget: plan.step,
        max_ops: u64::MAX,
        sync_each: false,
        seed,
    };
    let mut random_read = Vec::with_capacity(plan.random_depths.len());
    for &depth in &plan.random_depths {
        random_read.push(median(&file, plan.rounds, |round| {
            job(Pattern::RandomRead, plan.small, depth, round)
        })?);
    }
    let mut sequential_read = Vec::with_capacity(plan.sequential_depths.len());
    let mut sequential_write = Vec::with_capacity(plan.sequential_depths.len());
    for &depth in &plan.sequential_depths {
        sequential_read.push(median(&file, plan.rounds, |round| {
            job(Pattern::SequentialRead, plan.large, depth, round)
        })?);
        sequential_write.push(median(&file, plan.rounds, |round| {
            job(Pattern::SequentialWrite, plan.large, depth, round)
        })?);
    }
    let durable_write = median(&file, plan.rounds, |round| Job {
        pattern: Pattern::RandomWrite,
        block: plan.small,
        depth: 1,
        span,
        budget: plan.step.saturating_mul(4),
        max_ops: plan.durable_writes,
        sync_each: true,
        seed: round,
    })?;
    // No larger than the scratch file, and whole alignment units.
    let durable_large = usize::try_from(span)
        .map_or(plan.durable_large, |s| plan.durable_large.min(s))
        .checked_div(plan.small)
        .unwrap_or(0)
        .saturating_mul(plan.small)
        .max(plan.small);
    let durable_sequential = median(&file, plan.rounds, |round| Job {
        pattern: Pattern::SequentialWrite,
        block: durable_large,
        depth: 1,
        span,
        budget: plan.step.saturating_mul(4),
        max_ops: u64::MAX,
        sync_each: true,
        seed: round,
    })?;

    Ok(Calibration {
        caching: file.caching(),
        small: plan.small,
        large: plan.large,
        random_read,
        sequential_read,
        sequential_write,
        durable_write,
        durable_sequential,
        durable_large,
        elapsed: started.elapsed(),
    })
}

/// Runs a job `rounds` times and returns the round with the median throughput.
fn median(file: &DeviceFile, rounds: usize, job: impl Fn(u64) -> Job) -> Result<Point, DiskError> {
    let mut results: Vec<(Job, JobResult)> = Vec::with_capacity(rounds.max(1));
    for round in 0..rounds.max(1) {
        let j = job(u64::try_from(round).unwrap_or(0));
        let r = measure::run(file, &j)?;
        results.push((j, r));
    }
    results.sort_by(|a, b| a.1.ops_per_sec().total_cmp(&b.1.ops_per_sec()));
    let middle = results.len().checked_div(2).unwrap_or(0);
    let (j, r) = results.get(middle).ok_or_else(|| DiskError::Io {
        op: "calibrate",
        path: file.path().to_path_buf(),
        source: std::io::Error::other("no rounds ran"),
    })?;
    Ok(Point {
        depth: j.depth,
        ops_per_sec: r.ops_per_sec(),
        bytes_per_sec: r.bytes_per_sec(),
        p50_ns: r.latency.p50(),
        p99_ns: r.latency.p99(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(depth: usize, ops: f64) -> Point {
        Point {
            depth,
            ops_per_sec: ops,
            bytes_per_sec: 0.0,
            p50_ns: 0,
            p99_ns: 0,
        }
    }

    #[test]
    fn the_knee_is_the_first_depth_within_ten_percent_of_the_best() {
        let points = [
            point(1, 15_000.0),
            point(4, 60_000.0),
            point(16, 190_000.0),
            point(64, 205_000.0),
        ];
        assert_eq!(knee(&points).unwrap().depth, 16);
        assert_eq!(knee(&[]), None);
    }

    #[test]
    fn a_small_calibration_runs_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let align = Alignment::new(4096).unwrap();
        let plan = Plan {
            span: 8 << 20,
            rounds: 3,
            step: Duration::from_millis(20),
            random_depths: vec![1, 2],
            sequential_depths: vec![1],
            small: 4096,
            large: 1 << 20,
            durable_writes: 4,
            durable_large: 2 << 20,
        };
        let c = calibrate(dir.path(), align, None, &plan).unwrap();
        assert_eq!(c.random_read.len(), 2);
        assert!(c.random_read.iter().all(|p| p.ops_per_sec > 0.0));
        assert!(c.durable_write.ops_per_sec > 0.0);
        assert!(c.durable_sequential.bytes_per_sec > 0.0);
        assert_eq!(c.durable_large, 2 << 20);
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(leftovers.is_empty(), "scratch file left behind");
    }

    #[test]
    fn too_little_space_is_refused_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let align = Alignment::new(4096).unwrap();
        let plan = Plan::standard(align);
        let err = calibrate(dir.path(), align, Some(1 << 20), &plan).unwrap_err();
        assert!(matches!(
            err,
            DiskError::Io {
                op: "calibrate",
                ..
            }
        ));
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
