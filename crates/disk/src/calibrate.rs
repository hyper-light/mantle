//! Measuring what a device actually does, through the path mantle will use.
//!
//! The OS's description of a device is a claim (a virtio disk on flash reports itself
//! rotational; docs/research notes and the Linux probe test show it). Calibration writes a
//! scratch file under the data directory and measures random small reads and sequential large
//! transfers across queue depths, the latency of a durable write, and whether a durable write
//! into space the file system has allocated but never written costs more than one over written
//! space, with the same direct-I/O file layer the store uses. Runs on a loaded machine vary by 2-4x (docs/measurements), so
//! every point is measured in rounds until the 95% confidence interval of its throughput is
//! within the plan's precision of the mean, or the plan's rounds run out, as Georges, Buytaert
//! and Eeckhout's JavaStats stops (OOPSLA 2007, §3.3; docs/research/11 §13). Latency quantiles
//! come from every round's transfers together, and each is reported only when there are
//! enough transfers to estimate it.
//!
//! The scratch file's size is bounded by the plan and by a tenth of the free space, and it is
//! removed however calibration ends.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::DiskError;
use crate::buf::Alignment;
use crate::file::{Caching, CachingRequest, DeviceFile};
use crate::measure::{self, Job, Pattern};
use crate::scratch::Scratch;

/// What to measure and how hard.
#[derive(Debug, Clone)]
pub struct Plan {
    /// Bytes of scratch file the jobs touch.
    pub span: u64,
    /// Rounds per point.
    pub rounds: Rounds,
    /// Wall-clock budget of one job.
    pub step: Duration,
    /// Queue depths for random reads. Past the last, the ladder quadruples its depth while
    /// each step is faster than the one before beyond both intervals, so it reaches the depth
    /// where throughput stops growing, and goes no deeper than `max_read_depth`.
    pub random_depths: Vec<usize>,
    /// The deepest random-read point: NVMe queues hold at most 65,535 commands, its 16-bit,
    /// zero-based maximum queue entries less the slot that tells a full queue from an empty
    /// one (NVM Express Base Specification, CAP.MQES).
    pub max_read_depth: usize,
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

/// How many rounds a measured point runs: at least `min`, then until the 95% confidence
/// interval of its throughput is within `precision` of the mean, up to `max`, as Georges,
/// Buytaert and Eeckhout's JavaStats stops (OOPSLA 2007, §3.3; docs/research/11 §13).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rounds {
    pub min: usize,
    pub max: usize,
    pub precision: f64,
}

impl Rounds {
    /// Three rounds are the fewest that estimate a spread; six are enough for their minimum
    /// and maximum to bracket the median 95% of the time (docs/research/11 §13.3). The
    /// precision, ±5%, is half the ±10% that comparisons between benchmark runs resolve (§16).
    pub const STANDARD: Self = Self {
        min: 3,
        max: 6,
        precision: 0.05,
    };

    /// The most rounds a point runs; an interval needs two.
    pub fn limit(&self) -> usize {
        self.max.max(self.min).max(2)
    }

    /// Whether `throughputs`, one a round, are enough: the limit reached, or at least the
    /// minimum with an interval within the precision.
    pub fn enough(&self, throughputs: &[f64]) -> bool {
        throughputs.len() >= self.limit()
            || (throughputs.len() >= self.min.max(2)
                && relative_interval(throughputs) <= self.precision)
    }
}

impl Plan {
    /// About twenty seconds on a steady fast SSD, three rounds of half-second jobs a point, and
    /// up to twice that on a noisy one (`Rounds::STANDARD`).
    pub fn standard(align: Alignment) -> Self {
        let small = align
            .max(Alignment::new(4096).unwrap_or(Alignment::BYTE))
            .get();
        Self {
            span: 256 << 20,
            rounds: Rounds::STANDARD,
            step: Duration::from_millis(500),
            random_depths: vec![1, 4, 16, 64],
            max_read_depth: 65_535,
            sequential_depths: vec![1, 4],
            small,
            large: 1 << 20,
            durable_writes: 64,
            durable_large: 32 << 20,
        }
    }
}

/// One measured operating point, over the plan's rounds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub depth: usize,
    /// Mean throughput over the rounds.
    pub ops_per_sec: f64,
    pub bytes_per_sec: f64,
    /// Half-width of the throughput's 95% confidence interval, a fraction of its mean.
    pub spread: f64,
    pub rounds: usize,
    /// Latency quantiles over every round's transfers, when enough ran to estimate them.
    pub p50_ns: Option<u64>,
    pub p99_ns: Option<u64>,
}

/// Transfers needed to report a quantile `q`: enough that the order statistic lies within
/// half the tail's rank of `q` with 95% confidence, `n ≥ 1.96²·q(1−q)/δ²` at `δ = (1−q)/2`
/// (the binomial's normal approximation; docs/research/11 §13.3). 16 for the median, 1,522
/// for p99: 64 durable writes support a median and not a p99, which would be their maximum.
const P50_SAMPLES: u64 = 16;
const P99_SAMPLES: u64 = 1522;

/// Student's t for a two-sided 95% interval at 1 to 30 degrees of freedom; past 30 the normal
/// value, 1.96, is within 4%.
const T95: [f64; 30] = [
    12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
    2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
    2.052, 2.048, 2.045, 2.042,
];

#[derive(Debug, Clone)]
pub struct Calibration {
    pub caching: Caching,
    pub small: usize,
    pub large: usize,
    pub random_read: Vec<Point>,
    /// Random-read throughput still grew at the deepest depth the plan or the measuring
    /// backend allows: saturation lies deeper, and `random_read_saturation` is the fastest
    /// depth measured, not where throughput stops growing.
    pub random_read_capped: bool,
    pub sequential_read: Vec<Point>,
    pub sequential_write: Vec<Point>,
    /// A small write followed by the platform's full flush, one at a time.
    pub durable_write: Point,
    /// `durable_large` bytes written sequentially, then a full flush, one at a time.
    pub durable_sequential: Point,
    pub durable_large: usize,
    /// Small durable writes, one at a time, into space preallocated and never written.
    pub first_write: Point,
    /// The same writes again, over the space they wrote.
    pub overwrite: Point,
    pub elapsed: Duration,
}

impl Calibration {
    /// The random-read depth of greatest power, throughput over latency. In a closed loop
    /// latency is depth over throughput, so power is `X²/N`, greatest where the device's
    /// parallel units are just kept busy: Kleinrock's optimum, `N* = X_max · R_min`
    /// (ICC 1979, §3; docs/research/11 §13.3). Past it a deeper queue buys less throughput
    /// than it adds waiting.
    pub fn random_read_knee(&self) -> Option<Point> {
        knee(&self.random_read)
    }

    /// The shallowest random-read depth whose throughput interval overlaps the fastest
    /// point's: where throughput stops growing, as far as the rounds can tell (Georges,
    /// Buytaert and Eeckhout, OOPSLA 2007, §3.3). Past it a read added to the device only
    /// waits. Kleinrock's optimum (`random_read_knee`) lies at or before it, trading
    /// throughput for latency.
    pub fn random_read_saturation(&self) -> Option<Point> {
        saturation(&self.random_read)
    }

    /// The shallowest sequential-read depth of `large` transfers whose throughput interval
    /// overlaps the fastest point's: its bytes in flight are where large reads stop gaining
    /// throughput, and past them bytes added to the device only wait (Little's law; Georges,
    /// Buytaert and Eeckhout, OOPSLA 2007, §3.3).
    pub fn sequential_read_saturation(&self) -> Option<Point> {
        saturation(&self.sequential_read)
    }

    /// Whether a durable write into preallocated, never-written space is slower than the same
    /// write over written space: the first write's throughput interval lies wholly below the
    /// overwrite's, the test by which Georges, Buytaert and Eeckhout tell two measurements
    /// apart (OOPSLA 2007, §3.3). Where a file system journals the conversion of an extent on
    /// its first write, as ext4 does, the flush pays for it (5.7x on ext4;
    /// docs/measurements/2026-09-28-flush-cost-by-extent-state.md), and the chunk store writes
    /// a volume once before use (docs/design/chunk-store.md §2).
    pub fn first_write_penalty(&self) -> bool {
        slower(&self.first_write, &self.overwrite)
    }
}

/// `a`'s throughput interval lies wholly below `b`'s.
fn slower(a: &Point, b: &Point) -> bool {
    a.ops_per_sec * (1.0 + a.spread) < b.ops_per_sec * (1.0 - b.spread)
}

fn saturation(points: &[Point]) -> Option<Point> {
    let fastest = points
        .iter()
        .filter(|p| p.depth > 0)
        .max_by(|a, b| a.ops_per_sec.total_cmp(&b.ops_per_sec))?;
    points
        .iter()
        .filter(|p| p.depth > 0 && !slower(p, fastest))
        .min_by_key(|p| p.depth)
        .copied()
}

/// Whether the last point is faster than the one before beyond both intervals.
fn still_growing(points: &[Point]) -> bool {
    matches!(points, [.., before, last] if slower(before, last))
}

/// The random-read ladder's next depth: four times the last, or `cap` if that is nearer,
/// while throughput still grows; `None` once it stops growing or the cap is measured.
fn next_depth(points: &[Point], cap: usize) -> Option<usize> {
    let last = points.last()?.depth;
    let next = last.saturating_mul(4).min(cap);
    if still_growing(points) && next > last {
        Some(next)
    } else {
        None
    }
}

fn knee(points: &[Point]) -> Option<Point> {
    points
        .iter()
        .filter(|p| p.depth > 0 && p.ops_per_sec > 0.0)
        .max_by(|a, b| power(a).total_cmp(&power(b)))
        .copied()
}

fn power(p: &Point) -> f64 {
    let depth = u32::try_from(p.depth).map_or(f64::MAX, f64::from);
    p.ops_per_sec * p.ops_per_sec / depth
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
    // First writes need fresh space for every round they may run.
    let fresh = u64::try_from(plan.small)
        .ok()
        .and_then(|small| small.checked_mul(plan.durable_writes.max(1)))
        .unwrap_or(u64::MAX);
    let needed = u64::try_from(plan.large.max(plan.small))
        .unwrap_or(u64::MAX)
        .max(fresh.saturating_mul(u64::try_from(plan.rounds.limit()).unwrap_or(u64::MAX)));
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
    let scratch = Scratch::create(dir, ".mantle-calibrate")?;
    let file = DeviceFile::open(scratch.path(), false, CachingRequest::PreferDirect, align)?;
    file.preallocate(span)?;

    // Before anything is written: each round of first writes takes fresh space, and each
    // round of overwrites rewrites space a round of first writes wrote, the same way.
    let durable = |base| Job {
        pattern: Pattern::SequentialWrite,
        block: plan.small,
        depth: 1,
        base,
        span: fresh,
        budget: Some(plan.step.saturating_mul(4)),
        max_ops: plan.durable_writes,
        sync_each: true,
        seed: 0,
    };
    let first_write = point(&file, plan, |round| durable(round.saturating_mul(fresh)))?;
    let written = u64::try_from(first_write.rounds.max(1)).unwrap_or(1);
    let overwrite = point(&file, plan, |round| {
        durable(
            round
                .checked_rem(written)
                .unwrap_or(0)
                .saturating_mul(fresh),
        )
    })?;

    // Write the whole span first, so reads measure the medium rather than holes, and so
    // later writes overwrite written blocks (docs/measurements/2026-09-28).
    let fill = Job {
        pattern: Pattern::SequentialWrite,
        block: plan.large,
        depth: 1,
        base: 0,
        span,
        // Every block must be written, however long the device takes: its count bounds it.
        budget: None,
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
        base: 0,
        span,
        budget: Some(plan.step),
        max_ops: u64::MAX,
        sync_each: false,
        seed,
    };
    let mut random_read = Vec::with_capacity(plan.random_depths.len());
    for &depth in &plan.random_depths {
        random_read.push(point(&file, plan, |round| {
            job(Pattern::RandomRead, plan.small, depth, round)
        })?);
    }
    // Throughput still growing at the last depth: the ladder goes on, four times deeper each
    // step, no deeper than the plan allows or the measuring backend can run (audit S09).
    let cap = plan.max_read_depth.min(measure::MAX_DEPTH);
    while let Some(depth) = next_depth(&random_read, cap) {
        random_read.push(point(&file, plan, |round| {
            job(Pattern::RandomRead, plan.small, depth, round)
        })?);
    }
    let random_read_capped =
        still_growing(&random_read) && random_read.last().is_some_and(|p| p.depth >= cap);
    let mut sequential_read = Vec::with_capacity(plan.sequential_depths.len());
    let mut sequential_write = Vec::with_capacity(plan.sequential_depths.len());
    for &depth in &plan.sequential_depths {
        sequential_read.push(point(&file, plan, |round| {
            job(Pattern::SequentialRead, plan.large, depth, round)
        })?);
        sequential_write.push(point(&file, plan, |round| {
            job(Pattern::SequentialWrite, plan.large, depth, round)
        })?);
    }
    let durable_write = point(&file, plan, |round| Job {
        pattern: Pattern::RandomWrite,
        block: plan.small,
        depth: 1,
        base: 0,
        span,
        budget: Some(plan.step.saturating_mul(4)),
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
    let durable_sequential = point(&file, plan, |round| Job {
        pattern: Pattern::SequentialWrite,
        block: durable_large,
        depth: 1,
        base: 0,
        span,
        budget: Some(plan.step.saturating_mul(4)),
        max_ops: u64::MAX,
        sync_each: true,
        seed: round,
    })?;

    Ok(Calibration {
        caching: file.caching(),
        small: plan.small,
        large: plan.large,
        random_read,
        random_read_capped,
        sequential_read,
        sequential_write,
        durable_write,
        durable_sequential,
        durable_large,
        first_write,
        overwrite,
        elapsed: started.elapsed(),
    })
}

/// Runs a job in rounds, as many as the plan's `Rounds` take.
fn point(file: &DeviceFile, plan: &Plan, job: impl Fn(u64) -> Job) -> Result<Point, DiskError> {
    let max = plan.rounds.limit();
    let mut depth = 1;
    let (mut ops, mut bytes) = (Vec::with_capacity(max), Vec::with_capacity(max));
    let mut latency = crate::histogram::Histogram::new();
    for round in 0..max {
        let j = job(u64::try_from(round).unwrap_or(0));
        let r = measure::run(file, &j)?;
        depth = j.depth;
        ops.push(r.ops_per_sec());
        bytes.push(r.bytes_per_sec());
        latency.merge(&r.latency);
        if plan.rounds.enough(&ops) {
            break;
        }
    }
    let spread = relative_interval(&ops);
    let samples = latency.count();
    Ok(Point {
        depth,
        ops_per_sec: mean(&ops),
        bytes_per_sec: mean(&bytes),
        spread,
        rounds: ops.len(),
        p50_ns: (samples >= P50_SAMPLES).then(|| latency.p50()),
        p99_ns: (samples >= P99_SAMPLES).then(|| latency.p99()),
    })
}

pub fn mean(xs: &[f64]) -> f64 {
    let n = u32::try_from(xs.len()).map_or(f64::MAX, f64::from);
    xs.iter().sum::<f64>() / n
}

/// The half-width of the 95% confidence interval of the mean of `xs`, over the mean.
pub fn relative_interval(xs: &[f64]) -> f64 {
    let Some(df) = xs.len().checked_sub(1).filter(|&d| d > 0) else {
        return f64::INFINITY;
    };
    let m = mean(xs);
    let n = u32::try_from(xs.len()).map_or(f64::MAX, f64::from);
    let variance = xs.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (n - 1.0);
    let t = T95.get(df.saturating_sub(1)).copied().unwrap_or(1.96);
    let half = t * (variance / n).sqrt();
    if m > 0.0 { half / m } else { f64::INFINITY }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(depth: usize, ops: f64) -> Point {
        Point {
            depth,
            ops_per_sec: ops,
            bytes_per_sec: 0.0,
            spread: 0.0,
            rounds: 3,
            p50_ns: None,
            p99_ns: None,
        }
    }

    /// Power `X²/N` is greatest where more depth stops buying throughput in proportion.
    #[test]
    fn the_knee_is_the_depth_of_greatest_power() {
        let points = [
            at(1, 15_000.0),
            at(4, 60_000.0),
            at(16, 190_000.0),
            at(64, 235_000.0),
        ];
        assert_eq!(knee(&points).unwrap().depth, 16);
        // Throughput that doubles with depth keeps gaining power.
        assert_eq!(
            knee(&[at(1, 100.0), at(2, 200.0), at(4, 400.0)])
                .unwrap()
                .depth,
            4
        );
        assert_eq!(knee(&[]), None);
    }

    /// Throughput stops growing at the shallowest depth whose interval overlaps the fastest
    /// point's; the power knee, this machine's 16, comes before it when throughput still
    /// grows (docs/measurements/2026-09-29-read-depth.md).
    /// The ladder goes four times deeper while throughput grows, stops at the backend's
    /// limit, and says when throughput was still growing there: 64, then 256, and no 1,024
    /// on a backend of 256 threads (audit S09).
    #[test]
    fn the_ladder_stops_at_the_backends_limit_and_says_so() {
        let at = |depth: usize, ops: f64| Point {
            depth,
            ops_per_sec: ops,
            bytes_per_sec: ops * 4096.0,
            spread: 0.01,
            rounds: 3,
            p50_ns: None,
            p99_ns: None,
        };
        let growing = [at(16, 100.0), at(64, 200.0)];
        assert_eq!(next_depth(&growing, 256), Some(256));
        assert_eq!(next_depth(&growing, 100), Some(100));
        let at_cap = [at(64, 200.0), at(256, 400.0)];
        assert_eq!(next_depth(&at_cap, 256), None);
        assert!(still_growing(&at_cap));
        let flat = [at(64, 200.0), at(256, 201.0)];
        assert_eq!(next_depth(&flat, 1024), None);
        assert!(!still_growing(&flat));
        assert_eq!(next_depth(&[at(4, 10.0)], 256), None);
    }

    #[test]
    fn saturation_is_where_throughput_stops_growing() {
        let with = |depth: usize, ops: f64, spread: f64| Point {
            spread,
            ..at(depth, ops)
        };
        let points = [
            with(1, 14_000.0, 0.03),
            with(4, 54_800.0, 0.03),
            with(16, 165_000.0, 0.03),
            with(64, 214_000.0, 0.03),
            with(256, 218_000.0, 0.03),
        ];
        assert_eq!(knee(&points).unwrap().depth, 16);
        assert_eq!(saturation(&points).unwrap().depth, 64);
        // Still growing at the last depth: the last depth is the best found.
        assert_eq!(saturation(&points[..4]).unwrap().depth, 64);
        assert_eq!(saturation(&[]), None);
    }

    /// A penalty is measured only when the intervals part: 5.7x with tight intervals is one,
    /// and a difference inside the noise is none.
    #[test]
    fn a_first_write_penalty_needs_intervals_that_part() {
        let with = |ops: f64, spread: f64| Point {
            spread,
            ..at(1, ops)
        };
        assert!(slower(&with(380.0, 0.05), &with(2_170.0, 0.05)));
        assert!(!slower(&with(2_100.0, 0.05), &with(2_170.0, 0.05)));
        assert!(!slower(&with(380.0, 0.9), &with(2_170.0, 0.9)));
        assert!(!slower(&with(2_170.0, 0.05), &with(380.0, 0.05)));
        assert!(!slower(&with(380.0, f64::INFINITY), &with(2_170.0, 0.05)));
    }

    /// A point stops at the minimum once its interval is tight, runs on while it is not, and
    /// stops at the limit whatever its spread.
    #[test]
    fn rounds_stop_when_the_interval_is_tight_or_the_limit_is_reached() {
        let r = Rounds::STANDARD;
        assert!(!r.enough(&[100.0, 100.0]));
        assert!(r.enough(&[100.0, 100.0, 100.0]));
        assert!(!r.enough(&[50.0, 100.0, 150.0]));
        assert!(r.enough(&[50.0, 100.0, 150.0, 50.0, 100.0, 150.0]));
        assert_eq!(r.limit(), 6);
        let loose = Rounds {
            min: 1,
            max: 1,
            precision: 0.05,
        };
        assert_eq!(loose.limit(), 2);
        assert!(!loose.enough(&[100.0]));
    }

    #[test]
    fn the_interval_follows_students_t() {
        // Mean 100, sample deviation 10, n = 4: t(3) = 3.182, half-width 3.182·10/2.
        let xs = [90.0, 100.0, 110.0, 100.0];
        let sd = (200.0f64 / 3.0).sqrt();
        let want = 3.182 * sd / 2.0 / 100.0;
        assert!((relative_interval(&xs) - want).abs() < 1e-9);
        assert!(relative_interval(&[5.0]).is_infinite());
        assert_eq!(relative_interval(&[7.0, 7.0, 7.0]), 0.0);
    }

    #[test]
    fn a_small_calibration_runs_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let align = Alignment::new(4096).unwrap();
        let plan = Plan {
            span: 8 << 20,
            rounds: Rounds {
                min: 2,
                max: 3,
                precision: 0.05,
            },
            step: Duration::from_millis(20),
            random_depths: vec![1, 2],
            // The ladder goes no deeper than planned, however the reads scale.
            max_read_depth: 2,
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
        // At most twelve durable writes: too few for a p99 or a median.
        assert_eq!(c.durable_write.p99_ns, None);
        assert_eq!(c.durable_write.p50_ns, None);
        assert!((2..=3).contains(&c.durable_write.rounds));
        assert!(c.durable_sequential.bytes_per_sec > 0.0);
        assert_eq!(c.durable_large, 2 << 20);
        assert!(c.first_write.ops_per_sec > 0.0 && c.overwrite.ops_per_sec > 0.0);
        assert!((2..=3).contains(&c.first_write.rounds));
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert!(leftovers.is_empty(), "scratch file left behind");
    }

    /// A calibration that fails leaves its directory as it found it: a file already there,
    /// even one named as scratch files once were, is neither written nor removed, and no
    /// scratch file stays behind (audit S06).
    #[test]
    fn a_failed_calibration_leaves_the_directory_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let theirs = dir
            .path()
            .join(format!(".mantle-calibrate-{}", std::process::id()));
        std::fs::write(&theirs, b"customer-data").unwrap();
        let plan = Plan {
            small: 0,
            ..Plan::standard(Alignment::new(4096).unwrap())
        };
        assert!(calibrate(dir.path(), Alignment::new(4096).unwrap(), None, &plan).is_err());
        assert_eq!(std::fs::read(&theirs).unwrap(), b"customer-data");
        let left: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(left, vec![theirs]);
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
