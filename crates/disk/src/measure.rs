//! Measuring a device through the same file layer mantle stores data with.
//!
//! A [`Job`] issues fixed-size transfers against a [`DeviceFile`] at a fixed depth (transfers
//! in flight at once), bounded by a wall-clock budget and an operation count, and records
//! each transfer's latency. Depth is realised as that many threads issuing blocking
//! positional I/O, which is how the portable I/O path issues it; a measurement describes
//! the path mantle will use, not the device in the abstract.
//!
//! Write payloads are pseudo-random: some devices compress or deduplicate, and zero or
//! repeated buffers would measure that instead of the medium.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::DiskError;
use crate::buf::AlignedBuf;
use crate::file::DeviceFile;
use crate::histogram::Histogram;

/// The most threads one job may use.
pub const MAX_DEPTH: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pattern {
    SequentialRead,
    SequentialWrite,
    RandomRead,
    RandomWrite,
}

impl Pattern {
    fn writes(self) -> bool {
        matches!(self, Self::SequentialWrite | Self::RandomWrite)
    }

    fn random(self) -> bool {
        matches!(self, Self::RandomRead | Self::RandomWrite)
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub pattern: Pattern,
    /// Bytes per transfer; a multiple of the file's alignment.
    pub block: usize,
    /// Transfers in flight at once, 1..=MAX_DEPTH.
    pub depth: usize,
    /// The job touches bytes `[0, span)` of the file; at least one block.
    pub span: u64,
    pub budget: Duration,
    pub max_ops: u64,
    /// Flush after every write, so each recorded latency is that of a durable write.
    pub sync_each: bool,
    pub seed: u64,
}

#[derive(Debug, Clone)]
pub struct JobResult {
    pub ops: u64,
    pub bytes: u64,
    pub elapsed: Duration,
    /// Per-transfer latency in nanoseconds.
    pub latency: Histogram,
}

impl JobResult {
    pub fn bytes_per_sec(&self) -> f64 {
        rate(self.bytes, self.elapsed)
    }

    pub fn ops_per_sec(&self) -> f64 {
        rate(self.ops, self.elapsed)
    }
}

// Rates are floating point; u64 -> f64 rounds above 2^53, far beyond any count a bounded
// job produces.
fn rate(count: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    count as f64 / secs
}

/// SplitMix64 (Steele, Lea and Flood, "Fast Splittable Pseudorandom Number Generators",
/// OOPSLA 2014): statistically sound for choosing offsets and filling payloads, and one
/// line of state per thread.
#[derive(Debug, Clone)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `[0, bound)`; zero when `bound` is zero. Lemire's multiply-shift
    /// ("Fast Random Integer Generation in an Interval", ACM TOMACS 2019) without the
    /// rejection step: the bias is below bound / 2^64, immaterial for offsets.
    pub fn below(&mut self, bound: u64) -> u64 {
        let wide = u128::from(self.next_u64()).saturating_mul(u128::from(bound));
        u64::try_from(wide >> 64).unwrap_or(0)
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            for (dst, src) in chunk.iter_mut().zip(word.iter()) {
                *dst = *src;
            }
        }
    }
}

/// Runs `job` against `file` and returns what it measured.
pub fn run(file: &DeviceFile, job: &Job) -> Result<JobResult, DiskError> {
    let align = file.alignment();
    let block = u64::try_from(job.block).map_err(|_| invalid("block size"))?;
    if job.block == 0 || !align.is_aligned(job.block) {
        return Err(invalid(
            "block size must be a non-zero multiple of the alignment",
        ));
    }
    if job.depth == 0 || job.depth > MAX_DEPTH {
        return Err(invalid("depth must be in 1..=MAX_DEPTH"));
    }
    // Whole blocks only: the last transfer must end inside the span.
    let slots = job.span.checked_div(block).unwrap_or(0);
    if slots == 0 {
        return Err(invalid("span must hold at least one block"));
    }

    let started = Instant::now();
    let shared = Shared {
        file,
        job,
        block,
        slots,
        deadline: started.checked_add(job.budget).unwrap_or(started),
        next: AtomicU64::new(0),
        issued: AtomicU64::new(0),
    };

    let outcomes: Vec<Result<(u64, Histogram), DiskError>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..job.depth)
            .map(|worker| {
                let shared = &shared;
                scope.spawn(move || {
                    let seed = job.seed ^ u64::try_from(worker).unwrap_or(0).rotate_left(32);
                    shared.work(seed)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Err(invalid("measurement worker unwound")))
            })
            .collect()
    });
    let elapsed = started.elapsed();

    let mut latency = Histogram::new();
    let mut ops = 0u64;
    for outcome in outcomes {
        let (worker_ops, hist) = outcome?;
        ops = ops.saturating_add(worker_ops);
        latency.merge(&hist);
    }
    Ok(JobResult {
        ops,
        bytes: ops.saturating_mul(block),
        elapsed,
        latency,
    })
}

/// What every worker of one job reads.
struct Shared<'a> {
    file: &'a DeviceFile,
    job: &'a Job,
    block: u64,
    slots: u64,
    deadline: Instant,
    /// The next sequential slot.
    next: AtomicU64,
    /// Operations claimed so far, across workers.
    issued: AtomicU64,
}

impl Shared<'_> {
    fn work(&self, seed: u64) -> Result<(u64, Histogram), DiskError> {
        let (file, job) = (self.file, self.job);
        let mut rng = SplitMix64::new(seed);
        let mut buf = AlignedBuf::zeroed(job.block, file.alignment())?;
        if job.pattern.writes() {
            rng.fill(buf.as_mut_capacity());
        }
        buf.set_len(job.block)?;
        let mut hist = Histogram::new();
        let mut ops = 0u64;
        loop {
            // Claim an operation before issuing it, so the job never exceeds max_ops.
            if self.issued.fetch_add(1, Ordering::Relaxed) >= job.max_ops
                || Instant::now() >= self.deadline
            {
                break;
            }
            let slot = if job.pattern.random() {
                rng.below(self.slots)
            } else {
                self.next
                    .fetch_add(1, Ordering::Relaxed)
                    .checked_rem(self.slots)
                    .unwrap_or(0)
            };
            let offset = slot.saturating_mul(self.block);
            let begin = Instant::now();
            if job.pattern.writes() {
                file.write_all_at(buf.as_slice(), offset)?;
                if job.sync_each {
                    file.sync_data()?;
                }
            } else {
                file.read_exact_at(buf.as_mut_slice(), offset)?;
            }
            let nanos = u64::try_from(begin.elapsed().as_nanos()).unwrap_or(u64::MAX);
            hist.record(nanos);
            ops = ops.saturating_add(1);
        }
        Ok((ops, hist))
    }
}

fn invalid(what: &str) -> DiskError {
    DiskError::Io {
        op: "measure",
        path: std::path::PathBuf::new(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, what.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::Alignment;
    use crate::file::CachingRequest;

    fn scratch(len: u64) -> (tempfile::TempDir, DeviceFile) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("probe");
        let file = DeviceFile::open(
            &path,
            true,
            CachingRequest::PreferDirect,
            Alignment::new(4096).unwrap(),
        )
        .unwrap();
        file.preallocate(len).unwrap();
        (dir, file)
    }

    fn job(pattern: Pattern, depth: usize, max_ops: u64) -> Job {
        Job {
            pattern,
            block: 4096,
            depth,
            span: 1 << 20,
            budget: Duration::from_secs(10),
            max_ops,
            sync_each: false,
            seed: 7,
        }
    }

    #[test]
    fn a_job_performs_exactly_its_operation_bound() {
        let (_dir, file) = scratch(1 << 20);
        for depth in [1, 3, 8] {
            let r = run(&file, &job(Pattern::SequentialWrite, depth, 100)).unwrap();
            assert_eq!(r.ops, 100);
            assert_eq!(r.bytes, 100 * 4096);
            assert_eq!(r.latency.count(), 100);
            let r = run(&file, &job(Pattern::RandomRead, depth, 64)).unwrap();
            assert_eq!(r.ops, 64);
        }
    }

    #[test]
    fn sequential_writes_cover_the_span_with_random_payload() {
        let (_dir, file) = scratch(1 << 20);
        run(&file, &job(Pattern::SequentialWrite, 1, 256)).unwrap();
        let mut back = AlignedBuf::zeroed(1 << 20, file.alignment()).unwrap();
        file.read_exact_at(back.as_mut_capacity(), 0).unwrap();
        for block in back.as_mut_capacity().chunks(4096) {
            assert!(block.iter().any(|&b| b != 0), "a block was never written");
        }
    }

    #[test]
    fn invalid_jobs_are_refused() {
        let (_dir, file) = scratch(1 << 20);
        let mut j = job(Pattern::RandomRead, 1, 1);
        j.block = 1000;
        assert!(run(&file, &j).is_err());
        let mut j = job(Pattern::RandomRead, 0, 1);
        assert!(run(&file, &j).is_err());
        j.depth = MAX_DEPTH + 1;
        assert!(run(&file, &j).is_err());
        let mut j = job(Pattern::RandomRead, 1, 1);
        j.span = 100;
        assert!(run(&file, &j).is_err());
    }

    #[test]
    fn below_stays_in_range() {
        let mut rng = SplitMix64::new(1);
        for bound in [1u64, 2, 3, 1000, u64::MAX] {
            for _ in 0..1000 {
                assert!(rng.below(bound) < bound);
            }
        }
        assert_eq!(rng.below(0), 0);
    }
}
