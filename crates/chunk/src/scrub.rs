//! Reading every stored byte on a schedule to find damage before a read does
//! (docs/design/chunk-store.md §9).
//!
//! Latent sector errors are found by nothing but reads, and most are found by scrubbing
//! rather than by the workload (Bairavasundaram et al., SIGMETRICS 2007, §6). The scrubber
//! verifies every live fragment segment by segment, at a rate that finishes the volume
//! within its period (seven days is practice, NetApp scrubbing every two weeks and Ceph
//! every one: docs/research/11 §12.2), in a staggered order that reads a little of every
//! region of the volume before any region twice (Schroeder, Damouras and Gill, FAST 2010,
//! §5.2.3). It records each chunk that fails in a bounded list for repair. A failure also
//! marks the volume at risk: errors cluster in space and time (Bairavasundaram et al. 2007,
//! §5), so the whole volume is then scrubbed at once rather than on schedule.

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mantle_disk::block::BlockFile;

use crate::error::ChunkError;
use crate::frame::SegmentState;
use crate::index::Fragment;
use crate::key::ChunkKey;
use crate::read;
use crate::writer::Shared;

/// Damaged chunks remembered for repair. More than 80% of disks with latent sector errors had
/// fewer than 50, and errors cluster within 10 MB (docs/research/03 BGPS07-F2, F3), so a
/// volume with thousands of damaged chunks has failed in many places: past this many it is
/// failing, to be drained whole rather than repaired chunk by chunk, and further damage is
/// counted rather than listed (docs/research/11 §12.4).
pub const MAX_DAMAGED: usize = 4096;

/// Segments per region in the staggered order. Schroeder, Damouras and Gill stagger 128 MiB
/// regions read in 1 MiB steps (docs/design/chunk-store.md §9); here a step is a segment, so a
/// region is 128 segments and each round reads one segment of every region: the same 1/128 of
/// the volume, spread over all of it.
const STEPS: usize = 128;

/// What scrubbing has found.
#[derive(Debug, Default)]
pub struct Findings {
    /// Chunks with a fragment that failed verification, awaiting repair.
    pub damaged: BTreeSet<ChunkKey>,
    /// Damaged chunks found once the list was full, counted but not listed.
    pub dropped: u64,
    /// The list filled: the volume is to be drained whole. It stays so until reopened.
    pub failing: bool,
    /// When the volume first showed damage; it stays at risk until reopened.
    pub at_risk_since: Option<Instant>,
    /// Fragments verified and bytes read, over the volume's life so far.
    pub fragments: u64,
    pub bytes: u64,
    /// Full passes completed.
    pub passes: u64,
}

impl Findings {
    fn record(&mut self, key: ChunkKey) {
        if self.damaged.len() >= MAX_DAMAGED && !self.damaged.contains(&key) {
            self.failing = true;
            self.dropped = self.dropped.saturating_add(1);
            return;
        }
        self.damaged.insert(key);
        self.at_risk_since.get_or_insert_with(Instant::now);
    }
}

pub(crate) type SharedFindings = Arc<Mutex<Findings>>;

/// Verifies every live fragment in `segment`; returns (fragments, bytes, damaged).
pub(crate) fn scrub_segment<F: BlockFile>(
    shared: &Shared<F>,
    findings: &SharedFindings,
    segment: u32,
) -> Result<(u64, u64, u64), ChunkError> {
    let live: Vec<(ChunkKey, Fragment)> = {
        let index = shared.index.read().map_err(|_| ChunkError::Fenced)?;
        index
            .iter()
            .flat_map(|(key, entry)| {
                entry
                    .fragments
                    .iter()
                    .filter(move |f| f.segment == segment)
                    .map(move |f| (*key, *f))
            })
            .collect()
    };
    let (mut fragments, mut bytes, mut damaged) = (0u64, 0u64, 0u64);
    let mut scratch = Vec::new();
    for (key, fragment) in live {
        scratch.clear();
        let len = u64::from(fragment.payload_len);
        match read::fragment(shared, &key, &fragment, 0, len, &mut scratch) {
            Ok(()) => {}
            Err(ChunkError::Corrupt { .. }) => {
                // A fragment relocated or deleted since the index was read is not damage.
                let current = shared
                    .index
                    .read()
                    .map_err(|_| ChunkError::Fenced)?
                    .get(&key)
                    .and_then(|e| e.fragment_at(fragment.chunk_offset))
                    .copied();
                if current == Some(fragment) {
                    damaged = damaged.saturating_add(1);
                    findings.lock().map_err(|_| ChunkError::Fenced)?.record(key);
                }
            }
            Err(e) => return Err(e),
        }
        fragments = fragments.saturating_add(1);
        bytes = bytes.saturating_add(len);
    }
    let mut f = findings.lock().map_err(|_| ChunkError::Fenced)?;
    f.fragments = f.fragments.saturating_add(fragments);
    f.bytes = f.bytes.saturating_add(bytes);
    Ok((fragments, bytes, damaged))
}

/// Scrubs every sealed or open segment once; returns the damaged fragments found.
pub(crate) fn scrub_all<F: BlockFile>(
    shared: &Shared<F>,
    findings: &SharedFindings,
) -> Result<u64, ChunkError> {
    let segments: Vec<u32> = {
        let usage = shared.usage.read().map_err(|_| ChunkError::Fenced)?;
        (0..positions(usage.len()))
            .filter_map(|p| staggered(p, usage.len()))
            .filter(|&i| {
                usage
                    .get(i)
                    .is_some_and(|s| s.state != SegmentState::Free && s.live > 0)
            })
            .filter_map(|i| u32::try_from(i).ok())
            .collect()
    };
    let mut damaged = 0u64;
    for segment in segments {
        if shared.stopping.load(Ordering::Acquire) {
            break;
        }
        let (_, _, d) = scrub_segment(shared, findings, segment)?;
        damaged = damaged.saturating_add(d);
    }
    let mut f = findings.lock().map_err(|_| ChunkError::Fenced)?;
    f.passes = f.passes.saturating_add(1);
    Ok(damaged)
}

/// The background scrubber: one segment at a time, pausing between segments so a pass over
/// the volume takes the configured period, and starting over at once while the volume is at
/// risk.
pub(crate) struct Scrubber<F> {
    pub shared: Arc<Shared<F>>,
    pub findings: SharedFindings,
    pub wake: std::sync::mpsc::Receiver<()>,
    pub period: Duration,
}

impl<F: BlockFile> Scrubber<F> {
    pub fn run(self) {
        let mut next = 0usize;
        loop {
            if self.shared.stopping.load(Ordering::Acquire) {
                return;
            }
            let (position, segment, volume_bytes) = match self.pick(next) {
                Some(found) => found,
                None => {
                    // Nothing stored: wait a period, or until woken.
                    if self.wait(self.period) {
                        return;
                    }
                    continue;
                }
            };
            next = position.saturating_add(1);
            let bytes = match scrub_segment(&self.shared, &self.findings, segment) {
                Ok((_, bytes, _)) => bytes,
                // The volume is fenced or closing: stop until woken.
                Err(_) => {
                    if self.wait(self.period) {
                        return;
                    }
                    continue;
                }
            };
            let at_risk = self
                .findings
                .lock()
                .map(|f| f.at_risk_since.is_some())
                .unwrap_or(false);
            let pause = if at_risk {
                Duration::ZERO
            } else {
                pace(bytes, volume_bytes, self.period)
            };
            if self.wait(pause) {
                return;
            }
        }
    }

    /// The next segment holding live data at or after position `from` of the staggered
    /// order, wrapping around: its position, the segment, and the live bytes of the volume.
    fn pick(&self, from: usize) -> Option<(usize, u32, u64)> {
        let usage = self.shared.usage.read().ok()?;
        let total: u64 = usage.iter().map(|s| s.live).sum();
        let count = positions(usage.len());
        (0..count)
            .filter_map(|i| from.saturating_add(i).checked_rem(count))
            .find_map(|p| {
                let segment = staggered(p, usage.len())?;
                let live = usage
                    .get(segment)
                    .is_some_and(|s| s.state != SegmentState::Free && s.live > 0);
                if live {
                    Some((p, u32::try_from(segment).ok()?, total))
                } else {
                    None
                }
            })
    }

    /// Waits `pause`, or until woken; true if the volume is closing.
    fn wait(&self, pause: Duration) -> bool {
        match self.wake.recv_timeout(pause) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return true,
        }
        self.shared.stopping.load(Ordering::Acquire)
    }
}

/// Positions in a staggered pass over `count` segments: every step of every region.
fn positions(count: usize) -> usize {
    count.div_ceil(STEPS).saturating_mul(STEPS)
}

/// The segment at position `p` of a staggered pass over `count` segments: step `p / regions`
/// of region `p % regions`. `None` for a step past the end of the last, shorter region.
fn staggered(p: usize, count: usize) -> Option<usize> {
    let regions = count.div_ceil(STEPS);
    let segment = p
        .checked_rem(regions)?
        .checked_mul(STEPS)?
        .checked_add(p.checked_div(regions)?)?;
    if segment < count { Some(segment) } else { None }
}

/// The pause after scrubbing `bytes`, so that `volume_bytes` take `period` to scrub.
pub(crate) fn pace(bytes: u64, volume_bytes: u64, period: Duration) -> Duration {
    if volume_bytes == 0 {
        return period;
    }
    // bytes / volume_bytes of the period, computed in nanoseconds without overflow.
    let share = u128::from(bytes)
        .saturating_mul(period.as_nanos())
        .checked_div(u128::from(volume_bytes))
        .unwrap_or(0);
    Duration::from_nanos(u64::try_from(share).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pacing_spreads_the_volume_over_the_period() {
        let week = Duration::from_secs(7 * 24 * 3600);
        assert_eq!(pace(1, 7, week), Duration::from_secs(24 * 3600));
        assert_eq!(pace(0, 7, week), Duration::ZERO);
        assert_eq!(pace(5, 0, week), week);
    }

    #[test]
    fn findings_are_bounded() {
        let mut f = Findings::default();
        for n in 0..(MAX_DAMAGED as u128 + 10) {
            f.record(ChunkKey {
                block: n,
                epoch: 0,
                index: 0,
            });
        }
        assert_eq!(f.damaged.len(), MAX_DAMAGED);
        assert_eq!(f.dropped, 10);
        assert!(f.failing);
        assert!(f.at_risk_since.is_some());
    }

    /// A pass visits every segment once, and its first round one segment of every region.
    #[test]
    fn the_staggered_order_covers_the_volume_and_spreads_each_round() {
        for count in [1, 5, 127, 128, 129, 1000, 80_000] {
            let order: Vec<usize> = (0..positions(count))
                .filter_map(|p| staggered(p, count))
                .collect();
            let mut sorted = order.clone();
            sorted.sort_unstable();
            assert_eq!(sorted, (0..count).collect::<Vec<_>>(), "{count} segments");
            let regions = count.div_ceil(STEPS);
            let first: Vec<usize> = order.iter().take(regions).map(|s| s / STEPS).collect();
            assert_eq!(first, (0..regions).collect::<Vec<_>>(), "{count} segments");
        }
        assert_eq!(staggered(0, 300), Some(0));
        assert_eq!(staggered(1, 300), Some(128));
        assert_eq!(staggered(2, 300), Some(256));
        assert_eq!(staggered(3, 300), Some(1));
    }
}
