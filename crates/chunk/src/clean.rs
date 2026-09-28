//! Reclaiming the space of deleted chunks (docs/design/chunk-store.md §8).
//!
//! A segment whose chunks are all deleted is freed by the writer without copying anything.
//! One that is partly dead is cleaned: its live fragments are read back, verified, and handed
//! to the writer as relocations into the cleaner's own stream; once nothing live is left in
//! it, the writer frees it. The cleaner runs when free segments fall below a low watermark
//! and stops at a high one, choosing victims by LFS's cost-benefit ratio
//! `(1 − u) · age / (1 + u)`, where `u` is the fraction still live and `age` the time since
//! its youngest live record was written (Rosenblum and Ousterhout, TOCS 1992, §3.6): old,
//! mostly dead segments first, since cold data that survived is likely to stay.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::time::Duration;

use mantle_disk::block::BlockFile;

use crate::error::ChunkError;
use crate::frame::SegmentState;
use crate::index::Fragment;
use crate::key::ChunkKey;
use crate::read;
use crate::record::{FLAG_FINAL, Payload};
use crate::writer::{Move, Op, Request, Shared};

/// How often the cleaner checks free space without being woken.
const IDLE: Duration = Duration::from_secs(5);
/// Victims cleaned without a net gain in free segments before a pass gives up.
const FUTILE_AFTER: u32 = 4;

pub(crate) struct Cleaner<F> {
    pub shared: Arc<Shared<F>>,
    pub submit: SyncSender<Request>,
    pub wake: Receiver<()>,
    /// Start cleaning below this many free segments.
    pub low: usize,
    /// Stop at this many.
    pub high: usize,
    /// Payload bytes and fragments per relocation request: one writer batch each.
    pub batch_bytes: usize,
    pub batch_moves: usize,
}

/// What one cleaning pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanReport {
    pub segments: u32,
    /// Free segments gained.
    pub gained: u32,
    pub relocated: u64,
    /// Live fragments that failed verification and were left in place for repair.
    pub corrupt: u64,
}

impl<F: BlockFile> Cleaner<F> {
    pub fn run(self) {
        // The dead-byte count when a pass last gained nothing: until more data dies, another
        // pass would copy the same live data around for nothing.
        let mut futile_at: Option<u64> = None;
        loop {
            match self.wake.recv_timeout(IDLE) {
                Ok(()) | Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
            if self.shared.stopping.load(Ordering::Acquire) {
                return;
            }
            if self.shared.fenced.load(Ordering::Acquire) {
                continue;
            }
            let dead = self.shared.dead_bytes.load(Ordering::Relaxed);
            if futile_at == Some(dead) {
                continue;
            }
            // A failed pass (the volume fenced, or it closed) ends this round; the next wake
            // retries if there is still work.
            if let Ok(report) = self.pass() {
                futile_at = if report.segments > 0 && report.gained == 0 {
                    Some(dead)
                } else {
                    None
                };
            }
        }
    }

    fn free(&self) -> usize {
        self.shared.usage.read().map_or(0, |usage| {
            usage
                .iter()
                .filter(|s| s.state == SegmentState::Free)
                .count()
        })
    }

    /// Cleans victims until free segments reach the high watermark or none is worth it.
    pub fn pass(&self) -> Result<CleanReport, ChunkError> {
        if self.free() >= self.low {
            return Ok(CleanReport::default());
        }
        self.clean_until(|cleaner, _| cleaner.free() < cleaner.high)
    }

    /// Cleans the `n` best victims, whatever the free space.
    pub fn clean_best(&self, n: u32) -> Result<CleanReport, ChunkError> {
        self.clean_until(|_, report| report.segments < n)
    }

    fn clean_until(
        &self,
        more: impl Fn(&Self, &CleanReport) -> bool,
    ) -> Result<CleanReport, ChunkError> {
        let mut report = CleanReport::default();
        let mut tried = Vec::new();
        let start = self.free();
        while more(self, &report) {
            let Some(victim) = self.victim(&tried) else {
                break;
            };
            tried.push(victim);
            let (relocated, corrupt) = self.clean(victim)?;
            report.segments = report.segments.saturating_add(1);
            report.relocated = report.relocated.saturating_add(relocated);
            report.corrupt = report.corrupt.saturating_add(corrupt);
            // The writer frees a segment in the batch after the one that emptied it.
            self.flush()?;
            // Cleaning N segments of live fraction u frees about N(1 − u) (Rosenblum and
            // Ousterhout, TOCS 1992, §3.4), but a few victims can net nothing while their live
            // data opens the segment it moves into. Four victims without a net gain mean the live
            // data does not pack any tighter, and the pass stops.
            if report.segments >= FUTILE_AFTER && self.free() <= start {
                break;
            }
        }
        report.gained = u32::try_from(self.free().saturating_sub(start)).unwrap_or(u32::MAX);
        Ok(report)
    }

    /// The sealed segment with the best cost-benefit ratio.
    fn victim(&self, tried: &[u32]) -> Option<u32> {
        let usage = self.shared.usage.read().ok()?;
        let size = self.shared.geometry.segment_size;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        usage
            .iter()
            .enumerate()
            .filter(|(i, s)| {
                s.state == SegmentState::Sealed
                    && s.live < size
                    && u32::try_from(*i).is_ok_and(|i| !tried.contains(&i))
            })
            .map(|(i, s)| {
                // Scores are compared, not reported: f64's rounding does not change the order
                // that matters.
                let u = s.live as f64 / size as f64;
                let age = now.saturating_sub(s.youngest_ns) as f64;
                ((1.0 - u) * age / (1.0 + u), i)
            })
            .filter(|(score, _)| score.is_finite())
            .max_by(|a, b| a.0.total_cmp(&b.0))
            .and_then(|(_, i)| u32::try_from(i).ok())
    }

    /// Relocates every live fragment of `segment`; returns (relocated, corrupt).
    fn clean(&self, segment: u32) -> Result<(u64, u64), ChunkError> {
        let incarnation = self
            .shared
            .usage
            .read()
            .map_err(|_| ChunkError::Fenced)?
            .get(usize::try_from(segment).unwrap_or(usize::MAX))
            .map(|s| s.incarnation)
            .ok_or(ChunkError::Fenced)?;
        let live: Vec<(ChunkKey, Fragment, u8, u64)> = {
            let index = self.shared.index.read().map_err(|_| ChunkError::Fenced)?;
            index
                .iter()
                .flat_map(|(key, entry)| {
                    let last = entry.fragments.len().saturating_sub(1);
                    entry
                        .fragments
                        .iter()
                        .enumerate()
                        .filter(move |(_, f)| f.segment == segment && f.incarnation == incarnation)
                        .map(move |(i, f)| {
                            let flags = if entry.sealed && i == last {
                                FLAG_FINAL
                            } else {
                                0
                            };
                            (*key, *f, flags, entry.time_ns)
                        })
                })
                .collect()
        };
        let (mut relocated, mut corrupt) = (0u64, 0u64);
        let mut moves: Vec<Move> = Vec::new();
        let mut bytes = 0usize;
        for (key, from, flags, time_ns) in live {
            let mut data = Vec::with_capacity(usize::try_from(from.payload_len).unwrap_or(0));
            match read::fragment(
                &self.shared,
                &key,
                &from,
                0,
                u64::from(from.payload_len),
                &mut data,
            ) {
                Ok(()) => {}
                Err(ChunkError::Corrupt { .. }) => {
                    // Left in place: moving bytes that fail verification would launder them.
                    corrupt = corrupt.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            }
            bytes = bytes.saturating_add(data.len());
            let payload = Payload::new(data, self.shared.checksum_shift)
                .ok_or_else(|| ChunkError::Config("checksum block size".into()))?;
            moves.push(Move {
                key,
                from,
                payload,
                flags,
                time_ns,
            });
            if moves.len() >= self.batch_moves || bytes >= self.batch_bytes {
                relocated = relocated.saturating_add(self.relocate(std::mem::take(&mut moves))?);
                bytes = 0;
            }
        }
        if !moves.is_empty() {
            relocated = relocated.saturating_add(self.relocate(moves)?);
        }
        Ok((relocated, corrupt))
    }

    /// Hands one batch of moves to the writer and waits for it to be durable.
    fn relocate(&self, moves: Vec<Move>) -> Result<u64, ChunkError> {
        let count = u64::try_from(moves.len()).unwrap_or(u64::MAX);
        let (reply, answer) = sync_channel(1);
        self.shared.submitted.fetch_add(1, Ordering::AcqRel);
        self.submit
            .send(Request {
                op: Op::Relocate { moves },
                reply,
            })
            .map_err(|_| ChunkError::Closed)?;
        answer.recv().map_err(|_| ChunkError::Closed)??;
        Ok(count)
    }

    /// Submits a request that writes nothing, so the writer runs a batch and frees empty
    /// segments.
    fn flush(&self) -> Result<(), ChunkError> {
        let (reply, answer) = sync_channel(1);
        self.shared.submitted.fetch_add(1, Ordering::AcqRel);
        self.submit
            .send(Request {
                op: Op::Delete {
                    key: ChunkKey {
                        block: 0,
                        epoch: u32::MAX,
                        index: u16::MAX,
                    },
                },
                reply,
            })
            .map_err(|_| ChunkError::Closed)?;
        answer.recv().map_err(|_| ChunkError::Closed)?
    }
}
