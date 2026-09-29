//! Reclaiming the space of deleted chunks (docs/design/chunk-store.md §8).
//!
//! A segment whose chunks are all deleted is freed by the writer without copying anything.
//! One that is partly dead is cleaned: its live fragments are read back, verified, and handed
//! to the writer as relocations into the cleaner's own stream; once nothing live is left in
//! it, the writer frees it. Victims are chosen by LFS's cost-benefit ratio
//! `(1 − u) · age / (1 + u)`, where `u` is the fraction still live and `age` the time since
//! its youngest live record was written (Rosenblum and Ousterhout, TOCS 1992, §3.6): old,
//! mostly dead segments first, since cold data that survived is likely to stay.
//!
//! The cleaner runs, woken by the writer after each batch, when free segments fall below a
//! runway (`runway`): enough to take writes at the fastest rate seen while the cleaner reacts
//! and cleans one victim. LFS chose its thresholds without study and found performance
//! insensitive to them, and a fraction of the volume scales with the disk rather than the
//! write rate (docs/research/11 §10).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::{Duration, Instant};

use mantle_disk::DiskError;
use mantle_disk::block::BlockFile;

use crate::error::ChunkError;
use crate::frame::SegmentState;
use crate::key::ChunkKey;
use crate::record::{FLAG_FINAL, Payload};
use crate::recover::{Identity, verify_at};
use crate::writer::{CLEANER_RESERVE, Move, Op, Request, Shared};

pub(crate) struct Cleaner<F> {
    pub shared: Arc<Shared<F>>,
    pub submit: SyncSender<Request>,
    pub wake: Receiver<()>,
    /// Payload bytes and fragments per relocation request: one writer batch each.
    pub batch_bytes: usize,
    pub batch_moves: usize,
}

/// Free segments to hold before cleaning starts: the client's reserve (writer.rs) and the
/// runway, `W·(t_react + t_clean) + D` bytes, where `W` is the fastest payload rate seen,
/// `t_react` a batch (the writer wakes the cleaner after each), `t_clean` the longest one
/// victim has taken, and `D` one batch arriving between wakes (docs/research/11 §10.3). Until
/// a victim has been cleaned, `W·t_clean` is taken as two segments: reading and rewriting a
/// wholly live victim at the rate writes arrive.
pub(crate) fn runway(
    rate: u64,
    react_ns: u64,
    clean_ns: u64,
    batch_bytes: u64,
    segment_size: u64,
) -> usize {
    let per_ns = |ns: u64| {
        u128::from(rate)
            .saturating_mul(u128::from(ns))
            .checked_div(1_000_000_000)
            .unwrap_or(0)
    };
    let cleaning = if clean_ns == 0 {
        u128::from(segment_size).saturating_mul(2)
    } else {
        per_ns(clean_ns)
    };
    let bytes = per_ns(react_ns)
        .saturating_add(cleaning)
        .saturating_add(u128::from(batch_bytes));
    let segments = bytes.div_ceil(u128::from(segment_size.max(1)));
    usize::try_from(segments)
        .unwrap_or(usize::MAX)
        .saturating_add(CLEANER_RESERVE)
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
        // Every change to free space runs a writer batch, and every batch below the runway
        // wakes the cleaner, so it waits for nothing else.
        while self.wake.recv().is_ok() {
            if self.shared.stopping.load(Ordering::Acquire) {
                return;
            }
            if self.shared.fenced.load(Ordering::Acquire) {
                continue;
            }
            // Until more data dies, another pass would copy the same live data around for
            // nothing (`Shared::futile_at`).
            let dead = self.shared.dead_bytes.load(Ordering::Relaxed);
            if self.shared.futile_at.load(Ordering::Relaxed) == dead {
                continue;
            }
            // A failed pass (the volume fenced, or it closed) ends this round; the next wake
            // retries if there is still work.
            if let Ok(report) = self.pass() {
                let futile = if report.segments > 0 && report.gained == 0 {
                    dead
                } else {
                    u64::MAX
                };
                self.shared.futile_at.store(futile, Ordering::Relaxed);
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

    /// Cleans victims until free segments are one past the runway, or none is worth it.
    pub fn pass(&self) -> Result<CleanReport, ChunkError> {
        let low = self.shared.low_water.load(Ordering::Relaxed);
        if self.free() >= low {
            return Ok(CleanReport::default());
        }
        self.clean_until(|cleaner, _| {
            cleaner.free() <= cleaner.shared.low_water.load(Ordering::Relaxed)
        })
    }

    /// Cleans the `n` best victims, whatever the free space.
    pub fn clean_best(&self, n: u32) -> Result<CleanReport, ChunkError> {
        self.clean_until(|_, report| report.segments < n)
    }

    fn clean_until(
        &self,
        more: impl Fn(&Self, &CleanReport) -> bool,
    ) -> Result<CleanReport, ChunkError> {
        // One pass at a time, background or asked for, so two never take the same victims.
        let _cleaning = self
            .shared
            .cleaning
            .lock()
            .map_err(|_| ChunkError::Fenced)?;
        let mut report = CleanReport::default();
        let mut tried = Vec::new();
        let start = self.free();
        let (size, block) = (
            self.shared.geometry.segment_size,
            self.shared.geometry.block,
        );
        let batch = u64::try_from(self.batch_bytes).unwrap_or(u64::MAX).max(1);
        // Space the victims cleaned so far should give back once their live data is packed.
        let mut due = 0u64;
        while more(self, &report) {
            let Some(victim) = self.victim(&tried) else {
                break;
            };
            tried.push(victim);
            let live = self.live(victim);
            let started = Instant::now();
            let (relocated, corrupt) = self.clean(victim)?;
            report.segments = report.segments.saturating_add(1);
            report.relocated = report.relocated.saturating_add(relocated);
            report.corrupt = report.corrupt.saturating_add(corrupt);
            // The writer frees a segment in the batch after the one that emptied it.
            self.flush()?;
            self.learn(started.elapsed());
            // Victims of live fraction u free 1 − u segments each (Rosenblum and Ousterhout,
            // TOCS 1992, §3.4), so a net gain is due once they have held a segment's worth of
            // dead space, after ⌈1/(1 − ū)⌉ of them (docs/research/11 §10.3), less what packing
            // their live data costs: a block of padding per relocation batch and a share of a
            // segment header. None by then means it packs no tighter, and the pass stops.
            let packing = live.div_ceil(batch).saturating_add(1).saturating_mul(block);
            due = due.saturating_add(size.saturating_sub(live).saturating_sub(packing));
            if due >= size && self.free() <= start {
                break;
            }
        }
        report.gained = u32::try_from(self.free().saturating_sub(start)).unwrap_or(u32::MAX);
        Ok(report)
    }

    /// Live bytes of `segment`.
    fn live(&self, segment: u32) -> u64 {
        self.shared.usage.read().map_or(0, |usage| {
            usage
                .get(usize::try_from(segment).unwrap_or(usize::MAX))
                .map_or(0, |s| s.live)
        })
    }

    /// Records how long a victim took and moves the runway to match.
    fn learn(&self, took: Duration) {
        let took = u64::try_from(took.as_nanos()).unwrap_or(u64::MAX);
        let shared = &self.shared;
        let clean_ns = shared.clean_ns.fetch_max(took, Ordering::Relaxed).max(took);
        let low = runway(
            shared.peak_rate.load(Ordering::Relaxed),
            shared.service_ns.load(Ordering::Relaxed),
            clean_ns,
            u64::try_from(self.batch_bytes).unwrap_or(u64::MAX),
            shared.geometry.segment_size,
        );
        shared.low_water.store(low, Ordering::Relaxed);
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

    /// Relocates every live record of `segment`, found where they lie in the index's record
    /// places and each checked against the index as it is read; returns (relocated, corrupt).
    fn clean(&self, segment: u32) -> Result<(u64, u64), ChunkError> {
        let incarnation = self
            .shared
            .usage
            .read()
            .map_err(|_| ChunkError::Fenced)?
            .get(usize::try_from(segment).unwrap_or(usize::MAX))
            .map(|s| s.incarnation)
            .ok_or(ChunkError::Fenced)?;
        let offsets = self
            .shared
            .index
            .read()
            .map_err(|_| ChunkError::Fenced)?
            .placed_in(segment, 0..u32::MAX);
        let identity = Identity {
            volume: self.shared.volume,
            checksum_shift: self.shared.checksum_shift,
        };
        let (mut relocated, mut corrupt) = (0u64, 0u64);
        let mut moves: Vec<Move> = Vec::new();
        let mut bytes = 0usize;
        for offset in offsets {
            let mut data = Vec::new();
            let read = verify_at(
                &self.shared.file,
                &self.shared.pool,
                &self.shared.geometry,
                identity,
                segment,
                u64::from(offset),
                Some(&mut data),
            );
            let header = match read {
                Ok(Some(v)) if v.prefix.header.incarnation == incarnation => v.prefix.header,
                // Left in place: moving bytes that fail verification would launder them.
                Ok(_) | Err(ChunkError::Device(DiskError::Io { .. })) => {
                    corrupt = corrupt.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            };
            // Only a record the index still holds at this place is live.
            let found = {
                let index = self.shared.index.read().map_err(|_| ChunkError::Fenced)?;
                index
                    .live(
                        &header.key,
                        header.chunk_offset,
                        (segment, offset),
                        header.incarnation,
                        header.sequence,
                    )
                    .and_then(|from| {
                        let entry = index.get(&header.key)?;
                        let last = entry.fragments.last().map(|f| f.chunk_offset);
                        let flags = if entry.sealed && last == Some(from.chunk_offset) {
                            FLAG_FINAL
                        } else {
                            0
                        };
                        Some((from, flags, entry.time_ns))
                    })
            };
            let Some((from, flags, time_ns)) = found else {
                continue;
            };
            bytes = bytes.saturating_add(data.len());
            let payload = Payload::new(data, self.shared.checksum_shift)
                .ok_or_else(|| ChunkError::Config("checksum block size".into()))?;
            moves.push(Move {
                key: header.key,
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
                queued: None,
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
                queued: None,
            })
            .map_err(|_| ChunkError::Closed)?;
        answer.recv().map_err(|_| ChunkError::Closed)?
    }
}
