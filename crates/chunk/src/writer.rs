//! The group-commit loop: one per volume (docs/design/chunk-store.md §4).
//!
//! It takes every request that arrived while the previous batch was being made durable, the
//! zero-timer group commit of Helland et al. (Tandem TR 88.1, §2): one log write and flush
//! commits the group (DeWitt et al., SIGMOD 1984, §5.2). It validates them against the
//! index, lays their records into open segments, writes the records and one index frame, and
//! flushes the device once.
//! Only then does it publish the batch to readers and answer the requests. A failed write or
//! flush fences the volume; the flush is never retried (Rebello et al., ATC 2020).
//!
//! A batch that has just been answered frees its submitters to send their next requests,
//! but the next batch is formed at once from whatever is queued. With a few closed-loop
//! submitters, batches then alternate between those answered last time and the rest, and
//! every request waits for two flushes (docs/measurements/2026-09-28-chunk-store-benchmark.md,
//! finding 5). Research note 03 rule G6 adds an adaptive wait once
//! measurements show the flush rate limits throughput; the wait is derived, not chosen.
//! With `n` requests in the batch, waiting a further `dw` for one more that arrives with
//! probability `p` costs the batch `n·dw` of latency and saves the newcomer about `S − dw`,
//! where `S` is the batch's service time (writes and flush), because a request that misses
//! a batch waits for that batch to finish before its own starts. The expected total latency
//! falls while `dw < p·S / (n + p)`, so that is how long the writer waits for each next
//! request, and only while submitters it just answered have yet to return. `S` is measured
//! on every batch, and `p` is learned as the share of answered submitters that return while
//! the writer waits, a ratio of running counts so that each submitter weighs the same; both
//! decay with the gain of 1/8 that TCP uses for its round-trip estimate (Jacobson, SIGCOMM
//! 1988; RFC 6298 §2), which holds an estimate within 5% for samples varying by 20% and closes
//! 95% of a step in 23 batches (docs/research/11 §3.3).
//!
//! The rule is exact while submitters return well within `S`, the case measured, one newcomer
//! counts at a time, and a batch's service time does not grow with one more request, which
//! holds while the flush dominates. Submitters that take longer than `S/2` are never waited
//! for, even where waiting would lower total latency; the rule that covers them weighs, over
//! the measured distribution of return times, every returner a wait would gather against the
//! delay to the batch and to arrivals behind it, as anticipatory scheduling does (Iyer and
//! Druschel, SOSP 2001, §3.3; docs/research/11 §2.3, §2.6). It needs the return times of real
//! clients, which the gateway and replication will supply.
//!
//! Records go to one of two streams, each with its own open segment: new writes, and the
//! cleaner's relocations. Keeping relocated data apart groups data by age, which is what
//! lets later cleaning find segments that are mostly dead (Rosenblum and Ousterhout, TOCS
//! 1992, §3.6).

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, RwLock};

use mantle_disk::block::BlockFile;
use mantle_disk::buf::{AlignedBuf, Pool};

use crate::error::ChunkError;
use crate::frame::{
    self, DeleteRecord, KIND_BATCH, KIND_CHECKPOINT_BEGIN, KIND_CHECKPOINT_CHUNK,
    KIND_CHECKPOINT_END, KIND_WRAP, LogRecord, PutRecord, SegmentRecord, SegmentState,
};
use crate::index::{Fragment, Index, Inserted, SegmentInfo};
use crate::key::ChunkKey;
use crate::layout::{
    CHECKPOINT_RECORDS_PER_FRAME, Config, Geometry, batch_frame_bytes, checkpoint_bytes,
    largest_frame,
};
use crate::log::{self, Cursor};
use crate::record::{self, FLAG_FINAL, Payload, RecordHeader, SegmentHeader, Sink};
use crate::superblock::Superblock;

/// Free segments kept back from new writes so the cleaner can always relocate into one
/// (Rosenblum and Ousterhout, TOCS 1992, §3.6: the cleaner needs clean segments to work).
pub(crate) const CLEANER_RESERVE: usize = 1;

/// Record sequences and segment incarnations reserved at a time. The superblock records how
/// far they are reserved before any record carries a number beyond it, and recovery resumes
/// above the reservation. A freed segment keeps its records until they are overwritten, and
/// a batch whose flush failed may have left records on the device that the log never names;
/// numbers taken from what the log remembers could repeat theirs, and a reused incarnation
/// would let roll-forward take a stale record for a new one. Recovery skips what was
/// reserved but not used, the way Percolator's timestamp oracle does (docs/research/11 §11).
///
/// A reservation costs one superblock write and flush, `c`, so reserving `R` at a time for
/// numbers issued at rate `r` spends a fraction `c·r/R` of the writer's time on it. With the
/// measured durable 4 KiB write, `c` = 4.7 ms: 2^24 sequences keep that fraction under 10^-3
/// up to 3.6M puts a second, 1,000 times this machine's measured 3.7K, and 2^16 incarnations
/// do so to 14K new segments a second, where 3 GB/s into 256 MiB segments opens 11. Each
/// crash skips at most one reservation of 2^64, so neither space can run out.
pub(crate) const SEQUENCE_RESERVE: u64 = 1 << 24;
pub(crate) const INCARNATION_RESERVE: u64 = 1 << 16;

pub(crate) enum Op {
    Write {
        key: ChunkKey,
        offset: u64,
        payload: Payload,
        seal: bool,
    },
    Delete {
        key: ChunkKey,
    },
    /// Moves fragments' verified bytes to the cleaner's stream, together in one batch so they
    /// pack densely. Each move is applied only if its chunk still holds exactly that fragment,
    /// so a concurrent delete or rewrite wins.
    Relocate {
        moves: Vec<Move>,
    },
    /// Writes the whole index into the log after the batch it arrives in.
    Checkpoint,
}

/// One fragment the cleaner moves.
pub(crate) struct Move {
    pub key: ChunkKey,
    pub from: Fragment,
    pub payload: Payload,
    pub flags: u8,
    pub time_ns: u64,
}

pub(crate) struct Request {
    pub op: Op,
    pub reply: SyncSender<Result<(), ChunkError>>,
    /// Payload bytes the request holds of the client queue's room (`Queue`); `None` for the
    /// cleaner's requests, which it sends one at a time outside that room.
    pub queued: Option<u64>,
}

/// The room client requests hold in the writer's queue (docs/design/chunk-store.md §4).
#[derive(Debug, Default)]
pub(crate) struct Queue {
    requests: usize,
    bytes: u64,
}

impl Queue {
    /// Takes room for a request of `bytes` payload bytes, or refuses with `Busy`. A payload
    /// larger than the whole byte bound is admitted into a queue holding no other payload.
    pub fn admit(&mut self, bytes: u64, limits: &crate::layout::Limits) -> Result<(), ChunkError> {
        let requests = self.requests.checked_add(1).ok_or(ChunkError::Busy)?;
        let total = self.bytes.checked_add(bytes).ok_or(ChunkError::Busy)?;
        if requests > limits.queue_requests() || (self.bytes > 0 && total > limits.queue_bytes()) {
            return Err(ChunkError::Busy);
        }
        self.requests = requests;
        self.bytes = total;
        Ok(())
    }

    /// Gives back the room of a request that left the queue or was never sent.
    pub fn release(&mut self, bytes: u64) {
        self.requests = self.requests.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(bytes);
    }
}

/// What readers, the writer and the cleaner share.
pub(crate) struct Shared<F> {
    pub file: F,
    pub geometry: Geometry,
    pub volume: u128,
    pub checksum_shift: u8,
    /// Buffers for reading records: reads, verification, cleaning and scrubbing.
    pub pool: Pool,
    pub index: RwLock<Index>,
    pub fenced: std::sync::atomic::AtomicBool,
    /// Set when the volume closes: background threads stop at their next wake.
    pub stopping: std::sync::atomic::AtomicBool,
    /// Bytes of records deleted since the volume opened: the cleaner waits for this to grow
    /// after a pass that could not gain space. Relocations do not count; they free nothing.
    pub dead_bytes: std::sync::atomic::AtomicU64,
    /// `dead_bytes` when the cleaner's last pass gained no segment, `u64::MAX` if it gained or
    /// none has run: while the two are equal, cleaning cannot free space and a client write
    /// with no segment to go to is `Full` rather than `Busy`.
    pub futile_at: std::sync::atomic::AtomicU64,
    /// Requests submitted to the writer so far, counted just before each is sent.
    pub submitted: std::sync::atomic::AtomicU64,
    /// Checkpoints written since the volume opened.
    pub checkpoints: std::sync::atomic::AtomicU64,
    /// The room client requests hold in the writer's queue.
    pub queue: std::sync::Mutex<Queue>,
    /// The fastest the writer has laid down payload, in bytes a second: a batch's payload
    /// over its service time, the largest since the volume opened.
    pub peak_rate: std::sync::atomic::AtomicU64,
    /// The writer's moving average of a batch's service time, in nanoseconds.
    pub service_ns: std::sync::atomic::AtomicU64,
    /// The longest the cleaner has taken to clean one victim, in nanoseconds.
    pub clean_ns: std::sync::atomic::AtomicU64,
    /// Clean below this many free segments (`runway`).
    pub low_water: std::sync::atomic::AtomicUsize,
    /// Held through a cleaning pass: the background cleaner's or one asked for.
    pub cleaning: std::sync::Mutex<()>,
    pub usage: RwLock<Vec<SegmentInfo>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stream {
    Client = 0,
    Clean = 1,
}

/// The writer's state, owned by its thread.
pub(crate) struct Writer<F: BlockFile> {
    pub shared: Arc<Shared<F>>,
    pub config: Config,
    pub rx: Receiver<Request>,
    pub segments: Vec<SegmentInfo>,
    /// The open segment of each stream.
    pub opens: [Option<u32>; 2],
    /// Segments recovery found open beyond one per stream; sealed by the first batch.
    pub stale: Vec<u32>,
    pub incarnation: u64,
    pub sequence: u64,
    pub cursor: Cursor,
    pub superblock: Superblock,
    pub fragments: u64,
    /// Wakes the cleaner when free segments run low.
    pub poke: Option<SyncSender<()>>,
    /// Buffers for the batches the writer lays out, reused from batch to batch.
    pub pool: Pool,
    /// Moving average of a batch's service time (its writes and flush), in nanoseconds; zero
    /// until one is measured.
    pub service_ns: u64,
    /// Running counts, in 1/2^16ths and decaying by 1/8 a batch, of answered submitters that
    /// returned while the writer waited and of submitters answered. They start at one of two,
    /// Laplace's rule of succession for a share never observed.
    pub returns: (u64, u64),
    /// Requests received from the queue so far.
    pub received: u64,
}

/// The unit of the return counts and of `p`: one.
pub(crate) const RATE_ONE: u64 = 1 << 16;

/// One step of a moving average with gain 1/8 (RFC 6298 §2).
fn smooth(average: u64, sample: u64) -> u64 {
    average
        .saturating_sub(average / 8)
        .saturating_add(sample / 8)
}

/// A chunk as the batch being validated sees it: the index plus earlier requests in the batch.
#[derive(Clone)]
struct View {
    len: u64,
    sealed: bool,
    fragments: usize,
    /// Fragments added earlier in this batch: (chunk offset, length, CRC-32C).
    pending: Vec<(u64, u64, u32)>,
    /// A request earlier in this batch changed the chunk.
    touched: bool,
}

enum Decision {
    /// Write a fragment whose payload has this CRC-32C.
    Write(u32),
    /// Relocate the moves at these positions; the others' chunks changed.
    Relocate(Vec<usize>),
    Delete,
    /// Nothing to write: the request is already satisfied (a retry, a relocation of a chunk
    /// that changed, or deleting nothing).
    Done,
}

struct Accepted {
    request: Request,
    decision: Decision,
}

/// One contiguous run of one segment written by this batch: where it starts and ends, and
/// what goes in it, in order. It is encoded only once the whole batch is placed, straight
/// into one aligned buffer of exactly its size.
struct Region {
    segment: u32,
    /// Where the region starts in the segment, block-aligned.
    start: u64,
    /// Where its last record ends.
    end: u64,
    parts: Vec<Part>,
}

enum Part {
    /// A newly opened segment's first block.
    Segment(SegmentHeader),
    /// A data record; `payload` indexes the batch's payloads.
    Record {
        header: RecordHeader,
        payload: usize,
    },
}

/// One batch's placement of records into segments.
struct Layout {
    geometry: Geometry,
    volume: u128,
    now: u64,
    records: Vec<LogRecord>,
    regions: Vec<Region>,
    /// Where each touched segment's next record goes.
    positions: HashMap<u32, u64>,
    /// Segments this batch opened, with their incarnations.
    opened: Vec<(u32, u64)>,
    sealed: Vec<(u32, u32)>,
    freed: Vec<u32>,
    opens: [Option<u32>; 2],
    incarnation: u64,
    free: Vec<u32>,
    /// Incarnation and write position of each segment before this batch.
    before: HashMap<u32, (u64, u64)>,
}

impl Layout {
    fn incarnation_of(&self, segment: u32) -> u64 {
        self.opened
            .iter()
            .rev()
            .find(|(s, _)| *s == segment)
            .map(|(_, i)| *i)
            .or_else(|| self.before.get(&segment).map(|(i, _)| *i))
            .unwrap_or(0)
    }

    /// Finds room for `len` bytes in `stream`, sealing its segment and opening another when
    /// it is full. New writes leave `CLEANER_RESERVE` free segments to the cleaner.
    fn place(&mut self, stream: Stream, len: u64) -> Option<(u32, u64)> {
        let block = self.geometry.block;
        let slot = stream as usize;
        if let Some(s) = self.opens.get(slot).copied().flatten() {
            let written = self.before.get(&s).map_or(block, |(_, w)| (*w).max(block));
            let at = *self.positions.entry(s).or_insert(written);
            if at.saturating_add(len) <= self.geometry.segment_size {
                return Some((s, at));
            }
            let pos = at.checked_next_multiple_of(block).unwrap_or(at);
            let incarnation = self.incarnation_of(s);
            self.records.push(LogRecord::Segment(SegmentRecord {
                segment: s,
                incarnation,
                state: SegmentState::Sealed,
                write_pos: u32::try_from(pos).unwrap_or(u32::MAX),
            }));
            self.sealed
                .push((s, u32::try_from(pos).unwrap_or(u32::MAX)));
            if let Some(o) = self.opens.get_mut(slot) {
                *o = None;
            }
        }
        let reserve = if stream == Stream::Client {
            CLEANER_RESERVE
        } else {
            0
        };
        if self.free.len() <= reserve {
            return None;
        }
        let next = self.free.pop()?;
        self.incarnation = self.incarnation.saturating_add(1);
        self.records.push(LogRecord::Segment(SegmentRecord {
            segment: next,
            incarnation: self.incarnation,
            state: SegmentState::Open,
            write_pos: u32::try_from(block).unwrap_or(u32::MAX),
        }));
        let header = SegmentHeader {
            volume: self.volume,
            segment: next,
            incarnation: self.incarnation,
            time_ns: self.now,
        };
        self.regions.push(Region {
            segment: next,
            start: 0,
            end: block,
            parts: vec![Part::Segment(header)],
        });
        self.positions.insert(next, block);
        self.opened.push((next, self.incarnation));
        if let Some(o) = self.opens.get_mut(slot) {
            *o = Some(next);
        }
        Some((next, block))
    }

    /// Stages a record of `len` bytes at `at` in `segment` and advances the segment's
    /// position.
    fn stage(&mut self, segment: u32, at: u64, len: u64, header: RecordHeader, payload: usize) {
        let region = match self.regions.iter_mut().rev().find(|r| r.segment == segment) {
            Some(region) => region,
            None => {
                self.regions.push(Region {
                    segment,
                    start: at,
                    end: at,
                    parts: Vec::new(),
                });
                match self.regions.last_mut() {
                    Some(region) => region,
                    None => return,
                }
            }
        };
        region.parts.push(Part::Record { header, payload });
        region.end = at.saturating_add(len);
        self.positions.insert(segment, region.end);
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

type Reply = SyncSender<Result<(), ChunkError>>;

fn answer(tx: Reply, result: Result<(), ChunkError>) {
    // The submitter may have given up waiting; its answer has nowhere to go.
    let _ = tx.send(result);
}

fn reply(request: Request, result: Result<(), ChunkError>) {
    answer(request.reply, result);
}

fn slot<T>(items: &mut [T], index: u32) -> Option<&mut T> {
    items.get_mut(usize::try_from(index).ok()?)
}

impl<F: BlockFile> Writer<F> {
    pub fn run(mut self) {
        // Requests the previous batch answered, and requests already submitted when it did.
        let mut answered = 0u64;
        let mut backlog = 0u64;
        while let Ok(first) = self.rx.recv() {
            self.dequeued(&first);
            let mut bytes = request_bytes(&first);
            let mut batch = vec![first];
            while self.room(&batch, bytes) {
                match self.rx.try_recv() {
                    Ok(next) => {
                        self.dequeued(&next);
                        bytes = bytes.saturating_add(request_bytes(&next));
                        batch.push(next);
                    }
                    Err(_) => break,
                }
            }
            self.gather(&mut batch, &mut bytes, answered, backlog);
            answered = u64::try_from(batch.len()).unwrap_or(u64::MAX);
            if self.shared.fenced.load(Ordering::Acquire) {
                for request in batch {
                    reply(request, Err(ChunkError::Fenced));
                }
            } else {
                self.process(batch);
                self.poke_cleaner();
            }
            // Requests submitted before these answers went out are queued ahead of any the
            // answered submitters send next.
            backlog = self
                .shared
                .submitted
                .load(Ordering::Acquire)
                .saturating_sub(self.received);
        }
    }

    /// Counts a request out of the queue and gives back the room it held there.
    fn dequeued(&mut self, request: &Request) {
        self.received = self.received.saturating_add(1);
        if let Some(bytes) = request.queued
            && let Ok(mut queue) = self.shared.queue.lock()
        {
            queue.release(bytes);
        }
    }

    fn room(&self, batch: &[Request], bytes: usize) -> bool {
        batch.len() < self.config.limits.batch_requests && bytes < self.config.limits.batch_bytes
    }

    /// Waits for the submitters the last batch answered while waiting is expected to lower
    /// total latency (module docs), and learns how many of them return.
    fn gather(&mut self, batch: &mut Vec<Request>, bytes: &mut usize, answered: u64, backlog: u64) {
        if answered == 0 {
            return;
        }
        // The first `backlog` requests received were sent before the answers.
        let returned = |batch: &Vec<Request>| {
            u64::try_from(batch.len())
                .unwrap_or(u64::MAX)
                .saturating_sub(backlog)
        };
        while self.service_ns > 0 && returned(batch) < answered && self.room(batch, *bytes) {
            // dw < p·S / (n + p), with p in 1/2^16ths.
            let n = u128::try_from(batch.len()).unwrap_or(u128::MAX);
            let p = u128::from(self.return_share());
            let step = p
                .saturating_mul(u128::from(self.service_ns))
                .checked_div(n.saturating_mul(u128::from(RATE_ONE)).saturating_add(p))
                .and_then(|ns| u64::try_from(ns).ok())
                .unwrap_or(0);
            if step == 0 {
                break;
            }
            match self.rx.recv_timeout(std::time::Duration::from_nanos(step)) {
                Ok(next) => {
                    self.dequeued(&next);
                    *bytes = bytes.saturating_add(request_bytes(&next));
                    batch.push(next);
                }
                Err(_) => break,
            }
        }
        let came = returned(batch).min(answered).saturating_mul(RATE_ONE);
        self.returns = (
            smooth(self.returns.0, came.saturating_mul(8)),
            smooth(
                self.returns.1,
                answered.saturating_mul(RATE_ONE).saturating_mul(8),
            ),
        );
    }

    /// `p`: the share of answered submitters that return while the writer waits.
    fn return_share(&self) -> u64 {
        self.returns
            .0
            .saturating_mul(RATE_ONE)
            .checked_div(self.returns.1)
            .unwrap_or(RATE_ONE / 2)
            .min(RATE_ONE)
    }

    fn fence(&self) {
        self.shared.fenced.store(true, Ordering::Release);
    }

    /// Whether cleaning may still free a segment: unless it was tried on exactly the data
    /// deleted so far and gained nothing. Only trying can tell, since how tightly relocated
    /// records pack is what decides.
    fn reclaimable(&self) -> bool {
        self.shared.futile_at.load(Ordering::Relaxed)
            != self.shared.dead_bytes.load(Ordering::Relaxed)
    }

    fn free_segments(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| s.state == SegmentState::Free)
            .count()
    }

    fn poke_cleaner(&self) {
        if self.free_segments() < self.shared.low_water.load(Ordering::Relaxed)
            && let Some(poke) = &self.poke
        {
            // A poke already waiting is as good as a second one.
            let _ = poke.try_send(());
        }
    }

    fn process(&mut self, batch: Vec<Request>) {
        let (checkpoints, batch): (Vec<Request>, Vec<Request>) = batch
            .into_iter()
            .partition(|r| matches!(r.op, Op::Checkpoint));
        if self.checkpoint_due(batch.len())
            && let Err(e) = self.checkpoint()
        {
            self.fence();
            let mut first = Some(e);
            for request in batch.into_iter().chain(checkpoints) {
                reply(request, Err(first.take().unwrap_or(ChunkError::Fenced)));
            }
            return;
        }
        self.handle(batch);
        if checkpoints.is_empty() {
            return;
        }
        let result = if self.shared.fenced.load(Ordering::Acquire) {
            Err(ChunkError::Fenced)
        } else {
            self.checkpoint()
        };
        match result {
            Ok(()) => {
                for request in checkpoints {
                    reply(request, Ok(()));
                }
            }
            Err(e) => {
                self.fence();
                let mut first = Some(e);
                for request in checkpoints {
                    reply(request, Err(first.take().unwrap_or(ChunkError::Fenced)));
                }
            }
        }
    }

    /// Whether the log must be checkpointed before a batch of `requests` requests: after the
    /// live log, the batch's frame, then a wrap and a checkpoint of the index as the batch may
    /// leave it must fit, or the log would fill with nothing able to free it. Waiting until then
    /// leaves at least `3·C_max − 2·C` of frames between checkpoints of size `C` (the log's
    /// sizing, layout.rs), so a checkpoint takes at most half the log's writes, at the full
    /// budget, and far less below it; and replay never exceeds one log (docs/research/11 §9.3).
    fn checkpoint_due(&self, requests: usize) -> bool {
        let geometry = &self.shared.geometry;
        let reserve = || {
            let checkpoint = checkpoint_bytes(
                self.fragments.checked_add(u64::try_from(requests).ok()?)?,
                u64::from(geometry.segments),
                geometry.block,
            )?;
            let batch = batch_frame_bytes(self.config.limits.batch_requests, geometry.block)?;
            let wrap = largest_frame(&self.config, geometry.block)?;
            checkpoint
                .checked_add(batch)?
                .checked_add(wrap)?
                .checked_add(self.cursor.used)
        };
        reserve().is_none_or(|need| need > geometry.log_size)
    }

    /// Validates and commits the batch's writes, deletes and relocations.
    fn handle(&mut self, batch: Vec<Request>) {
        let accepted = match self.validate(batch) {
            Ok(accepted) => accepted,
            Err(()) => return,
        };
        let block = self.shared.geometry.block;
        let reclaimable = self.segments.iter().any(|s| reclaimable(s, block));
        if accepted.is_empty() && self.stale.is_empty() && !reclaimable {
            return;
        }
        self.commit(accepted);
    }

    /// Answers requests that need no write, refuses invalid ones, and returns the rest.
    fn validate(&mut self, batch: Vec<Request>) -> Result<Vec<Accepted>, ()> {
        let index = match self.shared.index.read() {
            Ok(index) => index,
            Err(_) => {
                self.fence();
                for request in batch {
                    reply(request, Err(ChunkError::Fenced));
                }
                return Err(());
            }
        };
        let limits = self.config.limits;
        let max_payload = max_payload(&self.shared.geometry, self.shared.checksum_shift);
        let mut views: HashMap<ChunkKey, Option<View>> = HashMap::new();
        let mut added: u64 = 0;
        let mut accepted = Vec::with_capacity(batch.len());
        let view_of = |views: &mut HashMap<ChunkKey, Option<View>>, key: &ChunkKey| {
            views
                .entry(*key)
                .or_insert_with(|| {
                    index.get(key).map(|e| View {
                        len: e.len(),
                        sealed: e.sealed,
                        fragments: e.fragments.len(),
                        pending: Vec::new(),
                        touched: false,
                    })
                })
                .clone()
        };
        for request in batch {
            let decision = match &request.op {
                Op::Write {
                    key,
                    offset,
                    payload,
                    seal,
                } => {
                    // Appending nothing without sealing writes nothing. A zero-length fragment
                    // may only end a chunk: fragments are found by their starting offset, which
                    // must be unique.
                    if payload.data.is_empty() && !*seal {
                        reply(request, Ok(()));
                        continue;
                    }
                    let view = view_of(&mut views, key);
                    let len = u64::try_from(payload.data.len()).unwrap_or(u64::MAX);
                    let crc = payload.crc;
                    let existing = index
                        .get(key)
                        .and_then(|e| e.fragment_at(*offset))
                        .map(|f| (u64::from(f.payload_len), f.payload_crc))
                        .or_else(|| {
                            view.as_ref().and_then(|v| {
                                v.pending
                                    .iter()
                                    .find(|(o, _, _)| o == offset)
                                    .map(|(_, l, c)| (*l, *c))
                            })
                        });
                    match view {
                        Some(v) if *offset < v.len || (v.sealed && *offset == v.len) => {
                            if existing == Some((len, crc)) && (!seal || v.sealed) {
                                Ok(Decision::Done)
                            } else if *offset == 0 && *seal {
                                Err(ChunkError::Exists(*key))
                            } else if v.sealed {
                                Err(ChunkError::Sealed(*key))
                            } else {
                                Err(ChunkError::Conflict {
                                    key: *key,
                                    offset: *offset,
                                })
                            }
                        }
                        Some(v) if *offset > v.len => Err(ChunkError::Gap {
                            key: *key,
                            offset: *offset,
                            end: v.len,
                        }),
                        None if *offset > 0 => Err(ChunkError::Gap {
                            key: *key,
                            offset: *offset,
                            end: 0,
                        }),
                        v => {
                            let fragments = v.as_ref().map_or(0, |v| v.fragments);
                            if len > max_payload {
                                Err(ChunkError::TooLarge {
                                    len,
                                    max: max_payload,
                                })
                            } else if fragments >= limits.fragments_per_chunk {
                                Err(ChunkError::TooManyFragments(*key))
                            } else if self.fragments.saturating_add(added)
                                >= self.config.max_fragments
                            {
                                Err(ChunkError::Full)
                            } else {
                                added = added.saturating_add(1);
                                let mut next = v.unwrap_or(View {
                                    len: 0,
                                    sealed: false,
                                    fragments: 0,
                                    pending: Vec::new(),
                                    touched: false,
                                });
                                next.pending.push((*offset, len, crc));
                                next.len = next.len.saturating_add(len);
                                next.sealed = *seal;
                                next.fragments = next.fragments.saturating_add(1);
                                next.touched = true;
                                views.insert(*key, Some(next));
                                Ok(Decision::Write(crc))
                            }
                        }
                    }
                }
                Op::Delete { key } => {
                    if view_of(&mut views, key).is_some() {
                        views.insert(*key, None);
                        Ok(Decision::Delete)
                    } else {
                        Ok(Decision::Done)
                    }
                }
                Op::Relocate { moves } => {
                    let current: Vec<usize> = moves
                        .iter()
                        .enumerate()
                        .filter(|(_, m)| {
                            let now = index
                                .get(&m.key)
                                .and_then(|e| e.fragment_at(m.from.chunk_offset));
                            let untouched = views
                                .get(&m.key)
                                .is_none_or(|v| v.as_ref().is_some_and(|v| !v.touched));
                            now == Some(&m.from) && untouched && m.payload.crc == m.from.payload_crc
                        })
                        .map(|(i, _)| i)
                        .collect();
                    if current.is_empty() {
                        Ok(Decision::Done)
                    } else {
                        Ok(Decision::Relocate(current))
                    }
                }
                // Taken out of the batch before validation (`process`).
                Op::Checkpoint => Ok(Decision::Done),
            };
            match decision {
                Ok(Decision::Done) => reply(request, Ok(())),
                Ok(decision) => accepted.push(Accepted { request, decision }),
                Err(e) => reply(request, Err(e)),
            }
        }
        Ok(accepted)
    }

    fn layout(&self, requests: usize) -> Layout {
        let mut before = HashMap::new();
        for (i, s) in self.segments.iter().enumerate() {
            if s.state != SegmentState::Free
                && let Ok(i) = u32::try_from(i)
            {
                before.insert(i, (s.incarnation, u64::from(s.write_pos)));
            }
        }
        // Sealed segments with nothing live are freed in this frame, so this batch may reuse
        // them; at most one per request keeps the frame within the log's headroom
        // (layout::batch_frame_bytes).
        let block = self.shared.geometry.block;
        let freed: Vec<u32> = self
            .segments
            .iter()
            .enumerate()
            .filter(|(_, s)| reclaimable(s, block))
            .filter_map(|(i, _)| u32::try_from(i).ok())
            .take(requests.max(1))
            .collect();
        // A stream whose open segment is freed opens a new one when it next writes.
        let mut opens = self.opens;
        for open in &mut opens {
            if open.is_some_and(|s| freed.contains(&s)) {
                *open = None;
            }
        }
        let mut free: Vec<u32> = self
            .segments
            .iter()
            .enumerate()
            .filter(|(_, s)| s.state == SegmentState::Free)
            .filter_map(|(i, _)| u32::try_from(i).ok())
            .chain(freed.iter().copied())
            .collect();
        // Lowest segment first: popped from the end.
        free.sort_unstable_by(|a, b| b.cmp(a));
        free.dedup();
        let mut records = Vec::new();
        for &segment in &freed {
            if let Some(&(incarnation, _)) = before.get(&segment) {
                records.push(LogRecord::Segment(SegmentRecord {
                    segment,
                    incarnation,
                    state: SegmentState::Free,
                    write_pos: 0,
                }));
            }
        }
        let mut sealed = Vec::new();
        for &segment in &self.stale {
            if let Some(&(incarnation, pos)) = before.get(&segment) {
                let pos = u32::try_from(pos).unwrap_or(u32::MAX);
                records.push(LogRecord::Segment(SegmentRecord {
                    segment,
                    incarnation,
                    state: SegmentState::Sealed,
                    write_pos: pos,
                }));
                sealed.push((segment, pos));
            }
        }
        Layout {
            geometry: self.shared.geometry,
            volume: self.shared.volume,
            now: now_ns(),
            records,
            regions: Vec::new(),
            positions: HashMap::new(),
            opened: Vec::new(),
            sealed,
            freed,
            opens,
            incarnation: self.incarnation,
            free,
            before,
        }
    }

    /// Lays out, writes, flushes, publishes and answers one batch.
    fn commit(&mut self, accepted: Vec<Accepted>) {
        let mut layout = self.layout(accepted.len());
        let mut sequence = self.sequence;
        let mut ok: Vec<Reply> = Vec::with_capacity(accepted.len());
        let mut payloads: Vec<Payload> = Vec::with_capacity(accepted.len());
        let shift = self.shared.checksum_shift;
        for Accepted { request, decision } in accepted {
            let Request { op, reply: tx, .. } = request;
            match (op, decision) {
                (
                    Op::Write {
                        key,
                        offset,
                        payload,
                        seal,
                    },
                    Decision::Write(crc),
                ) => {
                    sequence = sequence.saturating_add(1);
                    let flags = if seal { FLAG_FINAL } else { 0 };
                    let put = Put {
                        key,
                        chunk_offset: offset,
                        flags,
                        sequence,
                        time_ns: layout.now,
                        crc,
                    };
                    payloads.push(payload);
                    let index = payloads.len().saturating_sub(1);
                    match place_record(&mut layout, Stream::Client, shift, &put, &payloads, index) {
                        Ok(record) => {
                            layout.records.push(LogRecord::Put(record));
                            ok.push(tx);
                        }
                        // No free segment for the client: while cleaning can free one, the
                        // put is to be retried, not refused for good.
                        Err(ChunkError::Full) if self.reclaimable() => {
                            answer(tx, Err(ChunkError::Busy));
                        }
                        Err(e) => answer(tx, Err(e)),
                    }
                }
                (Op::Relocate { moves }, Decision::Relocate(current)) => {
                    let mut result = Ok(());
                    for (i, m) in moves.into_iter().enumerate() {
                        if !current.contains(&i) {
                            continue;
                        }
                        let put = Put {
                            key: m.key,
                            chunk_offset: m.from.chunk_offset,
                            flags: m.flags,
                            sequence: m.from.sequence,
                            time_ns: m.time_ns,
                            crc: m.from.payload_crc,
                        };
                        payloads.push(m.payload);
                        let index = payloads.len().saturating_sub(1);
                        match place_record(
                            &mut layout,
                            Stream::Clean,
                            shift,
                            &put,
                            &payloads,
                            index,
                        ) {
                            Ok(record) => layout.records.push(LogRecord::Put(record)),
                            Err(e) => {
                                result = Err(e);
                                break;
                            }
                        }
                    }
                    match result {
                        Ok(()) => ok.push(tx),
                        Err(e) => answer(tx, Err(e)),
                    }
                }
                (Op::Delete { key }, Decision::Delete) => {
                    sequence = sequence.saturating_add(1);
                    layout.records.push(LogRecord::Delete(DeleteRecord {
                        key,
                        sequence,
                        time_ns: layout.now,
                    }));
                    ok.push(tx);
                }
                _ => answer(tx, Ok(())),
            }
        }
        if ok.is_empty() && layout.freed.is_empty() && layout.sealed.is_empty() {
            return;
        }

        let result = self
            .reserve(sequence, layout.incarnation)
            .and_then(|()| self.write_batch(&layout, &payloads))
            .and_then(|()| self.publish(&layout));
        match result {
            Err(e) => {
                self.fence();
                let mut first = Some(e);
                for tx in ok {
                    answer(tx, Err(first.take().unwrap_or(ChunkError::Fenced)));
                }
            }
            Ok(()) => {
                self.incarnation = layout.incarnation;
                self.sequence = sequence;
                self.stale.clear();
                for tx in ok {
                    answer(tx, Ok(()));
                }
            }
        }
    }

    /// Raises the superblock's reservations, durably, if `sequence` or `incarnation` is past
    /// them; runs before any record carrying either is written.
    fn reserve(&mut self, sequence: u64, incarnation: u64) -> Result<(), ChunkError> {
        if sequence <= self.superblock.sequence_limit
            && incarnation <= self.superblock.incarnation_limit
        {
            return Ok(());
        }
        let mut superblock = self.superblock.clone();
        superblock.sequence = superblock.sequence.saturating_add(1);
        superblock.sequence_limit = superblock
            .sequence_limit
            .max(sequence.saturating_add(SEQUENCE_RESERVE));
        superblock.incarnation_limit = superblock
            .incarnation_limit
            .max(incarnation.saturating_add(INCARNATION_RESERVE));
        write_superblock(&self.shared.file, &superblock)?;
        self.shared.file.sync_data().map_err(ChunkError::Device)?;
        self.superblock = superblock;
        Ok(())
    }

    /// Encodes each region straight into an aligned buffer of its size, writes it, appends
    /// the batch's index frame and flushes once.
    fn write_batch(&mut self, layout: &Layout, payloads: &[Payload]) -> Result<(), ChunkError> {
        let started = std::time::Instant::now();
        let geometry = self.shared.geometry;
        let block = geometry.block_usize();
        for region in &layout.regions {
            let len = usize::try_from(region.end.saturating_sub(region.start))
                .map_err(|_| ChunkError::Full)?;
            let padded = len
                .checked_next_multiple_of(block)
                .ok_or(ChunkError::Full)?;
            let mut buf = self
                .pool
                .take(padded)
                .map_err(|e| ChunkError::Device(e.into()))?;
            for part in &region.parts {
                let encoded = match part {
                    Part::Segment(header) => (*buf).put(&header.encode(block)),
                    Part::Record { header, payload } => payloads
                        .get(*payload)
                        .and_then(|p| record::encode(header, &p.table, &p.data, &mut *buf)),
                };
                encoded.ok_or(ChunkError::Internal("a record did not encode as placed"))?;
            }
            if buf.len() != len {
                return Err(ChunkError::Internal(
                    "a region's records did not fill it as placed",
                ));
            }
            buf.extend_zeros(padded.saturating_sub(len))
                .map_err(|e| ChunkError::Device(e.into()))?;
            let at = geometry
                .segment_offset(region.segment)
                .and_then(|o| o.checked_add(region.start))
                .ok_or(ChunkError::Full)?;
            self.shared
                .file
                .write_all_at(buf.as_slice(), at)
                .map_err(ChunkError::Device)?;
        }
        let group = self.cursor.lsn;
        self.append_frame(KIND_BATCH, &layout.records, group)?;
        self.shared.file.sync_data().map_err(ChunkError::Device)?;
        let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.service_ns = if self.service_ns == 0 {
            took
        } else {
            smooth(self.service_ns, took)
        };
        let laid: u64 = payloads
            .iter()
            .map(|p| u64::try_from(p.data.len()).unwrap_or(u64::MAX))
            .fold(0, u64::saturating_add);
        let rate = u128::from(laid)
            .saturating_mul(1_000_000_000)
            .checked_div(u128::from(took))
            .and_then(|r| u64::try_from(r).ok())
            .unwrap_or(0);
        self.shared.peak_rate.fetch_max(rate, Ordering::Relaxed);
        self.shared
            .service_ns
            .store(self.service_ns, Ordering::Relaxed);
        Ok(())
    }

    /// Appends one frame to the log (preceded by a wrap frame when it must go around). `group`
    /// is the LSN of the first frame written since the last completed flush.
    fn append_frame(
        &mut self,
        kind: u16,
        records: &[LogRecord],
        group: u64,
    ) -> Result<(u64, u64), ChunkError> {
        let geometry = self.shared.geometry;
        let block = geometry.block_usize();
        let align = self.shared.file.alignment();
        let probe = frame::encode(kind, 0, 0, self.shared.volume, records, block)
            .ok_or(ChunkError::Full)?;
        let len = u64::try_from(probe.len()).map_err(|_| ChunkError::Full)?;
        let placement = self
            .cursor
            .place(len, geometry.block, geometry.log_size)
            .ok_or(ChunkError::Full)?;
        let mut lsn = self.cursor.lsn;
        if placement.wrap {
            let wrap = frame::encode(KIND_WRAP, lsn, group, self.shared.volume, &[], block)
                .ok_or(ChunkError::Full)?;
            log::write_frame(&self.shared.file, &geometry, self.cursor.pos, &wrap, align)
                .map_err(ChunkError::Device)?;
            lsn = lsn.saturating_add(1);
        }
        let bytes = frame::encode(kind, lsn, group, self.shared.volume, records, block)
            .ok_or(ChunkError::Full)?;
        log::write_frame(&self.shared.file, &geometry, placement.at, &bytes, align)
            .map_err(ChunkError::Device)?;
        self.cursor.advance(placement, len, geometry.log_size);
        Ok((placement.at, lsn))
    }

    /// Applies a durable batch to the index and segment table, where readers see it.
    fn publish(&mut self, layout: &Layout) -> Result<(), ChunkError> {
        let per_chunk = self.config.limits.fragments_per_chunk;
        let block = self.shared.geometry.block;
        let mut index = self.shared.index.write().map_err(|_| ChunkError::Fenced)?;
        for &segment in &layout.freed {
            if let Some(info) = slot(&mut self.segments, segment) {
                *info = SegmentInfo::FREE;
            }
        }
        for &(segment, incarnation) in &layout.opened {
            if let Some(info) = slot(&mut self.segments, segment) {
                *info = SegmentInfo {
                    state: SegmentState::Open,
                    incarnation,
                    write_pos: u32::try_from(block).unwrap_or(u32::MAX),
                    live: 0,
                    youngest_ns: 0,
                };
            }
        }
        for record in &layout.records {
            match record {
                LogRecord::Put(p) => {
                    let outcome =
                        index
                            .insert(p.key, p, per_chunk)
                            .map_err(|_| ChunkError::CorruptLog {
                                lsn: self.cursor.lsn,
                            })?;
                    let credit = match outcome {
                        Inserted::Added => {
                            self.fragments = self.fragments.saturating_add(1);
                            true
                        }
                        // A relocated copy: the old one dies as an equal one is born, so no
                        // space is freed that cleaning could gain.
                        Inserted::Replaced(old) => {
                            if let Some(info) = slot(&mut self.segments, old.segment) {
                                info.live = info.live.saturating_sub(u64::from(old.record_len));
                            }
                            true
                        }
                        Inserted::Unchanged => false,
                    };
                    if credit && let Some(info) = slot(&mut self.segments, p.segment) {
                        info.live = info.live.saturating_add(u64::from(p.record_len));
                        info.youngest_ns = info.youngest_ns.max(p.time_ns);
                    }
                }
                LogRecord::Delete(d) => {
                    if let Some(entry) = index.remove(&d.key) {
                        for f in &entry.fragments {
                            self.fragments = self.fragments.saturating_sub(1);
                            if let Some(info) = slot(&mut self.segments, f.segment) {
                                info.live = info.live.saturating_sub(u64::from(f.record_len));
                            }
                            self.shared
                                .dead_bytes
                                .fetch_add(u64::from(f.record_len), Ordering::Relaxed);
                        }
                    }
                }
                LogRecord::Segment(_) => {}
            }
        }
        drop(index);
        for region in &layout.regions {
            if let Some(info) = slot(&mut self.segments, region.segment) {
                let end = region
                    .end
                    .checked_next_multiple_of(block)
                    .unwrap_or(region.end);
                info.write_pos = u32::try_from(end).unwrap_or(u32::MAX).max(info.write_pos);
            }
        }
        for &(segment, pos) in &layout.sealed {
            if let Some(info) = slot(&mut self.segments, segment) {
                info.state = SegmentState::Sealed;
                info.write_pos = info.write_pos.max(pos);
            }
        }
        self.opens = layout.opens;
        if let Ok(mut usage) = self.shared.usage.write() {
            usage.clone_from(&self.segments);
        }
        Ok(())
    }

    /// Writes the whole index into the log and points the superblock at it, freeing the log
    /// before it (docs/design/chunk-store.md §5).
    pub(crate) fn checkpoint(&mut self) -> Result<(), ChunkError> {
        let segment_records: Vec<LogRecord> = self
            .segments
            .iter()
            .enumerate()
            .filter(|(_, s)| s.state != SegmentState::Free)
            .filter_map(|(i, s)| {
                Some(LogRecord::Segment(SegmentRecord {
                    segment: u32::try_from(i).ok()?,
                    incarnation: s.incarnation,
                    state: s.state,
                    write_pos: s.write_pos,
                }))
            })
            .collect();
        let puts: Vec<LogRecord> = {
            let index = self.shared.index.read().map_err(|_| ChunkError::Fenced)?;
            index
                .iter()
                .flat_map(|(key, entry)| {
                    let last = entry.fragments.len().saturating_sub(1);
                    entry.fragments.iter().enumerate().map(move |(i, f)| {
                        let flags = if entry.sealed && i == last {
                            FLAG_FINAL
                        } else {
                            0
                        };
                        put_of(*key, f, flags, entry.time_ns)
                    })
                })
                .collect()
        };
        // Every frame of the checkpoint shares one flush.
        let group = self.cursor.lsn;
        let (begin_pos, begin_lsn) =
            self.append_frame(KIND_CHECKPOINT_BEGIN, &segment_records, group)?;
        let per_frame = usize::try_from(CHECKPOINT_RECORDS_PER_FRAME)
            .unwrap_or(1)
            .max(1);
        for chunk in puts.chunks(per_frame) {
            self.append_frame(KIND_CHECKPOINT_CHUNK, chunk, group)?;
        }
        self.append_frame(KIND_CHECKPOINT_END, &[], group)?;
        self.shared.file.sync_data().map_err(ChunkError::Device)?;

        let mut superblock = self.superblock.clone();
        superblock.sequence = superblock.sequence.saturating_add(1);
        superblock.start_lsn = begin_lsn;
        superblock.start_pos = begin_pos;
        write_superblock(&self.shared.file, &superblock)?;
        self.shared.file.sync_data().map_err(ChunkError::Device)?;
        self.superblock = superblock;

        // The log now starts at the checkpoint.
        self.cursor.start_pos = begin_pos;
        self.cursor.start_lsn = begin_lsn;
        self.cursor.used = distance(begin_pos, self.cursor.pos, self.shared.geometry.log_size);
        self.shared.checkpoints.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// The largest payload one record can hold: a segment less its header block.
pub(crate) fn max_payload(geometry: &Geometry, checksum_shift: u8) -> u64 {
    let room = geometry.segment_size.saturating_sub(geometry.block);
    let table = room
        .checked_shr(u32::from(checksum_shift))
        .unwrap_or(0)
        .saturating_add(1)
        .saturating_mul(4);
    room.saturating_sub(record::HEADER_LEN as u64)
        .saturating_sub(table)
        .saturating_sub(record::RECORD_ALIGN as u64)
        .min(u64::from(u32::MAX))
}

/// What a record says about the fragment it holds, apart from its placement.
struct Put {
    key: ChunkKey,
    chunk_offset: u64,
    flags: u8,
    sequence: u64,
    time_ns: u64,
    crc: u32,
}

/// Places the record for `put`, whose payload is `payloads[payload]`, in `stream` and
/// returns its Put record. The record is encoded when the batch is written.
fn place_record(
    layout: &mut Layout,
    stream: Stream,
    shift: u8,
    put: &Put,
    payloads: &[Payload],
    payload: usize,
) -> Result<PutRecord, ChunkError> {
    let data_len = payloads
        .get(payload)
        .map(|p| p.data.len())
        .ok_or(ChunkError::Internal("a payload went missing"))?;
    let payload_len = u32::try_from(data_len).map_err(|_| ChunkError::Full)?;
    let len = record::record_len(payload_len, shift)
        .and_then(|l| u64::try_from(l).ok())
        .ok_or(ChunkError::Full)?;
    let (segment, at) = layout.place(stream, len).ok_or(ChunkError::Full)?;
    let header = RecordHeader {
        flags: put.flags,
        checksum_shift: shift,
        volume: layout.volume,
        segment,
        incarnation: layout.incarnation_of(segment),
        sequence: put.sequence,
        key: put.key,
        chunk_offset: put.chunk_offset,
        payload_len,
        time_ns: put.time_ns,
    };
    let incarnation = header.incarnation;
    layout.stage(segment, at, len, header, payload);
    Ok(PutRecord {
        key: put.key,
        segment,
        incarnation,
        offset: u32::try_from(at).map_err(|_| ChunkError::Full)?,
        record_len: u32::try_from(len).map_err(|_| ChunkError::Full)?,
        chunk_offset: put.chunk_offset,
        payload_len,
        payload_crc: put.crc,
        sequence: put.sequence,
        time_ns: put.time_ns,
        flags: put.flags,
    })
}

/// A segment the writer can free without copying: written to, and nothing in it is live.
/// That includes a stream's open segment once every chunk written into it is deleted.
fn reclaimable(s: &SegmentInfo, block: u64) -> bool {
    s.live == 0
        && (s.state == SegmentState::Sealed
            || (s.state == SegmentState::Open && u64::from(s.write_pos) > block))
}

/// Bytes from `from` forward to `to` in a circular region of `size` bytes.
pub(crate) fn distance(from: u64, to: u64, size: u64) -> u64 {
    if to >= from {
        to.saturating_sub(from)
    } else {
        size.saturating_sub(from).saturating_add(to)
    }
}

pub(crate) fn put_of(key: ChunkKey, f: &Fragment, flags: u8, time_ns: u64) -> LogRecord {
    LogRecord::Put(PutRecord {
        key,
        segment: f.segment,
        incarnation: f.incarnation,
        offset: f.offset,
        record_len: f.record_len,
        chunk_offset: f.chunk_offset,
        payload_len: f.payload_len,
        payload_crc: f.payload_crc,
        sequence: f.sequence,
        time_ns,
        flags,
    })
}

/// Writes a superblock to the copy its sequence selects.
pub(crate) fn write_superblock<F: BlockFile>(file: &F, sb: &Superblock) -> Result<(), ChunkError> {
    let bytes = sb.encode();
    let mut buf = AlignedBuf::zeroed(bytes.len(), file.alignment())
        .map_err(|e| ChunkError::Device(e.into()))?;
    buf.extend_from_slice(&bytes)
        .map_err(|e| ChunkError::Device(e.into()))?;
    file.write_all_at(buf.as_slice(), sb.offset_of(sb.slot()))
        .map_err(ChunkError::Device)
}

fn request_bytes(request: &Request) -> usize {
    match &request.op {
        Op::Write { payload, .. } => payload.data.len(),
        Op::Relocate { moves } => moves.iter().map(|m| m.payload.data.len()).sum(),
        Op::Delete { .. } | Op::Checkpoint => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::Limits;

    fn limits() -> Limits {
        Limits {
            batch_requests: 2,
            batch_bytes: 100,
            fragments_per_chunk: 4,
        }
    }

    #[test]
    fn the_queue_holds_two_batches_and_refuses_beyond() {
        let (limits, mut q) = (limits(), Queue::default());
        for _ in 0..4 {
            q.admit(10, &limits).unwrap();
        }
        assert!(matches!(q.admit(0, &limits), Err(ChunkError::Busy)));
        q.release(10);
        q.admit(0, &limits).unwrap();
        let mut q = Queue::default();
        q.admit(150, &limits).unwrap();
        assert!(matches!(q.admit(51, &limits), Err(ChunkError::Busy)));
        q.admit(50, &limits).unwrap();
    }

    /// A payload larger than the whole byte bound is admitted into a queue holding no other
    /// payload, so the bound is its size or two batches, whichever is larger.
    #[test]
    fn a_payload_larger_than_the_bound_waits_for_an_empty_queue() {
        let (limits, mut q) = (limits(), Queue::default());
        q.admit(500, &limits).unwrap();
        assert!(matches!(q.admit(1, &limits), Err(ChunkError::Busy)));
        q.release(500);
        q.admit(0, &limits).unwrap();
        q.admit(500, &limits).unwrap();
        assert!(matches!(q.admit(1, &limits), Err(ChunkError::Busy)));
    }
}
