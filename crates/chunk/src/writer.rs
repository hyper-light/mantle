//! The group-commit loop: one per volume (docs/design/chunk-store.md §4).
//!
//! It takes every request that arrived while the previous batch was being made durable
//! (DeWitt et al., SIGMOD 1984, §5.2), validates them against the index, lays their records
//! into open segments, writes the records and one index frame, and flushes the device once.
//! Only then does it publish the batch to readers and answer the requests. A failed write or
//! flush fences the volume; the flush is never retried (Rebello et al., ATC 2020).
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
use mantle_disk::buf::AlignedBuf;

use crate::error::ChunkError;
use crate::frame::{
    self, DeleteRecord, KIND_BATCH, KIND_CHECKPOINT_BEGIN, KIND_CHECKPOINT_CHUNK,
    KIND_CHECKPOINT_END, KIND_WRAP, LogRecord, PutRecord, SegmentRecord, SegmentState,
};
use crate::index::{Fragment, Index, Inserted, SegmentInfo};
use crate::key::ChunkKey;
use crate::layout::{CHECKPOINT_RECORDS_PER_FRAME, Config, Geometry};
use crate::log::{self, Cursor};
use crate::record::{self, FLAG_FINAL, RecordHeader, SegmentHeader};
use crate::superblock::Superblock;

/// Free segments kept back from new writes so the cleaner can always relocate into one
/// (Rosenblum and Ousterhout, TOCS 1992, §3.6: the cleaner needs clean segments to work).
pub(crate) const CLEANER_RESERVE: usize = 1;

pub(crate) enum Op {
    Write {
        key: ChunkKey,
        offset: u64,
        data: Vec<u8>,
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
}

/// One fragment the cleaner moves.
pub(crate) struct Move {
    pub key: ChunkKey,
    pub from: Fragment,
    pub data: Vec<u8>,
    pub flags: u8,
    pub time_ns: u64,
}

pub(crate) struct Request {
    pub op: Op,
    pub reply: SyncSender<Result<(), ChunkError>>,
}

/// What readers, the writer and the cleaner share.
pub(crate) struct Shared<F> {
    pub file: F,
    pub geometry: Geometry,
    pub volume: u128,
    pub checksum_shift: u8,
    pub index: RwLock<Index>,
    pub fenced: std::sync::atomic::AtomicBool,
    /// Set when the volume closes: background threads stop at their next wake.
    pub stopping: std::sync::atomic::AtomicBool,
    /// Bytes of records made dead since the volume opened: the cleaner waits for this to grow
    /// after a pass that could not gain space.
    pub dead_bytes: std::sync::atomic::AtomicU64,
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
    pub incarnation: u32,
    pub sequence: u64,
    pub cursor: Cursor,
    pub superblock: Superblock,
    pub fragments: u64,
    /// Wakes the cleaner when free segments run low.
    pub poke: Option<SyncSender<()>>,
    pub low_water: usize,
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

/// Bytes being laid into one segment in this batch.
struct Region {
    segment: u32,
    /// Where the region starts in the segment, block-aligned.
    start: u64,
    bytes: Vec<u8>,
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
    opened: Vec<(u32, u32)>,
    sealed: Vec<(u32, u32)>,
    freed: Vec<u32>,
    opens: [Option<u32>; 2],
    incarnation: u32,
    free: Vec<u32>,
    /// Incarnation and write position of each segment before this batch.
    before: HashMap<u32, (u32, u64)>,
}

impl Layout {
    fn incarnation_of(&self, segment: u32) -> u32 {
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
            bytes: header.encode(self.geometry.block_usize()),
        });
        self.positions.insert(next, block);
        self.opened.push((next, self.incarnation));
        if let Some(o) = self.opens.get_mut(slot) {
            *o = Some(next);
        }
        Some((next, block))
    }

    /// Encodes a record at `at` in `segment` and advances the segment's position.
    fn append(
        &mut self,
        segment: u32,
        at: u64,
        header: &RecordHeader,
        data: &[u8],
    ) -> Option<(u32, u64)> {
        let region = match self.regions.iter().rposition(|r| r.segment == segment) {
            Some(i) => i,
            None => {
                self.regions.push(Region {
                    segment,
                    start: at,
                    bytes: Vec::new(),
                });
                self.regions.len().checked_sub(1)?
            }
        };
        let region = self.regions.get_mut(region)?;
        let before = region.bytes.len();
        let crc = record::encode(header, data, &mut region.bytes)?;
        let len = u64::try_from(region.bytes.len().saturating_sub(before)).ok()?;
        self.positions.insert(segment, at.saturating_add(len));
        Some((crc, len))
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
        while let Ok(first) = self.rx.recv() {
            let mut bytes = request_bytes(&first);
            let mut batch = vec![first];
            while batch.len() < self.config.limits.batch_requests
                && bytes < self.config.limits.batch_bytes
            {
                match self.rx.try_recv() {
                    Ok(next) => {
                        bytes = bytes.saturating_add(request_bytes(&next));
                        batch.push(next);
                    }
                    Err(_) => break,
                }
            }
            if self.shared.fenced.load(Ordering::Acquire) {
                for request in batch {
                    reply(request, Err(ChunkError::Fenced));
                }
                continue;
            }
            self.process(batch);
            self.poke_cleaner();
        }
    }

    fn fence(&self) {
        self.shared.fenced.store(true, Ordering::Release);
    }

    fn free_segments(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| s.state == SegmentState::Free)
            .count()
    }

    fn poke_cleaner(&self) {
        if self.free_segments() < self.low_water
            && let Some(poke) = &self.poke
        {
            // A poke already waiting is as good as a second one.
            let _ = poke.try_send(());
        }
    }

    fn process(&mut self, batch: Vec<Request>) {
        if self.cursor.used > self.shared.geometry.log_size / 3
            && let Err(e) = self.checkpoint()
        {
            self.fence();
            let mut first = Some(e);
            for request in batch {
                reply(request, Err(first.take().unwrap_or(ChunkError::Fenced)));
            }
            return;
        }
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
        let max_payload = self.max_payload();
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
                    data,
                    seal,
                } => {
                    // Appending nothing without sealing writes nothing. A zero-length fragment
                    // may only end a chunk: fragments are found by their starting offset, which
                    // must be unique.
                    if data.is_empty() && !*seal {
                        reply(request, Ok(()));
                        continue;
                    }
                    let view = view_of(&mut views, key);
                    let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                    let crc = mantle_crc::crc32c(data);
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
                            now == Some(&m.from)
                                && untouched
                                && mantle_crc::crc32c(&m.data) == m.from.payload_crc
                        })
                        .map(|(i, _)| i)
                        .collect();
                    if current.is_empty() {
                        Ok(Decision::Done)
                    } else {
                        Ok(Decision::Relocate(current))
                    }
                }
            };
            match decision {
                Ok(Decision::Done) => reply(request, Ok(())),
                Ok(decision) => accepted.push(Accepted { request, decision }),
                Err(e) => reply(request, Err(e)),
            }
        }
        Ok(accepted)
    }

    /// The largest payload one record can hold: a segment less its header block.
    fn max_payload(&self) -> u64 {
        let room = self
            .shared
            .geometry
            .segment_size
            .saturating_sub(self.shared.geometry.block);
        let table = room
            .checked_shr(u32::from(self.shared.checksum_shift))
            .unwrap_or(0)
            .saturating_add(1)
            .saturating_mul(4);
        room.saturating_sub(record::HEADER_LEN as u64)
            .saturating_sub(table)
            .saturating_sub(record::RECORD_ALIGN as u64)
            .min(u64::from(u32::MAX))
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
        let shift = self.shared.checksum_shift;
        for Accepted { request, decision } in accepted {
            let Request { op, reply: tx } = request;
            match (op, decision) {
                (
                    Op::Write {
                        key,
                        offset,
                        data,
                        seal,
                    },
                    Decision::Write(crc),
                ) => {
                    sequence = sequence.saturating_add(1);
                    let flags = if seal { FLAG_FINAL } else { 0 };
                    let put = (key, offset, &data, flags, sequence, layout.now, crc);
                    match place_record(&mut layout, Stream::Client, shift, put) {
                        Ok(record) => {
                            layout.records.push(LogRecord::Put(record));
                            ok.push(tx);
                        }
                        Err(e) => answer(tx, Err(e)),
                    }
                }
                (Op::Relocate { moves }, Decision::Relocate(current)) => {
                    let mut result = Ok(());
                    for m in current.iter().filter_map(|&i| moves.get(i)) {
                        let put = (
                            m.key,
                            m.from.chunk_offset,
                            &m.data,
                            m.flags,
                            m.from.sequence,
                            m.time_ns,
                            m.from.payload_crc,
                        );
                        match place_record(&mut layout, Stream::Clean, shift, put) {
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
            .write_batch(&mut layout)
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

    fn write_batch(&mut self, layout: &mut Layout) -> Result<(), ChunkError> {
        let geometry = self.shared.geometry;
        let align = self.shared.file.alignment();
        for region in &mut layout.regions {
            let padded = region
                .bytes
                .len()
                .checked_next_multiple_of(geometry.block_usize())
                .ok_or(ChunkError::Full)?;
            region.bytes.resize(padded, 0);
            let mut buf =
                AlignedBuf::zeroed(padded, align).map_err(|e| ChunkError::Device(e.into()))?;
            buf.extend_from_slice(&region.bytes)
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
        self.shared.file.sync_data().map_err(ChunkError::Device)
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
                        Inserted::Replaced(old) => {
                            if let Some(info) = slot(&mut self.segments, old.segment) {
                                info.live = info.live.saturating_sub(u64::from(old.record_len));
                            }
                            self.shared
                                .dead_bytes
                                .fetch_add(u64::from(old.record_len), Ordering::Relaxed);
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
                    .start
                    .saturating_add(u64::try_from(region.bytes.len()).unwrap_or(0));
                let end = end.checked_next_multiple_of(block).unwrap_or(end);
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
        Ok(())
    }
}

/// Places a record for `put` = (key, chunk offset, payload, flags, sequence, time, CRC) in
/// `stream` and returns its Put record.
fn place_record(
    layout: &mut Layout,
    stream: Stream,
    shift: u8,
    put: (ChunkKey, u64, &Vec<u8>, u8, u64, u64, u32),
) -> Result<PutRecord, ChunkError> {
    let (key, chunk_offset, data, flags, sequence, time_ns, crc) = put;
    let payload_len = u32::try_from(data.len()).map_err(|_| ChunkError::Full)?;
    let len = record::record_len(payload_len, shift)
        .and_then(|l| u64::try_from(l).ok())
        .ok_or(ChunkError::Full)?;
    let (segment, at) = layout.place(stream, len).ok_or(ChunkError::Full)?;
    let header = RecordHeader {
        flags,
        checksum_shift: shift,
        volume: layout.volume,
        segment,
        incarnation: layout.incarnation_of(segment),
        sequence,
        key,
        chunk_offset,
        payload_len,
        time_ns,
    };
    let (written_crc, written_len) = layout
        .append(segment, at, &header, data)
        .ok_or(ChunkError::Full)?;
    if written_crc != crc {
        // The bytes changed between validation and encoding: memory corruption. The record is
        // laid out but no index record names it, so its bytes are dead space.
        return Err(ChunkError::Corrupt {
            key,
            detail: "payload changed in memory before it was written".into(),
        });
    }
    Ok(PutRecord {
        key,
        segment,
        incarnation: header.incarnation,
        offset: u32::try_from(at).map_err(|_| ChunkError::Full)?,
        record_len: u32::try_from(written_len).map_err(|_| ChunkError::Full)?,
        chunk_offset,
        payload_len,
        payload_crc: crc,
        sequence,
        time_ns,
        flags,
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
        Op::Write { data, .. } => data.len(),
        Op::Relocate { moves } => moves.iter().map(|m| m.data.len()).sum(),
        Op::Delete { .. } => 0,
    }
}
