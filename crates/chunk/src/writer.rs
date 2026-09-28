//! The group-commit loop: one per volume (docs/design/chunk-store.md §4).
//!
//! It takes every request that arrived while the previous batch was being made durable
//! (DeWitt et al., SIGMOD 1984, §5.2), validates them against the index, lays their records
//! into the open segment, writes the records and one index frame, and flushes the device
//! once. Only then does it publish the batch to readers and answer the requests. A failed
//! write or flush fences the volume; the flush is never retried (Rebello et al., ATC 2020).

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
use crate::index::{Fragment, Index, SegmentInfo};
use crate::key::ChunkKey;
use crate::layout::{CHECKPOINT_RECORDS_PER_FRAME, Config, Geometry};
use crate::log::{self, Cursor};
use crate::record::{self, FLAG_FINAL, RecordHeader, SegmentHeader};
use crate::superblock::Superblock;

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
}

pub(crate) struct Request {
    pub op: Op,
    pub reply: SyncSender<Result<(), ChunkError>>,
}

/// What readers and the writer share.
pub(crate) struct Shared<F> {
    pub file: F,
    pub geometry: Geometry,
    pub volume: u128,
    pub checksum_shift: u8,
    pub index: RwLock<Index>,
    pub fenced: std::sync::atomic::AtomicBool,
    pub usage: RwLock<Vec<SegmentInfo>>,
}

/// The writer's state, owned by its thread.
pub(crate) struct Writer<F: BlockFile> {
    pub shared: Arc<Shared<F>>,
    pub config: Config,
    pub rx: Receiver<Request>,
    pub segments: Vec<SegmentInfo>,
    pub open: Option<u32>,
    pub incarnation: u32,
    pub sequence: u64,
    pub cursor: Cursor,
    pub superblock: Superblock,
    pub fragments: u64,
}

/// A chunk as the batch being validated sees it: the index plus earlier requests in the batch.
#[derive(Clone)]
struct View {
    len: u64,
    sealed: bool,
    fragments: usize,
    /// Fragments added earlier in this batch: (chunk offset, length, CRC-32C).
    pending: Vec<(u64, u64, u32)>,
}

enum Decision {
    /// Write a fragment whose payload has this CRC-32C.
    Write(u32),
    Delete,
    /// Nothing to write: the request is already satisfied (a retry, or deleting nothing).
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
        }
    }

    fn fence(&self) {
        self.shared.fenced.store(true, Ordering::Release);
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
        if accepted.is_empty() {
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
        for request in batch {
            let decision = match &request.op {
                Op::Write {
                    key,
                    offset,
                    data,
                    seal,
                } => {
                    let view = views
                        .entry(*key)
                        .or_insert_with(|| {
                            index.get(key).map(|e| View {
                                len: e.len(),
                                sealed: e.sealed,
                                fragments: e.fragments.len(),
                                pending: Vec::new(),
                            })
                        })
                        .clone();
                    // Appending nothing without sealing writes nothing. A zero-length fragment
                    // may only end a chunk: fragments are found by their starting offset, which
                    // must be unique.
                    if data.is_empty() && !*seal {
                        reply(request, Ok(()));
                        continue;
                    }
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
                                });
                                next.pending.push((*offset, len, crc));
                                next.len = next.len.saturating_add(len);
                                next.sealed = *seal;
                                next.fragments = next.fragments.saturating_add(1);
                                views.insert(*key, Some(next));
                                Ok(Decision::Write(crc))
                            }
                        }
                    }
                }
                Op::Delete { key } => {
                    let exists = views
                        .entry(*key)
                        .or_insert_with(|| {
                            index.get(key).map(|e| View {
                                len: e.len(),
                                sealed: e.sealed,
                                fragments: e.fragments.len(),
                                pending: Vec::new(),
                            })
                        })
                        .is_some();
                    if exists {
                        views.insert(*key, None);
                        Ok(Decision::Delete)
                    } else {
                        Ok(Decision::Done)
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

    /// Lays out, writes, flushes, publishes and answers one batch.
    fn commit(&mut self, accepted: Vec<Accepted>) {
        let geometry = self.shared.geometry;
        let block = geometry.block;
        let now = now_ns();
        let mut records: Vec<LogRecord> = Vec::new();
        let mut regions: Vec<Region> = Vec::new();
        // Segment positions as this batch advances them, before they are committed.
        let mut positions: HashMap<u32, u64> = HashMap::new();
        let mut opened: Vec<(u32, u32)> = Vec::new();
        let mut sealed: Vec<u32> = Vec::new();
        let mut open = self.open;
        let mut incarnation = self.incarnation;
        let mut sequence = self.sequence;
        let mut ok: Vec<Reply> = Vec::with_capacity(accepted.len());

        // Sealed segments with nothing live are freed in this frame, so this batch may reuse them.
        // At most one per request, so the frame stays within the log's headroom
        // (layout::batch_frame_bytes); the rest are freed by later batches.
        let freed: Vec<u32> = self
            .segments
            .iter()
            .enumerate()
            .filter(|(_, s)| s.state == SegmentState::Sealed && s.live == 0)
            .filter_map(|(i, _)| u32::try_from(i).ok())
            .take(accepted.len().max(1))
            .collect();
        for &segment in &freed {
            if let Some(info) = self
                .segments
                .get(usize::try_from(segment).unwrap_or(usize::MAX))
            {
                records.push(LogRecord::Segment(SegmentRecord {
                    segment,
                    incarnation: info.incarnation,
                    state: SegmentState::Free,
                    write_pos: 0,
                }));
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
        free.sort_unstable();
        free.dedup();
        let mut free = free.into_iter();

        for Accepted { request, decision } in accepted {
            let Request { op, reply: tx } = request;
            match (&op, &decision) {
                (
                    Op::Write {
                        key,
                        offset,
                        data,
                        seal,
                    },
                    Decision::Write(crc),
                ) => {
                    let Ok(payload_len) = u32::try_from(data.len()) else {
                        answer(tx, Err(ChunkError::Full));
                        continue;
                    };
                    let Some(len) = record::record_len(payload_len, self.shared.checksum_shift)
                        .and_then(|l| u64::try_from(l).ok())
                    else {
                        answer(tx, Err(ChunkError::Full));
                        continue;
                    };
                    // Find room: the open segment, or a new one.
                    let segment = loop {
                        if let Some(s) = open {
                            // A segment left open by an earlier batch continues at its write
                            // position; record it so the placement below uses the same one.
                            let written = self
                                .segment(s)
                                .map_or(block, |i| u64::from(i.write_pos).max(block));
                            let at = *positions.entry(s).or_insert(written);
                            if at.saturating_add(len) <= geometry.segment_size {
                                break Some(s);
                            }
                            let pos = at.checked_next_multiple_of(block).unwrap_or(at);
                            records.push(LogRecord::Segment(SegmentRecord {
                                segment: s,
                                incarnation: self.incarnation_of(s, &opened),
                                state: SegmentState::Sealed,
                                write_pos: u32::try_from(pos).unwrap_or(u32::MAX),
                            }));
                            sealed.push(s);
                            open = None;
                        }
                        let Some(next) = free.next() else {
                            break None;
                        };
                        incarnation = incarnation.saturating_add(1);
                        records.push(LogRecord::Segment(SegmentRecord {
                            segment: next,
                            incarnation,
                            state: SegmentState::Open,
                            write_pos: u32::try_from(block).unwrap_or(u32::MAX),
                        }));
                        let header = SegmentHeader {
                            volume: self.shared.volume,
                            segment: next,
                            incarnation,
                            time_ns: now,
                        };
                        regions.push(Region {
                            segment: next,
                            start: 0,
                            bytes: header.encode(geometry.block_usize()),
                        });
                        positions.insert(next, block);
                        opened.push((next, incarnation));
                        open = Some(next);
                    };
                    let Some(segment) = segment else {
                        answer(tx, Err(ChunkError::Full));
                        continue;
                    };
                    let Some(at) = positions.get(&segment).copied() else {
                        answer(tx, Err(ChunkError::Full));
                        continue;
                    };
                    let region = match regions.iter_mut().rfind(|r| r.segment == segment) {
                        Some(r) => r,
                        None => {
                            regions.push(Region {
                                segment,
                                start: at,
                                bytes: Vec::new(),
                            });
                            match regions.last_mut() {
                                Some(r) => r,
                                None => continue,
                            }
                        }
                    };
                    sequence = sequence.saturating_add(1);
                    let header = RecordHeader {
                        flags: if *seal { FLAG_FINAL } else { 0 },
                        checksum_shift: self.shared.checksum_shift,
                        volume: self.shared.volume,
                        segment,
                        incarnation: self.incarnation_of(segment, &opened),
                        sequence,
                        key: *key,
                        chunk_offset: *offset,
                        payload_len,
                        time_ns: now,
                    };
                    let Some(written_crc) = record::encode(&header, data, &mut region.bytes) else {
                        answer(tx, Err(ChunkError::Full));
                        continue;
                    };
                    if written_crc != *crc {
                        // The bytes changed between validation and encoding: memory corruption.
                        answer(
                            tx,
                            Err(ChunkError::Corrupt {
                                key: *key,
                                detail: "payload changed in memory before it was written".into(),
                            }),
                        );
                        continue;
                    }
                    records.push(LogRecord::Put(PutRecord {
                        key: *key,
                        segment,
                        incarnation: header.incarnation,
                        offset: u32::try_from(at).unwrap_or(u32::MAX),
                        record_len: u32::try_from(len).unwrap_or(u32::MAX),
                        chunk_offset: *offset,
                        payload_len,
                        payload_crc: *crc,
                        sequence,
                        time_ns: now,
                        flags: header.flags,
                    }));
                    positions.insert(segment, at.saturating_add(len));
                    ok.push(tx);
                }
                (Op::Delete { key }, Decision::Delete) => {
                    sequence = sequence.saturating_add(1);
                    records.push(LogRecord::Delete(DeleteRecord {
                        key: *key,
                        sequence,
                        time_ns: now,
                    }));
                    ok.push(tx);
                }
                _ => answer(tx, Ok(())),
            }
        }
        if ok.is_empty() && freed.is_empty() {
            return;
        }

        // Write the data regions, padded to whole blocks, then the frame, then flush once.
        let result = self.write_batch(&mut regions, &records);
        match result {
            Err(e) => {
                self.fence();
                let mut first = Some(e);
                for tx in ok {
                    answer(tx, Err(first.take().unwrap_or(ChunkError::Fenced)));
                }
            }
            Ok(()) => {
                if let Err(e) = self.publish(&records, &regions, &freed, &sealed, &opened, open) {
                    self.fence();
                    let mut first = Some(e);
                    for tx in ok {
                        answer(tx, Err(first.take().unwrap_or(ChunkError::Fenced)));
                    }
                    return;
                }
                self.incarnation = incarnation;
                self.sequence = sequence;
                for tx in ok {
                    answer(tx, Ok(()));
                }
            }
        }
    }

    fn segment(&self, segment: u32) -> Option<&SegmentInfo> {
        self.segments.get(usize::try_from(segment).ok()?)
    }

    fn incarnation_of(&self, segment: u32, opened: &[(u32, u32)]) -> u32 {
        opened
            .iter()
            .rev()
            .find(|(s, _)| *s == segment)
            .map(|(_, i)| *i)
            .or_else(|| self.segment(segment).map(|s| s.incarnation))
            .unwrap_or(0)
    }

    fn write_batch(
        &mut self,
        regions: &mut [Region],
        records: &[LogRecord],
    ) -> Result<(), ChunkError> {
        let geometry = self.shared.geometry;
        let align = self.shared.file.alignment();
        for region in regions.iter_mut() {
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
        self.append_frame(KIND_BATCH, records, group)?;
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
    fn publish(
        &mut self,
        records: &[LogRecord],
        regions: &[Region],
        freed: &[u32],
        sealed: &[u32],
        opened: &[(u32, u32)],
        open: Option<u32>,
    ) -> Result<(), ChunkError> {
        let per_chunk = self.config.limits.fragments_per_chunk;
        let mut index = self.shared.index.write().map_err(|_| ChunkError::Fenced)?;
        for &segment in freed {
            if let Some(info) = self
                .segments
                .get_mut(usize::try_from(segment).unwrap_or(usize::MAX))
            {
                *info = SegmentInfo::FREE;
            }
        }
        for &(segment, incarnation) in opened {
            if let Some(info) = self
                .segments
                .get_mut(usize::try_from(segment).unwrap_or(usize::MAX))
            {
                *info = SegmentInfo {
                    state: SegmentState::Open,
                    incarnation,
                    write_pos: u32::try_from(self.shared.geometry.block).unwrap_or(u32::MAX),
                    live: 0,
                    youngest_ns: 0,
                };
            }
        }
        for record in records {
            match record {
                LogRecord::Put(p) => {
                    index
                        .insert(p.key, p, per_chunk)
                        .map_err(|_| ChunkError::CorruptLog {
                            lsn: self.cursor.lsn,
                        })?;
                    self.fragments = self.fragments.saturating_add(1);
                    if let Some(info) = self
                        .segments
                        .get_mut(usize::try_from(p.segment).unwrap_or(usize::MAX))
                    {
                        info.live = info.live.saturating_add(u64::from(p.record_len));
                        info.youngest_ns = info.youngest_ns.max(p.time_ns);
                    }
                }
                LogRecord::Delete(d) => {
                    if let Some(entry) = index.remove(&d.key) {
                        for f in &entry.fragments {
                            self.fragments = self.fragments.saturating_sub(1);
                            if let Some(info) = self
                                .segments
                                .get_mut(usize::try_from(f.segment).unwrap_or(usize::MAX))
                            {
                                info.live = info.live.saturating_sub(u64::from(f.record_len));
                            }
                        }
                    }
                }
                LogRecord::Segment(_) => {}
            }
        }
        drop(index);
        let block = self.shared.geometry.block;
        for region in regions {
            if let Some(info) = self
                .segments
                .get_mut(usize::try_from(region.segment).unwrap_or(usize::MAX))
            {
                let end = region
                    .start
                    .saturating_add(u64::try_from(region.bytes.len()).unwrap_or(0));
                let end = end.checked_next_multiple_of(block).unwrap_or(end);
                info.write_pos = u32::try_from(end).unwrap_or(u32::MAX).max(info.write_pos);
            }
        }
        for &segment in sealed {
            if let Some(info) = self
                .segments
                .get_mut(usize::try_from(segment).unwrap_or(usize::MAX))
            {
                info.state = SegmentState::Sealed;
            }
        }
        self.open = open;
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
                        put_of(
                            *key,
                            f,
                            if entry.sealed && i == last {
                                FLAG_FINAL
                            } else {
                                0
                            },
                            entry.time_ns,
                        )
                    })
                })
                .collect()
        };
        // Every frame of the checkpoint shares one flush.
        let group = self.cursor.lsn;
        let (begin_pos, begin_lsn) =
            self.append_frame(KIND_CHECKPOINT_BEGIN, &segment_records, group)?;
        let used_before = self.cursor.used;
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

        // The log now starts at the checkpoint; what it wrote is all that is still needed.
        let begin_len = self.cursor.used.saturating_sub(used_before);
        let _ = begin_len;
        self.cursor.start_pos = begin_pos;
        self.cursor.start_lsn = begin_lsn;
        self.cursor.used = distance(begin_pos, self.cursor.pos, self.shared.geometry.log_size);
        Ok(())
    }
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
        Op::Delete { .. } => 0,
    }
}
