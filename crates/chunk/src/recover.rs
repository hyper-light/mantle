//! Opening a volume after a clean shutdown or a crash (docs/design/chunk-store.md §6).
//!
//! Recovery replays the index log from the checkpoint the superblock names, frame by frame
//! in LSN order, to the first frame that is not there. It then decides whether that frame is
//! the torn tail of a crash or damage to acknowledged state: if any valid frame with a later
//! LSN exists anywhere in the log, the log was damaged, which a crash cannot cause, and the
//! volume refuses to open (Alagappan et al., FAST 2018, §3.3.3). The records of the last
//! replayed batch are read back, because that batch's flush may not have completed and its
//! frame can reach the disk before its data. Finally it rolls forward through each open
//! segment past the last indexed record, adding every record that verifies (Rosenblum and
//! Ousterhout, TOCS 1992, §4.2): a batch whose data reached the disk but whose frame did not.

use mantle_disk::DiskError;
use mantle_disk::block::BlockFile;
use mantle_disk::buf::{Pool, PoolBuf};

use crate::error::ChunkError;
use crate::frame::{KIND_BATCH, KIND_WRAP, LogRecord, PutRecord, SegmentState};
use crate::index::{Fragment, Index, Inserted, SegmentInfo};
use crate::layout::{Config, Geometry};
use crate::log::{self, Cursor};
use crate::record::{self, Prefix};
use crate::superblock::Superblock;
use crate::writer::distance;

/// What recovery found, for the operator and for tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Index frames replayed.
    pub frames: u64,
    /// Records of the last batch dropped because their data did not verify: that batch's
    /// flush had not completed, so none of them was acknowledged.
    pub unflushed_dropped: u64,
    /// Records found past the index log's end in open segments and indexed.
    pub rolled_forward: u64,
}

pub(crate) struct Recovered {
    pub index: Index,
    pub segments: Vec<SegmentInfo>,
    pub cursor: Cursor,
    pub sequence: u64,
    pub incarnation: u64,
    pub fragments: u64,
    /// Open segments, newest first: the first two continue the writer's two streams, the
    /// rest are sealed by its first batch.
    pub open: Vec<u32>,
    pub report: RecoveryReport,
}

pub(crate) fn recover<F: BlockFile>(
    file: &F,
    pool: &Pool,
    sb: &Superblock,
    geometry: &Geometry,
    config: &Config,
) -> Result<Recovered, ChunkError> {
    let per_chunk = config.limits.fragments_per_chunk;
    let mut index = Index::default();
    let segment_count = usize::try_from(geometry.segments).map_err(|_| ChunkError::Full)?;
    let mut segments = vec![SegmentInfo::FREE; segment_count];
    let mut report = RecoveryReport::default();
    // Every number the device might hold is within the superblock's reservations.
    let mut sequence = sb.sequence_limit;
    let mut incarnation = sb.incarnation_limit;

    // Replay.
    let mut pos = sb.start_pos;
    let mut lsn = sb.start_lsn;
    // The last batch's Puts, each with the fragment it displaced when it was a relocation.
    let mut last_batch: Vec<(PutRecord, Option<Fragment>)> = Vec::new();
    let max_frames = geometry
        .log_size
        .checked_div(geometry.block)
        .unwrap_or(0)
        .saturating_add(2);
    let mut frames = 0u64;
    loop {
        if frames > max_frames {
            return Err(ChunkError::CorruptLog { lsn });
        }
        let Some((header, records, len)) =
            log::read_frame(file, geometry, sb.volume, pos).map_err(ChunkError::Device)?
        else {
            break;
        };
        if header.lsn != lsn {
            break;
        }
        frames = frames.saturating_add(1);
        lsn = lsn.saturating_add(1);
        if header.kind == KIND_WRAP {
            pos = 0;
            continue;
        }
        if header.kind == KIND_BATCH {
            last_batch.clear();
        }
        for record in &records {
            match record {
                LogRecord::Put(p) => {
                    let outcome = index
                        .insert(p.key, p, per_chunk)
                        .map_err(|_| ChunkError::CorruptLog { lsn: header.lsn })?;
                    sequence = sequence.max(p.sequence);
                    if header.kind == KIND_BATCH {
                        let displaced = match outcome {
                            Inserted::Replaced(old) => Some(old),
                            Inserted::Added | Inserted::Unchanged => None,
                        };
                        last_batch.push((*p, displaced));
                    }
                    // The segment's high-water mark counts every record the log placed there,
                    // including ones a later delete removes from the index: roll-forward must
                    // start past all of them, or it would bring deleted chunks back.
                    if let Some(info) =
                        segments.get_mut(usize::try_from(p.segment).unwrap_or(usize::MAX))
                        && info.incarnation == p.incarnation
                    {
                        let end = u64::from(p.offset).saturating_add(u64::from(p.record_len));
                        let end = end.checked_next_multiple_of(geometry.block).unwrap_or(end);
                        info.write_pos = info.write_pos.max(u32::try_from(end).unwrap_or(u32::MAX));
                    }
                }
                LogRecord::Delete(d) => {
                    index.remove(&d.key);
                    sequence = sequence.max(d.sequence);
                }
                LogRecord::Segment(s) => {
                    let slot = segments
                        .get_mut(usize::try_from(s.segment).unwrap_or(usize::MAX))
                        .ok_or(ChunkError::CorruptLog { lsn: header.lsn })?;
                    *slot = SegmentInfo {
                        state: s.state,
                        incarnation: s.incarnation,
                        write_pos: s.write_pos,
                        live: 0,
                        youngest_ns: 0,
                    };
                    incarnation = incarnation.max(s.incarnation);
                }
            }
        }
        pos = pos.saturating_add(len);
        if pos >= geometry.log_size {
            pos = 0;
        }
    }
    report.frames = frames;
    if later_frame_exists(file, geometry, sb.volume, lsn)? {
        return Err(ChunkError::CorruptLog { lsn });
    }

    // The last batch's flush may not have completed: keep only records whose data verifies.
    // They are the last fragments of their chunks, so they come off in reverse order.
    for (put, displaced) in last_batch.iter().rev() {
        let verified = verify_at(file, pool, geometry, sb, put.segment, u64::from(put.offset))?
            .is_some_and(|v| v.matches(put));
        if verified {
            continue;
        }
        // A relocation whose new copy did not reach the disk goes back to the copy it moved:
        // that segment is freed only after a relocation is durable, so the old copy is intact.
        let undone = match displaced {
            Some(old) => index.restore_fragment(&put.key, old),
            None => index
                .pop_fragment(&put.key, put.chunk_offset, put.sequence)
                .is_some(),
        };
        if undone {
            report.unflushed_dropped = report.unflushed_dropped.saturating_add(1);
        }
    }

    // Roll forward through open segments.
    let mut open = Vec::new();
    for (i, info) in segments.iter_mut().enumerate() {
        if info.state != SegmentState::Open {
            continue;
        }
        let segment = u32::try_from(i).map_err(|_| ChunkError::Full)?;
        open.push((info.incarnation, segment));
        let mut at = u64::from(info.write_pos).max(geometry.block);
        while at < geometry.segment_size {
            let Some(found) = verify_at(file, pool, geometry, sb, segment, at)? else {
                break;
            };
            if found.prefix.header.incarnation != info.incarnation {
                break;
            }
            let put = found.as_put(segment, at);
            if index.insert(put.key, &put, per_chunk).is_err() {
                break;
            }
            sequence = sequence.max(put.sequence);
            report.rolled_forward = report.rolled_forward.saturating_add(1);
            at = at.saturating_add(u64::from(put.record_len));
        }
        let end = at.checked_next_multiple_of(geometry.block).unwrap_or(at);
        info.write_pos = u32::try_from(end).unwrap_or(u32::MAX).max(info.write_pos);
    }

    // Live bytes from the index.
    let mut fragments = 0u64;
    for (_, entry) in index.iter() {
        for f in &entry.fragments {
            fragments = fragments.saturating_add(1);
            if let Some(info) = segments.get_mut(usize::try_from(f.segment).unwrap_or(usize::MAX)) {
                info.live = info.live.saturating_add(u64::from(f.record_len));
                info.youngest_ns = info.youngest_ns.max(entry.time_ns);
            }
        }
    }

    open.sort_unstable_by(|a, b| b.cmp(a));
    let open: Vec<u32> = open.into_iter().map(|(_, s)| s).collect();
    let used = distance(sb.start_pos, pos, geometry.log_size);
    Ok(Recovered {
        index,
        segments,
        cursor: Cursor {
            pos,
            lsn,
            start_pos: sb.start_pos,
            start_lsn: sb.start_lsn,
            used,
        },
        sequence,
        incarnation,
        fragments,
        open,
        report,
    })
}

/// Whether the log shows that the missing frame `lsn` had been flushed: a valid frame of this
/// volume from a flush group that began after it. A new group is written only after the
/// previous group's flush completes, so such a frame proves the missing one was durable and
/// has since been damaged. Frames of the same group (a wrap and its batch, the frames of a
/// checkpoint) can survive a crash that tore an earlier one, and prove nothing.
fn later_frame_exists<F: BlockFile>(
    file: &F,
    geometry: &Geometry,
    volume: u128,
    lsn: u64,
) -> Result<bool, ChunkError> {
    let mut pos = 0u64;
    while pos < geometry.log_size {
        if let Some((header, _, _)) =
            log::read_frame(file, geometry, volume, pos).map_err(ChunkError::Device)?
            && header.lsn >= lsn
            && header.group > lsn
        {
            return Ok(true);
        }
        pos = pos.saturating_add(geometry.block);
    }
    Ok(false)
}

/// A data record read back and verified in full.
pub(crate) struct Verified {
    pub prefix: Prefix,
    pub payload_crc: u32,
    pub record_len: u32,
}

impl Verified {
    fn matches(&self, put: &PutRecord) -> bool {
        let h = &self.prefix.header;
        h.key == put.key
            && h.incarnation == put.incarnation
            && h.sequence == put.sequence
            && h.chunk_offset == put.chunk_offset
            && h.payload_len == put.payload_len
            && self.payload_crc == put.payload_crc
    }

    fn as_put(&self, segment: u32, offset: u64) -> PutRecord {
        let h = &self.prefix.header;
        PutRecord {
            key: h.key,
            segment,
            incarnation: h.incarnation,
            offset: u32::try_from(offset).unwrap_or(u32::MAX),
            record_len: self.record_len,
            chunk_offset: h.chunk_offset,
            payload_len: h.payload_len,
            payload_crc: self.payload_crc,
            sequence: h.sequence,
            time_ns: h.time_ns,
            flags: h.flags,
        }
    }
}

/// Reads the data record at `offset` in `segment` and verifies its header, identity and
/// every checksum block. `None` if no intact record of this volume and segment is there.
pub(crate) fn verify_at<F: BlockFile>(
    file: &F,
    pool: &Pool,
    geometry: &Geometry,
    sb: &Superblock,
    segment: u32,
    offset: u64,
) -> Result<Option<Verified>, ChunkError> {
    let Some(base) = geometry.segment_offset(segment) else {
        return Ok(None);
    };
    // The header first: it says how long the record is.
    let Some(header_span) = read_span(
        file,
        pool,
        geometry,
        base,
        offset,
        record::HEADER_LEN as u64,
    )?
    else {
        return Ok(None);
    };
    let Some(first) = record::peek_lengths(header_span.bytes()) else {
        return Ok(None);
    };
    let Some(record_len) = record::record_len(first.payload_len, first.checksum_shift) else {
        return Ok(None);
    };
    let record_len64 = u64::try_from(record_len).unwrap_or(u64::MAX);
    if offset.saturating_add(record_len64) > geometry.segment_size {
        return Ok(None);
    }
    drop(header_span);
    let Some(span) = read_span(file, pool, geometry, base, offset, record_len64)? else {
        return Ok(None);
    };
    let bytes = span.bytes();
    let Some(prefix) = record::decode_prefix(bytes) else {
        return Ok(None);
    };
    let h = &prefix.header;
    if h.volume != sb.volume || h.segment != segment || h.checksum_shift != sb.checksum_shift {
        return Ok(None);
    }
    let Some(start) = h.prefix_len() else {
        return Ok(None);
    };
    let Some(payload) =
        bytes.get(start..start.saturating_add(usize::try_from(h.payload_len).unwrap_or(0)))
    else {
        return Ok(None);
    };
    if !record::verify(&prefix, 0, payload) {
        return Ok(None);
    }
    let payload_crc = mantle_crc::crc32c(payload);
    Ok(Some(Verified {
        prefix,
        payload_crc,
        record_len: u32::try_from(record_len).unwrap_or(u32::MAX),
    }))
}

/// Bytes read from the device into a pooled buffer, which returns to its pool when the span
/// is dropped.
pub(crate) struct Span<'p> {
    buf: PoolBuf<'p>,
    skip: usize,
    len: usize,
}

impl Span<'_> {
    pub fn bytes(&self) -> &[u8] {
        self.buf
            .as_slice()
            .get(self.skip..self.skip.saturating_add(self.len))
            .unwrap_or_default()
    }
}

/// Reads `len` bytes at `offset` within the segment at `base`, through an aligned buffer
/// from `pool`. `None` if the span runs past the end of the file.
pub(crate) fn read_span<'p, F: BlockFile>(
    file: &F,
    pool: &'p Pool,
    geometry: &Geometry,
    base: u64,
    offset: u64,
    len: u64,
) -> Result<Option<Span<'p>>, ChunkError> {
    let block = geometry.block;
    let start = base.checked_add(offset).ok_or(ChunkError::Full)?;
    let end = start.checked_add(len).ok_or(ChunkError::Full)?;
    let aligned_start = start
        .checked_div(block)
        .and_then(|b| b.checked_mul(block))
        .ok_or(ChunkError::Full)?;
    let aligned_end = end
        .checked_next_multiple_of(block)
        .ok_or(ChunkError::Full)?;
    let span =
        usize::try_from(aligned_end.saturating_sub(aligned_start)).map_err(|_| ChunkError::Full)?;
    let mut buf = pool.take(span).map_err(|e| ChunkError::Device(e.into()))?;
    buf.set_len(span)
        .map_err(|e| ChunkError::Device(e.into()))?;
    match file.read_exact_at(buf.as_mut_slice(), aligned_start) {
        Ok(()) => {}
        Err(DiskError::ShortRead { .. }) => return Ok(None),
        Err(e) => return Err(ChunkError::Device(e)),
    }
    let skip = usize::try_from(start.saturating_sub(aligned_start)).unwrap_or(0);
    let len = usize::try_from(len).unwrap_or(0);
    if skip.saturating_add(len) > span {
        return Ok(None);
    }
    Ok(Some(Span { buf, skip, len }))
}
