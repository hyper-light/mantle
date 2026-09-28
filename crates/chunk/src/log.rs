//! The circular index log: reading and writing frames at positions within its region, and
//! tracking which part of it is still needed.

use mantle_disk::DiskError;
use mantle_disk::block::BlockFile;
use mantle_disk::buf::{Alignment, AlignedBuf};

use crate::frame::{self, FrameHeader, LogRecord};
use crate::layout::{Geometry, MAX_FRAME_BYTES};

/// The live span of the log: from the start frame recovery replays from, to where the next
/// frame goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    /// Byte position of the next frame within the log region.
    pub pos: u64,
    /// LSN of the next frame.
    pub lsn: u64,
    /// Position and LSN of the frame recovery starts from.
    pub start_pos: u64,
    pub start_lsn: u64,
    /// Bytes from `start_pos` to `pos`, going around the end.
    pub used: u64,
}

impl Cursor {
    /// Where a frame of `len` bytes goes, and whether a wrap frame must be written at the
    /// current position first. `None` if it would overwrite the live span.
    pub fn place(&self, len: u64, block: u64, log_size: u64) -> Option<Placement> {
        if len > log_size {
            return None;
        }
        let tail_room = log_size.checked_sub(self.pos)?;
        let (wrap, at, consumed) = if len <= tail_room {
            (false, self.pos, len)
        } else {
            // The rest of this lap is skipped; a wrap frame marks it when a block fits.
            (tail_room >= block, 0, tail_room.checked_add(len)?)
        };
        let free = log_size.checked_sub(self.used)?;
        if consumed > free {
            return None;
        }
        Some(Placement { wrap, at, consumed })
    }

    /// Advances past a frame placed by `place`.
    pub fn advance(&mut self, placement: Placement, len: u64, log_size: u64) {
        let lsns = if placement.wrap { 2 } else { 1 };
        self.lsn = self.lsn.saturating_add(lsns);
        let end = placement.at.saturating_add(len);
        self.pos = if end >= log_size { 0 } else { end };
        self.used = self.used.saturating_add(placement.consumed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// A wrap frame goes at the old position before the frame.
    pub wrap: bool,
    /// Where the frame goes.
    pub at: u64,
    /// Log bytes the frame (and any skipped tail) consumes.
    pub consumed: u64,
}

/// Reads the frame at `pos`. `Ok(None)` when no valid frame of `volume` starts there: the end
/// of the log, a torn frame, or stale bytes from an earlier lap or volume.
pub fn read_frame<F: BlockFile>(
    file: &F,
    geometry: &Geometry,
    volume: u128,
    pos: u64,
) -> Result<Option<(FrameHeader, Vec<LogRecord>, u64)>, DiskError> {
    let block = geometry.block_usize();
    let align = file.alignment();
    let Some(at) = geometry.log_offset.checked_add(pos) else {
        return Ok(None);
    };
    if pos.saturating_add(geometry.block) > geometry.log_size {
        return Ok(None);
    }
    let mut first = AlignedBuf::zeroed(block, align)?;
    first.set_len(block)?;
    match file.read_exact_at(first.as_mut_slice(), at) {
        Ok(()) => {}
        Err(DiskError::ShortRead { .. }) => return Ok(None),
        Err(e) => return Err(e),
    }
    let Some(header) = frame::peek(first.as_slice()) else {
        return Ok(None);
    };
    if header.volume != volume {
        return Ok(None);
    }
    let Some(len) = header.frame_len(block).and_then(|l| u64::try_from(l).ok()) else {
        return Ok(None);
    };
    if len > MAX_FRAME_BYTES || pos.saturating_add(len) > geometry.log_size {
        return Ok(None);
    }
    let bytes = if len == geometry.block {
        first
    } else {
        let mut whole = AlignedBuf::zeroed(usize::try_from(len).unwrap_or(0), align)?;
        whole.set_len(usize::try_from(len).unwrap_or(0))?;
        match file.read_exact_at(whole.as_mut_slice(), at) {
            Ok(()) => {}
            Err(DiskError::ShortRead { .. }) => return Ok(None),
            Err(e) => return Err(e),
        }
        whole
    };
    Ok(frame::decode(bytes.as_slice(), volume).map(|(h, r)| (h, r, len)))
}

/// Copies an encoded frame into an aligned buffer and writes it at `pos`.
pub fn write_frame<F: BlockFile>(
    file: &F,
    geometry: &Geometry,
    pos: u64,
    encoded: &[u8],
    align: Alignment,
) -> Result<(), DiskError> {
    let mut buf = AlignedBuf::zeroed(encoded.len(), align)?;
    buf.extend_from_slice(encoded)?;
    let at = geometry.log_offset.saturating_add(pos);
    file.write_all_at(buf.as_slice(), at)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(pos: u64, start: u64, used: u64) -> Cursor {
        Cursor { pos, lsn: 1, start_pos: start, start_lsn: 1, used }
    }

    #[test]
    fn frames_fit_until_they_would_reach_the_live_span() {
        let (block, size) = (4096, 16 * 4096);
        let c = cursor(0, 0, 0);
        assert_eq!(
            c.place(4096, block, size),
            Some(Placement { wrap: false, at: 0, consumed: 4096 })
        );
        // Near the end: the tail is skipped with a wrap frame and the frame goes to 0.
        let c = cursor(14 * 4096, 12 * 4096, 2 * 4096);
        assert_eq!(
            c.place(3 * 4096, block, size),
            Some(Placement { wrap: true, at: 0, consumed: 5 * 4096 })
        );
        // Not enough free space before the live span.
        let c = cursor(4096, 2 * 4096, 15 * 4096);
        assert_eq!(c.place(2 * 4096, block, size), None);
    }

    #[test]
    fn advancing_counts_the_wrap_frame_and_wraps_the_position() {
        let (block, size) = (4096, 16 * 4096);
        let mut c = cursor(14 * 4096, 12 * 4096, 2 * 4096);
        let p = c.place(3 * 4096, block, size).unwrap();
        c.advance(p, 3 * 4096, size);
        assert_eq!(c.pos, 3 * 4096);
        assert_eq!(c.lsn, 3);
        assert_eq!(c.used, 7 * 4096);
    }
}
