//! Volume geometry and configuration.

use mantle_disk::buf::Alignment;

use crate::error::ChunkError;
use crate::frame::{FRAME_HEADER, PUT_LEN, SEGMENT_LEN};
use crate::superblock::{OFFSET_B_COMPACT, OFFSET_B_STANDARD};

/// The largest index frame: bounds the memory one frame takes to read or write.
pub const MAX_FRAME_BYTES: u64 = 4 << 20;
/// Checkpoint records per frame, so a checkpoint frame stays under `MAX_FRAME_BYTES`.
pub const CHECKPOINT_RECORDS_PER_FRAME: u64 = (MAX_FRAME_BYTES - 8192) / PUT_LEN as u64;

/// Limits on the writer's queue and batches. Each bounds memory, not throughput: a batch
/// is everything that arrived while the previous flush ran, up to these limits.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Requests waiting for the writer; a full queue blocks the submitter.
    pub queue: usize,
    /// Requests in one group commit.
    pub batch_requests: usize,
    /// Payload bytes in one group commit.
    pub batch_bytes: usize,
    /// Fragments one appended chunk may have before it must be sealed.
    pub fragments_per_chunk: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            queue: 4096,
            batch_requests: 1024,
            batch_bytes: 32 << 20,
            fragments_per_chunk: 4096,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Bytes per segment, fixed at format. 256 MiB matches host-managed SMR zones and is in
    /// the range of ZNS zone capacities (docs/design/chunk-store.md §2).
    pub segment_size: u64,
    /// log2 of the checksum block size of data records; 16 is 64 KiB (Ghemawat et al.,
    /// SOSP 2003, §5.2).
    pub checksum_shift: u8,
    /// The most fragments the volume indexes. It bounds index memory and sizes the log at
    /// format so three full checkpoints fit.
    pub max_fragments: u64,
    /// Place superblock B and the log near the start: for small volumes and tests.
    pub compact: bool,
    /// How long one background scrub of the whole volume takes; `None` scrubs only when
    /// asked. Seven days is the target the literature supports, fourteen the most it allows
    /// (docs/research/03 §15.9).
    pub scrub_period: Option<std::time::Duration>,
    pub limits: Limits,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment_size: 256 << 20,
            checksum_shift: 16,
            max_fragments: 1 << 22,
            compact: false,
            scrub_period: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
            limits: Limits::default(),
        }
    }
}

/// Where everything is in a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    pub block: u64,
    pub segment_size: u64,
    pub segments: u32,
    pub offset_b: u64,
    pub log_offset: u64,
    pub log_size: u64,
    pub data_offset: u64,
}

impl Geometry {
    /// Lays out a volume of `size` bytes whose transfers align to `align`.
    pub fn plan(size: u64, align: Alignment, config: &Config) -> Result<Self, ChunkError> {
        let bad = |m: &str| ChunkError::Config(m.to_owned());
        let block = u64::try_from(
            align
                .max(Alignment::new(4096).map_err(|_| bad("block"))?)
                .get(),
        )
        .map_err(|_| bad("block"))?;
        if !config.segment_size.is_multiple_of(block)
            || config.segment_size < block.saturating_mul(4)
        {
            return Err(bad(
                "segment size must be a multiple of the block, at least four blocks",
            ));
        }
        if config.segment_size > u64::from(u32::MAX) {
            return Err(bad("segment size must be below 4 GiB"));
        }
        if !(9..=24).contains(&config.checksum_shift) {
            return Err(bad("checksum block must be between 512 B and 16 MiB"));
        }
        let (offset_b, log_offset, min_log) = if config.compact {
            (
                OFFSET_B_COMPACT,
                OFFSET_B_COMPACT.saturating_mul(2),
                1u64 << 20,
            )
        } else {
            (
                OFFSET_B_STANDARD,
                OFFSET_B_STANDARD.saturating_mul(2),
                64u64 << 20,
            )
        };
        if !offset_b.is_multiple_of(block) {
            return Err(bad("block too large for the superblock layout"));
        }
        let segments_upper = size.checked_div(config.segment_size).unwrap_or(0);
        let checkpoint = checkpoint_bytes(config.max_fragments, segments_upper, block)
            .ok_or_else(|| bad("fragment budget too large"))?;
        // Headroom for the largest frames written between checkpoints: one batch frame, and a
        // wrap that skips at most one frame's worth at the end of the region.
        let batch_frame = batch_frame_bytes(config.limits.batch_requests, block)
            .ok_or_else(|| bad("batch limit too large"))?;
        let checkpoint_frame = checkpoint_frame_bytes(config.max_fragments, block)
            .ok_or_else(|| bad("fragment budget too large"))?;
        let log_size = checkpoint
            .checked_mul(3)
            .and_then(|c| c.checked_add(batch_frame.max(checkpoint_frame).saturating_mul(2)))
            .map(|c| c.max(min_log))
            .and_then(|c| c.checked_next_multiple_of(block))
            .ok_or_else(|| bad("log too large"))?;
        let data_offset = log_offset
            .checked_add(log_size)
            .and_then(|e| e.checked_next_multiple_of(block))
            .ok_or_else(|| bad("volume too large"))?;
        let segments = size
            .checked_sub(data_offset)
            .and_then(|d| d.checked_div(config.segment_size))
            .ok_or_else(|| bad("volume too small for its log"))?;
        if segments < 2 {
            return Err(bad(
                "volume too small: it needs at least two segments after the log",
            ));
        }
        Ok(Self {
            block,
            segment_size: config.segment_size,
            segments: u32::try_from(segments).map_err(|_| bad("too many segments"))?,
            offset_b,
            log_offset,
            log_size,
            data_offset,
        })
    }

    /// The byte offset of a segment.
    pub fn segment_offset(&self, segment: u32) -> Option<u64> {
        u64::from(segment)
            .checked_mul(self.segment_size)
            .and_then(|o| o.checked_add(self.data_offset))
    }

    pub fn block_usize(&self) -> usize {
        usize::try_from(self.block).unwrap_or(4096)
    }
}

/// Bytes of the largest batch frame. Per request at most four records: its own Put or
/// Delete, sealing a full segment and opening the next when the record does not fit, and one
/// of the empty segments the batch frees (the writer frees at most one per request).
pub fn batch_frame_bytes(requests: usize, block: u64) -> Option<u64> {
    let records = u64::try_from(requests).ok()?.checked_mul(4)?;
    let payload = records.checked_mul(PUT_LEN as u64)?;
    (FRAME_HEADER as u64)
        .checked_add(payload)?
        .checked_next_multiple_of(block)
}

/// Bytes of the largest checkpoint frame.
pub fn checkpoint_frame_bytes(fragments: u64, block: u64) -> Option<u64> {
    let records = fragments.min(CHECKPOINT_RECORDS_PER_FRAME);
    (FRAME_HEADER as u64)
        .checked_add(records.checked_mul(PUT_LEN as u64)?)?
        .checked_next_multiple_of(block)
}

/// Bytes a checkpoint of `fragments` fragments and `segments` segments takes in the log.
pub fn checkpoint_bytes(fragments: u64, segments: u64, block: u64) -> Option<u64> {
    let records = fragments.checked_add(segments)?;
    let frames = records
        .div_ceil(CHECKPOINT_RECORDS_PER_FRAME)
        .checked_add(2)?;
    let payload = fragments
        .checked_mul(PUT_LEN as u64)?
        .checked_add(segments.checked_mul(SEGMENT_LEN as u64)?)?;
    payload.checked_add(frames.checked_mul((FRAME_HEADER as u64).checked_add(block)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_compact_volume_fits_its_log_and_segments() {
        let config = Config {
            segment_size: 1 << 20,
            max_fragments: 1000,
            compact: true,
            ..Config::default()
        };
        let g = Geometry::plan(16 << 20, Alignment::new(4096).unwrap(), &config).unwrap();
        assert!(g.segments >= 2);
        assert!(g.data_offset >= g.log_offset + g.log_size);
        assert!(g.segment_offset(g.segments - 1).unwrap() + g.segment_size <= 16 << 20);
        assert!(g.log_size >= 3 * checkpoint_bytes(1000, u64::from(g.segments), 4096).unwrap());
    }

    #[test]
    fn nonsense_is_refused() {
        let a = Alignment::new(4096).unwrap();
        let mut c = Config {
            compact: true,
            ..Config::default()
        };
        c.segment_size = 1000;
        assert!(Geometry::plan(1 << 30, a, &c).is_err());
        let c = Config {
            compact: true,
            segment_size: 1 << 20,
            ..Config::default()
        };
        assert!(Geometry::plan(1 << 20, a, &c).is_err(), "too small");
    }
}
