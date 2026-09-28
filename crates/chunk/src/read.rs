//! Reading a fragment's bytes back, verified: the record's header against the identity the
//! index expects, and every checksum block returned against the record's table
//! (docs/design/chunk-store.md §7).

use mantle_disk::block::BlockFile;

use crate::error::ChunkError;
use crate::index::Fragment;
use crate::key::ChunkKey;
use crate::record;
use crate::recover::read_span;
use crate::writer::Shared;

/// Appends payload bytes `[from, to)` of `fragment` to `out`. A device error or any failed
/// check is `Corrupt`: the caller reads another copy.
pub(crate) fn fragment<F: BlockFile>(
    shared: &Shared<F>,
    key: &ChunkKey,
    fragment: &Fragment,
    from: u64,
    to: u64,
    out: &mut Vec<u8>,
) -> Result<(), ChunkError> {
    let corrupt = |detail: &str| ChunkError::Corrupt {
        key: *key,
        detail: detail.to_owned(),
    };
    let geometry = &shared.geometry;
    let shift = shared.checksum_shift;
    let block_size = 1u64.checked_shl(u32::from(shift)).unwrap_or(u64::MAX);
    let prefix_len = record::prefix_len(fragment.payload_len, shift)
        .and_then(|p| u64::try_from(p).ok())
        .ok_or_else(|| corrupt("record size"))?;
    let base = geometry
        .segment_offset(fragment.segment)
        .ok_or_else(|| corrupt("segment"))?;
    let first_block = from.checked_div(block_size).unwrap_or(0);
    let last_block_end = to
        .div_ceil(block_size)
        .saturating_mul(block_size)
        .min(u64::from(fragment.payload_len));
    let payload_from = first_block.saturating_mul(block_size);
    // One read covering the header and the checksum blocks that hold the range.
    let span_len = prefix_len.saturating_add(last_block_end);
    let span = match read_span(
        &shared.file,
        &shared.pool,
        geometry,
        base,
        u64::from(fragment.offset),
        span_len,
    ) {
        Ok(Some(span)) => span,
        Ok(None) => return Err(corrupt("record extends past the end of the volume")),
        Err(ChunkError::Device(e)) => return Err(corrupt(&format!("read failed: {e}"))),
        Err(e) => return Err(e),
    };
    let bytes = span.bytes();
    let prefix =
        record::decode_prefix(bytes).ok_or_else(|| corrupt("record header did not verify"))?;
    let h = &prefix.header;
    if h.key != *key
        || h.volume != shared.volume
        || h.segment != fragment.segment
        || h.incarnation != fragment.incarnation
        || h.sequence != fragment.sequence
        || h.chunk_offset != fragment.chunk_offset
        || h.payload_len != fragment.payload_len
    {
        return Err(corrupt("record identity does not match the index"));
    }
    let start =
        usize::try_from(prefix_len.saturating_add(payload_from)).map_err(|_| corrupt("offset"))?;
    let stop = usize::try_from(prefix_len.saturating_add(last_block_end))
        .map_err(|_| corrupt("offset"))?;
    let blocks = bytes
        .get(start..stop)
        .ok_or_else(|| corrupt("short record"))?;
    let first = u32::try_from(first_block).map_err(|_| corrupt("offset"))?;
    if !record::verify(&prefix, first, blocks) {
        return Err(corrupt("payload checksum mismatch"));
    }
    let skip = usize::try_from(from.saturating_sub(payload_from)).map_err(|_| corrupt("offset"))?;
    let take = usize::try_from(to.saturating_sub(from)).map_err(|_| corrupt("offset"))?;
    let wanted = blocks
        .get(skip..skip.saturating_add(take))
        .ok_or_else(|| corrupt("short record"))?;
    out.extend_from_slice(wanted);
    Ok(())
}
