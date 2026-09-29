//! Reading a fragment's bytes back, verified: the record's header against the identity the
//! index expects, and every checksum block returned against the record's table; and the gate
//! that holds client reads at the device's measured depth (docs/design/chunk-store.md §7).

use std::collections::VecDeque;
use std::sync::Mutex;
use std::thread::Thread;

use mantle_disk::block::BlockFile;

use crate::error::ChunkError;
use crate::index::Fragment;
use crate::key::ChunkKey;
use crate::layout::Reads;
use crate::record;
use crate::recover::read_span;
use crate::writer::Shared;

/// What the gate has let through and refused.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadStats {
    /// The most reads at the device at once since the volume opened.
    pub most_at_device: usize,
    /// Reads refused with `Busy` since the volume opened.
    pub refused: u64,
}

#[derive(Debug, Default)]
struct Turns {
    at_device: usize,
    /// The reads waiting, in the order they arrived: each one's ticket, and its thread, woken
    /// alone when its turn comes.
    waiting: VecDeque<(u64, Thread)>,
    /// Tickets handed out, and turns handed to waiting reads, in the order they arrived.
    issued: u64,
    admitted: u64,
    stats: ReadStats,
}

/// Holds client reads at `Reads::depth` at the device, lets `Reads::waiting` more wait in the
/// order they arrived, and refuses the rest with `Busy`. A wait lasts while the reads ahead
/// take, each bounded by the operating system's I/O timeout.
#[derive(Debug)]
pub(crate) struct Gate {
    reads: Reads,
    turns: Mutex<Turns>,
}

/// A read's turn at the device, given back when dropped: to the read that has waited longest,
/// if one waits.
pub(crate) struct Turn<'a> {
    gate: &'a Gate,
}

impl Gate {
    pub fn new(reads: Reads) -> Self {
        Self {
            reads,
            turns: Mutex::new(Turns::default()),
        }
    }

    /// A turn at the device, at once if one is free, after the reads that came first if one
    /// may wait, and otherwise `Busy`.
    pub fn enter(&self) -> Result<Turn<'_>, ChunkError> {
        let mut turns = self.turns.lock().map_err(|_| ChunkError::Fenced)?;
        if turns.at_device < self.reads.depth && turns.waiting.is_empty() {
            turns.at_device = turns.at_device.checked_add(1).ok_or(ChunkError::Busy)?;
        } else if turns.waiting.len() < self.reads.waiting {
            let ticket = turns.issued;
            turns.issued = ticket.checked_add(1).ok_or(ChunkError::Busy)?;
            turns.waiting.push_back((ticket, std::thread::current()));
            // A turn given back is handed to the next ticket, counted at the device for it,
            // and only that read is woken. A wake-up before the park is kept by it.
            while turns.admitted <= ticket {
                drop(turns);
                std::thread::park();
                turns = self.turns.lock().map_err(|_| ChunkError::Fenced)?;
            }
        } else {
            turns.stats.refused = turns.stats.refused.saturating_add(1);
            return Err(ChunkError::Busy);
        }
        turns.stats.most_at_device = turns.stats.most_at_device.max(turns.at_device);
        Ok(Turn { gate: self })
    }

    pub fn stats(&self) -> Result<ReadStats, ChunkError> {
        Ok(self.turns.lock().map_err(|_| ChunkError::Fenced)?.stats)
    }
}

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        let Ok(mut turns) = self.gate.turns.lock() else {
            return;
        };
        if let Some(next) = turns.admitted.checked_add(1)
            && let Some((_, reader)) = turns.waiting.pop_front()
        {
            // The turn passes to the read that has waited longest, still at the device.
            turns.admitted = next;
            drop(turns);
            reader.unpark();
        } else if let Some(left) = turns.at_device.checked_sub(1) {
            turns.at_device = left;
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::mpsc;

    fn waiting(gate: &Gate) -> usize {
        gate.turns.lock().unwrap().waiting.len()
    }

    /// The gate holds its depth at the device, lets as many more wait, in the order they came,
    /// and refuses the rest.
    #[test]
    fn the_gate_holds_its_depth_and_lets_reads_wait_in_order() {
        let gate = Arc::new(Gate::new(Reads {
            depth: 2,
            waiting: 2,
        }));
        let first = gate.enter().unwrap();
        let second = gate.enter().unwrap();
        let (admitted, order) = mpsc::channel();
        let waiters: Vec<_> = (0..2)
            .map(|n| {
                let (theirs, admitted) = (Arc::clone(&gate), admitted.clone());
                let waiter = std::thread::spawn(move || {
                    let _turn = theirs.enter().unwrap();
                    admitted.send(n).unwrap();
                });
                // The next one arrives only once this one waits.
                while waiting(&gate) < n + 1 {
                    std::thread::yield_now();
                }
                waiter
            })
            .collect();
        assert!(matches!(gate.enter(), Err(ChunkError::Busy)));
        drop(first);
        assert_eq!(order.recv().unwrap(), 0);
        drop(second);
        assert_eq!(order.recv().unwrap(), 1);
        for waiter in waiters {
            waiter.join().unwrap();
        }
        let stats = gate.stats().unwrap();
        assert_eq!((stats.most_at_device, stats.refused), (2, 1));
        let turns = gate.turns.lock().unwrap();
        assert_eq!((turns.at_device, turns.waiting.len()), (0, 0));
    }

    /// However many readers come at once, no more than the depth are at the device, and every
    /// turn taken is given back.
    #[test]
    fn many_readers_never_pass_the_depth() {
        let gate = Gate::new(Reads {
            depth: 3,
            waiting: 5,
        });
        std::thread::scope(|s| {
            for _ in 0..16 {
                s.spawn(|| {
                    for _ in 0..2_000 {
                        match gate.enter() {
                            Ok(turn) => drop(turn),
                            Err(ChunkError::Busy) => std::thread::yield_now(),
                            Err(e) => panic!("{e}"),
                        }
                    }
                });
            }
        });
        let stats = gate.stats().unwrap();
        assert!(stats.most_at_device <= 3, "{stats:?}");
        let turns = gate.turns.lock().unwrap();
        assert_eq!((turns.at_device, turns.waiting.len()), (0, 0));
    }
}
