//! The blocks a put holds going down at once, learned from the node's own puts (docs/design/
//! node.md §2.8; the constants audit of 2026-11-12, §3 #10). A put's body waits while its window
//! is full: a block's chunks go to their volumes and its Block command is written before the next
//! may go. By Little's law (Little 1961) the blocks that must be out at once for the body to
//! arrive at its client's pace are the time a block takes to be recorded times the rate the body
//! comes at, over a block's bytes: fewer, and the body waits on the writes; more, and the node
//! holds bytes no write is waiting for. Both are measured: how long each block took from going
//! down to recorded (`Put::take_committed`), and how fast the first block of a longer body filled,
//! which no window holds back (`Put::take_filled`); each smoothed as RFC 6298 §2 smooths a round
//! trip, gain 1/8 (`hyper_block::commit::smooth`). Until both are measured the window is one
//! block, the least that moves.

use hyper_block::commit::smooth;

/// What the node has measured of its puts' blocks and bodies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Window {
    /// A block's time from going down to recorded, smoothed, in nanoseconds.
    committed_ns: Option<u64>,
    /// A body's pace, smoothed, in bytes a second.
    rate: Option<u64>,
}

/// Nanoseconds a second, the unit a pace is kept in.
const SECOND_NS: u128 = 1_000_000_000;

impl Window {
    /// A block took `ns` from going down to recorded.
    pub(crate) fn committed(&mut self, ns: u64) {
        self.committed_ns = Some(self.committed_ns.map_or(ns, |was| smooth(was, ns)));
    }

    /// A body's first block of `bytes` took `ns` to come.
    pub(crate) fn filled(&mut self, bytes: u64, ns: u64) {
        let Some(rate) = u128::from(bytes)
            .checked_mul(SECOND_NS)
            .and_then(|scaled| scaled.checked_div(u128::from(ns)))
            .and_then(|rate| u64::try_from(rate).ok())
        else {
            return;
        };
        self.rate = Some(self.rate.map_or(rate, |was| smooth(was, rate)));
    }

    /// The blocks a put of a body of `length` bytes holds going down, its blocks at most `block`
    /// bytes: the bytes its body brings in the time a block takes to be recorded, in whole blocks,
    /// at least one and no more than the body's own blocks.
    pub(crate) fn blocks(&self, length: u64, block: u64) -> usize {
        let most = length.div_ceil(block.max(1)).max(1);
        let (Some(committed), Some(rate)) = (self.committed_ns, self.rate) else {
            return 1;
        };
        let in_flight = u128::from(committed)
            .saturating_mul(u128::from(rate))
            .checked_div(SECOND_NS)
            .unwrap_or(0);
        let blocks = in_flight.div_ceil(u128::from(block.max(1)));
        let blocks = u64::try_from(blocks).unwrap_or(u64::MAX).clamp(1, most);
        usize::try_from(blocks).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One block until both a block's time and a body's pace are measured; then the bytes the body
    /// brings while a block is recorded, in whole blocks, never past the body's own blocks; and it
    /// follows what it measures.
    #[test]
    fn the_window_is_the_body_brought_while_a_block_is_recorded() {
        let block = 8 << 20;
        let mut window = Window::default();
        assert_eq!(window.blocks(1 << 30, block), 1);
        window.committed(20_000_000);
        assert_eq!(window.blocks(1 << 30, block), 1);
        // 1 GB/s for 20 ms: 20 MB, three blocks of 8 MiB.
        window.filled(1_000_000_000, 1_000_000_000);
        assert_eq!(window.blocks(1 << 30, block), 3);
        // A body of two blocks holds two.
        assert_eq!(window.blocks(16 << 20, block), 2);
        assert_eq!(window.blocks(1, block), 1);
        // A slower body: 100 MB/s, 2 MB a block's time, one block.
        for _ in 0..64 {
            window.filled(100_000_000, 1_000_000_000);
        }
        assert_eq!(window.blocks(1 << 30, block), 1);
        // A body that took no measurable time says nothing.
        window.filled(block, 0);
        assert_eq!(window.blocks(1 << 30, block), 1);
    }
}
