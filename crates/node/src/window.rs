//! The blocks an operation's driver holds at once, learned from the node's own operations (docs/
//! design/node.md §2.8; the constants audit of 2026-11-12, §3 #10): a put's going down, a get's
//! being read. By Little's law (Little 1961) the blocks that must be out at once for a body to move
//! at its client's pace are the time a block takes times the rate the client moves the body, over
//! a block's bytes: fewer, and the body waits on the blocks; more, and the node holds bytes no
//! client is waiting for.
//!
//! A put's body waits while its window is full: a block's chunks go to their volumes and its Block
//! command is written before the next may go. Its time is how long each block took from going
//! down to recorded (`Put::take_committed`), and its client's pace how fast the first block of a
//! longer body filled, which no window holds back (`Put::take_filled`).
//!
//! A get's client waits while its window is read: a block's row and chunks are read and its
//! segments opened before its bytes go. Its time is how long each block took from its row asked to
//! its plaintext opened (`Get::take_read`), and its client's pace how fast the client took a block
//! the node held whole once the client's room for it was full, which no window holds back: a
//! native stream's flow-control window (`host::Drain`), or an S3 connection's socket
//! (`listener::io::Drained`).
//!
//! Each is smoothed as RFC 6298 §2 smooths a round trip, gain 1/8 (`hyper_block::commit::smooth`).
//! Until both are measured the window is one block, the least that moves.
//!
//! A copy's get is read for its own put, which is its client: the put holds the window the node
//! learned of its puts, and drains that many blocks in the time one takes to be recorded, so the
//! get holds that window times a block's time to read over a block's time to be recorded
//! (`Windows::copy_get`). Its put's body comes at its get's pace, not a client's, so a copy's put
//! measures no client's pace (`Put::take_filled`); its blocks' times to be recorded and to be read
//! are the node's puts' and gets' own.

use hyper_block::commit::smooth;

/// What the node has measured of one kind of operation's blocks and bodies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Window {
    /// A block's time, smoothed, in nanoseconds: a put's from going down to recorded, a get's
    /// from its row asked to its plaintext opened.
    took_ns: Option<u64>,
    /// A body's pace at its client, smoothed, in bytes a second.
    rate: Option<u64>,
}

/// What the node has measured of its puts and of its gets: the windows each holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Windows {
    pub(crate) put: Window,
    pub(crate) get: Window,
}

/// Nanoseconds a second, the unit a pace is kept in.
const SECOND_NS: u128 = 1_000_000_000;

impl Window {
    /// A block took `ns`: a put's from going down to recorded, a get's from its row asked to its
    /// plaintext opened.
    pub(crate) fn took(&mut self, ns: u64) {
        self.took_ns = Some(self.took_ns.map_or(ns, |was| smooth(was, ns)));
    }

    /// A body's client moved `bytes` of it in `ns`: a put's first block came, a get's block was
    /// taken.
    pub(crate) fn paced(&mut self, bytes: u64, ns: u64) {
        let Some(rate) = u128::from(bytes)
            .checked_mul(SECOND_NS)
            .and_then(|scaled| scaled.checked_div(u128::from(ns)))
            .and_then(|rate| u64::try_from(rate).ok())
        else {
            return;
        };
        self.rate = Some(self.rate.map_or(rate, |was| smooth(was, rate)));
    }

    /// The blocks an operation on a body of `length` bytes holds at once, its blocks at most
    /// `block` bytes: the bytes its client moves in the time a block takes, in whole blocks, at
    /// least one and no more than the body's own blocks.
    pub(crate) fn blocks(&self, length: u64, block: u64) -> usize {
        let most = length.div_ceil(block.max(1)).max(1);
        let (Some(took), Some(rate)) = (self.took_ns, self.rate) else {
            return 1;
        };
        let in_flight = u128::from(took)
            .saturating_mul(u128::from(rate))
            .checked_div(SECOND_NS)
            .unwrap_or(0);
        let blocks = in_flight.div_ceil(u128::from(block.max(1)));
        let blocks = u64::try_from(blocks).unwrap_or(u64::MAX).clamp(1, most);
        usize::try_from(blocks).unwrap_or(usize::MAX)
    }
}

impl Windows {
    /// The blocks a copy's get holds being read while its put holds `put` blocks going down, its
    /// source's span `length` bytes in blocks of at least `block` bytes. By Little's law with the
    /// put as the get's client: the put drains `put` blocks in the time one takes to be recorded,
    /// so the get holds `put` times a block's time to read over a block's time to be recorded, in
    /// whole blocks, at least one and no more than the span's blocks. One until both times are
    /// measured, or where the put's blocks took no measurable time, which says nothing of its pace.
    pub(crate) fn copy_get(&self, put: usize, length: u64, block: u64) -> usize {
        let most = length.div_ceil(block.max(1)).max(1);
        let (Some(read), Some(committed)) = (
            self.get.took_ns,
            self.put.took_ns.filter(|&committed| committed > 0),
        ) else {
            return 1;
        };
        let draining = u128::try_from(put).unwrap_or(u128::MAX);
        let blocks = u128::from(read)
            .saturating_mul(draining)
            .div_ceil(u128::from(committed));
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
        window.took(20_000_000);
        assert_eq!(window.blocks(1 << 30, block), 1);
        // 1 GB/s for 20 ms: 20 MB, three blocks of 8 MiB.
        window.paced(1_000_000_000, 1_000_000_000);
        assert_eq!(window.blocks(1 << 30, block), 3);
        // A body of two blocks holds two.
        assert_eq!(window.blocks(16 << 20, block), 2);
        assert_eq!(window.blocks(1, block), 1);
        // A slower body: 100 MB/s, 2 MB a block's time, one block.
        for _ in 0..64 {
            window.paced(100_000_000, 1_000_000_000);
        }
        assert_eq!(window.blocks(1 << 30, block), 1);
        // A body that took no measurable time says nothing.
        window.paced(block, 0);
        assert_eq!(window.blocks(1 << 30, block), 1);
    }

    /// A get's window is the bytes its client takes while a block is read: a client that drains
    /// slowly holds fewer blocks being read, a fast one more, never fewer than one nor more than
    /// its object's blocks; and a put's measures leave it alone.
    #[test]
    fn a_gets_window_follows_its_clients_drain() {
        let block = 8 << 20;
        let mut windows = Windows::default();
        // Blocks read in 40 ms each.
        windows.get.took(40_000_000);
        // A client taking 100 MB/s: 4 MB while a block is read, one block.
        windows.get.paced(100_000_000, 1_000_000_000);
        assert_eq!(windows.get.blocks(u64::MAX, block), 1);
        // One taking 1 GB/s: 40 MB while a block is read, five blocks of 8 MiB.
        for _ in 0..64 {
            windows.get.paced(1_000_000_000, 1_000_000_000);
        }
        assert_eq!(windows.get.blocks(u64::MAX, block), 5);
        // No more than the object's blocks, and at least one.
        assert_eq!(windows.get.blocks(3 * block, block), 3);
        assert_eq!(windows.get.blocks(0, block), 1);
        // Slower again: the window narrows as the drain does.
        for _ in 0..64 {
            windows.get.paced(10_000_000, 1_000_000_000);
        }
        assert_eq!(windows.get.blocks(u64::MAX, block), 1);
        // Blocks read slower at the fast drain: more of them out at once.
        let mut slow_reads = Window::default();
        slow_reads.took(160_000_000);
        slow_reads.paced(1_000_000_000, 1_000_000_000);
        assert_eq!(slow_reads.blocks(u64::MAX, block), 20);
        assert_eq!(windows.put, Window::default());
    }

    /// A copy's get holds the blocks its put drains while one is read: its put's window times a
    /// block's time to read over a block's time to be recorded, so a slow put holds fewer of its
    /// source's blocks being read and a fast one more, never fewer than one nor more than the
    /// span's blocks; one until both times are measured.
    #[test]
    fn a_copys_get_follows_its_puts_pace() {
        let block = 8 << 20;
        let mut windows = Windows::default();
        assert_eq!(windows.copy_get(4, u64::MAX, block), 1);
        // Blocks read in 40 ms each; nothing recorded yet.
        windows.get.took(40_000_000);
        assert_eq!(windows.copy_get(4, u64::MAX, block), 1);
        // A put of four blocks, each recorded in 80 ms: it drains a block every 20 ms, so two are
        // read at once.
        let mut slow = windows;
        slow.put.took(80_000_000);
        assert_eq!(slow.copy_get(4, u64::MAX, block), 2);
        // Each recorded in 10 ms: a block every 2.5 ms, sixteen.
        let mut fast = windows;
        fast.put.took(10_000_000);
        assert_eq!(fast.copy_get(4, u64::MAX, block), 16);
        // A narrower put drains slower: one block each 10 ms, four.
        assert_eq!(fast.copy_get(1, u64::MAX, block), 4);
        // No more than the span's blocks, at least one.
        assert_eq!(fast.copy_get(4, 3 * block, block), 3);
        assert_eq!(fast.copy_get(4, 0, block), 1);
        let mut slowest = windows;
        slowest.put.took(1_000_000_000);
        assert_eq!(slowest.copy_get(4, u64::MAX, block), 1);
        // A put whose blocks took no measurable time says nothing of its pace.
        let mut unmeasured = windows;
        unmeasured.put.took(0);
        assert_eq!(unmeasured.copy_get(4, u64::MAX, block), 1);
        // The clients' paces are no part of it: a put's sizes the window the copy's put is given,
        // and a copy's get's client is its put.
        fast.put.paced(1, 1_000_000_000);
        fast.get.paced(1, 1_000_000_000);
        assert_eq!(fast.copy_get(4, u64::MAX, block), 16);
    }
}
