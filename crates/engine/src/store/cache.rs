//! The store's page cache (docs/design/engine-structure.md §5, step E8): verified node pages'
//! payloads kept in memory, so a point read that hits costs no I/O. With uncached I/O the device
//! serves every read the cache does not, as SplinterDB's own cache does (research/34 §1).
//!
//! Eviction is S3-FIFO (Yang et al., "FIFO queues are all you need for cache eviction", SOSP
//! 2023): a small FIFO that new pages enter, a main FIFO, and a ghost FIFO of addresses recently
//! evicted from the small one. A page read twice while in the small queue moves to main; one
//! evicted from it is remembered in the ghost, and enters main directly if read again; a page
//! leaving main with reads left goes round again with one read fewer. The parameters are the
//! reference implementation's (libCacheSim `S3FIFO.c`: small queue 10% of the cache, ghost 90%,
//! promotion at two reads, a page's count capped at 3).
//!
//! Every queue and map is bounded by the cache's page count. A page written at an address is
//! dropped from the cache (`forget`), so an address reused after its extent is freed never reads
//! the page it held before.

use std::collections::{HashMap, VecDeque};

/// Cited: the small queue's share of the cache, in tenths (libCacheSim `S3FIFO.c`
/// `small-size-ratio=0.10`).
const SMALL_TENTHS: usize = 1;
/// Cited: the ghost queue's length against the cache's, in tenths (libCacheSim
/// `ghost-size-ratio=0.90`).
const GHOST_TENTHS: usize = 9;
/// Cited: reads in the small queue that move a page to main (libCacheSim
/// `move-to-main-threshold=2`).
const PROMOTE: u8 = 2;
/// Cited: a page's read count's cap (libCacheSim `S3FIFO.c`, `MIN(freq, 3)`).
const MAX_FREQ: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Queue {
    Small,
    Main,
}

/// One cached page's slot.
#[derive(Clone, Copy, Debug)]
struct Slot {
    address: u64,
    len: usize,
    freq: u8,
    queue: Queue,
    /// Whether the slot holds a page.
    live: bool,
    /// Raised each time the slot takes a page: a queue entry of an earlier one is skipped where
    /// its queue reaches it.
    generation: u32,
}

/// A page cache of a fixed number of page-sized slots.
#[derive(Debug)]
pub struct Cache {
    page: usize,
    bytes: Vec<u8>,
    slots: Vec<Slot>,
    map: HashMap<u64, usize>,
    /// Slots and the generation each was queued at, newest at the front.
    small: VecDeque<(usize, u32)>,
    main: VecDeque<(usize, u32)>,
    /// The pages each queue holds: its entries less those skipped.
    small_live: usize,
    main_live: usize,
    /// Evicted pages' addresses, newest at the front, each with the sequence it was remembered
    /// at; `ghosts` names the live ones. A page read again leaves `ghosts` only, and its entry
    /// is skipped when it reaches the back: removing it from the queue cost a scan of the whole
    /// ghost (up to 90% of the slots) on the read.
    ghost: VecDeque<(u64, u64)>,
    ghosts: HashMap<u64, u64>,
    ghost_seq: u64,
    free: Vec<usize>,
    hits: u64,
    misses: u64,
    /// The most queue steps one eviction took.
    evict_steps_most: u64,
}

impl Cache {
    /// A cache of `pages` slots of `page` bytes, at least one slot.
    pub fn new(pages: usize, page: usize) -> Self {
        let pages = pages.max(1);
        Self {
            page,
            bytes: vec![0; pages.saturating_mul(page)],
            slots: vec![
                Slot {
                    address: 0,
                    len: 0,
                    freq: 0,
                    queue: Queue::Small,
                    live: false,
                    generation: 0,
                };
                pages
            ],
            map: HashMap::with_capacity(pages),
            small: VecDeque::with_capacity(pages),
            main: VecDeque::with_capacity(pages),
            small_live: 0,
            main_live: 0,
            ghost: VecDeque::with_capacity(Self::ghost_cap(pages)),
            ghosts: HashMap::with_capacity(Self::ghost_cap(pages)),
            ghost_seq: 0,
            free: (0..pages).rev().collect(),
            hits: 0,
            misses: 0,
            evict_steps_most: 0,
        }
    }

    fn ghost_cap(pages: usize) -> usize {
        pages.saturating_mul(GHOST_TENTHS) / 10
    }

    fn small_cap(&self) -> usize {
        (self.slots.len().saturating_mul(SMALL_TENTHS) / 10).max(1)
    }

    /// The payload cached for `address`, appended to `out`; false when it is not cached.
    pub fn get(&mut self, address: u64, out: &mut Vec<u8>) -> bool {
        let Some(&i) = self.map.get(&address) else {
            self.misses = self.misses.saturating_add(1);
            return false;
        };
        let Some(slot) = self.slots.get_mut(i) else {
            return false;
        };
        slot.freq = slot.freq.saturating_add(1).min(MAX_FREQ);
        let at = i.saturating_mul(self.page);
        let Some(payload) = self.bytes.get(at..at.saturating_add(slot.len)) else {
            return false;
        };
        out.extend_from_slice(payload);
        self.hits = self.hits.saturating_add(1);
        true
    }

    /// The payload cached for `address`, appended to `out`, without counting a read: a scan's
    /// look, which must not promote what it passes.
    pub fn peek(&self, address: u64, out: &mut Vec<u8>) -> bool {
        let Some((&i, slot)) = self
            .map
            .get_key_value(&address)
            .and_then(|(_, i)| self.slots.get(*i).map(|s| (i, s)))
        else {
            return false;
        };
        let at = i.saturating_mul(self.page);
        match self.bytes.get(at..at.saturating_add(slot.len)) {
            Some(payload) => {
                out.extend_from_slice(payload);
                true
            }
            None => false,
        }
    }

    /// Caches `payload` for `address`, evicting by the module's rule if the cache is full. A
    /// payload longer than a slot is not cached.
    pub fn insert(&mut self, address: u64, payload: &[u8]) {
        if payload.len() > self.page {
            return;
        }
        self.forget(address);
        if self.free.is_empty() {
            self.evict();
        }
        let Some(i) = self.free.pop() else {
            return;
        };
        let at = i.saturating_mul(self.page);
        if let Some(dst) = self.bytes.get_mut(at..at.saturating_add(payload.len())) {
            dst.copy_from_slice(payload);
        } else {
            self.free.push(i);
            return;
        }
        let to_main = self.forget_ghost(address);
        let queue = if to_main { Queue::Main } else { Queue::Small };
        if let Some(slot) = self.slots.get_mut(i) {
            *slot = Slot {
                address,
                len: payload.len(),
                freq: 0,
                queue,
                live: true,
                generation: slot.generation.wrapping_add(1),
            };
        }
        self.push(queue, i);
        self.map.insert(address, i);
    }

    fn push(&mut self, queue: Queue, i: usize) {
        let generation = self.slots.get(i).map_or(0, |s| s.generation);
        let pages = self.slots.len();
        let slots = &self.slots;
        let current = |&(j, g): &(usize, u32)| {
            slots
                .get(j)
                .is_some_and(|s| s.live && s.generation == g && s.queue == queue)
        };
        let (q, live) = match queue {
            Queue::Small => (&mut self.small, &mut self.small_live),
            Queue::Main => (&mut self.main, &mut self.main_live),
        };
        // Entries of forgotten pages wait in their queue until reached; past the cache's page
        // count of them, the queue keeps only its current entries.
        if q.len() >= pages.saturating_mul(2) {
            q.retain(current);
        }
        q.push_front((i, generation));
        *live = live.saturating_add(1);
    }

    /// The slot an entry names, when the entry is its current one in `queue`.
    fn current(&self, (i, g): (usize, u32), queue: Queue) -> Option<Slot> {
        self.slots
            .get(i)
            .copied()
            .filter(|s| s.live && s.generation == g && s.queue == queue)
    }

    /// Drops `address` from the cache: a page written there replaces what was cached.
    pub fn forget(&mut self, address: u64) {
        if let Some(i) = self.map.remove(&address) {
            self.release(i);
        }
    }

    /// Frees slot `i`: its queue entry is skipped when reached.
    fn release(&mut self, i: usize) {
        if let Some(slot) = self.slots.get_mut(i)
            && slot.live
        {
            slot.live = false;
            match slot.queue {
                Queue::Small => self.small_live = self.small_live.saturating_sub(1),
                Queue::Main => self.main_live = self.main_live.saturating_sub(1),
            }
            self.free.push(i);
        }
    }

    /// Frees one slot: from the small queue while it holds its share, else from main.
    fn evict(&mut self) {
        // Each pass frees a slot or moves one page between queues, and each page moves to main
        // at most once and goes round main at most `MAX_FREQ` times before it is freed: a slot
        // is freed within that many steps a page.
        let bound = self.slots.len().saturating_mul(usize::from(MAX_FREQ) + 2);
        for step in 0..bound {
            if !self.free.is_empty() {
                let step = u64::try_from(step).unwrap_or(u64::MAX);
                self.evict_steps_most = self.evict_steps_most.max(step);
                return;
            }
            if self.small_live >= self.small_cap() || self.main_live == 0 {
                self.evict_small();
            } else {
                self.evict_main();
            }
        }
    }

    /// The small queue's oldest page moves to main if it was read `PROMOTE` times, else is
    /// freed and remembered in the ghost.
    fn evict_small(&mut self) {
        let Some(entry) = self.small.pop_back() else {
            return;
        };
        let Some(slot) = self.current(entry, Queue::Small) else {
            return;
        };
        let i = entry.0;
        self.small_live = self.small_live.saturating_sub(1);
        if slot.freq >= PROMOTE {
            if let Some(s) = self.slots.get_mut(i) {
                s.queue = Queue::Main;
            }
            self.push(Queue::Main, i);
            return;
        }
        if let Some(s) = self.slots.get_mut(i) {
            s.live = false;
        }
        self.map.remove(&slot.address);
        self.free.push(i);
        self.remember_ghost(slot.address);
    }

    /// Remembers `address` in the ghost, its oldest live address forgotten first when the
    /// ghost holds its share.
    fn remember_ghost(&mut self, address: u64) {
        let cap = Self::ghost_cap(self.slots.len());
        if cap == 0 {
            return;
        }
        // Stale entries passed on the way are dropped: at most the queue's length.
        while self.ghosts.len() >= cap {
            let Some((old, seq)) = self.ghost.pop_back() else {
                break;
            };
            if self.ghosts.get(&old) == Some(&seq) {
                self.ghosts.remove(&old);
            }
        }
        self.ghost_seq = self.ghost_seq.wrapping_add(1);
        self.ghost.push_front((address, self.ghost_seq));
        self.ghosts.insert(address, self.ghost_seq);
    }

    /// Forgets `address` from the ghost; true if it was there. Its queue entry stays until it
    /// leaves or the queue is compacted, which happens once stale entries are as many as live
    /// ones: the queue stays within twice the ghost's share, at a constant cost a removal.
    fn forget_ghost(&mut self, address: u64) -> bool {
        if self.ghosts.remove(&address).is_none() {
            return false;
        }
        if self.ghost.len() >= self.ghosts.len().saturating_mul(2).max(1) {
            let ghosts = &self.ghosts;
            self.ghost.retain(|(a, seq)| ghosts.get(a) == Some(seq));
        }
        true
    }

    /// Main's oldest page goes round again with one read fewer if it has any, else is freed.
    fn evict_main(&mut self) {
        let Some(entry) = self.main.pop_back() else {
            return;
        };
        let Some(slot) = self.current(entry, Queue::Main) else {
            return;
        };
        let i = entry.0;
        if slot.freq > 0 {
            if let Some(s) = self.slots.get_mut(i) {
                s.freq = s.freq.saturating_sub(1);
            }
            self.main.push_front(entry);
            return;
        }
        self.main_live = self.main_live.saturating_sub(1);
        if let Some(s) = self.slots.get_mut(i) {
            s.live = false;
        }
        self.map.remove(&slot.address);
        self.free.push(i);
    }

    /// Resizes the cache to `pages` slots, at least one, keeping what it can. Growing adds free
    /// slots. Shrinking first frees pages by the cache's own rule (S3-FIFO's evictions) until
    /// the pages held fit, then moves the pages held in the slots past the new count into free
    /// slots below it, their queue entries renamed in place so each queue keeps its order.
    /// The ghost keeps its share of the new count. A tuning step's cost: O(slots).
    pub fn resize(&mut self, pages: usize) {
        let pages = pages.max(1);
        let old = self.slots.len();
        if pages >= old {
            self.bytes.resize(pages.saturating_mul(self.page), 0);
            self.slots.resize(
                pages,
                Slot {
                    address: 0,
                    len: 0,
                    freq: 0,
                    queue: Queue::Small,
                    live: false,
                    generation: 0,
                },
            );
            self.free.extend((old..pages).rev());
            return;
        }
        // Free pages by the rule until those held fit: each pass frees a page or moves one
        // between queues, a page at most `MAX_FREQ` + 2 times.
        let bound = old.saturating_mul(usize::from(MAX_FREQ) + 2);
        for _ in 0..bound {
            if self.small_live.saturating_add(self.main_live) <= pages {
                break;
            }
            if self.small_live >= self.small_cap() || self.main_live == 0 {
                self.evict_small();
            } else {
                self.evict_main();
            }
        }
        // Each page held past the new count moves to a free slot below it.
        let mut low_free: Vec<usize> = self.free.iter().copied().filter(|&i| i < pages).collect();
        let mut moved: HashMap<usize, (usize, u32)> = HashMap::new();
        for i in pages..old {
            let Some(slot) = self.slots.get(i).copied().filter(|s| s.live) else {
                continue;
            };
            let Some(j) = low_free.pop() else {
                break;
            };
            let (from, to) = (i.saturating_mul(self.page), j.saturating_mul(self.page));
            self.bytes
                .copy_within(from..from.saturating_add(slot.len), to);
            let generation = self
                .slots
                .get(j)
                .map_or(0, |s| s.generation)
                .wrapping_add(1);
            if let Some(dst) = self.slots.get_mut(j) {
                *dst = Slot { generation, ..slot };
            }
            self.map.insert(slot.address, j);
            moved.insert(i, (j, generation));
        }
        // A page past the new count that found no slot (the evictions did not make room, which
        // their bound rules out) leaves the cache rather than be named past its end.
        for i in pages..old {
            if moved.contains_key(&i) {
                continue;
            }
            if let Some(slot) = self.slots.get(i).copied().filter(|s| s.live) {
                self.map.remove(&slot.address);
                match slot.queue {
                    Queue::Small => self.small_live = self.small_live.saturating_sub(1),
                    Queue::Main => self.main_live = self.main_live.saturating_sub(1),
                }
            }
        }
        let slots = &self.slots;
        let rename = |q: &mut VecDeque<(usize, u32)>, queue: Queue| {
            q.retain_mut(|e| {
                if let Some(&(j, g)) = moved.get(&e.0)
                    && slots
                        .get(e.0)
                        .is_some_and(|s| s.live && s.generation == e.1 && s.queue == queue)
                {
                    *e = (j, g);
                    return true;
                }
                e.0 < pages
            });
        };
        rename(&mut self.small, Queue::Small);
        rename(&mut self.main, Queue::Main);
        self.free = low_free;
        self.slots.truncate(pages);
        self.bytes.truncate(pages.saturating_mul(self.page));
        let cap = Self::ghost_cap(pages);
        while self.ghosts.len() > cap {
            let Some((a, seq)) = self.ghost.pop_back() else {
                break;
            };
            if self.ghosts.get(&a) == Some(&seq) {
                self.ghosts.remove(&a);
            }
        }
    }

    /// Slots the cache has.
    pub fn pages(&self) -> usize {
        self.slots.len()
    }

    /// Reads served, and reads missed.
    pub fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }

    /// The most queue steps one eviction took.
    pub fn evict_steps_most(&self) -> u64 {
        self.evict_steps_most
    }

    /// The cache's bytes in memory: its slots.
    pub fn bytes(&self) -> usize {
        self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(a: u64) -> Vec<u8> {
        format!("page {a}").into_bytes()
    }

    #[test]
    fn a_cached_page_reads_back_and_a_forgotten_one_misses() {
        let mut c = Cache::new(10, 64);
        c.insert(7, &page(7));
        let mut out = Vec::new();
        assert!(c.get(7, &mut out));
        assert_eq!(out, page(7));
        c.forget(7);
        assert!(!c.get(7, &mut Vec::new()));
        // Rewritten: the new payload, never the old.
        c.insert(7, b"old");
        c.insert(7, b"new");
        let mut out = Vec::new();
        assert!(c.get(7, &mut out));
        assert_eq!(out, b"new");
    }

    #[test]
    fn a_scan_does_not_flush_pages_read_again() {
        // Ten slots: pages 0..3 read three times each move to main; a scan of 1,000 pages read
        // once passes through the small queue and leaves them.
        let mut c = Cache::new(10, 64);
        for a in 0..3u64 {
            c.insert(a, &page(a));
        }
        for _ in 0..3 {
            for a in 0..3u64 {
                assert!(c.get(a, &mut Vec::new()));
            }
        }
        for a in 100..1_100u64 {
            c.insert(a, &page(a));
        }
        for a in 0..3u64 {
            let mut out = Vec::new();
            assert!(c.get(a, &mut out), "page {a} flushed by a scan");
            assert_eq!(out, page(a));
        }
        // Bounded: never more pages than slots, nor ghosts past their share.
        assert!(c.map.len() <= 10);
        assert!(c.ghosts.len() <= 9 && c.ghost.len() <= 2 * 9 + 1);
    }

    #[test]
    fn a_page_read_again_after_its_eviction_enters_main() {
        let mut c = Cache::new(10, 64);
        // Fifteen pages into ten slots: the five oldest, 0 among them, leave the small queue
        // unread for the ghost, which holds nine.
        for a in 0..15u64 {
            c.insert(a, &page(a));
        }
        assert_eq!(c.ghosts.len(), 5);
        // Inserted again, page 0 enters main.
        assert!(c.ghosts.contains_key(&0));
        c.insert(0, &page(0));
        let i = c.map[&0];
        assert_eq!(c.slots[i].queue, Queue::Main);
        assert!(!c.ghosts.contains_key(&0));
    }

    #[test]
    fn the_lazy_ghost_forgets_as_an_eager_queue_does_and_stays_bounded() {
        let mut c = Cache::new(40, 64);
        let cap = Cache::ghost_cap(40);
        // The eager queue the ghost was: newest at the front, a forgotten address removed where
        // it stands, the oldest dropped at capacity.
        let mut model: VecDeque<u64> = VecDeque::new();
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for step in 0..20_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let address = x % 64;
            if x.is_multiple_of(3) {
                let was = model.contains(&address);
                model.retain(|&a| a != address);
                assert_eq!(c.forget_ghost(address), was, "step {step}");
            } else if !model.contains(&address) {
                if model.len() >= cap {
                    model.pop_back();
                }
                model.push_front(address);
                c.remember_ghost(address);
            }
            let mut live: Vec<u64> = c.ghosts.keys().copied().collect();
            live.sort_unstable();
            let mut want: Vec<u64> = model.iter().copied().collect();
            want.sort_unstable();
            assert_eq!(live, want, "step {step}");
            assert!(
                c.ghost.len() <= 2 * cap + 1,
                "step {step}: {}",
                c.ghost.len()
            );
        }
    }

    #[test]
    fn a_resized_cache_serves_only_what_it_was_given_and_keeps_its_bounds() {
        let mut c = Cache::new(64, 32);
        let mut latest: HashMap<u64, Vec<u8>> = HashMap::new();
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut out = Vec::new();
        for step in 0..30_000u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let address = x % 200;
            match x % 11 {
                0..=4 => {
                    let payload: Vec<u8> = (0..(1 + x % 32))
                        .map(|b| ((b ^ address) & 0xff) as u8)
                        .collect();
                    c.insert(address, &payload);
                    latest.insert(address, payload);
                }
                5..=8 => {
                    out.clear();
                    if c.get(address, &mut out) {
                        assert_eq!(
                            Some(&out),
                            latest.get(&address),
                            "step {step} read {address}"
                        );
                    }
                }
                9 => {
                    c.forget(address);
                    latest.remove(&address);
                }
                _ => c.resize(1 + usize::try_from((x >> 20) % 128).unwrap()),
            }
            // The map names live slots holding their own addresses; free and live slots are all
            // the slots; the queues' live counts are the pages held; the ghost keeps its share.
            let live = c.slots.iter().filter(|s| s.live).count();
            assert_eq!(c.map.len(), live, "step {step}");
            assert!(
                c.map
                    .iter()
                    .all(|(&a, &i)| c.slots[i].live && c.slots[i].address == a)
            );
            assert_eq!(c.free.len() + live, c.slots.len(), "step {step}");
            assert_eq!(c.small_live + c.main_live, live, "step {step}");
            assert!(
                c.ghosts.len() <= Cache::ghost_cap(c.slots.len()),
                "step {step}"
            );
            assert_eq!(c.bytes.len(), c.slots.len() * 32, "step {step}");
        }
    }
}
