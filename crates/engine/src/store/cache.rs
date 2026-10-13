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
//! Pages lie in chunks of memory. A new chunk has room for as many pages as all before it together
//! (a table that doubles, CLRS §17.4), so a cache that grows to `n` pages allocates O(log n)
//! times; it holds at most one tuning step of the limit (`TUNE_STEP_PERCENT`, Luo and Carey, PVLDB
//! 2021 §5.4), so a step that lowers the limit gives back whole chunks, and at most what the limit
//! has left. A chunk's pages become slots in idle time (`reserve`), each written once as it is
//! made, and only as many as the reads since the last such time asked for: a read never
//! allocates or touches memory for the first time, and the cache takes the memory its reads use,
//! not its limit (measured: reserving the whole limit ahead took 38.7 GB, 7.9 M allocations, for a
//! 10M-key store of 2.3 GB, and halved its gets; research/50 §9). A read that finds no slot made
//! leaves its page out, as an insert past its eviction bound does. Resizing sets the page limit;
//! a cache above it is brought down a few pages an operation (`trim`) by the rule above, and the
//! chunks above it are given back the last first, its pages dropped. A freed slot is the next
//! page's, so a warm cache allocates nothing. The index and the ghost grow a few buckets a write
//! (`util::incmap`), so no read rehashes them.
//!
//! The ghost names the addresses evicted within the last `ghost_cap` evictions. A ghost hit
//! leaves a hole rather than keeping an older address longer, as libCacheSim's ghost FIFO does,
//! so the ghost may name fewer: what keeps it a map with no queue to compact.
//!
//! Every queue and map is bounded by the cache's page count. A page written at an address is
//! dropped from the cache (`forget`), so an address reused after its extent is freed never reads
//! the page it held before.

use std::num::NonZeroUsize;

use crate::shard_db::TUNE_STEP_PERCENT;
use crate::util::incmap::{IncMap, SWEEP};

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

/// Queue steps an insert may take to free a slot: S3-FIFO's amortized cost of an eviction. A
/// page moves from small to main at most once and goes round main at most `MAX_FREQ` times
/// before it is freed, each a step, so evictions take `MAX_FREQ + 2` steps a slot on average
/// (Yang et al., SOSP '23, §4); a bound of that keeps up with inserts over time while no one
/// insert sweeps the queues.
const EVICT_STEPS: usize = MAX_FREQ as usize + 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Queue {
    Small,
    Main,
}

/// A slot's index plus one, so an absent queue link occupies no extra word.
fn slot_link(i: usize) -> Option<NonZeroUsize> {
    i.checked_add(1).and_then(NonZeroUsize::new)
}

fn slot_index(link: NonZeroUsize) -> Option<usize> {
    link.get().checked_sub(1)
}

/// A queue's newest and oldest slots. Each live slot is linked once, so forgetting an address
/// removes its entry without leaving a queue to compact on a later admission.
#[derive(Debug, Default)]
struct Fifo {
    newest: Option<NonZeroUsize>,
    oldest: Option<NonZeroUsize>,
}

/// One cached page's slot.
#[derive(Clone, Copy, Debug)]
struct Slot {
    address: u64,
    freq: u8,
    queue: Queue,
    /// Whether the slot holds a page.
    live: bool,
    newer: Option<NonZeroUsize>,
    older: Option<NonZeroUsize>,
    /// Its page's place: the chunk, the byte it starts at there, and its payload's length.
    chunk: usize,
    at: usize,
    len: usize,
}

/// A page cache of at most `limit` pages.
#[derive(Debug)]
pub struct Cache {
    page: usize,
    limit: usize,
    /// The pages' memory (the module's rule), each chunk's first slot, and its pages.
    chunks: Vec<Vec<u8>>,
    starts: Vec<usize>,
    sizes: Vec<usize>,
    slots: Vec<Slot>,
    map: IncMap,
    small: Fifo,
    main: Fifo,
    /// The pages each queue holds.
    small_live: usize,
    main_live: usize,
    /// Evicted pages' addresses, each with the eviction's sequence: in the ghost while within
    /// the last `ghost_cap` evictions.
    ghosts: IncMap,
    ghost_seq: u64,
    /// Slots made that hold no page.
    free: Vec<usize>,
    /// Pages inserted since `reserve` last caught up: the free slots it keeps ready.
    wanted: usize,
    /// Slots a read made itself because idle time had not made one.
    grown: u64,
    /// While the chunks have room for more pages than the limit: the last chunk's first slot,
    /// whose pages `trim` drops before the chunk is given back, and the next of its slots to
    /// look at. Its free slots are set aside meanwhile, kept in case the limit grows back first.
    retiring: Option<usize>,
    retire_at: usize,
    retired: Vec<usize>,
    hits: u64,
    misses: u64,
    /// The most queue steps one eviction took.
    evict_steps_most: u64,
    /// Inserts left out because their bounded eviction freed no slot.
    skipped: u64,
}

impl Cache {
    /// A cache of `pages` pages of `page` bytes, at least one. Its memory is taken as reads
    /// want it ([`Self::reserve`]).
    pub fn new(pages: usize, page: usize) -> Self {
        Self {
            page,
            limit: pages.max(1),
            chunks: Vec::new(),
            starts: Vec::new(),
            sizes: Vec::new(),
            slots: Vec::new(),
            map: IncMap::new(),
            small: Fifo::default(),
            main: Fifo::default(),
            small_live: 0,
            main_live: 0,
            ghosts: IncMap::new(),
            ghost_seq: 0,
            free: Vec::new(),
            wanted: 0,
            grown: 0,
            retiring: None,
            retire_at: 0,
            retired: Vec::new(),
            hits: 0,
            misses: 0,
            evict_steps_most: 0,
            skipped: 0,
        }
    }

    fn ghost_cap(pages: usize) -> usize {
        pages.saturating_mul(GHOST_TENTHS) / 10
    }

    fn small_cap(&self) -> usize {
        (self.limit.saturating_mul(SMALL_TENTHS) / 10).max(1)
    }

    /// The payload cached for `address`, appended to `out`; false when it is not cached.
    pub fn get(&mut self, address: u64, out: &mut Vec<u8>) -> bool {
        self.map.settle();
        let Some(i) = self.slot_of(address) else {
            self.misses = self.misses.saturating_add(1);
            return false;
        };
        let Some(slot) = self.slots.get_mut(i) else {
            return false;
        };
        slot.freq = slot.freq.saturating_add(1).min(MAX_FREQ);
        let Some(payload) = self.payload(i) else {
            return false;
        };
        out.extend_from_slice(payload);
        self.hits = self.hits.saturating_add(1);
        true
    }

    /// The payload slot `i` holds.
    fn payload(&self, i: usize) -> Option<&[u8]> {
        let s = self.slots.get(i)?;
        self.chunks
            .get(s.chunk)?
            .get(s.at..s.at.checked_add(s.len)?)
    }

    /// The payload cached for `address`, lent in place and counted as a read; none when it is
    /// not cached. A point read parses the page where it lies and copies only what it returns.
    pub fn get_ref(&mut self, address: u64) -> Option<&[u8]> {
        // A phase of hits writes nothing: the read moves the index's migration on.
        self.map.settle();
        let Some(i) = self.slot_of(address) else {
            self.misses = self.misses.saturating_add(1);
            return None;
        };
        let slot = self.slots.get_mut(i)?;
        slot.freq = slot.freq.saturating_add(1).min(MAX_FREQ);
        self.hits = self.hits.saturating_add(1);
        self.payload(i)
    }

    /// The payload cached for `address`, appended to `out`, without counting a read: a scan's
    /// look, which must not promote what it passes.
    pub fn peek(&self, address: u64, out: &mut Vec<u8>) -> bool {
        let Some(i) = self.slot_of(address) else {
            return false;
        };
        match self.payload(i) {
            Some(payload) => {
                out.extend_from_slice(payload);
                true
            }
            None => false,
        }
    }

    /// Whether the page can be read from memory, without counting or promoting the look.
    pub fn contains(&self, address: u64) -> bool {
        self.slot_of(address)
            .and_then(|i| self.payload(i))
            .is_some()
    }

    /// Caches `payload` for `address`, evicting by the module's rule if the cache is full. A
    /// payload longer than a slot is not cached.
    pub fn insert(&mut self, address: u64, payload: &[u8]) {
        if payload.len() > self.page {
            return;
        }
        self.forget(address);
        // Admission is optional: the page is the caller's whether or not the cache keeps it. An
        // eviction may cost a step for each page the queues cycle, so it is bounded at the
        // policy's own amortized cost, and an insert whose bound frees no slot leaves the page
        // out; the queues keep their place, so the next insert goes on from there.
        self.wanted = self.wanted.saturating_add(1);
        if self.held() >= self.limit && !self.evict(EVICT_STEPS) {
            self.skipped = self.skipped.saturating_add(1);
            return;
        }
        // A slot made in idle time, else one made now: its page touched here, and at most
        // O(log n) times over the cache's growth a chunk taken (the module's rule). Idle time
        // makes the slots the reads since asked for, so a read seldom does.
        let Some(i) = self.take_free().or_else(|| self.make_now()) else {
            self.skipped = self.skipped.saturating_add(1);
            return;
        };
        let Some(&Slot { chunk, at, .. }) = self.slots.get(i) else {
            return;
        };
        let Some(room) = at
            .checked_add(payload.len())
            .and_then(|end| self.chunks.get_mut(chunk)?.get_mut(at..end))
        else {
            self.free.push(i);
            return;
        };
        room.copy_from_slice(payload);
        let to_main = self.forget_ghost(address);
        let queue = if to_main { Queue::Main } else { Queue::Small };
        if let Some(slot) = self.slots.get_mut(i) {
            *slot = Slot {
                address,
                freq: 0,
                queue,
                live: true,
                newer: None,
                older: None,
                chunk,
                at,
                len: payload.len(),
            };
        }
        self.push(queue, i);
        self.map
            .insert(address, u64::try_from(i).unwrap_or(u64::MAX));
    }

    /// Pages held.
    fn held(&self) -> usize {
        self.small_live.saturating_add(self.main_live)
    }

    /// A made slot holding no page, none in a retiring chunk: those met are set aside.
    fn take_free(&mut self) -> Option<usize> {
        while let Some(i) = self.free.pop() {
            if self.retiring.is_some_and(|first| i >= first) {
                // Room was taken for the chunk's slots when it began to retire.
                self.retired.push(i);
                continue;
            }
            return Some(i);
        }
        None
    }

    /// A slot made for a read when idle time has not made one: none at the limit or while a
    /// chunk retires.
    fn make_now(&mut self) -> Option<usize> {
        if self.retiring.is_some() || self.slots.len() >= self.limit {
            return None;
        }
        self.grown = self.grown.saturating_add(1);
        if self.make_slot() {
            self.take_free()
        } else {
            None
        }
    }

    /// Pages the chunks have room for, made into slots or not.
    fn room(&self) -> usize {
        self.sizes.iter().fold(0usize, |a, &p| a.saturating_add(p))
    }

    /// One more slot, its page written once so the OS maps it now: the last chunk's next page,
    /// or a new chunk's first. False when the limit or the memory refuses one.
    fn make_slot(&mut self) -> bool {
        let full = match (self.chunks.last(), self.sizes.last()) {
            (Some(c), Some(&pages)) => c.len() >= pages.saturating_mul(self.page),
            _ => true,
        };
        if full && !self.new_chunk() {
            return false;
        }
        let Some(chunk) = self.chunks.len().checked_sub(1) else {
            return false;
        };
        let Some(c) = self.chunks.get_mut(chunk) else {
            return false;
        };
        let at = c.len();
        let Some(end) = at.checked_add(self.page) else {
            return false;
        };
        // Within the room taken for the chunk: no reallocation, the page written once.
        c.resize(end, 0);
        let i = self.slots.len();
        self.slots.push(Slot {
            address: 0,
            freq: 0,
            queue: Queue::Small,
            live: false,
            newer: None,
            older: None,
            chunk,
            at,
            len: 0,
        });
        self.free.push(i);
        true
    }

    /// Takes a new chunk by the module's rule, with room in the slot and free lists for its
    /// pages, so making them reallocates nothing. False when the limit has no room left or the
    /// memory refuses it.
    fn new_chunk(&mut self) -> bool {
        let room = self.room();
        let step = (self.limit.saturating_mul(TUNE_STEP_PERCENT) / 100).max(1);
        let pages = room.max(1).min(step).min(self.limit.saturating_sub(room));
        if pages == 0 {
            return false;
        }
        let Some(bytes) = pages.checked_mul(self.page) else {
            return false;
        };
        let mut chunk = Vec::new();
        if chunk.try_reserve_exact(bytes).is_err()
            || self.slots.try_reserve_exact(pages).is_err()
            || self.free.try_reserve_exact(pages).is_err()
            || self.chunks.try_reserve(1).is_err()
            || self.starts.try_reserve(1).is_err()
            || self.sizes.try_reserve(1).is_err()
        {
            return false;
        }
        self.starts.push(self.slots.len());
        self.sizes.push(pages);
        self.chunks.push(chunk);
        true
    }

    fn push(&mut self, queue: Queue, i: usize) {
        let Some(link) = slot_link(i) else {
            return;
        };
        let q = match queue {
            Queue::Small => &mut self.small,
            Queue::Main => &mut self.main,
        };
        let older = q.newest;
        q.newest = Some(link);
        if q.oldest.is_none() {
            q.oldest = Some(link);
        }
        if let Some(slot) = self.slots.get_mut(i) {
            slot.newer = None;
            slot.older = older;
        }
        if let Some(slot) = older
            .and_then(slot_index)
            .and_then(|j| self.slots.get_mut(j))
        {
            slot.newer = Some(link);
        }
        match queue {
            Queue::Small => self.small_live = self.small_live.saturating_add(1),
            Queue::Main => self.main_live = self.main_live.saturating_add(1),
        }
    }

    /// Unlinks live slot `i`, touching only its neighbours and the queue's ends.
    fn unlink(&mut self, i: usize) {
        let Some(slot) = self.slots.get(i).copied().filter(|s| s.live) else {
            return;
        };
        let Some(link) = slot_link(i) else {
            return;
        };
        let q = match slot.queue {
            Queue::Small => &mut self.small,
            Queue::Main => &mut self.main,
        };
        if q.newest == Some(link) {
            q.newest = slot.older;
        }
        if q.oldest == Some(link) {
            q.oldest = slot.newer;
        }
        if let Some(s) = slot
            .newer
            .and_then(slot_index)
            .and_then(|j| self.slots.get_mut(j))
        {
            s.older = slot.older;
        }
        if let Some(s) = slot
            .older
            .and_then(slot_index)
            .and_then(|j| self.slots.get_mut(j))
        {
            s.newer = slot.newer;
        }
        if let Some(s) = self.slots.get_mut(i) {
            s.newer = None;
            s.older = None;
        }
        match slot.queue {
            Queue::Small => self.small_live = self.small_live.saturating_sub(1),
            Queue::Main => self.main_live = self.main_live.saturating_sub(1),
        }
    }

    /// Drops `address` from the cache: a page written there replaces what was cached.
    pub fn forget(&mut self, address: u64) {
        if let Some(i) = self
            .map
            .remove(address)
            .and_then(|i| usize::try_from(i).ok())
        {
            self.release(i);
        }
    }

    /// Frees slot `i` and removes its queue entry.
    fn release(&mut self, i: usize) {
        self.unlink(i);
        if let Some(slot) = self.slots.get_mut(i)
            && slot.live
        {
            slot.live = false;
            self.free.push(i);
        }
    }

    /// The slot holding `address`.
    fn slot_of(&self, address: u64) -> Option<usize> {
        self.map.get(address).and_then(|i| usize::try_from(i).ok())
    }

    /// Frees one slot within `steps` queue steps: from the small queue while it holds its
    /// share, else from main. Whether one was freed.
    fn evict(&mut self, steps: usize) -> bool {
        let held = self.held();
        let mut taken = 0u64;
        for _ in 0..steps {
            if self.held() < held {
                break;
            }
            self.step();
            taken = taken.saturating_add(1);
        }
        self.evict_steps_most = self.evict_steps_most.max(taken);
        self.held() < held
    }

    /// One queue step of eviction: the small queue's oldest page while that queue holds its
    /// share, else main's.
    fn step(&mut self) {
        if self.small_live >= self.small_cap() || self.main_live == 0 {
            self.evict_small();
        } else {
            self.evict_main();
        }
    }

    /// The small queue's oldest page moves to main if it was read `PROMOTE` times, else is
    /// freed and remembered in the ghost.
    fn evict_small(&mut self) {
        let Some(i) = self.small.oldest.and_then(slot_index) else {
            return;
        };
        let Some(slot) = self.slots.get(i).copied() else {
            return;
        };
        self.unlink(i);
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
        self.map.remove(slot.address);
        self.free.push(i);
        self.remember_ghost(slot.address);
    }

    /// The ghost's length in evictions.
    fn window(&self) -> u64 {
        u64::try_from(Self::ghost_cap(self.limit)).unwrap_or(u64::MAX)
    }

    /// Remembers `address` in the ghost, and sweeps a few of its entries out of the window.
    fn remember_ghost(&mut self, address: u64) {
        let window = self.window();
        if window == 0 {
            return;
        }
        self.ghost_seq = self.ghost_seq.wrapping_add(1);
        self.ghosts.insert(address, self.ghost_seq);
        let now = self.ghost_seq;
        self.ghosts
            .sweep(SWEEP, |_, seq| now.wrapping_sub(seq) < window);
    }

    /// Forgets `address` from the ghost; true if it was there.
    fn forget_ghost(&mut self, address: u64) -> bool {
        let window = self.window();
        let now = self.ghost_seq;
        self.ghosts
            .remove(address)
            .is_some_and(|seq| now.wrapping_sub(seq) < window)
    }

    /// Main's oldest page goes round again with one read fewer if it has any, else is freed.
    fn evict_main(&mut self) {
        let Some(i) = self.main.oldest.and_then(slot_index) else {
            return;
        };
        let Some(slot) = self.slots.get(i).copied() else {
            return;
        };
        self.unlink(i);
        if slot.freq > 0 {
            if let Some(s) = self.slots.get_mut(i) {
                s.freq = s.freq.saturating_sub(1);
            }
            self.push(Queue::Main, i);
            return;
        }
        if let Some(s) = self.slots.get_mut(i) {
            s.live = false;
        }
        self.map.remove(slot.address);
        self.free.push(i);
    }

    /// Sets the cache's limit to `pages` pages, at least one. Growing takes effect at once; a
    /// cache above a lowered limit is brought down by `trim`, and meanwhile an insert evicts a
    /// page for each it adds, so the pages held only fall. The ghost's window follows the limit.
    pub fn resize(&mut self, pages: usize) {
        self.limit = pages.max(1);
    }

    /// Up to `n` queue steps toward the limit: pages freed by the cache's own rule (S3-FIFO's
    /// evictions), then up to `n` freed buffers dropped. True while the cache is still above it.
    pub fn trim(&mut self, n: usize) -> bool {
        // `n` queue steps, not `n` evictions: an eviction may take a step for each page the
        // queues cycle.
        for _ in 0..n {
            if self.held() <= self.limit {
                break;
            }
            self.step();
        }
        self.settle_retirement();
        // Then the chunks above the limit, the last first: its slots looked at `n` a call, the
        // pages they hold dropped, and the chunk given back once none is left.
        let mut looked = 0usize;
        while self.room() > self.limit && looked < n {
            let (Some(&first), Some(&pages)) = (self.starts.last(), self.sizes.last()) else {
                break;
            };
            if self.retiring.is_none() {
                if self.retired.try_reserve_exact(pages).is_err() {
                    break;
                }
                self.retiring = Some(first);
                self.retire_at = self.slots.len();
            }
            while looked < n && self.retire_at > first {
                self.retire_at = self.retire_at.saturating_sub(1);
                looked = looked.saturating_add(1);
                let i = self.retire_at;
                if let Some(address) = self.slots.get(i).filter(|s| s.live).map(|s| s.address) {
                    self.map.remove(address);
                    self.release(i);
                }
            }
            if self.retire_at > first {
                break;
            }
            // Every page of the chunk dropped: its slots leave the free list and it is given back.
            self.free.retain(|&i| i < first);
            self.retired.clear();
            self.slots.truncate(first);
            self.chunks.pop();
            self.starts.pop();
            self.sizes.pop();
            self.retiring = None;
        }
        self.over() > 0
    }

    /// A retirement the limit no longer asks for ends: the slots it set aside are free again.
    fn settle_retirement(&mut self) {
        if self.retiring.is_some() && self.room() <= self.limit {
            self.retiring = None;
            self.free.append(&mut self.retired);
        }
    }

    /// Slots wanted and not made: the pages inserted since [`Self::reserve`] last caught up that
    /// the free slots do not cover, within the limit; none while chunks retire.
    pub fn unreserved(&self) -> usize {
        if self.retiring.is_some() {
            return 0;
        }
        self.wanted
            .saturating_sub(self.free.len())
            .min(self.limit.saturating_sub(self.slots.len()))
    }

    /// Makes up to `n` of the slots wanted, each its page written once so the OS maps it now: a
    /// read that misses later takes a made slot, with no allocation and no first-touch page
    /// fault on its path. Run in idle time; the slots made.
    pub fn reserve(&mut self, n: usize) -> usize {
        self.settle_retirement();
        let mut made = 0usize;
        while made < n && self.unreserved() > 0 {
            if !self.make_slot() {
                break;
            }
            made = made.saturating_add(1);
        }
        if self.unreserved() == 0 {
            self.wanted = 0;
        }
        made
    }

    /// The bytes its index and ghost take.
    pub fn index_bytes(&self) -> usize {
        self.map.bytes().saturating_add(self.ghosts.bytes())
    }

    /// Pages held, or pages the chunks have room for, above the limit: what `trim` has left
    /// to do.
    pub fn over(&self) -> usize {
        self.held().max(self.room()).saturating_sub(self.limit)
    }

    /// Whether `address` was evicted recently enough that the ghost still names it: a miss on
    /// it is a read a larger cache would have saved.
    pub fn in_ghost(&self, address: u64) -> bool {
        let window = self.window();
        self.ghosts
            .get(address)
            .is_some_and(|seq| self.ghost_seq.wrapping_sub(seq) < window)
    }

    /// Addresses the ghost may hold: the pages a miss on a ghost stands for.
    pub fn ghost_pages(&self) -> usize {
        Self::ghost_cap(self.limit)
    }

    /// Pages the cache may hold.
    pub fn pages(&self) -> usize {
        self.limit
    }

    /// Reads served, and reads missed.
    pub fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }

    /// The most queue steps one eviction took.
    pub fn evict_steps_most(&self) -> u64 {
        self.evict_steps_most
    }

    /// Inserts left out because their bounded eviction freed no slot, or no slot was made.
    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    /// Slots reads made themselves, in a chunk's room, when idle time had not made one.
    pub fn grown(&self) -> u64 {
        self.grown
    }

    /// The cache's bytes in memory: its slots' pages, holding a page or not.
    pub fn bytes(&self) -> usize {
        self.slots.len().saturating_mul(self.page)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    fn page(a: u64) -> Vec<u8> {
        format!("page {a}").into_bytes()
    }

    /// A cache whose slots are all made, as idle time leaves one its reads have filled.
    fn warm(pages: usize, page: usize) -> Cache {
        let mut c = Cache::new(pages, page);
        c.wanted = pages;
        c.reserve(usize::MAX);
        c
    }

    #[test]
    fn a_cache_takes_memory_only_as_reads_want_it_in_chunks_and_never_on_a_read() {
        // A cache of a million pages takes memory as its reads come, not its limit: the first
        // read makes its own slot, and idle time then makes as many as the reads since wanted.
        let mut c = Cache::new(1 << 20, 64);
        assert_eq!(c.unreserved(), 0);
        c.insert(1, &page(1));
        assert!(c.contains(1));
        assert_eq!(c.slots.len(), 1);
        assert_eq!(c.reserve(usize::MAX), 1);
        assert_eq!(c.unreserved(), 0);
        // The next read takes the slot idle time made; the one after makes its own. Each chunk
        // has room for as many pages as those before it.
        for a in 2..4u64 {
            c.insert(a, &page(a));
        }
        assert!(c.contains(2) && c.contains(3));
        assert_eq!(c.grown(), 2);
        assert_eq!(c.sizes, vec![1, 1, 2]);
        assert_eq!(c.bytes(), c.slots.len() * 64);
        assert_eq!(c.slots.len(), 3);
        // Chunks double up to a tuning step of the limit, so a cache filled to its limit took
        // O(log n) of them plus the steps.
        let mut c = Cache::new(4096, 64);
        c.wanted = 4096;
        c.reserve(usize::MAX);
        assert_eq!(c.slots.len(), 4096);
        let step = 4096 * TUNE_STEP_PERCENT / 100;
        assert!(c.sizes.iter().all(|&p| p <= step), "{:?}", c.sizes);
        assert!(c.chunks.len() <= 13 + 4096 / step + 1, "{}", c.chunks.len());
    }

    #[test]
    fn a_lowered_limit_gives_back_whole_chunks_the_last_first() {
        let mut c = warm(1000, 64);
        for a in 0..1000u64 {
            c.insert(a, &page(a));
        }
        let before = c.chunks.len();
        c.resize(500);
        while c.trim(64) {}
        assert!(c.room() <= 500, "{}", c.room());
        assert!(c.chunks.len() < before);
        // What it holds reads back exactly; what it dropped misses.
        for a in 0..1000u64 {
            let mut out = Vec::new();
            if c.get(a, &mut out) {
                assert_eq!(out, page(a));
            }
        }
        assert!(c.held() <= 500);
        // Grown back, it makes slots again as reads want them.
        c.resize(1000);
        for a in 1000..1100u64 {
            c.insert(a, &page(a));
        }
        c.reserve(usize::MAX);
        for a in 1000..1100u64 {
            c.insert(a, &page(a));
        }
        assert!(c.contains(1099));
    }

    #[test]
    fn a_cached_page_reads_back_and_a_forgotten_one_misses() {
        let mut c = warm(10, 64);
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
    fn repeated_rewrites_and_invalidations_keep_serving_current_pages() {
        let mut c = warm(64, 64);
        for a in 0..64u64 {
            c.insert(a, &page(a));
        }
        for _ in 0..3 {
            for a in 0..6u64 {
                assert!(c.get(a, &mut Vec::new()));
            }
        }
        // Rewrite pages throughout a full queue, including its newest page. Every rewrite
        // replaces a cached address, which used to leave another entry for later cleanup.
        for round in 0..512u64 {
            let a = 6 + round % 58;
            let payload = format!("rewrite {round}");
            c.insert(a, payload.as_bytes());
            let mut out = Vec::new();
            assert!(c.get(a, &mut out));
            assert_eq!(out, payload.as_bytes());
        }
        // A stream still admits its newest pages and retains the pages read again.
        let mut admitted = 0;
        for a in 64..256u64 {
            c.insert(a, &page(a));
            let mut out = Vec::new();
            if c.get(a, &mut out) {
                admitted += 1;
                assert_eq!(out, page(a));
            }
        }
        assert!(admitted > 0);
        for a in 0..6u64 {
            let mut out = Vec::new();
            assert!(c.get(a, &mut out));
            assert_eq!(out, page(a));
        }
        // Remove pages from main and small; the freed slots must serve only their new values.
        for a in [0, 3, 5, 250, 253, 255] {
            c.forget(a);
            assert!(!c.get(a, &mut Vec::new()));
            c.insert(a, b"replacement");
            let mut out = Vec::new();
            assert!(c.get(a, &mut out));
            assert_eq!(out, b"replacement");
        }
    }

    #[test]
    fn an_insert_into_a_cache_read_hot_takes_a_bounded_step_count_and_still_admits() {
        // Every page read to the most frequency: freeing a slot takes a step a page round
        // main, which no one insert pays. Each takes at most the bound, the pages it does not
        // admit are only left out, and the hand goes on, so new pages are admitted again.
        let n = 2048u64;
        let mut c = warm(usize::try_from(n).unwrap(), 64);
        for a in 0..n {
            c.insert(a, &page(a));
        }
        for _ in 0..usize::from(MAX_FREQ) + 2 {
            for a in 0..n {
                assert!(c.get(a, &mut Vec::new()));
            }
        }
        let mut admitted = 0u64;
        for a in n..4 * n {
            c.insert(a, &page(a));
            if c.get(a, &mut Vec::new()) {
                admitted += 1;
            }
        }
        assert!(
            c.evict_steps_most() <= EVICT_STEPS as u64,
            "{}",
            c.evict_steps_most()
        );
        assert!(
            c.skipped() > 0 && admitted > 0,
            "skipped {} admitted {admitted}",
            c.skipped()
        );
        // Whatever it holds reads back exactly.
        for a in 0..4 * n {
            let mut out = Vec::new();
            if c.get(a, &mut out) {
                assert_eq!(out, page(a));
            }
        }
    }

    #[test]
    fn a_scan_does_not_flush_pages_read_again() {
        // Ten slots: pages 0..3 read three times each move to main; a scan of 1,000 pages read
        // once passes through the small queue and leaves them.
        let mut c = warm(10, 64);
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
        // Bounded: never more pages than the limit, nor a ghost past a few times its window.
        assert!(c.map.len() <= 10);
        assert!(c.ghosts.len() <= 4 * 16);
    }

    #[test]
    fn a_page_read_again_after_its_eviction_enters_main() {
        let mut c = warm(10, 64);
        // Fifteen pages into ten slots: the five oldest, 0 among them, leave the small queue
        // unread for the ghost, which holds nine.
        for a in 0..15u64 {
            c.insert(a, &page(a));
        }
        assert_eq!(c.ghosts.len(), 5);
        // Inserted again, page 0 enters main.
        assert!(c.in_ghost(0));
        c.insert(0, &page(0));
        let i = c.slot_of(0).unwrap();
        assert_eq!(c.slots[i].queue, Queue::Main);
        assert!(!c.in_ghost(0));
    }

    #[test]
    fn the_ghost_names_what_an_eager_window_of_evictions_does_and_stays_bounded() {
        let mut c = warm(40, 64);
        let cap = Cache::ghost_cap(40);
        // The eager window: the last `cap` evictions, newest at the front, a forgotten address
        // leaving a hole where it stands.
        let mut model: VecDeque<Option<u64>> = VecDeque::new();
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for step in 0..20_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let address = x % 64;
            let named = model.contains(&Some(address));
            if x.is_multiple_of(3) {
                for e in model.iter_mut() {
                    if *e == Some(address) {
                        *e = None;
                    }
                }
                assert_eq!(c.forget_ghost(address), named, "step {step}");
            } else if !named {
                if model.len() >= cap {
                    model.pop_back();
                }
                model.push_front(Some(address));
                c.remember_ghost(address);
            }
            for a in 0..64u64 {
                assert_eq!(c.in_ghost(a), model.contains(&Some(a)), "step {step} {a}");
            }
            assert!(c.ghosts.len() <= 4 * cap, "step {step}: {}", c.ghosts.len());
        }
    }

    #[test]
    fn a_resized_cache_serves_only_what_it_was_given_and_keeps_its_bounds() {
        let mut c = warm(64, 32);
        let mut most = 64;
        let mut latest: std::collections::HashMap<u64, Vec<u8>> = std::collections::HashMap::new();
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut out = Vec::new();
        let mut held = 0;
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
                9 if x & (1 << 41) == 0 => {
                    c.forget(address);
                    latest.remove(&address);
                }
                9 => {
                    c.reserve(1 + usize::try_from((x >> 20) % 16).unwrap());
                }
                _ if x & (1 << 40) == 0 => {
                    let pages = 1 + usize::try_from((x >> 20) % 128).unwrap();
                    c.resize(pages);
                    most = most.max(pages);
                }
                _ => {
                    c.trim(1 + usize::try_from((x >> 20) % 8).unwrap());
                }
            }
            // The map names live slots holding their own addresses; free, emptied and live
            // slots are all the slots; the queues' live counts are the pages held, within the
            // limit with the freed buffers; the ghost keeps its share.
            let live = c.slots.iter().filter(|s| s.live).count();
            assert_eq!(c.map.len(), live, "step {step}");
            assert!(c.map.entries().into_iter().all(|(a, i)| {
                let i = usize::try_from(i).unwrap();
                c.slots[i].live && c.slots[i].address == a
            }));
            assert_eq!(
                c.free.len() + c.retired.len() + live,
                c.slots.len(),
                "step {step}"
            );
            assert!(c.free.iter().all(|&i| i < c.slots.len()), "step {step}");
            assert_eq!(c.small_live + c.main_live, live, "step {step}");
            // Never above the limit by more than it was, nor growing while above it.
            assert!(live <= c.limit.max(held), "step {step}");
            held = live;
            if c.over() == 0 {
                assert!(c.slots.len() <= c.limit, "step {step}");
            }
            assert!(c.slots.len() <= most, "step {step}");
            assert!(
                c.ghosts.len() <= 4 * Cache::ghost_cap(most).max(16),
                "step {step}"
            );
            assert_eq!(
                c.bytes(),
                (live + c.free.len() + c.retired.len()) * 32,
                "step {step}"
            );
        }
    }
}
