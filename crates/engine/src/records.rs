//! A shard's cache of hot records, after F2's read cache (Kanellis et al., PVLDB 18, 2025, §7;
//! research/37): replicas of records read from the trunk, so a get of a read-hot key skips the
//! filters, the leaf index and the page search. The trunk keeps every original, so the cache needs
//! no durability of its own.
//!
//! Records live in a byte ring, appended at the tail and evicted at the head. A record read since
//! it was appended is moved to the tail once instead of evicted (the paper's second chance), so the
//! most read-hot records stay. A table maps each key's hash ([`crate::branch::filter::hash`]) to
//! its record. The paper's invariant holds: a write to a key drops its cached record
//! ([`RecordCache::invalidate`]) before the write returns, so the cache never answers with a
//! version older than one written.
//!
//! A record: key length (2), value length (4), flags (1), the key's hash (8), the key, the value.
//! A record that would cross the ring's end leaves a skip marker and starts at its beginning.

use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};

/// A record's fixed bytes: key length, value length, flags, hash.
const RECORD_HEAD: usize = 2 + 4 + 1 + 8;
/// A key length no record has: the rest of the ring to its end is empty.
const SKIP: u16 = u16::MAX;
/// Flags: the record is current, and it was read since it was appended.
const LIVE: u8 = 1;
const READ: u8 = 2;

/// The table's hasher: the key's hash is already xxh3, so it is used as it is.
#[derive(Default)]
struct Identity(u64);

impl Hasher for Identity {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

/// The cache: its ring, the ring's head and tail as offsets that only grow, and the table.
#[derive(Debug)]
pub struct RecordCache {
    ring: Vec<u8>,
    head: u64,
    tail: u64,
    table: HashMap<u64, u64, BuildHasherDefault<Identity>>,
    hits: u64,
    misses: u64,
    /// The hashes of records evicted unread, newest at the front, each with the sequence it was
    /// remembered at, as many as the ring holds records; `ghosts` names the live ones. A miss on
    /// one is a get a larger cache would have served: what the memory tuner prices the cache's
    /// bytes at (research/36). Removal is lazy, as the page cache's ghost's is.
    ghost: VecDeque<(u64, u64)>,
    ghosts: HashMap<u64, u64, BuildHasherDefault<Identity>>,
    ghost_seq: u64,
    ghost_hits: u64,
    /// Records held, for the ghost's length.
    live: usize,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Identity")
    }
}

/// A record's head, read at a ring position.
#[derive(Clone, Copy, Debug)]
struct Head {
    klen: u16,
    vlen: u32,
    flags: u8,
    hash: u64,
}

impl RecordCache {
    /// A cache of `bytes` of records; none fit below a record's head.
    pub fn new(bytes: usize) -> Self {
        Self {
            ring: vec![0; bytes],
            head: 0,
            tail: 0,
            table: HashMap::default(),
            hits: 0,
            misses: 0,
            ghost: VecDeque::new(),
            ghosts: HashMap::default(),
            ghost_seq: 0,
            ghost_hits: 0,
            live: 0,
        }
    }

    /// Misses on keys the ghost named: gets a larger cache would have served.
    pub fn ghost_hits(&self) -> u64 {
        self.ghost_hits
    }

    /// Remembers an evicted record's hash, the ghost as long as the records held.
    fn remember(&mut self, hash: u64) {
        while self.ghosts.len() >= self.live.max(1) {
            let Some((h, seq)) = self.ghost.pop_back() else {
                break;
            };
            if self.ghosts.get(&h) == Some(&seq) {
                self.ghosts.remove(&h);
            }
        }
        self.ghost_seq = self.ghost_seq.wrapping_add(1);
        self.ghost.push_front((hash, self.ghost_seq));
        self.ghosts.insert(hash, self.ghost_seq);
        // Stale entries as many as live ones: compacted, the queue within twice the ghost.
        if self.ghost.len() > self.ghosts.len().saturating_mul(2).max(1) {
            let ghosts = &self.ghosts;
            self.ghost.retain(|(h, seq)| ghosts.get(h) == Some(seq));
        }
    }

    /// Rebuilds the cache in `bytes`, keeping the newest records that fit, in their order.
    /// A tuning step's cost: a pass over the ring.
    pub fn resize(&mut self, bytes: usize) {
        let mut old = std::mem::replace(self, Self::new(bytes));
        (self.hits, self.misses, self.ghost_hits) = (old.hits, old.misses, old.ghost_hits);
        let mut offset = old.head;
        // Each step passes a record or a skip marker: at most the ring's bytes of steps.
        for _ in 0..=old.ring.len() {
            if offset >= old.tail {
                break;
            }
            let pos = old.at(offset);
            let to_end = old
                .cap()
                .saturating_sub(u64::try_from(pos).unwrap_or(u64::MAX));
            let skipped = to_end < 2
                || old
                    .ring
                    .get(pos..pos.saturating_add(2))
                    .and_then(<[u8]>::first_chunk::<2>)
                    .is_some_and(|b| u16::from_le_bytes(*b) == SKIP);
            if skipped {
                offset = offset.saturating_add(to_end);
                continue;
            }
            let Some(h) = old.head_at(pos) else { break };
            let Some(size) = usize::try_from(h.vlen)
                .ok()
                .and_then(|v| Self::size(usize::from(h.klen), v))
            else {
                break;
            };
            if h.flags & LIVE != 0 && old.table.get(&h.hash) == Some(&offset) {
                let kstart = pos.saturating_add(RECORD_HEAD);
                let kend = kstart.saturating_add(usize::from(h.klen));
                let vend = kend.saturating_add(usize::try_from(h.vlen).unwrap_or(0));
                if let (Some(k), Some(v)) = (old.ring.get(kstart..kend), old.ring.get(kend..vend)) {
                    self.insert(k, h.hash, v);
                }
            }
            offset = offset.saturating_add(size);
        }
        old.ring = Vec::new();
    }

    /// The ring's bytes.
    pub fn bytes(&self) -> usize {
        self.ring.len()
    }

    /// Gets served, and gets missed.
    pub fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }

    fn cap(&self) -> u64 {
        u64::try_from(self.ring.len()).unwrap_or(u64::MAX)
    }

    /// The ring position of an offset.
    fn at(&self, offset: u64) -> usize {
        usize::try_from(offset.checked_rem(self.cap()).unwrap_or(0)).unwrap_or(0)
    }

    fn head_at(&self, pos: usize) -> Option<Head> {
        let b = self.ring.get(pos..pos.checked_add(RECORD_HEAD)?)?;
        Some(Head {
            klen: u16::from_le_bytes([*b.first()?, *b.get(1)?]),
            vlen: u32::from_le_bytes(b.get(2..6)?.try_into().ok()?),
            flags: *b.get(6)?,
            hash: u64::from_le_bytes(b.get(7..15)?.try_into().ok()?),
        })
    }

    fn set_flags(&mut self, pos: usize, flags: u8) {
        if let Some(f) = self.ring.get_mut(pos.saturating_add(6)) {
            *f = flags;
        }
    }

    /// The bytes a record of `klen` and `vlen` takes.
    fn size(klen: usize, vlen: usize) -> Option<u64> {
        u64::try_from(RECORD_HEAD.checked_add(klen)?.checked_add(vlen)?).ok()
    }

    /// The value cached for `key` of hash `hash`, if its record is current; counted as read, so
    /// it has a second chance at the head.
    pub fn get(&mut self, key: &[u8], hash: u64) -> Option<&[u8]> {
        let Some(&offset) = self.table.get(&hash) else {
            self.misses = self.misses.saturating_add(1);
            if self.ghosts.contains_key(&hash) {
                self.ghost_hits = self.ghost_hits.saturating_add(1);
            }
            return None;
        };
        let pos = self.at(offset);
        let h = self.head_at(pos)?;
        let kstart = pos.checked_add(RECORD_HEAD)?;
        let kend = kstart.checked_add(usize::from(h.klen))?;
        let vend = kend.checked_add(usize::try_from(h.vlen).ok()?)?;
        if h.flags & LIVE == 0 || self.ring.get(kstart..kend)? != key {
            self.misses = self.misses.saturating_add(1);
            return None;
        }
        self.set_flags(pos, h.flags | READ);
        self.hits = self.hits.saturating_add(1);
        self.ring.get(kend..vend)
    }

    /// Drops `key`'s record, if cached: a write to the key makes it stale.
    pub fn invalidate(&mut self, key: &[u8], hash: u64) {
        let Some(&offset) = self.table.get(&hash) else {
            return;
        };
        let pos = self.at(offset);
        let Some(h) = self.head_at(pos) else { return };
        let kstart = pos.saturating_add(RECORD_HEAD);
        let same = self
            .ring
            .get(kstart..kstart.saturating_add(usize::from(h.klen)))
            .is_some_and(|k| k == key);
        if same {
            self.set_flags(pos, h.flags & !LIVE);
            self.table.remove(&hash);
            self.live = self.live.saturating_sub(1);
        }
    }

    /// Caches `value` for `key` of hash `hash`, evicting from the head as it needs room. A
    /// record larger than a quarter of the ring is not cached: it would evict too much for one.
    pub fn insert(&mut self, key: &[u8], hash: u64, value: &[u8]) {
        // The key's older record goes first, whether or not this one is cached: a cache that
        // keeps it would answer with a version older than the one just read.
        self.invalidate(key, hash);
        let (Ok(klen), Ok(vlen)) = (u16::try_from(key.len()), u32::try_from(value.len())) else {
            return;
        };
        if klen == SKIP {
            return;
        }
        let Some(size) = Self::size(key.len(), value.len()) else {
            return;
        };
        if size.saturating_mul(4) > self.cap() {
            return;
        }
        if self.append(klen, vlen, hash, key, value, LIVE).is_some()
            && let Some(offset) = self.tail.checked_sub(size)
        {
            self.table.insert(hash, offset);
            self.live = self.live.saturating_add(1);
            self.ghosts.remove(&hash);
        }
    }

    /// Free bytes in the ring.
    fn free(&self) -> u64 {
        self.cap()
            .saturating_sub(self.tail.saturating_sub(self.head))
    }

    /// Bytes from the tail's position to the ring's end.
    fn tail_to_end(&self) -> u64 {
        self.cap()
            .saturating_sub(u64::try_from(self.at(self.tail)).unwrap_or(u64::MAX))
    }

    /// Appends a record at the tail, evicting from the head until it fits; its offset, none if
    /// it could not. The wrap is recomputed each step, since a second chance moves the tail.
    fn append(
        &mut self,
        klen: u16,
        vlen: u32,
        hash: u64,
        key: &[u8],
        value: &[u8],
        flags: u8,
    ) -> Option<u64> {
        let size = Self::size(key.len(), value.len())?;
        // Each step frees a record or moves one, each record at most once a pass: two passes.
        let bound = self.ring.len().saturating_mul(2);
        let mut fits = false;
        for _ in 0..=bound {
            let to_end = self.tail_to_end();
            let skip = if size > to_end { to_end } else { 0 };
            if self.free() >= size.checked_add(skip)? {
                if skip > 0 {
                    self.mark_skip();
                    self.tail = self.tail.checked_add(skip)?;
                }
                fits = true;
                break;
            }
            self.evict_one()?;
        }
        if !fits {
            return None;
        }
        let offset = self.tail;
        let at = self.at(offset);
        let end = at.checked_add(usize::try_from(size).ok()?)?;
        let dst = self.ring.get_mut(at..end)?;
        let (head, rest) = dst.split_at_mut(RECORD_HEAD);
        head.get_mut(..2)?.copy_from_slice(&klen.to_le_bytes());
        head.get_mut(2..6)?.copy_from_slice(&vlen.to_le_bytes());
        *head.get_mut(6)? = flags;
        head.get_mut(7..15)?.copy_from_slice(&hash.to_le_bytes());
        let (k, v) = rest.split_at_mut(key.len());
        k.copy_from_slice(key);
        v.copy_from_slice(value);
        self.tail = self.tail.checked_add(size)?;
        Some(offset)
    }

    /// Marks the rest of the ring from the tail as empty, when two bytes remain to mark it.
    fn mark_skip(&mut self) {
        let at = self.at(self.tail);
        if self.tail_to_end() >= 2
            && let Some(b) = self.ring.get_mut(at..at.saturating_add(2))
        {
            b.copy_from_slice(&SKIP.to_le_bytes());
        }
    }

    /// One step at the head: past a skip marker, past a record no longer current, an unread
    /// record evicted, or a read one moved to the tail with its read flag cleared (its second
    /// chance) when it fits there without wrapping, else evicted.
    fn evict_one(&mut self) -> Option<()> {
        if self.head >= self.tail {
            return None;
        }
        let pos = self.at(self.head);
        let to_end = self.cap().checked_sub(u64::try_from(pos).ok()?)?;
        let skipped = to_end < 2
            || self
                .ring
                .get(pos..pos.checked_add(2)?)
                .and_then(<[u8]>::first_chunk::<2>)
                .is_some_and(|b| u16::from_le_bytes(*b) == SKIP);
        if skipped {
            self.head = self.head.checked_add(to_end)?;
            return Some(());
        }
        let h = self.head_at(pos)?;
        let size = Self::size(usize::from(h.klen), usize::try_from(h.vlen).ok()?)?;
        let offset = self.head;
        self.head = self.head.checked_add(size)?;
        if !(h.flags & LIVE != 0 && self.table.get(&h.hash) == Some(&offset)) {
            return Some(());
        }
        if h.flags & READ == 0 || size > self.tail_to_end() || size > self.free() {
            self.table.remove(&h.hash);
            self.live = self.live.saturating_sub(1);
            self.remember(h.hash);
            return Some(());
        }
        // Moved within the ring: its old bytes lie in what was just freed, and copy_within
        // copies through any overlap.
        let len = usize::try_from(size).ok()?;
        let dst = self.at(self.tail);
        self.ring.copy_within(pos..pos.checked_add(len)?, dst);
        self.set_flags(dst, LIVE);
        self.table.insert(h.hash, self.tail);
        self.tail = self.tail.checked_add(size)?;
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::filter::hash;

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn a_cached_value_is_the_latest_written_or_nothing() {
        for cap in [64usize, 200, 512, 4096] {
            let mut c = RecordCache::new(cap);
            // What a get may answer for each key: the value last inserted, or nothing once
            // invalidated (the cache may also have evicted it, so a miss is always allowed).
            let mut latest: HashMap<u32, Option<Vec<u8>>> = HashMap::new();
            let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ cap as u64;
            let mut hits = 0u64;
            for step in 0..40_000u32 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let n = (x % 48) as u32;
                let key = n.to_be_bytes();
                let h = hash(&key);
                match x % 10 {
                    0..=3 => {
                        let len = (x >> 16) as usize % 60;
                        let value: Vec<u8> = (0..len).map(|i| (i as u32 ^ step) as u8).collect();
                        c.insert(&key, h, &value);
                        latest.insert(n, Some(value));
                    }
                    4 => {
                        c.invalidate(&key, h);
                        latest.insert(n, None);
                    }
                    5 if step % 97 == 0 => {
                        // Resized, the newest records kept: what stays still answers exactly.
                        c.resize(cap / 2 + (x >> 24) as usize % cap);
                    }
                    _ => {
                        if let Some(v) = c.get(&key, h) {
                            hits += 1;
                            assert_eq!(
                                Some(v),
                                latest.get(&n).and_then(Option::as_deref),
                                "cap {cap} step {step} key {n}"
                            );
                        }
                    }
                }
                assert!(c.tail - c.head <= c.cap(), "cap {cap} step {step}");
                for (&h, &offset) in &c.table {
                    let rec = c.head_at(c.at(offset)).unwrap();
                    assert!(
                        rec.flags & LIVE != 0 && rec.hash == h,
                        "cap {cap} step {step}"
                    );
                    assert!(offset >= c.head && offset < c.tail, "cap {cap} step {step}");
                }
            }
            if cap >= 512 {
                assert!(hits > 0, "cap {cap}: the cache served nothing");
            }
        }
    }

    #[test]
    fn a_record_read_again_survives_the_head_once() {
        // Records of 16 + 15 bytes in a ring of 124: four fit. The first is read, so when a
        // fifth needs room it moves to the tail and the second is evicted instead.
        let mut c = RecordCache::new(4 * 31);
        let keys: Vec<[u8; 4]> = (0..5u32).map(u32::to_be_bytes).collect();
        let value = [7u8; 12];
        for k in &keys[..4] {
            c.insert(k, hash(k), &value);
        }
        assert!(c.get(&keys[0], hash(&keys[0])).is_some());
        c.insert(&keys[4], hash(&keys[4]), &value);
        assert!(
            c.get(&keys[0], hash(&keys[0])).is_some(),
            "the read record was evicted"
        );
        assert!(
            c.get(&keys[1], hash(&keys[1])).is_none(),
            "the unread record stayed"
        );
        assert!(c.get(&keys[4], hash(&keys[4])).is_some());
    }
}
